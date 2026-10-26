"""LifecycleCoordinator ownership; the orchestration context supplies explicit runtime ports.

Trading algorithms are unchanged. This service owns scheduling/logic, while
shared session state remains on Scanner during the incremental extraction.
"""

import asyncio
import logging
from typing import Any, Protocol

from .features import candle_features
from .fundamentals import facts_asof
from .horizons import DURATIONS
from .models import Signal
from .orderflow import footprint
from .storage import now_ms

log = logging.getLogger(__name__)
TERMINAL = {"INVALIDATED", "EXPIRED", "RESOLVED"}


class RuntimePorts(Protocol):
    cached_features: Any
    candle_cache: Any
    context: Any
    cross_venue: Any
    exchange: Any
    lifecycle_bootstrapped: Any
    monitor_alerted: Any
    notifier: Any
    reconcile_pending: Any
    recorder: Any
    settings: Any
    setup_runtime: Any
    store: Any


class LifecycleCoordinator:
    def __init__(self, runtime: RuntimePorts):
        self.runtime = runtime

    async def monitor_alerted(self, s, now):
        runtime = self.runtime
        from .lifecycle import source_feed

        previous = dict(s.coverage)
        same_source = s.source == runtime.exchange
        _, streams = source_feed(runtime, s.source)
        book = streams.books.get(s.symbol) if streams else None
        tape = streams.tapes.get(s.symbol) if streams else None
        context = (
            runtime.context.get(s.symbol)
            if same_source
            else runtime.cross_venue.contexts.get((s.source, s.symbol))
        )
        candles = (
            (lambda interval: runtime.candle_cache.get((s.symbol, interval), (None, []))[1])
            if same_source
            else (lambda interval: runtime.cross_venue.candles.get((s.source, s.symbol, interval), []))
        )
        fresh_book = bool(book and book.fresh(now, runtime.settings.book_stale_ms))
        fresh_tape = bool(
            tape
            and 0 <= now - tape.last_receipt <= runtime.settings.trade_stale_ms
            and -2000 <= now - tape.last_event <= runtime.settings.trade_stale_ms
        )
        context_fresh = bool(
            context and 0 <= now - context["asof"] <= runtime.settings.scan_seconds * 1000 + 60_000
        )
        reasons = []
        if not streams:
            reasons.append("Original exchange feed unavailable")
        if not fresh_book or not fresh_tape:
            reasons.append("Live prices unavailable or stale")
        if not context_fresh:
            reasons.append("Market context unavailable or stale")
        if not runtime.recorder.healthy:
            reasons.append("Recording unavailable")
        if reasons and s.horizon_profile != "LEGACY":
            s.evidence["primary_coverage_complete"] = False
        s.coverage = previous | dict(
            checked_ms=now,
            monitoring="paused" if reasons else "active",
            monitoring_reasons=reasons,
            monitoring_event=None,
        )
        if not reasons and s.horizon_profile != "LEGACY":
            from .evidence import flow_response
            from .thesis_health import evaluate as health_evaluate

            setup = candles(s.setup_timeframe)
            feature = (
                runtime.cached_features(s.symbol, s.setup_timeframe, setup, now)
                if same_source and len(setup) >= 60
                else candle_features(setup, now)
                if len(setup) >= 60
                else {}
            )
            health_window = 60_000 if s.horizon_profile == "SHORT_INTRADAY" else 900_000
            recent = tape.window(now - health_window, now)
            observed = await asyncio.to_thread(
                footprint,
                recent,
                context["instrument"].tick,
                feature.get("atr", float(context["instrument"].tick) * 10),
                book,
            )
            observed.update(await asyncio.to_thread(flow_response, recent, book.features(now)))
            bf = book.features(now)
            if ":flow-quality-v1" in s.version:
                from .flow_quality import assess as assess_flow_quality
                from .flow_quality import baseline as flow_baseline

                _, history = flow_baseline(
                    runtime.store, s.source, s.symbol, s.entry_session, s.horizon_profile, now
                )
                observed["quality"] = await asyncio.to_thread(
                    assess_flow_quality,
                    recent,
                    context["instrument"].tick,
                    bf,
                    observed,
                    now,
                    now - health_window,
                    now,
                    history,
                    runtime.store.get("cross:" + s.symbol, {}),
                )
            structure = (
                feature.get("bos") == ("down" if s.direction == "LONG" else "up")
                and bool(setup)
                and (setup[-1].close < s.zone[0] if s.direction == "LONG" else setup[-1].close > s.zone[1])
            )
            observation = dict(
                coverage_complete=bool(
                    tape.coverage_start is not None
                    and tape.coverage_start <= now - health_window
                    and observed.get("available")
                ),
                flow=observed,
                obi=bf.get("obi_persistence", 0),
                structural_failure=structure,
                structure_timeframe=s.setup_timeframe,
                window_end_ms=now,
            )
            sign = 1 if s.direction == "LONG" else -1
            profile = (
                getattr(
                    runtime,
                    "swing_profiles"
                    if s.horizon_profile in {"SWING", "EXTENDED_SWING"}
                    else "volume_profiles",
                    {},
                ).get(s.symbol, {})
                if same_source
                else {}
            )
            if profile.get("available") and 0 <= now - profile.get("available_ms", 0) <= 120000:
                adverse_value = profile.get("value_migration_direction") == ("DOWN" if sign > 0 else "UP")
                adverse_acceptance = profile.get("acceptance") == (
                    "BELOW_VALUE" if sign > 0 else "ABOVE_VALUE"
                )
                observation["auction_health"] = (
                    "DEGRADED" if adverse_value and adverse_acceptance else "HEALTHY"
                )
            factor_bars = (
                runtime.candle_cache.get(("BTCUSDT", s.context_timeframe), (None, []))[1]
                if same_source
                else []
            )
            original_alignment = s.evidence.get("market_alignment", {})
            if original_alignment.get("alignment") == "IDIOSYNCRATIC_DIVERGENCE":
                from .evidence import factor_context

                original_sign = 1 if s.direction == "LONG" else -1
                current_factor = (
                    factor_context(candles(s.context_timeframe), factor_bars, [], asof=now)
                    if len(factor_bars) >= 60
                    else {}
                )
                threshold = original_alignment.get("residual_threshold", 0.0015)
                current_residual = original_sign * current_factor.get("residual_btc", 0)
                if current_factor.get("available"):
                    observation["factor_health"] = (
                        "DEGRADED" if current_residual < 0.5 * threshold else "HEALTHY"
                    )
                    observation["original_divergence_residual"] = current_residual
            elif (
                len(factor_bars) >= 60 and now - factor_bars[-1].end <= DURATIONS[s.context_timeframe] + 60000
            ):
                factor = (
                    runtime.cached_features("BTCUSDT", s.context_timeframe, factor_bars, now)
                    if same_source
                    else candle_features(factor_bars, now)
                )
                observation["factor_health"] = (
                    "DEGRADED"
                    if factor.get("regime") == ("trending down" if sign > 0 else "trending up")
                    else "HEALTHY"
                )
            derivatives = context.get("derivatives", {})
            if (
                0 <= now - derivatives.get("collected_ms", 0) <= 360000
                and derivatives.get("oi_change_pct") is not None
            ):
                observation["derivatives_health"] = (
                    "DEGRADED"
                    if sign * observed.get("delta_pct", 0) <= -20 and abs(derivatives["oi_change_pct"]) >= 2
                    else "HEALTHY"
                )
            cross = runtime.store.get("cross:" + s.symbol, {})
            if s.evidence.get("flow_confirmation_mode") == "CROSS_VENUE_SUBSTITUTION":
                original_sign = s.evidence.get("flow_substitution", {}).get("trusted_consensus_delta_sign")
                support_persists = bool(
                    cross.get("available")
                    and 0 <= now - cross.get("receipt_ms", 0) <= 45_000
                    and cross.get("trusted_consensus_delta_sign") == original_sign
                )
                current_quality = observed.get("quality", {})
                adverse_local = bool(
                    (current_quality.get("flow_trust_score") or 0) >= 0.6
                    and sign * observed.get("delta_pct", 0) <= -20
                    and sign * observed.get("cvd_slope", 0) < 0
                )
                observation["cross_venue_health"] = (
                    "HEALTHY" if support_persists else "DEGRADED" if adverse_local and structure else "MIXED"
                )
            elif (
                cross.get("available")
                and s.source in cross.get("exchanges", [])
                and cross.get("aligned_flow_venues", 0) >= 2
                and 0 <= now - cross.get("receipt_ms", 0) <= 45000
            ):
                observation["cross_venue_health"] = (
                    "DEGRADED"
                    if cross.get("trusted_consensus_delta_sign") == ("NEGATIVE" if sign > 0 else "POSITIVE")
                    else "MIXED"
                    if cross.get("consensus_delta_sign") == "MIXED"
                    else "HEALTHY"
                )
            observation["volatility_liquidity_health"] = (
                "DEGRADED" if bf.get("spread_bps", 0) > runtime.settings.max_spread_bps else "HEALTHY"
            )
            key = "thesis_health:" + s.id
            health = health_evaluate(s, observation, runtime.store.get(key, {}), now)
            runtime.store.put(key, health)
            if health["withdraw"]:
                s.state = "INVALIDATED"
                s.coverage["terminal_reason"] = health["reason"]
                s.evidence["withdrawal"] = health
                runtime.store.signal(s, health["reason"])
                await runtime.notifier.send_research(s, update=True)
                return
        if now >= (s.holding_deadline_ms or s.expires_ms):
            deadline = s.holding_deadline_ms or s.expires_ms
            if (
                not reasons
                and s.coverage.get("monitor_status") == "MONITOR_READY"
                and s.id not in getattr(runtime, "reconcile_pending", set())
            ):
                from .lifecycle import expiry_mark

                mark = expiry_mark(
                    s,
                    s.evidence.get("latest_observed_price"),
                    s.coverage.get("monitor_cursor_event_ms"),
                    deadline,
                    runtime.settings.trade_stale_ms,
                    "LIVE_EXECUTED_TRADE",
                )
                if mark:
                    s.evidence["expiry_mark"] = mark
            s.state = "EXPIRED"
            s.coverage["tracking_end"] = True
            s.coverage["monitoring_event"] = "ended"
            s.invalidation = "Tracking period ended; no account outcome is inferred"
            s.coverage["terminal_reason"] = s.invalidation
            s.evidence["terminal_event"] = dict(
                effective_ms=deadline,
                detected_ms=now,
                reference_price=s.entry,
                source=s.source,
                method="LIFECYCLE_DEADLINE",
            )
        elif reasons:
            since = previous.get("pause_since_ms") or now
            s.coverage["pause_since_ms"] = since
            if now - since >= 60_000 and not previous.get("pause_notice_sent"):
                s.coverage["monitoring_event"] = "paused"
                result = await runtime.notifier.send_research(s, update=True)
                s.coverage["pause_notice_sent"] = result in {"sent", "already-attempted"}
            runtime.store.signal(s, "Monitoring paused; setup outcome unknown")
            return
        else:
            facts = facts_asof(runtime.store, context["instrument"].base, now)
            context_bars = context["h4"]
            if s.horizon_profile != "LEGACY":
                cached = candles(s.context_timeframe)
                if cached:
                    context_bars = cached
            regime = candle_features(context_bars, now)["regime"]
            supportive = regime in {"range", "trending up" if s.direction == "LONG" else "trending down"}
            if s.horizon_profile == "LEGACY" and (not supportive or any(f["major_event"] for f in facts)):
                s.state = "INVALIDATED"
                s.invalidation = (
                    "Higher-timeframe market regime no longer supports the setup"
                    if not supportive
                    else "A known major asset event invalidated the setup"
                )
            elif previous.get("pause_since_ms"):
                s.coverage["monitoring_event"] = "resumed" if previous.get("pause_notice_sent") else None
                if s.coverage["monitoring_event"]:
                    await runtime.notifier.send_research(s, update=True)
                s.coverage.pop("pause_since_ms", None)
                s.coverage.pop("pause_notice_sent", None)
        if s.state not in TERMINAL and not reasons:
            s.coverage["last_trustworthy_ms"] = now
            if s.evidence.get("production_policies", {}).get("trade_quality") == "trade-validity-v22":
                await runtime.setup_runtime.observe_health(
                    s, observed, bf, observation["coverage_complete"], book, now_ms()
                )
        runtime.store.signal(s, s.invalidation if s.state in TERMINAL else "Monitoring active")
        if s.state in TERMINAL:
            await runtime.notifier.send_research(s, update=True)

    async def health_loop(self):
        runtime = self.runtime
        await runtime.lifecycle_bootstrapped.wait()
        while True:
            for payload in runtime.store.active_signals():
                s = Signal.model_validate(payload)
                if s.source == "tradingview":
                    continue
                try:
                    if (
                        s.id not in runtime.reconcile_pending
                        and s.coverage.get("monitor_status") != "RECONCILED_PENDING_PATH"
                    ):
                        await runtime.monitor_alerted(s, now_ms())
                except Exception as exc:
                    runtime.store.put(
                        "thesis_health:" + s.id,
                        dict(
                            state="DEGRADED",
                            checked_ms=now_ms(),
                            coverage_status="UNAVAILABLE",
                            error_type=type(exc).__name__,
                        ),
                    )
            await asyncio.sleep(15)
