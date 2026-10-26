"""ConfirmationEngine ownership; the orchestration context supplies explicit runtime ports.

Trading algorithms are unchanged. This service owns scheduling/logic, while
shared session state remains on Scanner during the incremental extraction.
"""

import asyncio
import logging
from copy import deepcopy
from typing import Any, Protocol

from .fundamentals import facts_asof
from .horizons import DURATIONS
from .models import DAY, Signal
from .native_streams import NativeStreams
from .orderflow import Book, footprint
from .risk import evaluate_risk
from .scoring import score
from .storage import now_ms
from .strategy import confirm

log = logging.getLogger(__name__)
TERMINAL = {"INVALIDATED", "EXPIRED", "RESOLVED"}


class RuntimePorts(Protocol):
    apply_trade_quality: Any
    cached_features: Any
    candle_cache: Any
    context: Any
    entry_ready: Any
    evaluate: Any
    exchange: Any
    generation_profile: Any
    lifecycle_bootstrapped: Any
    market_context: Any
    notifier: Any
    pending_symbols: Any
    record_confirmation_book_gap: Any
    recorder: Any
    settings: Any
    spread_history: Any
    store: Any
    streams: Any


class ConfirmationEngine:
    def __init__(self, runtime: RuntimePorts):
        self.runtime = runtime

    def flow_ready(self, symbol, now):
        runtime = self.runtime
        tape = runtime.streams.tapes.get(symbol)
        book = runtime.streams.books.get(symbol)
        start = now // 60000 * 60000 - runtime.settings.execution_window_seconds * 1000
        ready = bool(
            tape
            and book
            and book.fresh(now, runtime.settings.book_stale_ms)
            and 0 < tape.coverage_start <= start
            and 0 <= now - tape.last_receipt <= runtime.settings.trade_stale_ms
        )
        runtime.store.put(
            f"flow_warmup:{runtime.exchange}:{symbol}",
            dict(
                ready=ready,
                coverage_start_ms=tape.coverage_start if tape else None,
                tape_last_event_ms=tape.last_event if tape else None,
                book_last_event_ms=book.event_ms if book else None,
                required_window_ms=runtime.settings.execution_window_seconds * 1000,
            ),
        )
        return ready

    def entry_ready(self, signal, book, now):
        runtime = self.runtime
        from . import quality_v211
        from .production import entry_position
        from .quality_v22 import enabled as v22_enabled

        if quality_v211.enabled(signal) or v22_enabled(signal):
            price = (
                float(min(book.asks) if signal.direction == "LONG" else max(book.bids))
                if book.asks and book.bids
                else None
            )
            quality = quality_v211.entry_quality(
                signal,
                price,
                now,
                runtime.settings.min_net_rr,
                quote=quality_v211.quote_evidence(signal, book, now, runtime.settings),
                current=quality_v211.current_cost(signal, price, book),
            )
            signal.coverage["entry_quality_now"] = quality
            return quality["acceptable"]

        if now >= min(signal.expires_ms, signal.trigger_expires_ms or signal.expires_ms):
            return False
        if not book.fresh(now, runtime.settings.book_stale_ms):
            return False
        price = float(min(book.asks) if signal.direction == "LONG" else max(book.bids))
        from .quality import enabled, entry_quality

        if enabled(signal):
            quality = entry_quality(signal, price, now, runtime.settings.min_net_rr)
            signal.coverage["entry_quality_now"] = quality
            return quality["entry_not_chased"]
        signal.coverage["entry_position"] = entry_position(
            signal, price, book.features(now).get("spread_bps", 0)
        )
        return signal.coverage["entry_position"]["state"] in {"IDEAL_ENTRY", "ACCEPTABLE_ENTRY"}

    def apply_trade_quality(self, signal, book, original_now):
        runtime = self.runtime
        from .quality import apply

        # Live books advance while flow workers yield. Execution is observed
        # now, independently of the original completed flow-window timestamp.
        from .quality_v22 import enabled as v22_enabled
        from .quality_v211 import enabled

        checked_ms = now_ms() if enabled(signal) or v22_enabled(signal) else original_now
        liquidity = runtime.store.get(f"liquidity-v21:{signal.source}:{signal.symbol}", {})
        if not 0 <= checked_ms - liquidity.get("at_ms", 0) <= 120000:
            liquidity = {}
        price = float(min(book.asks) if signal.direction == "LONG" else max(book.bids))
        apply(
            signal,
            price,
            checked_ms,
            liquidity,
            runtime.settings.min_net_rr,
            book=book,
            settings=runtime.settings,
        )

    def record_confirmation_book_gap(self, signal, now, start, end):
        runtime = self.runtime
        from .coverage import record as coverage_record
        from .funnel import emit

        signal.coverage.update(
            book_fresh=False,
            trade_window_complete=False,
            reasons=["order book unavailable during confirmation"],
        )
        coverage_record(runtime, signal, now, ["COVERAGE_BOOK_STALE"], start, end)
        runtime.store.signal(signal, signal.coverage["readiness"])
        emit(runtime.store, "coverage_incomplete", now, signal=signal, reason="COVERAGE_BOOK_STALE")

    async def evaluate(self):
        runtime = self.runtime
        now = now_ms()
        runtime.recorder.critical_symbols = set(runtime.pending_symbols()) | {"BTCUSDT", "ETHUSDT"}
        runtime.recorder.critical_symbols.update(
            r[0]
            for r in runtime.store.db.execute(
                "SELECT DISTINCT json_extract(s.payload,'$.signal.symbol') FROM ml_snapshots s WHERE s.stage='decision' "
                "AND NOT EXISTS (SELECT 1 FROM ml_labels l WHERE l.snapshot_id=s.id AND l.policy='prints-v1') "
                "AND s.decision_ms>?",
                (now - 7 * DAY,),
            )
        )
        for payload in runtime.store.active_signals():
            s = Signal.model_validate(payload)
            # Feed callbacks advance receipts while earlier candidates await flow
            # calculations. Assess this candidate against the current clock.
            now = now_ms()
            if s.source == "tradingview":
                continue  # Legacy chart rows are retired at startup, not live-monitored.
            if s.state in TERMINAL:
                continue
            if s.id in getattr(runtime, "reconcile_pending", set()):
                continue
            if s.state == "ALERTED":
                continue
            from .v2 import is_legacy

            if is_legacy(s, runtime.store.get("bybitflow_v2_cutover")):
                # Original-source lifecycle/health loops remain responsible. Only
                # a previously confirmed frozen decision may finish deferred delivery.
                if s.state == "CONFIRMED" and not s.gates:
                    from .macro import state as macro_state

                    book = runtime.streams.books.get(s.symbol) if s.source == runtime.exchange else None
                    if (
                        book
                        and book.fresh(now, runtime.settings.book_stale_ms)
                        and runtime.recorder.healthy
                        and runtime.entry_ready(s, book, now)
                        and not macro_state(runtime.store, runtime.settings, now)["macro_pause_active"]
                    ):
                        if await runtime.notifier.send_research(s) == "sent":
                            s.state = "ALERTED"
                            runtime.store.signal(s, "Original confirmed decision delivered after cutover")
                continue
            c = runtime.context.get(s.symbol)
            book, tape = runtime.streams.books.get(s.symbol), runtime.streams.tapes.get(s.symbol)
            connected = (
                runtime.streams.connected_for(s.symbol)
                if isinstance(runtime.streams, NativeStreams)
                else runtime.streams.connected
            )
            mid = float((max(book.bids) + min(book.asks)) / 2) if book and book.valid else None
            production_v2 = ":production-v2" in s.version
            if now >= s.expires_ms or (
                production_v2 and s.trigger_expires_ms is not None and now >= s.trigger_expires_ms
            ):
                s.state = "EXPIRED"
            elif (
                not production_v2
                and s.evidence.get("execution_window_ms", 900_000) < 900_000
                and now > s.evidence.get("execution_window_end_ms", now) + 120_000
            ):
                s.state = "EXPIRED"
            elif s.source != runtime.exchange:
                continue
            if s.state in TERMINAL:
                runtime.store.signal(
                    s, s.invalidation if s.state == "INVALIDATED" else "entry window expired"
                )
                continue
            from .macro import state as macro_state

            macro = macro_state(runtime.store, runtime.settings, now)
            if not s.evidence.get("score_components"):
                s.evidence["macro"] = macro
            if macro["macro_pause_active"]:
                s.evidence["macro"] = macro
                from .funnel import emit

                emit(runtime.store, "macro_deferred", now, signal=s, reason="MACRO_EVENT_PAUSE")
                s.coverage["macro_deferred_ms"] = now
                runtime.store.signal(s, "High-impact USD entry pause; lifecycle continues")
                continue
            if not c or not tape or not book:
                from .coverage import record as coverage_record

                reason = (
                    "COVERAGE_SYMBOL_NOT_SELECTED"
                    if s.symbol not in runtime.streams.selected
                    else "COVERAGE_TAPE_NOT_CONNECTED"
                    if not tape
                    else "COVERAGE_BOOK_NOT_CONNECTED"
                    if not book
                    else "COVERAGE_CONTEXT_STALE"
                )
                coverage_record(runtime, s, now, [reason], now // 60000 * 60000 - 60000, now // 60000 * 60000)
                runtime.store.signal(s, reason)
                continue
            if s.horizon_profile not in {"LEGACY", "CORE_INTRADAY"}:
                cached = runtime.candle_cache.get((s.symbol, s.execution_timeframe))
                if not cached:
                    continue
                c = c | {"m15": cached[1]}
            execution_bars = c["m15"]
            candle_end = execution_bars[-1].end
            window_ms = s.evidence.get("execution_window_ms", 900_000)
            end = s.evidence.get("execution_window_end_ms", candle_end) if window_ms < 900_000 else candle_end
            if production_v2 and not s.evidence.get("score_components") and window_ms < 900_000:
                end = now // 60000 * 60000
            start = end - window_ms
            checks = {
                "recorder unavailable": runtime.recorder.healthy,
                "symbol feed unavailable or stale": connected,
                "order book stale": book.fresh(now, runtime.settings.book_stale_ms),
                f"full closed {window_ms // 1000}-second trade window not yet retained": 0
                < tape.coverage_start
                <= start,
                "latest trade stale": 0 <= now - tape.last_receipt <= runtime.settings.trade_stale_ms
                and -2000 <= now - tape.last_event <= runtime.settings.trade_stale_ms,
                "closed candle stale": 0 <= now - candle_end <= DURATIONS[s.execution_timeframe] + 60_000,
                "execution window stale": 0 <= now - end <= (120_000 if window_ms < 900_000 else 960_000),
                "market context stale": 0 <= now - c["asof"] <= runtime.settings.scan_seconds * 1000 + 60_000,
            }
            coverage_reasons = [reason for reason, passed in checks.items() if not passed]
            healthy = not coverage_reasons
            macro_deferred_ms = s.coverage.get("macro_deferred_ms")
            s.coverage = s.coverage | dict(
                book_fresh=book.fresh(now, runtime.settings.book_stale_ms),
                trade_window_complete=healthy,
                recording=runtime.recorder.healthy,
                historical_depth="not backfilled",
                fundamentals="manual facts or missing",
                checked_ms=now,
                window_start_ms=start,
                window_end_ms=end,
                retained_from_ms=tape.coverage_start,
                reasons=coverage_reasons,
            )
            if macro_deferred_ms is not None:
                s.coverage["macro_deferred_ms"] = macro_deferred_ms
            # One immutable first-covered execution decision per setup ID. Re-evaluating
            # changed flow against yesterday's frozen ML vector would corrupt meta-labels.
            if s.state == "CONFIRMED" and not s.gates and healthy:
                if s.horizon_profile == "EXTENDED_SWING":
                    continue
                if s.coverage.get("macro_deferred_ms"):
                    current_flow = footprint(
                        tape.window(start, end),
                        c["instrument"].tick,
                        runtime.cached_features(s.symbol, s.execution_timeframe, execution_bars, now)["atr"],
                    )
                    resumed_flow_ok = confirm(s, current_flow, execution_bars)
                    if production_v2:
                        from .evidence import flow_response
                        from .production import flow_support, structure_response

                        current_flow.update(
                            flow_response([t for t in tape.window(start, end) if t.receipt_ms <= now], {})
                        )
                        if ":flow-quality-v1" in s.version:
                            from .flow_quality import assess as assess_flow_quality
                            from .flow_quality import baseline as flow_baseline

                            _, history = flow_baseline(
                                runtime.store, s.source, s.symbol, s.entry_session, s.horizon_profile, end
                            )
                            current_flow["quality"] = await asyncio.to_thread(
                                assess_flow_quality,
                                [t for t in tape.window(start, end) if t.receipt_ms <= now],
                                c["instrument"].tick,
                                book.features(now),
                                current_flow,
                                now,
                                start,
                                end,
                                history,
                                runtime.store.get("cross:" + s.symbol, {}),
                            )
                        support = flow_support(
                            current_flow,
                            1 if s.direction == "LONG" else -1,
                            structure_response(s, execution_bars, now),
                        )
                        resumed_flow_ok = support["raw_fallback"] and support["state"] == "FLOW_SUPPORTIVE"
                    if end <= s.coverage["macro_deferred_ms"] or not resumed_flow_ok:
                        continue
                # A deferred last-mile send must not silently strand an immutable decision.
                if (
                    runtime.entry_ready(s, book, now_ms())
                    and await runtime.notifier.send_research(s) == "sent"
                ):
                    s.state = "ALERTED"
                    runtime.store.signal(s, "research card delivered; no trade executed")
                continue
            if s.evidence.get("score_components"):
                continue
            s.state = "PENDING CONFIRMATION"
            runtime.store.signal(s)
            from .funnel import emit, reject

            if not healthy:
                from .coverage import record as coverage_record

                codes = []
                for reason in coverage_reasons:
                    codes.append(
                        "COVERAGE_RECORDER_UNHEALTHY"
                        if reason == "recorder unavailable"
                        else "COVERAGE_TAPE_NOT_CONNECTED"
                        if reason == "symbol feed unavailable or stale"
                        else "COVERAGE_BOOK_STALE"
                        if reason == "order book stale"
                        else "COVERAGE_TRADE_STALE"
                        if reason == "latest trade stale"
                        else "COVERAGE_TAPE_WARMING"
                        if "not yet retained" in reason and now < tape.coverage_start + window_ms
                        else "COVERAGE_WINDOW_PREDATES_TAPE"
                        if "not yet retained" in reason
                        else "COVERAGE_CONTEXT_STALE"
                    )
                coverage_record(runtime, s, now, codes, start, end)
                runtime.store.signal(s, s.coverage["readiness"])
                emit(
                    runtime.store,
                    "coverage_incomplete",
                    now,
                    signal=s,
                    reason=codes[0],
                )
                continue
            emit(runtime.store, "confirmation_attempts", now, signal=s, key=f"confirmation:{s.id}:{end}")
            trades = [t for t in tape.window(start, end) if t.receipt_ms <= now]
            execution_features = runtime.cached_features(s.symbol, s.execution_timeframe, execution_bars, now)
            s.evidence["m15" if s.horizon_profile == "LEGACY" else "execution_features"] = execution_features
            # Generation already fixed these deadlines. Confirmation must not
            # extend or replace an existing setup's original lifecycle.
            if s.trigger_expires_ms is None:
                s.trigger_expires_ms = end + (120_000 if window_ms < 900_000 else 900_000)
            if s.holding_deadline_ms is None:
                s.holding_deadline_ms = now + s.expected_hold_max * 60_000
            if s.primary_tracking_deadline is None and s.horizon_profile != "LEGACY":
                s.primary_tracking_deadline = s.holding_deadline_ms
            # Use only additions at/before window close to avoid confirmation leakage.
            window_book = Book()
            window_book.valid = book.valid
            window_book.changes.extend(x for x in book.changes if x[0] <= end)
            flow = await asyncio.to_thread(
                footprint, trades, c["instrument"].tick, execution_features["atr"], window_book
            )
            bf = book.features(now)
            if not bf.get("available"):
                # A stream reset can occur while footprint work yields to its
                # worker. Missing depth is a coverage gap, not a spread value.
                runtime.record_confirmation_book_gap(s, now, start, end)
                continue
            book_epoch = book.ofi_epoch
            from .evidence import flow_response

            prior_trades = tape.window(start - window_ms, start)
            previous_response = await asyncio.to_thread(flow_response, prior_trades, bf)
            flow.update(await asyncio.to_thread(flow_response, trades, bf, previous_response))
            from .flow_quality import assess as assess_flow_quality
            from .flow_quality import baseline as flow_baseline

            _, quality_history = flow_baseline(
                runtime.store, s.source, s.symbol, s.entry_session, s.horizon_profile, end
            )
            quality = await asyncio.to_thread(
                assess_flow_quality,
                trades,
                c["instrument"].tick,
                bf,
                flow,
                now,
                start,
                end,
                quality_history,
                runtime.store.get("cross:" + s.symbol, {}),
            )
            # Flow response/quality workers also yield. A reconnect can clear
            # the live book after the earlier footprint check, or replace its
            # epoch before execution pricing. Retry without freezing a decision.
            if not book.valid or not book.bids or not book.asks or book.ofi_epoch != book_epoch:
                runtime.record_confirmation_book_gap(s, now, start, end)
                continue
            flow["quality"] = quality
            s.evidence["flow_quality"] = quality
            if ":flow-quality-v1" not in s.version:
                flow.pop("quality")
            s.evidence["effective_volume_profile"] = {
                "policy": quality["flow_quality_policy"],
                "available": quality["flow_quality_state"] != "UNAVAILABLE",
                "coverage_complete": healthy,
                "profile_confidence": quality.get("profile_confidence", "LOW"),
                "poc": quality.get("effective_poc"),
                "vah": quality.get("effective_vah"),
                "val": quality.get("effective_val"),
                "hvn": quality.get("effective_hvn"),
                "lvn": quality.get("effective_lvn"),
                "raw_vs_effective_profile_shift": quality.get("raw_vs_effective_profile_shift"),
                "available_ms": now,
                "end_ms": end,
            }
            flow_ok = confirm(s, flow, execution_bars)
            if flow_ok and not production_v2:
                emit(runtime.store, "flow_confirmed", now, signal=s, key=f"flow:{s.id}:{end}")
            s.gates = []
            normal_spread = runtime.spread_history(s.symbol).assess(
                now, runtime.settings.max_spread_bps, runtime.settings.spread_min_samples
            )
            s.evidence["normal_spread"] = normal_spread
            s.gates.extend(normal_spread["reasons"])
            if not flow_ok and not production_v2:
                s.gates.append("executed order flow did not confirm family trigger")
            from .quality_v22 import enabled as v22_enabled
            from .quality_v211 import enabled as quality_v211_enabled

            def new_quality(signal):
                return quality_v211_enabled(signal) or v22_enabled(signal)

            if now - end > 900_000 and not new_quality(s):
                s.gates.append("execution trigger expired")
            if end < s.evidence["trigger_bar_end"]:
                s.gates.append("execution confirmation predates setup")
            if (mid is None or not s.zone[0] <= mid <= s.zone[1]) and not new_quality(s):
                s.gates.append("current price outside planned entry zone")
            if bf["spread_bps"] > runtime.settings.max_spread_bps:
                s.gates.append("current spread exceeds gate")
            d = dict(c["derivatives"])
            rate = d["funding_rate"]
            if rate is None or not 0 <= now - d["ticker_observed_ms"] <= 1_200_000:
                s.gates.append("derivatives ticker unavailable or stale")
            if rate is not None and (rate if s.direction == "LONG" else -rate) > 0.001:
                s.gates.append("extreme same-direction funding crowding")
            d["liquidation_feed"] = {
                "bybit": "actual allLiquidation events; reported bankruptcy prices",
                "binance": "sampled forceOrder events; approximate last-fill notional, not a census",
                "okx": "unavailable; stream not implemented",
            }[runtime.exchange]
            d["liquidations"] = (
                list(runtime.streams.liquidations[s.symbol])[-50:] if runtime.exchange != "okx" else None
            )
            facts = facts_asof(runtime.store, c["instrument"].base, now)
            if any(f["major_event"] for f in facts):
                s.gates.append("known major asset event")
            portfolio = runtime.store.get("portfolio")
            if portfolio and now - portfolio["at_ms"] > DAY:
                portfolio = None
            s.risk = evaluate_risk(s, c["instrument"], runtime.settings, book, portfolio, rate)
            s.gates.extend(s.risk["reasons"])
            from .cross_venue import fresh_comparison

            s.evidence.update(
                cross_exchange=fresh_comparison(
                    runtime.store.get("cross:" + s.symbol, {}), now, runtime.exchange
                ),
                flow=flow,
                book=bf,
                derivatives=d,
                fundamentals=facts,
                cross_market=runtime.market_context(now),
            )
            s.evidence["flow_confirmation_mode"] = "NONE"
            if ":cross-venue-flow-substitution-v1" in s.version:
                from .cross_venue import substitution
                from .production import structure_response

                remote = substitution(
                    s,
                    flow,
                    s.evidence["cross_exchange"],
                    start,
                    end,
                    now,
                    structure_response(s, execution_bars, now),
                )
                s.evidence["flow_substitution"] = remote
                if remote["passed"]:
                    s.evidence["flow_confirmation_mode"] = "CROSS_VENUE_SUBSTITUTION"
            if (
                ":flow-quality-v1" in s.version
                and quality.get("flow_trust_score") is not None
                and quality["flow_trust_score"] < 0.6
                and not s.evidence.get("flow_substitution", {}).get("passed")
            ):
                s.gates.append("LOW_INFORMATION_EXECUTED_FLOW")
            if s.horizon_profile != "LEGACY":
                from .evidence import (
                    auction_context,
                    derivatives_context,
                    execution_context,
                    factor_context,
                    range_context,
                    session_baseline,
                )
                from .observations import recommend

                setup_bars = runtime.candle_cache.get((s.symbol, s.setup_timeframe), (None, c["h1"]))[1]
                previous_flow = footprint(
                    tape.window(start - window_ms, start), c["instrument"].tick, execution_features["atr"]
                )

                def factor_bars(symbol):
                    if symbol == s.symbol:
                        return []
                    return runtime.candle_cache.get((symbol, s.setup_timeframe), (None, []))[1]

                if s.feature_schema_version == "candidate-v20":
                    # Profile readiness can change after candidate generation.
                    # Freeze the causal native profile at the first scored decision,
                    # alongside the current flow; never revise an existing decision.
                    s.evidence["volume_profile"] = deepcopy(
                        runtime.generation_profile(c["instrument"], setup_bars, now)
                    )
                s.evidence.update(
                    range=range_context(setup_bars, now),
                    auction=auction_context(flow, previous_flow, mid),
                    market_factor=factor_context(
                        setup_bars, factor_bars("BTCUSDT"), factor_bars("ETHUSDT"), asof=now
                    ),
                    execution=execution_context(s, execution_bars, book, runtime.settings),
                    selection=runtime.store.get("deep_selection", {}).get(s.symbol, {}),
                    horizon_suitability=recommend(runtime.store, s, now),
                    derivatives=derivatives_context(d, setup_bars),
                    session_metrics=session_baseline(runtime.store, s, flow, bf, now, window_end_ms=end),
                )
                s.evidence["factor_timeframes"] = {
                    tf: factor_context(
                        runtime.candle_cache.get((s.symbol, tf), (None, []))[1],
                        runtime.candle_cache.get(("BTCUSDT", tf), (None, []))[1]
                        if s.symbol != "BTCUSDT"
                        else [],
                        runtime.candle_cache.get(("ETHUSDT", tf), (None, []))[1]
                        if s.symbol != "ETHUSDT"
                        else [],
                        asof=now,
                    )
                    for tf in dict.fromkeys((s.context_timeframe, s.setup_timeframe, s.execution_timeframe))
                }
                if production_v2:
                    from .production import (
                        INTRADAY,
                        REVERSALS,
                        evaluate_intraday_confirmation,
                        market_alignment,
                        structure_response,
                    )

                    s.evidence["factor_regimes"] = {}
                    for tf in dict.fromkeys((s.context_timeframe, s.setup_timeframe, s.execution_timeframe)):
                        regimes = {}
                        for label, symbol in (("btc", "BTCUSDT"), ("eth", "ETHUSDT")):
                            closed = [
                                b
                                for b in runtime.candle_cache.get((symbol, tf), (None, []))[1]
                                if b.end <= now
                            ]
                            regimes[label] = (
                                runtime.cached_features(symbol, tf, closed, now) if len(closed) >= 60 else {}
                            )
                        s.evidence["factor_regimes"][tf] = regimes
                    regimes = s.evidence["factor_regimes"].get(s.setup_timeframe, {})
                    alignment = market_alignment(
                        s,
                        regimes.get("btc", {}),
                        regimes.get("eth", {}),
                        now,
                        structure_response(s, execution_bars, now),
                    )
                    s.evidence["market_alignment"] = alignment
                    if alignment.get("policy") == "market-alignment-v3":
                        history = runtime.store.get("market-alignment-history-v3", [])
                        identity = f"{s.id}:{end}"
                        if not any(row["identity"] == identity for row in history):
                            history.append(
                                dict(
                                    identity=identity,
                                    at_ms=now,
                                    source=s.source,
                                    state=alignment["market_alignment_state"],
                                    timeframe_context=alignment.get("timeframe_context"),
                                    blocked=alignment.get("blocked", False),
                                )
                            )
                            runtime.store.put("market-alignment-history-v3", history[-1000:])
                    if s.horizon_profile in INTRADAY and s.family in REVERSALS:
                        confirmation = evaluate_intraday_confirmation(s, flow, execution_bars, now, alignment)
                        s.evidence["confirmation"] = confirmation
                        flow_ok = confirmation["passed"]
                        s.evidence["flow_confirmation_mode"] = confirmation["flow_confirmation_mode"]
                        s.gates.extend(confirmation["reason_codes"])
                    else:
                        flow_ok = flow_ok or s.evidence.get("flow_substitution", {}).get("passed", False)
                        if flow_ok and s.evidence["flow_confirmation_mode"] == "NONE":
                            s.evidence["flow_confirmation_mode"] = (
                                "LOCAL_GENUINE_ABSORPTION"
                                if quality.get("flow_quality_state") == "GENUINE_ABSORPTION"
                                else "LOCAL_TRUSTED_FLOW"
                            )
                        if not flow_ok:
                            s.gates.append("executed order flow did not confirm family trigger")
                        if alignment["blocked"]:
                            s.gates.append(alignment["reason"])
                    if flow_ok:
                        emit(runtime.store, "flow_confirmed", now, signal=s, key=f"flow:{s.id}:{end}")
                elif ":autonomy-v1" in s.version:
                    from .thesis_health import market_gate

                    alignment = market_gate(
                        s,
                        runtime.cached_features("BTCUSDT", s.setup_timeframe, factor_bars("BTCUSDT"), now)
                        if len(factor_bars("BTCUSDT")) >= 60
                        else {},
                        runtime.cached_features("ETHUSDT", s.setup_timeframe, factor_bars("ETHUSDT"), now)
                        if len(factor_bars("ETHUSDT")) >= 60
                        else {},
                    )
                    s.evidence["market_alignment"] = alignment
                    if alignment["blocked"]:
                        s.gates.append(alignment["reason"])
                    if s.family in {"liquidity_sweep", "range_rejection"}:
                        sign = 1 if s.direction == "LONG" else -1
                        participation = (
                            sign * flow.get("delta_pct", 0) >= 10 and flow.get("delta_persistence", 0) >= 0.5
                        )
                        if not flow_ok or not participation:
                            s.gates.append("UNCONFIRMED_LIQUIDITY_SWEEP")
                if s.evidence["execution"]["stop_noise_ratio"] < 0.5:
                    s.gates.append("stop is inside half the observed execution-bar noise")
                if s.evidence["execution"]["volatility_regime"] == "VOLATILITY_SHOCK":
                    s.gates.append("volatility shock; execution assumptions require requalification")
                if s.horizon_profile == "EXTENDED_SWING":
                    s.evidence["publication_policy"] = "shadow research until horizon-specific validation"
            if production_v2 and s.gates:
                if new_quality(s):
                    runtime.apply_trade_quality(s, book, now)
                for gate in s.gates:
                    reject(runtime.store, s, gate, now)
                runtime.store.signal(s, "Awaiting valid evidence within the original entry window")
                continue
            new_v2 = s.feature_schema_version == "candidate-v20"
            if not new_v2:
                score(s, flow_ok, rate is not None, preserve_original=":hardening-v1" not in s.version)
            try:
                from .v8_runtime import decorate_signal

                if not s.gates:
                    decorate_signal(runtime, s, c, now, rank_opportunities=not new_v2)
                    if new_v2:
                        from .v2_evidence import score as score_v2
                        from .v8_runtime import rank_signal

                        if tape.event_features:
                            size, rotation = tape.event_features.snapshot(start, end, now, healthy)
                            s.evidence.update(trade_size_state=size, event_rotation_state=rotation)
                        score_v2(s, flow_ok, now)
                        rank_signal(runtime, s, now)
                    if s.evidence.get("confirmation_policy") == "v8-production-gating-v1":
                        from .v8_gating import apply_new_candidate

                        s.evidence["flow_confirmed"] = flow_ok
                        acceptable = runtime.entry_ready(s, book, now_ms() if new_quality(s) else now)
                        s.evidence["entry_quality_acceptable"] = acceptable
                        s.evidence["entry_not_chased"] = (
                            s.coverage["entry_quality_now"]["entry_not_chased"]
                            if new_quality(s)
                            else acceptable
                        )
                        from .production import structure_response

                        s.evidence["v8_structure_response"] = dict(
                            supportive=structure_response(s, execution_bars, now),
                            source_ms=execution_bars[-1].end,
                            available_ms=now,
                        )
                        apply_new_candidate(runtime, s, now, portfolio=portfolio)
            except Exception as exc:
                s.evidence["v8_research_error"] = type(exc).__name__
                if new_v2:
                    # Never send an unscored/malformed product-v2 decision.
                    s.gates.append("V2_EVIDENCE_UNAVAILABLE")
            if runtime.settings.sss_research and s.quality >= 95 and not s.gates and s.risk.get("accepted"):
                s.final_tier = "SSS RESEARCH · UNCALIBRATED"
            from .quality import enabled

            if enabled(s):
                runtime.apply_trade_quality(s, book, now)
            from .ml.inference import apply as apply_ml

            if production_v2:
                # ML is advisory for this explicit deterministic policy, including abstention.
                try:
                    apply_ml(
                        s,
                        runtime.settings.model_copy(update={"ml_filter_research": False}),
                        runtime.store,
                        now_ms(),
                    )
                except Exception as exc:
                    s.qualification["ml_reason"] = "Optional ML unavailable: " + type(exc).__name__
            else:
                apply_ml(s, runtime.settings, runtime.store, now_ms())
            runtime.recorder.offer(
                "features/signal",
                s.symbol,
                end,
                {
                    "exchange": runtime.exchange,
                    "signal_id": s.id,
                    "quality": s.quality,
                    "flow": flow,
                    "risk": s.risk,
                    "gates": s.gates,
                    "strategy_version": s.version,
                },
            )
            for gate in s.gates:
                reject(runtime.store, s, gate, now)
            if not s.gates:
                s.state = "CONFIRMED"
            runtime.store.signal(s)
            if s.state == "CONFIRMED":
                if s.horizon_profile == "EXTENDED_SWING":
                    continue
                # Last-mile freshness immediately before webhook I/O.
                if not book.fresh(now_ms(), runtime.settings.book_stale_ms) or not runtime.recorder.healthy:
                    continue
                if (
                    runtime.entry_ready(s, book, now_ms())
                    and await runtime.notifier.send_research(s) == "sent"
                ):
                    s.state = "ALERTED"
                    runtime.store.signal(s, "research card delivered; no trade executed")

    async def evaluation_loop(self):
        runtime = self.runtime
        await runtime.lifecycle_bootstrapped.wait()
        while True:
            try:
                await runtime.evaluate()
                runtime.store.put(
                    "confirmation_health",
                    runtime.store.get("confirmation_health", {})
                    | dict(state="HEALTHY", last_success_ms=now_ms()),
                )
            except Exception as exc:
                import traceback

                frames = traceback.extract_tb(exc.__traceback__)
                runtime.store.put(
                    "confirmation_health",
                    runtime.store.get("confirmation_health", {})
                    | dict(
                        state="DEGRADED",
                        last_error_ms=now_ms(),
                        last_error_type=type(exc).__name__,
                        sqlite_error=getattr(exc, "sqlite_errorname", None),
                        location=f"{frames[-1].name}:{frames[-1].lineno}" if frames else None,
                    ),
                )
                log.warning("evaluation_failed", extra={"error_type": type(exc).__name__})
            await asyncio.sleep(10)
