"""MarketDiscoveryCoordinator ownership; the orchestration context supplies explicit runtime ports.

Trading algorithms are unchanged. This service owns scheduling/logic, while
shared session state remains on Scanner during the incremental extraction.
"""

import asyncio
import json
import logging
from copy import deepcopy
from statistics import median
from typing import Any, Protocol

from .exchanges import VenueAPI
from .horizons import DURATIONS, PROFILES, assign
from .ingestion import candle_records, parse_eligible_metadata
from .models import DAY
from .native_streams import NativeStreams
from .orderflow import Book
from .recovery import failure_health, rest_batch, success_health
from .storage import now_ms
from .strategy import candidates

log = logging.getLogger(__name__)
TERMINAL = {"INVALIDATED", "EXPIRED", "RESOLVED"}


class RuntimePorts(Protocol):
    api: Any
    bounded_rest: Any
    cached_candles: Any
    cached_derivative_history: Any
    cached_features: Any
    context: Any
    depth_context: Any
    derivative_cache: Any
    derivative_cache_hits: Any
    derivative_cache_misses: Any
    discover_horizons: Any
    exchange: Any
    feature_cache: Any
    feature_cache_hits: Any
    feature_cache_misses: Any
    flow_ready: Any
    generation_profile: Any
    instrument_cache: Any
    lifecycle_bootstrapped: Any
    observe_duplicate: Any
    observe_generation: Any
    pending_symbols: Any
    record_membership: Any
    record_quotes: Any
    recorder: Any
    remember_structures: Any
    rest_queue_depth: Any
    rest_reduced_until: Any
    save_candidates: Any
    scan_lock: Any
    scan_once: Any
    select_source: Any
    settings: Any
    source_ready: Any
    spread_history: Any
    status: Any
    store: Any
    streams: Any
    volume_research: Any


class MarketDiscoveryCoordinator:
    def __init__(self, runtime: RuntimePorts):
        self.runtime = runtime

    def generation_profile(self, instrument, bars, now):
        runtime = self.runtime
        cache = "swing_profiles" if bars and bars[-1].interval >= 14400000 else "volume_profiles"
        profile = getattr(runtime, cache, {}).get(instrument.symbol, {})
        if profile.get("source") == runtime.exchange and 0 <= now - profile.get("available_ms", 0) <= 120000:
            return profile
        return {"available": False, "reason": "No fresh complete cached executed-volume profile"}

    def observe_generation(self, signal, instrument, bars, now, initial_book=None):
        runtime = self.runtime
        from .research_v212 import capture_initial, relative_volume, remember_volume

        checked = now_ms()
        book = initial_book or runtime.streams.books.get(instrument.symbol)
        derivatives = runtime.context.get(instrument.symbol, {}).get("derivatives", {})
        from .quality_v22 import prepare_generation

        prepare_generation(
            signal, bars, checked, runtime.settings, instrument, book, derivatives.get("funding_rate")
        )
        capture_initial(signal, book, runtime.settings, checked, instrument, derivatives.get("funding_rate"))
        if bars:
            remember_volume(
                runtime.store, instrument.exchange, instrument.symbol, bars[-1].interval, bars, now
            )
            signal.evidence["relative_volume_research"] = relative_volume(
                runtime.store, instrument.exchange, instrument.symbol, bars[-1], now
            )

    def observe_duplicate(self, signal, reason):
        runtime = self.runtime
        from .research_v212 import suppression

        structure = signal.evidence.get("frozen_structure", {})
        row = next(
            (
                r
                for r in runtime.store.active_signals()
                if all(
                    r.get(k) == getattr(signal, k)
                    for k in ("source", "symbol", "horizon_profile", "direction", "family")
                )
            ),
            None,
        )
        if row:
            suppression(
                runtime.store,
                row.get("evidence", {}).get("frozen_structure", {}),
                structure,
                source=signal.source,
                symbol=signal.symbol,
                horizon=signal.horizon_profile,
                family=signal.family,
                direction=signal.direction,
                now=now_ms(),
                reason=reason,
            )

    def save_candidates(
        self, instrument, context_bars, setup_bars, execution_bars, evaluated, initial_book=None
    ):
        runtime = self.runtime
        if not runtime.recorder.healthy:
            return []
        if not runtime.flow_ready(instrument.symbol, evaluated):
            return []
        observations = []
        plans = candidates(
            instrument,
            context_bars,
            setup_bars,
            execution_bars,
            evaluated,
            execution_window_ms=runtime.settings.execution_window_seconds * 1000,
            structural_targets=True,
            volume_profile=runtime.generation_profile(instrument, setup_bars, evaluated),
            horizon="CORE_INTRADAY",
            quality_policy="trade-validity-v22",
            structural_observations=observations,
        )
        runtime.remember_structures(instrument, "CORE_INTRADAY", observations, evaluated)
        for signal in plans:
            assign(signal, "CORE_INTRADAY")
            runtime.observe_generation(signal, instrument, setup_bars, evaluated, initial_book)
            from .quality import overlaps_existing

            if overlaps_existing(signal, runtime.store.active_signals()):
                runtime.observe_duplicate(signal, "existing active event")
                continue
            from .v2 import overlaps_legacy

            if overlaps_legacy(
                signal, runtime.store.active_signals(), runtime.store.get("bybitflow_v2_cutover")
            ):
                continue
            old = runtime.store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()
            if old:
                from .research_v212 import suppression

                suppression(
                    runtime.store,
                    json.loads(old[0]).get("evidence", {}).get("frozen_structure", {}),
                    signal.evidence.get("frozen_structure", {}),
                    source=signal.source,
                    symbol=signal.symbol,
                    horizon=signal.horizon_profile,
                    family=signal.family,
                    direction=signal.direction,
                    now=evaluated,
                    reason="existing candidate identity",
                )
            if not old and signal.expires_ms > evaluated:
                from .quality import remember_attempt

                remember_attempt(runtime.store, signal)
                runtime.store.signal(signal)
                for reason in signal.gates:
                    from .funnel import reject

                    reject(runtime.store, signal, reason, signal.created_ms)
        return plans

    async def discover_horizons(self, instrument, now):
        runtime = self.runtime
        """All profiles reuse one venue stream and a shared closed-candle cache."""
        if not runtime.flow_ready(instrument.symbol, now):
            return
        for name in runtime.settings.horizon_profiles:
            if name == "CORE_INTRADAY":
                continue
            profile = PROFILES[name]
            bars = [
                await runtime.cached_candles(instrument.symbol, tf, now, 120)
                for tf in (profile.context, profile.setup, profile.execution)
            ]
            await runtime.volume_research(instrument, profile.setup, bars[1], now)
            observations = []
            plans = candidates(
                instrument,
                *bars,
                now,
                execution_window_ms=runtime.settings.execution_window_seconds * 1000,
                structural_targets=True,
                volume_profile=runtime.generation_profile(instrument, bars[1], now),
                horizon=name,
                quality_policy="trade-validity-v22",
                structural_observations=observations,
            )
            runtime.remember_structures(instrument, name, observations, now)
            for signal in plans:
                assign(signal, name)
                runtime.observe_generation(signal, instrument, bars[1], now)
                from .quality import overlaps_existing

                if overlaps_existing(signal, runtime.store.active_signals()):
                    runtime.observe_duplicate(signal, "existing active event")
                    continue
                from .v2 import overlaps_legacy

                if overlaps_legacy(
                    signal, runtime.store.active_signals(), runtime.store.get("bybitflow_v2_cutover")
                ):
                    continue
                signal.expires_ms = bars[1][-1].end + DURATIONS[profile.setup]
                from .production import confirmation_entry_window

                timing = confirmation_entry_window(signal, now)
                signal.trigger_expires_ms = timing["trigger_expires_ms"]
                signal.evidence["confirmation_entry_window"] = timing
                old = runtime.store.db.execute(
                    "SELECT payload FROM signals WHERE id=?", (signal.id,)
                ).fetchone()
                if old:
                    from .research_v212 import suppression

                    suppression(
                        runtime.store,
                        json.loads(old[0]).get("evidence", {}).get("frozen_structure", {}),
                        signal.evidence.get("frozen_structure", {}),
                        source=signal.source,
                        symbol=signal.symbol,
                        horizon=name,
                        family=signal.family,
                        direction=signal.direction,
                        now=now,
                        reason="existing candidate identity",
                    )
                if not old:
                    from .quality import remember_attempt

                    remember_attempt(runtime.store, signal)
                    runtime.store.signal(signal)
                    for reason in signal.gates:
                        from .funnel import reject

                        reject(runtime.store, signal, reason, signal.created_ms)

    def remember_structures(self, instrument, horizon, observations, now):
        runtime = self.runtime
        """Compact bounded event observations, including pending states; no new feed."""
        rows = runtime.store.get("structural-observations-v211", [])
        keys = (
            "structure_event_id",
            "event_start_ms",
            "reference_high",
            "reference_low",
            "range_state",
            "breakout_state",
            "accepted",
            "reclaimed",
            "reclaim_delay_bars",
            "retest_delay_bars",
            "retest_held",
            "research_state",
            "candidate_event",
            "reasons",
        )
        for observation in observations:
            if observation.get("diagnostic_suppression"):
                from .research_v212 import suppression

                suppression(
                    runtime.store,
                    observation["existing"],
                    observation["observed"],
                    source=instrument.exchange,
                    symbol=instrument.symbol,
                    horizon=horizon,
                    family=observation["family"],
                    direction=observation["direction"],
                    now=now,
                    reason="earlier anchor represents production event",
                )
                continue
            value = {k: observation.get(k) for k in keys}
            value.update(
                source=instrument.exchange,
                symbol=instrument.symbol,
                horizon=horizon,
                family=observation["family"],
                direction=observation["direction"],
                at_ms=now,
            )
            identity = (value["source"], value["symbol"], horizon, value["structure_event_id"])
            previous = next(
                (
                    x
                    for x in reversed(rows)
                    if (x["source"], x["symbol"], x["horizon"], x["structure_event_id"]) == identity
                ),
                None,
            )
            if previous is None or any(previous.get(k) != value.get(k) for k in keys):
                rows.append(value)
        if observations:
            runtime.store.put("structural-observations-v211", rows[-400:])

    def record_membership(self, evaluated_ms, symbol, eligible, payload):
        runtime = self.runtime
        available = now_ms()
        payload = payload | {
            "evaluated_ms": evaluated_ms,
            "available_ms": available,
            "exchange": runtime.exchange,
        }
        runtime.store.membership(available, symbol, eligible, payload)
        runtime.recorder.offer(
            "universe/membership",
            symbol,
            available,
            {"exchange": runtime.exchange, "eligible": eligible, "details": payload},
            available,
        )

    async def depth_context(self, inst, ticker, ticker_time, evaluated):
        runtime = self.runtime
        t = ticker
        (oi, oi_observed), (funding, funding_observed) = await rest_batch(
            runtime.cached_derivative_history(
                "open-interest", inst.symbol, evaluated - 4 * 3_600_000, evaluated, 180_000
            ),
            runtime.cached_derivative_history(
                "funding/history", inst.symbol, evaluated - 2 * DAY, evaluated, 900_000
            ),
        )
        if runtime.exchange == "okx":
            current_funding = await runtime.api.funding_now(inst.symbol)
            t = t | {
                "fundingRate": current_funding.get("fundingRate"),
                "nextFundingTime": current_funding.get("nextFundingTime"),
                "observed_ms": int(current_funding["ts"]),
            }
        oi = sorted(oi, key=lambda x: int(x["timestamp"]))
        f = sorted(funding, key=lambda x: int(x["fundingRateTimestamp"]))
        # Current predicted funding is context only, never injected into historical decisions.
        derivatives = {
            "exchange": runtime.exchange,
            "funding_rate": float(t["fundingRate"]) if t.get("fundingRate") else None,
            "next_funding_ms": int(t.get("nextFundingTime") or 0),
            "funding_interval_minutes": inst.funding_interval_minutes,
            "funding_history": f,
            "oi_history": oi,
            "oi_observed_ms": oi_observed,
            "funding_history_observed_ms": funding_observed,
            "oi_change_pct": (float(oi[-1]["openInterest"]) / float(oi[0]["openInterest"]) - 1) * 100
            if len(oi) >= 2 and float(oi[0]["openInterest"])
            else None,
            "oi_unit": "base coin, API openInterest field; see audited definition",
            "oi_notional": float(t["openInterestValue"])
            if t.get("openInterestValue")
            else (float(oi[-1]["notional"]) if oi and oi[-1].get("notional") else None),
            "mark": float(t["markPrice"]) if t.get("markPrice") else None,
            "index": float(t["indexPrice"]) if t.get("indexPrice") else None,
            "ticker_observed_ms": int(t.get("funding_observed_ms", t.get("observed_ms", ticker_time))),
            "collected_ms": now_ms(),
        }
        return derivatives

    async def scan_once(self):
        runtime = self.runtime
        from .discovery import depth_shortlist, horizon_pre_ranks

        async with runtime.scan_lock:
            await runtime.select_source()
            scan_started = now_ms()
            for key in ("error_type", "reason", "live_feed_preserved"):
                runtime.status.pop(key, None)
            runtime.status.update(state="scanning", started_ms=scan_started, at_ms=scan_started)
            runtime.store.put("scanner", runtime.status)
            clock_started = now_ms()
            body = await runtime.api.get("time")
            asof = int(body["time"])
            clock_received = now_ms()
            runtime.store.put(
                "clock_health",
                dict(
                    source=runtime.exchange,
                    at_ms=clock_received,
                    skew_ms=(clock_started + clock_received) // 2 - asof,
                    round_trip_ms=clock_received - clock_started,
                    receipt_offset_ms=clock_received - asof,
                    method="request midpoint minus exchange time; warning only",
                ),
            )
            if abs(asof - clock_received) > 2000:
                raise ValueError("Local/exchange clock skew exceeds two seconds")
            metadata_cache = getattr(runtime, "instrument_cache", None)
            if metadata_cache and 0 <= asof - metadata_cache[0] < 1_800_000:
                rows = deepcopy(metadata_cache[1])
            else:
                rows = await runtime.bounded_rest(runtime.api.instruments())
                runtime.instrument_cache = (asof, deepcopy(rows))
            pinned = runtime.pending_symbols()
            priority = list(dict.fromkeys(pinned + runtime.settings.core_watchlist))
            rows.sort(
                key=lambda r: (
                    priority.index(r["symbol"]) if r["symbol"] in priority else len(priority),
                    r["symbol"],
                )
            )
            ticker_body = await runtime.api.get("tickers", category="linear")
            runtime.record_quotes(ticker_body, now_ms())
            tickers = {t["symbol"]: t for t in ticker_body["result"]["list"]}
            ranked, broad, errors = [], {}, 0
            from .diagnostics import record_failure

            scan_id = f"market-broad:{scan_started}"
            verified = []
            requests = 0

            async def scan_candles(symbol, timeframe, checked, count):
                try:
                    return await runtime.cached_candles(symbol, timeframe, checked, count)
                except Exception as exc:
                    exc.scan_operation = "completed candle history: " + timeframe
                    exc.scan_range_start_ms = checked - count * DURATIONS[timeframe]
                    raise

            async def admit(symbol, *, select_stream=True):
                nonlocal requests, errors
                if symbol in verified:
                    return
                operation = "orderbook admission"
                try:
                    inst, context_bars, setup_bars, execution_bars = broad[symbol]
                    requests += 1
                    data = await runtime.bounded_rest(
                        runtime.api.get("orderbook", category="linear", symbol=symbol, limit=50)
                    )
                    book = Book()
                    book.apply(
                        dict(
                            type="snapshot",
                            ts=data["time"],
                            cts=data["result"].get("cts", data["time"]),
                            data=data["result"],
                        ),
                        now_ms(),
                    )
                    if not book.fresh(now_ms(), runtime.settings.book_stale_ms) or not all(
                        book.impact(side, runtime.settings.hypothetical_notional)
                        for side in ("LONG", "SHORT")
                    ):
                        return
                    evaluated = now_ms()
                    operation = "open interest/funding context"
                    derivatives = await runtime.depth_context(
                        inst, tickers[symbol], ticker_body["time"], evaluated
                    )
                    runtime.context[symbol] = dict(
                        instrument=inst,
                        h4=context_bars,
                        h1=setup_bars,
                        m15=execution_bars,
                        derivatives=derivatives,
                        asof=evaluated,
                    )
                    normal = runtime.spread_history(symbol).assess(
                        evaluated, runtime.settings.max_spread_bps, runtime.settings.spread_min_samples
                    )
                    runtime.record_membership(
                        evaluated,
                        symbol,
                        normal["eligible"],
                        dict(depth_verified=True, normal_spread=normal, metadata=inst.metadata),
                    )
                    await runtime.volume_research(inst, "60", setup_bars, evaluated)
                    operation = "candidate generation / horizon discovery"
                    runtime.save_candidates(inst, context_bars, setup_bars, execution_bars, evaluated, book)
                    await runtime.discover_horizons(inst, evaluated)
                    runtime.store.put(
                        "market:" + symbol,
                        dict(
                            exchange=runtime.exchange,
                            instrument=inst.model_dump(mode="json"),
                            candles=candle_records(setup_bars),
                            h4=runtime.cached_features(symbol, "240", context_bars, evaluated),
                            h1=runtime.cached_features(symbol, "60", setup_bars, evaluated),
                            derivatives=derivatives,
                            normal_spread=normal,
                            asof=evaluated,
                        ),
                    )
                    verified.append(symbol)
                    if select_stream and symbol in priority:
                        await runtime.streams.select([symbol], retain_selected=True)
                except Exception as exc:
                    if not runtime.recorder.healthy:
                        raise RuntimeError("Recording unavailable; depth verification aborted") from exc
                    errors += 1
                    record_failure(
                        runtime.store,
                        exc,
                        scan_id=scan_id,
                        phase="admission",
                        operation=getattr(exc, "scan_operation", operation),
                        source=runtime.exchange,
                        symbol=symbol,
                        range_start_ms=getattr(exc, "scan_range_start_ms", asof - 120 * DAY),
                        range_end_ms=now_ms(),
                    )
                    if isinstance(exc, PermissionError):
                        raise

            # Restore existing opportunities before broad eligibility ranking. Admission
            # verifies depth; it never changes their original plan or lifecycle state.
            for raw in rows:
                if raw["symbol"] not in pinned:
                    continue
                try:
                    inst = (
                        runtime.api.parse(raw, asof)
                        if isinstance(runtime.api, VenueAPI)
                        else parse_eligible_metadata(raw, runtime.settings, asof)
                    )
                    if inst is None:
                        continue
                    now = now_ms()
                    bars = await rest_batch(
                        *(
                            scan_candles(inst.symbol, tf, now, count)
                            for tf, count in (("240", 160), ("60", 200), ("15", 120))
                        )
                    )
                    broad[inst.symbol] = (inst, *bars)
                    await admit(inst.symbol)
                except Exception as exc:
                    errors += 1
                    record_failure(
                        runtime.store,
                        exc,
                        scan_id=scan_id,
                        phase="active restoration",
                        operation=getattr(exc, "scan_operation", "active market candle history"),
                        source=runtime.exchange,
                        symbol=raw.get("symbol"),
                        range_start_ms=getattr(exc, "scan_range_start_ms", asof - 120 * DAY),
                        range_end_ms=now_ms(),
                    )

            async def broad_scan(raw):
                nonlocal errors
                operation = "instrument eligibility metadata"
                try:
                    inst = (
                        runtime.api.parse(raw, asof)
                        if isinstance(runtime.api, VenueAPI)
                        else parse_eligible_metadata(raw, runtime.settings, asof)
                    )
                    if not inst:
                        return
                    operation = "daily turnover qualification"
                    t = tickers.get(inst.symbol, {})
                    bid, ask = float(t.get("bid1Price") or 0), float(t.get("ask1Price") or 0)
                    spread = (ask - bid) / ((ask + bid) / 2) * 10000 if ask > bid > 0 else float("inf")
                    if spread > runtime.settings.max_spread_bps:
                        return
                    now = now_ms()
                    daily = await scan_candles(inst.symbol, "D", now, 120)
                    if (
                        len(daily) < 30
                        or now - daily[-1].end >= DAY
                        or any(c.volume <= 0 for c in daily[-30:])
                    ):
                        return
                    turnover = median(c.turnover for c in daily[-7:])
                    if turnover < runtime.settings.min_daily_turnover:
                        return
                    context_bars, setup_bars, execution_bars = await rest_batch(
                        scan_candles(inst.symbol, "240", now, 160),
                        scan_candles(inst.symbol, "60", now, 200),
                        scan_candles(inst.symbol, "15", now, 120),
                    )
                    if now - execution_bars[-1].end > 960_000 or now - setup_bars[-1].end > 3_660_000:
                        return
                    operation = "horizon preliminary ranking"
                    pre = horizon_pre_ranks(
                        daily,
                        context_bars,
                        setup_bars,
                        execution_bars,
                        now,
                        spread,
                        runtime.settings.max_spread_bps,
                        t.get("openInterestValue") is not None,
                    )
                    normal = runtime.spread_history(inst.symbol).assess(
                        now, runtime.settings.max_spread_bps, runtime.settings.spread_min_samples
                    )
                    ranked.append(
                        dict(
                            symbol=inst.symbol,
                            exchange=runtime.exchange,
                            eligible=True,
                            normal_spread=normal,
                            turnover_7d=turnover,
                            spread_bps=spread,
                            asof=now,
                            **pre,
                        )
                    )
                    broad[inst.symbol] = (inst, context_bars, setup_bars, execution_bars)
                except Exception as exc:
                    if not runtime.recorder.healthy:
                        raise RuntimeError("Recording unavailable; broad scan aborted") from exc
                    errors += 1
                    record_failure(
                        runtime.store,
                        exc,
                        scan_id=scan_id,
                        phase="broad selection",
                        operation=getattr(exc, "scan_operation", operation),
                        source=runtime.exchange,
                        symbol=raw.get("symbol"),
                        range_start_ms=getattr(exc, "scan_range_start_ms", asof - 120 * DAY),
                        range_end_ms=now_ms(),
                    )
                    if isinstance(exc, PermissionError):
                        raise

            # Small batches keep admission and stream work ahead of optional discovery.
            for offset in range(0, len(rows), runtime.settings.rest_concurrency):
                await rest_batch(
                    *(broad_scan(raw) for raw in rows[offset : offset + runtime.settings.rest_concurrency])
                )
            ranked.sort(key=lambda r: r["best_pre_rank"], reverse=True)
            available = {r["symbol"] for r in ranked}
            core = [x for x in runtime.settings.core_watchlist if x in available]
            capacity = runtime.settings.deep_symbols
            if (
                getattr(runtime.recorder, "state", "NORMAL") != "NORMAL"
                or runtime.store.get("storage_maintenance", {}).get("state") == "STORAGE_BACKPRESSURE"
            ):
                capacity = max(len(pinned), len(core), capacity // 2)
            shortlist, selection = depth_shortlist(
                ranked,
                core,
                [x for x in pinned if x in available],
                capacity,
                runtime.settings.stage_a_depth_candidates,
                now_ms() // 900_000,
                runtime.settings.exploration_fraction,
            )
            admit_started = now_ms()
            admission_limit = max(capacity, len([x for x in priority if x in available]))
            for offset in range(0, len(shortlist), runtime.settings.rest_concurrency):
                if len(verified) >= admission_limit:
                    break
                batch = shortlist[
                    offset : offset + min(runtime.settings.rest_concurrency, admission_limit - len(verified))
                ]
                await rest_batch(*(admit(symbol, select_stream=False) for symbol in batch))
            deep_admit_duration_ms = now_ms() - admit_started
            retained = [x for x in pinned if x in runtime.streams.selected]
            selected = list(dict.fromkeys(retained + [x for x in shortlist if x in verified]))[
                : max(capacity, len(retained))
            ]
            for symbol in selected:
                selection.setdefault(
                    symbol,
                    dict(
                        selection_reason="ACTIVE"
                        if symbol in pinned
                        else "CORE"
                        if symbol in core
                        else "EXPLOIT",
                        selection_probability=1.0,
                        preliminary_rank=None,
                    ),
                )
            selection = {k: v for k, v in selection.items() if k in selected}
            runtime.store.put("deep_selection", selection)
            runtime.store.put("watchlist", ranked)
            try:
                from .breadth import assess as assess_breadth

                breadth_rows = [
                    dict(
                        symbol=symbol,
                        source=runtime.exchange,
                        eligible=True,
                        h4=bars[1],
                        h1=bars[2],
                        m15=bars[3],
                        h1_features=runtime.cached_features(symbol, "60", bars[2], now_ms()),
                        h4_features=runtime.cached_features(symbol, "240", bars[1], now_ms()),
                    )
                    for symbol, bars in broad.items()
                    if symbol in available
                ]
                from .breadth import retain_history

                history_key = "v8_breadth_history:" + runtime.exchange
                history = runtime.store.get(history_key, [])
                assessed_ms = now_ms()
                current_breadth = assess_breadth(breadth_rows, assessed_ms, runtime.exchange, history)
                runtime.store.put("v8_breadth", current_breadth)
                runtime.store.put(history_key, retain_history(history, current_breadth, assessed_ms))
            except Exception as exc:
                runtime.store.put(
                    "v8_breadth",
                    dict(state="INSUFFICIENT", error_type=type(exc).__name__, available_ms=now_ms()),
                )
            await runtime.streams.select(selected)
            distribution = {
                h: sum(r["best_pre_rank_horizon"] == h for r in ranked)
                for h in ("SHORT_INTRADAY", "CORE_INTRADAY", "SWING")
            }
            runtime.status = dict(
                state="collecting",
                at_ms=now_ms(),
                discovered=len(rows),
                preeligible=len(ranked),
                eligible=sum(r["normal_spread"]["eligible"] for r in ranked),
                provisional=sum(not r["normal_spread"]["eligible"] for r in ranked),
                depth_verified=len(verified),
                deep_selected=len(selected),
                retained_active=len(retained),
                depth_requests=requests,
                errors=errors,
                deep_symbols=selected,
                exchange=runtime.exchange,
                best_horizon_distribution=distribution,
                selection_counts={
                    kind: sum(v["selection_reason"] == kind for v in selection.values())
                    for kind in ("ACTIVE", "CORE", "EXPLOIT", "EXPLORE")
                },
            )
            from .diagnostics import failure_summary

            causes = failure_summary(runtime.store, scan_id)
            causes = dict(causes, scan_id=scan_id, at_ms=now_ms())
            causes["without_durable_causes"] = max(0, errors - causes["with_durable_causes"])
            runtime.status["broad_scan_causes"] = causes
            runtime.store.put("broad_scan_causes", causes)
            runtime.store.put("scanner", runtime.status)
            candle_total = runtime.feature_cache_hits + runtime.feature_cache_misses
            derivative_total = runtime.derivative_cache_hits + runtime.derivative_cache_misses
            runtime.store.put(
                "scanner_performance",
                dict(
                    at_ms=now_ms(),
                    candle_cache_hit_rate=runtime.feature_cache_hits / candle_total if candle_total else None,
                    candle_feature_cache_entries=len(runtime.feature_cache),
                    derivative_cache_hit_rate=runtime.derivative_cache_hits / derivative_total
                    if derivative_total
                    else None,
                    derivative_cache_entries=len(runtime.derivative_cache),
                    rest_concurrency=runtime.settings.rest_concurrency,
                    rest_effective_concurrency=1
                    if now_ms() < runtime.rest_reduced_until
                    else runtime.settings.rest_concurrency,
                    rest_queue_depth=runtime.rest_queue_depth,
                    scan_duration_ms=now_ms() - scan_started,
                    deep_admit_duration_ms=deep_admit_duration_ms,
                ),
            )
            from .funnel import emit

            for field, metric in [
                ("discovered", "broad_markets_discovered"),
                ("preeligible", "preeligible_markets"),
                ("depth_verified", "depth_verified_markets"),
                ("deep_selected", "deep_selected_markets"),
            ]:
                emit(
                    runtime.store,
                    metric,
                    runtime.status["at_ms"],
                    amount=runtime.status[field],
                    key=f"scan:{runtime.status['at_ms']}:{metric}",
                )
            return runtime.status

    async def scan_loop(self):
        runtime = self.runtime
        await runtime.lifecycle_bootstrapped.wait()
        while True:
            delay = runtime.settings.scan_seconds
            try:
                # A cold history load can take several minutes. Bound an
                # actually stuck cycle so its lock is released and supervision
                # can retry without changing any existing setup plan.
                async with asyncio.timeout(max(900, runtime.settings.scan_seconds * 3)):
                    await runtime.scan_once()
                runtime.store.put("scan_health", success_health(now_ms()))
            except Exception as exc:
                delay = min(delay, 60)
                health = failure_health(runtime.store.get("scan_health", {}), exc, now_ms(), delay)
                runtime.store.put("scan_health", health)
                runtime.status = dict(state="error", error_type=type(exc).__name__, at_ms=now_ms())
                runtime.status.update(
                    error_category=health["error_category"], retry_at_ms=health["retry_at_ms"]
                )
                if isinstance(exc, ValueError):
                    runtime.status["reason"] = (
                        "Local/exchange clock skew exceeds two seconds"
                        if str(exc) == "Local/exchange clock skew exceeds two seconds"
                        else "Public market data validation failed"
                    )
                runtime.store.put("scanner", runtime.status)
                # REST outages must not erase independent, healthy live history.
                feed_available = (
                    any(runtime.streams.connected_for(s) for s in runtime.streams.selected)
                    if isinstance(runtime.streams, NativeStreams)
                    else getattr(runtime.streams, "connected", False)
                )
                preserve_feed = feed_available and runtime.source_ready and runtime.recorder.healthy
                runtime.streams.required_symbols = set(runtime.pending_symbols())
                await runtime.streams.select(runtime.pending_symbols(), retain_selected=True)
                runtime.status["live_feed_preserved"] = preserve_feed
                runtime.store.put("scanner", runtime.status)
                log.warning(
                    "scan_failed", extra={k: health[k] for k in ("error_type", "error_category", "location")}
                )
                # A transient outage should not suspend collection for a full scan interval.
                delay = min(delay, 60)
            await asyncio.sleep(delay)
