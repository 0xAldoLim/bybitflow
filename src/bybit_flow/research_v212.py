"""Prospective execution and comparable-session observations; no new trading gate."""

from copy import deepcopy
from statistics import mean, median

from .quality_v211 import current_cost, entry_quality, number, quote_evidence

POLICY = "research-observations-v212"
DAY = 86_400_000


def enabled(signal):
    return signal.evidence.get("research_instrumentation") == POLICY


def capture_initial(signal, book, settings, now, instrument=None, funding_rate=None):
    if (
        not enabled(signal)
        or "initial_executable_quote" in signal.evidence
        or not book
        or not book.fresh(now, settings.book_stale_ms)
    ):
        return False
    copy = deepcopy(signal)
    if instrument is not None and "fee_bps_assumption" not in copy.risk:
        from .risk import evaluate_risk

        copy.risk = evaluate_risk(copy, instrument, settings, book, funding_rate=funding_rate)
    price = float(min(book.asks) if signal.direction == "LONG" else max(book.bids))
    cost = current_cost(copy, price, book)
    q = entry_quality(
        copy, price, now, settings.min_net_rr, quote=quote_evidence(copy, book, now, settings), current=cost
    )
    bid, ask = float(max(book.bids)), float(min(book.asks))
    signal.evidence["initial_executable_quote"] = dict(
        policy="initial-executable-v1",
        available_ms=now,
        direction=signal.direction,
        best_bid=bid,
        best_ask=ask,
        mid=(bid + ask) / 2,
        spread_bps=q["quote_provenance"]["spread_bps"],
        executable_price=price,
        book_event_ms=book.event_ms,
        book_receipt_ms=book.receipt_ms,
        book_epoch=book.ofi_epoch,
        quote_age_ms=q["quote_age_ms"],
        source=signal.source,
        symbol=signal.symbol,
        entry=signal.entry,
        stop=signal.stop,
        tp1=signal.tp1,
        gross_rr=q["gross_rr_now"],
        net_rr=q["cost_adjusted_rr_now"],
        cost_per_base=cost.get("cost_per_base"),
        cost_components=cost,
        zone_relation=q["zone_relation"],
        quote_state=q["quote_state"],
        assumptions="Existing risk reserves; account fills and costs unverified",
    )
    return True


def execution_change(signal, quality):
    initial = signal.evidence.get("initial_executable_quote", {})
    current = quality.get("current_execution_cost_estimate", {})
    a, b = initial.get("net_rr"), quality.get("cost_adjusted_rr_now")
    delta = b - a if number(a) and number(b) else None
    atr = (
        signal.evidence.get("stop_plan", {}).get("setup_atr")
        or signal.evidence.get("execution_features", {}).get("atr")
        or signal.evidence.get("h1", {}).get("atr")
        or signal.evidence.get("m15", {}).get("atr")
    )

    def difference(new, old):
        return new - old if number(new) and number(old) else None

    price_move = difference(quality.get("executable_price"), initial.get("executable_price"))
    cost_change = difference(current.get("cost_per_base"), initial.get("cost_per_base"))
    risk = abs(signal.entry - signal.stop)
    return dict(
        policy="execution-deterioration-v1",
        available_ms=quality["available_ms"],
        state="EXECUTION_UNAVAILABLE"
        if delta is None
        else "EXECUTION_IMPROVED"
        if delta > 0
        else "EXECUTION_DEGRADED"
        if delta < 0
        else "EXECUTION_STABLE",
        initial_executable_gross_rr=initial.get("gross_rr"),
        initial_executable_net_rr=a,
        current_executable_gross_rr=quality.get("gross_rr_now"),
        current_executable_net_rr=b,
        gross_rr_change=difference(quality.get("gross_rr_now"), initial.get("gross_rr")),
        net_rr_change=delta,
        initial_to_current_price_move_atr=price_move / atr
        if price_move is not None and number(atr) and atr > 0
        else None,
        initial_to_current_spread_change_bps=difference(
            quality.get("quote_provenance", {}).get("spread_bps"), initial.get("spread_bps")
        ),
        initial_to_current_cost_change_r=cost_change / risk if cost_change is not None and risk > 0 else None,
        elapsed_ms=quality["available_ms"] - initial["available_ms"] if initial else None,
        research_only=True,
    )


def remember_volume(store, source, symbol, interval, bars, asof):
    key = f"relative-volume-v212:{source}:{symbol}:{interval}"
    previous = store.get(key, [])
    rows = {r["start"]: r for r in previous if asof - 30 * DAY <= r["start"] and r["end"] <= asof}
    for b in bars:
        if (
            b.interval == interval
            and b.end <= asof
            and b.start >= asof - 30 * DAY
            and number(b.volume)
            and b.volume >= 0
        ):
            rows.setdefault(b.start, dict(start=b.start, end=b.end, volume=b.volume))
    buckets = {}
    for r in sorted(rows.values(), key=lambda r: r["start"]):
        bucket = 0 if interval >= DAY else r["start"] % DAY
        buckets.setdefault(bucket, []).append(r)
        buckets[bucket] = buckets[bucket][-21:]
    retained = sorted([r for group in buckets.values() for r in group], key=lambda r: r["start"])
    if retained != previous:
        store.put(key, retained)
    index = store.get("relative-volume-v212-index", [])
    if key not in index:
        index.append(key)
        for old in index[:-128]:
            with store.db:
                store.db.execute("DELETE FROM kv WHERE key=?", (old,))
        store.put("relative-volume-v212-index", index[-128:])
    return retained


def relative_volume(store, source, symbol, bar, asof):
    key = f"relative-volume-v212:{source}:{symbol}:{bar.interval}"
    bucket = 0 if bar.interval >= DAY else bar.start % DAY
    rows = [
        r
        for r in store.get(key, [])
        if asof - 30 * DAY <= r["start"] < bar.start
        and r["end"] <= bar.start
        and (bar.interval >= DAY or r["start"] % DAY == bucket)
    ][-20:]
    volumes = [r["volume"] for r in rows]
    known = bar.end <= asof and len(rows) >= 3 and mean(volumes) > 0
    return dict(
        policy="comparable-session-volume-v1",
        available_ms=asof,
        research_only=True,
        state="KNOWN" if known else "UNKNOWN",
        relative_volume=bar.volume / mean(volumes) if known else None,
        current_completed_volume=bar.volume if bar.end <= asof else None,
        reference_mean_volume=mean(volumes) if volumes else None,
        reference_median_volume=median(volumes) if volumes else None,
        reference_sample_count=len(rows),
        reference_start_ms=rows[0]["start"] if rows else None,
        reference_end_ms=rows[-1]["end"] if rows else None,
        comparison_time_bucket=bucket,
        comparison_session="UTC_DAY",
        source=source,
        symbol=symbol,
        setup_interval_ms=bar.interval,
        minimum_samples=3,
        target_samples=10,
        maximum_samples=20,
        max_calendar_days=30,
        relative_volume_percentile=sum(v <= bar.volume for v in volumes) / len(volumes) if known else None,
    )


def suppression(store, existing, observed, *, source, symbol, horizon, family, direction, now, reason):
    a, b = existing, observed
    same_ref = (
        all(
            a.get(k) == b.get(k)
            for k in ("reference_start_ms", "reference_end_ms", "reference_high", "reference_low")
        )
        if a.get("reference_start_ms") is not None and b.get("reference_start_ms") is not None
        else None
    )
    same_start = (
        a.get("event_start_ms") == b.get("event_start_ms")
        if a.get("event_start_ms") is not None and b.get("event_start_ms") is not None
        else None
    )
    same_accept = (
        all(a.get(k) == b.get(k) for k in ("acceptance_bar_1", "acceptance_bar_2"))
        if a.get("acceptance_bar_2") and b.get("acceptance_bar_2")
        else None
    )
    same_id = bool(a.get("structure_event_id") and a.get("structure_event_id") == b.get("structure_event_id"))
    classification = (
        "TRUE_DUPLICATE"
        if same_id
        else "POTENTIAL_DISTINCT_EVENT"
        if same_start is False and b.get("candidate_event") and not b.get("reasons")
        else "UNKNOWN"
    )
    row = dict(
        policy="structural-suppression-v1",
        at_ms=now,
        source=source,
        symbol=symbol,
        horizon=horizon,
        family=family,
        direction=direction,
        existing_structure_event_id=a.get("structure_event_id"),
        observed_structure_event_id=b.get("structure_event_id"),
        suppression_reason=reason,
        same_reference=same_ref,
        same_excursion=same_start if family == "range_rejection" else None,
        same_acceptance_sequence=same_accept,
        same_retest=a.get("retest_bar") == b.get("retest_bar")
        if a.get("retest_bar") and b.get("retest_bar")
        else None,
        same_direction=True,
        time_since_original_ms=now - a["event_start_ms"] if a.get("event_start_ms") is not None else None,
        bars_since_original=(b["event_start_ms"] - a["event_start_ms"]) / b["setup_interval_ms"]
        if same_start is not None and b.get("setup_interval_ms")
        else None,
        classification=classification,
        research_only=True,
    )
    rows = store.get("structural-suppressions-v212", [])
    identity = (
        source,
        symbol,
        horizon,
        family,
        direction,
        row["existing_structure_event_id"],
        row["observed_structure_event_id"],
        reason,
    )
    for old in reversed(rows):
        if (
            old["source"],
            old["symbol"],
            old["horizon"],
            old["family"],
            old["direction"],
            old["existing_structure_event_id"],
            old["observed_structure_event_id"],
            old["suppression_reason"],
        ) == identity:
            old.update(last_seen_ms=now, observations=old.get("observations", 1) + 1)
            break
    else:
        rows.append(row)
    store.put("structural-suppressions-v212", rows[-1000:])
    return row
