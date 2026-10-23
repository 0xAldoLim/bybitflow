"""Second-bucket observed queue changes; executed-trade delta stays separate."""

from statistics import mean

POLICY = "ofi-v3"


def l1_event(before_bid, before_ask, after_bid, after_ask):
    if not all((before_bid, before_ask, after_bid, after_ask)):
        return 0.0
    bp, bq = before_bid
    ap, aq = before_ask
    nbp, nbq = after_bid
    nap, naq = after_ask
    bid = nbq if nbp > bp else nbq - bq if nbp == bp else -bq
    ask = -naq if nap < ap else aq - naq if nap == ap else aq
    return float(bid + ask)


def update_bucket(book, receipt, l1, old_mid, mid, old_micro, micro, changes):
    """Bounded changed levels per event; depth is sampled only once per second."""
    second = receipt // 1000 * 1000
    # Separate new-policy continuity from the historical ofi-v3 interpretation.
    if not book.event_ofi_start_ms or not 0 <= receipt - book.event_ofi_last_ms <= 5000:
        book.event_ofi_epoch += 1
        book.event_ofi_start_ms = receipt
    book.event_ofi_last_ms = receipt
    if book.ofi_buckets and second < book.ofi_buckets[-1]["second_ms"]:
        book.ofi_buckets.clear()
        book.ofi_epoch += 1
    if not book.ofi_buckets or second != book.ofi_buckets[-1]["second_ms"]:
        bid, ask = max(book.bids), min(book.asks)
        depth = {
            bps: sum(
                float(p * q)
                for levels in (book.bids, book.asks)
                for p, q in levels.items()
                if abs(float(p) / mid - 1) <= bps / 10_000
            )
            for bps in (5, 10, 25)
        }
        book.ofi_buckets.append(
            dict(
                second_ms=second,
                source_ms=book.event_ms,
                epoch=book.ofi_epoch,
                ofi_l1_sum=0.0,
                ofi_5bps_notional_sum=0.0,
                ofi_10bps_notional_sum=0.0,
                ofi_25bps_notional_sum=0.0,
                event_count=0,
                mid_open=old_mid,
                mid_close=mid,
                microprice_open=old_micro,
                microprice_close=micro,
                touch_depth_mean=float(book.bids[bid] + book.asks[ask]),
                depth_5bps_mean=depth[5],
                depth_10bps_mean=depth[10],
                depth_25bps_mean=depth[25],
                event_epoch=book.event_ofi_epoch,
                continuity_start_ms=book.event_ofi_start_ms,
                last_receipt_ms=receipt,
            )
        )
    row = book.ofi_buckets[-1]
    if row.get("event_epoch") != book.event_ofi_epoch:
        row["event_epoch"] = None  # Mixed bucket excluded only by the v22 reader.
    row["last_receipt_ms"] = receipt
    row["source_ms"] = max(row.get("source_ms", 0), book.event_ms)
    row["ofi_l1_sum"] += l1
    row["event_count"] += 1
    row["mid_close"], row["microprice_close"] = mid, micro
    for bps in (5, 10, 25):
        row[f"ofi_{bps}bps_notional_sum"] += sum(
            float(p * q) * (1 if side == "bid" else -1)
            for side, p, q in changes
            if abs(float(p) / mid - 1) <= bps / 10_000
        )


def assess(book, asof_ms, source, *, baseline=None, min_events=5):
    base = dict(
        policy=POLICY,
        ofi_available=False,
        production_coverage_ready=False,
        source=source,
        source_ms=getattr(book, "event_ms", None),
        available_ms=asof_ms,
        reason="45 continuous observed seconds and fresh book required",
        ofi_coverage_seconds=0,
        ofi_event_count=0,
        ofi_continuity_epoch=getattr(book, "ofi_epoch", None),
        baseline_samples=len(baseline or ()),
        ofi_strength_percentile=None,
    )
    if book is None or not book.fresh(asof_ms):
        return base
    rows = [
        r
        for r in book.ofi_buckets
        if asof_ms - 60_000 < r["second_ms"] <= asof_ms and r.get("source_ms", asof_ms + 1) <= asof_ms
    ]
    base["ofi_coverage_seconds"] = len(rows)
    base["ofi_event_count"] = sum(r["event_count"] for r in rows)
    if not rows:
        return base
    if any(r["epoch"] != book.ofi_epoch for r in rows):
        return base | dict(epoch_contradiction=True)
    ready = len(rows) >= 45 and asof_ms - rows[-1]["second_ms"] <= 5000
    l1 = sum(r["ofi_l1_sum"] for r in rows)
    norm = (
        l1 / mean(r["touch_depth_mean"] for r in rows)
        if all(r["touch_depth_mean"] > 0 for r in rows)
        else None
    )
    result = base | dict(
        ofi_available=ready,
        production_coverage_ready=ready,
        reason="observed second buckets" if ready else base["reason"],
        source_ms=rows[-1]["source_ms"],
        ofi_l1_60s=l1,
        ofi_normalized_l1=norm,
    )
    for bps in (5, 10):
        amount = sum(r[f"ofi_{bps}bps_notional_sum"] for r in rows)
        depth = mean(r[f"depth_{bps}bps_mean"] for r in rows)
        result[f"ofi_{bps}bps_60s"] = amount
        result[f"ofi_normalized_{bps}bps"] = amount / depth if depth > 0 else None
    signs = [1 if r["ofi_l1_sum"] > 0 else -1 if r["ofi_l1_sum"] < 0 else 0 for r in rows]
    first = [r["ofi_l1_sum"] for r in rows if r["second_ms"] < asof_ms - 30_000]
    second = [r["ofi_l1_sum"] for r in rows if r["second_ms"] >= asof_ms - 30_000]
    result.update(
        ofi_persistence=abs(sum(signs)) / len(rows),
        ofi_acceleration=mean(second) - mean(first) if first and second else None,
        mid_response_bps=(rows[-1]["mid_close"] / rows[0]["mid_open"] - 1) * 10_000,
        microprice_response_bps=(rows[-1]["microprice_close"] / rows[0]["microprice_open"] - 1) * 10_000,
    )
    prior = list(baseline or ())
    if ready and len(prior) >= 20 and norm is not None and result["ofi_normalized_5bps"] is not None:
        # Both normalized magnitudes must be locally strong; no raw universal cutoff.
        result["ofi_strength_percentile"] = min(
            sum(abs(r[key]) <= abs(result[key]) for r in prior) / len(prior)
            for key in ("ofi_normalized_l1", "ofi_normalized_5bps")
        )
    return result


def event_assess(book, asof_ms, source, symbol, direction="LONG"):
    """Touch queue-event OFI and distinct fixed-price depth-band changes.

    At most 120 second aggregates per book. Partial leading buckets and buckets
    mutated after asof are excluded. Missing quiet seconds are not fabricated.
    """
    import math

    from .setup_common import number

    result = dict(
        policy="event-ofi-v1",
        source=source,
        symbol=symbol,
        available_ms=asof_ms,
        source_ms=None,
        state="UNAVAILABLE",
        coverage_complete=False,
        windows={},
        epoch=getattr(book, "event_ofi_epoch", None),
        band_definition="Signed observed changes at fixed prices inside each current-mid band; not touch OFI",
    )
    if book is None:
        return result
    if not book.valid:
        return result | dict(state="GAPPED")
    if not book.fresh(asof_ms):
        return result | dict(state="STALE")
    if book.event_ms > asof_ms or book.receipt_ms > asof_ms:
        return result | dict(reason="FUTURE_TIMESTAMP")
    rows = [
        r
        for r in book.ofi_buckets
        if r.get("event_epoch") == book.event_ofi_epoch
        # The first post-gap delta compares with a pre-gap queue. Exclude its
        # entire aggregate, including a reset exactly on a second boundary.
        and r["second_ms"] > book.event_ofi_start_ms // 1000 * 1000
        and r.get("last_receipt_ms", asof_ms + 1) <= asof_ms
        and r.get("source_ms", asof_ms + 1) <= asof_ms
    ]
    sign = 1 if direction == "LONG" else -1
    for seconds in (1, 5, 15, 30, 60):
        end = asof_ms // 1000 * 1000
        start = end - seconds * 1000
        selected = [r for r in rows if start <= r["second_ms"] < end]
        ready = bool(
            len(selected) >= math.ceil(seconds * 0.75)
            and book.event_ofi_start_ms <= start
            and sum(r["event_count"] for r in selected) >= (5 if seconds >= 15 else 1)
            and selected
            and asof_ms - selected[-1]["last_receipt_ms"] <= min(5000, seconds * 1000)
        )
        window = dict(
            state="READY" if ready else "WARMING",
            coverage_complete=ready,
            observed_seconds=len(selected),
            start_ms=start,
            end_ms=end,
            available_ms=asof_ms,
            source=source,
            symbol=symbol,
            event_count=sum(r["event_count"] for r in selected),
        )
        for band in ("touch", "5bps", "10bps", "25bps"):
            value_key = "ofi_l1_sum" if band == "touch" else f"ofi_{band}_notional_sum"
            depth_key = "touch_depth_mean" if band == "touch" else f"depth_{band}_mean"
            raw = sum(r[value_key] for r in selected)
            depths = [r[depth_key] for r in selected if number(r.get(depth_key)) and r[depth_key] > 0]
            depth = mean(depths) if len(depths) == len(selected) and depths else None
            norm = raw / depth if depth else None
            signs = [1 if r[value_key] > 0 else -1 if r[value_key] < 0 else 0 for r in selected]
            halves = [
                [
                    r[value_key] / r[depth_key]
                    for r in selected
                    if bool(r["second_ms"] >= asof_ms - seconds * 500) == last and r.get(depth_key, 0) > 0
                ]
                for last in (False, True)
            ]
            window[band] = dict(
                raw=raw,
                units="base quantity" if band == "touch" else "quote notional",
                depth_normalized=norm,
                directional=sign * norm if norm is not None else None,
                persistence=abs(sum(signs)) / len(signs) if signs else None,
                acceleration=mean(halves[1]) - mean(halves[0]) if all(halves) else None,
                mean_observed_depth=depth,
            )
        result["windows"][str(seconds)] = window
    sixty = result["windows"]["60"]
    return result | dict(
        state=sixty["state"],
        coverage_complete=sixty["coverage_complete"],
        source_ms=rows[-1]["source_ms"] if rows else None,
        reason="Observed continuous quote events"
        if sixty["coverage_complete"]
        else "Insufficient continuous observed quote seconds",
    )
