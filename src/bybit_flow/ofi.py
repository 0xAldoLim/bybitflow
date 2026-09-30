"""Observed queue-change OFI; separate from executed-trade delta and attribution."""

from statistics import mean

POLICY = "ofi-v2"


def l1_event(before_bid, before_ask, after_bid, after_ask):
    """Cont-style touch change from prices and visible sizes, not inferred cancels."""
    if not all((before_bid, before_ask, after_bid, after_ask)):
        return 0.0
    bp, bq = before_bid
    ap, aq = before_ask
    nbp, nbq = after_bid
    nap, naq = after_ask
    bid = nbq if nbp > bp else nbq - bq if nbp == bp else -bq
    ask = -naq if nap < ap else aq - naq if nap == ap else aq
    return float(bid + ask)


def assess(book, asof_ms, source, *, min_events=5):
    unavailable = dict(
        policy=POLICY,
        ofi_available=False,
        source=source,
        source_ms=getattr(book, "event_ms", None),
        available_ms=asof_ms,
        reason="continuous fresh book history required",
    )
    if book is None or not book.fresh(asof_ms) or not hasattr(book, "ofi_events"):
        return unavailable
    events = [row for row in book.ofi_events if asof_ms - 60_000 <= row[0] <= asof_ms]
    if len(events) < min_events or events[-1][0] - events[0][0] < 2_000:
        return unavailable | dict(observed_events=len(events))
    bid, ask = max(book.bids), min(book.asks)
    mid = (bid + ask) / 2
    touch_depth = float(book.bids[bid] + book.asks[ask])
    l1 = sum(row[1] for row in events)
    bands = {}
    for bps in (5, 10):
        width = bps / 10_000
        observed = sum(
            float(price * change) * (1 if side == "bid" else -1)
            for receipt, side, price, change in book.changes
            if asof_ms - 60_000 <= receipt <= asof_ms and abs(float(price / mid) - 1) <= width
        )
        depth = sum(
            float(price * size)
            for side in (book.bids, book.asks)
            for price, size in side.items()
            if abs(float(price / mid) - 1) <= width
        )
        bands[bps] = (observed, observed / depth if depth else None)
    half = asof_ms - 30_000
    first = [row[1] for row in events if row[0] < half]
    second = [row[1] for row in events if row[0] >= half]
    signs = [1 if row[1] > 0 else -1 if row[1] < 0 else 0 for row in events]
    normalized = l1 / touch_depth if touch_depth else None
    price_change = events[-1][3] - events[0][2]
    micro_change = events[-1][5] - events[0][4]
    return unavailable | dict(
        ofi_available=True,
        reason="continuous observed book updates",
        source_ms=book.event_ms,
        ofi_l1=l1,
        ofi_5bps=bands[5][0],
        ofi_10bps=bands[10][0],
        ofi_normalized_l1=normalized,
        ofi_normalized_5bps=bands[5][1],
        ofi_normalized_10bps=bands[10][1],
        ofi_persistence=abs(sum(signs)) / len(signs),
        ofi_acceleration=mean(second) - mean(first) if first and second else None,
        price_change_per_ofi=price_change / normalized if normalized and abs(normalized) > 1e-9 else None,
        microprice_response_per_ofi=micro_change / normalized
        if normalized and abs(normalized) > 1e-9
        else None,
        observed_events=len(events),
        attribution="observed queue additions/removals; reductions are not classified as cancellations",
    )
