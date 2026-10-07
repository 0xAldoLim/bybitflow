"""Diagnostic eligibility from committed prints; never a trading or training gate."""

import json

PRIMARY_PREDECISION_MIN_MS = 0  # No warm-up duration in prints-v1; a prior valid print is required.
COVERAGE_POLICY = "committed-prints-readiness-v1"


def market_source(source):
    if source.startswith("native/"):
        _, venue, topic = source.split("/", 2)
        return venue, topic
    return "bybit", source


def update_coverage(store, rows, stale_ms):
    """Run only after segment publication, in the writer's short transaction."""
    state = store.get("primary_recorded_coverage", {})
    previous = state.get("receipt_ms", -1)
    markets = state.get("markets", {})
    for row in rows:
        at = row["receipt_ms"]
        venue, topic = market_source(row["source"])
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        if at < previous:
            markets.clear()  # Larger clock damage remains unusable, not normalized away.
        previous = at
        key = venue + ":" + row["symbol"]
        if topic == "control/subscribed" and row.get("complete", True):
            for symbol in payload.get("symbols", []):
                markets[venue + ":" + symbol] = dict(subscription_start_ms=at, subscribed=True)
        removed = payload.get("removed", []) if topic == "control/rotation" else []
        for symbol in removed:
            markets.pop(venue + ":" + symbol, None)
        if topic == "control/source_change":
            markets.clear()
        elif topic == "control/gap" or not row.get("complete", True):
            for name in list(markets):
                v, symbol = name.split(":", 1)
                if (row["source"] == "control/gap" or v == venue) and row["symbol"] in {"ALL", symbol}:
                    markets.pop(name, None)
        elif topic.startswith("ws/publicTrade."):
            valid = any(not trade.get("BT") for trade in payload.get("data", []))
            if valid:
                market = markets.setdefault(key, dict(subscribed=False))
                last = market.get("last_trade_ms")
                if last is None or at - last > stale_ms:
                    market["coverage_start_ms"] = at
                market["last_trade_ms"] = at
    store.db.execute(
        "INSERT OR REPLACE INTO kv VALUES('primary_recorded_coverage',?)",
        (
            json.dumps(
                dict(policy=COVERAGE_POLICY, receipt_ms=previous, trade_stale_ms=stale_ms, markets=markets)
            ),
        ),
    )


def eligibility(store, source, symbol, decision_ms, stale_ms):
    coverage = store.get("primary_recorded_coverage", {})
    stale_ms = coverage.get("trade_stale_ms", stale_ms)
    state = coverage.get("markets", {}).get(source + ":" + symbol, {})
    last, start = state.get("last_trade_ms"), state.get("coverage_start_ms")
    ready = bool(
        state.get("subscribed")
        and start is not None
        and last is not None
        and 0 <= decision_ms - last <= stale_ms
        and start <= decision_ms
    )
    return dict(
        eligible=ready,
        evaluated_ms=decision_ms,
        reason="COMMITTED_PRINTS_READY"
        if ready
        else "SUBSCRIPTION_NOT_READY"
        if not state.get("subscribed")
        else "PREDECISION_COVERAGE_MISSING",
        source=source,
        predecision_coverage_ms=max(0, decision_ms - start) if start is not None else 0,
        policy=COVERAGE_POLICY,
        minimum_predecision_ms=PRIMARY_PREDECISION_MIN_MS,
        last_committed_trade_ms=last,
        subscription_start_ms=state.get("subscription_start_ms"),
        diagnostic_only=True,
    )
