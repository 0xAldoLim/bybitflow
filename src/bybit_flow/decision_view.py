"""Information-only dashboard projection. Never ranks away delivery eligibility."""

from collections import Counter

from .setup_common import number


def overview(rows, now, quotes=None):
    active = [r for r in rows if r["state"] not in {"INVALIDATED", "EXPIRED", "RESOLVED"}]
    groups = Counter((r.get("source"), r.get("direction")) for r in active)
    result = []
    for s in active:
        e = s.get("evidence", {})
        v = e.get("v22", {})
        health = e.get("thesis_health", {})
        if not 0 <= now - health.get("checked_ms", 0) <= 90_000:
            health = {"state": "UNKNOWN", "reason": "No recent thesis observation"}
        entry, stop = s.get("entry"), s.get("stop")
        quote = (quotes or {}).get((s.get("source"), s["symbol"]), {})
        price = quote.get("price") if quote.get("fresh") else None
        progress = (
            (price - entry) * (1 if s["direction"] == "LONG" else -1) / abs(entry - stop)
            if all(number(x) for x in (price, entry, stop)) and entry != stop
            else None
        )
        result.append(
            dict(
                id=s["id"],
                symbol=s["symbol"],
                direction=s["direction"],
                source=s.get("source"),
                horizon=s.get("horizon_profile"),
                family=s.get("family"),
                published_score=s.get("quality"),
                evidence_score=v.get("evidence_score"),
                tradeability_score=v.get("tradeability_score"),
                net_rr=s.get("risk", {}).get("net_rr"),
                reachability=e.get("target_reachability", {}).get("state", "UNKNOWN"),
                stop_viability=e.get("stop_viability", {}).get("state", "UNKNOWN"),
                impact=e.get("impact_efficiency", {}).get("state", "UNKNOWN"),
                ofi=e.get("event_ofi", {}).get("state", "UNKNOWN"),
                entry_deadline=s.get("trigger_expires_ms") or s.get("expires_ms"),
                original_plan={k: s.get(k) for k in ("entry", "stop", "tp1", "tp2")},
                current_health=health or {"state": "UNKNOWN"},
                current_price=price,
                current_quote_state="FRESH" if quote.get("fresh") else "UNKNOWN",
                progress_r=progress,
                monitoring=s.get("coverage", {}).get("monitoring", "UNKNOWN"),
                data_freshness=s.get("coverage", {}),
                blocked_reasons=s.get("gates", []),
                state=s["state"],
                shared_exposure=groups[(s.get("source"), s.get("direction"))],
                exposure_scope="Same-source direction count; correlation not measured",
                correlation_context=e.get("opportunity_priority", {}),
            )
        )
    valid = [
        r
        for r in result
        if not r["blocked_reasons"]
        and r["state"] in {"CONFIRMED", "ALERTED"}
        and r["entry_deadline"] >= now
        and r["monitoring"] != "paused"
        and r["current_quote_state"] == "FRESH"
    ]
    valid.sort(key=lambda r: (-(r["published_score"] or 0), r["id"]))
    for rank, r in enumerate(valid, 1):
        r["rank"] = rank
    return dict(
        opportunities=valid,
        active=result,
        blocked=[r for r in result if r["blocked_reasons"]],
        ranking="Informational; no top-N suppression or delivery authority",
        source_row_limit=len(rows),
    )


def rows(store):
    import json

    fields = (
        "id",
        "symbol",
        "direction",
        "source",
        "horizon_profile",
        "family",
        "quality",
        "state",
        "entry",
        "stop",
        "tp1",
        "tp2",
        "trigger_expires_ms",
        "expires_ms",
        "gates",
        "coverage",
        "risk",
    )
    evidence = (
        "v22",
        "target_reachability",
        "stop_viability",
        "impact_efficiency",
        "event_ofi",
        "thesis_health",
        "current_price",
        "opportunity_priority",
    )
    columns = (
        ",".join("json_extract(payload,'$." + k + "')" for k in fields)
        + ","
        + ",".join("json_extract(payload,'$.evidence." + k + "')" for k in evidence)
    )
    result = []
    for row in store.db.execute(
        "SELECT " + columns + " FROM signals WHERE state NOT IN ('INVALIDATED','EXPIRED','RESOLVED')"
    ):
        record = dict(zip(fields, row[: len(fields)]))
        for key in ("gates", "coverage", "risk"):
            record[key] = json.loads(record[key]) if record[key] else [] if key == "gates" else {}
        record["evidence"] = {
            k: json.loads(v) if isinstance(v, str) and v.startswith(("{", "[")) else v
            for k, v in zip(evidence, row[len(fields) :])
            if v is not None
        }
        record["evidence"]["thesis_health"] = store.get("thesis_health:" + record["id"], {})
        result.append(record)
    return result


def current_quotes(scanner, records, now):
    result = {}
    for row in records:
        key = (row["source"], row["symbol"])
        if key in result:
            continue
        source = row["source"]
        streams = (
            scanner.streams
            if source == scanner.exchange
            else scanner.cross_venue.peers.get(source, (None, None))[1]
        )
        book = streams.books.get(row["symbol"]) if streams else None
        fresh = bool(book and book.fresh(now, scanner.settings.book_stale_ms))
        result[key] = dict(
            fresh=fresh,
            price=(float(max(book.bids)) + float(min(book.asks))) / 2
            if fresh and book.bids and book.asks
            else None,
        )
    return result
