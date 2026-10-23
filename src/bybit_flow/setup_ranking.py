"""All valid opportunities may be delivered. Ranking only describes their order."""

from .opportunity import cluster


def value(signal, key, default=None):
    return signal.get(key, default) if isinstance(signal, dict) else getattr(signal, key, default)


def rank(signals, now):
    valid = [
        s
        for s in signals
        if not value(s, "gates", [])
        and (
            value(s, "state") in {"CONFIRMED", "ALERTED"}
            or value(s, "state") == "PENDING CONFIRMATION"
            and value(s, "evidence", {}).get("flow_confirmed")
            and value(s, "evidence", {}).get("entry_quality", {}).get("acceptable")
        )
    ]
    groups = {}
    for s in valid:
        group = (value(s, "source"), cluster(s), value(s, "direction"))
        groups.setdefault(group, set()).add(value(s, "symbol"))

    def order(s):
        e = value(s, "evidence", {})
        v = e.get("v22") or {}
        return (
            value(s, "quality", 0),
            v.get("tradeability_score") or 0,
            (e.get("impact_efficiency") or {}).get("score") or 0,
            (e.get("target_reachability") or {}).get("score") or 0,
            (e.get("entry_quality") or {}).get("cost_adjusted_rr_now") or 0,
        )

    results = {}
    for position, s in enumerate(sorted(valid, key=lambda s: (*(-v for v in order(s)), value(s, "id"))), 1):
        group = (value(s, "source"), cluster(s), value(s, "direction"))
        size = len(groups[group])
        shared = (
            "UNKNOWN"
            if group[1] == "LOW_BETA_IDIOSYNCRATIC"
            else "HIGH"
            if size >= 3
            else "MEDIUM"
            if size == 2
            else "LOW"
        )
        results[value(s, "id")] = dict(
            policy="setup-ranking-v22",
            source=value(s, "source"),
            symbol=value(s, "symbol"),
            available_ms=now,
            rank=position,
            rank_of=len(valid),
            setup_score=value(s, "quality", 0),
            tradeability=(value(s, "evidence", {}).get("v22") or {}).get("tradeability_score"),
            cluster_id=group[1],
            cluster_size=size,
            shared_exposure=shared,
            delivery_suppressed=False,
            reason="Informational Setup Score ranking; no quota, top-N restriction or correlation veto",
        )
    return results


def rank_signal(scanner, signal, now):
    active = [
        r
        for r in scanner.store.active_signals()
        if r["id"] != signal.id
        and r.get("source") == signal.source
        and r.get("state") in {"CONFIRMED", "ALERTED"}
        and not r.get("gates")
    ]
    signal.evidence["opportunity_priority"] = rank([signal, *active], now).get(signal.id, {})
