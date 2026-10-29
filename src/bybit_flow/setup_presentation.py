"""Compact public v2.2 setup explanation. No second headline or probability."""

from datetime import UTC, datetime

from .setup_common import number


def label(value):
    return str(value or "UNKNOWN").replace("_", " ").capitalize()


def numeric(value, digits=2, suffix=""):
    return f"{value:.{digits}f}{suffix}" if number(value) else "Unknown"


def band(value):
    return (
        "Unknown"
        if not number(value)
        else "Strong"
        if value >= 75
        else "Good"
        if value >= 60
        else "Mixed"
        if value >= 40
        else "Weak"
    )


def embed(signal, dashboard_url):
    s, e = signal, signal.evidence
    v = e.get("v22") or {}
    snap = e.get("delivery_execution_snapshot") or {}
    q = e.get("entry_quality") or {}
    response = e.get("impact_efficiency") or {}
    target = e.get("target_reachability") or {}
    reach = target.get("reachability") or {}
    path = target.get("path") or {}
    stop = e.get("stop_viability") or {}
    ofi = e.get("event_ofi") or {}
    priority = e.get("opportunity_priority") or {}
    fields = []

    def field(name, value, inline=False):
        fields.append(dict(name=name, value=str(value)[:850], inline=inline))

    exception = e.get("discord_opposition_exception", {})
    if exception.get("allowed"):
        field(
            "Why an opposite setup is allowed",
            "\n".join(r["explanation"] for r in exception.get("reviews", [])),
        )

    price = snap.get("executable_price", q.get("executable_price"))
    entry = (
        f"{price:g} observed {'ask' if s.direction == 'LONG' else 'bid'}"
        if number(price)
        else f"{s.entry:g} planned"
    )
    field("Entry", f"{entry}\nPlan {s.entry:g} · zone {s.zone[0]:g}–{s.zone[1]:g}", True)
    field("Stop loss", f"{s.stop:g}\n{label(stop.get('state'))}", True)
    structural = e.get("structural_tp1", s.tp1)
    target_text = f"{s.tp1:g}"
    if structural != s.tp1:
        target_text += f"\nStructural objective {structural:g}"
    field("Target", target_text, True)
    field(
        "Trade plan",
        f"Net reward:risk {numeric(q.get('cost_adjusted_rr_now'), suffix='R')} · "
        f"stop/noise {numeric(stop.get('stop_noise_ratio'), suffix='×')} · "
        f"target/setup ATR {numeric(reach.get('target_setup_atr'), suffix='×')}\n"
        f"Entry until <t:{min(s.expires_ms, s.trigger_expires_ms or s.expires_ms) // 1000}:t> · "
        f"tracking until <t:{(s.holding_deadline_ms or s.expires_ms) // 1000}:f>",
    )
    evidence = v.get("evidence", {}).get("components", {})
    tradeability = v.get("tradeability", {}).get("components", {})
    field(
        "Assessment",
        f"Evidence {band(v.get('evidence_score'))} · Tradeability {band(v.get('tradeability_score'))}\n"
        f"Location {numeric(evidence.get('location', {}).get('score'), 0)} · "
        f"Structure {numeric(evidence.get('structure', {}).get('score'), 0)} · "
        f"Mechanism {numeric(response.get('mechanism_score'), 0)}\n"
        f"Follow-through {numeric(response.get('score'), 0)} · "
        f"Target {numeric(target.get('score'), 0)} · Stop {numeric(stop.get('score'), 0)} · "
        f"Execution {numeric(tradeability.get('execution', {}).get('score'), 0)}",
    )
    field(
        "Flow and execution",
        f"{label(response.get('state'))} · directional delta {numeric(response.get('directional_aggression'), 1, '%')}\n"
        f"Price response {numeric(response.get('displacement_bps'), 1, ' bps')} · "
        f"impact persistence {numeric(100 * response['impact_persistence'], 0, '%') if number(response.get('impact_persistence')) else 'Unknown'}\n"
        f"Quote {label(q.get('quote_state'))} · spread {numeric(snap.get('spread_bps'), 2, ' bps')} · "
        f"OFI {label(response.get('ofi_state'))} ({label(ofi.get('state'))})",
    )
    barrier = path.get("strongest_barrier_distance_r")
    field(
        "Target and context",
        f"Reachability {label(target.get('state'))} · path {label(path.get('state'))}\n"
        + (f"Strongest observed barrier {barrier:.2f}R from entry\n" if number(barrier) else "")
        + f"Market {label(e.get('market_alignment', {}).get('alignment'))} · "
        f"cross venue {'Available' if e.get('cross_exchange', {}).get('available') else 'Unknown'}\n"
        + f"Shared market exposure {label(priority.get('shared_exposure'))}"
        + (
            f" · rank {priority['rank']}/{priority['rank_of']} (informational)"
            if priority.get("rank")
            else ""
        ),
    )
    strengths = v.get("strengths") or ["No strong independently supported component"]
    weaknesses = v.get("weaknesses") or ["Market conditions can change after this observation"]
    field(
        "Strengths / limiting factors",
        "\n".join(["+ " + x for x in strengths[:2]] + ["• " + x for x in weaknesses[:2]]),
    )
    field(
        "Why this setup",
        f"{label(s.family)} with a frozen structural reference. {label(response.get('state'))}; "
        f"the execution target is {label(target.get('state')).lower()} under the historical-horizon assumption. "
        "Component scores describe the evidence; they do not estimate success probability.",
    )
    field(
        "Invalidation",
        f"Planned stop {s.stop:g}, or failure of the frozen structural premise. "
        "Feed interruptions pause monitoring. No account entry or automatic execution is assumed.",
    )
    at = snap.get("available_ms", v.get("available_ms", s.created_ms))
    return dict(
        allowed_mentions=dict(parse=[]),
        embeds=[
            dict(
                title=f"NEW SETUP · {s.symbol} {s.direction}",
                description=f"**Setup Score {s.quality:.1f}/100 · {s.raw_tier}**\n"
                f"{label(s.horizon_profile)} · {s.expected_hold_min / 60:g}–{s.expected_hold_max / 60:g} hours · "
                f"{label(s.family)} · {s.source.capitalize()}",
                url=f"{dashboard_url.rstrip('/')}/#signal/{s.id}",
                color=0x4CC9A4 if s.direction == "LONG" else 0xEF7F86,
                timestamp=datetime.fromtimestamp(at / 1000, UTC).isoformat(),
                fields=fields,
                footer=dict(text=f"{s.id} · Deterministic setup quality; not a win probability"),
            )
        ],
    )
