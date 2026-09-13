"""Counterfactual labels for ALL frozen candidates, using verified recorder envelopes.

Never imports chart outcomes as verified execution. Funding is a conservative configured
reserve in this policy, explicitly not reconstructed settlement costs.
"""

import json
from decimal import Decimal

from ..backtest import PaperPosition
from ..models import Signal, Trade
from .store import FeatureStore, digest


def label_chart(store, events_path, candles_path, settings, symbol):
    """Separate source-attested TV + OHLC labels; never promoted as native execution data."""
    import hashlib

    from ..tv_research import family_replay, read_candles, read_events

    if events_path.stat().st_size > 64_000_000 or candles_path.stat().st_size > 64_000_000:
        raise ValueError("Chart files exceed the 64 MB per-input research budget")
    events, bars = read_events(events_path), read_candles(candles_path)
    if len(events) > 10_000 or len(bars) > 250_000:
        raise ValueError("Chart research input exceeds bounded worker capacity")
    report = family_replay(events, bars, settings, symbol, include_rejected=True)
    fs = FeatureStore(store)
    snapshots = {}
    for row in report["candidate_snapshots"]:
        signal = Signal.model_validate(row["signal"])
        snapshots[signal.id] = fs.capture(signal, row["decision_ms"], "chart")
    inputs = {
        "events": hashlib.sha256(events_path.read_bytes()).hexdigest(),
        "candles": hashlib.sha256(candles_path.read_bytes()).hexdigest(),
    }
    for outcome in report["outcomes"]:
        label = outcome | dict(
            policy="chart-v1",
            data_kind="source-attested-TV-OHLC",
            costs_verified=False,
            classification="win" if outcome["net_r"] > 0 else "loss" if outcome["net_r"] < 0 else "breakeven",
            input_hashes=inputs,
            quantity=1.0,
            fee_bps=settings.taker_fee_bps,
            slippage_bps=settings.slippage_bps,
            funding_reserve_bps=settings.funding_reserve_bps,
            excursion_definition="OHLC extremes through exit bar: bounds, not an observed intrabar path",
            limitation="No native prints/depth, partial fills or verified settlements; never full-flow validation",
        )
        fs.label(snapshots[outcome["signal_id"]], label, outcome["exit_ms"])
    return dict(
        candidates=len(snapshots),
        complete=len(report["outcomes"]),
        policy="chart-v1",
        stage="chart",
        rejections=report["rejections"],
    )


def label_recordings(store, rows, settings, stage="decision", max_active=2000):
    fs = FeatureStore(store)
    retained_after = store.get("recording_retention", {}).get("through_ms", 0)
    # Previously frozen labels stay available for learning. Unlabelled decisions
    # before the retained boundary cannot be reconstructed from newer prints.
    pending = (s for s in fs.snapshots(stage) if s["decision_ms"] > retained_after)
    next_snapshot = next(pending, None)
    active, results, subscribed, last, seen = {}, [], set(), {}, set()
    now, previous = 0, -1

    def finish(ident, snapshot, position, at_ms):
        outcome = position.outcome()
        # Liquidity participation may prevent a timely time-stop exit; do not label a later fill as 4H.
        if outcome["exit_ms"] and outcome["exit_ms"] > position.signal.created_ms + 14_400_000:
            outcome["complete"] = False
            outcome["data_gaps"].append("exit beyond maximum four-hour policy")
        reserve = position.entry_value * settings.funding_reserve_bps / 10000
        risk = position.quantity * abs(position.signal.entry - position.signal.stop)
        if outcome["complete"]:
            outcome["net_pnl"] -= reserve
            outcome["net_r"] = outcome["net_pnl"] / risk if risk else None
            outcome["classification"] = (
                "win" if outcome["net_r"] > 1e-10 else "loss" if outcome["net_r"] < -1e-10 else "breakeven"
            )
        else:
            outcome["net_pnl"] = outcome["net_r"] = None
            outcome["classification"] = "incomplete"
        outcome.update(
            policy="prints-v1",
            data_kind="recorded-public-prints",
            source_methodology=snapshot["source"],
            funding_reserve=reserve,
            costs_verified=False,
            fees_bps=settings.taker_fee_bps,
            slippage_bps=settings.slippage_bps,
            funding_assumption_bps=settings.funding_reserve_bps,
            feature_schema_version=snapshot["schema_version"],
            model_version=snapshot["signal"].get("model_version"),
            input_event_hash=event_hash.hexdigest(),
            outcome_definition="whole position TP1/stop/4H, 1% trade participation, fee/slip/funding assumptions",
        )
        # Unresolved outcomes remain reproducible reports, not frozen labels blocking later completion.
        if position.exit_ms is not None:
            fs.label(ident, outcome, at_ms)
        results.append(dict(snapshot_id=ident, **outcome))

    import hashlib

    event_hash = hashlib.sha256()
    for row in rows:
        now = row["receipt_ms"]
        if now < previous:
            raise ValueError("Labels require receipt-ordered records")
        previous = now
        event_hash.update(digest(row).encode())
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        source, symbol, venue = row["source"], row["symbol"], "bybit"
        # Recorder-chain gaps have no venue prefix: they affect every source,
        # unlike a venue-specific native subscription gap.
        global_gap = source == "control/gap"
        if source.startswith("native/"):
            _, venue, source = source.split("/", 2)
        while next_snapshot and next_snapshot["decision_ms"] <= now:
            s = next_snapshot
            next_snapshot = next(pending, None)
            if store.db.execute(
                "SELECT 1 FROM ml_labels WHERE snapshot_id=? AND policy='prints-v1'", (s["id"],)
            ).fetchone():
                continue
            if len(active) >= max_active:
                raise ValueError(
                    "Paper candidate capacity exceeded; partition recordings, do not silently sample"
                )
            signal = Signal.model_validate(s["signal"])
            signal.created_ms = s["decision_ms"]  # availability, never original earlier candle close
            p = PaperPosition(
                signal,
                settings.hypothetical_notional / signal.entry,
                fee_bps=settings.taker_fee_bps,
                slippage_bps=settings.slippage_bps,
            )
            if (signal.source, signal.symbol) not in subscribed or now - s[
                "decision_ms"
            ] > settings.trade_stale_ms:
                p.data_gaps.append("entry coverage not continuously observed")
            active[s["id"]] = (s, p)
        if source == "control/subscribed":
            subscribed.update((venue, s) for s in payload["symbols"])
        removed = {(venue, s) for s in payload.get("removed", [])} if source == "control/rotation" else set()
        if source == "control/gap":
            removed = {
                key
                for key in subscribed
                if (global_gap or key[0] == venue) and (symbol == "ALL" or key[1] == symbol)
            }
        if source == "control/source_change":
            removed = set(subscribed)
        subscribed.difference_update(removed)
        for s, p in active.values():
            affected = (p.signal.source, p.signal.symbol) in removed
            if affected or (
                not row.get("complete", True)
                and (
                    source == "control/source_change"
                    or ((global_gap or p.signal.source == venue) and symbol in {"ALL", p.signal.symbol})
                )
            ):
                if "recording continuity lost" not in p.data_gaps:
                    p.data_gaps.append("recording continuity lost")
        if source.startswith("ws/publicTrade."):
            source_symbol = (venue, symbol)
            if source_symbol in last and now - last[source_symbol] > settings.trade_stale_ms:
                for s, p in active.values():
                    if (p.signal.source, p.signal.symbol) == source_symbol:
                        p.data_gaps.append("trade-feed stale interval")
            last[source_symbol] = now
            for raw in payload["data"]:
                key = (venue, symbol, raw["i"])
                if raw.get("BT") or key in seen:
                    continue
                seen.add(key)
                if len(seen) > settings.tape_max_trades:
                    # ID set only bounds duplicate suppression, not proof of complete tape coverage.
                    seen = {key}
                trade = Trade(
                    symbol,
                    int(raw["T"]),
                    now,
                    raw["i"],
                    raw["S"],
                    Decimal(raw["p"]),
                    Decimal(raw["v"]),
                    venue,
                )
                for s, p in active.values():
                    if p.signal.source == venue:
                        p.on_trade(trade)
        done = [
            ident
            for ident, (s, p) in active.items()
            if p.exit_ms is not None or now > p.signal.created_ms + 14_460_000
        ]
        for ident in done:
            s, p = active.pop(ident)
            finish(ident, s, p, now)
    for ident, (s, p) in active.items():
        finish(ident, s, p, now)
    return dict(
        policy="prints-v1",
        outcomes=results,
        complete=sum(x["complete"] for x in results),
        limitation="Recorded subscription continuity is not exchange-certified completeness; costs assumed",
    )
