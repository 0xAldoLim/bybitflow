"""Counterfactual labels for ALL frozen candidates, using verified recorder envelopes.

Never imports chart outcomes as verified execution. Funding is a conservative configured
reserve in this policy, explicitly not reconstructed settlement costs.
"""

import json
from decimal import Decimal

from ..backtest import PaperPosition
from ..models import Signal, Trade
from .store import FeatureStore, digest


def label_recordings(store, rows, settings, stage="decision", max_active=2000):
    fs = FeatureStore(store)
    pending = iter(fs.snapshots(stage))
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
            if signal.symbol not in subscribed or now - s["decision_ms"] > settings.trade_stale_ms:
                p.data_gaps.append("entry coverage not continuously observed")
            active[s["id"]] = (s, p)
        source, symbol = row["source"], row["symbol"]
        if source == "control/subscribed":
            subscribed.update(payload["symbols"])
        removed = set(payload.get("removed", [])) if source == "control/rotation" else set()
        if source == "control/gap":
            removed = set(subscribed)
        subscribed.difference_update(removed)
        for s, p in active.values():
            if not row.get("complete", True) or p.signal.symbol in removed or source == "control/gap":
                if "recording continuity lost" not in p.data_gaps:
                    p.data_gaps.append("recording continuity lost")
        if source.startswith("ws/publicTrade."):
            if symbol in last and now - last[symbol] > settings.trade_stale_ms:
                for s, p in active.values():
                    if p.signal.symbol == symbol:
                        p.data_gaps.append("trade-feed stale interval")
            last[symbol] = now
            for raw in payload["data"]:
                key = (symbol, raw["i"])
                if raw.get("BT") or key in seen:
                    continue
                seen.add(key)
                if len(seen) > settings.tape_max_trades:
                    # ID set only bounds duplicate suppression, not proof of complete tape coverage.
                    seen = {key}
                trade = Trade(
                    symbol, int(raw["T"]), now, raw["i"], raw["S"], Decimal(raw["p"]), Decimal(raw["v"])
                )
                for s, p in active.values():
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
