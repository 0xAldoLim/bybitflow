"""Optional account-fill review; never writes decisions, labels or model evidence."""

import csv
import math


def review(path):
    results = []
    with path.open(encoding="utf-8-sig", newline="") as stream:
        for line, row in enumerate(csv.DictReader(stream), 2):
            try:
                values = {
                    key: float(row[key])
                    for key in (
                        "planned_entry",
                        "original_stop",
                        "entry",
                        "exit",
                        "quantity",
                        "fees",
                        "funding_cost",
                        "delivered_ms",
                        "entry_ms",
                        "exit_ms",
                    )
                }
                if not all(math.isfinite(v) for v in values.values()):
                    raise ValueError("nonfinite value")
                direction = {"LONG": 1, "SHORT": -1}[row["direction"]]
                v = values
                if min(v[k] for k in ("planned_entry", "original_stop", "entry", "exit", "quantity")) <= 0:
                    raise ValueError("prices and quantity must be positive")
                if direction * (v["planned_entry"] - v["original_stop"]) <= 0:
                    raise ValueError("invalid original stop")
                if not 0 <= v["delivered_ms"] <= v["entry_ms"] <= v["exit_ms"]:
                    raise ValueError("fills must follow delivery in time order")
                risk = abs(v["planned_entry"] - v["original_stop"]) * v["quantity"]
                net = direction * (v["exit"] - v["entry"]) * v["quantity"] - v["fees"] - v["funding_cost"]
                results.append(
                    dict(
                        signal_id=row["signal_id"],
                        source=row["source"],
                        net_pnl=net,
                        net_r=net / risk,
                        entry_delay_seconds=(v["entry_ms"] - v["delivered_ms"]) / 1000,
                        adverse_entry_slippage_bps=direction * (v["entry"] / v["planned_entry"] - 1) * 10000,
                    )
                )
            except (ValueError, KeyError) as exc:
                raise ValueError(f"Invalid fill journal row {line}: {exc}") from exc
    return dict(
        rows=results,
        count=len(results),
        production_ml=False,
        provenance="User-reported equal-quantity linear round trips, not independently verified fills",
        limitation="Use volume-weighted prices for partial fills. Fees and funding are signed costs; rebates are negative. No inverse contracts. This report cannot satisfy model promotion.",
    )
