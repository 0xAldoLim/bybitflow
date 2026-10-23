"""Fixed, descriptive v2.2 replay of loss-review exports; never a backtest.

Usage: python examples/compare_v22.py --archive-dir PATH --output-dir PATH
Requires published.jsonl and flattened.json produced by the loss-review export.
The latter contains deduplicated complete proxy labels. No live database is opened.
Historical candles, multi-window OFI and trimmed profile bins remain UNKNOWN.
"""

import argparse
import csv
import hashlib
import json
from collections import Counter
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from statistics import mean, median

from bybit_flow.config import Settings
from bybit_flow.models import Signal
from bybit_flow.orderflow import Book
from bybit_flow.quality_v22 import POLICY, apply
from bybit_flow.setup_common import number

ASSUMPTIONS = {
    "scope": "Selection/score-only replay on original frozen plan; no retargeted P&L",
    "minimum_net_rr": 2.0,
    "maximum_spread_bps": 5.0,
    "missing_history": "UNKNOWN; no synthetic candles, OFI or profile volume",
    "timing": "Critical feature availability must be at/before initial acknowledgement",
    "entry_coverage": "Restore only from timestamped original v2 complete-coverage proof",
    "outcomes": "Attach original proxy outcomes only after scoring; no verified fills",
    "tuning": "One fixed evaluation; no outcome-driven parameter search",
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def score_export(record, settings):
    """Feature-only calculation: no label, exit, realized P&L or outcome access."""
    raw = deepcopy(record["signal"])
    e = raw["evidence"]
    # Monitor-added outcomes are excluded from the feature-only input.
    for key in ("primary_outcome", "proxy_outcome", "expiry_mark", "research_path", "withdrawal"):
        e.pop(key, None)
    raw["coverage"] = {}
    raw["state"] = "CONFIRMED"
    delivered = record["delivered_ms"]
    stamps = [
        e.get("v2", {}).get("available_ms"),
        e.get("flow_quality", {}).get("available_ms"),
        e.get("flow_quality", {}).get("window_end_ms"),
        e.get("entry_quality", {}).get("available_ms"),
        e.get("book", {}).get("receipt_ms"),
        e.get("book", {}).get("event_ms"),
    ]
    if not all(number(t) and t <= delivered for t in stamps):
        return dict(excluded="Missing or post-delivery critical feature timestamp")
    now = max(stamps)
    v2 = e.get("v2", {})
    if not v2.get("coverage_complete"):
        return dict(excluded="No frozen complete native entry-coverage proof")
    raw["coverage"]["trade_window_complete"] = True
    e["production_policies"] = dict(e.get("production_policies", {}), trade_quality=POLICY)
    e["structural_tp1"] = raw["tp1"]
    e["horizon_movement_capacity"] = dict(
        state="UNKNOWN",
        source=raw["source"],
        symbol=raw["symbol"],
        available_ms=now,
        reason="Completed predecision horizon candles absent from export",
    )
    e["event_ofi"] = dict(
        state="UNAVAILABLE",
        source=raw["source"],
        symbol=raw["symbol"],
        available_ms=now,
        reason="Raw multi-window quote-event buckets absent from export",
    )
    # Latest terminal gates are not the initial card's qualification.
    raw["gates"] = list(record.get("initial_card", {}).get("gates", []))
    signal = Signal.model_validate(raw)
    observed = e.get("book", {})
    book = Book()
    book.bids = {Decimal(p): Decimal(q) for p, q in observed.get("bids", [])}
    book.asks = {Decimal(p): Decimal(q) for p, q in observed.get("asks", [])}
    if not book.bids or not book.asks:
        return dict(excluded="No causal executable book levels")
    book.valid = True
    book.event_ms, book.receipt_ms = observed["event_ms"], observed["receipt_ms"]
    price = float(min(book.asks) if signal.direction == "LONG" else max(book.bids))
    liquidity = e.get("liquidity_stress", {})
    if not 0 <= now - liquidity.get("at_ms", 0) <= 120000:
        liquidity = {}
    apply(signal, price, now, liquidity, settings.min_net_rr, book=book, settings=settings)
    v22 = signal.evidence["v22"]
    return dict(
        signal_id=signal.id,
        symbol=signal.symbol,
        direction=signal.direction,
        original_policy=record["signal"]["evidence"].get("production_policies", {}).get("trade_quality"),
        feature_available_ms=now,
        delivered_ms=delivered,
        old_score=record["signal"]["quality"],
        setup_score=signal.quality,
        evidence_score=v22["evidence_score"],
        tradeability_score=v22["tradeability_score"],
        confidence_cap=v22["confidence_cap"],
        gates=signal.gates,
        v22_eligible=not signal.gates and v22["coverage_complete"],
        required_evidence_complete=v22["coverage_complete"],
        reachability=signal.evidence["target_reachability"]["state"],
        path=signal.evidence["target_reachability"]["path"]["state"],
        stop=signal.evidence["stop_viability"]["state"],
        impact=signal.evidence["impact_efficiency"]["state"],
        executable_price=price,
    )


def avg(rows, key):
    values = [r[key] for r in rows if number(r.get(key))]
    return dict(value=mean(values) if values else None, observations=len(values))


def outcomes(rows):
    n = len(rows)
    return dict(
        setups=n,
        losses=sum(r["net_r"] < 0 for r in rows),
        positives=sum(r["net_r"] > 0 for r in rows),
        sum_original_proxy_net_r=sum(r["net_r"] for r in rows),
        average_original_proxy_net_r=avg(rows, "net_r"),
        stops=sum(r["exit_reason"] == "STOP" for r in rows),
        time_exits=sum(r["exit_reason"] == "TIME_EXIT" for r in rows),
        stop_rate=sum(r["exit_reason"] == "STOP" for r in rows) / n if n else None,
        time_exit_rate=sum(r["exit_reason"] == "TIME_EXIT" for r in rows) / n if n else None,
        mfe_lower_bound_r=avg(rows, "mfe_r"),
        mae_lower_bound_r=avg(rows, "mae_r"),
    )


def compare(rows):
    kept = [r for r in rows if r["v22_eligible"]]
    removed = [r for r in rows if not r["v22_eligible"]]
    return dict(
        original=outcomes(rows),
        v22_selection_only=outcomes(kept),
        losing_labels_excluded=sum(r["net_r"] < 0 for r in removed),
        positive_labels_excluded=sum(r["net_r"] > 0 for r in removed),
        selection_only_original_proxy_net_r_delta=sum(r["net_r"] for r in kept)
        - sum(r["net_r"] for r in rows),
        rejection_reasons=dict(Counter(g for r in rows for g in r["gates"])),
        reachability=dict(Counter(r["reachability"] for r in rows)),
        path=dict(Counter(r["path"] for r in rows)),
        stop_viability=dict(Counter(r["stop"] for r in rows)),
        scores={
            name: dict(
                count=len(group),
                old_median=median([r["old_score"] for r in group]) if group else None,
                v22_median=median([r["setup_score"] for r in group]) if group else None,
            )
            for name, group in (
                ("losses", [r for r in rows if r["net_r"] < 0]),
                ("positives", [r for r in rows if r["net_r"] > 0]),
            )
        },
        fixed_score_bands=[
            dict(
                band=f"{lo}-{lo + 20}",
                **outcomes(
                    [
                        r
                        for r in rows
                        if lo <= r["setup_score"] < lo + 20 or lo == 80 and r["setup_score"] == 100
                    ]
                ),
            )
            for lo in (0, 20, 40, 60, 80)
        ],
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    archive, out = args.archive_dir, args.output_dir
    inputs = [archive / "published.jsonl", archive / "flattened.json"]
    fingerprints = {p.name: sha(p) for p in inputs}
    settings = Settings(
        _env_file=None,
        min_net_rr=ASSUMPTIONS["minimum_net_rr"],
        max_spread_bps=ASSUMPTIONS["maximum_spread_bps"],
    )
    published = {d["signal_id"]: d for d in (json.loads(line) for line in inputs[0].open(encoding="utf-8"))}
    # Score the entire exported published universe before looking at outcome labels.
    scored = {ident: score_export(d, settings) for ident, d in published.items()}
    data = json.loads(inputs[1].read_text(encoding="utf-8"))
    labels = [
        r for r in data["rows"] if r["track"] == "proxy" and r["schema"] == "candidate-v20" and r["published"]
    ]
    matched, excluded = [], []
    for label in labels:
        observation = scored[label["signal_id"]]
        if observation.get("excluded"):
            excluded.append(dict(signal_id=label["signal_id"], reason=observation["excluded"]))
            continue
        matched.append(
            observation
            | {key: label.get(key) for key in ("net_r", "exit_reason", "entry_ms", "mfe_r", "mae_r")}
            | dict(post_card_entry=label["entry_ms"] >= label["delivered_ms"])
        )
    newer = [r for r in matched if r["original_policy"] == "trade-validity-v21.1"]
    post = [r for r in newer if r["post_card_entry"]]
    full_newer_labels = [
        r
        for r in labels
        if published[r["signal_id"]]["signal"]["evidence"].get("production_policies", {}).get("trade_quality")
        == "trade-validity-v21.1"
    ]
    src = Path(__file__).resolve().parents[1] / "src" / "bybit_flow"
    modules = (
        "quality_v22.py",
        "setup_scoring.py",
        "impact_efficiency.py",
        "target_reachability.py",
        "stop_viability.py",
    )
    report = dict(
        assumptions=ASSUMPTIONS,
        input_sha256=fingerprints,
        source_sha256={name: sha(src / name) for name in modules},
        current_v211_full_reference=outcomes(full_newer_labels),
        all_v20_timely_features=compare(matched),
        v211_matched_timely_features=compare(newer),
        v211_matched_post_card_proxy_entry=compare(post),
        exclusions=excluded,
        limitations=[
            "Descriptive in-sample selection replay, not validated profitability or a retargeted backtest.",
            "Missing candles/OFI/volume bins remain unknown; full v2.2 target-construction performance cannot be computed.",
            "Proxy outcomes retain original entry, costs and plan; changed executable-price economics are not resimulated.",
            "Predelivery proxy entries are excluded from the post-card comparison.",
            "Repeated related setups and overlapping days are dependent; score bands do not establish monotonicity.",
            "MFE/MAE are available candle lower bounds, with stated observation counts, not exact tick extrema.",
            "Actual losses avoided, causal P&L improvement and forward performance remain UNKNOWN.",
        ],
    )
    out.mkdir(parents=True, exist_ok=True)
    (out / "v22-fixed-comparison.json").write_text(
        json.dumps(report, indent=2, allow_nan=False), encoding="utf-8"
    )
    with (out / "v22-selection-rows.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(matched[0]) if matched else ["signal_id"])
        writer.writeheader()
        writer.writerows(matched)
    print(json.dumps(report, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
