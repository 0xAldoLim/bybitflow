"""Offline memory benchmark. Uses synthetic state; never connects to live services.

Run fresh processes for before/after comparisons. --baseline-dir contains old
orderflow.py and exchanges.py sources exported with git show, not a checkout.
The optional sample is a JSON object with a small public-wire 'trades' list.
"""

import argparse
import asyncio
import gc
import hashlib
import json
import sys
import tempfile
import time
import tracemalloc
import types
import weakref
from collections import deque
from dataclasses import fields, is_dataclass
from decimal import Decimal
from pathlib import Path

from bybit_flow import exchanges, orderflow
from bybit_flow.config import Settings
from bybit_flow.models import Candle, Signal

BASE = 1_790_000_000_000


def process_memory():
    path = Path("/proc/self/status")
    if not path.exists():
        return {"platform": sys.platform, "rss": None}
    values = {}
    for line in path.read_text().splitlines():
        key = line.split(":")[0]
        if key in {"VmRSS", "VmHWM", "RssAnon", "RssFile", "VmSwap"}:
            values[key] = int(line.split()[1]) * 1024
    rollup = Path("/proc/self/smaps_rollup")
    if rollup.exists():
        for line in rollup.read_text().splitlines():
            if line.startswith("Pss:"):
                values["Pss"] = int(line.split()[1]) * 1024
    return values


def deep_bytes(root, seen=None):
    """Controlled object-graph estimate; run only outside the live process."""
    seen = set() if seen is None else seen
    ident = id(root)
    if ident in seen:
        return 0
    seen.add(ident)
    size = sys.getsizeof(root)
    if isinstance(root, dict):
        return size + sum(deep_bytes(k, seen) + deep_bytes(v, seen) for k, v in root.items())
    if isinstance(root, (list, tuple, set, frozenset, deque)):
        return size + sum(deep_bytes(v, seen) for v in root)
    if is_dataclass(root):
        return size + sum(deep_bytes(getattr(root, f.name), seen) for f in fields(root))
    if hasattr(root, "__dict__") and not isinstance(root, (type, types.ModuleType)):
        return size + deep_bytes(vars(root), seen)
    return size


def original(path, name):
    namespace = {"__name__": "bybit_flow._benchmark_" + name, "__package__": "bybit_flow"}
    exec(compile((path / (name + ".py")).read_text(), name, "exec"), namespace)
    return namespace


class Workload:
    def __init__(self, args):
        self.args = args
        self.tape_type = orderflow.Tape
        self.decode = exchanges.trade_from_wire
        if args.baseline_dir:
            self.tape_type = original(args.baseline_dir, "orderflow")["Tape"]
            self.decode = original(args.baseline_dir, "exchanges")["trade_from_wire"]
        rows = json.loads(args.sample.read_text())["trades"] if args.sample else []
        self.samples = {}
        for row in rows:
            self.samples.setdefault(row["s"], []).append(row)
        names = list(self.samples)
        self.symbols = [names[i] if i < len(names) else f"SYN{i:03}USDT" for i in range(args.symbols)]
        self.tapes, self.books, self.frames = {}, {}, {}
        self.history, self.context, self.spreads, self.liquidations = {}, {}, {}, {}
        self.stages = []
        self.cpu = time.process_time()
        self.fingerprint = hashlib.sha256()

    def mark(self, stage):
        self.stages.append(
            {
                "stage": stage,
                "cpu_seconds": time.process_time() - self.cpu,
                "memory": process_memory(),
                "markets": len(self.tapes),
                "trades": sum(len(t.trades) for t in self.tapes.values()),
            }
        )

    def fill(self, symbol, serial=0, count=None):
        tape = self.tapes.setdefault(
            symbol, self.tape_type(self.args.trades, source="binance", symbol=symbol)
        )
        rows = self.samples.get(symbol, [dict(p="100.00", q="10.000", m=False)])
        templates = [json.dumps(dict(r, s=symbol)) for r in rows]
        for i in range(serial, serial + (count or self.args.trades)):
            raw = json.loads(templates[i % len(templates)])
            raw.update(a=i, T=BASE + i * 20)
            tape.add(self.decode("binance", raw, raw["T"] + 100))

    def market_state(self, symbol):
        book = orderflow.Book()
        book.apply(
            dict(
                type="snapshot",
                ts=BASE,
                data=dict(
                    u=10,
                    seq=10,
                    b=[[str(99 - i / 1000), "10.000"] for i in range(500)],
                    a=[[str(101 + i / 1000), "10.000"] for i in range(500)],
                ),
            ),
            BASE,
        )
        for i in range(120):
            book.apply(
                dict(
                    type="delta",
                    ts=BASE + i * 1000,
                    data=dict(u=11 + i, seq=11 + i, b=[["99.0", str(10 + i % 3)]], a=[]),
                ),
                BASE + i * 1000,
            )
        for i in range(5000):
            book.changes.append((BASE + i * 20, "bid" if i % 2 else "ask", Decimal("99.00"), Decimal("0.01")))
        self.books[symbol] = book
        frame = json.dumps(book.features(BASE + 120000))
        self.frames[symbol] = deque((json.loads(frame) for _ in range(120)), maxlen=120)
        self.liquidations[symbol] = deque(
            (
                dict(event_ms=BASE + i, price="100.00", quantity="1.000", liquidated_side="LONG")
                for i in range(1000)
            ),
            maxlen=1000,
        )
        self.spreads[symbol] = [
            dict(bucket=i, known_ms=BASE + i * 60000, event_ms=BASE + i * 60000, bps=1.0) for i in range(31)
        ]
        bars = [
            Candle(
                BASE - i * 60000,
                60000,
                100 + i / 100,
                101 + i / 100,
                99 + i / 100,
                100 + i / 100,
                1000 + i / 10,
                100000 + i,
            )
            for i in range(600)
        ]
        self.history[symbol] = bars
        self.context[symbol] = dict(
            h4=bars[:200],
            h1=bars[200:400],
            m15=bars[400:],
            derivatives=dict(oi=[dict(at=BASE + i, value=100000 + i) for i in range(288)]),
        )

    def evaluate(self):
        for symbol in self.symbols[:3]:
            tape = self.tapes[symbol]
            end = tape.last_event // 60000 * 60000
            flow = orderflow.footprint(tape.window(end - 60000, end), Decimal("0.01"), 1.0)
            features = tape.event_features.snapshot(end - 60000, end, tape.last_receipt, True)
            self.fingerprint.update(json.dumps([flow, features], sort_keys=True).encode())

    def run(self):
        self.mark("cold")
        for symbol in self.symbols:
            self.fill(symbol)
            self.market_state(symbol)
        self.mark("warm")
        for cycle in range(3):
            temporary = [
                [
                    Candle(
                        BASE - i * 86400000,
                        86400000,
                        100 + i / 100,
                        101 + i / 100,
                        99 + i / 100,
                        100 + i / 100,
                        1000 + i / 10,
                        100000 + i,
                    )
                    for i in range(120)
                ]
                for _ in range(self.args.broad_symbols)
            ]
            self.evaluate()
            del temporary
            for symbol in self.symbols:
                self.fill(symbol, serial=self.args.trades + cycle * 5000, count=5000)
            self.mark(f"scan-{cycle + 1}")
        collectible = []
        removed_count = max(1, self.args.symbols // 3)
        for cycle in range(2):
            removed = self.symbols[1 : removed_count + 1]
            for symbol in removed:
                collectible.append(weakref.ref(self.tapes[symbol]))
                for mapping in (
                    self.tapes,
                    self.books,
                    self.frames,
                    self.liquidations,
                    self.spreads,
                    self.history,
                    self.context,
                ):
                    mapping.pop(symbol, None)
            new = [f"ROT{cycle}{i:03}USDT" for i in range(removed_count)]
            self.symbols[1 : removed_count + 1] = new
            for symbol in new:
                self.fill(symbol)
                self.market_state(symbol)
            gc.collect()
            assert all(ref() is None for ref in collectible)
            self.mark(f"rotation-{cycle + 1}")
        self.evaluate()
        for symbol, tape in self.tapes.items():
            self.fingerprint.update(symbol.encode())
            for trade in tape.trades:
                self.fingerprint.update(repr(tuple(getattr(trade, f.name) for f in fields(trade))).encode())
            self.fingerprint.update(
                repr((list(tape.ids), tape.coverage_start, tape.last_event, tape.last_receipt)).encode()
            )
        gc.collect()
        self.mark("post-gc")
        return dict(
            stages=self.stages,
            semantic_digest=self.fingerprint.hexdigest(),
            removed_tapes_collected=len(collectible),
            retained_trade_count=sum(len(t.trades) for t in self.tapes.values()),
            configured_markets=self.args.symbols,
            configured_tape_max=self.args.trades,
        )

    def components(self):
        symbol = self.symbols[0]
        self.fill(symbol)
        self.market_state(symbol)
        seen = set()
        parts = {}
        # Accounting order prevents duplicate ownership between shared containers.
        for name, value in (
            ("tape_trades", self.tapes[symbol].trades),
            ("id_index", self.tapes[symbol].ids),
            ("event_features", self.tapes[symbol].event_features),
            ("book", self.books[symbol]),
            ("frames", self.frames[symbol]),
            ("liquidations", self.liquidations[symbol]),
            ("candles", self.history[symbol]),
            ("context_derivatives", self.context[symbol]),
            ("spread_history", self.spreads[symbol]),
        ):
            parts[name] = deep_bytes(value, seen)
        return dict(
            parts_bytes=parts, attributed_bytes=sum(parts.values()), configured_tape_max=self.args.trades
        )


def readiness(args):
    from bybit_flow.ml import SCHEMA_VERSION
    from bybit_flow.ml.operations import trainability
    from bybit_flow.storage import Store

    populations = {}
    with tempfile.TemporaryDirectory() as directory:
        store = Store(Path(directory))
        snapshots = []
        labels = []
        for source in ("binance", "bybit", "okx"):
            for i in range(args.readiness_rows):
                ident = f"{source}-{i:08}"
                decision = BASE + i * 600000
                snapshot = dict(source=source, sequence=[0] * 16, signal=dict(symbol="TESTUSDT"))
                snapshots.append((ident, ident, "decision", decision, SCHEMA_VERSION, json.dumps(snapshot)))
                label = dict(
                    complete=True,
                    net_r=1 if i % 2 else -1,
                    exit_ms=decision + 60000,
                    classification="win" if i % 2 else "loss",
                    data_gaps=[],
                )
                labels.append((ident, "prints-v1", decision + 60000, json.dumps(label)))
        store.db.executemany("INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)", snapshots)
        store.db.executemany("INSERT INTO ml_labels VALUES(?,?,?,?)", labels)
        store.db.commit()
        del snapshots, labels

        def profile(frame, event, arg):
            if event == "return" and frame.f_code is trainability.__code__:
                for source, bucket in frame.f_locals["sources"].items():
                    populations[source] = {
                        key: len(bucket[key])
                        for key in ("identities", "unique", "current", "sequence", "planning_rows")
                    }

        queries = []
        store.db.set_trace_callback(
            lambda sql: (
                queries.append(sql) if sql.startswith("SELECT") and "ORDER BY s.decision_ms" in sql else None
            )
        )
        tracemalloc.start()
        sys.setprofile(profile)
        try:
            result = trainability(store, BASE + (args.readiness_rows + 1) * 600000)
        finally:
            sys.setprofile(None)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        plans = (
            [str(tuple(row)) for row in store.db.execute("EXPLAIN QUERY PLAN " + queries[0])]
            if queries
            else []
        )
        store.close()
    return dict(
        peak_traced_bytes=peak,
        populations=populations,
        queries=len(queries),
        temporary_sort=any("TEMP B-TREE" in row for row in plans),
        trainable={k: v["trainable_current_schema"] for k, v in result["by_source"].items()},
    )


def recovery(args):
    from bybit_flow import ml
    from bybit_flow.backtest import PaperPosition
    from bybit_flow.ml import print_recovery
    from bybit_flow.ml.labels import primary_outcome
    from bybit_flow.ml.print_recovery import recover_one
    from bybit_flow.ml.store import FeatureStore, digest
    from bybit_flow.storage import Store

    with tempfile.TemporaryDirectory() as directory:
        settings = Settings(_env_file=None, data_dir=Path(directory))
        store = Store(settings.data_dir)
        signal = Signal(
            id="memory-recovery",
            symbol="TESTUSDT",
            source="binance",
            family="trend_pullback",
            direction="LONG",
            created_ms=1000000,
            expires_ms=2000000,
            regime="trending up",
            entry=100,
            zone=(99, 101),
            stop=95,
            tp1=115,
            tp2=120,
            invalidation="fixture",
            reason="offline fixture",
            feature_schema_version=ml.SCHEMA_VERSION,
        )
        store.put(
            "primary_recorded_coverage",
            dict(
                receipt_ms=999000,
                trade_stale_ms=15000,
                markets={
                    "binance:TESTUSDT": dict(
                        subscribed=True,
                        coverage_start_ms=998000,
                        last_trade_ms=999000,
                        subscription_start_ms=998000,
                    )
                },
            ),
        )
        fs = FeatureStore(store)
        ident = fs.capture(signal, 1000000, "decision")
        snapshot = json.loads(
            store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0]
        )
        position = PaperPosition(
            signal, 10, fee_bps=settings.taker_fee_bps, slippage_bps=settings.slippage_bps
        )
        position.data_gaps.append("recording continuity lost")
        fs.label(ident, primary_outcome(snapshot, position, settings, "a" * 64), 1100000)

        class Page(list):
            pass

        class History:
            name = "bounded-offline-aggregate-fixture"

            def __init__(self):
                self.refs = []
                self.max_previous_pages = 0

            async def page(self, snapshot, state, metrics, asof):
                self.max_previous_pages = max(
                    self.max_previous_pages, sum(ref() is not None for ref in self.refs)
                )
                offset = state["trades"]
                page = Page(
                    dict(
                        a=i,
                        T=999999 if i == 0 else 1000500 + i * 20,
                        p="94" if i == 11999 else "100.00",
                        q="1000",
                        m=False,
                    )
                    for i in range(offset, min(offset + 1000, 12000))
                )
                self.refs.append(weakref.ref(page))
                metrics["requests"] += 1
                return dict(trades=page)

        history = History()
        original_clock = print_recovery.now_ms
        original_decode = print_recovery.trade_from_wire
        print_recovery.now_ms = lambda: 20000000
        if args.baseline_dir:
            print_recovery.trade_from_wire = original(args.baseline_dir, "exchanges")["trade_from_wire"]

        async def run():
            first = await recover_one(store, settings, ident, history, dict(requests=0), 6)
            checkpoint = store.get("primary_print_recovery:" + ident)
            second = await recover_one(store, settings, ident, history, dict(requests=0), 6)
            return first, second, checkpoint

        tracemalloc.start()
        try:
            first, second, checkpoint = asyncio.run(run())
        finally:
            print_recovery.now_ms = original_clock
            print_recovery.trade_from_wire = original_decode
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        label = json.loads(
            store.db.execute("SELECT payload FROM ml_labels WHERE policy='prints-recovered-v1'").fetchone()[0]
        )
        gc.collect()
        result = dict(
            first=first,
            second=second,
            peak_traced_bytes=peak,
            pages=len(history.refs),
            pages_still_live=sum(ref() is not None for ref in history.refs),
            max_previous_pages_at_fetch=history.max_previous_pages,
            checkpoint_bytes=deep_bytes(checkpoint),
            position_bytes=deep_bytes(checkpoint["position"]),
            sample_page_bytes=deep_bytes(
                [dict(a=i, T=1000000 + i * 20, p="100.00", q="1000", m=False) for i in range(1000)]
            ),
            checkpoint_digest=digest(checkpoint),
            outcome_digest=digest(label),
            classification=label["classification"],
            certificate=label["certificate"]["status"],
        )
        store.close()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("full", "components", "readiness", "recovery"), default="full")
    parser.add_argument("--symbols", type=int, default=8)
    parser.add_argument("--trades", type=int, default=50000)
    parser.add_argument("--broad-symbols", type=int, default=525)
    parser.add_argument("--readiness-rows", type=int, default=10000)
    parser.add_argument("--sample", type=Path)
    parser.add_argument("--baseline-dir", type=Path)
    args = parser.parse_args()
    if not 1 <= args.symbols <= 30 or not 1 <= args.trades <= 200000 or not 1 <= args.readiness_rows <= 20000:
        parser.error("Use 1-30 markets, 1-200000 trades and 1-20000 readiness rows")
    result = (
        readiness(args)
        if args.mode == "readiness"
        else recovery(args)
        if args.mode == "recovery"
        else Workload(args).components()
        if args.mode == "components"
        else Workload(args).run()
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
