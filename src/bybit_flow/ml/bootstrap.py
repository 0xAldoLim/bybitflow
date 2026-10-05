"""Bounded original-venue candle research. Never relaxes prints or live gates."""

import asyncio
import hashlib
import json
import math
from collections import Counter
from dataclasses import asdict

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from ..exchanges import VenueAPI
from ..models import Candle
from ..storage import directory_bytes, now_ms
from .bootstrap_features import SCHEMA, SUPPORTED, project
from .store import FeatureStore, digest

POLICY = "ohlc-path-v1"
MINUTE = 60_000
MINIMUM = 500
MAX_CACHE = 128_000_000


def deadline(row):
    signal = row["signal"]
    return row["decision_ms"] + signal.get("expected_hold_max", 240) * MINUTE


def evaluate(row, bars, source, asof_ms, materialized_ms=None):
    """Close-time availability; uncertainty never becomes a manufactured loss."""
    if source != row["source"]:
        raise ValueError("OHLC labels require the frozen original venue")
    s, decision, until = row["signal"], row["decision_ms"], deadline(row)
    result = dict(
        policy=POLICY,
        data_kind="original-venue-public-1m-OHLC",
        source=source,
        execution_fidelity="proxy",
        costs_verified=False,
        production_execution_verified=False,
        complete=False,
        net_r=None,
        exit_ms=None,
        materialized_ms=materialized_ms if materialized_ms is not None else now_ms(),
        event_available_ms=decision,
        entry_policy="Frozen entry/zone; full candle open in zone or zone intersection. Partial decision candle cannot establish ordering.",
    )

    def finish(outcome, at, reason, price=None):
        value = result | dict(
            outcome=outcome, classification="incomplete", reason=reason, event_available_ms=at
        )
        if price is not None:
            net = (sign * (price - result.get("proxy_entry_price", entry)) - cost) / distance
            if math.isfinite(net):
                value.update(
                    complete=True,
                    net_r=net,
                    exit_ms=at,
                    exit_price=price,
                    classification="win" if net > 0 else "loss" if net < 0 else "breakeven",
                )
        return value

    try:
        entry, stop, target = (float(s[k]) for k in ("entry", "stop", "tp1"))
        lo, hi = sorted(map(float, s["zone"]))
        sign = 1 if s["direction"] == "LONG" else -1
        cost = float(s["risk"]["cost_per_base"])
        distance = abs(entry - stop)
        if (
            not all(math.isfinite(x) for x in (entry, stop, target, lo, hi, cost))
            or cost < 0
            or distance <= 0
        ):
            raise ValueError("Invalid frozen plan/cost")
        if not sign * (entry - stop) > 0 or not sign * (target - entry) > 0 or not lo <= entry <= hi:
            raise ValueError("Invalid direction/levels")
    except (KeyError, TypeError, ValueError):
        return finish("INCOMPLETE", decision, "Frozen plan or cost assumptions unavailable")
    result["cost_assumptions"] = dict(
        cost_per_base=cost, method="Frozen planned fee, slippage and funding reserve; not verified costs"
    )
    cutoff = min(until, asof_ms) // MINUTE * MINUTE
    expected = decision // MINUTE * MINUTE
    entered, partial_entry, last = False, False, None
    entry_until = min(until, s.get("trigger_expires_ms") or s["expires_ms"], s["expires_ms"])
    unique = {}
    for bar in bars:
        if (
            bar.interval != MINUTE
            or bar.start % MINUTE
            or not all(math.isfinite(x) and x > 0 for x in (bar.open, bar.high, bar.low, bar.close))
        ):
            return finish("INCOMPLETE", decision, "Invalid one-minute candle")
        if not bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high:
            return finish("INCOMPLETE", decision, "Invalid candle bounds")
        if bar.start in unique and unique[bar.start] != bar:
            return finish("INCOMPLETE", decision, "Conflicting duplicate candle")
        unique[bar.start] = bar
    for b in sorted(unique.values(), key=lambda b: b.start):
        if b.start < expected or b.end > cutoff:
            continue
        if b.start != expected:
            return finish("INCOMPLETE", max(decision, b.end), "Missing original-venue closed candle")
        expected = b.end
        touch = b.low <= hi and b.high >= lo
        sl = b.low <= stop if sign > 0 else b.high >= stop
        tp = b.high >= target if sign > 0 else b.low <= target
        if b.start < decision:
            partial_entry = touch
            if touch and (sl or tp):
                return finish("AMBIGUOUS", b.end, "Partial decision candle cannot establish entry/exit order")
            last = b
            continue
        if not entered:
            if b.start >= entry_until:
                return finish(
                    "AMBIGUOUS" if partial_entry else "NO_ENTRY",
                    b.end,
                    "No established entry during frozen entry window",
                )
            if not touch:
                if partial_entry and (sl or tp):
                    return finish("AMBIGUOUS", b.end, "Possible partial-candle entry changes the outcome")
                last = b
                continue
            if b.end > entry_until and not lo <= b.open <= hi:
                return finish("AMBIGUOUS", b.end, "Entry window ends inside an entry-touch candle")
            if (sl or tp) and not lo <= b.open <= hi:
                return finish("AMBIGUOUS", b.end, "Entry and exit touched in the same candle; order unknown")
            entered = True
            result["entry_available_ms"] = b.end
            result["proxy_entry_price"] = (
                b.open if lo <= b.open <= hi else min(b.high, hi) if sign > 0 else max(b.low, lo)
            )
            result["entry_fill_method"] = (
                "Candle open when inside zone; otherwise adverse edge of observed zone intersection. Planned risk denominator retained."
            )
        if sl and tp:
            return finish("AMBIGUOUS", b.end, "Stop and target touched in the same candle")
        if sl:
            price = min(stop, b.open) if sign > 0 else max(stop, b.open)
            return finish("STOP", b.end, "Stop reached before TP1", price)
        if tp:
            return finish("TARGET", b.end, "TP1 reached before stop", target)
        last = b
    if asof_ms < until:
        return finish("INCOMPLETE", max(decision, cutoff), "Frozen horizon not yet complete")
    if expected < cutoff or not last:
        return finish(
            "INCOMPLETE", max(decision, cutoff), "Closed-candle coverage does not reach the frozen deadline"
        )
    if not entered:
        return finish("AMBIGUOUS" if partial_entry else "NO_ENTRY", last.end, "No established entry")
    return finish("TIME_EXIT", last.end, "Frozen deadline; final fully closed candle", last.close)


def candidate_index(store, source, asof_ms):
    """Canonical economic opportunities, including rejected frozen decisions."""
    rows, seen, cursor = [], set(), None
    while True:
        page = " AND (s.decision_ms,s.id)>(?,?)" if cursor else ""
        batch = store.db.execute(
            "SELECT s.id,s.schema_version,coalesce(c.candidate_identity,s.signal_id),s.decision_ms,"
            "s.decision_ms+coalesce(json_extract(s.payload,'$.signal.expected_hold_max'),240)*60000,"
            "EXISTS(SELECT 1 FROM ml_labels l WHERE l.snapshot_id=s.id AND l.policy=?) "
            "FROM ml_snapshots s LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
            "WHERE s.stage='decision' AND json_extract(s.payload,'$.source')=?"
            + page
            + " ORDER BY s.decision_ms,s.id LIMIT 250",
            [POLICY, source] + (list(cursor) if cursor else []),
        ).fetchall()
        if not batch:
            break
        for ident, schema, identity, decision, until, labelled in batch:
            cursor = (decision, ident)
            if identity in seen:
                continue
            seen.add(identity)
            if schema not in SUPPORTED or until > asof_ms:
                continue
            rows.append(
                dict(
                    id=ident,
                    candidate_identity=identity,
                    decision_ms=decision,
                    deadline_ms=until,
                    already_labelled=bool(labelled),
                )
            )
        if len(batch) < 250:
            break
    return rows


def candidates(store, source, asof_ms, limit=1000, pending_only=True):
    indexed = [
        r for r in candidate_index(store, source, asof_ms) if not pending_only or not r["already_labelled"]
    ][:limit]
    return [
        json.loads(store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (r["id"],)).fetchone()[0])
        | r
        for r in indexed
    ]


def merged_ranges(rows):
    groups = {}
    for row in rows:
        key = row["source"], row["signal"]["symbol"]
        groups.setdefault(key, []).append(
            (row["decision_ms"] // MINUTE * MINUTE, deadline(row) // MINUTE * MINUTE)
        )
    result = []
    for (source, symbol), ranges in sorted(groups.items()):
        merged = []
        for start, end in sorted(ranges):
            if merged and start <= merged[-1][1]:
                merged[-1][1] = max(end, merged[-1][1])
            else:
                merged.append([start, end])
        result.extend((source, symbol, a, b) for a, b in merged)
    return result


def plan(store, source, limit=100, asof_ms=None):
    asof = asof_ms or now_ms()
    rows = candidate_index(store, source, asof)
    pending = candidates(store, source, asof, limit)
    return dict(
        source=source,
        unique_candidates=len(rows),
        already_labelled=sum(r["already_labelled"] for r in rows),
        eligible=len(pending),
        dates=[
            min((r["decision_ms"] for r in pending), default=None),
            max((deadline(r) for r in pending), default=None),
        ],
        symbols=sorted({r["signal"]["symbol"] for r in pending}),
        merged_ranges=merged_ranges(pending),
    )


class CandleCache:
    def __init__(self, store):
        self.store = store
        self.root = store.root / "ml" / "bootstrap-candles"
        self.root.mkdir(parents=True, exist_ok=True)

    def reclaim(self, at_ms):
        files = sorted(self.root.glob("*.parquet"), key=lambda p: p.stat().st_mtime)
        used = directory_bytes(self.root)
        for path in files:
            if used <= MAX_CACHE and at_ms - path.stat().st_mtime * 1000 < 6 * 3_600_000:
                continue
            used -= path.stat().st_size
            path.unlink()

    async def get(self, market, symbol, start, end, state):
        key = digest([market.name, symbol, start, end, POLICY])
        path = self.root / (key + ".parquet")
        meta = self.store.get("ml_bootstrap_cache:" + key, {})
        if path.exists() and hashlib.sha256(path.read_bytes()).hexdigest() == meta.get("sha256"):
            state["cache_hits"] += 1
            return [Candle(**r) for r in pq.read_table(path).to_pylist()]
        # Small requests and one request per second are independent of live REST.
        result, cursor = {}, start
        while cursor < end:
            if state["requests"] >= 120:
                raise RuntimeError("Bounded bootstrap REST budget exhausted; resume next cycle")
            state["requests"] += 1
            await asyncio.sleep(1)
            bars = None
            for attempt in range(3):
                try:
                    bars = await market.candles(
                        symbol, "1", min(end, cursor + 480 * MINUTE), limit=480, start=cursor
                    )
                    break
                except httpx.TransportError:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(2**attempt)
            if not bars:
                break
            for bar in bars:
                if cursor <= bar.start < end and bar.end <= end:
                    result[bar.start] = bar
            following = max(b.end for b in bars)
            if following <= cursor:
                raise ValueError("Bootstrap candle pagination made no progress")
            cursor = following
        bars = sorted(result.values(), key=lambda b: b.start)
        if bars:
            temporary = path.with_suffix(".tmp")
            pq.write_table(pa.Table.from_pylist([asdict(b) for b in bars]), temporary, compression="zstd")
            temporary.replace(path)
            self.store.put(
                "ml_bootstrap_cache:" + key,
                dict(
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    source=market.name,
                    symbol=symbol,
                    start_ms=start,
                    end_ms=end,
                ),
            )
            state["cached_ranges"] += 1
        return bars


async def backfill(store, settings, source, limit=100, resume=True, dry_run=False, market=None):
    if source not in {"binance", "bybit", "okx"} or not 1 <= limit <= 1000:
        raise ValueError("Supported source and limit 1..1000 required")
    if dry_run:
        return plan(store, source, limit)
    from .locking import exclusive

    try:
        with exclusive(store.root / ("bootstrap-" + source + ".lock")):
            return await _backfill(store, settings, source, limit, resume, market)
    except OSError as exc:
        if isinstance(exc, BlockingIOError) or getattr(exc, "winerror", None) in {33, 36}:
            return dict(
                status="DEFERRED_BUSY", source=source, reason="Bootstrap already running for this source"
            )
        raise


async def _backfill(store, settings, source, limit, resume, market):
    key = "ml_bootstrap_backfill:" + source + ":" + POLICY
    previous = store.get(key, {}) if resume else {}
    state = dict(
        previous,
        source=source,
        policy=POLICY,
        at_ms=now_ms(),
        status="RUNNING",
        requests=0,
        cache_hits=0,
        cached_ranges=0,
    )
    if directory_bytes(store.root) >= settings.max_storage_gb * 1e9 * 0.95:
        state.update(
            status="STORAGE_BACKPRESSURE",
            reason="Recorder and safe pruning have priority; bootstrap deferred",
        )
        store.put(key, state)
        return state
    asof = now_ms()
    rows = candidates(store, source, asof, limit)
    ranges = merged_ranges(rows)
    cache = CandleCache(store)
    cache.reclaim(asof)
    owned = market is None
    market = market or VenueAPI(source, settings.model_copy(update={"rest_requests_per_second": 1}))
    try:
        for venue, symbol, start, end in ranges:
            if market.name != venue:
                raise ValueError("Backfill market does not match frozen venue")
            if directory_bytes(store.root) >= settings.max_storage_gb * 1e9 * 0.95:
                state.update(status="STORAGE_BACKPRESSURE")
                break
            bars = await cache.get(market, symbol, start, end, state)
            for row in rows:
                if (
                    row["signal"]["symbol"] != symbol
                    or not start <= row["decision_ms"] < end
                    or deadline(row) > end
                ):
                    continue
                outcome = evaluate(row, bars, source, asof)
                FeatureStore(store).label(row["id"], outcome, outcome["event_available_ms"])
                state["candidates_seen"] = state.get("candidates_seen", 0) + 1
                category = "complete" if outcome["complete"] else outcome["outcome"].lower()
                state[category] = state.get(category, 0) + 1
                state["cursor"] = [row["decision_ms"], row["id"]]
            state["last_success_ms"] = now_ms()
            store.put(key, state)
        if state["status"] == "RUNNING":
            state["status"] = "OBSERVED"
    except (ValueError, PermissionError, RuntimeError, httpx.HTTPError) as exc:
        state.update(status="DEFERRED", reason=type(exc).__name__ + ": " + str(exc))
    finally:
        if owned:
            await market.close()
    state["at_ms"] = now_ms()
    state["total_requests"] = state.get("total_requests", 0) + state["requests"]
    store.put(key, state)
    return state


def dataset(store, source, asof_ms=None):
    asof = asof_ms or now_ms()
    if not store.db.execute("SELECT 1 FROM ml_labels WHERE policy=? LIMIT 1", (POLICY,)).fetchone():
        return []
    canonical_ids = {r["id"] for r in candidate_index(store, source, asof)}
    result = []
    cursor = None
    while len(result) < 10_000:
        page = " AND (s.decision_ms,s.id)<(?,?)" if cursor else ""
        batch = store.db.execute(
            "SELECT s.id,s.payload,l.available_ms,l.payload,s.decision_ms FROM ml_snapshots s JOIN ml_labels l ON l.snapshot_id=s.id "
            "WHERE s.stage='decision' AND l.policy=? AND json_extract(s.payload,'$.source')=? "
            "AND json_extract(l.payload,'$.complete')=1 AND l.available_ms<=?"
            + page
            + " ORDER BY s.decision_ms DESC,s.id DESC LIMIT 50",
            [POLICY, source, asof] + (list(cursor) if cursor else []),
        ).fetchall()
        if not batch:
            break
        for ident, payload, available, label, decision in batch:
            cursor = (decision, ident)
            if ident not in canonical_ids:
                continue
            row = json.loads(payload) | dict(id=ident, label=json.loads(label), label_available_ms=available)
            outcome = row["label"]
            if (
                not isinstance(outcome.get("net_r"), (int, float))
                or not math.isfinite(outcome["net_r"])
                or not decision < (outcome.get("exit_ms") or 0) <= available
            ):
                continue
            try:
                result.append(project(row))
            except ValueError:
                continue
            if len(result) >= 10_000:
                break
        if len(batch) < 50:
            break
    return list(reversed(result))


def readiness(store, asof_ms=None):
    result = {}
    asof = asof_ms or now_ms()
    for source in ("binance", "bybit", "okx"):
        counts = Counter()
        for row in store.db.execute(
            "SELECT json_extract(l.payload,'$.outcome'),count(*) FROM ml_labels l JOIN ml_snapshots s ON s.id=l.snapshot_id "
            "WHERE l.policy=? AND json_extract(s.payload,'$.source')=? GROUP BY 1",
            (POLICY, source),
        ).fetchall():
            counts[row[0]] = row[1]
        count = len(dataset(store, source, asof_ms))
        complete_unique = store.db.execute(
            "SELECT count(DISTINCT coalesce(c.candidate_identity,s.signal_id)) FROM ml_labels l "
            "JOIN ml_snapshots s ON s.id=l.snapshot_id LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
            "WHERE s.stage='decision' AND l.policy=? AND json_extract(s.payload,'$.source')=? "
            "AND json_extract(l.payload,'$.complete')=1 AND l.available_ms<=?",
            (POLICY, source, asof),
        ).fetchone()[0]
        result[source] = dict(
            outcomes=dict(counts),
            complete_unique=complete_unique,
            raw_complete_labels=sum(counts[k] for k in ("TARGET", "STOP", "TIME_EXIT")),
            excluded_from_training=max(0, complete_unique - count),
            training_window_limit=10_000,
            minimum_required=MINIMUM,
            trainable=count,
            ready=count >= MINIMUM,
            schema=SCHEMA,
            label_policy=POLICY,
        )
    return result


def export(store, source):
    rows = dataset(store, source)
    if not rows:
        raise ValueError("No complete unique bootstrap outcomes")
    return FeatureStore(store).write_dataset(rows)


def train(store, source):
    from .training import train as fit

    if len(dataset(store, source)) < MINIMUM:
        raise ValueError("Bootstrap requires >=500 complete unique finite causal outcomes")
    return fit(store, export(store, source), kinds=("logistic", "lightgbm"), track="bootstrap")
