"""Learn frozen setup paths recovered from exchange history after local downtime.

Full current decision features are retained. Price touches imply hypothetical
fills, never account executions. Prints labels and setup lifecycles stay intact.
"""

import json
import math

import httpx

from ..exchanges import VenueAPI
from ..storage import directory_bytes, now_ms
from . import SCHEMA_VERSION
from .bootstrap import MINUTE, CandleCache
from .bootstrap import evaluate as evaluate_candles
from .store import FeatureStore

POLICY = "monitored-ohlc-v1"
SOURCES = ("binance", "bybit", "okx")


def deadline(row):
    signal = row["signal"]
    return signal.get("holding_deadline_ms") or (
        signal["created_ms"] + signal.get("expected_hold_max", 240) * MINUTE
    )


def observed_entry(row, current):
    """Use only a causal original-venue live touch within the frozen entry window."""
    evidence = current.get("evidence", {})
    at, price = evidence.get("observed_entry_ms"), evidence.get("observed_entry_price")
    signal = row["signal"]
    entry_until = min(
        deadline(row), signal.get("trigger_expires_ms") or signal["expires_ms"], signal["expires_ms"]
    )
    if (
        evidence.get("observed_entry_method") == "LIVE_EXECUTED_TRADE"
        and evidence.get("observed_entry_source") == row["source"]
        and type(at) is int
        and row["decision_ms"] <= at < entry_until
        and isinstance(price, (int, float))
        and math.isfinite(price)
        and signal["zone"][0] <= price <= signal["zone"][1]
    ):
        return dict(at_ms=at, price=price)
    return None


def evaluate(row, current, bars, source, asof_ms, materialized_ms=None):
    entry = observed_entry(row, current)
    result = evaluate_candles(
        row,
        bars,
        source,
        asof_ms,
        materialized_ms,
        observed_entry=entry,
        until_ms=deadline(row),
    )
    return result | dict(
        policy=POLICY,
        feature_schema_version=row["schema_version"],
        signal_id=row["signal_id"],
        direction=row["signal"]["direction"],
        horizon_profile=row["signal"].get("horizon_profile", "LEGACY"),
        entry_basis="recorded-live-price-touch" if entry else "historical-zone-touch",
        account_fill_verified=False,
        label_fidelity="MONITORED_OHLC_PROXY",
        local_continuous_recording_required=False,
    )


def pending(store, source, asof_ms, limit=100):
    """Mature or terminal current decisions; canonical identities are sampled once."""
    first = {}
    for ident, identity in store.db.execute(
        "SELECT s.id,coalesce(c.candidate_identity,s.signal_id) FROM ml_snapshots s "
        "LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
        "WHERE s.stage='decision' ORDER BY s.decision_ms,s.id"
    ).fetchall():
        first.setdefault(identity, ident)
    canonical = set(first.values())
    result, cursor = [], None
    while len(result) < limit:
        page = " AND (s.decision_ms,s.id)>(?,?)" if cursor else ""
        rows = store.db.execute(
            "SELECT s.id,s.payload,p.payload,s.decision_ms FROM ml_snapshots s "
            "JOIN signals p ON p.id=s.signal_id WHERE s.stage='decision' AND s.schema_version IN ('candidate-v10',?) "
            "AND json_extract(s.payload,'$.source')=? AND s.decision_ms<? "
            "AND ((p.state IN ('INVALIDATED','EXPIRED','RESOLVED') AND "
            "json_extract(p.payload,'$.evidence.primary_outcome') IN ('STOP','TARGET','UNCLEAR')) OR "
            "coalesce(json_extract(s.payload,'$.signal.holding_deadline_ms'),"
            "json_extract(s.payload,'$.signal.created_ms')+"
            "coalesce(json_extract(s.payload,'$.signal.expected_hold_max'),240)*60000)<=?) "
            "AND NOT EXISTS(SELECT 1 FROM ml_labels l WHERE l.snapshot_id=s.id AND l.policy=?)"
            + page
            + " ORDER BY s.decision_ms,s.id LIMIT 100",
            [SCHEMA_VERSION, source, asof_ms, asof_ms, POLICY] + (list(cursor) if cursor else []),
        ).fetchall()
        if not rows:
            break
        for ident, payload, current, decision in rows:
            cursor = (decision, ident)
            if ident not in canonical:
                continue
            result.append(dict(id=ident, **json.loads(payload), current=json.loads(current)))
            if len(result) == limit:
                break
        if len(rows) < 100:
            break
    return result


async def backfill(store, settings, source, limit=100, market=None):
    if source not in SOURCES or not 1 <= limit <= 1000:
        raise ValueError("Supported source and limit 1..1000 required")
    from .locking import exclusive

    try:
        with exclusive(store.root / ("monitored-" + source + ".lock")):
            return await _backfill(store, settings, source, limit, market)
    except OSError as exc:
        if isinstance(exc, BlockingIOError) or getattr(exc, "winerror", None) in {33, 36}:
            return dict(status="DEFERRED_BUSY", source=source)
        raise


async def _backfill(store, settings, source, limit, market):
    key = f"ml_monitored_backfill:{source}:{POLICY}"
    asof = now_ms()
    state = dict(
        at_ms=asof,
        status="RUNNING",
        source=source,
        policy=POLICY,
        requests=0,
        cache_hits=0,
        cached_ranges=0,
        complete=0,
        excluded=0,
        pending=0,
    )
    if directory_bytes(store.root) >= settings.max_storage_gb * 1e9 * 0.95:
        state.update(status="STORAGE_BACKPRESSURE")
        store.put(key, state)
        return state
    rows = pending(store, source, asof, limit)
    cache = CandleCache(store, POLICY)
    cache.reclaim(asof)
    owned = market is None
    market = market or VenueAPI(source, settings.model_copy(update={"rest_requests_per_second": 1}))
    try:
        if market.name != source:
            raise ValueError("Recovery market does not match frozen venue")
        for row in rows:
            if directory_bytes(store.root) >= settings.max_storage_gb * 1e9 * 0.95:
                state.update(status="STORAGE_BACKPRESSURE")
                break
            entry = observed_entry(row, row["current"])
            start = (entry["at_ms"] if entry else row["decision_ms"]) // MINUTE * MINUTE
            end = min(asof, deadline(row)) // MINUTE * MINUTE
            terminal = row["current"].get("evidence", {}).get("terminal_event", {}).get("effective_ms")
            if (
                row["current"].get("evidence", {}).get("primary_outcome") in {"STOP", "TARGET", "UNCLEAR"}
                and type(terminal) is int
                and terminal > row["decision_ms"]
            ):
                end = min(end, (terminal + MINUTE - 1) // MINUTE * MINUTE)
            if end <= start:
                state["pending"] += 1
                continue
            bars = await cache.get(market, row["signal"]["symbol"], start, end, state, require_complete=True)
            outcome = evaluate(row, row["current"], bars, source, asof, now_ms())
            if outcome["outcome"] == "INCOMPLETE":
                # Public history outages and active paths are retried, not frozen.
                state["pending"] += 1
                continue
            available = max(outcome["event_available_ms"], outcome["materialized_ms"])
            FeatureStore(store).label(row["id"], outcome, available)
            state["complete" if outcome["complete"] else "excluded"] += 1
            state["last_snapshot_id"] = row["id"]
            store.put(key, state)
        if state["status"] == "RUNNING":
            state["status"] = "OBSERVED"
    except (ValueError, PermissionError, RuntimeError, httpx.HTTPError) as exc:
        state.update(status="DEFERRED", reason=type(exc).__name__ + ": " + str(exc))
    finally:
        if owned:
            await market.close()
    state["at_ms"] = now_ms()
    store.put(key, state)
    return state


def dataset(store, source, asof_ms=None, *, schema=SCHEMA_VERSION):
    return FeatureStore(store).dataset(
        asof_ms or now_ms(), policy=POLICY, source=source, schema_version=schema
    )


def readiness(store, asof_ms=None):
    from .operations import cache_partition_feasibility

    asof = asof_ms or now_ms()
    result = {}
    for source in SOURCES:
        rows = dataset(store, source, asof)
        count = len(rows)
        detail = dict(
            trainable=count,
            minimum_required=500,
            ready=count >= 500,
            sequence_ready_16=sum(len(r.get("sequence", [])) == 16 for r in rows),
            schema=SCHEMA_VERSION,
            label_policy=POLICY,
        )
        if count >= 500:
            detail["partition_feasibility"] = cache_partition_feasibility(
                store, rows, "monitored", source, SCHEMA_VERSION, POLICY, asof
            )
            detail["model_fit_feasibility"] = store.get(
                f"ml_model_fit_feasibility:monitored:{source}:{SCHEMA_VERSION}:{POLICY}"
            )
        result[source] = detail
    return result


def train(store, source, two_stage=False):
    from .stacking import KINDS
    from .training import train as fit

    rows = dataset(store, source)
    path = FeatureStore(store).write_dataset(rows)
    kinds = (
        KINDS
        if two_stage and sum(len(r.get("sequence", [])) == 16 for r in rows) >= 500
        else ("logistic", "lightgbm")
    )
    return fit(store, path, kinds=kinds, track="monitored")
