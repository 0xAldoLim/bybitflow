"""Certified trade-history recovery of unchanged prints-v1 hypothetical outcomes."""

import json
import math
import time
from decimal import Decimal, InvalidOperation

import httpx

from ..backtest import PaperPosition
from ..exchanges import symbol_id, trade_from_wire
from ..models import Signal
from ..storage import directory_bytes, now_ms
from . import SCHEMA_VERSION
from .labels import primary_outcome
from .store import FeatureStore, digest

POLICY = "prints-recovered-v1"
SIMULATION = "prints-v1:latency500:participation0.01:fill60000:whole-tp1-stop-horizon"


def validate_recovered(row):
    label = row["label"]
    proof = dict(label.get("certificate", {}))
    signature = proof.pop("sha256", None)
    value = {k: v for k, v in label.items() if k != "certificate"}
    snapshot = {
        k: v for k, v in row.items() if k not in {"id", "candidate_identity", "label", "label_available_ms"}
    }
    if (
        label.get("policy") != POLICY
        or label.get("outcome_policy") != "prints-v1"
        or label.get("data_kind") != "verified-original-venue-public-prints"
        or proof.get("status") != "VERIFIED"
        or signature != digest(proof)
        or proof.get("outcome_sha256") != digest(value)
        or proof.get("snapshot_id") != row["id"]
        or digest(snapshot) != row["id"]
        or proof.get("source") != row["source"]
        or row["source"] != "binance"
        or proof.get("simulation") != SIMULATION
        or not label.get("complete")
        or label.get("feature_schema_version") != row["schema_version"]
        or label.get("source_methodology") != row["source"]
        or label.get("data_gaps") != []
        or proof.get("trades", 0) <= 0
        or proof.get("first_id", -1) < 0
        or not isinstance(label.get("net_r"), (int, float))
        or not math.isfinite(label["net_r"])
        or proof.get("first_event_ms", math.inf) > row["decision_ms"]
        or proof.get("last_event_ms") != label.get("exit_ms")
        or proof.get("last_id", -1) - proof.get("first_id", 0) + 1 != proof.get("trades", 0)
        or not row["decision_ms"]
        < label["exit_ms"]
        <= proof.get("materialized_ms", -1)
        <= row["label_available_ms"]
        or label.get("costs_verified") is not False
    ):
        raise ValueError("Invalid recovered primary trade-history certificate")


def pending(store, asof, limit=3):
    """One current-schema opportunity per identity, using bounded scalar pages."""
    seen, candidates, cursor = set(), [], None
    while True:
        page = " AND (s.decision_ms,s.id)>(?,?)" if cursor else ""
        rows = store.db.execute(
            "SELECT s.decision_ms,s.id,coalesce(c.candidate_identity,s.signal_id),"
            "json_extract(s.payload,'$.source'),"
            "json_extract(s.payload,'$.primary_ml_eligibility.eligible'),"
            "json_extract(l.payload,'$.classification'),json_extract(l.payload,'$.quantity'),"
            "EXISTS(SELECT 1 FROM ml_labels r WHERE r.snapshot_id=s.id AND r.policy=?),"
            "k.payload FROM ml_snapshots s INDEXED BY ml_snapshot_stage_time "
            "LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
            "LEFT JOIN ml_labels l ON l.snapshot_id=s.id AND l.policy='prints-v1' "
            "LEFT JOIN kv k ON k.key='primary_print_recovery_attempt:'||s.id "
            "WHERE s.stage='decision' AND s.schema_version=? AND s.decision_ms<?"
            + page
            + " ORDER BY s.decision_ms,s.id LIMIT 500",
            [POLICY, SCHEMA_VERSION, asof] + (list(cursor) if cursor else []),
        ).fetchall()
        if not rows:
            break
        for decision, ident, identity, source, eligible, kind, quantity, recovered, raw in rows:
            cursor = (decision, ident)
            if identity in seen:
                continue
            seen.add(identity)
            state = json.loads(raw) if raw else {}
            if (
                source != "binance"
                or eligible != 1
                or kind != "incomplete"
                or recovered
                or state.get("status") == "NO_FILL"
                or state.get("retry_after_ms", 0) > asof
            ):
                continue
            candidates.append((not bool(quantity), state.get("last_attempt_ms", 0), -decision, ident))
        if len(rows) < 500:
            break
    return [r[-1] for r in sorted(candidates)[:limit]]


def restore(snapshot, original, saved):
    signal = Signal.model_validate(snapshot["signal"])
    signal.created_ms = snapshot["decision_ms"]
    requested = original.get("requested_qty")
    costs = [original.get(k) for k in ("fees_bps", "slippage_bps", "funding_assumption_bps")]
    if (
        not isinstance(requested, (int, float))
        or not math.isfinite(requested)
        or requested <= 0
        or any(not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 for v in costs)
    ):
        raise ValueError("Frozen original prints-v1 quantity/cost assumptions unavailable")
    position = PaperPosition(
        signal,
        requested,
        fee_bps=costs[0],
        slippage_bps=costs[1],
        horizon_ms=signal.expected_hold_max * 60000,
    )
    state = dict(saved)
    if state:
        checksum = state.pop("checkpoint_sha256", None)
        if checksum != digest(state) or state.get("snapshot_sha256") != digest(snapshot):
            raise ValueError("Recovery checkpoint integrity mismatch")
        if state.get("original_label_sha256") != digest(original):
            raise ValueError("Original primary label changed")
        values = dict(state["position"])
        values["signal"] = Signal.model_validate(values["signal"])
        values["funding_timestamps"] = set(values["funding_timestamps"])
        position = PaperPosition(**values)
        if position.signal.model_dump(mode="json") != signal.model_dump(mode="json"):
            raise ValueError("Frozen recovery plan changed")
        if (
            position.requested_qty != requested
            or position.fee_bps != costs[0]
            or position.slippage_bps != costs[1]
            or position.horizon_ms != signal.expected_hold_max * 60000
            or position.latency_ms != 500
            or position.participation != 0.01
            or position.fill_window_ms != 60000
            or position.data_gaps
            or position.funding_timestamps
            or position.funding != 0
        ):
            raise ValueError("Recovery simulation assumptions changed")
    else:
        state = dict(
            status="PENDING",
            snapshot_sha256=digest(snapshot),
            original_label_sha256=digest(original),
            trades=0,
            pages=0,
            input_hash=digest([]),
            first_id=None,
            last_id=None,
            first_event_ms=None,
            last_event_ms=None,
        )
    return position, state, costs[2]


def save(store, ident, state, position):
    values = dict(vars(position))
    values["signal"] = position.signal.model_dump(mode="json")
    values["funding_timestamps"] = sorted(position.funding_timestamps)
    state = state | dict(position=values)
    store.put("primary_print_recovery:" + ident, state | dict(checkpoint_sha256=digest(state)))


async def recover_one(store, settings, ident, provider, metrics, page_budget=12, deadline=None):
    snapshot_row = store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()
    label_row = store.db.execute(
        "SELECT payload FROM ml_labels WHERE snapshot_id=? AND policy='prints-v1'", (ident,)
    ).fetchone()
    if not snapshot_row or not label_row:
        raise ValueError("Original snapshot or label missing")
    snapshot, original = json.loads(snapshot_row[0]), json.loads(label_row[0])
    existing = store.db.execute(
        "SELECT payload,available_ms FROM ml_labels WHERE snapshot_id=? AND policy=?",
        (ident, POLICY),
    ).fetchone()
    if existing:
        validate_recovered(
            dict(id=ident, **snapshot, label=json.loads(existing[0]), label_available_ms=existing[1])
        )
        return "VERIFIED"
    if (
        snapshot["schema_version"] != SCHEMA_VERSION
        or snapshot["source"] != "binance"
        or not snapshot.get("primary_ml_eligibility", {}).get("eligible")
        or original.get("classification") != "incomplete"
    ):
        return "INELIGIBLE"
    position, state, funding = restore(snapshot, original, store.get("primary_print_recovery:" + ident, {}))
    asof = now_ms()
    for _ in range(page_budget):
        if deadline is not None and time.monotonic() >= deadline:
            return "PENDING"
        from .cli import prioritize_live_recording

        prioritize_live_recording(store)
        if directory_bytes(store.root) >= settings.max_storage_gb * 1e9 * 0.95:
            return "STORAGE_BACKPRESSURE"
        response = await provider.page(snapshot, state, metrics, asof)
        records = response["trades"]
        if not records:
            if response.get("cursor"):
                state["archive"] = response["cursor"]
                save(store, ident, state, position)
                continue
            return "PENDING"
        consumed = []
        for raw in records:
            symbol_id(position.signal.symbol, "binance")
            try:
                ident_value, event = raw["a"], raw["T"]
                price, quantity = Decimal(raw["p"]), Decimal(raw["q"])
                valid = (
                    type(ident_value) is int
                    and ident_value >= 0
                    and type(event) is int
                    and event >= 0
                    and type(raw["m"]) is bool
                    and price.is_finite()
                    and price > 0
                    and quantity.is_finite()
                    and quantity > 0
                )
            except (InvalidOperation, TypeError, KeyError) as exc:
                raise ValueError("Invalid historical aggregate trade") from exc
            if not valid:
                raise ValueError("Invalid historical aggregate trade")
            if event > asof:
                break
            if state["last_id"] is not None and ident_value != state["last_id"] + 1:
                raise ValueError("Historical aggregate trade ID gap/duplicate")
            if state["last_event_ms"] is not None and event < state["last_event_ms"]:
                raise ValueError("Historical aggregate trade time reversed")
            if state["first_id"] is None:
                if event > snapshot["decision_ms"]:
                    raise ValueError("Historical predecision anchor missing")
                state.update(first_id=ident_value, first_event_ms=event)
            state.update(last_id=ident_value, last_event_ms=event)
            state["trades"] += 1
            if raw.get("_archive"):
                state["archive"] = raw["_archive"]
            consumed.append({k: raw[k] for k in ("a", "p", "q", "T", "m")})
            position.on_trade(trade_from_wire("binance", raw | dict(s=position.signal.symbol), asof))
            if (
                position.exit_ms is not None
                or event > position.signal.created_ms + position.horizon_ms + 60000
            ):
                break
        state["input_hash"] = digest([state["input_hash"], consumed])
        state["pages"] += 1
        if position.exit_ms is not None or (
            state["last_event_ms"] is not None
            and state["last_event_ms"] > position.signal.created_ms + position.horizon_ms + 60000
        ):
            costs = settings.model_copy(
                update={
                    "taker_fee_bps": original["fees_bps"],
                    "slippage_bps": original["slippage_bps"],
                    "funding_reserve_bps": funding,
                }
            )
            outcome = primary_outcome(snapshot, position, costs, state["input_hash"])
            if not outcome["complete"]:
                state["status"] = "NO_FILL"
                save(store, ident, state, position)
                return "NO_FILL"
            outcome.update(
                policy=POLICY, outcome_policy="prints-v1", data_kind="verified-original-venue-public-prints"
            )
            proof = dict(
                status="VERIFIED",
                source="binance",
                snapshot_id=ident,
                simulation=SIMULATION,
                original_label_sha256=digest(original),
                first_id=state["first_id"],
                last_id=state["last_id"],
                first_event_ms=state["first_event_ms"],
                last_event_ms=state["last_event_ms"],
                trades=state["trades"],
                input_hash=state["input_hash"],
                materialized_ms=now_ms(),
                outcome_sha256=digest(outcome),
                historical_source=provider.name,
            )
            outcome["certificate"] = proof | dict(sha256=digest(proof))
            available = max(now_ms(), position.exit_ms)
            validate_recovered(dict(id=ident, **snapshot, label=outcome, label_available_ms=available))
            FeatureStore(store).label(ident, outcome, available)
            state["status"] = "VERIFIED"
            save(store, ident, state, position)
            return "VERIFIED"
        save(store, ident, state, position)
        if not consumed or state["last_event_ms"] >= asof:
            break
    return "PENDING"


async def backfill(store, settings, limit=3, page_budget=12, provider=None):
    """Optional bounded work; never changes live setup lifecycle or Discord state."""
    from .locking import exclusive

    if not settings.ml_enabled or not settings.ml_print_recovery_enabled:
        return dict(status="DISABLED")
    from .trade_history import BinanceHistory

    owned = provider is None
    provider = provider or BinanceHistory(store, settings)
    metrics = dict(at_ms=now_ms(), status="OBSERVED", verified=0, pending=0, excluded=0, requests=0)
    deadline = time.monotonic() + 45
    try:
        with exclusive(store.root / "print-recovery.lock"):
            for ident in pending(store, now_ms(), limit):
                result = "DEFERRED"
                try:
                    result = await recover_one(
                        store, settings, ident, provider, metrics, page_budget, deadline
                    )
                    metrics[
                        "verified"
                        if result == "VERIFIED"
                        else "excluded"
                        if result in {"NO_FILL", "INELIGIBLE"}
                        else "pending"
                    ] += 1
                except (ValueError, OSError, httpx.HTTPError, PermissionError) as exc:
                    metrics.update(
                        status="DEFERRED", last_error_type=type(exc).__name__, reason=str(exc)[:250]
                    )
                store.put(
                    "primary_print_recovery_attempt:" + ident,
                    dict(
                        last_attempt_ms=now_ms(),
                        status=result,
                        retry_after_ms=now_ms() + (900000 if result == "DEFERRED" else 0),
                    ),
                )
                if metrics["requests"] >= page_budget or time.monotonic() >= deadline:
                    break
    except BlockingIOError:
        metrics["status"] = "DEFERRED_BUSY"
    finally:
        if owned:
            await provider.close()
    metrics["complete_recovered_labels"] = store.db.execute(
        "SELECT count(*) FROM ml_labels WHERE policy=? AND json_extract(payload,'$.complete')=1",
        (POLICY,),
    ).fetchone()[0]
    store.put("ml_print_recovery", metrics)
    return metrics
