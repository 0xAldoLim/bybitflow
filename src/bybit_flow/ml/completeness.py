"""Bounded, read-only prints-v1 forensics. No label or plan writes occur here."""

import gzip
import hashlib
import io
import json
import zipfile
from collections import Counter, OrderedDict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from ..backtest import PaperPosition
from ..models import Signal, Trade
from ..packing import manifest_for
from ..storage import now_ms
from . import SCHEMA_VERSION
from .checkpoint import consumed_cursor
from .coverage import market_source

ROOT_CLASSES = (
    "COMPLETE",
    "STILL_OPEN",
    "NO_ENTRY",
    "TECHNICAL_DUPLICATE",
    "PREDECISION_COVERAGE_MISSING",
    "SUBSCRIPTION_NOT_READY",
    "REAL_TRADE_FEED_GAP",
    "RECORDER_DROP",
    "RECORDER_CHAIN_GAP",
    "CLOCK_OR_ORDERING_DAMAGE",
    "RETENTION_LOSS",
    "MATERIALIZER_CHECKPOINT_GAP",
    "LABELER_FALSE_GAP",
    "SOURCE_MISMATCH",
    "HORIZON_NOT_MATURE",
    "UNKNOWN_INCOMPLETE",
)
HORIZONS = ("SHORT_INTRADAY", "CORE_INTRADAY", "SWING", "EXTENDED_SWING", "LEGACY")
SOURCES = ("binance", "bybit", "okx")
BOOKKEEPING_BUG_ID = "prints-v1-valid-raw-false-gap"


def continuity_rules(settings):
    return dict(
        policy="prints-v1",
        trade_stale_ms=settings.trade_stale_ms,
        stale_comparison="strictly greater than the limit; equal is accepted",
        subscription="acknowledged subscribed marker; removals/gaps end coverage; first retained print bootstraps only absent market history",
        segment_chain="previous id and sha256 must match; a normal chained 250 ms rotation is not a gap",
        receipt_order="nondecreasing within and between segments; equal receipt times accepted; overlapping intervals excluded",
        venue_event_order="receipt order drives availability; unique non-BT trade event time drives fills; event time may lead receipt by at most 1000 ms at normalization",
        boundaries="decision <= envelope receipt activates; entry event >= decision+500 ms and <= min(entry deadline, decision+60500 ms)",
        predecision_policy="no arbitrary warm-up; prospective diagnostics require subscription and a fresh committed valid print at or before decision",
        end_policy="whole-position TP1/stop/frozen horizon; late exit beyond maximum horizon excluded; 60000 ms materializer closure grace",
        depth="complete book-only recovery notices do not end executed-print coverage; incomplete envelopes still fail closed",
        historical_labels="immutable; reconstruction is read-only and cannot promote a training result",
    )


def select_candidates(store, source, limit, days, horizon, asof):
    where = ["s.stage='decision'", "s.schema_version=?", "s.decision_ms<=?"]
    args = [SCHEMA_VERSION, asof]
    if days is not None:
        where.append("s.decision_ms>=?")
        args.append(asof - days * 86_400_000)
    if source != "all":
        where.append("json_extract(s.payload,'$.source')=?")
        args.append(source)
    if horizon:
        where.append("coalesce(json_extract(s.payload,'$.signal.horizon_profile'),'LEGACY')=?")
        args.append(horizon)
    # Group metadata first. Fetch payloads only for a bounded set of identities.
    groups = store.db.execute(
        "SELECT coalesce(c.candidate_identity,s.signal_id),count(*),max(s.decision_ms) "
        "FROM ml_snapshots s LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id WHERE "
        + " AND ".join(where)
        + " GROUP BY 1 ORDER BY 3 DESC,1 LIMIT ?",
        args + [limit + 1],
    ).fetchall()
    truncated = len(groups) > limit
    result, duplicates = [], []
    for identity, count, _ in groups[:limit]:
        rows = store.db.execute(
            "SELECT s.id,s.payload,l.payload,l.available_ms FROM ml_snapshots s "
            "LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
            "LEFT JOIN ml_labels l ON l.snapshot_id=s.id AND l.policy='prints-v1' "
            "WHERE s.stage='decision' AND s.schema_version=? "
            "AND coalesce(c.candidate_identity,s.signal_id)=? ORDER BY s.decision_ms,s.id LIMIT 1001",
            (SCHEMA_VERSION, identity),
        ).fetchall()
        canonical = next(
            (r for r in rows if not r[2] or json.loads(r[2]).get("classification") != "technical_duplicate"),
            rows[0],
        )
        ident, payload, label, available = canonical
        snap = dict(id=ident, **json.loads(payload), candidate_identity=identity)
        if days is not None and snap["decision_ms"] < asof - days * 86_400_000:
            # Recent repetitions of a prior opportunity are not new decisions in this cohort.
            duplicates.extend(
                dict(candidate_id=r[0], candidate_identity=identity, audit_class="TECHNICAL_DUPLICATE")
                for r in rows
                if r[0] != ident
            )
            continue
        snap.update(
            label=json.loads(label) if label else None, label_available_ms=available, cohort_snapshots=count
        )
        result.append(snap)
        duplicates.extend(
            dict(
                candidate_id=r[0],
                candidate_identity=identity,
                signal_id=json.loads(r[1])["signal_id"],
                audit_class="TECHNICAL_DUPLICATE",
            )
            for r in rows
            if r[0] != ident
        )
    return result, duplicates, truncated


class EvidenceReader:
    """Shared compact segment cache with hard I/O/row budgets; no SQLite writes."""

    def __init__(self, store, markets, max_segments=1500, max_rows=3_000_000, max_bytes=384_000_000):
        self.store, self.markets = store, markets
        self.symbols = {symbol for _, symbol in markets}
        self.max_segments, self.max_rows, self.max_bytes = max_segments, max_rows, max_bytes
        self.segments = self.rows = self.bytes = 0
        self.cache = OrderedDict()
        self.packs = dict(store.db.execute("SELECT segment_id,archive FROM segment_packs").fetchall())
        self.exhausted = False
        # Prior worker exclusions remain useful even when safe cleanup removed raw files.
        self.exclusions = {}
        directory = store.root / "ml" / "recording-audits"
        paths = sorted(directory.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:1024]
        diagnostic_bytes = 0
        for path in paths:
            size = path.stat().st_size
            if diagnostic_bytes + size > 8_000_000:
                break
            encoded = path.read_text()
            diagnostic_bytes += size
            if path.stem != hashlib.sha256(encoded.encode()).hexdigest():
                continue  # Only the worker's content-addressed integrity evidence is trusted.
            for excluded in json.loads(encoded).get("excluded", []):
                self.exclusions[excluded["id"]] = excluded

    def manifests(self, start, end):
        rows = self.store.db.execute(
            "SELECT s.payload,k.payload FROM segments s LEFT JOIN kv k ON k.key='pruned_segment:'||s.id "
            "WHERE json_extract(s.payload,'$.max_receipt_ms')>=? "
            "AND json_extract(s.payload,'$.min_receipt_ms')<=? "
            "ORDER BY json_extract(s.payload,'$.min_receipt_ms'),s.id LIMIT 20001",
            (start, end),
        ).fetchall()
        return [(json.loads(r[0]), json.loads(r[1]) if r[1] else None) for r in rows[:20000]], len(
            rows
        ) > 20000

    def read(self, manifest, tombstone):
        ident = manifest["id"]
        if ident in self.cache:
            self.cache.move_to_end(ident)
            return self.cache[ident]
        value = dict(rows=[], valid=False, present=False, reason=None, tombstone=tombstone)
        path = Path(manifest["raw"])
        known = self.exclusions.get(ident)
        if known and (known.get("sha256"), known.get("start"), known.get("end")) != (
            manifest["sha256"],
            manifest["min_receipt_ms"],
            manifest["max_receipt_ms"],
        ):
            known = None
        if tombstone:
            value["reason"] = (
                known["reason"] if known else "raw safely released; original cause not reconstructable"
            )
        elif not path.exists() and ident not in self.packs:
            value["reason"] = "committed raw missing without a retention tombstone"
        elif self.segments >= self.max_segments or self.rows >= self.max_rows or self.bytes >= self.max_bytes:
            self.exhausted = True
            value["reason"] = "forensic decoding budget reached"
        else:
            try:
                if path.exists() and path.stat().st_size > self.max_bytes - self.bytes:
                    self.exhausted = True
                    value["reason"] = "forensic compressed-byte budget reached"
                    return value
                if path.exists():
                    data = path.read_bytes()
                    sidecar = manifest_for(path)
                else:
                    with zipfile.ZipFile(self.packs[ident]) as archive:
                        if archive.getinfo(path.name).file_size > self.max_bytes - self.bytes:
                            self.exhausted = True
                            value["reason"] = "forensic compressed-byte budget reached"
                            return value
                        data = archive.read(path.name)
                        sidecar = json.loads(archive.read(ident + ".manifest.json"))
                self.segments += 1
                self.bytes += len(data)
                value["present"] = True
                if self.bytes > self.max_bytes:
                    self.exhausted = True
                    value["reason"] = "forensic compressed-byte budget reached"
                    return value
                if hashlib.sha256(data).hexdigest() != manifest["sha256"] or sidecar != manifest:
                    value["reason"] = "raw hash or manifest integrity mismatch"
                    return value
                count, previous, low, high = 0, -1, None, None
                with gzip.open(io.BytesIO(data), "rt") as stream:
                    for line in stream:
                        row = json.loads(line)
                        at = row["receipt_ms"]
                        count += 1
                        self.rows += 1
                        if at < previous:
                            value["reason"] = "non-monotonic receipt time"
                        previous = at
                        low, high = at if low is None else min(low, at), at if high is None else max(high, at)
                        venue, topic = market_source(row["source"])
                        if (
                            (venue, row["symbol"]) in self.markets
                            or row["symbol"] == "ALL"
                            or (row["source"] == "control/gap" and row["symbol"] in self.symbols)
                        ):
                            if topic.startswith(("control/", "ws/publicTrade.")) or not row.get(
                                "complete", True
                            ):
                                value["rows"].append(row)
                        if self.rows >= self.max_rows:
                            self.exhausted = True
                            value["reason"] = "forensic decoded-row budget reached"
                            break
                if not self.exhausted and (low, high, count) != (
                    manifest["min_receipt_ms"],
                    manifest["max_receipt_ms"],
                    manifest["rows"],
                ):
                    value["reason"] = "raw bounds or row-count integrity mismatch"
                value["valid"] = value["reason"] is None
            except (OSError, ValueError, KeyError, EOFError, zipfile.BadZipFile) as exc:
                value["reason"] = "unreadable raw: " + type(exc).__name__
        self.cache[ident] = value
        while len(self.cache) > 128:
            self.cache.popitem(last=False)
        return value


def audit_candidate(store, settings, snapshot, reader, asof):
    signal = Signal.model_validate(snapshot["signal"])
    decision = snapshot["decision_ms"]
    signal.created_ms = decision
    label = snapshot["label"] or {}
    horizon_end = decision + signal.expected_hold_max * 60_000
    available = snapshot.get("label_available_ms")
    label_exit = label.get("exit_ms")
    # Coverage exclusions freeze early: inspect only the interval the label actually observed.
    end = min(asof, horizon_end + 60_000, max(decision, available or asof, label_exit or 0))
    start = max(0, decision - 900_000)
    checkpoint = store.get("primary_materialization", {})
    retention = store.get("recording_retention", {}).get("through_ms", 0)
    row = dict(
        candidate_id=snapshot["id"],
        candidate_identity=snapshot["candidate_identity"],
        signal_id=signal.id,
        source=signal.source,
        symbol=signal.symbol,
        horizon_profile=signal.horizon_profile,
        family=signal.family,
        direction=signal.direction,
        decision_ms=decision,
        entry_deadline_ms=signal.expires_ms,
        expected_horizon_end_ms=horizon_end,
        label_exit_ms=label_exit,
        label_present=bool(label),
        label_complete=bool(label.get("complete")),
        label_classification=label.get("classification"),
        label_reason=label.get("reason") or "; ".join(label.get("data_gaps", [])),
        first_recorded_trade_ms=None,
        last_recorded_trade_ms=None,
        coverage_start_ms=None,
        coverage_end_ms=None,
        predecision_coverage_ms=None,
        gap_count=0,
        largest_gap_ms=0,
        gap_total_ms=0,
        first_gap_start_ms=None,
        first_gap_end_ms=None,
        gap_family=None,
        subscription_start_ms=None,
        subscription_end_ms=None,
        selected_at_decision=None,
        subscribed_at_decision=None,
        first_valid_trade_at_decision=None,
        segment_count=0,
        segment_chain_valid=None,
        retention_through_ms=retention,
        primary_materialization_cursor_ms=checkpoint.get("cursor_ms"),
        checkpoint_at_ms=checkpoint.get("saved_ms"),
        raw_evidence_present=False,
        raw_evidence_expected=True,
        eligible_at_decision=None,
        eligibility_basis="historical raw reconstruction",
        mature=asof >= horizon_end or bool(label_exit and label_exit <= asof),
        reconstruction_complete=False,
        reconstruction_reason=None,
        reconstruction_exit_ms=None,
        audit_class="UNKNOWN_INCOMPLETE",
        audit_reason="Required evidence has not been verified",
    )
    prospective = snapshot.get("primary_ml_eligibility")
    # Empty early development captures have no prospective state; historical payloads stay immutable.
    if prospective:
        row.update(
            eligible_at_decision=prospective["eligible"],
            eligibility_basis=prospective.get("policy"),
            predecision_coverage_ms=prospective.get("predecision_coverage_ms"),
        )
    if snapshot["source"] != signal.source or label.get("source_methodology", signal.source) != signal.source:
        row.update(
            audit_class="SOURCE_MISMATCH", audit_reason="Frozen snapshot/plan/label venue identities disagree"
        )
        return row
    manifests, truncated = reader.manifests(start, end)
    row["segment_count"] = len(manifests)
    row["segment_chain_valid"] = bool(manifests) and not truncated
    position = PaperPosition(
        signal,
        settings.hypothetical_notional / signal.entry,
        fee_bps=settings.taker_fee_bps,
        slippage_bps=settings.slippage_bps,
        horizon_ms=signal.expected_hold_max * 60_000,
    )
    failures, uncertainties, seen = [], [], set()
    previous, last_trade, subscription, healthy_start = None, None, None, None
    decision_checked = False
    ready = False
    raw_valid = bool(manifests) and not truncated
    missing_required = False
    consumed = consumed_cursor(store, snapshot["id"], snapshot, asof) if not label else None

    def gap(kind, a, b, reason):
        if b >= decision and a <= end:
            failures.append((kind, max(decision, a), min(end, b), reason))

    def check_decision():
        nonlocal decision_checked, ready
        if decision_checked:
            return
        decision_checked = True
        ready = (
            subscription is not None
            and last_trade is not None
            and 0 <= decision - last_trade <= settings.trade_stale_ms
        )
        row.update(
            subscribed_at_decision=subscription is not None,
            first_valid_trade_at_decision=last_trade,
            predecision_coverage_ms=max(0, decision - healthy_start) if healthy_start is not None else 0,
        )
        if not prospective and not uncertainties:
            row["eligible_at_decision"] = ready

    for manifest, tombstone in manifests:
        a, b = manifest["min_receipt_ms"], manifest["max_receipt_ms"]
        if previous:
            chained = manifest.get("previous_segment") == {"id": previous["id"], "sha256": previous["sha256"]}
            if a < previous["max_receipt_ms"]:
                gap("CLOCK_OR_ORDERING_DAMAGE", a, b, "overlapping committed recording intervals")
                raw_valid = False
                subscription = healthy_start = None
            if not chained:
                row["segment_chain_valid"] = False
                gap("RECORDER_CHAIN_GAP", previous["max_receipt_ms"], a, "previous segment id/hash mismatch")
                raw_valid = False
                subscription = healthy_start = None
        previous = manifest
        value = reader.read(manifest, tombstone)
        row["raw_evidence_present"] |= value["present"]
        if not value["valid"]:
            raw_valid = False
            reason = value["reason"] or "raw not verified"
            if "non-monotonic" in reason or "overlap" in reason:
                gap("CLOCK_OR_ORDERING_DAMAGE", a, b, reason)
                subscription = healthy_start = None
            elif tombstone:
                # Loss requires proof of unconsumed evidence, not a safely pruned old label.
                if (
                    not label
                    and tombstone.get("at_ms", asof + 1) <= asof
                    and b >= decision
                    and (consumed is None or consumed < b)
                ):
                    gap(
                        "RETENTION_LOSS",
                        a,
                        b,
                        "committed evidence pruned while unresolved and not represented by a valid durable checkpoint",
                    )
                else:
                    uncertainties.append(reason)
                    missing_required |= b >= decision
            else:
                uncertainties.append(reason)
                missing_required |= b >= decision
            continue
        for event in value["rows"]:
            at = event["receipt_ms"]
            if at < start or at > end:
                continue
            if at >= decision:
                check_decision()
            venue, topic = market_source(event["source"])
            global_gap = event["source"] == "control/gap"
            if not global_gap and venue != signal.source:
                continue
            payload = json.loads(event["payload"]) if isinstance(event["payload"], str) else event["payload"]
            symbol = event["symbol"]
            matching = symbol in {signal.symbol, "ALL"}
            removed = topic == "control/rotation" and signal.symbol in payload.get("removed", [])
            if topic == "control/subscribed" and signal.symbol in payload.get("symbols", []):
                subscription = at
                healthy_start = last_trade = None
                row["subscription_start_ms"] = at
                row["selected_at_decision"] = True if at <= decision else row["selected_at_decision"]
            lost = (
                removed
                or topic == "control/source_change"
                or (matching and (topic == "control/gap" or not event.get("complete", True)))
            )
            if lost:
                a_gap, b_gap = payload.get("gap_start_ms", at), payload.get("gap_end_ms", at)
                kind = (
                    "RECORDER_DROP"
                    if payload.get("reason") == "RECORDER_BACKPRESSURE"
                    and (
                        payload.get("source") == signal.source
                        or event["source"].startswith("native/" + signal.source + "/")
                    )
                    else "REAL_TRADE_FEED_GAP"
                )
                gap(
                    kind,
                    a_gap,
                    b_gap,
                    str(payload.get("reason", "subscription removed or incomplete envelope")),
                )
                subscription = healthy_start = None
                row["subscription_end_ms"] = at
            if symbol != signal.symbol or not topic.startswith("ws/publicTrade."):
                continue
            if not event.get("complete", True):
                continue
            for raw in payload.get("data", []):
                if raw.get("BT") or raw["i"] in seen:
                    continue
                seen.add(raw["i"])
                if last_trade is not None and at - last_trade > settings.trade_stale_ms:
                    gap(
                        "REAL_TRADE_FEED_GAP",
                        last_trade,
                        at,
                        "no valid recorded print within the configured stale interval",
                    )
                    healthy_start = at
                if last_trade is None:
                    healthy_start = at
                    if retention and at < decision and subscription is None:
                        subscription = at  # Same first-retained-print bootstrap as current labeler.
                last_trade = at
                if row["first_recorded_trade_ms"] is None:
                    row["first_recorded_trade_ms"] = at
                row["last_recorded_trade_ms"] = at
                row["coverage_start_ms"] = healthy_start
                row["coverage_end_ms"] = at
                if at >= decision:
                    position.on_trade(
                        Trade(
                            signal.symbol,
                            int(raw["T"]),
                            at,
                            raw["i"],
                            raw["S"],
                            Decimal(raw["p"]),
                            Decimal(raw["v"]),
                            venue,
                        )
                    )
        # A valid completed paper exit needs no later raw evidence.
        if position.exit_ms is not None and raw_valid and ready and not failures:
            break
    check_decision()
    # A valid old complete label independently proves the old labeler's coverage contract.
    if label.get("complete") and label.get("exit_ms") and label.get("net_r") is not None:
        row.update(
            audit_class="COMPLETE",
            audit_reason="Immutable complete prints-v1 outcome",
            eligible_at_decision=prospective["eligible"] if prospective else True,
        )
    else:
        outcome = position.outcome()
        reconstructed = (
            raw_valid and ready and not failures and outcome["complete"] and outcome["exit_ms"] <= horizon_end
        )
        row.update(
            reconstruction_complete=bool(reconstructed),
            reconstruction_exit_ms=outcome["exit_ms"],
            reconstruction_reason=outcome["exit_reason"],
        )
        if prospective and not prospective["eligible"]:
            row.update(
                audit_class=prospective["reason"]
                if prospective["reason"] in ROOT_CLASSES
                else "PREDECISION_COVERAGE_MISSING",
                audit_reason="Prospective committed coverage was not ready; this is an ineligible decision, not a later pipeline failure",
            )
        elif (
            not label
            and checkpoint.get("cursor_ms", -1) >= decision
            and checkpoint.get("verified_boundary_ms", -1) > decision
            and consumed is None
        ):
            row.update(
                audit_class="MATERIALIZER_CHECKPOINT_GAP",
                audit_reason="Advanced cursor cannot durably represent this unresolved frozen position",
            )
        elif failures:
            # Prefer concrete interval damage over a derived generic readiness failure.
            priority = {
                name: i
                for i, name in enumerate(
                    (
                        "RETENTION_LOSS",
                        "RECORDER_DROP",
                        "CLOCK_OR_ORDERING_DAMAGE",
                        "RECORDER_CHAIN_GAP",
                        "REAL_TRADE_FEED_GAP",
                    )
                )
            }
            kind, _, _, reason = min(failures, key=lambda f: priority[f[0]])
            row.update(audit_class=kind, audit_reason=reason)
        elif not ready and not uncertainties and manifests:
            row.update(
                audit_class="SUBSCRIPTION_NOT_READY"
                if row["subscribed_at_decision"] is False
                else "PREDECISION_COVERAGE_MISSING",
                audit_reason="No acknowledged subscription or fresh valid committed print before decision",
            )
        elif reconstructed and label and not label.get("complete"):
            row.update(
                audit_class="LABELER_FALSE_GAP",
                audit_reason="Valid, ordered, chained raw evidence reconstructs a complete frozen outcome; stored incomplete label remains unchanged",
                bug_id=BOOKKEEPING_BUG_ID,
            )
        elif outcome["exit_reason"] == "missed_fill" and raw_valid and ready:
            row.update(
                audit_class="NO_ENTRY",
                audit_reason="Verified prints did not fill the frozen entry within its entry window",
            )
        elif not label and not row["mature"] and ready and raw_valid:
            row.update(
                audit_class="STILL_OPEN",
                audit_reason="Clean recorded position has not resolved or reached its frozen horizon",
            )
        elif not label and not row["mature"]:
            row.update(
                audit_class="HORIZON_NOT_MATURE",
                audit_reason="Frozen horizon is not mature; required coverage is not yet fully verifiable",
            )
        elif uncertainties or truncated or missing_required:
            row["audit_reason"] = (
                "; ".join(dict.fromkeys(uncertainties))
                or "Required interval exceeds the bounded manifest audit"
            )
        elif last_trade is not None and end - last_trade > settings.trade_stale_ms:
            row.update(
                audit_class="REAL_TRADE_FEED_GAP",
                audit_reason="Recorded trade interval ends before the required observation cutoff",
            )
            gap("REAL_TRADE_FEED_GAP", last_trade, end, row["audit_reason"])
    if failures:
        row.update(
            gap_count=len(failures),
            largest_gap_ms=max(b - a for _, a, b, _ in failures),
            gap_total_ms=sum(max(0, b - a) for _, a, b, _ in failures),
            gap_family=row["audit_class"],
            first_gap_start_ms=failures[0][1],
            first_gap_end_ms=failures[0][2],
        )
    return row


def aggregate(rows):
    mature = [r for r in rows if r["mature"]]
    eligible = [r for r in mature if r["eligible_at_decision"] is True]
    complete = sum(r["label_complete"] for r in eligible)
    rate = complete / len(eligible) if eligible else None
    failures = Counter(r["audit_class"] for r in eligible if not r["label_complete"])
    return dict(
        total_unique_candidates=len(rows),
        not_yet_mature=len(rows) - len(mature),
        mature_candidates=len(mature),
        eligible_at_decision=sum(r["eligible_at_decision"] is True for r in rows),
        ineligible_at_decision=sum(r["eligible_at_decision"] is False for r in rows),
        eligibility_unknown=sum(r["eligible_at_decision"] is None for r in rows),
        mature_eligible=len(eligible),
        complete=complete,
        incomplete_eligible=len(eligible) - complete,
        completion_rate_among_mature_eligible=rate,
        completion_rate=rate,
        status="INSUFFICIENT_COHORT"
        if len(eligible) < 30
        else "HEALTHY"
        if rate >= 0.8
        else "WATCH"
        if rate >= 0.6
        else "DEGRADED",
        dominant_incomplete_reason=failures.most_common(1)[0][0] if failures else None,
        root_cause_counts=dict(Counter(r["audit_class"] for r in rows)),
    )


def summarize(rows, duplicates, asof, epoch):
    by_source = {source: aggregate([r for r in rows if r["source"] == source]) for source in SOURCES}
    by_horizon = {
        horizon: aggregate([r for r in rows if r["horizon_profile"] == horizon]) for horizon in HORIZONS
    }
    eras = {
        name: aggregate([r for r in rows if epoch is not None and (r["decision_ms"] >= epoch) == post])
        for name, post in (("pre_maturity", False), ("post_maturity", True))
    }
    daily, weekly = {}, {}
    for row in rows:
        date = datetime.fromtimestamp(row["decision_ms"] / 1000, UTC).date()
        daily.setdefault(date.isoformat(), []).append(row)
        weekly.setdefault((date - timedelta(days=date.weekday())).isoformat(), []).append(row)
    issues = []
    for source, symbol in sorted({(r["source"], r["symbol"]) for r in rows}):
        detail = aggregate([r for r in rows if r["source"] == source and r["symbol"] == symbol])
        if detail["mature_eligible"] >= 10 and detail["incomplete_eligible"]:
            issues.append(dict(source=source, symbol=symbol, **detail))
    long_limited = any(by_horizon[h]["status"] == "DEGRADED" for h in ("SWING", "EXTENDED_SWING")) and any(
        by_horizon[h]["status"] == "HEALTHY" for h in ("SHORT_INTRADAY", "CORE_INTRADAY")
    )
    return dict(
        summary=aggregate(rows) | dict(duplicate_snapshots=len(duplicates), last_audit_ms=asof),
        root_causes=dict.fromkeys(ROOT_CLASSES, 0)
        | dict(Counter(r["audit_class"] for r in rows))
        | {"TECHNICAL_DUPLICATE": len(duplicates)},
        by_source=by_source,
        by_horizon=by_horizon,
        maturity_eras=eras,
        epoch_started_ms=epoch,
        prints_v1_completion_rate_by_cohort={
            "daily": {k: aggregate(v) for k, v in daily.items()},
            "weekly": {k: aggregate(v) for k, v in weekly.items()},
        },
        top_symbol_issues=issues,
        horizon_limitation="LONG_HORIZON_COVERAGE_LIMITATION" if long_limited else None,
    )


def audit_completeness(
    store, settings, source="all", limit=200, days=None, horizon=None, asof_ms=None, progress=None, **budgets
):
    if (
        source not in (*SOURCES, "all")
        or not 1 <= limit <= 10000
        or (days is not None and not 1 <= days <= 365)
        or (horizon and horizon not in HORIZONS)
    ):
        raise ValueError("Use a supported source/horizon, limit 1..10000 and days 1..365")
    asof = asof_ms if asof_ms is not None else now_ms()
    snapshots, duplicates, truncated = select_candidates(store, source, limit, days, horizon, asof)
    reader = EvidenceReader(store, {(s["source"], s["signal"]["symbol"]) for s in snapshots}, **budgets)
    rows = []
    for index, snapshot in enumerate(snapshots):
        rows.append(audit_candidate(store, settings, snapshot, reader, asof))
        if progress:
            progress(index + 1, len(snapshots))
    report = summarize(rows, duplicates, asof, store.get("ml_maturity_epoch", {}).get("started_ms"))
    return report | dict(
        policy="prints-v1",
        candidate_schema=SCHEMA_VERSION,
        source=source,
        limit=limit,
        days=days,
        horizon=horizon,
        cohort_start_ms=min((r["decision_ms"] for r in rows), default=None),
        cohort_end_ms=max((r["decision_ms"] for r in rows), default=None),
        truncated=truncated,
        continuity_rules=continuity_rules(settings),
        recent_candidates=rows,
        duplicate_snapshots=duplicates,
        forensic_budget=dict(
            decoded_segments=reader.segments,
            decoded_rows=reader.rows,
            compressed_bytes=reader.bytes,
            exhausted=reader.exhausted,
        ),
        limitations="Unknown/pruned/budget-limited evidence cannot establish a false gap or retention loss. Completion denominator is mature decisions with known eligible coverage; unknown eligibility is reported separately.",
    )


def cache_report(store, report):
    """Only compact diagnostics are written, after all raw reads finish."""
    cached = {}
    for source, detail in report["by_source"].items():
        cached[source] = detail | dict(
            source=source,
            cohort_start_ms=report["cohort_start_ms"],
            cohort_end_ms=report["cohort_end_ms"],
            last_audit_ms=report["summary"]["last_audit_ms"],
            forensic=True,
            truncated=report["truncated"],
            forensic_budget=report["forensic_budget"],
            scope=dict(
                source=report["source"], limit=report["limit"], days=report["days"], horizon=report["horizon"]
            ),
        )
    existing = store.get("primary_data_quality", {})
    existing.update({s: d for s, d in cached.items() if report["source"] in {"all", s}})
    store.put("primary_data_quality", existing)
    fields = (
        "source",
        "symbol",
        "decision_ms",
        "expected_horizon_end_ms",
        "gap_count",
        "largest_gap_ms",
        "gap_total_ms",
        "gap_family",
        "first_gap_start_ms",
        "first_gap_end_ms",
        "eligible_at_decision",
        "audit_class",
        "audit_reason",
    )
    with store.db:
        store.db.executemany(
            "INSERT OR REPLACE INTO kv VALUES(?,?)",
            [
                (
                    "primary_gap_summary:" + row["candidate_id"],
                    json.dumps(
                        {key: row[key] for key in fields}
                        | dict(audited_ms=report["summary"]["last_audit_ms"])
                    ),
                )
                for row in report["recent_candidates"]
            ],
        )


def cache_readiness(store, asof_ms=None):
    """Worker-only scalar counts; status never replays raw or scans decisions."""
    asof = asof_ms if asof_ms is not None else now_ms()
    rows = store.db.execute(
        "WITH ranked AS (SELECT s.id,json_extract(s.payload,'$.source') AS source,"
        "coalesce(c.candidate_identity,s.signal_id) AS identity,"
        "json_extract(s.payload,'$.primary_ml_eligibility.eligible') AS eligible,"
        "row_number() OVER(PARTITION BY coalesce(c.candidate_identity,s.signal_id) ORDER BY s.decision_ms,s.id) AS rn "
        "FROM ml_snapshots s LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
        "WHERE s.stage='decision' AND s.schema_version=? AND s.decision_ms<=?) "
        "SELECT r.source,r.identity,r.eligible,coalesce(json_extract(l.payload,'$.complete'),0) "
        "FROM ranked r LEFT JOIN ml_labels l ON l.snapshot_id=r.id AND l.policy='prints-v1' WHERE r.rn=1",
        (SCHEMA_VERSION, asof),
    ).fetchall()
    cached = store.get("primary_data_quality", {})
    for source in SOURCES:
        selected = [r for r in rows if r[0] == source]
        counts = dict(
            current_schema_decisions=len({r[1] for r in selected}),
            primary_ml_eligible_decisions=len({r[1] for r in selected if r[2] == 1}),
            complete_eligible_outcomes=len({r[1] for r in selected if r[2] == 1 and r[3]}),
            ineligible_decisions=len({r[1] for r in selected if r[2] == 0}),
            historical_eligibility_unmeasured=len({r[1] for r in selected if r[2] is None}),
            at_ms=asof,
        )
        detail = cached.setdefault(
            source, dict(source=source, status="AWAITING_FORENSIC_AUDIT", last_audit_ms=None)
        )
        detail["prospective_readiness"] = counts
    store.put("primary_data_quality", cached)


def human_report(report):
    sections = (
        ("SUMMARY", report["summary"]),
        ("COHORT COMPLETION", report["prints_v1_completion_rate_by_cohort"]),
        ("ROOT CAUSES", report["root_causes"]),
        ("BY SOURCE", report["by_source"]),
        ("BY HORIZON", report["by_horizon"]),
        ("TOP SYMBOL ISSUES", report["top_symbol_issues"]),
        ("RECENT CANDIDATES", report["recent_candidates"]),
    )
    return "\n\n".join(
        title + "\n" + json.dumps(value, indent=2, allow_nan=False) for title, value in sections
    )
