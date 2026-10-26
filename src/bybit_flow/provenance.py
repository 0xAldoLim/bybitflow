"""Reproducible operational configuration; never expose secrets, URLs or paths."""

import hashlib
import json
import os
from pathlib import Path

from . import __version__
from .migrations import status as migration_status
from .v2 import code_commit

CONFIG_FIELDS = (
    "book_depth",
    "book_stale_ms",
    "cooldown_minutes",
    "core_watchlist",
    "correlated_risk_limit",
    "daily_loss_limit",
    "deep_symbols",
    "execution_window_seconds",
    "exploration_fraction",
    "funding_reserve_bps",
    "horizon_model_min_samples",
    "horizon_profiles",
    "hypothetical_notional",
    "leverage",
    "liquidation_buffer_multiple",
    "macro_news_enabled",
    "macro_news_post_minutes",
    "macro_news_pre_minutes",
    "macro_refresh_minutes",
    "maintenance_margin_assumption",
    "market_source",
    "max_spread_bps",
    "max_storage_gb",
    "min_age_days",
    "min_daily_turnover",
    "min_net_rr",
    "ml_enabled",
    "ml_filter_research",
    "ml_print_recovery_enabled",
    "ml_raw_retention_minutes",
    "ml_two_stage",
    "post_terminal_checkpoints",
    "post_terminal_enabled",
    "queue_byte_limit",
    "queue_size",
    "quote_sample_seconds",
    "raw_retention_days",
    "recorder_segment_rows",
    "recorder_segment_seconds",
    "recording_retention_enabled",
    "research_alerts",
    "rest_concurrency",
    "rest_requests_per_second",
    "risk_fraction",
    "scan_enabled",
    "scan_seconds",
    "score_profile_auto_promote",
    "settle_coins",
    "slippage_bps",
    "spread_bucket_seconds",
    "spread_min_samples",
    "spread_window_minutes",
    "sss_research",
    "stage_a_depth_candidates",
    "taker_fee_bps",
    "tape_max_trades",
    "trade_stale_ms",
    "v8_gate_anchored",
    "v8_gate_breadth",
    "v8_gate_context_ev",
    "v8_gate_liquidation",
    "v8_gate_ofi",
    "v8_gate_spot_perp",
    "v8_gate_volatility",
    "v8_production_gating",
    "validated_alert_tiers",
    "weekly_loss_limit",
)


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def config_snapshot(settings):
    fields = settings.model_dump(mode="json", include=set(CONFIG_FIELDS))
    return dict(
        settings=fields,
        config_digest=digest(fields),
        redaction="Explicit operational allowlist; no account values, URLs, tokens or filesystem paths",
    )


def lock_digest():
    root = Path(__file__).resolve().parents[2]
    if not (root / "requirements.lock").is_file() and Path("/app/requirements.lock").is_file():
        root = Path("/app")
    files = {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in (
            "requirements.lock",
            "requirements-ml.lock",
            "requirements-lstm.lock",
            "requirements-dev.lock",
        )
        if (root / name).is_file()
    }
    return digest(files) if files else os.environ.get("FLOW_LOCK_DIGEST", "UNKNOWN")


def status(store, settings):
    deployment = store.get("deployment_provenance", {})
    return dict(
        version=__version__,
        git_commit=code_commit(),
        dependency_lock_digest=lock_digest(),
        database=migration_status(store),
        image_id=os.environ.get("FLOW_IMAGE_ID", deployment.get("image_id", "UNKNOWN")),
        deployed_ms=deployment.get("deployed_ms"),
        effective_config=config_snapshot(settings),
        configured_resource_limits={
            "memory": os.environ.get("FLOW_MEMORY_LIMIT", "UNKNOWN"),
            "cpus": os.environ.get("FLOW_CPU_LIMIT", "UNKNOWN"),
        },
    )


def record(store, settings, now):
    row = status(store, settings) | dict(deployed_ms=now)
    ident = digest(row)
    with store.db:
        store.db.execute(
            "INSERT OR IGNORE INTO deployment_records VALUES(?,?,?)", (ident, now, json.dumps(row))
        )
    store.put("deployment_provenance", row)
    return row
