import json
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx

from .storage import now_ms


def iso(ms):

    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def embed(signal, dashboard_url):

    s = signal

    monitoring_event = s.coverage.get("monitoring_event")

    monitoring_update = monitoring_event in {"paused", "resumed", "ended"}

    terminal = s.state in {"INVALIDATED", "EXPIRED", "RESOLVED"}

    status = {
        "CONFIRMED": "NEW SETUP",
        "ALERTED": "ACTIVE",
        "EXPIRED": "ENTRY EXPIRED",
        "INVALIDATED": "SETUP WITHDRAWN",
        "RESOLVED": "CLOSED",
    }.get(s.state, s.state)

    if monitoring_update:
        status = {"paused": "MONITORING PAUSED", "resumed": "MONITORING RESUMED", "ended": "TRACKING ENDED"}[
            monitoring_event
        ]

    fields = []

    def field(name, value, inline=False):

        fields.append(dict(name=name, value=str(value)[:850], inline=inline))

    if monitoring_update:
        field(
            "Update",
            {
                "paused": "Live data interrupted. Monitoring is paused; this is not a stop-loss hit or setup invalidation. Do not open a new entry while data is unavailable.",
                "resumed": "Historical catch-up is complete and live data is fresh. Monitoring resumed.",
                "ended": "The tracking period has ended. This does not establish a profit, loss, or account fill.",
            }[monitoring_event],
        )

        field("Original plan", f"Entry {s.entry:g} · SL {s.stop:g} · TP1 {s.tp1:g} · TP2 {s.tp2:g}")

    elif terminal:
        reason = (
            s.invalidation
            if s.state == "INVALIDATED"
            else (
                "The entry window has ended."
                if s.state == "EXPIRED"
                else "Tracking has ended; see the recorded outcome."
            )
        )

        if s.state == "INVALIDATED" and s.coverage.get("reasons"):
            reason += " · " + "; ".join(s.coverage["reasons"])

        field("Update", s.coverage.get("terminal_reason", reason))

        field("Original plan", f"Entry {s.entry:g} · SL {s.stop:g} · TP1 {s.tp1:g} · TP2 {s.tp2:g}")

    else:
        field("Entry", f"{s.entry:g}\nZone {s.zone[0]:g}–{s.zone[1]:g}", True)

        field("Stop loss", f"{s.stop:g}", True)

        field("Targets", f"TP1 {s.tp1:g}\nTP2 {s.tp2:g}", True)

        deadline = min(s.expires_ms, s.trigger_expires_ms or s.expires_ms)

        timing = f"Entry valid until <t:{deadline // 1000}:t> · expires <t:{deadline // 1000}:R>"

        if s.holding_deadline_ms:
            timing += f"\nTracking ends <t:{s.holding_deadline_ms // 1000}:F> (local time)"

        field("Timing", timing)

        flow = s.evidence.get("flow", {})

        summary = s.family.replace("_", " ").capitalize()

        if s.source == "tradingview":
            summary += " · chart-volume confirmation (not executed order flow)"

        elif flow.get("available"):
            seconds = s.evidence.get("execution_window_ms", 900_000) // 1000

            delta = flow.get("delta_pct")

            summary += f" · {seconds}s executed-flow confirmation"

            if delta is not None:
                summary += f" · delta {delta:+.1f}%"

        if s.risk.get("net_rr") is not None:
            summary += f"\nNet reward:risk {s.risk['net_rr']:.2f}R to TP1"

        field("Setup", summary)

        if s.horizon_profile != "LEGACY":
            a, f, d, market, execution = (
                s.evidence.get(k, {})
                for k in ("auction", "flow", "derivatives", "market_factor", "execution")
            )

            def number(value, precision=2):

                return f"{value:.{precision}f}" if isinstance(value, (float, int)) else "unavailable"

            field(
                "Context",
                f"{s.entry_session or 'OFF_SESSION'} · {a.get('state', 'UNCLEAR')}\n"
                f"Flow Δ {number(f.get('delta_pct'))}% · CVD {number(f.get('cvd'))} · OBI {number(s.evidence.get('book', {}).get('obi_persistence'))}\n"
                f"OI {number(d.get('oi_change_pct'))}% · funding {number(d.get('funding_rate'), 5)}\n"
                f"BTC β {number(market.get('beta_to_btc'))} · residual {number(market.get('residual_return'), 5)}\n"
                f"Spread {number(s.evidence.get('book', {}).get('spread_bps'))} bps · stop/noise {number(execution.get('stop_noise_ratio'))}",
            )

            validated = s.validation_status == "validated" and s.calibrated_probability is not None

            field(
                "Model",
                f"P {s.calibrated_probability:.1%} · expected {number(s.expected_net_r)}R"
                if validated
                else "Collecting model validation evidence",
            )

    return {
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": f"{status} · {s.symbol} {s.direction}",
                "description": (
                    f"**{s.horizon_profile} · {s.expected_hold_min / 60:g}–{s.expected_hold_max / 60:g} hours**"
                    if s.horizon_profile != "LEGACY"
                    else "**INTRADAY · up to 4 hours**"
                )
                + f"\n{s.raw_tier} · {s.quality:.1f}/100 quality · {s.source.capitalize()}"
                + ("\nUpdate only · no new entry" if terminal or monitoring_update else ""),
                "url": f"{dashboard_url.rstrip('/')}/#signal/{s.id}",
                "color": 0x8B949E
                if terminal or monitoring_update
                else (0x4CC9A4 if s.direction == "LONG" else 0xEF7F86),
                "timestamp": iso(now_ms() if terminal or monitoring_update else s.created_ms),
                "fields": fields,
                "footer": {"text": f"{s.id}"},
            }
        ],
    }


def related_embed(signal, primary, relation, dashboard_url, cluster_id):
    """One setup-card layout; relationship changes presentation, never either plan."""

    category = {
        "CONFIRMING_HORIZON": "Setup confirmation",
        "STRONGER_REPLACEMENT": "Stronger setup",
        "CONFLICTING_HORIZON": "Conflicting horizon",
    }[relation]

    payload = embed(signal, dashboard_url)

    card = payload["embeds"][0]

    card["title"] = f"NEW SETUP [{category}] · {signal.symbol} {signal.direction}"

    card["description"] += f"\n[Updated information] {signal.family.replace('_', ' ')} · {signal.state}"

    deadline = min(primary.expires_ms, primary.trigger_expires_ms or primary.expires_ms)

    old_plan = (
        f"**{primary.symbol} {primary.direction} · {primary.horizon_profile}**\n"
        f"{primary.family.replace('_', ' ')} · {primary.raw_tier} · {primary.quality:.1f}/100 · {primary.state}\n"
        f"Entry {primary.entry:g} · zone {primary.zone[0]:g}–{primary.zone[1]:g}\n"
        f"SL {primary.stop:g} · TP1 {primary.tp1:g} · TP2 {primary.tp2:g}\n"
        f"Original entry deadline <t:{deadline // 1000}:t> · ID {primary.id}"
    )

    relationship_note = {
        "CONFIRMING_HORIZON": "The new setup supports the same direction with its own horizon and plan. It is not an instruction to double exposure.",
        "STRONGER_REPLACEMENT": "The new setup has a higher quality score. The earlier plan is not replaced or cancelled.",
        "CONFLICTING_HORIZON": "The plans point in opposite directions during overlapping tracking periods. Compare their horizons and entry conditions before choosing an entry.",
    }[relation]

    card["fields"].insert(
        0, dict(name="Relationship [" + category + "]", value=relationship_note, inline=False)
    )

    card["fields"].append(dict(name="Previous setup [unchanged]", value=old_plan, inline=False))

    card["fields"].append(
        dict(
            name="Monitoring",
            value="Both setups retain their original stops, targets and deadlines and remain separately monitored.",
            inline=False,
        )
    )

    card["footer"]["text"] = f"New setup {signal.id} · {cluster_id}"

    return payload


class Notifier:
    def __init__(self, settings, store, transport=None):

        self.settings, self.store, self.transport = settings, store, transport

    async def send_research(self, signal, update=False):

        if signal.synthetic:
            return "blocked: use explicitly labeled test-signal delivery"

        if not update and signal.horizon_profile == "EXTENDED_SWING":
            return "shadow-only horizon"

        if signal.validation_status == "validated" and not self.settings.research_alerts:
            return await self.send_public(signal, update)

        if not (
            self.settings.research_alerts
            or (self.settings.sss_research and signal.final_tier.startswith("SSS RESEARCH"))
        ):
            return "disabled"

        secret = self.settings.research_webhook.get_secret_value()

        if not secret:
            return "dry-run"

        u = urlparse(secret)

        if u.scheme != "https" or u.hostname != "discord.com" or not u.path.startswith("/api/webhooks/"):
            raise ValueError("Expected official Discord HTTPS webhook")

        key = f"research:{signal.id}:{signal.state if update else 'initial'}"

        if update and signal.coverage.get("monitoring_event"):
            key = f"research:{signal.id}:monitoring:{signal.coverage['monitoring_event']}:{signal.coverage.get('pause_since_ms', 0)}"

        if update and self.store.db.execute("SELECT 1 FROM outbox WHERE key=?", (key,)).fetchone():
            return "already-attempted"

        payload = embed(signal, self.settings.dashboard_url)

        if not update:
            return await self.deliver_initial(key, signal, payload, secret)

        return await self.deliver(key, signal.id, payload, secret)

    async def deliver_initial(self, key, signal, payload, secret):

        from .identity import claim
        from .macro import state as macro_state

        if macro_state(self.store, self.settings, now_ms())["macro_pause_active"]:
            return "macro-paused"

        from .funnel import emit

        emit(self.store, "alert_claim_attempts", now_ms(), signal=signal, key="claim-attempt:" + signal.id)

        claimed = claim(self.store, signal, now_ms())

        emit(
            self.store,
            "alert_claimed" if claimed["claimed"] else "duplicate_suppressed",
            now_ms(),
            signal=signal,
            key="claim-result:" + signal.id,
        )

        if not claimed["claimed"]:
            return "duplicate-plan-suppressed"

        primary = claimed["primary"]

        relation = claimed["relationship"]

        if primary is not None:
            payload = related_embed(
                signal, primary, relation, self.settings.dashboard_url, claimed["cluster_id"]
            )

        status = await self.deliver(key, signal.id, payload, secret)

        row = self.store.db.execute("SELECT message_id FROM outbox WHERE key=?", (key,)).fetchone()

        message = row[0] if row else None

        with self.store.db:
            self.store.db.execute(
                "UPDATE initial_alert_claims SET delivery_status=?,message_id=? WHERE fingerprint=?",
                (status, message, claimed["fingerprint"]),
            )

            self.store.db.execute(
                "UPDATE real_delivery_attempts SET status=?,message_id=? WHERE id=?",
                (status, message, claimed["attempt"]),
            )

        return status

    async def send_connection_test(self, event_id, source_ms):

        if not self.settings.research_alerts:
            return "disabled"

        secret = self.settings.research_webhook.get_secret_value()

        if not secret:
            return "dry-run"

        payload = {
            "allowed_mentions": {"parse": []},
            "embeds": [
                {
                    "title": "CONNECTION TEST · NOT A TRADE",
                    "description": "NOT A TRADE SIGNAL\nBybitFlow outbound Discord connectivity test. No setup, price recommendation, probability, or execution.",
                    "timestamp": iso(source_ms),
                    "footer": {"text": event_id},
                }
            ],
        }

        return await self.deliver("connection-test:" + event_id, None, payload, secret)

    async def deliver(self, key, signal_id, payload, secret):

        u = urlparse(secret)

        if u.scheme != "https" or u.hostname != "discord.com" or not u.path.startswith("/api/webhooks/"):
            raise ValueError("Expected official Discord HTTPS webhook")

        if self.store.db.execute("SELECT 1 FROM outbox WHERE key=?", (key,)).fetchone():
            return "already-attempted"

        with self.store.db:
            claimed = self.store.db.execute(
                "INSERT OR IGNORE INTO outbox VALUES(?,?,?,?,?,?)",
                (key, signal_id, "sending", json.dumps(payload), None, now_ms()),
            ).rowcount

        if not claimed:
            return "already-attempted"

        # Discord has no atomic idempotency key. An ambiguous network failure is NOT retried:

        # reconcile it manually; at-most-once attempts prevent duplicate signal cards.

        from .funnel import emit

        real_initial = signal_id is not None and key.endswith(":initial")

        if real_initial:
            emit(self.store, "discord_http_attempts", now_ms(), key="http:" + key)

        status, message_id = "uncertain", None

        transport_detail = dict(at_ms=now_ms(), category="UNKNOWN", http_status=None)

        try:
            async with httpx.AsyncClient(timeout=15, transport=self.transport) as client:
                response = await client.post(secret, params={"wait": "true"}, json=payload)

                transport_detail.update(http_status=response.status_code, category="HTTP")

                if response.status_code == 429:
                    status = "rate-limited"

                elif response.is_success:
                    status, message_id = "sent", response.json().get("id")

                else:
                    status = "rejected"

        except httpx.TransportError as exc:
            # Store a category only: exception messages can contain webhook secrets.

            transport_detail["category"] = (
                "TIMEOUT" if isinstance(exc, httpx.TimeoutException) else "CONNECTION_ERROR"
            )

            transport_detail["error_type"] = type(exc).__name__

        with self.store.db:
            self.store.db.execute(
                "UPDATE outbox SET status=?,message_id=?,updated_ms=? WHERE key=?",
                (status, message_id, now_ms(), key),
            )

        self.store.put("discord_transport", transport_detail | dict(status=status))

        if real_initial:
            metric = {"sent": "discord_sent", "uncertain": "discord_uncertain"}.get(
                status, "discord_rejected"
            )

            emit(
                self.store,
                metric,
                now_ms(),
                key="http-result:" + key,
                reason="" if status == "sent" else status.upper(),
            )

        elif signal_id is not None and status == "sent":
            emit(self.store, "lifecycle_updates_sent", now_ms(), key="lifecycle:" + key)

        return status

    async def send_public(self, signal, update=False):

        from .ml.inference import delivery_eligible

        if update:
            sent = self.store.db.execute(
                "SELECT 1 FROM outbox WHERE key=? AND status='sent'", (f"validated:{signal.id}:initial",)
            ).fetchone()

            if not sent or signal.state not in {"EXPIRED", "INVALIDATED", "RESOLVED"}:
                return "blocked: no prior validated delivery or terminal update"

        elif not delivery_eligible(signal, self.settings, self.store):
            return "blocked: no validated deployment model"

        if not update:
            recent = self.store.db.execute(
                "SELECT 1 FROM outbox o JOIN signals s ON s.id=o.signal_id WHERE s.symbol=? "
                "AND o.updated_ms>? AND o.status IN ('sent','uncertain','sending') LIMIT 1",
                (signal.symbol, now_ms() - self.settings.cooldown_minutes * 60_000),
            ).fetchone()

            if recent:
                return "cooldown"

        secret = self.settings.discord_webhook.get_secret_value()

        if not secret:
            return "dry-run"

        key = f"validated:{signal.id}:{signal.state if update else 'initial'}"

        payload = embed(signal, self.settings.dashboard_url)

        return (
            await self.deliver(key, signal.id, payload, secret)
            if update
            else await self.deliver_initial(key, signal, payload, secret)
        )

    async def send_operational(self, topic, details):

        secret = self.settings.ops_webhook.get_secret_value()

        if not secret:
            return "disabled"

        # Fixed summaries avoid accidentally forwarding arbitrary exception strings or secrets.

        safe = {k: details[k] for k in ("status", "model_id", "n", "minimum") if k in details}

        payload = {
            "allowed_mentions": {"parse": []},
            "embeds": [
                {
                    "title": "ML OPERATIONS · " + topic,
                    "description": json.dumps(safe)[:1500],
                    "footer": {"text": "Research only; no risk changes"},
                }
            ],
        }

        return await self.deliver(f"ml-ops:{topic}:{now_ms() // 86_400_000}", None, payload, secret)
