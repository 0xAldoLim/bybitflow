import json
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx

from .storage import now_ms


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def embed(signal, dashboard_url):
    from .scoring import tier

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
                "resumed": "Live data is available again. Monitoring resumed; prices during the gap are unverified. This is not a new entry signal.",
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
        field("Update", reason)
        field("Original plan", f"Entry {s.entry:g} · SL {s.stop:g} · TP1 {s.tp1:g} · TP2 {s.tp2:g}")
    else:
        field("Entry", f"{s.entry:g}\nZone {s.zone[0]:g}–{s.zone[1]:g}", True)
        field("Stop loss", f"{s.stop:g}", True)
        field("Targets", f"TP1 {s.tp1:g}\nTP2 {s.tp2:g}", True)
        deadline = min(s.expires_ms, s.trigger_expires_ms or s.expires_ms)
        timing = f"Entry valid until <t:{deadline // 1000}:t> · expires <t:{deadline // 1000}:R>"
        if s.holding_deadline_ms:
            timing += f"\nTracking ends <t:{s.holding_deadline_ms // 1000}:t> at the latest"
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

    return {
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": f"{status} · {s.symbol} {s.direction}",
                "description": f"**INTRADAY · up to 4 hours**\n{tier(s.quality)} · {s.quality:.1f}/100 quality · {s.source.capitalize()}"
                + ("\nUpdate only · no new entry" if terminal or monitoring_update else ""),
                "url": f"{dashboard_url.rstrip('/')}/#signal/{s.id}",
                "color": 0x8B949E
                if terminal or monitoring_update
                else (0x4CC9A4 if s.direction == "LONG" else 0xEF7F86),
                "timestamp": iso(now_ms() if terminal or monitoring_update else s.created_ms),
                "fields": fields,
                "footer": {
                    "text": f"Research · score is not win probability · no automatic execution · {s.id}"
                },
            }
        ],
    }


class Notifier:
    def __init__(self, settings, store, transport=None):
        self.settings, self.store, self.transport = settings, store, transport

    async def send_research(self, signal, update=False):
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
        if self.store.db.execute("SELECT 1 FROM outbox WHERE key=?", (key,)).fetchone():
            return "already-attempted"
        if not update:
            cutoff = now_ms() - self.settings.cooldown_minutes * 60_000
            recent = self.store.db.execute(
                "SELECT 1 FROM outbox o JOIN signals s ON s.id=o.signal_id "
                "WHERE s.symbol=? AND o.updated_ms>? AND o.status IN ('sent','uncertain','sending') LIMIT 1",
                (signal.symbol, cutoff),
            ).fetchone()
            if recent:
                return "cooldown"
        payload = embed(signal, self.settings.dashboard_url)
        return await self.deliver(key, signal.id, payload, secret)

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
                    "description": "Authenticated TradingView gateway test. No setup, price recommendation, probability or execution.",
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
            self.store.db.execute(
                "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
                (key, signal_id, "sending", json.dumps(payload), None, now_ms()),
            )
        # Discord has no atomic idempotency key. An ambiguous network failure is NOT retried:
        # reconcile it manually; at-most-once attempts prevent duplicate signal cards.
        status, message_id = "uncertain", None
        try:
            async with httpx.AsyncClient(timeout=15, transport=self.transport) as client:
                response = await client.post(secret, params={"wait": "true"}, json=payload)
                if response.status_code == 429:
                    status = "rate-limited"
                elif response.is_success:
                    status, message_id = "sent", response.json().get("id")
                else:
                    status = "rejected"
        except httpx.TransportError:
            pass
        with self.store.db:
            self.store.db.execute(
                "UPDATE outbox SET status=?,message_id=?,updated_ms=? WHERE key=?",
                (status, message_id, now_ms(), key),
            )
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
        return await self.deliver(key, signal.id, embed(signal, self.settings.dashboard_url), secret)

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
