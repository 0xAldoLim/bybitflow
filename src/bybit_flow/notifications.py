import json
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx

from .storage import now_ms


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def embed(signal, dashboard_url):
    s, f, d, r = signal, signal.evidence.get("flow", {}), signal.evidence.get("derivatives", {}), signal.risk
    fields = []

    def field(name, value, inline=False):
        fields.append({"name": name, "value": str(value)[:850] or "Unavailable", "inline": inline})

    field(
        "Quality / probability",
        f"{s.final_tier} · {s.quality:.1f}/100 (raw {s.raw_tier})\nUncalibrated",
        True,
    )
    field("Setup / regime", f"{s.family}\n{s.regime}", True)
    field("Entry zone / invalidation", f"{s.zone[0]:g} – {s.zone[1]:g}\nStop {s.stop:g}", True)
    field(
        "Targets", f"TP1 {s.tp1:g} · TP2 {s.tp2:g}\n{s.evidence.get('target_method', 'defined plan')}", True
    )
    field("Reward:risk", f"Gross {r.get('gross_rr', 0):.2f}R · net {r.get('net_rr', 0):.2f}R", True)
    field(
        "Hypothetical risk",
        f"{100 * r.get('risk_fraction', 0.0025):.2f}% · qty {r.get('quantity') or 'equity not set'}\n"
        f"Margin {r.get('estimated_margin') or 'N/A'} · leverage ceiling {r.get('illustrative_leverage', 3):g}×",
        True,
    )
    field(
        "Executed flow · 15M window",
        f"Delta {f.get('delta_pct', 'N/A')}% · CVD {f.get('cvd', 'N/A')} base\n"
        f"Stack buy/sell {f.get('stacked_buy', 'N/A')}/{f.get('stacked_sell', 'N/A')} · "
        f"absorption L/S {f.get('absorption_long', 'N/A')}/{f.get('absorption_short', 'N/A')}\n"
        f"Window PoC {f.get('poc', 'N/A')} · VA {f.get('val', 'N/A')} – {f.get('vah', 'N/A')}",
    )
    smc = s.evidence.get("h1", {})
    field(
        "Structure",
        f"Sweep L/S {smc.get('sweep_long')}/{smc.get('sweep_short')} · BOS {smc.get('bos')} · "
        f"CHoCH {smc.get('choch')}\nLiquidity {smc.get('low')} / {smc.get('high')} · "
        f"FVG {len(smc.get('fvg', []))} · order blocks {len(smc.get('order_blocks', []))}",
    )
    field(
        "Derivatives",
        f"OI change {d.get('oi_change_pct', 'N/A')}% · funding {d.get('funding_rate', 'N/A')}\n"
        f"Actual liquidation events (bankruptcy notional): {d.get('liquidations', 'Unavailable')}",
    )
    field(
        "Cross-market / fundamentals",
        f"BTC/ETH: {s.evidence.get('cross_market', 'Unavailable')}\n"
        f"Binance unavailable · {len(s.evidence.get('fundamentals', []))} source-attributed asset facts",
    )
    field("Data / why / invalidation", f"{s.coverage}\n{s.reason}\n{s.invalidation}")
    if s.gates:
        field("Rejections", "; ".join(s.gates))
    return {
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": f"UNVALIDATED RESEARCH · {s.symbol} · {s.direction}",
                "description": f"Bybit linear perpetual · 4H / 1H / 15M · UTC session · {s.state}",
                "url": f"{dashboard_url.rstrip('/')}/#signal/{s.id}",
                "color": 0x4CC9A4 if s.direction == "LONG" else 0xEF7F86,
                "timestamp": iso(s.created_ms),
                "fields": fields,
                "footer": {
                    "text": f"{s.id} · {s.version} · expires {iso(s.expires_ms)} · alerts only; no execution"
                },
            }
        ],
    }


class Notifier:
    def __init__(self, settings, store, transport=None):
        self.settings, self.store, self.transport = settings, store, transport

    async def send_research(self, signal, update=False):
        if not self.settings.research_alerts:
            return "disabled"
        secret = self.settings.research_webhook.get_secret_value()
        if not secret:
            return "dry-run"
        u = urlparse(secret)
        if u.scheme != "https" or u.hostname != "discord.com" or not u.path.startswith("/api/webhooks/"):
            raise ValueError("Expected official Discord HTTPS webhook")
        key = f"research:{signal.id}:{signal.state if update else 'initial'}"
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
        with self.store.db:
            self.store.db.execute(
                "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
                (key, signal.id, "sending", json.dumps(payload), None, now_ms()),
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

    async def send_public(self, signal):
        # A future reviewed model release must implement this boundary; a config toggle cannot unlock it.
        return "blocked: no validated deployment model"
