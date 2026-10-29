import asyncio
import json
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx

from .discord_health import record_transport
from .storage import now_ms

INITIAL_REQUEST_TIMEOUT = 15


def iso(ms):

    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def price_result(signal):
    """Classify an evidenced price result, never an account fill or ambiguous bar."""
    event = signal.evidence.get("terminal_event") or {}
    if event.get("ambiguous") or event.get("method") not in {"LIVE_EXECUTED_TRADE", "CLOSED_1M_OHLC"}:
        return None
    outcome = signal.evidence.get("primary_outcome")
    if outcome == "TARGET" and signal.state == "RESOLVED":
        return "TARGET"
    if outcome == "STOP" and signal.state == "INVALIDATED":
        return "STOP"
    return None


def embed(signal, dashboard_url):

    s = signal

    terminal = s.state in {"INVALIDATED", "EXPIRED", "RESOLVED"}
    monitoring_event = s.coverage.get("monitoring_event")
    tracking_end = s.state == "EXPIRED" and s.coverage.get("tracking_end")
    expiry_mark = s.evidence.get("expiry_mark") if tracking_end else None
    if terminal and monitoring_event in {"paused", "resumed"}:
        monitoring_event = None
    monitoring_update = monitoring_event in {"paused", "resumed", "ended"}
    if not terminal and not monitoring_update:
        from .quality_v22 import enabled

        if enabled(s):
            from .setup_presentation import embed as setup_embed

            return setup_embed(s, dashboard_url)

    status = {
        "CONFIRMED": "NEW SETUP",
        "ALERTED": "ACTIVE",
        "EXPIRED": "ENTRY EXPIRED",
        "INVALIDATED": "SETUP WITHDRAWN",
        "RESOLVED": "CLOSED",
    }.get(s.state, s.state)
    result = price_result(s)
    if result:
        status = "TP1 REACHED" if result == "TARGET" else "STOP REACHED"
    if tracking_end:
        status = "TRACKING ENDED"

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
                "ended": "The tracking period has ended. Any price result below is an estimate, not an account fill."
                if expiry_mark
                else "The tracking period has ended. This does not establish a profit, loss, or account fill.",
            }[monitoring_event],
        )

        field("Original plan", f"Entry {s.entry:g} · SL {s.stop:g} · TP1 {s.tp1:g} · TP2 {s.tp2:g}")

    elif terminal:
        reason = (
            s.invalidation
            if s.state == "INVALIDATED"
            else (
                "The tracking period has ended."
                if tracking_end
                else "The entry window has ended."
                if s.state == "EXPIRED"
                else "Tracking has ended; see the recorded outcome."
            )
        )

        if s.state == "INVALIDATED" and s.coverage.get("reasons"):
            reason += " · " + "; ".join(s.coverage["reasons"])

        terminal_reason = s.coverage.get("terminal_reason", reason)
        if terminal_reason == "TRADINGVIEW_SUBSYSTEM_RETIRED":
            terminal_reason = "TradingView subsystem retired"
        if result:
            event = s.evidence["terminal_event"]
            level = s.tp1 if result == "TARGET" else s.stop
            terminal_reason = f"{'First target' if result == 'TARGET' else 'Planned stop'} {level:g} reached on {s.source.capitalize()}."
            at = event.get("effective_ms")
            if at is not None:
                terminal_reason += f"\nObserved <t:{at // 1000}:F>"
            if event["method"] == "CLOSED_1M_OHLC":
                terminal_reason += "\nRecovered from closed 1-minute candles; time is a candle-end bound."
            distance = abs(s.entry - s.stop)
            if distance > 0 and s.evidence.get("observed_entry_ms") is not None:
                move = (1 if s.direction == "LONG" else -1) * (level - s.entry)
                field(
                    "Planned price result",
                    f"{100 * move / s.entry:+.2f}% · {move / distance:+.2f}R gross, before fees",
                )
            terminal_reason += "\nPrice-path result only; account fills and realized profit are not verified."
        field("Update", terminal_reason)

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
        v2 = s.evidence.get("v2", {})
        if v2.get("score_profile") == "bybitflow-v2-evidence-1":
            field(
                "Evidence · v2",
                "\n".join(
                    (
                        f"Location {v2['location']['score']:.0f} · Mechanism {v2['mechanism']['score']:.0f} · Offer {v2['trade_offer']['score']:.0f}",
                        v2["mechanism"]["explanation"],
                        v2["trade_offer"]["explanation"],
                        f"Confidence cap {v2['evidence_confidence']:.0f}/100 · quality is not win probability",
                    )
                ),
            )
        ml = s.evidence.get("ml", {})
        if ml.get("track") == "bootstrap":
            field(
                "ML Research",
                f"Bootstrap score {ml['research_score']:g}/100\nOHLC-proxy historical challenger\nProduction authority: none",
            )
        elif ml.get("track") == "monitored":
            field(
                "ML Research",
                f"Monitored setup score {ml['research_score']:g}/100\nRecovered price-path research\nHypothetical fills · advisory only",
            )

        alignment = s.evidence.get("market_alignment", {})
        if alignment.get("alignment") == "IDIOSYNCRATIC_DIVERGENCE":
            prior = alignment.get("htf_factor_direction", "uncertain").lower()
            timeframe = alignment.get("timeframe", s.context_timeframe)
            field(
                "Market context",
                f"BTC {timeframe}M: {prior} · {s.direction} against market trend\n"
                "Override: idiosyncratic divergence",
            )
            reasons = [
                "beta-adjusted relative move",
                "trusted persistent executed flow",
                "price response",
                "acceptance beyond structure or value",
            ]
            if alignment.get("cross_venue_support"):
                reasons.append("trusted cross-venue support")
            field("Why override passed", " · ".join(reasons))
        local_quality = s.evidence.get("flow_quality", {})
        if local_quality.get("flow_trust_score") is not None and local_quality["flow_trust_score"] < 0.6:
            if s.evidence.get("flow_confirmation_mode") == "CROSS_VENUE_SUBSTITUTION":
                field(
                    "Flow quality",
                    "MIXED LOCAL / CONFIRMED CROSS-VENUE · local printed volume was de-emphasized; independent trusted venue flow confirmed the setup",
                )
            else:
                detail = (
                    "repetitive two-sided prints had weak price or book response"
                    if local_quality.get("flow_quality_state") == "REPETITIVE_TWO_SIDED_CHURN"
                    else "local printed volume did not provide trusted directional evidence"
                )
                field("Flow quality", "LOW · " + detail)

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
            insights = []
            feature_states = s.evidence.get("v8_gate", {}).get("feature_states", {})
            for name, label in (
                ("spot_perp", "Spot/perp"),
                ("ofi", "OFI"),
                ("breadth", "Breadth"),
                ("anchored", "AVWAP"),
            ):
                row = feature_states.get(name, {})
                if row.get("gate_ready") and row.get("state") == "SUPPORT":
                    insights.append(label + ": " + row["reason"].replace("_", " ").lower())
            if insights:
                title = (
                    "V8 CONTEXT"
                    if alignment.get("alignment") == "IDIOSYNCRATIC_DIVERGENCE"
                    else "V8 CONFIRMATION"
                )
                field(title, "\n".join(insights[:4]))
            priority = s.evidence.get("opportunity_priority", {})
            if priority.get("rank_of", 0) > 1:
                field(
                    "Opportunity · informational",
                    f"#{priority['rank']} of {priority['rank_of']} · {priority['cluster_id'].replace('_', ' ')}",
                )

    if tracking_end:
        if expiry_mark:
            method = (
                "last original-exchange trade"
                if expiry_mark["method"] == "LIVE_EXECUTED_TRADE"
                else "original-exchange closed 1-minute candle"
            )
            net_r = expiry_mark.get("estimated_net_r")
            if net_r is None:
                outcome = f"Directional move {expiry_mark['gross_pct']:+.2f}% · costs unavailable; profit unconfirmed"
            else:
                outcome = (
                    f"Estimated {'profit' if net_r > 0 else 'loss' if net_r < 0 else 'flat'} "
                    f"{net_r:+.2f}R ({expiry_mark['estimated_net_pct']:+.2f}%) after assumed costs"
                )
            field(
                "At tracking end",
                f"Entry {s.entry:g} → observed {expiry_mark['mark_price']:g} · {outcome}\n"
                f"{method}; no account exit or fill verified. Record your actual close in the paper journal.",
            )
        else:
            field(
                "At tracking end",
                "No reliable price near the deadline or entry-zone evidence was available; profit or loss is unknown.",
            )

    if monitoring_event in {"paused", "resumed"}:
        color = 0x8B949E
    elif s.state == "INVALIDATED":
        color = 0xD9534F
    elif s.state == "EXPIRED" or monitoring_event == "ended":
        color = 0xE6A23C
    elif s.state == "RESOLVED":
        color = 0x4CC9A4
    else:
        color = 0x4CC9A4 if s.direction == "LONG" else 0xEF7F86

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
                "color": color,
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

    card["fields"].append(dict(name="Previous setup [at publication]", value=old_plan, inline=False))

    card["fields"].append(
        dict(
            name="Monitoring",
            value="Both setups retain their original stops, targets and deadlines and are separately monitored until terminal. The previous status is a snapshot at publication; check each original card for its current result. This is not an instruction to close or reverse an existing position.",
            inline=False,
        )
    )

    card["footer"]["text"] = f"New setup {signal.id} · {cluster_id}"

    return payload


class Notifier:
    def __init__(self, settings, store, transport=None):

        self.settings, self.store, self.transport = settings, store, transport
        self.prepare_initial = self.finalize_initial = self.guard_initial = None

    def was_initially_delivered(self, signal_id, channel):
        row = self.store.db.execute(
            "SELECT status,message_id FROM outbox WHERE key=? AND signal_id=?",
            (f"{channel}:{signal_id}:initial", signal_id),
        ).fetchone()
        delivery = self.store.get(f"discord_initial_delivery:{channel}:{signal_id}", {})
        webhook_id = delivery.get("webhook_id") or self.store.get(f"discord_initial_webhook_id:{signal_id}")
        current_path = urlparse(self.settings.research_webhook.get_secret_value()).path.split("/")
        current_webhook_id = current_path[3] if len(current_path) > 3 else None
        # A successful initial in a retired destination does not make a setup
        # visible in the currently configured signals channel.
        destination_matches = not webhook_id or webhook_id == current_webhook_id
        visible = bool(row and row[0] == "sent" and row[1] and destination_matches)
        self.store.put(
            f"discord_visibility:{channel}:{signal_id}",
            dict(
                user_visible_initial=visible,
                initial_delivery_status=row[0] if row else "never-attempted",
                initial_message_id=row[1] if row else None,
                initial_channel=channel,
                initial_webhook_id=webhook_id,
                initial_discord_channel_id=delivery.get("channel_id"),
            ),
        )
        return visible

    async def send_research(self, signal, update=False):

        channel = (
            "validated"
            if signal.validation_status == "validated" and not self.settings.research_alerts
            else "research"
        )
        if not update and signal.evidence.get("delivery_policy") == "REDUNDANT_OPPORTUNITY":
            return "blocked:REDUNDANT_OPPORTUNITY"
        if update and not self.was_initially_delivered(signal.id, channel):
            return "blocked:no-visible-initial"

        if update and signal.state in {"INVALIDATED", "EXPIRED", "RESOLVED"}:
            # Store.signal commits the terminal event first. Delivery belongs to
            # the independent durable worker, never to lifecycle monitoring.
            return "terminal-persisted"

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

        monitoring_event = (
            signal.coverage.get("monitoring_event")
            if update and signal.state not in {"INVALIDATED", "EXPIRED", "RESOLVED"}
            else None
        )
        secret = (
            self.settings.monitoring_webhook
            if monitoring_event in {"paused", "resumed"}
            else self.settings.research_webhook
        ).get_secret_value()

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

    async def terminal_loop(self):
        import asyncio

        while True:
            try:
                await self.retry_terminals()
            except Exception as exc:
                self.store.put("terminal_delivery_error", dict(at_ms=now_ms(), error_type=type(exc).__name__))
            await asyncio.sleep(5)

    async def retry_terminals(self):
        from .models import Signal

        rows = self.store.db.execute(
            "SELECT t.signal_id,t.attempts,s.payload FROM terminal_events t JOIN signals s ON s.id=t.signal_id WHERE t.notification_status='pending' AND t.next_ms<=? LIMIT 20",
            (now_ms(),),
        ).fetchall()
        for ident, attempts, raw in rows:
            initial = self.store.db.execute(
                "SELECT key,message_id,updated_ms FROM outbox WHERE signal_id=? AND key LIKE '%:initial' AND status='sent' ORDER BY updated_ms LIMIT 1",
                (ident,),
            ).fetchone()
            if not initial or not initial[1]:
                with self.store.db:
                    self.store.db.execute(
                        "UPDATE terminal_events SET notification_status='failed' WHERE signal_id=?", (ident,)
                    )
                continue
            if not self.was_initially_delivered(ident, initial[0].split(":", 1)[0]):
                with self.store.db:
                    self.store.db.execute(
                        "UPDATE terminal_events SET notification_status='blocked:no-visible-initial' WHERE signal_id=?",
                        (ident,),
                    )
                continue
            secret = self.settings.research_webhook.get_secret_value()
            u = urlparse(secret)
            if u.scheme != "https" or u.hostname != "discord.com" or not u.path.startswith("/api/webhooks/"):
                continue
            signal = Signal.model_validate_json(raw)
            status = "pending"
            delay = min(300, 2 ** min(attempts + 1, 8))
            current_webhook_id = u.path.split("/")[3]
            channel = initial[0].split(":", 1)[0]
            delivery = self.store.get(f"discord_initial_delivery:{channel}:{ident}", {})
            initial_webhook_id = delivery.get("webhook_id") or self.store.get(
                f"discord_initial_webhook_id:{ident}"
            )
            if initial_webhook_id == current_webhook_id:
                payload = embed(signal, self.settings.dashboard_url)
                card = payload["embeds"][0]
                # Keep the single-card display, but make its history explicit.
                # The outbox retains the immutable initial payload and send time.
                card["description"] += (
                    f"\n**Updated original card** · First posted <t:{initial[2] // 1000}:f>"
                )
                transport_detail = dict(category="UNKNOWN", http_status=None)
                try:
                    async with httpx.AsyncClient(timeout=15, transport=self.transport) as client:
                        from .db_runtime import measure_await

                        response = await measure_await(
                            self.store,
                            client.patch(
                                secret.split("?")[0].rstrip("/") + "/messages/" + initial[1],
                                json=payload,
                            ),
                            "discord_request_latency_ms",
                        )
                        transport_detail.update(category="HTTP", http_status=response.status_code)
                        if response.is_success:
                            status = "sent"
                        elif response.status_code == 429:
                            delay = max(delay, min(3600, float(response.json().get("retry_after", delay))))
                        elif response.status_code < 500:
                            status = "failed"
                except httpx.TransportError as exc:
                    # Editing the same known message remains idempotent. Never
                    # persist exception text, which may contain webhook secrets.
                    transport_detail.update(
                        category="TIMEOUT" if isinstance(exc, httpx.TimeoutException) else "CONNECTION_ERROR",
                        error_type=type(exc).__name__,
                    )
                record_transport(
                    self.store, transport_detail | dict(at_ms=now_ms(), status=status), "signals", "PATCH"
                )
            else:
                # Never post a standalone terminal card when the original
                # destination is unknown or retired. Internal tracking continues.
                status = "blocked:no-visible-initial"
            if status == "sent":
                # PATCH is idempotent, but an edit does not notify Discord users.
                # Attempt before terminal commit: a crash resumes here safely,
                # while the durable outbox prevents a duplicate result POST.
                await self.send_price_result(signal)
            with self.store.db:
                self.store.db.execute(
                    "UPDATE terminal_events SET notification_status=?,attempts=attempts+1,next_ms=? WHERE signal_id=?",
                    (status, now_ms() + int(delay * 1000), ident),
                )
            if status == "sent":
                self.store.put("last_terminal_delivery_ms", now_ms())

    async def send_price_result(self, signal):
        from .models import Signal

        saved = self.store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()
        if not saved:
            return "blocked:no-price-result"
        signal = Signal.model_validate_json(saved[0])
        if not price_result(signal):
            return "blocked:no-price-result"
        channel = next(
            (c for c in ("research", "validated") if self.was_initially_delivered(signal.id, c)), None
        )
        if channel is None:
            return "blocked:no-visible-initial"
        secret = self.settings.research_webhook.get_secret_value()
        if not secret:
            return "dry-run"
        delivery = self.store.get(f"discord_initial_delivery:{channel}:{signal.id}", {})
        original_webhook_id = delivery.get("webhook_id") or self.store.get(
            f"discord_initial_webhook_id:{signal.id}"
        )
        path = urlparse(secret).path.split("/")
        if not original_webhook_id or len(path) <= 3 or original_webhook_id != path[3]:
            return "blocked:no-visible-initial"
        payload = embed(signal, self.settings.dashboard_url)
        return await self.deliver(f"{channel}:{signal.id}:price-result", signal.id, payload, secret)

    def opposing_visible_setup(self, plan):
        """Presentation guard only: every original plan stays independently monitored."""
        from .notification_conflicts import POLICY, assess

        evidence = plan.setdefault("evidence", {})
        evidence.pop("discord_opposition_exception", None)
        reviews = []
        for ident, raw in self.store.db.execute(
            "SELECT id,payload FROM signals WHERE symbol=? AND "
            "state NOT IN ('INVALIDATED','EXPIRED','RESOLVED') AND id!=?",
            (plan["symbol"], plan["id"]),
        ).fetchall():
            other = json.loads(raw)
            if (
                other["direction"] == plan["direction"]
                or (other.get("holding_deadline_ms") or other["expires_ms"]) <= now_ms()
            ):
                continue
            if any(self.was_initially_delivered(ident, c) for c in ("research", "validated")):
                review = assess(plan, other, now_ms(), self.settings)
                reviews.append(review)
                if not review["allowed"]:
                    self.store.put(
                        "discord_conflict:" + plan["id"],
                        dict(
                            policy=POLICY,
                            blocking_signal_id=ident,
                            at_ms=now_ms(),
                            reason="OPPOSING_VISIBLE_ACTIVE_SETUP",
                            review=review,
                            lifecycle_changed=False,
                        ),
                    )
                    return ident
        if reviews:
            evidence["discord_opposition_exception"] = dict(policy=POLICY, allowed=True, reviews=reviews)
            self.store.put("discord_conflict:" + plan["id"], evidence["discord_opposition_exception"])
        return None

    def initial_delivery_block(self, signal_id, signal=None):
        current = self.store.db.execute(
            "SELECT state,payload FROM signals WHERE id=?", (signal_id,)
        ).fetchone()
        if current and current[0] in {"INVALIDATED", "EXPIRED", "RESOLVED"}:
            return "terminal-setup"
        plan = json.loads(current[1]) if current else signal.model_dump() if signal is not None else None
        if plan is None:
            return "blocked:no-active-setup"
        deadline = min(plan["expires_ms"], plan.get("trigger_expires_ms") or plan["expires_ms"])
        remaining = deadline - now_ms()
        if remaining <= 0:
            return "blocked:entry-window-expired"
        # Reserve the existing HTTP timeout, not extra time for the trading plan.
        if remaining < INITIAL_REQUEST_TIMEOUT * 1000:
            return "blocked:entry-window-too-short"
        if signal is not None:
            # The last-mile runtime may have refreshed evidence since Store.signal;
            # lifecycle state, prices and deadlines still come from the durable plan.
            plan["evidence"] = dict(signal.evidence)
        blocking = self.opposing_visible_setup(plan)
        if signal is not None:
            signal.evidence.pop("discord_opposition_exception", None)
            if not blocking and plan["evidence"].get("discord_opposition_exception"):
                signal.evidence["discord_opposition_exception"] = plan["evidence"][
                    "discord_opposition_exception"
                ]
        if blocking:
            return "blocked:opposing-visible-active-setup"
        return None

    async def deliver_initial(self, key, signal, payload, secret):
        blocked = self.initial_delivery_block(signal.id, signal)
        if blocked:
            return blocked
        from .quality_v22 import enabled as v22_enabled

        is_v22 = v22_enabled(signal)
        if is_v22:
            if not self.prepare_initial or not self.finalize_initial:
                return "blocked:v22-live-check-unavailable"
            blocked = await self.prepare_initial(signal)
            if blocked:
                return blocked

        from .identity import claim
        from .macro import state as macro_state

        if macro_state(self.store, self.settings, now_ms())["macro_pause_active"]:
            return "macro-paused"

        from .funnel import emit

        emit(self.store, "alert_claim_attempts", now_ms(), signal=signal, key="claim-attempt:" + signal.id)

        claimed = (
            claim(self.store, signal, now_ms(), retry_unattempted=True)
            if is_v22
            else claim(self.store, signal, now_ms())
        )

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

        def payload_factory():
            if primary is not None:
                return related_embed(
                    signal, primary, relation, self.settings.dashboard_url, claimed["cluster_id"]
                )
            return embed(signal, self.settings.dashboard_url)

        status = await self.deliver(
            key,
            signal.id,
            payload,
            secret,
            initial_signal=signal,
            payload_factory=payload_factory if is_v22 else None,
        )
        self.was_initially_delivered(signal.id, key.split(":", 1)[0])
        if status == "sent":
            with self.store.db:
                self.store.db.execute(
                    "UPDATE terminal_events SET notification_status='pending' WHERE signal_id=? AND notification_status='unseen'",
                    (signal.id,),
                )

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

    async def deliver(self, key, signal_id, payload, secret, *, initial_signal=None, payload_factory=None):

        channel = key.split(":", 1)[0]
        if signal_id is not None and channel in {"research", "validated"} and not key.endswith(":initial"):
            if not self.was_initially_delivered(signal_id, channel):
                return "blocked:no-visible-initial"

        u = urlparse(secret)

        if u.scheme != "https" or u.hostname != "discord.com" or not u.path.startswith("/api/webhooks/"):
            raise ValueError("Expected official Discord HTTPS webhook")

        existing = self.store.db.execute("SELECT status FROM outbox WHERE key=?", (key,)).fetchone()
        safe_retry = bool(
            payload_factory
            and existing
            and existing[0].startswith("blocked:")
            and self.store.get("v22_http_dispatched:" + signal_id) is False
        )
        if existing and not safe_retry:
            return "already-attempted"

        real_initial = signal_id is not None and key.endswith(":initial")
        if real_initial:
            blocked = self.initial_delivery_block(signal_id, initial_signal)
            if blocked:
                return blocked

        with self.store.db:
            if safe_retry:
                claimed = self.store.db.execute(
                    "UPDATE outbox SET status='sending',payload=?,updated_ms=? WHERE key=? AND status LIKE 'blocked:%'",
                    (json.dumps(payload), now_ms(), key),
                ).rowcount
            else:
                claimed = self.store.db.execute(
                    "INSERT OR IGNORE INTO outbox VALUES(?,?,?,?,?,?)",
                    (key, signal_id, "sending", json.dumps(payload), None, now_ms()),
                ).rowcount

        if not claimed:
            return "already-attempted"
        if payload_factory:
            self.store.put("v22_http_dispatched:" + signal_id, False)

        # Discord has no atomic idempotency key. An ambiguous network failure is NOT retried:

        # reconcile it manually; at-most-once attempts prevent duplicate signal cards.

        from .funnel import emit

        http_attempted = False
        status, message_id, discord_channel_id = "uncertain", None, None

        transport_detail = dict(at_ms=now_ms(), category="UNKNOWN", http_status=None)

        try:
            async with httpx.AsyncClient(timeout=15, transport=self.transport) as client:
                # Claiming, SQLite contention and client setup can consume the entry window.
                # Re-read durable lifecycle state immediately before dispatching an initial.
                blocked = self.initial_delivery_block(signal_id, initial_signal) if real_initial else None
                if not blocked and payload_factory:
                    blocked = self.finalize_initial(initial_signal)
                    if not blocked:
                        payload = payload_factory()
                        # Persist the full observed snapshot before HTTP dispatch;
                        # the immutable outbox holds the exact card sent.
                        self.store.monitor(initial_signal)
                        with self.store.db:
                            self.store.db.execute(
                                "UPDATE outbox SET payload=? WHERE key=?", (json.dumps(payload), key)
                            )
                    blocked = blocked or self.initial_delivery_block(signal_id, initial_signal)
                    if not blocked and self.guard_initial:
                        blocked = self.guard_initial(initial_signal)
                if blocked:
                    status = blocked
                    transport_detail["category"] = "BLOCKED"
                else:
                    if real_initial:
                        emit(self.store, "discord_http_attempts", now_ms(), key="http:" + key)
                    if payload_factory:
                        self.store.put("v22_http_dispatched:" + signal_id, True)
                    # Durable accounting can also contend on SQLite. Do not send
                    # a stale snapshot merely because the earlier guard passed.
                    blocked = (
                        self.initial_delivery_block(signal_id, initial_signal)
                        or (self.guard_initial(initial_signal) if self.guard_initial else None)
                        if payload_factory
                        else None
                    )
                    if blocked:
                        self.store.put("v22_http_dispatched:" + signal_id, False)
                        status = blocked
                        transport_detail["category"] = "BLOCKED"
                    else:
                        http_attempted = True
                        # HTTPX's timeout is per phase. Bound the full request.
                        async with asyncio.timeout(INITIAL_REQUEST_TIMEOUT if real_initial else None):
                            from .db_runtime import measure_await

                            response = await measure_await(
                                self.store,
                                client.post(secret, params={"wait": "true"}, json=payload),
                                "discord_request_latency_ms",
                            )
                        transport_detail.update(http_status=response.status_code, category="HTTP")
                        if response.status_code == 429:
                            status = "rate-limited"
                        elif response.is_success:
                            result = response.json()
                            status, message_id = "sent", result.get("id")
                            discord_channel_id = result.get("channel_id")
                        else:
                            status = "rejected"

        except (httpx.TransportError, TimeoutError) as exc:
            # Store a category only: exception messages can contain webhook secrets.

            transport_detail["category"] = (
                "TIMEOUT" if isinstance(exc, (httpx.TimeoutException, TimeoutError)) else "CONNECTION_ERROR"
            )

            transport_detail["error_type"] = type(exc).__name__

        with self.store.db:
            self.store.db.execute(
                "UPDATE outbox SET status=?,message_id=?,updated_ms=? WHERE key=?",
                (status, message_id, now_ms(), key),
            )
            if real_initial and status == "sent" and message_id:
                # Destination and delivery status commit together, so a restart
                # after the POST cannot misroute a later lifecycle update.
                delivery = dict(
                    webhook_id=u.path.split("/")[3],
                    channel_id=discord_channel_id,
                    message_id=message_id,
                )
                self.store.db.execute(
                    "INSERT OR REPLACE INTO kv VALUES(?,?)",
                    (f"discord_initial_delivery:{channel}:{signal_id}", json.dumps(delivery)),
                )
                self.store.db.execute(
                    "INSERT OR REPLACE INTO kv VALUES(?,?)",
                    (f"discord_initial_webhook_id:{signal_id}", json.dumps(delivery["webhook_id"])),
                )

        if http_attempted:
            destination = (
                "monitoring" if ":monitoring:" in key else "operations" if channel == "ml-ops" else "signals"
            )
            record_transport(
                self.store, transport_detail | dict(at_ms=now_ms(), status=status), destination, "POST"
            )

        if real_initial and http_attempted:
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

    async def send_deterioration(self, signal, episode_ms):
        if signal.state != "ALERTED":
            return "blocked:terminal-or-unpublished"
        current_state = self.store.db.execute("SELECT state FROM signals WHERE id=?", (signal.id,)).fetchone()
        if not current_state or current_state[0] != "ALERTED":
            return "blocked:terminal-or-unpublished"
        channel = next(
            (c for c in ("research", "validated") if self.was_initially_delivered(signal.id, c)), None
        )
        if channel is None:
            return "blocked:no-visible-initial"
        secret = self.settings.research_webhook.get_secret_value()
        if not secret:
            return "dry-run"
        current = signal.coverage.get("v22_current", {})
        payload = dict(
            allowed_mentions=dict(parse=[]),
            embeds=[
                dict(
                    title=f"SETUP DETERIORATING · {signal.symbol} {signal.direction}",
                    description="Update only · no new entry · original plan remains independently monitored",
                    color=0xE6A23C,
                    timestamp=iso(now_ms()),
                    fields=[
                        dict(
                            name="Observed change",
                            value=current.get("reason", "Sustained adverse response"),
                            inline=False,
                        ),
                        dict(
                            name="Original plan",
                            value=f"Entry {signal.entry:g} · stop {signal.stop:g} · target {signal.tp1:g}",
                            inline=False,
                        ),
                    ],
                    footer=dict(text=f"{signal.id} · No account position or fill is assumed"),
                )
            ],
        )
        return await self.deliver(
            f"{channel}:{signal.id}:deteriorating:{episode_ms}", signal.id, payload, secret
        )

    async def send_public(self, signal, update=False):
        if not update and signal.evidence.get("delivery_policy") == "REDUNDANT_OPPORTUNITY":
            return "blocked:REDUNDANT_OPPORTUNITY"

        if update and not self.was_initially_delivered(signal.id, "validated"):
            return "blocked:no-visible-initial"

        from .ml.inference import delivery_eligible

        monitoring_event = (
            signal.coverage.get("monitoring_event")
            if update and signal.state not in {"INVALIDATED", "EXPIRED", "RESOLVED"}
            else None
        )
        if update:
            sent = self.store.db.execute(
                "SELECT 1 FROM outbox WHERE key=? AND status='sent'", (f"validated:{signal.id}:initial",)
            ).fetchone()

            if not sent or (
                signal.state not in {"EXPIRED", "INVALIDATED", "RESOLVED"}
                and monitoring_event not in {"paused", "resumed", "ended"}
            ):
                return "blocked: no prior validated delivery or terminal update"

        elif not delivery_eligible(signal, self.settings, self.store):
            return "blocked: no validated deployment model"

        from .quality_v22 import enabled as v22_enabled

        if not update and not v22_enabled(signal):
            recent = self.store.db.execute(
                "SELECT 1 FROM outbox o JOIN signals s ON s.id=o.signal_id WHERE s.symbol=? "
                "AND o.updated_ms>? AND o.status IN ('sent','uncertain','sending') LIMIT 1",
                (signal.symbol, now_ms() - self.settings.cooldown_minutes * 60_000),
            ).fetchone()

            if recent:
                return "cooldown"

        secret = (
            self.settings.monitoring_webhook
            if monitoring_event in {"paused", "resumed"}
            else self.settings.research_webhook
        ).get_secret_value()

        if not secret:
            return "dry-run"

        key = f"validated:{signal.id}:{signal.state if update else 'initial'}"

        if monitoring_event:
            key = f"validated:{signal.id}:monitoring:{monitoring_event}:{signal.coverage.get('pause_since_ms', 0)}"

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
