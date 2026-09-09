"""Untrusted chart observations -> durable inbox -> independently evaluated research card.

TradingView classified volume is never promoted to exchange taker-side evidence.
This single-worker queue shares the application's SQLite durability boundary.
"""

import asyncio
import hashlib
import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import DAY, Instrument, Signal
from .notifications import Notifier
from .risk import evaluate_risk
from .scoring import tier
from .storage import now_ms


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class Observation(Strict):
    direction: Literal["LONG", "SHORT"]
    family: Literal["tv_sweep_reclaim", "tv_continuation"]
    setup_ms: int = Field(gt=0)
    entry: float = Field(gt=0)
    stop: float = Field(gt=0)
    tp1: float = Field(gt=0)
    tp2: float = Field(gt=0)
    atr: float = Field(gt=0)
    regime_slope_atr: float
    regime_efficiency: float = Field(ge=0, le=1)
    level: float = Field(gt=0)
    setup_low: float = Field(gt=0)
    setup_high: float = Field(gt=0)
    setup_close: float = Field(gt=0)
    prior_close: float = Field(gt=0)
    bos_level: float = Field(gt=0)
    fvg_low: float | None = Field(None, gt=0)
    fvg_high: float | None = Field(None, gt=0)
    buy_volume: float | None = Field(None, ge=0)
    sell_volume: float | None = Field(None, ge=0)
    stacked_buy: int = Field(0, ge=0, le=10000)
    stacked_sell: int = Field(0, ge=0, le=10000)
    poc: float | None = Field(None, gt=0)
    val: float | None = Field(None, gt=0)
    vah: float | None = Field(None, gt=0)
    cvd: float | None = None
    prior_buy_volume: float | None = Field(None, ge=0)
    prior_sell_volume: float | None = Field(None, ge=0)
    prior_high: float | None = Field(None, gt=0)
    prior_low: float | None = Field(None, gt=0)
    turnover_median_7d: float = Field(ge=0)
    continuous_days: int = Field(ge=0)
    footprint_source: Literal["tradingview_intrabar_classification", "unavailable"]

    @model_validator(mode="after")
    def consistency(self):
        if not self.setup_low <= self.setup_close <= self.setup_high:
            raise ValueError("inconsistent setup OHLC")
        if (self.fvg_low is None) != (self.fvg_high is None):
            raise ValueError("both gap bounds required")
        if self.fvg_low is not None and self.fvg_low >= self.fvg_high:
            raise ValueError("invalid gap")
        if self.footprint_source != "unavailable":
            if any(v is None for v in (self.buy_volume, self.sell_volume, self.poc, self.val, self.vah)):
                raise ValueError("incomplete footprint")
            if self.buy_volume + self.sell_volume <= 0 or not self.val <= self.poc <= self.vah:
                raise ValueError("invalid volume profile")
        return self


class TVEvent(Strict):
    schema_version: Literal[1] = 1
    strategy_version: Literal["tv-1"] = "tv-1"
    event_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{8,160}$")
    signal_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{8,120}$")
    symbol: str = Field(pattern=r"^BYBIT:[A-Z0-9]{2,30}\.P$")
    event: Literal["setup", "heartbeat", "invalidate", "outcome", "connection_test"]
    source_ms: int = Field(gt=0)
    bar_close_ms: int = Field(gt=0)
    timeframe: Literal["15"] = "15"
    observation: Observation | None = None
    price: float = Field(gt=0)
    outcome_net_r: float | None = Field(None, ge=-100, le=100)

    @model_validator(mode="after")
    def consistency(self):
        if self.event == "setup" and self.observation is None:
            raise ValueError("setup observations required")
        if self.event != "setup" and self.observation is not None:
            raise ValueError("observations belong to setup events only")
        if self.event == "outcome" and self.outcome_net_r is None:
            raise ValueError("paper outcome required")
        if self.bar_close_ms % 900_000 or not 0 <= self.source_ms - self.bar_close_ms <= 90_000:
            raise ValueError("expected a recently closed 15-minute bar")
        if self.observation and (
            self.observation.setup_ms % 3_600_000
            or not 0 <= self.bar_close_ms - self.observation.setup_ms <= 3_600_000
        ):
            raise ValueError("setup must originate in the last confirmed hour")
        return self

    @property
    def native_symbol(self):
        return self.symbol[6:-2]

    @property
    def internal_id(self):
        return "tv-" + hashlib.sha256(f"{self.symbol}|{self.signal_id}".encode()).hexdigest()[:24]


class LiquidityObservation(Strict):
    """Operator-provided observed data, not a spread/depth assumption or chart proxy."""

    instrument: Instrument
    source: str = Field(min_length=8, max_length=300)
    observed_ms: int = Field(gt=0)
    continuous_days: int = Field(ge=0)
    median_turnover_7d: float = Field(ge=0)
    normal_spread_bps: float = Field(ge=0)
    spread_samples: int = Field(ge=12)
    bid: float = Field(gt=0)
    ask: float = Field(gt=0)
    executable_notional: float = Field(gt=0)
    long_impact_bps: float = Field(ge=0)
    short_impact_bps: float = Field(ge=0)

    @model_validator(mode="after")
    def valid_market(self):
        i = self.instrument
        if self.ask <= self.bid or i.tick <= 0 or i.qty_step <= 0:
            raise ValueError("invalid quote or instrument increments")
        if (
            i.metadata.get("contractType") != "LinearPerpetual"
            or i.metadata.get("status") != "Trading"
            or i.metadata.get("isPreListing", False)
        ):
            raise ValueError("only trading, non-premarket linear perpetuals")
        return self

    @property
    def valid(self):
        return True  # Time/source/liquidity validity checked before risk evaluation below.

    def impact(self, direction, notional):
        if notional > self.executable_notional:
            return None
        return {
            "impact_bps": self.long_impact_bps if direction == "LONG" else self.short_impact_bps,
            "source": self.source,
            "observed_ms": self.observed_ms,
        }


def fraction(value, full):
    return max(0.0, min(1.0, value / full))


def trapped_participants(o):
    """Failed execution-bar auction heuristic, not identified traders or forced exits."""
    total = (o.prior_buy_volume or 0) + (o.prior_sell_volume or 0)
    delta = ((o.prior_buy_volume or 0) - (o.prior_sell_volume or 0)) / total if total else None
    return {
        "available": delta is not None and o.prior_high is not None and o.prior_low is not None,
        "potential_trapped_buyers": bool(
            delta is not None
            and delta >= 0.2
            and o.prior_high is not None
            and o.prior_high > o.bos_level
            and o.entry < o.bos_level
        ),
        "potential_trapped_sellers": bool(
            delta is not None
            and delta <= -0.2
            and o.prior_low is not None
            and o.prior_low < o.bos_level
            and o.entry > o.bos_level
        ),
        "prior_delta_pct": 100 * delta if delta is not None else None,
        "definition": "Prior 15M excursion beyond structure with >=20% classified delta, then close back across level",
        "limitation": "Potential failed auction; positions, entries and trapped inventory are not observable",
    }


def evaluate(event, settings, liquidity=None, portfolio=None, at_ms=None):
    """Versioned TV-only rubric. No derivative/fundamental/native-flow credit is implied."""
    at_ms = now_ms() if at_ms is None else at_ms
    o = event.observation
    sign = 1 if o.direction == "LONG" else -1
    s = Signal(
        id=event.internal_id,
        symbol=event.native_symbol,
        source="tradingview",
        version="tv-1",
        direction=o.direction,
        family=o.family,
        created_ms=event.source_ms,
        expires_ms=o.setup_ms + 3_600_000,
        trigger_expires_ms=event.bar_close_ms + 900_000,
        holding_deadline_ms=event.bar_close_ms + 4 * 3_600_000,
        regime="trending up" if sign > 0 else "trending down",
        entry=o.entry,
        zone=(o.entry - 0.15 * o.atr, o.entry + 0.15 * o.atr),
        stop=o.stop,
        tp1=o.tp1,
        tp2=o.tp2,
        invalidation=f"Structure stop {o.stop:g}, expired plan or lost chart feed",
        reason=f"{o.family}: confirmed HTF structure and TradingView-classified execution volume",
    )
    gates = []
    aligned = sign * o.regime_slope_atr > 0 and o.regime_efficiency >= 0.2
    sweep = o.setup_low < o.level < o.setup_close if sign > 0 else o.setup_high > o.level > o.setup_close
    continuation = (
        o.prior_close > o.level and o.setup_low <= o.level < o.setup_close
        if sign > 0
        else o.prior_close < o.level and o.setup_high >= o.level > o.setup_close
    )
    structure = sweep if o.family == "tv_sweep_reclaim" else continuation
    shift = sign * (o.entry - o.bos_level) > 0
    if not aligned:
        gates.append("4H slope/efficiency not supportive")
    if not structure or not shift:
        gates.append("family structure / confirmed execution break missing")
    if not (o.stop < o.setup_low if sign > 0 else o.stop > o.setup_high):
        gates.append("stop does not invalidate setup structure")
    if sign * (o.tp2 - o.tp1) < 0:
        gates.append("targets not directionally ordered")
    if at_ms >= min(s.expires_ms, s.trigger_expires_ms) or at_ms - event.source_ms > settings.tv_max_age_ms:
        gates.append("stale trigger or expired setup")
    if not s.zone[0] <= event.price <= s.zone[1]:
        gates.append("price outside entry zone")
    total = (o.buy_volume or 0) + (o.sell_volume or 0)
    delta = 100 * ((o.buy_volume or 0) - (o.sell_volume or 0)) / total if total else 0
    stack = o.stacked_buy if sign > 0 else o.stacked_sell
    flow_ok = o.footprint_source != "unavailable" and sign * delta >= 10 and stack >= 3
    if not flow_ok:
        gates.append("TradingView footprint missing or execution confirmation contradicts setup")
    liquidity_ok = False
    if liquidity:
        liq = liquidity
        spread = (liq.ask - liq.bid) / ((liq.ask + liq.bid) / 2) * 10000
        liquidity_ok = (
            liq.instrument.symbol == s.symbol
            and liq.instrument.settle in settings.settle_coins
            and 0 <= at_ms - liq.observed_ms <= 60_000
            and at_ms - liq.instrument.launch_ms >= settings.min_age_days * DAY
            and liq.continuous_days >= settings.min_age_days
            and liq.median_turnover_7d >= settings.min_daily_turnover
            and liq.normal_spread_bps <= settings.max_spread_bps
            and spread <= settings.max_spread_bps
            and s.zone[0] <= (liq.ask + liq.bid) / 2 <= s.zone[1]
        )
        s.risk = evaluate_risk(s, liq.instrument, settings, liq, portfolio)
        gates.extend(s.risk["reasons"])
    else:
        s.risk = {"accepted": False, "reasons": ["observed instrument and executable liquidity missing"]}
    proxy = (
        not liquidity
        and settings.tv_proxy_research
        and o.continuous_days >= settings.min_age_days
        and o.turnover_median_7d >= settings.min_daily_turnover
        and settings.equity is None
    )
    if proxy:
        # Explicitly lower-tier, unsized chart research. No fictitious book, quantity or margin.
        loss, reward = sign * (o.entry - o.stop), sign * (o.tp1 - o.entry)
        cost = (
            o.entry
            * (2 * settings.taker_fee_bps + 2 * settings.slippage_bps + settings.funding_reserve_bps)
            / 10000
        )
        rr = (reward - cost) / (loss + cost) if loss > 0 else 0
        buffer_ok = (
            1 / settings.leverage - settings.maintenance_margin_assumption
            > settings.liquidation_buffer_multiple * ((loss + cost) / o.entry)
        )
        s.risk = {
            "accepted": loss > 0 and rr >= settings.min_net_rr and buffer_ok,
            "net_rr": rr,
            "gross_rr": reward / loss if loss > 0 else 0,
            "quantity": None,
            "risk_fraction": settings.risk_fraction,
            "cost_per_base": cost,
            "warning": "UNSIZED PROXY RESEARCH: turnover is not executable depth; spread and impact unobserved",
        }
    if not liquidity_ok and not proxy:
        gates.append(
            "fresh, source-attributed liquidity observations required; chart turnover is insufficient"
        )
    if not s.risk.get("accepted"):
        gates.append("risk sizing/cost gates failed")
    # Each correlated event is a single group. FVG is reported, not another vote for the same move.
    components = {
        "regime": 20 * min(fraction(sign * o.regime_slope_atr, 0.3), fraction(o.regime_efficiency, 0.5)),
        "structure": 25 * int(structure and shift),
        "tv_footprint": 30 * min(fraction(sign * delta, 25), fraction(stack, 4)) * int(total > 0),
        "risk": 15 * fraction(s.risk.get("net_rr", 0), 2.5) * int(s.risk.get("accepted", False)),
        "observed_liquidity": 10 * int(liquidity_ok),
    }
    s.gates = list(dict.fromkeys(gates))
    s.quality = round(sum(components.values()), 2)
    if proxy:
        s.quality = min(84, s.quality)  # Never use turnover to unlock high-conviction tiers.
    s.raw_tier = "F" if gates else tier(s.quality)
    s.final_tier = (
        "REJECTED"
        if gates
        else "SSS RESEARCH · UNCALIBRATED"
        if settings.sss_research and s.quality >= 95
        else "TV PROXY RESEARCH"
        if proxy
        else "RESEARCH"
    )
    s.state = "PENDING CONFIRMATION" if gates else "CONFIRMED"
    s.evidence = {
        "score_profile": "tv-evidence-1 (not comparable to native seven-factor score)",
        "score_components": components,
        "observations": o.model_dump(),
        "flow": {
            "source": o.footprint_source,
            "delta_pct": delta if total else None,
            "cvd": o.cvd,
            "stacked_buy": o.stacked_buy,
            "stacked_sell": o.stacked_sell,
            "poc": o.poc,
            "val": o.val,
            "vah": o.vah,
        },
        "liquidity": liquidity.model_dump(mode="json") if liquidity else None,
        "derivatives": {},
        "native_orderflow": "Unavailable; not inferred from TradingView",
    }
    s.evidence["trapped_participants"] = trapped_participants(o)
    s.coverage = {
        "chart": "source-attested closed bars; gateway cannot independently verify Pine history",
        "native_taker_trades": False,
        "liquidity_observed": liquidity_ok,
        "absorption": "Unavailable: passive replenishment not supplied by TradingView",
    }
    s.qualification = {
        "status": "Uncalibrated",
        "probability": None,
        "validated": False,
        "reason": "Research rubric only; no approved out-of-sample model",
    }
    return s


class Gateway:
    def __init__(self, settings, store, transport=None):
        self.settings, self.store = settings, store
        self.scanner = None
        self.notifier = Notifier(settings, store, transport)
        self.wake = asyncio.Event()
        with store.db:
            store.db.execute("UPDATE tv_inbox SET status='pending' WHERE status='processing'")

    def native_observations(self, event):
        from .liquidity import SpreadHistory
        from .orderflow import Book, footprint

        scanner, now = self.scanner, now_ms()
        missing = {"available": False, "reason": "Bybit collector unavailable or incomplete"}
        if not scanner or not scanner.recorder.healthy or not scanner.streams.connected:
            return None, missing
        c = scanner.context.get(event.native_symbol)
        book = scanner.streams.books.get(event.native_symbol)
        tape = scanner.streams.tapes.get(event.native_symbol)
        if not c or now - c["asof"] > 960_000 or not book or not book.fresh(now, self.settings.book_stale_ms):
            return None, missing
        normal = SpreadHistory(self.store.get("spreads:" + event.native_symbol, [])).assess(
            now, self.settings.max_spread_bps, self.settings.spread_min_samples
        )
        member = self.store.membership_asof(event.native_symbol, now)
        liq = None
        if normal["eligible"] and member and member["eligible"]:
            point = json.loads(member["payload"])
            amount = self.settings.hypothetical_notional
            if self.settings.equity:
                o = event.observation
                amount = (
                    self.settings.equity
                    * self.settings.risk_fraction
                    * o.entry
                    / max(abs(o.entry - o.stop), 1e-12)
                )
            impacts = [book.impact(side, amount) for side in ("LONG", "SHORT")]
            if all(impacts):
                liq = LiquidityObservation(
                    instrument=c["instrument"],
                    source="Bybit public reconstructed book + observed spread history",
                    observed_ms=now,
                    continuous_days=point.get("continuous_daily_bars", 0),
                    median_turnover_7d=point.get("turnover_median_7d", 0),
                    normal_spread_bps=normal.get("median_bps", 0),
                    spread_samples=self.settings.spread_min_samples,
                    bid=float(max(book.bids)),
                    ask=float(min(book.asks)),
                    executable_notional=amount,
                    long_impact_bps=impacts[0]["impact_bps"],
                    short_impact_bps=impacts[1]["impact_bps"],
                )
        start, end = event.bar_close_ms - 900_000, event.bar_close_ms
        if (
            not tape
            or not 0 < tape.coverage_start <= start
            or not 0 <= now - tape.last_receipt <= self.settings.trade_stale_ms
        ):
            return liq, missing
        historical_book = Book()
        historical_book.valid = book.valid
        historical_book.changes.extend(x for x in book.changes if x[0] <= end)
        flow = footprint(
            tape.window(start, end), c["instrument"].tick, event.observation.atr, historical_book
        )
        derivatives = (
            c["derivatives"] if now - c["derivatives"].get("ticker_observed_ms", 0) <= 90_000 else {}
        )
        return liq, {
            "available": bool(flow.get("available")),
            "flow": flow,
            "derivatives": derivatives,
            "liquidations": list(scanner.streams.liquidations.get(event.native_symbol, []))[-50:],
        }

    def enqueue(self, event):
        now = now_ms()
        if not -5000 <= now - event.source_ms <= self.settings.tv_max_age_ms:
            raise ValueError("source timestamp stale or in future")
        if event.native_symbol not in self.settings.tv_symbols:
            raise ValueError("symbol not approved in FLOW_TV_SYMBOLS")
        body = event.model_dump_json()
        digest = hashlib.sha256(body.encode()).hexdigest()
        row = self.store.db.execute(
            "SELECT digest FROM tv_inbox WHERE event_id=?", (event.event_id,)
        ).fetchone()
        if row:
            if row[0] != digest:
                raise ValueError("event ID reused with different payload")
            return False
        size = sum(p.stat().st_size for p in self.store.root.glob("research.sqlite*") if p.is_file())
        if size > self.settings.max_storage_gb * 1_000_000_000:
            raise OverflowError("database storage budget reached; archive before continuing")
        if self.store.db.execute("SELECT count(*) FROM tv_inbox").fetchone()[0] >= 100_000:
            raise OverflowError("inbox retention budget reached; export and archive before continuing")
        pending = self.store.db.execute(
            "SELECT count(*) FROM tv_inbox WHERE status IN ('pending','processing')"
        ).fetchone()[0]
        if pending >= self.settings.tv_queue_limit:
            raise OverflowError("durable ingress queue full")
        with self.store.db:
            self.store.db.execute(
                "INSERT INTO tv_inbox VALUES(?,?,?,?,?,?)",
                (event.event_id, digest, now, body, "pending", None),
            )
        self.wake.set()
        return True

    async def process_one(self):
        row = self.store.db.execute(
            "SELECT * FROM tv_inbox WHERE status='pending' ORDER BY received_ms,rowid LIMIT 1"
        ).fetchone()
        if not row:
            return False
        with self.store.db:
            self.store.db.execute(
                "UPDATE tv_inbox SET status='processing' WHERE event_id=?", (row["event_id"],)
            )
        try:
            result = await self.process(TVEvent.model_validate_json(row["payload"]))
            status = "done"
        except (ValueError, KeyError) as exc:
            result, status = type(exc).__name__, "rejected"
        except Exception as exc:
            # Durable operator-visible failure; do not retry ambiguous external writes blindly.
            result, status = type(exc).__name__, "error"
        with self.store.db:
            self.store.db.execute(
                "UPDATE tv_inbox SET status=?,result=? WHERE event_id=?", (status, result, row["event_id"])
            )
        return True

    async def process(self, event):
        now = now_ms()
        if not -5000 <= now - event.source_ms <= self.settings.tv_max_age_ms:
            return "expired in queue"
        if event.event == "connection_test":
            return await self.notifier.send_connection_test(event.event_id, event.source_ms)
        row = self.store.db.execute("SELECT payload FROM signals WHERE id=?", (event.internal_id,)).fetchone()
        if event.event == "setup":
            if row:
                return "setup already evaluated; IDs cannot mutate or resurrect a plan"
            raw = self.store.get("tv_liquidity:" + event.native_symbol)
            liquidity = LiquidityObservation.model_validate(raw) if raw else None
            native_liquidity, native = self.native_observations(event)
            liquidity = native_liquidity or liquidity
            portfolio = self.store.get("portfolio")
            if portfolio and now - portfolio["at_ms"] > DAY:
                portfolio = None
            signal = evaluate(event, self.settings, liquidity, portfolio, now)
            signal.evidence["optional_native"] = native
            signal.coverage["native_taker_trades"] = native["available"]
            if native["available"]:
                signal.evidence["native_orderflow"] = (
                    "Actual Bybit public trades; separate from TradingView classification"
                )
            from .fundamentals import facts_asof

            base = (
                liquidity.instrument.base
                if liquidity
                else event.native_symbol.removesuffix("USDT").removesuffix("USDC")
            )
            facts = facts_asof(self.store, base, now)
            signal.evidence["fundamentals"] = facts
            if any(f["major_event"] for f in facts):
                signal.gates.append("known major asset event")
                signal.final_tier, signal.state = "REJECTED", "PENDING CONFIRMATION"
            if self.settings.tv_require_native_confirmation:
                direction = 1 if signal.direction == "LONG" else -1
                delta = native.get("flow", {}).get("delta_pct", 0)
                if not native["available"] or direction * delta < 10:
                    signal.gates.append("required native executed delta missing or contradictory")
                    signal.final_tier, signal.state = "REJECTED", "PENDING CONFIRMATION"
            from .ml.inference import apply as apply_ml

            if signal.gates:
                signal.raw_tier = "F"
            apply_ml(signal, self.settings, self.store, now_ms())
            self.store.signal(signal, "TradingView observation evaluated")
            self.store.put("tv_seen:" + signal.id, event.source_ms)
            if not signal.gates:
                result = await self.notifier.send_research(signal)
                if result == "sent":
                    signal.state = "ALERTED"
                    self.store.signal(signal, "Discord research card delivered; no execution")
                return result
            return "rejected: " + "; ".join(signal.gates)
        if not row:
            raise ValueError("unknown setup")
        signal = Signal.model_validate_json(row[0])
        last = self.store.get("tv_seen:" + signal.id, signal.created_ms)
        if event.source_ms < last:
            return "out-of-order lifecycle event"
        self.store.put("tv_seen:" + signal.id, event.source_ms)
        if event.event == "outcome":
            if event.bar_close_ms <= signal.created_ms:
                raise ValueError("outcome must follow the trigger bar")
            payload = {
                "event_id": event.event_id,
                "signal_id": signal.id,
                "at_ms": now,
                "hypothetical_net_r": event.outcome_net_r,
                "source": "TradingView paper outcome; unverified fills/costs, excluded from calibration",
            }
            with self.store.db:
                self.store.db.execute(
                    "INSERT INTO journal(at_ms,payload) SELECT ?,? WHERE NOT EXISTS "
                    "(SELECT 1 FROM journal WHERE json_extract(payload,'$.event_id')=?)",
                    (now, json.dumps(payload), event.event_id),
                )
            if signal.state == "ALERTED":
                signal.state = "RESOLVED"
                self.store.signal(signal, payload["source"])
                await self.notifier.send_research(signal, update=True)
            return "paper outcome journaled; never used as verified execution"
        breached = event.price <= signal.stop if signal.direction == "LONG" else event.price >= signal.stop
        if signal.state not in {"INVALIDATED", "EXPIRED", "RESOLVED"} and (
            event.event == "invalidate" or breached
        ):
            notified = signal.state == "ALERTED"
            signal.state = "INVALIDATED"
            signal.invalidation = "TradingView invalidation or observed stop breach"
            self.store.signal(signal, signal.invalidation)
            if notified:
                await self.notifier.send_research(signal, update=True)
        return signal.state

    async def maintain(self):
        now = now_ms()
        rows = self.store.db.execute(
            "SELECT payload FROM signals WHERE state NOT IN ('INVALIDATED','EXPIRED','RESOLVED')"
        ).fetchall()
        for row in rows:
            signal = Signal.model_validate_json(row[0])
            if signal.source != "tradingview":
                continue
            was_alerted = signal.state == "ALERTED"
            if now >= signal.expires_ms:
                signal.state = "EXPIRED"
            elif (
                now - self.store.get("tv_seen:" + signal.id, signal.created_ms)
                > 900_000 + self.settings.tv_max_age_ms
            ):
                signal.state = "INVALIDATED"
                signal.invalidation = "TradingView heartbeat missing; chart coverage lost"
            if signal.state in {"EXPIRED", "INVALIDATED"}:
                self.store.signal(signal, "setup validity ended; not a paper position closure")
                if was_alerted:
                    await self.notifier.send_research(signal, update=True)
        # Reconcile a crash after Discord acknowledged a card but before signal state was saved.
        for row in self.store.db.execute(
            "SELECT s.payload FROM signals s JOIN outbox o ON s.id=o.signal_id "
            "WHERE s.state='CONFIRMED' AND o.status='sent' AND o.key LIKE '%:initial'"
        ).fetchall():
            signal = Signal.model_validate_json(row[0])
            if signal.source == "tradingview":
                signal.state = "ALERTED"
                self.store.signal(signal, "recovered durable Discord acknowledgement")

    async def run(self):
        while True:
            self.wake.clear()
            await self.process_one()
            await self.maintain()
            pending = self.store.db.execute(
                "SELECT 1 FROM tv_inbox WHERE status='pending' LIMIT 1"
            ).fetchone()
            if not pending:
                try:
                    await asyncio.wait_for(self.wake.wait(), 5)
                except TimeoutError:
                    pass
