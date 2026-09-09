"""Event-level hypothetical market fills, conservative participation, costs and time stops."""

from dataclasses import dataclass, field

from .models import Signal, Trade


@dataclass
class PaperPosition:
    signal: Signal
    requested_qty: float
    latency_ms: int = 500
    participation: float = 0.01
    fill_window_ms: int = 60_000
    horizon_ms: int = 14_400_000
    fee_bps: float = 5.5
    slippage_bps: float = 2
    quantity: float = 0
    entry_value: float = 0
    fees: float = 0
    funding: float = 0
    exit_value: float = 0
    exited_qty: float = 0
    exit_reason: str | None = None
    exit_ms: int | None = None
    fills: list = field(default_factory=list)
    data_gaps: list = field(default_factory=list)
    mfe_r: float = 0
    mae_r: float = 0
    funding_timestamps: set = field(default_factory=set)

    def __post_init__(self):
        if self.requested_qty <= 0 or not 0 < self.horizon_ms <= 14_400_000:
            raise ValueError("Positive quantity and at most four-hour paper horizon required")

    @property
    def sign(self):
        return 1 if self.signal.direction == "LONG" else -1

    @property
    def entry(self):
        return self.entry_value / self.quantity if self.quantity else self.signal.entry

    @property
    def remaining(self):
        return max(0, self.quantity - self.exited_qty)

    def on_trade(self, trade: Trade):
        if trade.symbol != self.signal.symbol or self.exit_ms is not None:
            return
        t, price = trade.event_ms, float(trade.price)
        ready = self.signal.created_ms + self.latency_ms
        if t < ready:
            return
        if self.quantity:
            distance = abs(self.signal.entry - self.signal.stop)
            excursion = self.sign * (price - self.entry) / distance if distance else 0
            self.mfe_r = max(self.mfe_r, excursion)
            self.mae_r = min(self.mae_r, excursion)
        can_enter = t <= min(ready + self.fill_window_ms, self.signal.expires_ms) and self.exit_reason is None
        if (
            can_enter
            and self.quantity < self.requested_qty
            and self.signal.zone[0] <= price <= self.signal.zone[1]
        ):
            q = min(self.requested_qty - self.quantity, float(trade.size) * self.participation)
            fill = price * (1 + self.sign * self.slippage_bps / 10000)
            self.quantity += q
            self.entry_value += q * fill
            self.fees += q * fill * self.fee_bps / 10000
            self.fills.append(dict(at_ms=t, kind="entry", quantity=q, price=fill))
            return  # Do not enter and exit against the very same printed trade.
        if not self.quantity:
            if t > ready + self.fill_window_ms:
                self.exit_reason, self.exit_ms = "missed_fill", t
            return
        if self.exit_reason is None:
            if self.sign * (price - self.signal.stop) <= 0:
                self.exit_reason = "stop"
            elif self.sign * (price - self.signal.tp1) >= 0:
                self.exit_reason = "tp1"
            elif t >= self.signal.created_ms + self.horizon_ms:
                self.exit_reason = "time_stop"
        if self.exit_reason:
            q = min(self.remaining, float(trade.size) * self.participation)
            # Gaps exit at observed price plus adverse slippage, never guaranteed stop price.
            fill = price * (1 - self.sign * self.slippage_bps / 10000)
            self.exit_value += q * fill
            self.exited_qty += q
            self.fees += q * fill * self.fee_bps / 10000
            self.fills.append(dict(at_ms=t, kind="exit", quantity=q, price=fill))
            if self.remaining < 1e-10:
                self.exit_ms = t

    def on_funding(self, timestamp, rate, mark):
        if (
            self.quantity
            and self.remaining
            and timestamp >= self.fills[0]["at_ms"]
            and timestamp not in self.funding_timestamps
        ):
            self.funding += self.sign * self.remaining * mark * rate
            self.funding_timestamps.add(timestamp)

    def on_mark(self, timestamp, mark, leverage=3, maintenance=0.01):
        if self.remaining and self.sign * (mark - self.entry) / self.entry <= -(1 / leverage - maintenance):
            # Stress breach only; actual account-dependent liquidation is not reconstructed.
            self.exit_reason = "mark_margin_stress"

    def outcome(self):
        complete = (
            self.exit_ms is not None and self.quantity > 0 and self.remaining < 1e-10 and not self.data_gaps
        )
        net = (
            self.sign * (self.exit_value - self.entry_value) - self.fees - self.funding if complete else None
        )
        planned_risk = self.quantity * abs(self.signal.entry - self.signal.stop)
        return dict(
            signal_id=self.signal.id,
            family=self.signal.family,
            direction=self.signal.direction,
            regime=self.signal.regime,
            entry_ms=self.signal.created_ms,
            actual_entry_ms=self.fills[0]["at_ms"] if self.quantity else None,
            planned_entry=self.signal.entry,
            simulated_entry=self.entry if self.quantity else None,
            stop=self.signal.stop,
            target=self.signal.tp1,
            mfe_r=self.mfe_r,
            mae_r=self.mae_r,
            classification=("win" if net > 1e-10 else "loss" if net < -1e-10 else "breakeven")
            if net is not None
            else "incomplete",
            exit_ms=self.exit_ms,
            cluster=self.signal.created_ms // 604_800_000,
            quantity=self.quantity,
            requested_qty=self.requested_qty,
            filled_fraction=self.quantity / self.requested_qty,
            exit_reason=self.exit_reason,
            complete=complete,
            net_pnl=net,
            net_r=net / planned_risk if net is not None and planned_risk else None,
            fees=self.fees,
            funding=self.funding,
            fills=self.fills,
            data_gaps=self.data_gaps,
        )


def performance(outcomes):
    from .calibration import clustered_statistics

    completed = [x for x in outcomes if x["complete"]]
    stats = clustered_statistics(completed)
    curve, peak, dd = 0.0, 0.0, 0.0
    for row in sorted(completed, key=lambda r: r["exit_ms"]):
        curve += row["net_r"]
        peak = max(peak, curve)
        dd = max(dd, peak - curve)
    return stats | {
        "max_drawdown_additive_r": dd,
        "incomplete_or_unfilled": len(outcomes) - len(completed),
        "outcome": "whole-position exit at TP1, stop or four-hour time stop; TP2 is informational",
        "limitations": "1% printed-volume participation is an execution assumption, not observed own fills",
    }
