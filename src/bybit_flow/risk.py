import math
from decimal import ROUND_FLOOR, Decimal


def evaluate_risk(signal, instrument, settings, book, portfolio=None, funding_rate=None):
    reasons = []
    sign = 1 if signal.direction == "LONG" else -1
    entry, stop = signal.entry, signal.stop
    loss, reward = sign * (entry - stop), sign * (signal.tp1 - entry)
    if loss <= 0 or reward <= 0 or not all(math.isfinite(v) for v in (entry, stop, reward)):
        return {"accepted": False, "reasons": ["undefined stop or target"]}
    if not signal.zone[0] <= entry <= signal.zone[1]:
        reasons.append("entry outside planned zone")
    fee = settings.taker_fee_bps / 10000
    # Four-hour horizon: reserve for every possible settlement, with unknown schedule over-reserved.
    settlements = math.ceil(signal.expected_hold_max / max(1, instrument.funding_interval_minutes))
    funding_bps = max(
        settings.funding_reserve_bps,
        abs(funding_rate) * 10000 * settlements if funding_rate is not None else 0,
    )
    cost_per_base = (
        fee * (entry + max(stop, signal.tp1)) + entry * (2 * settings.slippage_bps + funding_bps) / 10000
    )
    net_rr = (reward - cost_per_base) / (loss + cost_per_base)
    if net_rr < settings.min_net_rr:
        reasons.append("net reward:risk below minimum")
    leverage = min(settings.leverage, instrument.max_leverage)
    stress_distance = 1 / leverage - settings.maintenance_margin_assumption
    if stress_distance <= settings.liquidation_buffer_multiple * (loss / entry + cost_per_base / entry):
        reasons.append("illustrative liquidation buffer inadequate")
    result = dict(
        gross_rr=reward / loss,
        net_rr=net_rr,
        cost_per_base=cost_per_base,
        fee_bps_assumption=settings.taker_fee_bps,
        funding_reserve_bps=funding_bps,
        risk_fraction=settings.risk_fraction,
        illustrative_leverage=leverage,
        liquidation_model="isolated-margin stress approximation; not Bybit account liquidation price",
        maintenance_margin_assumption=settings.maintenance_margin_assumption,
        adverse_mark_distance_fraction=stress_distance,
        warning="Stops, fills and maximum losses are not guaranteed in gaps or extreme volatility",
    )
    if settings.equity:
        budget = settings.equity * settings.risk_fraction
        qty = (Decimal(str(budget / (loss + cost_per_base))) / instrument.qty_step).to_integral_value(
            rounding=ROUND_FLOOR
        ) * instrument.qty_step
        notional = float(qty) * entry
        result.update(
            quantity=str(qty),
            notional=notional,
            estimated_margin=notional / leverage,
            estimated_loss=float(qty) * (loss + cost_per_base),
            risk_budget=budget,
            estimated_cost=float(qty) * cost_per_base,
        )
        if qty < instrument.min_qty or qty > instrument.max_qty or notional < float(instrument.min_notional):
            reasons.append("quantity outside current instrument limits")
        if notional / leverage + float(qty) * cost_per_base > settings.equity:
            reasons.append("insufficient illustrative margin")
        if portfolio is None:
            reasons.append("equity configured but current manual portfolio snapshot missing")
        else:
            if portfolio.get("daily_loss_fraction", 0) >= settings.daily_loss_limit:
                reasons.append("daily loss limit")
            if portfolio.get("weekly_loss_fraction", 0) >= settings.weekly_loss_limit:
                reasons.append("weekly loss limit")
            # Conservative: all crypto positions share one correlation group.
            if (
                sum(p["risk_fraction"] for p in portfolio.get("positions", [])) + settings.risk_fraction
                > settings.correlated_risk_limit
            ):
                reasons.append("combined correlated risk limit")
    else:
        notional = settings.hypothetical_notional
        result.update(
            quantity=None,
            notional=notional,
            estimated_margin=None,
            portfolio_status="not assessed; no equity configured",
        )
    impact = book.impact(signal.direction, notional) if book and book.valid else None
    if not impact:
        reasons.append("insufficient executable visible depth")
    elif impact["impact_bps"] > settings.slippage_bps + settings.max_spread_bps / 2:
        reasons.append("estimated entry impact exceeds cost allowance")
    result.update(impact=impact, accepted=not reasons, reasons=reasons)
    return result
