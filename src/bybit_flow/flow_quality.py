"""Causal, descriptive quality of public executed prints (flow-quality-v1)."""

import math
from collections import Counter, defaultdict
from statistics import mean, median, pstdev

from .orderflow import value_area

POLICY = "flow-quality-v1"
WEIGHTS = {
    "INFORMATIVE_DIRECTIONAL_FLOW": 1.0,
    "GENUINE_ABSORPTION": 1.0,
    "NORMAL_TWO_SIDED_AUCTION": 0.85,
    "LOW_INFORMATION_VOLUME": 0.45,
    "REPETITIVE_TWO_SIDED_CHURN": 0.2,
    "LIQUIDITY_DRIVEN_REPRICING": 0.9,
}


def _fraction(numerator, denominator):
    return numerator / denominator if denominator else 0.0


def _percentile(prior, current):
    return _fraction(sum(value <= current for value in prior), len(prior)) if len(prior) >= 20 else None


def assess(trades, tick, book, raw, available_ms, start_ms, end_ms, baseline=(), cross=None):
    """Return quality and weighted levels; never alter trades or raw profile."""
    result = dict(
        flow_quality_policy=POLICY,
        flow_quality_state="UNAVAILABLE",
        flow_trust_score=None,
        available_ms=available_ms,
        window_start_ms=start_ms,
        window_end_ms=end_ms,
        baseline_sample_count=len(baseline),
    )
    rows = sorted(
        (t for t in trades if start_ms <= t.event_ms < end_ms and t.receipt_ms <= available_ms),
        key=lambda t: (t.event_ms, t.trade_id),
    )
    if not rows or not raw.get("available") or end_ms > available_ms:
        return result
    prices = [float(t.price) for t in rows]
    sizes = [float(t.size) for t in rows]
    notionals = [p * q for p, q in zip(prices, sizes)]
    sides = [1 if t.side == "Buy" else -1 for t in rows]
    gross = sum(notionals)
    buy = sum(v for v, sign in zip(notionals, sides) if sign > 0)
    sell = gross - buy
    signed = buy - sell
    ratio = _fraction(signed, gross)
    price = max(prices[0], 1e-12)
    displacement = 10000 * (prices[-1] - prices[0]) / price
    realized_range = 10000 * (max(prices) - min(prices)) / price
    alternations = [i for i in range(1, len(rows)) if sides[i] != sides[i - 1]]
    alternation = _fraction(len(alternations), len(rows) - 1)
    same_price = _fraction(
        sum(abs(prices[i] - prices[i - 1]) <= float(tick) * 2 for i in alternations), len(alternations)
    )
    size_classes = Counter(round(math.log(max(size, 1e-12)), 2) for size in sizes)
    repeat = _fraction(sum(count for count in size_classes.values() if count >= 3), len(rows))
    concentration = _fraction(max(size_classes.values()), len(rows))
    entropy = -sum((count / len(rows)) * math.log(count / len(rows)) for count in size_classes.values())
    entropy = _fraction(entropy, math.log(max(2, len(size_classes))))
    paired = set()
    for i in alternations:
        if i - 1 in paired or i in paired:
            continue
        if (
            rows[i].event_ms - rows[i - 1].event_ms <= 2000
            and abs(sizes[i] - sizes[i - 1]) <= 0.02 * max(sizes[i], sizes[i - 1])
            and abs(prices[i] - prices[i - 1]) <= 2 * float(tick)
        ):
            paired.update((i - 1, i))
    mirror_pairs = len(paired) // 2
    mirror_notional = sum(notionals[i] for i in paired)
    mirror_ratio = _fraction(mirror_notional, gross)
    intervals = [(b.event_ms - a.event_ms) / 1000 for a, b in zip(rows, rows[1:])]
    typical_interval = median(intervals) if intervals else None
    interval_cv = (
        _fraction(pstdev(intervals), mean(intervals)) if len(intervals) > 1 and mean(intervals) > 0 else None
    )
    periodicity = (
        _fraction(
            sum(abs(d - typical_interval) <= max(0.01, 0.1 * typical_interval) for d in intervals),
            len(intervals),
        )
        if intervals
        else None
    )
    depletion = book.get("depletion_60s", {})
    replenishment = book.get("replenishment_60s", {})
    bid_depletion, ask_depletion = depletion.get("bid", 0), depletion.get("ask", 0)
    bid_replenishment, ask_replenishment = replenishment.get("bid", 0), replenishment.get("ask", 0)
    directional_depletion = ask_depletion if signed > 0 else bid_depletion
    book_consistency = _fraction(
        directional_depletion,
        directional_depletion + (ask_replenishment if signed > 0 else bid_replenishment),
    )
    response = raw.get("directional_price_change")
    if response is None:
        response = (prices[-1] - prices[0]) * (1 if signed >= 0 else -1)
    persistence = raw.get("impact_persistence")
    if persistence is None:
        persistence = 0.0
    efficient = abs(ratio) >= 0.2 and abs(displacement) >= 3 and response > 0
    absorbed = (
        abs(ratio) >= 0.2
        and (raw.get("absorption_long") if signed < 0 else raw.get("absorption_short"))
        and (raw.get("potential_trapped_sellers") if signed < 0 else raw.get("potential_trapped_buyers"))
    )
    values = [r.get("gross_notional", 0) for r in baseline]
    gross_pct = _percentile(values, gross)
    churn = (
        len(rows) >= 20
        and alternation >= 0.7
        and same_price >= 0.7
        and repeat >= 0.6
        and mirror_pairs >= 8
        and mirror_ratio >= 0.35
        and abs(ratio) <= 0.12
        and abs(displacement) <= 3
        and book_consistency < 0.4
        and (gross_pct is None or gross_pct >= 0.5)
    )
    cross_price = bool(
        cross
        and cross.get("available")
        and 0 <= available_ms - cross.get("receipt_ms", -1) <= 15_000
        and cross.get("wick_context") == "MARKET_WIDE_SWEEP"
    )
    thin = book.get("depth", {}).get("10", {}).get("bid" if signed < 0 else "ask", 0)
    repricing = (
        abs(displacement) >= 10
        and response >= 0
        and (gross_pct <= 0.5 if gross_pct is not None else len(rows) <= 10)
        and (directional_depletion > 0 or book.get("spread_bps", 0) >= 5)
        and (cross_price or book_consistency >= 0.5 or thin < gross)
    )
    if absorbed:
        state = "GENUINE_ABSORPTION"
    elif churn:
        state = "REPETITIVE_TWO_SIDED_CHURN"
    elif repricing:
        state = "LIQUIDITY_DRIVEN_REPRICING"
    elif efficient:
        state = "INFORMATIVE_DIRECTIONAL_FLOW"
    elif (
        len(rows) >= 20
        and abs(ratio) < 0.1
        and abs(displacement) < 3
        and (gross_pct is None or gross_pct >= 0.5)
    ):
        state = "LOW_INFORMATION_VOLUME"
    else:
        state = "NORMAL_TWO_SIDED_AUCTION"
    weights = [
        0.2
        if state == "REPETITIVE_TWO_SIDED_CHURN" and i in paired
        else 0.35
        if state == "REPETITIVE_TWO_SIDED_CHURN"
        else 0.3
        if state == "LOW_INFORMATION_VOLUME" and i in paired
        else 0.6
        if state == "LOW_INFORMATION_VOLUME"
        else WEIGHTS[state]
        for i in range(len(rows))
    ]
    effective_buy = sum(v * w for v, w, s in zip(notionals, weights, sides) if s > 0)
    effective_sell = sum(v * w for v, w, s in zip(notionals, weights, sides) if s < 0)
    levels = defaultdict(float)
    bucket = float(raw.get("bucket") or tick)
    for p, q, w in zip(prices, sizes, weights):
        levels[round(math.floor(p / bucket) * bucket, 12)] += q * w
    effective = value_area(levels) if levels else dict(poc=None, vah=None, val=None)
    typical_level = median(levels.values()) if levels else 0
    effective_gross = effective_buy + effective_sell
    participation = _fraction(effective_gross, gross)
    trust = WEIGHTS[state]
    result.update(
        flow_quality_state=state,
        flow_trust_score=trust,
        gross_notional=gross,
        buy_notional=buy,
        sell_notional=sell,
        net_signed_notional=signed,
        net_to_gross_ratio=ratio,
        price_displacement_bps=displacement,
        realized_range_bps=realized_range,
        price_impact_per_million=_fraction(displacement * 1_000_000, abs(signed)),
        flow_efficiency=_fraction(abs(displacement), gross),
        impact_persistence=persistence,
        reversal_after_flow=bool(raw.get("reversal_after_flow")),
        aggressor_side_alternation_rate=alternation,
        same_price_alternation_rate=same_price,
        same_size_repeat_ratio=repeat,
        size_entropy=entropy,
        size_concentration=concentration,
        mirror_pair_count=mirror_pairs,
        mirror_pair_notional=mirror_notional,
        mirror_pair_ratio=mirror_ratio,
        trade_interval_cv=interval_cv,
        trade_interval_periodicity=periodicity,
        book_response_consistency=book_consistency,
        bid_depletion_after_selling=bid_depletion,
        ask_depletion_after_buying=ask_depletion,
        bid_replenishment=bid_replenishment,
        ask_replenishment=ask_replenishment,
        replenishment_asymmetry=_fraction(
            bid_replenishment - ask_replenishment, bid_replenishment + ask_replenishment
        ),
        effective_cvd_ratio=_fraction(effective_buy - effective_sell, effective_gross),
        effective_price_response=_fraction(displacement, effective_gross),
        effective_buy_notional=effective_buy,
        effective_sell_notional=effective_sell,
        effective_gross_notional=effective_gross,
        effective_delta_notional=effective_buy - effective_sell,
        effective_trade_intensity=_fraction(sum(weights), max(1, (end_ms - start_ms) / 1000)),
        effective_volume_ratio=participation,
        effective_volume_profile=[dict(price=p, volume=v) for p, v in sorted(levels.items())],
        effective_poc=effective["poc"],
        effective_vah=effective["vah"],
        effective_val=effective["val"],
        effective_hvn=[p for p, v in levels.items() if v >= 1.5 * typical_level],
        effective_lvn=[p for p, v in levels.items() if v <= 0.5 * typical_level],
        raw_vs_effective_profile_shift=effective["poc"] - raw["poc"]
        if effective["poc"] is not None and raw.get("poc") is not None
        else None,
        profile_confidence="LOW" if len(rows) < 20 or participation < 0.3 else "HIGH",
        price_change_per_effective_volume=_fraction(displacement, effective_gross),
        depth_consumption=directional_depletion,
        spread_expansion=book.get("spread_bps"),
        microprice_displacement=book.get("microprice_minus_mid"),
        cross_venue_price_agreement=cross_price,
        baseline_percentiles={
            k: _percentile([r[k] for r in baseline if r.get(k) is not None], v)
            for k, v in [
                ("aggressor_side_alternation_rate", alternation),
                ("mirror_pair_ratio", mirror_ratio),
                ("net_to_gross_ratio", ratio),
                ("effective_volume_ratio", participation),
            ]
        },
    )
    return result


def baseline(store, source, symbol, session, horizon, end_ms):
    key = f"flow-quality-baseline-v1:{source}:{symbol}:{session}:{horizon}"
    return key, [r for r in store.get(key, []) if r.get("window_end_ms", 0) < end_ms]


def remember(store, key, history, result):
    if result["flow_quality_state"] == "UNAVAILABLE":
        return
    compact = {
        k: result.get(k)
        for k in (
            "flow_quality_state",
            "flow_trust_score",
            "gross_notional",
            "aggressor_side_alternation_rate",
            "same_price_alternation_rate",
            "same_size_repeat_ratio",
            "mirror_pair_ratio",
            "net_to_gross_ratio",
            "price_impact_per_million",
            "flow_efficiency",
            "effective_volume_ratio",
            "size_entropy",
        )
    }
    compact["window_end_ms"] = result["window_end_ms"]
    if not history or history[-1]["window_end_ms"] < compact["window_end_ms"]:
        store.put(key, (history + [compact])[-120:])
