"""Cheap causal scanner priorities, separate from quality and probabilities."""

from .features import candle_features
from .scoring import unit


def horizon_pre_ranks(
    daily, context_bars, setup_bars, execution_bars, now, spread, max_spread, derivatives_available=False
):
    df, cf, sf, ef = [candle_features(b, now) for b in (daily, context_bars, setup_bars, execution_bars)]
    liquidity = unit(1 - spread / max_spread)

    def rank(context, setup):
        return round(
            25 * (context["regime"] not in {"uncertain", "high-volatility disorder"})
            + 30 * unit(setup["efficiency"], 0.5)
            + 20 * unit(setup["volume_expansion"], 2)
            + 15 * liquidity
            + 10 * unit(context["efficiency"], 0.5),
            2,
        )

    ranks = {
        "SHORT_INTRADAY": rank(sf, ef),
        "CORE_INTRADAY": rank(cf, sf),
        "SWING": round(0.95 * rank(df, cf) + 5 * bool(derivatives_available), 2),
    }
    best = max(ranks, key=ranks.get)
    return {
        **{"pre_rank_" + k.lower(): v for k, v in ranks.items()},
        "best_pre_rank": ranks[best],
        "best_pre_rank_horizon": best,
        "rank_score": ranks[best],
    }


def depth_shortlist(ranked, core, pinned, capacity, depth_candidates, seed, exploration):
    from .evidence import select_deep

    chosen, metadata = select_deep(ranked, core, pinned, capacity, seed, exploration)
    eligible = {r["symbol"] for r in ranked}
    chosen = list(dict.fromkeys([s for s in pinned + core if s in eligible] + chosen))
    limit = max(len(chosen), depth_candidates or capacity * 3)
    chosen += [r["symbol"] for r in ranked if r["symbol"] not in chosen][: max(0, limit - len(chosen))]
    return chosen, metadata
