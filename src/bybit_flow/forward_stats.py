"""Day-resampled, cluster-aware descriptive statistics. Research-only; never an authority.

Observations are dicts with ``t`` (decision ms), ``key`` (cluster key: source,
direction, shared-exposure group) and ``value``. Whole UTC days are resampled
with replacement, so same-day dependence is kept inside each draw. Clusters
merge observations with one key whose decisions fall within a fixed window of
the cluster's first member; intervals from cluster means are the primary
inference. Below the existing reporting floor nothing inferential is shown.
"""

import math
import random
from statistics import NormalDist, mean, stdev

DAY = 86_400_000
ALPHA = 0.05
POWER = 0.80


def day(t):
    return int(t // DAY)


def clusters(obs, window_ms):
    """Deterministic chaining; a cluster spans [first member, first member + window)."""
    current, out = {}, []
    for o in sorted(obs, key=lambda o: (repr(o["key"]), o["t"])):
        group = current.get(o["key"])
        if group is None or o["t"] - group["t"] >= window_ms:
            group = dict(t=o["t"], key=o["key"], values=[])
            current[o["key"]] = group
            out.append(group)
        group["values"].append(o["value"])
    return [dict(t=g["t"], key=g["key"], value=mean(g["values"]), size=len(g["values"])) for g in out]


def _days(obs):
    days = {}
    for o in obs:
        days.setdefault(day(o["t"]), []).append(o["value"])
    return [(sum(v), len(v)) for _, v in sorted(days.items())]


def bootstrap_mean(obs, draws, seed):
    """Means of whole-UTC-day resamples; None when fewer than two days exist."""
    days = _days(obs)
    if len(days) < 2:
        return None
    rng = random.Random(seed)
    result = []
    for _ in range(draws):
        total = count = 0
        for i in rng.choices(range(len(days)), k=len(days)):
            total += days[i][0]
            count += days[i][1]
        result.append(total / count)
    return result


def bootstrap_difference(a, b, draws, seed):
    """mean(b) - mean(a), resampling the union of UTC days jointly."""
    days = {}
    for side, obs in ((0, a), (1, b)):
        for o in obs:
            row = days.setdefault(day(o["t"]), [0.0, 0, 0.0, 0])
            row[2 * side] += o["value"]
            row[2 * side + 1] += 1
    rows = [days[k] for k in sorted(days)]
    if len(rows) < 2:
        return None
    rng = random.Random(seed)
    result = []
    for _ in range(draws):
        sa = na = sb = nb = 0
        for i in rng.choices(range(len(rows)), k=len(rows)):
            sa, na, sb, nb = sa + rows[i][0], na + rows[i][1], sb + rows[i][2], nb + rows[i][3]
        if na and nb:
            result.append(sb / nb - sa / na)
    return result or None


def bootstrap_statistic(obs, statistic, draws, seed):
    days = {}
    for o in obs:
        days.setdefault(day(o["t"]), []).append(o)
    keys = sorted(days)
    if len(keys) < 2:
        return None
    rng = random.Random(seed)
    result = []
    for _ in range(draws):
        value = statistic([o for k in rng.choices(keys, k=len(keys)) for o in days[k]])
        if value is not None:
            result.append(value)
    return result or None


def quantile(values, q):
    ordered = sorted(values)
    position = q * (len(ordered) - 1)
    low = math.floor(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def interval(draws):
    if not draws:
        return None
    return [quantile(draws, ALPHA / 2), quantile(draws, 1 - ALPHA / 2)]


def p_value(draws):
    """Two-sided percentile-bootstrap sign test of zero; bounded below by 1/(B+1)."""
    if not draws:
        return None
    below = sum(v <= 0 for v in draws)
    above = sum(v >= 0 for v in draws)
    return min(1.0, 2 * (min(below, above) + 1) / (len(draws) + 1))


def holm(p_values):
    """Holm step-down adjustment; unknown p-values are counted as 1."""
    values = [1.0 if p is None else p for p in p_values]
    order = sorted(range(len(values)), key=lambda i: values[i])
    adjusted, running = [None] * len(values), 0.0
    for rank, i in enumerate(order):
        running = max(running, min(1.0, (len(values) - rank) * values[i]))
        adjusted[i] = running
    return adjusted


def minimum_detectable(sd, n):
    """Two-sided alpha .05, 80% power, normal approximation at the cluster N."""
    if sd is None or n < 2:
        return None
    z = NormalDist().inv_cdf(1 - ALPHA / 2) + NormalDist().inv_cdf(POWER)
    return z * sd / math.sqrt(n)


def ranks(values):
    order = sorted(range(len(values)), key=lambda i: values[i])
    result = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        for k in range(i, j + 1):
            result[order[k]] = (i + j) / 2 + 1
        i = j + 1
    return result


def spearman(pairs):
    if len(pairs) < 3:
        return None
    x, y = ranks([p[0] for p in pairs]), ranks([p[1] for p in pairs])
    mx, my = mean(x), mean(y)
    sxy = sum((a - mx) * (b - my) for a, b in zip(x, y))
    sxx = sum((a - mx) ** 2 for a in x)
    syy = sum((b - my) ** 2 for b in y)
    return sxy / math.sqrt(sxx * syy) if sxx and syy else None


def summarize(obs, settings, minimum):
    """One-sample mean or paired difference, raw and clustered."""
    draws, seed, window = settings["bootstrap"], settings["seed"], settings["cluster_window_ms"]
    groups = clusters(obs, window)
    result = dict(
        raw_n=len(obs),
        cluster_n=len(groups),
        day_n=len({day(o["t"]) for o in obs}),
        mean=mean(o["value"] for o in obs) if obs else None,
        status="INSUFFICIENT SAMPLE" if len(obs) < minimum else "DESCRIPTIVE",
        ci95_day_resampled=None,
        ci95_cluster_means=None,
        p_value=None,
        cluster_sd=None,
        minimum_detectable_difference=None,
    )
    if len(obs) < minimum:
        return result
    boot = bootstrap_mean(groups, draws, seed)
    sd = stdev(g["value"] for g in groups) if len(groups) > 1 else None
    result.update(
        ci95_day_resampled=interval(bootstrap_mean(obs, draws, seed)),
        ci95_cluster_means=interval(boot),
        p_value=p_value(boot),
        cluster_sd=sd,
        minimum_detectable_difference=minimum_detectable(sd, len(groups)),
        effective_sample_below_floor=len(groups) < minimum,
    )
    return result


def compare(a, b, settings, minimum):
    """Unpaired cohorts: mean(b) - mean(a) over cluster means, days resampled jointly."""
    draws, seed, window = settings["bootstrap"], settings["seed"], settings["cluster_window_ms"]
    ga, gb = clusters(a, window), clusters(b, window)
    enough = min(len(a), len(b)) >= minimum
    result = dict(
        raw_n=[len(a), len(b)],
        cluster_n=[len(ga), len(gb)],
        difference=mean(o["value"] for o in b) - mean(o["value"] for o in a) if a and b else None,
        status="DESCRIPTIVE" if enough else "INSUFFICIENT SAMPLE",
        ci95_cluster_means=None,
        p_value=None,
    )
    if enough:
        boot = bootstrap_difference(ga, gb, draws, seed)
        result.update(ci95_cluster_means=interval(boot), p_value=p_value(boot))
    return result


def correlation(obs, settings, minimum):
    """Spearman rank correlation of (score, value) with a day-resampled interval."""
    pairs = [(o["score"], o["value"]) for o in obs]
    result = dict(
        raw_n=len(obs),
        rho=spearman(pairs),
        status="DESCRIPTIVE" if len(obs) >= minimum else "INSUFFICIENT SAMPLE",
        ci95_day_resampled=None,
    )
    if len(obs) >= minimum:
        boot = bootstrap_statistic(
            obs,
            lambda s: spearman([(o["score"], o["value"]) for o in s]),
            settings["bootstrap"],
            settings["seed"],
        )
        result["ci95_day_resampled"] = interval(boot)
    else:
        result["rho"] = None
    return result
