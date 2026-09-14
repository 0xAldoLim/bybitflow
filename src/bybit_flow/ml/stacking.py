"""Chronological two-stage research; CPU training, JSON/NumPy-only live inference."""

import numpy as np

SEQUENCE_KEYS = (
    "delta_pct",
    "stacked_buy",
    "stacked_sell",
    "trades_per_second",
    "book_spread_bps",
    "m15_efficiency",
    "m15_slope",
    "m15_volume_expansion",
    "h4_efficiency",
    "funding_rate",
)
KINDS = ("two_stage_logistic", "two_stage_svm", "two_stage_random_forest")


def sequence_values(rows, excluded=()):
    from .features import CATALOG

    values = []
    for row in rows:
        sequence = row.get("sequence", [])
        times = [r["at_ms"] for r in sequence]
        if (
            len(sequence) != 16
            or times != sorted(set(times))
            or times[-1] != row["decision_ms"]
            or times[0] < times[-1] - 7_200_000
        ):
            raise ValueError("LSTM requires 16 causal observations within two hours")
        values.append(
            [
                [
                    float("nan")
                    if point["values"].get(k) is None or CATALOG[k][0] in excluded
                    else float(point["values"][k])
                    for k in SEQUENCE_KEYS
                ]
                for point in sequence
            ]
        )
    return np.asarray(values, dtype=float)


def sequence_transform(model, rows):
    x = sequence_values(rows, model["excluded"])
    missing = ~np.isfinite(x)
    x = np.where(missing, np.asarray(model["median"]), x)
    x = (x - np.asarray(model["mean"])) / np.asarray(model["scale"])
    return np.concatenate((x, missing.astype(float)), axis=2).astype(np.float32)


def fit_lstm(rows, excluded=(), seed=7):
    import torch

    if len(rows) < 200:
        raise ValueError("LSTM needs at least 200 complete sequence-labelled training samples")
    torch.set_num_threads(1)
    torch.manual_seed(seed)
    raw = sequence_values(rows, excluded)
    median = np.array(
        [
            np.median(v[np.isfinite(v)]) if np.isfinite(v).any() else 0
            for v in raw.reshape(-1, len(SEQUENCE_KEYS)).T
        ]
    )
    clean = np.where(np.isfinite(raw), raw, median)
    model = dict(
        excluded=list(excluded),
        median=median.tolist(),
        mean=clean.mean(axis=(0, 1)).tolist(),
        scale=np.maximum(clean.std(axis=(0, 1)), 1e-6).tolist(),
    )
    x = torch.tensor(sequence_transform(model, rows))
    y = torch.tensor([float(r["label"]["net_r"] > 0) for r in rows])
    if len(torch.unique(y)) != 2:
        raise ValueError("LSTM requires both win and loss outcomes")
    recurrent = torch.nn.LSTM(x.shape[2], 12, batch_first=True)
    head = torch.nn.Linear(12, 1)
    optimizer = torch.optim.AdamW(
        list(recurrent.parameters()) + list(head.parameters()), lr=0.005, weight_decay=0.01
    )
    for _ in range(20):
        for start in range(0, len(rows), 64):
            optimizer.zero_grad()
            hidden, _ = recurrent(x[start : start + 64])
            logits = head(hidden[:, -1]).flatten()
            loss = torch.nn.functional.binary_cross_entropy_with_logits(logits, y[start : start + 64])
            loss.backward()
            torch.nn.utils.clip_grad_norm_(list(recurrent.parameters()) + list(head.parameters()), 1)
            optimizer.step()
    model.update(
        weights={k: v.detach().numpy().tolist() for k, v in recurrent.state_dict().items()},
        head_weight=head.weight.detach().numpy()[0].tolist(),
        head_bias=float(head.bias.detach()[0]),
        training_samples=len(rows),
        epochs=20,
    )
    with torch.no_grad():
        expected = torch.sigmoid(head(recurrent(x)[0][:, -1]).flatten()).numpy()
    if not np.allclose(lstm_predict(model, rows), expected, atol=2e-6):
        raise ValueError("LSTM JSON inference parity failed")
    return model


def lstm_predict(model, rows):
    from .models import sigmoid

    x = sequence_transform(model, rows)
    w = {k: np.asarray(v) for k, v in model["weights"].items()}
    h = np.zeros((len(rows), 12))
    c = np.zeros_like(h)
    for t in range(16):
        gates = x[:, t] @ w["weight_ih_l0"].T + h @ w["weight_hh_l0"].T + w["bias_ih_l0"] + w["bias_hh_l0"]
        i, f, g, o = np.split(gates, 4, axis=1)
        c = sigmoid(f) * c + sigmoid(i) * np.tanh(g)
        h = sigmoid(o) * np.tanh(c)
    return sigmoid(h @ np.asarray(model["head_weight"]) + model["head_bias"])


def meta_rows(artifact, rows):
    from .models import predict

    probabilities = {kind: predict(model, rows, False) for kind, model in artifact["base_models"].items()}
    probabilities["lstm"] = lstm_predict(artifact["lstm"], rows)
    return [
        row | {"values": {f"stage1_{kind}": float(p[i]) for kind, p in probabilities.items()}}
        for i, row in enumerate(rows)
    ]


def fit_stack(rows, meta_kind="logistic", excluded=(), seed=7):
    from .models import fit

    rows = sorted(rows, key=lambda r: r["decision_ms"])
    sequence_values(rows, excluded)  # Never replace missing temporal history with repeated static rows.
    times = sorted({r["decision_ms"] for r in rows})
    boundary = times[int(len(times) * 0.65)]
    base = [
        r
        for r in rows
        if r["decision_ms"] < boundary
        and max(r["label_available_ms"], r["label"]["exit_ms"]) + 14_400_000 < boundary
    ]
    meta = [r for r in rows if r["decision_ms"] >= boundary]
    if len(base) < 200 or len(meta) < 50:
        raise ValueError("Two-stage training requires 200 base and 50 later meta samples after purging")
    bases = {kind: fit(base, kind, excluded, seed)[0] for kind in ("lightgbm", "random_forest")}
    artifact = dict(bases["lightgbm"])
    artifact.pop("trees", None)
    artifact.update(
        kind="two_stage_" + meta_kind,
        base_models=bases,
        lstm=fit_lstm(base, excluded, seed),
        stage_split=dict(
            base_samples=len(base),
            meta_samples=len(meta),
            boundary_ms=boundary,
            last_base_label_ms=max(r["label_available_ms"] for r in base),
        ),
    )
    artifact["meta_model"] = fit(meta_rows(artifact, meta), meta_kind, (), seed)[0]
    return artifact
