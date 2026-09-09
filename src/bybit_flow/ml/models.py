"""Sklearn training pipelines; bounded JSON-only inference, no pickle/joblib loading."""

import math

import numpy as np


def records(rows, excluded=()):
    from .features import CATALOG

    return [
        {
            k: (float("nan") if v is None else v)
            for k, v in r["values"].items()
            if CATALOG.get(k, ("context",))[0] not in excluded
        }
        for r in rows
    ]


def fit(rows, kind="logistic", excluded=(), seed=7):
    from sklearn.feature_extraction import DictVectorizer
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    y = [int(r["label"]["net_r"] > 0) for r in rows]
    if len(rows) < 50 or len(set(y)) != 2:
        raise ValueError("Need at least 50 complete training labels and both classes")
    if kind == "logistic":
        estimator = LogisticRegression(C=0.1, max_iter=2000, random_state=seed)
    elif kind in {"lightgbm", "lightgbm_r"}:
        from lightgbm import LGBMClassifier, LGBMRegressor

        if kind == "lightgbm_r" and len(rows) < 1000:
            raise ValueError("Expected-R regression requires at least 1000 training labels")
        cls = LGBMRegressor if kind == "lightgbm_r" else LGBMClassifier
        estimator = cls(
            n_estimators=80,
            num_leaves=7,
            max_depth=3,
            learning_rate=0.03,
            min_child_samples=30,
            reg_lambda=5,
            n_jobs=1,
            random_state=seed,
            deterministic=True,
            force_col_wise=True,
            verbosity=-1,
        )
        if kind == "lightgbm_r":
            y = [r["label"]["net_r"] for r in rows]
    else:
        raise ValueError("Unknown model type")
    pipe = Pipeline(
        [
            ("vector", DictVectorizer(sparse=False)),
            ("impute", SimpleImputer(strategy="median", keep_empty_features=True, add_indicator=True)),
            ("scale", StandardScaler()),
            ("model", estimator),
        ]
    )
    pipe.fit(records(rows, excluded), y)
    vector, impute, scale, model = (pipe.named_steps[k] for k in ("vector", "impute", "scale", "model"))
    names = vector.get_feature_names_out().tolist()
    artifact = dict(
        kind=kind,
        excluded=list(excluded),
        vocabulary=vector.vocabulary_,
        names=names,
        medians=impute.statistics_.tolist(),
        indicators=impute.indicator_.features_.tolist(),
        mean=scale.mean_.tolist(),
        scale=scale.scale_.tolist(),
        transformed_names=impute.get_feature_names_out(names).tolist(),
    )
    if kind == "logistic":
        artifact.update(coef=model.coef_[0].tolist(), intercept=float(model.intercept_[0]))
        importance = np.abs(model.coef_[0]).tolist()
    else:
        # LightGBM's documented JSON tree dump, evaluated without loading executable Python objects.
        artifact["trees"] = model.booster_.dump_model()["tree_info"]
        importance = model.booster_.feature_importance(importance_type="gain").astype(float).tolist()
    artifact["importance"] = dict(zip(artifact["transformed_names"], importance, strict=True))
    artifact["reference_mean"] = np.mean(transform(artifact, rows), axis=0).tolist()
    artifact["reference_std"] = np.std(transform(artifact, rows), axis=0).tolist()
    return artifact, pipe


def transform(artifact, rows):
    vocabulary = artifact["vocabulary"]
    x = np.zeros((len(rows), len(vocabulary)), dtype=float)
    for i, record in enumerate(records(rows, artifact["excluded"])):
        for k, value in record.items():
            name = f"{k}={value}" if isinstance(value, str) else k
            if name in vocabulary:
                x[i, vocabulary[name]] = 1 if isinstance(value, str) else value
    missing = np.isnan(x)
    x = np.where(missing, np.array(artifact["medians"]), x)
    if artifact["indicators"]:
        x = np.column_stack((x, missing[:, artifact["indicators"]]))
    return (x - np.array(artifact["mean"])) / np.array(artifact["scale"])


def sigmoid(x):
    return 1 / (1 + np.exp(-np.clip(x, -40, 40)))


def tree_value(tree, row):
    for _ in range(32):
        if "leaf_value" in tree:
            return tree["leaf_value"]
        if tree.get("decision_type") != "<=":
            raise ValueError("Unsupported model tree split; abstaining")
        value = row[tree["split_feature"]]
        left = tree["default_left"] if math.isnan(value) else value <= float(tree["threshold"])
        tree = tree["left_child" if left else "right_child"]
    raise ValueError("Model exceeds approved tree depth")


def margins(artifact, rows):
    x = transform(artifact, rows)
    if artifact["kind"] == "logistic":
        return x @ np.array(artifact["coef"]) + artifact["intercept"]
    if artifact["kind"] not in {"lightgbm", "lightgbm_r"} or len(artifact["trees"]) > 100:
        raise ValueError("Unsupported or oversized model")
    return np.array([sum(tree_value(t["tree_structure"], r) for t in artifact["trees"]) for r in x])


def predict(artifact, rows, calibrated=True):
    scores = margins(artifact, rows)
    if artifact["kind"] == "lightgbm_r":
        return scores
    calibration = artifact.get("calibration") if calibrated else None
    if calibration:
        if calibration["method"] == "sigmoid":
            return sigmoid(scores * calibration["coef"] + calibration["intercept"])
        return np.interp(scores, calibration["x"], calibration["y"])
    return sigmoid(scores)


def calibrate(artifact, rows, method="sigmoid"):
    y = [int(r["label"]["net_r"] > 0) for r in rows]
    if len(rows) < 100 or len(set(y)) != 2:
        raise ValueError("Independent calibration needs >=100 observations and both classes")
    x = margins(artifact, rows)
    if method == "isotonic":
        from sklearn.isotonic import IsotonicRegression

        if len(rows) < 1000:
            raise ValueError("Isotonic calibration requires >=1000 independent calibration rows")
        model = IsotonicRegression(out_of_bounds="clip").fit(x, y)
        result = dict(method=method, x=model.X_thresholds_.tolist(), y=model.y_thresholds_.tolist())
    elif method == "sigmoid":
        from sklearn.linear_model import LogisticRegression

        model = LogisticRegression(C=1.0, random_state=7).fit(x.reshape(-1, 1), y)
        result = dict(method=method, coef=float(model.coef_[0][0]), intercept=float(model.intercept_[0]))
    else:
        raise ValueError("Unsupported calibration method")
    artifact["calibration"] = result | {"samples": len(rows)}
    return artifact


def explain(artifact, row):
    if artifact["kind"] == "logistic":
        contribution = transform(artifact, [row])[0] * np.array(artifact["coef"])
        values = dict(zip(artifact["transformed_names"], contribution.tolist(), strict=True))
        method = "standardized log-odds contributions; correlated features are not causal"
    else:
        values, method = artifact["importance"], "global split gain, not individual attribution or causation"
    return dict(method=method, factors=sorted(values.items(), key=lambda x: abs(x[1]), reverse=True)[:6])
