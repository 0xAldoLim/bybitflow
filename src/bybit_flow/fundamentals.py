"""Source-attributed manual facts; availability-time filtering prevents future knowledge."""

import json
from typing import Literal

from pydantic import BaseModel, Field, HttpUrl, model_validator


class Fact(BaseModel):
    asset: str = Field(min_length=1, max_length=30, pattern=r"^[A-Z0-9]+(?:_[A-Z0-9]+)*$")
    category: str = Field(max_length=80)
    definition: str = Field(min_length=10, max_length=1000)
    value: str = Field(max_length=2000)
    source: HttpUrl
    known_ms: int = Field(gt=0)
    effective_ms: int = Field(gt=0)
    expires_ms: int = Field(gt=0)
    major_event: bool = False
    assessment: Literal["positive", "neutral", "adverse", "severe", "unassessed"] = "unassessed"
    directional_note: str = Field(default="context only", max_length=1000)

    @model_validator(mode="after")
    def dates(self):
        if self.expires_ms <= self.known_ms:
            raise ValueError("Fact must expire after it was known")
        return self


def add_fact(store, fact, collected_ms):
    if fact.known_ms > collected_ms:
        raise ValueError("Cannot record future knowledge")
    payload = fact.model_dump(mode="json") | {
        "collected_ms": collected_ms,
        "verification": "user-sourced; review source",
    }
    with store.db:
        store.db.execute(
            "INSERT INTO facts(asset,known_ms,effective_ms,expires_ms,payload) VALUES(?,?,?,?,?)",
            (fact.asset, fact.known_ms, fact.effective_ms, fact.expires_ms, json.dumps(payload)),
        )


def facts_asof(store, asset, asof):
    rows = store.db.execute(
        "SELECT payload FROM facts WHERE asset=? AND known_ms<=? AND expires_ms>?", (asset, asof, asof)
    )
    # Future event date is legitimate when announcement was already known.
    return [json.loads(r[0]) for r in rows if json.loads(r[0])["collected_ms"] <= asof]


def quality_evidence(facts, asof):
    required = {
        "economic_purpose",
        "value_accrual",
        "dilution",
        "concentration",
        "security",
        "governance",
        "events",
    }
    valid = [
        f
        for f in facts
        if f.get("source")
        and f.get("definition")
        and max(f.get("known_ms", float("inf")), f.get("collected_ms", 0)) <= asof < f.get("expires_ms", 0)
    ]
    by_category = {}
    values = {"positive": 1.0, "neutral": 0.5, "adverse": 0.0, "severe": 0.0, "unassessed": 0.0}
    for f in valid:
        category = f.get("category")
        if category in required:
            # Conflicting current evidence takes the conservative assessment.
            value = values.get(f.get("assessment", "unassessed"), 0.0)
            by_category[category] = min(by_category.get(category, 1.0), value)
    coverage = len(by_category) / len(required)
    quality = sum(by_category.values()) / len(required)
    severe = any(f.get("assessment") == "severe" or f.get("major_event") for f in valid)
    adverse = any(f.get("assessment") == "adverse" for f in valid)
    fraction = (0.2 * coverage + 0.8 * quality) * (0 if severe else 0.5 if adverse else 1)
    return dict(
        coverage=coverage,
        quality_risk=quality,
        fraction=fraction,
        severe=severe,
        method="20% provenance coverage, 80% explicit sourced assessment; unassessed earns no favorable quality",
    )
