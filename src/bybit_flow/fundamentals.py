"""Source-attributed manual facts; availability-time filtering prevents future knowledge."""

import json

from pydantic import BaseModel, Field, HttpUrl, model_validator


class Fact(BaseModel):
    asset: str = Field(min_length=1, max_length=30, pattern=r"^[A-Z0-9]+$")
    category: str = Field(max_length=80)
    definition: str = Field(min_length=10, max_length=1000)
    value: str = Field(max_length=2000)
    source: HttpUrl
    known_ms: int = Field(gt=0)
    effective_ms: int = Field(gt=0)
    expires_ms: int = Field(gt=0)
    major_event: bool = False
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
