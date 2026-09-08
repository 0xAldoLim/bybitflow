# Repository plan and milestones

1. Public GET-only Bybit adapter, paginated universe and historical candles, liquidity gates,
   deterministic 4H/1H features, candidate journal, dry-run Discord embeds, local dashboard.
2. Prioritized public WebSocket subscriptions, bounded durable raw/Parquet recording,
   executed-trade footprint, resettable books, data-health circuit breakers.
3. Four separate experimental setup families, derivatives context, conservative hypothetical
   sizing, shared replay/live rules, event-level exit simulation, reproducible experiments.
4. Cluster-aware uncertainty and chronological evaluation utilities. Qualification remains
   locked until adequate real out-of-sample evidence and a reviewed release exist.

No exchange credentials, order transport, or execution UI. No fabricated market history.
SQLite stores operational metadata; compressed JSONL + Parquet store market observations.
FastAPI serves a dependency-free dark browser UI. One process owns the collector and DB.
See STATUS.md for implementation boundaries; these milestones are scope, not performance claims.
