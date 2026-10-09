# v1 to v2 gap analysis

Audited baseline: `ad902d4f47db394750e78cd56cc38c771f33a2ec`.

The baseline names `production-v2`, `flow-score-v2`, `context-ev-v2` and `liquidation-state-v2` are component policy revisions. They do **not** indicate that the product/schema/scoring cutover already existed. Baseline product version was 0.1.0; learning schema was candidate-v10; production quality used a flat seven-category score. This upgrade declares product version 2.0.0.

| Capability | Classification | Existing implementation | v2 decision |
|---|---|---|---|
| Actual footprint, CVD, stack, defended absorption | ALREADY_IMPLEMENTED | `orderflow.py`, `evidence.flow_response` | Keep actual tape/book calculations; consume them once. |
| Churn/trust, raw/effective profiles | ALREADY_IMPLEMENTED | `flow_quality.py`, `profiles.py`, `flow_views.py` | Keep source-specific coverage and alternative flow substitution. |
| Profile location, causal AVWAP, acceptance, references | PARTIALLY_IMPLEMENTED | `profiles.py`, `anchored.py`, `levels.py`, `evidence.py` | Add compact location and path/offer interpretation; retain original structural plan construction. |
| Chronological relative trade-size buckets | MISSING | Existing entropy/repetition measures describe a different property | Add bounded prior-only quantile classification; do not infer identities. |
| Event-clock rotations | MISSING | Tape and HTF candles are present | Add one bounded activity summary to existing tape processing, supplementing HTF bars. |
| Breakout participation | ALREADY_IMPLEMENTED | `production.participation`, structure response, anchored/V8 gates | Reuse grouped categories. Multiple activity aliases must not receive independent points. |
| Liquidation response and OI | PARTIALLY_IMPLEMENTED | `liquidation.py`, `current_oi.py`, `v8_runtime.py` | Add compact sequence/continuation-risk interpretation; retain sampled-feed limitations and original gates. |
| BTC/ETH factors, stable/unstable beta, residuals, disagreement | ALREADY_IMPLEMENTED | `evidence.factor_context`, `production.market_alignment`, breadth | Reuse. Do not add a second beta engine or unvalidated lead-lag estimate. |
| Spot/perp, basis, funding | ALREADY_IMPLEMENTED | `spot_perp.py`, derivatives evidence, V8 | Keep freshness, native source and missing-data behavior. |
| Stop/target geometry and costs | ALREADY_IMPLEMENTED | `risk.py`, `levels.py`, execution/noise gates | Keep current gates and structural levels. Add path-aware offer assessment. |
| Hierarchical bottleneck/confidence score | MISSING | `scoring.py` currently flat categories | Replace only new-candidate scoring; explicitly version new semantics. |
| Old active plan freeze at product cutover | MISSING | Existing confirmed decisions are immutable, but pending evaluations can acquire new score/decision features | Add immutable epoch and stored-version dispatch; protect plan fields before any snapshot capture. |
| Source/schema-separated ML | PARTIALLY_IMPLEMENTED | Dataset/model compatibility exists, but snapshot capture and monitored recovery use a global schema | Preserve schema attached to each object; recover old pending labels without pooling them with v20. |
| Original-source lifecycle through outages | ALREADY_IMPLEMENTED | `lifecycle.py`, reconciliation, `thesis_health.py`, monitored OHLC recovery | Preserve these fixes; never retire active plans or require 24/7 laptop recording for the separate monitored track. |
| Durable Discord initial visibility, pause/resume episodes, independent monitoring | ALREADY_IMPLEMENTED | Notifier/outbox, episode keys, thesis presentation | Preserve exact-setup visibility guard and channel split; add one compact v2 explanation to initial cards. |
| Recorder/retention, public REST/streams, shared caches | ALREADY_IMPLEMENTED | Recorder, native streams, REST semaphore, retention and packing | Reuse; no second scanner/flow engine, no large derived event histories. |
| Options/GEX, participant identity, social beliefs | NOT_TESTABLE | Required public causal datasets not present | DEFER; do not derive them from perp OI or prints. |
| Literal session openings, 85% overlap, fixed contract sizes, no-risk breakouts | DANGEROUS_TO_HARDCODE | Conflicts with continuous crypto and existing risk rules | REJECT. |
| Market-making/order execution, second pairs bot | CONFLICTS | Public-data/manual setup service | RESEARCH_ONLY; do not add account keys or automated orders. |
| Extra Hull/ribbon trend indicators or multiple event-chart engines | DUPLICATE | Existing causal trend state and the proposed single activity summary cover the useful interpretation | DEFER without portable source/test evidence. |
| Quantum computation, unrelated abstract examples | NOT_USEFUL | No concrete causal setup improvement | Do not implement. |

The existing scanner, four families, horizon profiles, causal structure, risk gates, V7/V8 qualification, source recovery, lifecycle, outcomes and Discord delivery remain the foundation. The upgrade must add missing evidence discrimination rather than replace working capabilities with a simpler indicator stack.
