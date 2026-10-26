# Source verification — Reliability & Evidence Loop

Date: 2026-10-26. Starting main: `82051a14351db5bebb0b804c3f24433cdae1e3cb`.
Ending revision: the commit containing this report. Maintenance version: **2.2.1**.
This is source verification, not a deployment or live profitability audit.

## Scope and invariants

No live service was stopped, restarted or recreated. The original desk, trainer
and DNS container IDs and start times were compared read-only and were unchanged.
Live recording continued. No live database migration, model training, historical
backfill, model promotion, destructive maintenance or remote push was performed.
Unrelated local `desk-last45m.txt` remains untracked and outside the release.

Frozen production files were compared by SHA-256 against starting main:
quality_v22, quality_v211, quality_v21, setup_scoring, OFI, impact efficiency,
target reachability, stop viability, strategy, risk, production, scoring, V8 gating
and identity remained byte-for-byte identical. Notifications changed only by
wrapping existing POST/PATCH awaits with redacted timing/outcome measurements.
Other adapter changes likewise measure existing awaits without changing retries,
validation or public endpoint allowlists.

Trading policy IDs, score weights, RR, source isolation, candidate-v20, 500
outcomes/sequences, chronological partitions, purge/embargo/holdout, abstention,
lifecycle, uncertain-delivery handling, retention protections and absence of
signal quotas remain in force. Model selection precedence and historical plans
and labels are unchanged.

## Architecture audit

Scanner changed from **2,640 lines / 127,090 bytes** to **525 lines / 20,735 bytes**.
Six responsibility services own data coordination, discovery, confirmation,
lifecycle monitoring, research scheduling and maintenance. MarketDataCoordinator
owns candle/feature/derivative caches and REST concurrency state; explicit public
properties retain existing integrations. Runtime-port protocols expose dependencies.
Algorithm fingerprints verify extracted method bodies against starting main.
Focused existing suites were run during extraction; test-double patches now target
the component that owns each dependency, with assertions preserved.

There is no eager project-import cycle involving any extracted service. Shared
session clients/context and dynamic `Any` ports remain an incremental-refactor
limit. Discovery (852 lines) and confirmation (815) are still substantial; split
further only around a stable contract. Other largest modules after this release:
recording_maintenance 1,040, notifications 982, ML completeness 935 and Store 700
lines. Their transaction/provenance boundaries were audited and left cohesive.

## Implemented product surfaces

- Prospective immutable candidate/source policy ledger, seven scoped controls,
  source/provenance-separated outcomes, score bands, cohort safeguards and paired
  loss-avoidance/winner-rejection observations. Manual research-review receipts
  never alter production. Older candidates are excluded from the forward epoch.
- Ordered, checksummed migration receipts (001–009), supported old-schema fixture
  startup, unchanged historical payloads, repeatability and integrity rejection.
- Bounded SQLite/loop/recorder/network/delivery/trainer measurements, allowlisted
  diagnostic write coalescing, critical acknowledged-or-failed writes, passive
  checkpoint reporting and explicit operational warnings.
- Background-only priority admission; recorder/lifecycle/feeds/confirmation/scans
  stay protected. Stale observed admission state defers optional work. Heavy
  hourly storage walks and checkpoints defer under collection pressure.
- Hourly category growth with 24h/7d/30d prerequisites, UNKNOWN forecasts when
  insufficient, and explicit existing protected/reclaimable/permanent inventory.
- Read-only recovery vocabulary/progress projection retaining independent engine
  implementations, immutable labels and existing retry fairness.
- Decision-first overview: all currently valid opportunities with fresh original-
  source quotes, immutable plan/published score versus current health, midpoint/R
  progress, blocked/unknown evidence, shared direction and measured correlation
  context. Deep research pages remain available. Rank has no delivery authority.
- CI-gated deploy separated from explicit ML/recovery commands, continuity checks,
  current-volume image rollback and redacted version/commit/lock/image/schema/
  deployment/config provenance. Intentionally stopped trainers stay stopped.
- Isolated Docker image/Compose startup, offline doctor, health, graceful stop,
  migrated backup and separate-volume restore CI; CodeQL and dependency review.
  Existing Python matrix, Ruff, JS, offline research and recorder soak remain.

## Verification

- `python scripts/verify.py`: **PASS** on Windows / Python 3.12.14.
- Full suite: **943 passed, 4 skipped**. The skipped tests require optional
  PyTorch (three two-stage variants and one two-stage registry/holdout test).
- Ruff, formatting, JavaScript syntax and Git whitespace checks: **PASS**.
- Isolated Docker build/Compose health/offline doctor/version, graceful stop,
  migrated backup and fresh-volume restore: **PASS**.
- Synthetic offline research and isolated recorder soak: **PASS**.
- PowerShell operating scripts: parser checks **PASS**; continuity/isolated-stack
  contracts are tested without a live deployment.
- Source algorithm fingerprints, frozen policy hashes, current documentation
  links and staged-path/webhook-secret checks: **PASS**.
- GitHub Python 3.12/3.13, container and security jobs: configured, **not run for
  this unpushed commit**. Three local warnings are dependency deprecations
  (Starlette/httpx/anyio and sklearn SVM probability mode), not test failures.

The disposable Compose test does not load the real `.env` or Compose stack, use
real webhooks, publish the live host port, access exchange networks or mount
existing volumes. Unique test image/projects/volumes keep the production stack
separate. Only the test-created disposable volumes are removed. Its clean-stop
check accepts standard SIGTERM exit 143 only with an application shutdown-complete
log. Windows copied-SQLite handles are explicitly closed before cleanup.

The synthetic offline research example now defaults to a disposable directory;
optional retained output requires a new/empty directory. The separate recorder
soak offered and wrote **8,350 events**, with **zero drops/overflows**, under a
2-CPU/4-GiB software-test envelope. Actual offered/written rate was about **395/s**
with 500/s requested; this is not a live scanner capacity or 24-hour guarantee.

## Remaining risks and next release

The new source is deliberately not deployed or pushed. Remote CI, CodeQL and
review checks for this commit remain unexecuted until a later authorized push.
Deployment/rollback scripts were parsed and their continuity contracts tested;
no live rollout or rollback was exercised.

The forward epoch begins only after deployment. Research-only OHLC paths are
conservative bounds, not prints, account fills or proven edge. Actual-policy
prints/proxy labels join only when source, exact decision time and frozen prices
match. Earlier provisional ML snapshots often differ and are intentionally not
pooled. Alternate targets cannot inherit actual-policy prints results. No minimum
cohort count proves statistical independence or calibrated probabilities.

SQLite critical transactions/reads remain synchronous, though runtime busy sleeps
were removed. Long readers can hold WAL pages. Per-process metrics and bounded
recovery views do not claim a complete historical census. Filesystem scans run in
a private worker and defer under pressure. Permanent database metadata versus
immutable evidence cannot be separated into exact physical page usage by this
release. Forecasts need real elapsed history and assume observed growth persists.
Host sleep, disk/network outages and clock drift remain operating constraints.

The next release should first forward-observe this infrastructure and reduce any
measured bottlenecks, then narrow remaining runtime ports. Evaluate policy changes
only after adequate independent prospective cohorts, costs and uncertainty review.
