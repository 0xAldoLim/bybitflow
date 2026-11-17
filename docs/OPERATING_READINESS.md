# Operating readiness

Use one installation: the existing scanner, recorder, maintenance worker and
optional trainer. These improvements add no market-data pipeline and change no
strategy threshold, score weight, model promotion requirement or Discord rule.

## Startup and operational limits

From the repository directory in PowerShell:

```powershell
pwsh -NoProfile -File scripts/start.ps1 -WithTrainer
pwsh -NoProfile -File scripts/start.ps1 -WithTrainer -InstallLoginTask
```

The optional login task starts Docker Desktop when unavailable and starts only
existing containers. It does not rebuild, pull code, recreate volumes, restart
healthy services or change clock settings. It runs as the current interactive
user; Windows may require permission to register the task. Inspect failures in
Task Scheduler under `BybitFlow-StartExisting`. Remove that task to disable login
startup. Logging out, sleep and power loss still interrupt collection. Continuous
operation requires an awake host. Docker's `unless-stopped` policy alone cannot
start Docker Desktop.

The dashboard surfaces maintenance failure separately from storage pressure.
Capacity upkeep treats a raw checksum mismatch as a durable, explicit coverage
gap, never as usable prints. It does not decode damaged bytes. The replay audit
retains the manifest ID, expected hash, interval and exclusion reason; the gap
prevents affected outcomes from becoming complete wins. Strict replay retains its
integrity failure. Existing safe retention remains the only deletion authority.
Neither this recovery nor a restart guarantees enough space when permanent data
itself fills the budget. No automatic budget increase is performed.

## Learning and evidence

The existing ML page separates worker, data, training, compatibility, validation
and authority. Counts remain source-specific. A live worker can coexist with a
stale materialized summary (over 15 minutes old in the readiness view). A saved model is not proof of compatibility, training
readiness or production authority. Assumed costs and proxy labels do not become
verified by collecting additional rows.

Setup details show evidence and tradeability coverage alongside the unchanged
frozen score. Unknown evidence earns no supporting credit. Neither coverage nor
the score is a win probability.

Use the existing [evidence-power reports](EVIDENCE_POWER.md) to compare flow-confirmed
and structure-only candidates, delivery delays, execution assumptions and repeat
attempts. Pre-register evaluation periods and acceptance criteria before observing
results. Keep family, horizon, source and policy distinctions. No new scoring fit,
strategy retuning or model promotion occurs as part of operational readiness.

## Optional account-fill review

Copy `examples/fill-journal.csv` to a private location and enter actual fills.
Run `bybit-flow review-fills PATH.csv` in the installed Python environment, or
mount/copy the file into the desk and run that command there. Epoch timestamps
are milliseconds. One row represents a fully closed, equal-quantity linear
contract round trip. Use volume-weighted prices for partial fills and signed total
fee/funding costs in quote currency (credits negative).

The report computes actual net R using original planned risk, entry delay and
adverse entry slippage. It opens no application database and cannot alter signals,
ML labels or cost verification. Manually reported fills require separate evidence
verification before any future production-model cost integration.

## Backups

The existing `bybit-flow backup NEW_PATH.sqlite` now checks SQLite integrity on
the independent copy before reporting success. It does not checkpoint or vacuum
production. This remains a database backup, not a full recording/model-file backup;
follow [operations](OPERATIONS.md) for full-volume restoration. No duplicate data
store, automatic local backup schedule or unbounded backup copies are introduced.
Choose separate storage before scheduling full backups, and test restoration in
an isolated volume rather than overwriting production.
