# Current status

See [implementation status](docs/STATUS.md), [executed checks](docs/VERIFICATION.md), and
[self-hosted operator runbook](docs/USER_RUNBOOK.md). TradingView and a domain are not required.

The existing `main` now includes the preserved TradingView gateway and an optional supervised
ML research layer. See [ML instructions and limitations](docs/ML_RESEARCH.md).

Implemented: immutable candidate/label storage, logistic and LightGBM research training,
purged chronological evaluation, separate calibration, bounded acceptance search, safe JSON
artifacts, guarded manual promotion/rollback, drift checks, CLI worker, Discord and dashboard
integration. No live order execution exists.

No real datasets, trained production models, calibrated live probabilities or approved champion
exist here. Bybit TLS hostname verification still fails on host and Docker. Real TradingView
and Discord delivery needs account-side setup; software mocks are not delivery evidence.
