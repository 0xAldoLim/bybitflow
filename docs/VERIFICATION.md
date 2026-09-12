# Verification

## Automated checks

CI runs the pinned Python dependencies on Python 3.12 and 3.13:

```sh
ruff check src tests examples
ruff format --check src tests
pytest -q
python examples/offline_research.py
node --check src/bybit_flow/static/app.js
```

Tests cover exchange normalization and sequencing, source transitions, setup and
risk rules, notification grades and deduplication, data integrity, point-in-time
features, ML labels and validation, chart compatibility, and dashboard access.

Synthetic fixtures and mocked transports verify software behavior. They are excluded
from financial evidence and do not establish live exchange or Discord connectivity.

## Deployment checks

```sh
docker compose --profile ml ps
docker compose exec desk bybit-flow doctor
docker compose exec desk bybit-flow test-market
docker compose exec desk bybit-flow test-discord
docker compose exec desk python -m pip check
docker compose exec trainer python -m pip check
```

Use a separate test project and empty data directory for smoke or restore exercises.
The examples directory includes isolated Docker and operations checks; each script
documents its prerequisites. `compose_smoke.py` requires port 8000 to be free.

A successful webhook test establishes delivery of that test message. A real signal
additionally requires a qualified setup and current source coverage. A running
container can still report unhealthy when exchange access fails.

## Research verification

Preserve source hashes, strategy/schema versions, experiment parameters, costs,
excluded intervals, and consumed holdout periods. Software checks do not approve
models or establish future returns. The implemented admission policy is documented
in [ML research](ML_RESEARCH.md).
