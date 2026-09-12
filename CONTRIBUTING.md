# Contributing

## Development environment

Use Python 3.12 or 3.13. Create a virtual environment and install the pinned
development and ML dependencies:

```sh
python -m venv .venv
# Activate .venv using the platform's shell.
python -m pip install -r requirements-dev.lock -r requirements-ml.lock
python -m pip install --no-deps -e .
ruff check src tests examples
ruff format --check src tests
pytest -q
python examples/offline_research.py
node --check src/bybit_flow/static/app.js
```

The application has no frontend build step. JavaScript, HTML, and CSS are served
directly from `src/bybit_flow/static`.

## Change requirements

- Preserve source identity, event time, receipt time, and recording integrity.
- Keep setup quality, research model ranking, and validated probability distinct.
- Add regression coverage for changes to risk, data continuity, labels, or delivery.
- Use isolated data directories and mock transports for tests. Production recordings,
  credentials, outbox entries, and model artifacts do not belong in fixtures.
- Preserve failed experiments and consumed holdouts in research data.
- Document behavior and limitations in the relevant reference; avoid installation
  transcripts and duplicate milestone summaries.

Pull requests should explain the affected behavior, the change, and verification.
Documentation and interface copy should use neutral terminology for operators,
researchers, and contributors.
