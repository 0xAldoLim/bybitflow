"""Deterministic SOFTWARE TEST, not historical data or trading evidence.

Run: .venv/bin/python examples/offline_research.py
The resulting experiment explicitly records synthetic provenance.
"""

import math

from bybit_flow.config import Settings
from bybit_flow.models import Candle
from bybit_flow.research import research_run, save_experiment
from bybit_flow.storage import Store

settings = Settings(_env_file=None, scan_enabled=False)
bars = []
for i in range(600):
    p = 100 + 0.02 * i + 2 * math.sin(i / 11)
    bars.append(Candle(i * 3_600_000, 3_600_000, p, p + 1, p - 1, p + 0.1, 100, (p + 0.1) * 100))
store = Store(settings.data_dir)
try:
    result = research_run(bars, settings)
    result["provenance"] = "SYNTHETIC SOFTWARE TEST ONLY — excluded from all validation"
    result["verdict"] = "No financial inference permitted"
    print(
        save_experiment(
            store,
            "SYNTHETIC SOFTWARE TEST",
            {
                "generator": "examples/offline_research.py",
                "bars": 600,
                "seed": "deterministic analytic function",
                "settings": settings.public(),
            },
            result,
            [__file__],
        )
    )
finally:
    store.close()
