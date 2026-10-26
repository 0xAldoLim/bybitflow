"""Deterministic SOFTWARE TEST, not historical data or trading evidence.

Run: .venv/bin/python examples/offline_research.py
The resulting experiment explicitly records synthetic provenance. The default
uses a disposable directory; --data-dir preserves output in a new empty folder.
"""

import argparse
import math
import tempfile
from pathlib import Path

from bybit_flow.config import Settings
from bybit_flow.models import Candle
from bybit_flow.research import research_run, save_experiment
from bybit_flow.storage import Store


def run(root):
    settings = Settings(_env_file=None, scan_enabled=False, data_dir=root)
    bars = []
    for i in range(600):
        p = 100 + 0.02 * i + 2 * math.sin(i / 11)
        bars.append(Candle(i * 3_600_000, 3_600_000, p, p + 1, p - 1, p + 0.1, 100, (p + 0.1) * 100))
    store = Store(settings.data_dir)
    try:
        result = research_run(bars, settings)
        result["provenance"] = "SYNTHETIC SOFTWARE TEST ONLY — excluded from all validation"
        result["verdict"] = "No financial inference permitted"
        saved = save_experiment(
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
        print("PASS: synthetic offline research (not trading evidence)", saved.name)
    finally:
        store.close()


def main():
    parser = argparse.ArgumentParser(description="Isolated synthetic software check; no financial inference")
    parser.add_argument("--data-dir", type=Path)
    args = parser.parse_args()
    if args.data_dir is not None:
        if args.data_dir.exists() and any(args.data_dir.iterdir()):
            parser.error("Use a new or empty directory; existing evidence is never a test destination")
        run(args.data_dir)
    else:
        with tempfile.TemporaryDirectory(prefix="bybitflow-offline-") as folder:
            run(Path(folder))


if __name__ == "__main__":
    main()
