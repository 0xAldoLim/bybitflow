"""Run the offline direct-main checks with the active Python environment."""

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STEPS = (
    [sys.executable, "-m", "ruff", "check", "src", "tests", "examples"],
    [sys.executable, "-m", "ruff", "format", "--check", "src", "tests"],
    ["node", "--check", "src/bybit_flow/static/app.js"],
    ["git", "diff", "--check"],
    [sys.executable, "-m", "pytest", "-q"],
)


def main():
    for command in STEPS:
        print("Running:", " ".join(command), flush=True)
        try:
            result = subprocess.run(command, cwd=ROOT, check=False)
        except OSError as exc:
            print(f"Cannot start {command[0]}: {exc}", file=sys.stderr)
            return 1
        if result.returncode:
            return result.returncode
    return 0


if __name__ == "__main__":
    sys.exit(main())
