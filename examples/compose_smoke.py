"""Actual Compose startup + stopped-writer backup/restore in two unique test projects.

Requires localhost:8000 free. Never uses real webhook secrets or deletes existing volumes.
Only these unique projects are stopped/removed; their disposable test volumes are retained.
"""

import json
import os
import secrets
import subprocess
import tempfile
import time
from pathlib import Path


def main():
    project = "bybitflow-smoke-" + secrets.token_hex(4)
    restored_project = project + "-restore"
    env = os.environ | {
        "FLOW_ADMIN_TOKEN": secrets.token_hex(32),
        "FLOW_SCAN_ENABLED": "true",
        "FLOW_MARKET_SOURCE": "auto",
    }
    with tempfile.TemporaryDirectory(prefix="bybitflow-compose-") as temporary:
        override = Path(temporary) / "override.json"
        override.write_text(
            json.dumps(
                {
                    "services": {
                        "desk": {
                            "environment": {
                                "FLOW_RESEARCH_WEBHOOK": "",
                                "FLOW_DISCORD_WEBHOOK": "",
                                "FLOW_OPS_WEBHOOK": "",
                                "FLOW_RESEARCH_ALERTS": "false",
                                "FLOW_SSS_RESEARCH": "false",
                                "FLOW_TV_ENABLED": "false",
                                "FLOW_ML_ENABLED": "false",
                            }
                        }
                    }
                }
            )
        )
        base = ["docker", "compose", "-p", project, "-f", "compose.yaml", "-f", str(override)]

        def run(*args, check=True):
            return subprocess.run([*base, *args], env=env, text=True, capture_output=True, check=check)

        try:
            run("up", "-d", "--build", "desk")
            for _ in range(30):
                status = run("exec", "-T", "desk", "bybit-flow", "ml", "status", check=False)
                if status.returncode == 0:
                    break
                time.sleep(1)
            assert status.returncode == 0
            print("Compose startup and registry CLI: OK (scanner enabled; no webhook configured)")
            marker = (
                "from bybit_flow.storage import Store; from pathlib import Path; "
                "s=Store(Path('/app/data')); s.put('backup_probe', {'kind':'software-test-only'}); s.close()"
            )
            run("exec", "-T", "desk", "python", "-c", marker)
            report = run("exec", "-T", "desk", "bybit-flow", "doctor", "--json")
            print(report.stdout)
            # All writers stopped before copying the entire named data volume.
            run("stop", "desk")
            backup = Path(temporary) / "backup"
            run("cp", "desk:/app/data", str(backup))
            import sqlite3

            db = sqlite3.connect(backup / "research.sqlite")
            assert db.execute("PRAGMA quick_check").fetchone()[0] == "ok"
            assert db.execute("SELECT count(*) FROM signals").fetchone()[0] == 0
            db.close()
            print("Stopped-writer full-volume copy and copied SQLite integrity: OK")
            base[3] = restored_project
            run("create", "desk")
            run("cp", str(backup) + "/.", "desk:/app/data")
            run(
                "run",
                "--rm",
                "--no-deps",
                "--user",
                "0",
                "--cap-add",
                "CHOWN",
                "--cap-add",
                "DAC_OVERRIDE",
                "--entrypoint",
                "chown",
                "desk",
                "-R",
                "10001:10001",
                "/app/data",
            )
            run("up", "-d", "desk")
            check = (
                "from bybit_flow.storage import Store; from pathlib import Path; "
                "s=Store(Path('/app/data')); "
                "assert s.get('backup_probe') == {'kind':'software-test-only'}; "
                "assert s.db.execute('PRAGMA quick_check').fetchone()[0] == 'ok'; "
                "assert s.db.execute('SELECT count(*) FROM signals').fetchone()[0] == 0; s.close()"
            )
            run("exec", "-T", "desk", "python", "-c", check)
            run("exec", "-T", "desk", "bybit-flow", "ml", "status")
            print("Fresh-project volume restore, permissions, SQLite marker and registry CLI: OK")
            print(
                "Disposable test volumes retained:",
                project + "_research-data",
                restored_project + "_research-data",
            )
        finally:
            for name in (project, restored_project):
                base[3] = name
                run("down", check=False)  # No -v; never delete market data automatically.


if __name__ == "__main__":
    main()
