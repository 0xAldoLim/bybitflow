"""Disposable-container CLI checks. Never creates a candidate or claims live compatibility."""

import json
import secrets
import subprocess


def main():
    name = "bybitflow-native-ops-" + secrets.token_hex(4)
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            name,
            "--read-only",
            "--tmpfs",
            "/app/data:uid=10001,gid=10001,size=128m",
            "--tmpfs",
            "/tmp:size=64m",
            "-e",
            "FLOW_ADMIN_TOKEN=" + secrets.token_hex(32),
            "bybit-flow:native-review",
        ],
        check=True,
        capture_output=True,
    )
    try:
        for command in (
            ["doctor", "--json"],
            ["doctor"],
            ["test-market"],
            ["test-discord"],
            ["ml", "status"],
        ):
            result = subprocess.run(
                ["docker", "exec", name, "bybit-flow", *command], capture_output=True, text=True, timeout=90
            )
            if command == ["doctor", "--json"]:
                report = json.loads(result.stdout)
                assert report["checks"]["database"]["status"] == "OK"
                print(json.dumps({"command": "doctor --json", "report": report}))
            elif command == ["test-market"]:
                print(
                    json.dumps(
                        {
                            "command": "test-market",
                            "exit": result.returncode,
                            "report": json.loads(result.stdout),
                        }
                    )
                )
            elif command == ["test-discord"]:
                assert result.returncode == 1 and json.loads(result.stdout)["status"] == "NOT_CONFIGURED"
                print("test-discord correctly refuses absent webhook; no delivery attempted")
            else:
                assert result.returncode == 0, result.stderr
                print("CLI check:", " ".join(command), "OK")
        code = (
            "from pathlib import Path; import sqlite3; from bybit_flow.storage import Store; "
            "s=Store(Path('/app/data')); s.backup(Path('/app/data/software-check.sqlite')); s.close(); "
            "c=sqlite3.connect('/app/data/software-check.sqlite'); "
            "assert c.execute('PRAGMA quick_check').fetchone()[0]=='ok'; "
            "assert c.execute('SELECT count(*) FROM signals').fetchone()[0]==0; c.close(); print('SQLite backup reopened and verified')"
        )
        result = subprocess.run(
            ["docker", "exec", name, "python", "-c", code], check=True, capture_output=True, text=True
        )
        print(result.stdout.strip())
    finally:
        subprocess.run(["docker", "stop", name], check=True, capture_output=True)


if __name__ == "__main__":
    main()
