"""Ephemeral Docker software checks. No real webhook, signal, order or financial result."""

import argparse
import base64
import json
import secrets
import subprocess
import time
import urllib.error
import urllib.request


def command(*args):
    return subprocess.check_output(args, text=True).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="bybit-flow:ml-review")
    parser.add_argument("--trainer", default="bybit-flow:ml-trainer-review")
    args = parser.parse_args()
    password = secrets.token_hex(24)
    name = "bybitflow-ml-smoke-" + secrets.token_hex(4)
    command(
        "docker",
        "run",
        "--rm",
        "-d",
        "--name",
        name,
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges:true",
        "--memory=512m",
        "--tmpfs",
        "/app/data:uid=10001,gid=10001,size=64m",
        "--tmpfs",
        "/tmp:size=64m",
        "-p",
        "127.0.0.1::8000",
        "-e",
        "FLOW_ADMIN_TOKEN=" + password,
        args.image,
    )
    try:
        port = command("docker", "port", name, "8000/tcp").rsplit(":", 1)[1]
        root = "http://127.0.0.1:" + port
        auth = "Basic " + base64.b64encode(("research:" + password).encode()).decode()

        def get(path):
            request = urllib.request.Request(root + path, headers={"Authorization": auth})
            return json.loads(urllib.request.urlopen(request, timeout=5).read())

        for _ in range(50):
            try:
                health = get("/healthz")
                break
            except (urllib.error.URLError, ConnectionError):
                time.sleep(0.2)
        else:
            raise RuntimeError("Application did not start")
        try:
            urllib.request.urlopen(root + "/api/ml", timeout=5)
            raise AssertionError("Unauthenticated ML access succeeded")
        except urllib.error.HTTPError as exc:
            assert exc.code == 401
        ml = get("/api/ml")
        assert health["alerts_only"] and not ml["models"] and ml["champion"] is None
        print(
            json.dumps(
                dict(
                    check="runtime software smoke",
                    health=health,
                    ml_models=len(ml["models"]),
                    unauthenticated_status=401,
                )
            )
        )
        probe = (
            "import urllib.request; "
            "urllib.request.urlopen('https://api.bybit.com/v5/market/time',timeout=10).read()"
        )
        result = subprocess.run(
            ["docker", "exec", name, "python", "-c", probe], capture_output=True, text=True
        )
        print(
            json.dumps(
                dict(
                    check="Bybit verified TLS inside Docker",
                    succeeded=result.returncode == 0,
                    hostname_failure="Hostname mismatch" in result.stderr
                    or "hostname" in result.stderr.lower(),
                )
            )
        )
    finally:
        # Only the explicitly created ephemeral test container and its tmpfs data are removed.
        command("docker", "stop", name)
    check = command(
        "docker",
        "run",
        "--rm",
        "--read-only",
        "--tmpfs",
        "/tmp:size=64m",
        "--tmpfs",
        "/app/data:uid=10001,gid=10001,size=64m",
        args.trainer,
        "python",
        "-c",
        "import sklearn,lightgbm; from bybit_flow.ml.cli import cycle; "
        "print('trainer imports OK',sklearn.__version__,lightgbm.__version__)",
    )
    print(check)


if __name__ == "__main__":
    main()
