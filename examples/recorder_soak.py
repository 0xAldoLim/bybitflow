"""Offline recorder soak. Never contacts exchanges or Discord."""

import argparse
import asyncio
import json
import time
from pathlib import Path

from bybit_flow.config import Settings
from bybit_flow.storage import Recorder, Store, now_ms


async def run(seconds, rate, root):
    cfg = Settings(_env_file=None, data_dir=root, max_storage_gb=10)
    store = Store(root)
    recorder = Recorder(store, cfg)
    count = 0
    snapshots = []
    started = time.monotonic()

    async def produce():
        nonlocal count
        next_report = started
        try:
            while time.monotonic() - started < seconds:
                for _ in range(max(1, rate // 20)):
                    at = now_ms()
                    count += 1
                    recorder.offer(
                        "ws/publicTrade.BTCUSDT",
                        "BTCUSDT",
                        at,
                        {"data": [dict(T=at, p="80000", v="0.01", i=str(count), S="Buy")]},
                        receipt_ms=at,
                    )
                if time.monotonic() >= next_report:
                    snapshots.append(dict(elapsed_seconds=time.monotonic() - started, **recorder.metrics()))
                    next_report += 30
                await asyncio.sleep(0.05)
        finally:
            recorder.running = False

    try:
        await asyncio.gather(recorder.run(), produce())
        elapsed = time.monotonic() - started
        result = dict(
            offered=count,
            written=recorder.written,
            elapsed_seconds=elapsed,
            actual_rate=count / elapsed,
            requested_rate=rate,
            resource_limit="2 CPU, 4 GiB",
            scope="isolated offline recorder; not a live scanner soak",
            samples=snapshots,
            final=recorder.metrics(),
        )
        print(json.dumps(result), flush=True)
        assert recorder.healthy and recorder.written == count and recorder.events_dropped == 0
    finally:
        store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--rate", type=int, default=1000)
    parser.add_argument("--data-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.seconds < 1 or args.rate < 1:
        parser.error("seconds and rate must be positive")
    asyncio.run(run(args.seconds, args.rate, args.data_dir))
