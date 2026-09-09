import argparse
import asyncio
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Settings
from .storage import Recorder, Store, now_ms


def main():
    parser = argparse.ArgumentParser(description="Bybit Flow: public data and paper research; no execution")
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    sub.add_parser("scan-once")
    candles = sub.add_parser("download-candles")
    candles.add_argument("symbol")
    candles.add_argument("--interval", choices=["15", "60", "240", "D"], default="60")
    candles.add_argument("--days", type=int, default=90)
    archive = sub.add_parser("download-trades")
    archive.add_argument("symbol")
    archive.add_argument("day", help="UTC YYYY-MM-DD; download an actual official daily archive")
    agg = sub.add_parser("aggregate-trades")
    agg.add_argument("path", type=Path)
    agg.add_argument("--minutes", type=int, default=60)
    replay = sub.add_parser("replay")
    replay.add_argument("paths", nargs="+", type=Path)
    research = sub.add_parser("research")
    research.add_argument("candles", type=Path)
    tv = sub.add_parser("tv-research")
    tv.add_argument("events", type=Path)
    tv.add_argument("candles", type=Path)
    tv.add_argument("--symbol", required=True)
    sub.add_parser("tv-export")
    backup = sub.add_parser("backup")
    backup.add_argument("target", type=Path)
    sub.add_parser("retention-plan")
    sub.add_parser("sample-alert")
    ml = sub.add_parser("ml", help="Offline supervised research; use ml --help")
    ml.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    settings = Settings()
    from .observability import configure_logging

    configure_logging()
    if args.command == "serve":
        if args.host not in {"127.0.0.1", "localhost", "::1"} and not settings.admin_token.get_secret_value():
            parser.error("Binding remotely requires FLOW_ADMIN_TOKEN")
        import uvicorn

        uvicorn.run("bybit_flow.app:app", host=args.host, port=args.port, workers=1, access_log=False)
        return
    store = Store(settings.data_dir)
    try:
        if args.command == "ml":
            from .ml.cli import run

            run(args.arguments, settings, store)
        elif args.command == "backup":
            store.backup(args.target)
            print(args.target)
        elif args.command == "retention-plan":
            cutoff = now_ms() - settings.raw_retention_days * 86_400_000
            plan = [r for r in store.rows("segments", 100000) if r["collected_ms"] < cutoff]
            print(
                json.dumps(
                    {
                        "action": "report only; archive verified segments before manual removal",
                        "segments": plan,
                    },
                    indent=2,
                )
            )
        elif args.command == "tv-export":
            for row in store.db.execute("SELECT payload FROM tv_inbox ORDER BY received_ms,rowid"):
                print(row[0])
        elif args.command == "tv-research":
            from .research import save_experiment
            from .tv_research import family_replay, read_candles, read_events

            result = family_replay(
                read_events(args.events), read_candles(args.candles), settings, args.symbol
            )
            print(
                save_experiment(
                    store, "TV family replay", settings.public(), result, [args.events, args.candles]
                )
            )
        elif args.command == "sample-alert":
            from .models import Signal
            from .notifications import embed

            sample = Signal(
                id="SYNTHETIC-FORMAT-ONLY",
                symbol="EXAMPLEUSDT",
                direction="LONG",
                family="liquidity_sweep",
                created_ms=0,
                expires_ms=3_600_000,
                regime="range",
                entry=100,
                zone=(99, 101),
                stop=95,
                tp1=115,
                tp2=120,
                invalidation="Illustrative stop breach",
                reason="Synthetic formatting example; not market data",
            )
            print(json.dumps(embed(sample, settings.dashboard_url), indent=2))
        elif args.command == "aggregate-trades":
            from .history import aggregate_trades

            if args.minutes <= 0:
                parser.error("minutes must be positive")
            rows = aggregate_trades(args.path, args.minutes)
            path = args.path.with_suffix(f".{args.minutes}m.parquet")
            if path.exists():
                parser.error("Output exists")
            pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")
            print(path)
        elif args.command == "research":
            from .models import Candle
            from .research import research_run, save_experiment

            bars = [Candle(**r) for r in pq.read_table(args.candles).to_pylist()]
            result = research_run(bars, settings)
            print(save_experiment(store, "OHLCV baseline", settings.public(), result, [args.candles]))
        elif args.command == "replay":
            from .replay import replay, segment_rows
            from .research import save_experiment

            result = replay(segment_rows(args.paths), settings)
            print(save_experiment(store, "recorded-event replay", settings.public(), result, args.paths))
        else:
            asyncio.run(network_command(args, settings, store))
    finally:
        store.close()


async def network_command(args, settings, store):
    if args.command == "download-trades":
        from .history import download_trades

        print(
            json.dumps(await download_trades(args.symbol, args.day, settings.data_dir / "archives"), indent=2)
        )
        return
    from .ingestion import Bybit, candle_records
    from .scanner import Scanner

    recorder = Recorder(store, settings)
    writer = asyncio.create_task(recorder.run())
    api = Bybit(settings, recorder)
    try:
        if args.command == "scan-once":
            scanner = Scanner(settings, store, recorder)
            try:
                print(json.dumps(await scanner.scan_once(), indent=2))
            finally:
                await scanner.stop()
        elif args.command == "download-candles":
            if not 1 <= args.days <= 3650:
                raise ValueError("days must be between 1 and 3650")
            asof = int((await api.get("time"))["time"])
            minutes = 1440 if args.interval == "D" else int(args.interval)
            rows = await api.candles(
                args.symbol,
                args.interval,
                asof,
                limit=args.days * 1440 // minutes,
                start=asof - args.days * 86_400_000,
            )
            if not rows:
                raise ValueError("No actual candles returned")
            path = settings.data_dir / f"{args.symbol}-{args.interval}-{asof}.parquet"
            pq.write_table(pa.Table.from_pylist(candle_records(rows)), path, compression="zstd")
            print(path)
    finally:
        await api.close()
        recorder.running = False
        await writer


if __name__ == "__main__":
    main()
