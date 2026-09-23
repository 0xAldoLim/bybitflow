import asyncio
import json
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from .config import Settings
from .fundamentals import Fact, add_fact
from .models import Signal
from .notifications import embed
from .scanner import Scanner
from .storage import Recorder, Store, now_ms

STATIC = Path(__file__).parent / "static"


def read_report(root, callback):
    """Research reports must not block the live market-data event loop."""
    import sqlite3

    store = Store.__new__(Store)
    store.root = root
    store.db = sqlite3.connect((root / "research.sqlite").resolve().as_uri() + "?mode=ro", uri=True)
    store.db.row_factory = sqlite3.Row
    try:
        return callback(store)
    finally:
        store.close()


class Position(BaseModel):
    symbol: str = Field(pattern=r"^[A-Z0-9]{2,30}$")
    risk_fraction: float = Field(ge=0, le=1)


class Portfolio(BaseModel):
    positions: list[Position] = Field(default_factory=list, max_length=100)
    daily_loss_fraction: float = Field(0, ge=0, le=1)
    weekly_loss_fraction: float = Field(0, ge=0, le=1)


class JournalEntry(BaseModel):
    signal_id: str | None = Field(None, max_length=40)
    note: str = Field(min_length=1, max_length=2000)
    hypothetical_net_r: float | None = Field(None, ge=-100, le=100)
    resolve: bool = False


def create_app(settings=None):
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app):
        store = Store(settings.data_dir)
        store.put("post_terminal_checkpoints", settings.post_terminal_checkpoints)
        recorder = Recorder(store, settings)
        scanner = Scanner(settings, store, recorder)
        app.state.store, app.state.recorder, app.state.scanner = store, recorder, scanner
        task = asyncio.create_task(recorder.run())
        scanner.start()
        yield
        await scanner.stop()
        recorder.running = False
        await task
        store.close()

    app = FastAPI(
        title="Bybit Flow · Research", version="0.1.0", lifespan=lifespan, docs_url=None, redoc_url=None
    )

    @app.middleware("http")
    async def access(request: Request, call_next):
        token = settings.admin_token.get_secret_value()
        host = request.headers.get("host", "").split(":")[0]
        if token:
            import base64

            auth = request.headers.get("authorization", "")
            try:
                user, password = (
                    base64.b64decode(auth.removeprefix("Basic "), validate=True).decode().split(":", 1)
                )
            except Exception:
                user, password = "", ""
            if not secrets.compare_digest(user, "research") or not secrets.compare_digest(password, token):
                return JSONResponse(
                    {"detail": "Authentication required"},
                    401,
                    headers={"WWW-Authenticate": 'Basic realm="Bybit Flow"'},
                )
        elif request.client.host not in {"127.0.0.1", "::1", "testclient"} or host not in {
            "127.0.0.1",
            "localhost",
            "testserver",
        }:
            return JSONResponse({"detail": "Remote dashboard requires FLOW_ADMIN_TOKEN"}, 403)
        if request.method not in {"GET", "HEAD"}:
            origin = request.headers.get("origin")
            if origin and urlparse(origin).netloc != request.headers.get("host"):
                return JSONResponse({"detail": "Cross-origin writes forbidden"}, 403)
            if request.headers.get("sec-fetch-site") == "cross-site":
                return JSONResponse({"detail": "Cross-site writes forbidden"}, 403)
        response = await call_next(request)
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'"
        )
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/")
    async def index():
        return FileResponse(STATIC / "index.html")

    @app.get("/api/ml")
    async def ml_research(request: Request):
        from .ml.registry import Registry

        return await asyncio.to_thread(
            read_report, settings.data_dir, lambda store: Registry(store).cached_summary()
        )

    @app.get("/api/funnel")
    async def funnel_view(request: Request):
        from .funnel import status

        return status(request.app.state.store, configured=bool(settings.research_webhook.get_secret_value()))

    @app.get("/api/delivery")
    async def delivery_view(request: Request):
        from .identity import delivery_status

        return delivery_status(request.app.state.store)

    @app.get("/api/horizons")
    async def horizon_research(request: Request):
        from dataclasses import asdict

        from .horizons import PROFILES, session_context
        from .observations import metrics

        def report(store):
            return dict(
                profiles={k: asdict(v) for k, v in PROFILES.items()},
                session=session_context(now_ms()),
                observations=dict(
                    store.db.execute("SELECT status,count(*) FROM observations GROUP BY status")
                ),
                score_buckets=metrics(store),
                storage=store.get("storage_status", {}),
                selection=store.get("deep_selection", {}),
            )

        return await asyncio.to_thread(read_report, settings.data_dir, report)

    @app.get("/api/observations/{signal_id}")
    async def observation(signal_id: str, request: Request):
        row = request.app.state.store.db.execute(
            "SELECT payload FROM observations WHERE signal_id=?", (signal_id,)
        ).fetchone()
        return json.loads(row[0]) if row else {"status": "No post-terminal observation"}

    @app.get("/api/exchanges")
    async def exchange_health(request: Request):
        from .exchanges import VENUES

        store, scanner = request.app.state.store, request.app.state.scanner
        return {
            "configured": settings.market_source,
            "active": scanner.exchange,
            "source_ready": scanner.source_ready,
            "transition": store.get("active_exchange"),
            "probes": [store.get("probe:" + n, {"exchange": n, "status": "NOT_TESTED"}) for n in VENUES],
        }

    @app.get("/api/orderflow/{symbol}")
    async def orderflow_view(symbol: str, request: Request):
        from .flow_views import profiles

        scanner = request.app.state.scanner
        tape, context = scanner.streams.tapes.get(symbol), scanner.context.get(symbol)
        if not tape or not context:
            return {"available": False, "reason": "No selected live tape and closed-candle context"}
        from .features import candle_features

        now = now_ms()
        return {
            "exchange": scanner.exchange,
            "profiles": profiles(
                tape,
                context["instrument"].tick,
                candle_features(context["m15"], now)["atr"],
                now,
                scanner.exchange,
            ),
            "tape": [
                dict(event_ms=t.event_ms, side=t.side, price=str(t.price), size=str(t.size))
                for t in list(tape.trades)[-50:]
            ],
            "coverage_start": tape.coverage_start,
        }

    @app.get("/api/liquidity/{symbol}")
    async def liquidity_frames(symbol: str, request: Request):
        scanner = request.app.state.scanner
        return {
            "exchange": scanner.exchange,
            "symbol": symbol,
            "frames": list(scanner.streams.frames.get(symbol, []))[-120:],
            "methodology": "5-second observed depth samples, not every update or hidden liquidity",
        }

    @app.get("/assets/{name}")
    async def asset(name: str):
        if name not in {"app.js", "style.css"}:
            raise HTTPException(404)
        return FileResponse(STATIC / name)

    @app.get("/healthz")
    async def health(request: Request):
        rec, scanner = request.app.state.recorder, request.app.state.scanner
        status = 200 if rec.healthy and scanner.status["state"] != "error" else 503
        return JSONResponse(
            {
                "ok": status == 200,
                "scanner": scanner.status,
                "recording": rec.healthy,
                "alerts_only": True,
            },
            status,
        )

    @app.get("/api/overview")
    async def overview(request: Request):
        from .thesis_health import summary as health_summary

        store, rec, scanner = request.app.state.store, request.app.state.recorder, request.app.state.scanner
        return dict(
            scanner=scanner.status,
            exchange=scanner.exchange,
            watchlist=store.get("watchlist", []),
            signals=store.signals(),
            health=dict(
                recorder=rec.healthy,
                recorder_reason=rec.reason,
                recorder_metrics=rec.metrics(),
                thesis_health=health_summary(store),
                macro=store.get("macro_health", {}),
                queued=rec.queue.qsize(),
                records_written=rec.written,
                disk_bytes=rec.disk_bytes,
                streams=store.get("stream_health", {"connected": False}),
                gap=store.get("recorder_gap"),
                quotes=store.get("quote_health"),
            ),
            qualification="Research alerts; model qualification evaluated per signal",
            horizons=store.get("horizon_counts", {}),
            storage=store.get("storage_status", {}),
            at_ms=now_ms(),
        )

    @app.get("/api/markets/{symbol}")
    async def market(symbol: str, request: Request):
        store, streams = request.app.state.store, request.app.state.scanner.streams
        data = store.get("market:" + symbol)
        if data is None:
            raise HTTPException(404, "No collected data for this symbol")
        if data.get("exchange", "bybit") != request.app.state.scanner.exchange:
            data["book"], data["book_fresh"] = (
                {"available": False, "reason": "stored market context is from a previous source"},
                False,
            )
            return data
        book = streams.books.get(symbol)
        data["book"] = book.features(now_ms()) if book else {"available": False}
        data["book_fresh"] = book.fresh(now_ms(), settings.book_stale_ms) if book else False
        return data

    @app.get("/api/signals/{ident}")
    async def detail(ident: str, request: Request):
        r = request.app.state.store.db.execute("SELECT payload FROM signals WHERE id=?", (ident,)).fetchone()
        if not r:
            raise HTTPException(404, "Unknown signal")
        signal = Signal.model_validate_json(r[0])
        return {"signal": signal, "discord_preview": embed(signal, settings.dashboard_url)}

    @app.get("/api/settings")
    async def config():
        return settings.public() | {
            "discord_configured": bool(settings.discord_webhook.get_secret_value()),
            "research_discord_configured": bool(settings.research_webhook.get_secret_value()),
            "editing": "Change environment configuration and restart; secrets never returned",
        }

    @app.get("/api/journal")
    async def journal(request: Request):
        return request.app.state.store.rows("journal")

    @app.post("/api/journal")
    async def journal_add(entry: JournalEntry, request: Request):
        store = request.app.state.store
        signal = None
        if entry.signal_id:
            row = store.db.execute("SELECT payload FROM signals WHERE id=?", (entry.signal_id,)).fetchone()
            if not row:
                raise HTTPException(404, "Unknown signal")
            signal = Signal.model_validate_json(row[0])
        if entry.resolve and (signal is None or signal.state != "ALERTED"):
            raise HTTPException(409, "Only an alerted signal can be manually resolved")
        payload = entry.model_dump() | {
            "at_ms": now_ms(),
            "source": "manual paper journal; not verified execution",
        }
        with store.db:
            store.db.execute(
                "INSERT INTO journal(at_ms,payload) VALUES(?,?)", (payload["at_ms"], json.dumps(payload))
            )
        if entry.resolve:
            signal.state = "RESOLVED"
            store.signal(signal, "manual paper outcome")
        return payload

    @app.get("/api/portfolio")
    async def portfolio_get(request: Request):
        return request.app.state.store.get("portfolio")

    @app.post("/api/portfolio")
    async def portfolio_put(portfolio: Portfolio, request: Request):
        payload = portfolio.model_dump() | {"at_ms": now_ms()}
        request.app.state.store.put("portfolio", payload)
        return payload

    @app.post("/api/facts")
    async def facts_put(fact: Fact, request: Request):
        try:
            add_fact(request.app.state.store, fact, now_ms())
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None
        return {"stored": True, "scoring": "context only; no automatic authoritative quality score"}

    @app.get("/api/research")
    async def research(request: Request):
        store = request.app.state.store
        return dict(
            experiments=store.rows("experiments"),
            segments=store.rows("segments", 50),
            facts=store.rows("facts"),
            qualification="Uncalibrated",
        )

    return app


app = create_app()
