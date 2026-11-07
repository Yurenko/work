"""FastAPI dashboard — single Start/Stop button + live prices / PnL."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from app.core.config import get_settings
from app.core.logging import logger
from app.strategy.engine import get_engine

app = FastAPI(title="Hedge Bot PF+PM")
STATIC = Path(__file__).resolve().parents[2] / "static"
if STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    path = STATIC / "index.html"
    if path.exists():
        return path.read_text(encoding="utf-8")
    return "<h1>static/index.html missing</h1>"


@app.get("/api/state")
async def state() -> dict:
    return get_engine().snapshot()


@app.websocket("/ws/state")
async def ws_state(ws: WebSocket) -> None:
    """Push bot snapshot ~4x/sec so the UI does not poll HTTP."""
    await ws.accept()
    try:
        while True:
            await ws.send_json(get_engine().snapshot())
            await asyncio.sleep(0.25)
    except WebSocketDisconnect:
        return
    except Exception:
        return


@app.post("/api/start")
async def start() -> dict:
    eng = get_engine()
    await eng.start()
    return {"ok": True, "running": True}


@app.post("/api/stop")
async def stop() -> dict:
    eng = get_engine()
    await eng.stop()
    return {"ok": True, "running": False}


@app.post("/api/test-buy")
async def test_buy(body: dict | None = None) -> dict:
    usd = float((body or {}).get("usd") or 1.0)
    return await get_engine().test_buy(usd)


@app.get("/api/health")
async def health() -> dict:
    return {"ok": True, "mode": get_settings().mode}


@app.on_event("startup")
async def startup() -> None:
    settings = get_settings()
    if settings.is_live:
        logger.info("LIVE mode — deposits/keys required")


def run() -> None:
    settings = get_settings()
    uvicorn.run(
        "app.dashboard.server:app",
        host=settings.host,
        port=settings.port,
        reload=False,
        log_level=settings.log_level.lower(),
    )
