"""Live Chainlink BTC/USD prints from Polymarket's public market-data socket.

This is the stream the BTC 5-minute page uses for the running price. It is not
the order book, and it is not the contract price. Settlement of the 5-minute
market is the Chainlink BTC/USD 60-second TWAP
(https://data.chain.link/streams/btc-usd-twap-60s-streams). The socket print is
the last price we can read before that TWAP is published.

Subscribe payload, verified against the live socket: action=subscribe,
topic=crypto_prices_chainlink, type=*, filters={"symbol":"btc/usd"}.
"""

from __future__ import annotations

import asyncio
import json
from typing import Optional

import websockets

from app.core.logging import logger
from app.polymarket.dns import install as install_public_dns
from app.strategy.btc_candle import parse_chainlink_frame

URL = "wss://ws-live-data.polymarket.com"
SUBSCRIBE = {
    "action": "subscribe",
    "subscriptions": [
        {
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": "{\"symbol\":\"btc/usd\"}",
        }
    ],
}


class ChainlinkBtcStream:
    def __init__(self) -> None:
        self.price: Optional[float] = None
        self.ts: float = 0.0
        self.source: str = "polymarket.ws.crypto_prices_chainlink"
        self.last_error: str = ""
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def latest(self) -> Optional[tuple[float, float, str]]:
        if self.price is None or self.ts <= 0:
            return None
        return float(self.price), float(self.ts), self.source

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        install_public_dns()
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="chainlink-btc")

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None

    async def _loop(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                async with websockets.connect(URL, ping_interval=20, open_timeout=15) as ws:
                    await ws.send(json.dumps(SUBSCRIBE))
                    logger.info("Chainlink BTC stream subscribed")
                    backoff = 1.0
                    while self._running:
                        raw = await ws.recv()
                        for price, ts, source in parse_chainlink_frame(raw):
                            if ts >= self.ts:
                                self.price = price
                                self.ts = ts
                                self.source = source
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.last_error = str(exc)
                logger.warning("Chainlink BTC stream reconnect: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(20.0, backoff * 1.5)
