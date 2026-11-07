"""Predict.fun orderbook WebSocket.

Endpoint: wss://ws.predict.fun/ws
Auth: x-api-key header (or ?apiKey=) on handshake.
Topics:
  predictOrderbook/{marketId}  — full book on each change
  predictTopOfBook/{marketId}  — best bid/ask on each change
Protocol:
  → {"requestId": N, "method": "subscribe", "params": ["predictOrderbook/123"]}
  ← {"type": "R", "requestId": N, "success": true}
  ← {"type": "M", "topic": "predictOrderbook/123", "data": {...}}
Heartbeat every ~15s: server topic=heartbeat; client echoes method=heartbeat.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from app.core.config import get_settings
from app.core.logging import logger

WSS_DEFAULT = "wss://ws.predict.fun/ws"


def _complement(price: float) -> float:
    return max(0.0, min(1.0, round(1.0 - float(price), 6)))


def _empty_book() -> dict[str, Any]:
    return {
        "up": {"bid": None, "ask": None},
        "down": {"bid": None, "ask": None},
        "updated": 0.0,
    }


def _top(levels: Any, is_bid: bool) -> Optional[float]:
    if not levels:
        return None
    try:
        prices: list[float] = []
        for x in levels:
            if isinstance(x, (list, tuple)) and x:
                prices.append(float(x[0]))
            elif isinstance(x, dict):
                prices.append(float(x.get("price") or x.get("p") or 0))
            else:
                prices.append(float(x))
        prices = [p for p in prices if 0 < p < 1]
        if not prices:
            return None
        return round(max(prices) if is_bid else min(prices), 6)
    except (TypeError, ValueError, IndexError):
        return None


def _write_side(book: dict, side: str, bid: Optional[float], ask: Optional[float]) -> None:
    other = "down" if side == "up" else "up"
    if bid is not None:
        book[side]["bid"] = bid
        book[other]["ask"] = _complement(bid)
    if ask is not None:
        book[side]["ask"] = ask
        book[other]["bid"] = _complement(ask)


def _level_price(level: Any) -> Optional[float]:
    if level is None:
        return None
    try:
        if isinstance(level, (list, tuple)) and level:
            p = float(level[0])
        elif isinstance(level, dict):
            p = float(level.get("price") or level.get("p") or 0)
        else:
            p = float(level)
        if 0 < p < 1:
            return p
    except (TypeError, ValueError, IndexError):
        return None
    return None


class PredictOrderbookWS:
    """Maintain live Up/Down books for subscribed Predict markets.

    Yes-side book = Up; Down is complement (1 - price).
    Market switches re-subscribe on the same socket (no full reconnect).
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self.api_key = (self.settings.predict_api_key or "").strip().strip('"').strip("'")
        self.base = (self.settings.predict_wss_url or WSS_DEFAULT).rstrip("/")
        self._ws: Any = None
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self.books: dict[str, dict[str, Any]] = {}
        self.market_meta: dict[str, dict] = {}
        self.on_update: Optional[Callable[[], None]] = None
        self.last_error: str = ""
        self._request_id = 1
        self._last_data_at = 0.0
        self._msg_count = 0
        self._market_ids: list[str] = []
        self._subscribed: set[str] = set()  # topic strings currently subscribed
        self._resub_event: Optional[asyncio.Event] = None
        self._close_event: Optional[asyncio.Event] = None

    def set_markets(self, markets: list[dict]) -> None:
        self.market_meta = {str(m["id"]): m for m in markets if m.get("id")}
        ids = [str(m["id"]) for m in markets if m.get("id")]
        changed = set(ids) != set(self._market_ids)
        self._market_ids = ids
        for mid in ids:
            self.books.setdefault(mid, _empty_book())
        if changed:
            logger.info("Predict WS market set → %s", ids)
            if self._resub_event is not None:
                self._resub_event.set()

    def best(self, market_id: str, side: str) -> tuple[Optional[float], Optional[float]]:
        b = self.books.get(str(market_id), {}).get(side, {})
        return b.get("bid"), b.get("ask")

    def apply_rest_book(
        self,
        market_id: str,
        book: dict,
        side: str | None = None,
        *,
        force: bool = False,
    ) -> None:
        """Inject REST snapshot only if WS book is missing or stale."""
        mid = str(market_id)
        cur = self.books.get(mid) or {}
        age = time.time() - float(cur.get("updated") or 0)
        if not force and cur.get("updated") and age < 2.0:
            return
        payload = book
        if isinstance(book, dict) and "data" in book and isinstance(book["data"], dict):
            payload = book["data"]
        bids = (payload or {}).get("bids") or []
        asks = (payload or {}).get("asks") or []
        self._apply_book(mid, bids, asks, (payload or {}).get("updateTimestampMs"))

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._running = True
        self._resub_event = asyncio.Event()
        self._close_event = asyncio.Event()
        self._task = asyncio.create_task(self._run_loop(), name="predict-ws")

    def stop(self) -> None:
        self._running = False
        if self._close_event is not None:
            self._close_event.set()
        if self._resub_event is not None:
            self._resub_event.set()
        if self._task:
            self._task.cancel()
            self._task = None

    async def _run_loop(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                await self._connect_once()
                backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.last_error = str(e)
                logger.warning("Predict WS reconnect: %s", e)
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 1.5)

    async def _connect_once(self) -> None:
        extra_headers = {}
        if self.api_key:
            extra_headers["x-api-key"] = self.api_key

        url = self.base
        if self.api_key and "apiKey=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}apiKey={self.api_key}"

        logger.info("Predict WS connecting %s markets=%s", self.base, self._market_ids)
        self._subscribed.clear()
        if self._resub_event is not None:
            self._resub_event.clear()
        if self._close_event is not None:
            self._close_event.clear()

        async with websockets.connect(
            url,
            additional_headers=extra_headers or None,
            ping_interval=None,
            max_size=2**22,
        ) as ws:
            self._ws = ws
            await self._sync_subscriptions(ws)
            logger.info(
                "Predict WS subscribed markets=%s topics=%d",
                self._market_ids,
                len(self._subscribed),
            )

            while self._running:
                if self._close_event is not None and self._close_event.is_set():
                    break
                # Resubscribe on market change without dropping the socket
                if self._resub_event is not None and self._resub_event.is_set():
                    self._resub_event.clear()
                    await self._sync_subscriptions(ws)
                    logger.info(
                        "Predict WS resubscribed markets=%s topics=%d",
                        self._market_ids,
                        len(self._subscribed),
                    )

                try:
                    recv_task = asyncio.create_task(ws.recv())
                    waiters: set[asyncio.Task] = {recv_task}
                    if self._resub_event is not None:
                        waiters.add(asyncio.create_task(self._resub_event.wait()))
                    if self._close_event is not None:
                        waiters.add(asyncio.create_task(self._close_event.wait()))

                    done, pending = await asyncio.wait(
                        waiters,
                        return_when=asyncio.FIRST_COMPLETED,
                        timeout=15.0,
                    )
                    for t in pending:
                        t.cancel()
                        try:
                            await t
                        except (asyncio.CancelledError, Exception):
                            pass

                    if not self._running:
                        break
                    if self._close_event is not None and self._close_event.is_set():
                        break
                    # resub event handled at top of next loop iteration
                    if recv_task in done:
                        try:
                            raw = recv_task.result()
                        except ConnectionClosed:
                            break
                        except Exception as e:
                            logger.warning("Predict WS recv: %s", e)
                            break
                        await self._handle(ws, raw)
                except ConnectionClosed:
                    break

        self._ws = None
        self._subscribed.clear()

    async def _sync_subscriptions(self, ws: Any) -> None:
        """Diff current topics vs desired; unsubscribe old, subscribe new."""
        desired: set[str] = set()
        for mid in self._market_ids:
            desired.add(f"predictOrderbook/{mid}")
            desired.add(f"predictTopOfBook/{mid}")

        for topic in sorted(self._subscribed - desired):
            await self._request(ws, "unsubscribe", topic)
            self._subscribed.discard(topic)

        for topic in sorted(desired - self._subscribed):
            ok = await self._request(ws, "subscribe", topic)
            if ok:
                self._subscribed.add(topic)

    async def _request(self, ws: Any, method: str, topic: str) -> bool:
        rid = self._request_id
        self._request_id += 1
        msg = {"requestId": rid, "method": method, "params": [topic]}
        try:
            await ws.send(json.dumps(msg))
            return True
        except Exception as e:
            logger.warning("Predict WS %s %s fail: %s", method, topic, e)
            return False

    async def _handle(self, ws: Any, raw: Any) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        mtype = msg.get("type")
        if mtype == "R":
            if not msg.get("success"):
                err = msg.get("error") or msg
                self.last_error = str(err)
                logger.warning("Predict WS request fail: %s", err)
            return

        if mtype != "M":
            return

        topic = str(msg.get("topic") or "")
        data = msg.get("data")

        if topic == "heartbeat":
            try:
                await ws.send(json.dumps({"method": "heartbeat", "data": data}))
            except Exception:
                pass
            return

        if topic.startswith("predictOrderbook/"):
            mid = topic.split("/", 1)[1]
            if not isinstance(data, dict):
                return
            bids = data.get("bids") or []
            asks = data.get("asks") or []
            self._apply_book(mid, bids, asks, data.get("updateTimestampMs"))
            self._note_data()
            return

        if topic.startswith("predictTopOfBook/"):
            mid = topic.split("/", 1)[1]
            if not isinstance(data, dict):
                return
            bid = _level_price(
                data.get("bid")
                or data.get("bestBid")
                or (data.get("bids") or [None])[0]
            )
            ask = _level_price(
                data.get("ask")
                or data.get("bestAsk")
                or (data.get("asks") or [None])[0]
            )
            if bid is None and ask is None:
                return
            book = self.books.setdefault(str(mid), _empty_book())
            _write_side(book, "up", bid, ask)
            book["updated"] = time.time()
            self._note_data()
            return

    def _note_data(self) -> None:
        self._msg_count += 1
        self._last_data_at = time.time()
        if self._msg_count == 1 or self._msg_count % 500 == 0:
            logger.info("Predict WS live data msgs=%d", self._msg_count)
        if self.on_update:
            try:
                self.on_update()
            except Exception:
                pass

    def _apply_book(
        self,
        market_id: str,
        bids: Any,
        asks: Any,
        ts: Any = None,
    ) -> None:
        mid = str(market_id)
        book = self.books.setdefault(mid, _empty_book())
        bid = _top(bids, is_bid=True)
        ask = _top(asks, is_bid=False)
        _write_side(book, "up", bid, ask)
        book["updated"] = time.time()
        if ts:
            try:
                book["ts_ms"] = int(float(ts))
            except (TypeError, ValueError):
                pass
