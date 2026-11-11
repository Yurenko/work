"""Polymarket CLOB + Gamma client (kept from original hedge-bot)."""

from __future__ import annotations

import asyncio
import json
import math
import time
from typing import Any, Optional

import httpx

from app.core.config import get_settings
from app.core.logging import logger
from app.polymarket.markets import parse_updown_market, updown_slug, resolved_updown

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _f(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _floor_dec(value: float, decimals: int) -> float:
    f = 10 ** decimals
    return math.floor(value * f + 1e-12) / f


def _ceil_dec(value: float, decimals: int) -> float:
    f = 10 ** decimals
    return math.ceil(value * f - 1e-12) / f


def _quantize_buy_size(size: float, limit: float) -> float:
    """Largest size ≤ target with ≤4dp such that size*limit has ≤2dp (maker USDC rule).

    CLOB rejects: maker amount max 2 decimals, taker (shares) max 4.
    size_units (1e-4) * limit_cents must be divisible by 10000.
    """
    if size <= 0 or limit <= 0:
        return 0.0
    limit_cents = int(round(float(limit) * 100))
    if limit_cents <= 0:
        return 0.0
    units = int(math.floor(float(size) * 10000 + 1e-9))
    while units > 0:
        if (units * limit_cents) % 10000 == 0:
            return units / 10000.0
        units -= 1
    return 0.0


def polymarket_buy_order(target_shares: float, ask: float) -> tuple[float, float]:
    """Equal-share BUY for Polymarket CLOB.

    Filled shares should stay ≤ PF net shares (except min-notional bump).

    Maker USDC is floored to target*ask so a FOK fill cheaper than `limit`
    cannot deliver more shares than we ordered for.
    """
    MIN_CENTS = 105   # $1.05
    BUMP_CENTS = 120  # $1.20
    MAX_BUMP_CENTS = 150
    if ask is None or ask <= 0 or target_shares is None or target_shares <= 0:
        return 0.0, 0.0
    ask = float(ask)
    target = float(target_shares)

    limit = _ceil_dec(ask, 2)
    if limit < ask - 1e-12:
        limit = _ceil_dec(ask + 1e-9, 2)
    limit = min(0.99, max(0.01, limit))

    maker_cents = int(math.floor(target * ask * 100 + 1e-9))
    bumped = False
    if maker_cents < MIN_CENTS:
        bumped = True
        maker_cents = min(MAX_BUMP_CENTS, max(BUMP_CENTS, MIN_CENTS))

    # size from budget at limit price, 4dp floor
    size = math.floor(maker_cents / 100.0 / limit * 10000 + 1e-9) / 10000.0
    while size > 0 and int(round(size * limit * 100)) > maker_cents:
        size = math.floor(size * 10000 - 1) / 10000.0
    if not bumped and size > target:
        size = math.floor(target * 10000 + 1e-9) / 10000.0
        while size > 0 and int(round(size * limit * 100)) > maker_cents:
            size = math.floor(size * 10000 - 1) / 10000.0

    if size <= 0:
        return 0.0, 0.0
    return limit, size



def top_of_book(bids: list, asks: list) -> tuple[Optional[float], Optional[float]]:
    bid = max((_f(x.get("price") if isinstance(x, dict) else (x[0] if x else None)) or 0) for x in bids) if bids else None
    ask = min((_f(x.get("price") if isinstance(x, dict) else (x[0] if x else None)) or 1) for x in asks) if asks else None
    if bid is not None and bid <= 0:
        bid = None
    if ask is not None and (ask <= 0 or ask >= 1):
        ask = None
    return bid, ask


class PolymarketClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._http: Optional[httpx.AsyncClient] = None
        self._clob = None
        self.books: dict[str, dict[str, Optional[float]]] = {}
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_tokens: list[str] = []
        self._running = False
        self.last_error: str = ""

    async def close(self) -> None:
        self._running = False
        if self._ws_task:
            self._ws_task.cancel()
            self._ws_task = None
        if self._http:
            await self._http.aclose()
            self._http = None

    async def _ensure_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=20.0)
        return self._http

    async def event_by_slug(self, slug: str) -> Optional[dict]:
        http = await self._ensure_http()
        try:
            r = await http.get(f"{GAMMA}/events", params={"slug": slug})
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and data:
                return data[0]
            if isinstance(data, dict):
                return data
        except Exception as e:
            self.last_error = str(e)
            logger.warning("PM event_by_slug %s: %s", slug, e)
        return None

    async def current_btc_5m(self, now: Optional[float] = None) -> Optional[dict]:
        """Resolve the live BTC 5m window on Polymarket.

        Window is [start, start+300) in unix seconds (same as slug suffix).
        Gamma often still returns the previous event as closed=false for a while,
        so we always prefer the slug whose [start,end) contains `now`.
        """
        now = float(now or time.time())
        dur = 300
        cur_start = int(now) // dur * dur
        # Try current, next (pre-list), previous (only if still inside by clock)
        starts = [cur_start, cur_start + dur, cur_start - dur]
        parsed_by_start: dict[int, dict] = {}
        for st in starts:
            slug = f"btc-updown-5m-{st}"
            event = await self.event_by_slug(slug)
            if not event:
                continue
            parsed = parse_updown_market(event)
            if not parsed:
                continue
            # Force window bounds from slug (authoritative for 5m series)
            parsed["slug"] = slug
            parsed["start"] = float(st)
            parsed["end"] = float(st + dur)
            parsed_by_start[st] = parsed

        # 1) current window if listed
        if cur_start in parsed_by_start:
            best = parsed_by_start[cur_start]
            logger.info(
                "PM market slug=%s ends_in=%ds (current window)",
                best.get("slug"),
                int(best["end"] - now),
            )
            return best

        # 2) if we are in the last seconds and Gamma lags, still build synthetic from next? no
        # 3) previous only when still within its end (+grace for settle)
        prev = cur_start - dur
        if prev in parsed_by_start and now < parsed_by_start[prev]["end"] + 15:
            best = parsed_by_start[prev]
            logger.info(
                "PM market slug=%s ends_in=%ds (prev, gamma lag)",
                best.get("slug"),
                int(best["end"] - now),
            )
            return best

        # 4) next window if already published and current missing (gap)
        nxt = cur_start + dur
        if nxt in parsed_by_start:
            best = parsed_by_start[nxt]
            logger.info(
                "PM market slug=%s ends_in=%ds (next, current missing)",
                best.get("slug"),
                int(best["end"] - now),
            )
            return best

        logger.warning("PM: no btc-updown-5m event for starts %s", starts)
        return None


    async def resolved_leg_price(self, slug: str, direction: str) -> Optional[float]:
        event = await self.event_by_slug(slug)
        res = resolved_updown(event) if event else None
        if res is None:
            # Fallback: /markets?slug= (sometimes more up-to-date than /events)
            try:
                http = await self._ensure_http()
                r = await http.get(f"{GAMMA}/markets", params={"slug": slug})
                if r.status_code == 200:
                    data = r.json()
                    mkt = data[0] if isinstance(data, list) and data else data
                    if isinstance(mkt, dict):
                        res = resolved_updown({"markets": [mkt], "closed": mkt.get("closed")})
            except Exception as e:
                logger.debug("PM markets slug fallback: %s", e)
        if not res:
            return None
        return res.get(direction.lower())

    async def rest_book(self, token_id: str) -> dict:
        http = await self._ensure_http()
        try:
            r = await http.get(f"{CLOB}/book", params={"token_id": token_id})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            self.last_error = str(e)
            return {"bids": [], "asks": []}

    async def refresh_quotes(self, token_ids: list[str]) -> None:
        for tid in token_ids:
            book = await self.rest_book(tid)
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            bid, ask = top_of_book(bids, asks)
            self.books[tid] = {"bid": bid, "ask": ask}

    def best(self, token_id: str) -> tuple[Optional[float], Optional[float]]:
        b = self.books.get(str(token_id), {})
        return b.get("bid"), b.get("ask")

    def start_ws(self, token_ids: list[str]) -> None:
        new_tokens = [str(t) for t in token_ids if t]
        changed = new_tokens != self._ws_tokens
        self._ws_tokens = new_tokens
        if not new_tokens:
            return
        # Must restart WS when asset ids change, otherwise we keep old book
        if self._ws_task and not self._ws_task.done():
            if not changed:
                return
            self._running = False
            self._ws_task.cancel()
            self._ws_task = None
        self._running = True
        self._ws_task = asyncio.create_task(self._ws_loop(), name="pm-ws")
        logger.info("PM WS (re)start tokens=%d changed=%s", len(new_tokens), changed)

    async def _ws_loop(self) -> None:
        import websockets

        backoff = 1.0
        while self._running:
            try:
                async with websockets.connect(WS_URL, ping_interval=20) as ws:
                    sub = {
                        "assets_ids": self._ws_tokens,
                        "type": "market",
                    }
                    await ws.send(json.dumps(sub))
                    logger.info("Polymarket WS subscribed %d tokens", len(self._ws_tokens))
                    backoff = 1.0
                    while self._running:
                        raw = await ws.recv()
                        self._handle_ws(raw)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.last_error = str(e)
                logger.warning("Polymarket WS reconnect: %s", e)
                await asyncio.sleep(backoff)
                backoff = min(20.0, backoff * 1.5)

    def _handle_ws(self, raw: Any) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if raw in ("PONG", "PING"):
            return
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return
        if isinstance(msg, list):
            for m in msg:
                self._handle_ws_one(m)
        else:
            self._handle_ws_one(msg)

    def _handle_ws_one(self, msg: dict) -> None:
        if not isinstance(msg, dict):
            return
        et = str(msg.get("event_type") or msg.get("type") or "")
        asset = str(msg.get("asset_id") or msg.get("asset") or "")
        if not asset:
            return
        if et in ("book", "price_change", "best_bid_ask"):
            bids = msg.get("bids") or []
            asks = msg.get("asks") or []
            if et == "best_bid_ask":
                bid = _f(msg.get("best_bid") or msg.get("bid"))
                ask = _f(msg.get("best_ask") or msg.get("ask"))
            else:
                bid, ask = top_of_book(bids, asks)
            cur = self.books.get(asset, {})
            if bid is not None:
                cur["bid"] = bid
            if ask is not None:
                cur["ask"] = ask
            self.books[asset] = cur

    def _ensure_clob(self) -> Any:
        if self._clob is not None:
            return self._clob
        if not self.settings.private_key:
            raise RuntimeError("PRIVATE_KEY required for LIVE Polymarket")
        try:
            from py_clob_client_v2.client import ClobClient
            from py_clob_client_v2.clob_types import ApiCreds
        except ImportError:
            from py_clob_client.client import ClobClient  # type: ignore
        host = CLOB
        chain_id = 137
        key = self.settings.private_key
        funder = self.settings.funder_address or None
        sig_type = self.settings.signature_type
        client = ClobClient(
            host,
            key=key,
            chain_id=chain_id,
            signature_type=sig_type,
            funder=funder,
        )
        try:
            creds = None
            for name in (
                "create_or_derive_api_key",
                "create_or_derive_api_creds",
                "createOrDeriveApiKey",
                "derive_api_key",
                "create_api_key",
            ):
                fn = getattr(client, name, None)
                if callable(fn):
                    creds = fn()
                    logger.info("PM api creds via %s", name)
                    break
            if creds is None:
                raise AttributeError(
                    "ClobClient has no create_or_derive_api_key/creds — upgrade py-clob-client-v2"
                )
            if hasattr(client, "set_api_creds"):
                client.set_api_creds(creds)
            elif hasattr(client, "set_creds"):
                client.set_creds(creds)
            else:
                # v2 often takes creds in constructor — rebuild
                client = ClobClient(
                    host,
                    key=key,
                    chain_id=chain_id,
                    signature_type=sig_type,
                    funder=funder,
                    creds=creds,
                )
            logger.info("PM api creds ready")
        except Exception as e:
            logger.warning("PM api creds: %s", e)
        self._clob = client
        return client

    async def place_market_buy(self, token_id: str, shares: float, price: float) -> dict:
        if not self.settings.is_live:
            limit, size = polymarket_buy_order(shares, price)
            logger.info("PAPER PM BUY token=%s size=%.4f @ %.4f", token_id, size or shares, limit or price)
            return {
                "status": "FILLED",
                "orderID": f"paper-pm-{int(time.time()*1000)}",
                "filledShares": size or shares,
                "avgPrice": limit or price,
                "paper": True,
            }
        return await self._live_buy(token_id, shares, price)

    async def place_market_sell(self, token_id: str, shares: float, price: float) -> dict:
        if not self.settings.is_live:
            logger.info("PAPER PM SELL token=%s size=%.4f @ %.4f", token_id, shares, price)
            return {
                "status": "FILLED",
                "orderID": f"paper-pm-sell-{int(time.time()*1000)}",
                "filledShares": shares,
                "avgPrice": price,
                "paper": True,
            }
        return await self._live_order(token_id, shares, price, "SELL")

    async def _live_buy(self, token_id: str, shares: float, price: float) -> dict:
        return await self._live_order(token_id, shares, price, "BUY")

    async def _live_order(self, token_id: str, shares: float, price: float, side: str) -> dict:
        try:
            from py_clob_client_v2.clob_types import OrderArgs, OrderType
            from py_clob_client_v2.order_builder.constants import BUY, SELL
        except ImportError:
            from py_clob_client.clob_types import OrderArgs, OrderType  # type: ignore
            from py_clob_client.order_builder.constants import BUY, SELL  # type: ignore

        client = self._ensure_clob()
        if side == "BUY":
            # `price` is the observed ask (engine may pass ask or ask+slip — treat as ask)
            ask = float(price)
            limit, size = polymarket_buy_order(shares, ask)
            if size <= 0 or limit <= 0:
                return {"status": "REJECTED", "reason": "no Polymarket ask / zero size"}
            # size already quantized so size*limit is exact cents (maker 2dp)
            maker = round(size * limit, 2)
            logger.info(
                "PM BUY target %.4f sh ask %.4f → limit %.2f size %.4f maker=$%.2f%s",
                shares, ask, limit, size, maker,
                " [min-notional bump]" if (shares * ask < 1.05 and size > shares * 1.02) else "",
            )
            order_side = BUY
            px, sz = limit, size
        else:
            order_side = SELL
            px = _floor_dec(price, 2) if price else 0.01
            sz = _floor_dec(shares, 4)

        try:
            args = OrderArgs(token_id=token_id, price=px, size=sz, side=order_side)
            signed = client.create_order(args)
            # FOK only — never leave resting GTC that can fill later + duplicate size.
            # On FOK kill (no liquidity) return REJECTED; engine retries with a *new* signed order at worse price.
            order_type = getattr(OrderType, "FOK", None) or getattr(OrderType, "FAK", None) or OrderType.GTC
            try:
                resp = client.post_order(signed, order_type)
            except Exception as e1:
                msg = str(e1)
                logger.warning("PM post_order %s failed (%s)", order_type, msg)
                # Do not re-post the same signature (→ "Duplicated")
                return {"status": "REJECTED", "reason": msg, "avgPrice": px}
            logger.info("PM LIVE %s type=%s resp=%s", side, order_type, resp)
            if not resp or (isinstance(resp, dict) and (resp.get("error") or (resp.get("success") is False and resp.get("status") != "matched"))):
                reason = ""
                if isinstance(resp, dict):
                    reason = str(resp.get("error") or resp.get("errorMsg") or "rejected")
                return {"status": "REJECTED", "reason": reason or "rejected", "avgPrice": px}
            if isinstance(resp, dict):
                oid = str(resp.get("orderID") or resp.get("id") or "")
                status = str(resp.get("status") or "SUBMITTED").upper()
                taking = _f(resp.get("takingAmount") or resp.get("size_matched") or resp.get("filled"))
                making = _f(resp.get("makingAmount"))
                filled = taking or 0.0
                avg = px
                if taking and taking > 0 and making and making > 0:
                    avg = making / taking
                if status in ("MATCHED", "FILLED"):
                    status = "FILLED"
                    if filled <= 0:
                        filled = sz
                elif status == "LIVE":
                    status = "SUBMITTED"
                    filled = filled or 0.0
                return {
                    "status": status,
                    "orderID": oid,
                    "filledShares": filled,
                    "takingAmount": taking,
                    "makingAmount": making,
                    "avgPrice": avg,
                    "price": avg,
                    "raw": resp,
                }
            return {"status": "SUBMITTED", "orderID": str(resp), "filledShares": 0.0, "avgPrice": px}
        except Exception as e:
            logger.warning("PM LIVE %s rejected: %s", side, e)
            return {"status": "REJECTED", "reason": str(e)}

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a resting CLOB order so retry does not double size."""
        if not order_id:
            return False
        try:
            clob = self._ensure_clob()
            if clob is None:
                return False
            for name in ("cancel", "cancel_order", "cancelOrder"):
                fn = getattr(clob, name, None)
                if callable(fn):
                    try:
                        res = fn(order_id)
                    except TypeError:
                        res = fn(order_id=order_id)
                    logger.info("PM cancel %s → %s", order_id[:18], res)
                    return True
            logger.warning("PM cancel: no cancel method on ClobClient")
        except Exception as e:
            logger.warning("PM cancel %s failed: %s", order_id[:18], e)
        return False

    async def get_collateral_balance(self) -> float | None:
        """USDC collateral available on Polymarket CLOB (LIVE only)."""
        try:
            clob = self._ensure_clob()
        except Exception:
            return None
        if clob is None:
            return None
        try:
            # py-clob-client variants
            ba = None
            # v2: get_balance_allowance(BalanceAllowanceParams)
            try:
                from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
                params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            except Exception:
                try:
                    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                except Exception:
                    params = {"asset_type": "COLLATERAL"}
            if hasattr(clob, "get_balance_allowance"):
                try:
                    ba = clob.get_balance_allowance(params)
                except TypeError:
                    ba = clob.get_balance_allowance(params=params)
            elif hasattr(clob, "getBalanceAllowance"):
                ba = clob.getBalanceAllowance(params)
            else:
                return None
            if isinstance(ba, dict):
                bal = ba.get("balance") or ba.get("balanceAllowance") or ba.get("available")
                if bal is not None:
                    # often in micro USDC (6 decimals) as string
                    v = float(bal)
                    if v > 1e6:
                        v = v / 1e6
                    return round(v, 4)
            if isinstance(ba, (int, float, str)):
                v = float(ba)
                if v > 1e6:
                    v = v / 1e6
                return round(v, 4)
        except Exception:
            return None
        return None

    async def get_order(self, order_id: str) -> Optional[dict]:
        if not self.settings.is_live:
            return None
        try:
            client = self._ensure_clob()
            return client.get_order(order_id)
        except Exception as e:
            self.last_error = str(e)
            return None
