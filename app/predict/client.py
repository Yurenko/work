"""Predict.fun REST client — markets, orderbook, orders (JWT auth)."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

import httpx

from app.core.config import get_settings
from app.core.logging import logger


class RateLimiter:
    def __init__(self, min_interval_ms: int = 250) -> None:
        self.min_interval = max(0.0, min_interval_ms / 1000.0)
        self._last = 0.0

    async def wait(self) -> None:
        now = time.monotonic()
        gap = self.min_interval - (now - self._last)
        if gap > 0:
            await asyncio.sleep(gap)
        self._last = time.monotonic()


def _num(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _complement(price: float) -> float:
    return max(0.0, min(1.0, round(1.0 - float(price), 6)))


class PredictClient:
    """REST client for https://api.predict.fun

    Auth:
      - x-api-key on all mainnet requests
      - Authorization: Bearer <JWT> for order endpoints
    JWT is obtained via GET /v1/auth/message + signed POST /v1/auth.
    In PAPER mode orders are simulated; market data still uses the API.
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self.api_key, self.private_key = self.settings.predict_auth()
        self.base = (self.settings.predict_rest_base_url or "https://api.predict.fun").rstrip("/")
        self._http: Optional[httpx.AsyncClient] = None
        self._limiter = RateLimiter(self.settings.predict_min_request_interval_ms)
        self._jwt: Optional[str] = None
        self._jwt_exp = 0.0
        self.last_error: str = ""

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    def _headers(self, with_jwt: bool = False) -> dict[str, str]:
        h = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.api_key:
            h["x-api-key"] = self.api_key
        if with_jwt and self._jwt:
            h["Authorization"] = f"Bearer {self._jwt}"
        return h

    async def _http_client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=30.0)
        return self._http

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        json_body: Any = None,
        with_jwt: bool = False,
    ) -> Any:
        await self._limiter.wait()
        client = await self._http_client()
        url = f"{self.base}{path}"
        try:
            resp = await client.request(
                method,
                url,
                params=params,
                json=json_body,
                headers=self._headers(with_jwt=with_jwt),
            )
            if resp.status_code >= 400:
                text = resp.text[:500]
                self.last_error = f"{resp.status_code}: {text}"
                logger.warning("Predict %s %s → %s %s", method, path, resp.status_code, text)
                return None
            data = resp.json()
            if isinstance(data, dict) and data.get("success") is False:
                self.last_error = str(data.get("error") or data)
                return None
            return data.get("data", data) if isinstance(data, dict) else data
        except Exception as e:
            self.last_error = str(e)
            logger.warning("Predict request error %s %s: %s", method, path, e)
            return None

    # ── Auth (JWT) ──────────────────────────────────────────────

    async def ensure_jwt(self) -> Optional[str]:
        """Fetch / refresh JWT.

        EOA: sign auth message with eth_account (signer = EOA address).
        Predict Account: sign via SDK sign_predict_account_message
        (signer = PREDICT_ACCOUNT address). Plain EOA JWT cannot place
        orders for a smart-wallet account.
        """
        if self._jwt and time.time() < self._jwt_exp - 60:
            return self._jwt
        if not self.private_key:
            return None

        msg_data = await self._request("GET", "/v1/auth/message")
        if not msg_data:
            return None
        message = msg_data.get("message") if isinstance(msg_data, dict) else None
        if not message:
            message = str(msg_data)

        pk = self.private_key.strip()
        if not pk.startswith("0x"):
            pk = "0x" + pk
        predict_account = (getattr(self.settings, "predict_account", None) or "").strip()

        try:
            if predict_account:
                # Smart wallet path — required when funds are on Predict Account
                from predict_sdk import OrderBuilder, ChainId
                try:
                    from predict_sdk import OrderBuilderOptions
                    builder = OrderBuilder.make(
                        ChainId.BNB_MAINNET, pk, OrderBuilderOptions(predict_account=predict_account)
                    )
                except Exception:
                    try:
                        builder = OrderBuilder.make(
                            ChainId.BNB_MAINNET, pk, predict_account=predict_account
                        )
                    except TypeError:
                        chain = getattr(ChainId, "BNB_MAINNET", None) or getattr(ChainId, "BnbMainnet")
                        builder = OrderBuilder.make(chain, pk, predict_account=predict_account)

                sig = None
                for name in (
                    "sign_predict_account_message",
                    "signPredictAccountMessage",
                    "sign_predict_account_message_async",
                ):
                    fn = getattr(builder, name, None)
                    if callable(fn):
                        sig = fn(message)
                        # await if coroutine
                        if hasattr(sig, "__await__"):
                            sig = await sig
                        break
                if not sig:
                    raise RuntimeError(
                        "SDK missing sign_predict_account_message — upgrade predict-sdk"
                    )
                if isinstance(sig, bytes):
                    signature = "0x" + sig.hex()
                else:
                    signature = str(sig)
                    if not signature.startswith("0x"):
                        signature = "0x" + signature
                body = {
                    "signer": predict_account,
                    "signature": signature,
                    "message": message,
                }
                signer_log = predict_account
            else:
                from eth_account import Account
                from eth_account.messages import encode_defunct

                acct = Account.from_key(pk)
                signed = acct.sign_message(encode_defunct(text=message))
                sig_hex = signed.signature.hex()
                if not sig_hex.startswith("0x"):
                    sig_hex = "0x" + sig_hex
                body = {
                    "signer": acct.address,
                    "signature": sig_hex,
                    "message": message,
                }
                signer_log = acct.address

            token_data = await self._request("POST", "/v1/auth", json_body=body)
            if not token_data:
                return None
            token = token_data.get("token") if isinstance(token_data, dict) else None
            if token:
                self._jwt = token
                self._jwt_exp = time.time() + 3600
                logger.info("Predict JWT obtained for %s", signer_log)
            return self._jwt
        except Exception as e:
            logger.warning("Predict JWT error: %s", e)
            self.last_error = str(e)
            return None

    # ── Markets ─────────────────────────────────────────────────

    async def search_markets(self, query: str = "btc updown 5m", limit: int = 20) -> list[dict]:
        # API requires param name `query` (not `q`); limit max 25
        data = await self._request(
            "GET",
            "/v1/search",
            params={"query": query, "limit": str(min(int(limit), 25))},
        )
        if not data:
            return []
        if isinstance(data, dict):
            markets = list(data.get("markets") or [])
            for cat in data.get("categories") or []:
                if isinstance(cat, dict):
                    markets.extend(cat.get("markets") or [])
            return markets
        return list(data) if isinstance(data, list) else []

    async def list_markets(self, params: Optional[dict] = None) -> list[dict]:
        # Docs use `first` / `after` / `status` / `marketVariant` — not `limit`
        base = {"first": "50", "status": "OPEN"}
        if params:
            base.update(params)
        data = await self._request("GET", "/v1/markets", params=base)
        if not data:
            return []
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return list(data.get("markets") or data.get("data") or data.get("items") or [])
        return []

    async def get_category(self, slug: str) -> Optional[dict]:
        """GET /v1/categories/{slug} — includes nested markets."""
        return await self._request("GET", f"/v1/categories/{slug}")

    async def get_series_rounds(self, series_slug: str = "btc-updown-5m", first: int = 10) -> list[dict]:
        """GET /v1/series/{slug}/rounds — ACTIVE rounds, first = live window."""
        data = await self._request(
            "GET",
            f"/v1/series/{series_slug}/rounds",
            params={"status": "ACTIVE", "first": str(first)},
        )
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return list(data.get("data") or data.get("rounds") or data.get("categories") or [])
        return []


    async def get_market(self, market_id: int | str) -> Optional[dict]:
        return await self._request("GET", f"/v1/markets/{market_id}")


    async def market_up_won(self, market_id: str | int) -> bool | None:
        """True if Up/Yes won, False if Down/No won, None if not resolved yet."""
        data = await self._request("GET", f"/v1/markets/{market_id}")
        mkt = data.get("data") if isinstance(data, dict) and "data" in data else data
        if not isinstance(mkt, dict):
            return None
        status = str(mkt.get("tradingStatus") or mkt.get("status") or "").upper()
        outcomes = mkt.get("outcomes") or []
        if not isinstance(outcomes, list) or not outcomes:
            return None
        # Log raw outcome statuses for debugging false divergence
        try:
            snap = [
                {
                    "name": o.get("name"),
                    "status": o.get("status"),
                    "indexSet": o.get("indexSet"),
                }
                for o in outcomes if isinstance(o, dict)
            ]
            logger.info(
                "PF market %s status=%s outcomes=%s",
                market_id, status, snap,
            )
        except Exception:
            pass
        up_won = None
        for o in outcomes:
            if not isinstance(o, dict):
                continue
            name = str(o.get("name") or "").lower()
            ost = str(o.get("status") or "").upper()
            if ost not in ("WON", "LOST"):
                continue
            is_up = name in ("up", "yes") or o.get("indexSet") in (1, "1")
            is_down = name in ("down", "no") or o.get("indexSet") in (2, "2")
            if is_up:
                up_won = ost == "WON"
            elif is_down:
                up_won = ost != "WON" if up_won is None else up_won
        if up_won is None and status in ("RESOLVED", "CLOSED", "SETTLED"):
            res = mkt.get("resolution") if isinstance(mkt.get("resolution"), dict) else {}
            rname = str(res.get("name") or "").lower()
            if rname in ("up", "yes"):
                return True
            if rname in ("down", "no"):
                return False
        return up_won

    async def get_orderbook(self, market_id: int | str) -> Optional[dict]:
        """Orderbook prices are for the Yes outcome (0–1)."""
        return await self._request("GET", f"/v1/markets/{market_id}/orderbook")

    def parse_book_top(self, book: Optional[dict]) -> tuple[Optional[float], Optional[float]]:
        """Return (best_bid, best_ask) for Yes from REST orderbook."""
        if not book:
            return None, None
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        bid = self._top_price(bids, is_bid=True)
        ask = self._top_price(asks, is_bid=False)
        return bid, ask

    @staticmethod
    def _top_price(levels: Any, is_bid: bool) -> Optional[float]:
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

    async def find_btc_5m_markets(self, now: Optional[float] = None) -> list[dict]:
        """Find live + upcoming BTC Up/Down 5m markets via series rounds.

        Official path: GET /v1/series/btc-updown-5m/rounds?status=ACTIVE
        First item = current live round. Category has startsAt/endsAt.
        Nested markets[0] is the tradable binary market (Yes=Up).
        """
        now = now or time.time()
        out: list[dict] = []
        seen: set[str] = set()

        rounds = await self.get_series_rounds("btc-updown-5m", first=12)
        if not rounds:
            rounds = []
            cat = await self.get_category("btc-updown-5m")
            if isinstance(cat, dict):
                rounds = [cat]
            if not rounds:
                searched = await self.search_markets("btc updown 5m", limit=25)
                for mkt in searched:
                    if isinstance(mkt, dict):
                        rounds.append({
                            "markets": [mkt],
                            "slug": mkt.get("slug") or mkt.get("categorySlug"),
                            "endsAt": mkt.get("endsAt"),
                            "startsAt": mkt.get("startsAt"),
                            "title": mkt.get("title"),
                        })

        def _parse_end(*vals) -> Optional[float]:
            import re as _re
            from datetime import datetime
            for end_raw in vals:
                if end_raw is None:
                    continue
                n = _num(end_raw)
                if n is not None:
                    if n > 1e12:
                        n /= 1000.0
                    if 1e9 < n < 2e10:
                        return float(n)
                if isinstance(end_raw, str):
                    s = end_raw.strip()
                    if "T" in s or s.endswith("Z"):
                        try:
                            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
                        except Exception:
                            pass
                    if s.isdigit():
                        n = float(s)
                        if n > 1e12:
                            n /= 1000.0
                        return n
                    mm = _re.search(r"(?:btc-updown-5m-|updown-5m-)(\d{9,12})", s)
                    if mm:
                        return float(mm.group(1))
            return None

        for rnd in rounds:
            if not isinstance(rnd, dict):
                continue
            markets = rnd.get("markets") or []
            if not markets and rnd.get("id") and (rnd.get("question") or rnd.get("title")):
                markets = [rnd]
            if not markets:
                continue
            mkt = markets[0] if isinstance(markets[0], dict) else None
            if not mkt:
                continue

            mid = mkt.get("id") or mkt.get("marketId")
            if mid is None:
                continue
            mid_s = str(mid)
            if mid_s in seen:
                continue

            slug = str(
                rnd.get("slug")
                or mkt.get("categorySlug")
                or mkt.get("slug")
                or mkt.get("seriesSlug")
                or ""
            )
            # Prefer explicit API times. Slug timestamp is window START (not end).
            start_ts = _parse_end(rnd.get("startsAt"), mkt.get("startsAt"))
            end_ts = _parse_end(
                rnd.get("endsAt"),
                rnd.get("endTime"),
                mkt.get("endsAt"),
                mkt.get("endTime"),
            )
            slug_ts = _parse_end(slug, mkt.get("categorySlug"), rnd.get("slug"))
            if start_ts is None and slug_ts is not None:
                start_ts = slug_ts
            if end_ts is None and start_ts is not None:
                end_ts = start_ts + 300.0  # BTC up/down 5m window
            if end_ts is None and slug_ts is not None:
                # last resort: slug as start
                start_ts = start_ts or slug_ts
                end_ts = slug_ts + 300.0

            if end_ts is not None and end_ts <= now:
                continue
            if end_ts is None:
                logger.warning("Predict market %s missing endsAt (slug=%s) — skip", mid_s, slug)
                continue

            title = mkt.get("title") or mkt.get("question") or rnd.get("title") or f"BTC 5m {mid_s}"
            status = str(mkt.get("tradingStatus") or mkt.get("status") or rnd.get("status") or "OPEN").upper()

            up_bid = up_ask = None
            for o in mkt.get("outcomes") or []:
                if not isinstance(o, dict):
                    continue
                name = str(o.get("name") or "").lower()
                if name in ("up", "yes") or o.get("indexSet") == 1:
                    bb = o.get("bestBid") or {}
                    ba = o.get("bestAsk") or {}
                    if isinstance(bb, dict):
                        up_bid = _num(bb.get("price"))
                    if isinstance(ba, dict):
                        up_ask = _num(ba.get("price"))
                    break

            seen.add(mid_s)
            out.append(
                {
                    "id": mid_s,
                    "market_id": mid_s,
                    "title": title,
                    "end": end_ts,
                    "start": start_ts,
                    "status": status,
                    "slug": slug,
                    "up_market_id": mid_s,
                    "down_market_id": mid_s,
                    "up_bid": up_bid,
                    "up_ask": up_ask,
                    "raw": mkt,
                    "round": rnd,
                }
            )

        out.sort(key=lambda x: x.get("end") or 0)
        extra = ""
        if out:
            extra = " live_end_in=%ds" % int(out[0]["end"] - now)
        logger.info("Predict: matched %d BTC 5m market(s)%s", len(out), extra)
        return out


    async def get_collateral_balance(self) -> float | None:
        """USDT balance of trading wallet (EOA or PREDICT_ACCOUNT) on BNB Chain."""
        try:
            from eth_account import Account
            from web3 import Web3
        except ImportError:
            return None
        pk = (self.settings.predict_private_key or "").strip()
        if not pk:
            return None
        if not pk.startswith("0x"):
            pk = "0x" + pk
        try:
            eoa = Account.from_key(pk).address
        except Exception:
            return None
        account = (getattr(self.settings, "predict_account", None) or "").strip()
        addr = account or eoa
        usdt = "0x55d398326f99059fF775485246999027B3197955"
        rpc = "https://bsc-dataseed.binance.org"
        try:
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 8}))
            abi = [{
                "name": "balanceOf", "type": "function", "stateMutability": "view",
                "inputs": [{"name": "a", "type": "address"}],
                "outputs": [{"type": "uint256"}],
            }, {
                "name": "decimals", "type": "function", "stateMutability": "view",
                "inputs": [], "outputs": [{"type": "uint8"}],
            }]
            contract = w3.eth.contract(address=Web3.to_checksum_address(usdt), abi=abi)
            raw = contract.functions.balanceOf(Web3.to_checksum_address(addr)).call()
            try:
                dec = int(contract.functions.decimals().call())
            except Exception:
                dec = 18
            return round(raw / (10 ** dec), 4)
        except Exception as e:
            logger.debug("PF balance fetch failed: %s", e)
            return None

    async def get_position_for_market(
        self, market_id: str | int, is_yes: bool | None = None
    ) -> dict | None:
        """GET /v1/positions?marketId= — amount (shares) + averageBuyPriceUsd (real cost).

        is_yes: if set, only return the YES or NO outcome position.
        """
        data = await self._request(
            "GET",
            "/v1/positions",
            params={"marketId": str(market_id), "first": "20"},
            with_jwt=True,
        )
        rows = data if isinstance(data, list) else (
            (data or {}).get("data") if isinstance(data, dict) else None
        )
        if not isinstance(rows, list) or not rows:
            return None
        best = None
        best_amt = -1.0
        for row in rows:
            if not isinstance(row, dict):
                continue
            if is_yes is not None:
                # Filter by outcome / isYes / outcomeName
                row_yes = row.get("isYes")
                if row_yes is None:
                    name = str(row.get("outcome") or row.get("outcomeName") or row.get("name") or "").lower()
                    if name in ("yes", "up"):
                        row_yes = True
                    elif name in ("no", "down"):
                        row_yes = False
                if row_yes is not None and bool(row_yes) != bool(is_yes):
                    continue
            raw = row.get("amount") or 0
            try:
                amt = float(raw)
            except (TypeError, ValueError):
                continue
            if amt > 1e12:  # wei
                amt = amt / 1e18
            if amt > best_amt:
                best_amt = amt
                best = row
        if not best:
            return None
        avg = best.get("averageBuyPriceUsd") or best.get("averageBuyPrice") or best.get("avgPrice") or best.get("avgCost")
        try:
            avg_f = float(avg) if avg is not None else 0.0
        except (TypeError, ValueError):
            avg_f = 0.0
        # Cents style (43 = 0.43) or wei
        if 1.5 < avg_f <= 100:
            avg_f = avg_f / 100.0
        elif avg_f > 100:
            avg_f = avg_f / 1e18
        cost_usd = 0.0
        for k in ("costBasisUsd", "costBasis", "costUsd", "totalCostUsd", "totalCost", "cost", "invested"):
            try:
                v = float(best.get(k) or 0)
            except (TypeError, ValueError):
                continue
            if v <= 0:
                continue
            # Normalize possible cents / micro
            if best_amt > 0 and v / best_amt > 1.5:
                for div in (100.0, 1e6, 1e18):
                    if 0.01 <= (v / div) / best_amt <= 1.0:
                        v = v / div
                        break
            cost_usd = v
            if best_amt > 0:
                cand = cost_usd / best_amt
                if 0.01 <= cand <= 1.0:
                    avg_f = cand
            break
        if (avg_f <= 0 or avg_f >= 1.0) and best_amt > 0 and cost_usd > 0:
            cand = cost_usd / best_amt
            if 0.01 <= cand <= 1.0:
                avg_f = cand
        # Derive cost from avg if API only gave averageBuyPriceUsd
        if cost_usd <= 0 and best_amt > 0 and 0.01 <= avg_f <= 1.0:
            cost_usd = avg_f * best_amt
        # valueUsd is mark-to-market, NOT cost — only use if no avg
        if (avg_f <= 0 or avg_f >= 1.0) and best_amt > 0:
            try:
                v = float(best.get("valueUsd") or 0)
                if 0 < v < best_amt:  # value at entry-ish unlikely; skip MTM
                    pass
            except (TypeError, ValueError):
                pass
        return {
            "shares": round(best_amt, 4),
            "avg_price": round(avg_f, 4) if 0 < avg_f < 1.5 else 0.0,
            "cost_usd": round(cost_usd, 4) if cost_usd > 0 else 0.0,
            "value_usd": best.get("valueUsd"),
            "raw": best,
        }

    async def get_fill_price_from_activity(
        self, market_id: str | int, order_id: str = "", limit_price: float = 0.0
    ) -> dict | None:
        """GET /v1/account/activity — real priceExecuted from MATCH_SUCCESS."""
        data = await self._request(
            "GET",
            "/v1/account/activity",
            params={"first": "30"},
            with_jwt=True,
        )
        rows = data if isinstance(data, list) else (
            (data or {}).get("data") if isinstance(data, dict) else None
        )
        if not isinstance(rows, list):
            return None
        mid = str(market_id)
        # Collect fills for VWAP (do NOT take lowest tick — that understates cost)
        fills = []  # (px, sh, usd)
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or row.get("type") or row.get("outcome") or row.get("title") or "").upper()
            # UI labels: "Filled Buy", "Placed Buy", MATCH_SUCCESS, etc.
            is_fillish = any(k in name for k in ("MATCH", "FILL", "BUY", "FILLED", "EXECUTE"))
            if not is_fillish:
                if row.get("priceExecuted") is None and row.get("price") is None and row.get("value") is None:
                    continue
            # Skip pure "Placed" (limit resting) when a Filled row exists later
            if "PLACED" in name and "FILL" not in name:
                # still collect; VWAP prefers rows with value
                pass
            m = row.get("market") if isinstance(row.get("market"), dict) else {}
            rid = str(m.get("id") or row.get("marketId") or "")
            if rid and rid != mid:
                continue
            # Prefer BUY / MATCH fills for entry cost (skip sells)
            side = str(row.get("side") or row.get("orderSide") or row.get("type") or "").upper()
            title = str(row.get("title") or row.get("action") or "").upper()
            if any(x in side or x in title for x in ("SELL", "ASK", "REDEEM", "CLAIM")):
                continue
            raw_px = row.get("priceExecuted") or row.get("price")
            if isinstance(row.get("order"), dict) and raw_px is None:
                raw_px = row["order"].get("price")
            try:
                px = float(raw_px) if raw_px is not None else 0.0
            except (TypeError, ValueError):
                px = 0.0
            # Activity UI shows 22¢ — API may return 0.22 OR 22 (cents)
            if 1.5 < px <= 100:
                px = px / 100.0
            elif px > 100:
                px = px / 1e18
            raw_sh = row.get("amountFilled") or row.get("amount") or row.get("shares") or 0
            try:
                sh = float(raw_sh)
            except (TypeError, ValueError):
                sh = 0.0
            if sh > 1e12:
                sh = sh / 1e18
            raw_usd = (
                row.get("value")
                or row.get("valueUsd")
                or row.get("usd")
                or row.get("cost")
                or row.get("quoteAmount")
                or row.get("notional")
            )
            try:
                usd = float(raw_usd) if raw_usd is not None else 0.0
            except (TypeError, ValueError):
                usd = 0.0
            # Value on UI is dollars; sometimes micro-units
            if usd > 1000 and sh > 0:
                # likely wei or micro
                for div in (1e18, 1e6, 100.0):
                    if 0.01 <= (usd / div) / max(sh, 1e-9) <= 1.0:
                        usd = usd / div
                        break
            if usd > 0 and sh > 0:
                cand = usd / sh
                if 0.01 <= cand <= 1.0:
                    px = cand  # cost/shares overrides tick
            if not (0 < px < 1.5):
                continue
            if limit_price > 0 and px > limit_price + 0.05:
                continue  # unrelated fill
            if sh <= 0 and usd > 0 and px > 0:
                sh = usd / px
            fills.append((px, max(sh, 0.0), usd))
        if not fills:
            return None
        # VWAP by shares; if USD known, prefer cost/shares
        tot_sh = sum(s for _, s, _ in fills if s > 0)
        tot_usd = sum(u for _, _, u in fills if u and u > 0)
        if tot_usd > 0 and tot_sh > 0:
            vwap = tot_usd / tot_sh
        elif tot_sh > 0:
            vwap = sum(p * s for p, s, _ in fills if s > 0) / tot_sh
        else:
            vwap = fills[0][0]
            tot_sh = fills[0][1]
        return {
            "avg_price": round(float(vwap), 4),
            "shares": round(float(tot_sh), 4) if tot_sh > 0 else 0.0,
            "cost_usd": round(float(tot_usd), 4) if tot_usd > 0 else 0.0,
        }

    async def place_order(
        self,
        market_id: str | int,
        side: str,
        shares: float,
        price: float,
        *,
        is_yes: bool = True,
        order_type: str = "LIMIT",
    ) -> dict:
        """Place LIMIT order. side = BUY | SELL.

        In PAPER mode returns a simulated FILLED response.
        Live mode requires JWT + signed order body (via SDK when available).
        """
        price = round(float(price), 2)  # Predict max 2 decimal places
        side = side.upper()
        shares = float(shares)
        price = round(float(price), 4)

        if not self.settings.is_live:
            usd = round(shares * price, 4)
            logger.info(
                "PAPER Predict %s %s market=%s shares=%.4f @ %.4f ($%.2f)",
                side,
                "YES" if is_yes else "NO",
                market_id,
                shares,
                price,
                usd,
            )
            return {
                "status": "FILLED",
                "orderId": f"paper-pf-{int(time.time()*1000)}",
                "filledShares": shares,
                "avgPrice": price,
                "usd": usd,
                "paper": True,
            }

        await self.ensure_jwt()
        if not self._jwt:
            return {"status": "REJECTED", "reason": "no JWT — set PREDICT_PRIVATE_KEY"}

        try:
            return await self._place_via_sdk(market_id, side, shares, price, is_yes, order_type)
        except Exception as e:
            logger.exception("Predict LIVE place_order failed: %s", e)
            return {"status": "REJECTED", "reason": str(e)}

    async def _place_via_sdk(
        self,
        market_id: str | int,
        side: str,
        shares: float,
        price: float,
        is_yes: bool,
        order_type: str,
    ) -> dict:
        """Build + sign order with official predict-sdk, POST /v1/orders.

        Body must be:
          { "data": { "order": { ...signed, hash }, "pricePerShare": "<wei>", "strategy": "LIMIT" } }
        """
        try:
            from predict_sdk import (
                OrderBuilder,
                ChainId,
                Side as SdkSide,
                BuildOrderInput,
                LimitHelperInput,
            )
        except ImportError as e:
            raise RuntimeError(
                "predict-sdk not installed. Run: pip install predict-sdk"
            ) from e

        pk = (self.settings.predict_private_key or "").strip()
        if not pk:
            raise RuntimeError("PREDICT_PRIVATE_KEY required for LIVE orders")

        # Fetch market for tokenId, feeRateBps, flags
        mkt = await self.get_market(market_id)
        if not mkt:
            raise RuntimeError(f"market {market_id} not found")
        if isinstance(mkt, dict) and "data" in mkt and isinstance(mkt["data"], dict):
            mkt = mkt["data"]

        outcomes = mkt.get("outcomes") or []
        token_id = None
        for o in outcomes:
            if not isinstance(o, dict):
                continue
            name = str(o.get("name") or "").lower()
            idx_set = o.get("indexSet")
            if is_yes and (name in ("yes", "up") or idx_set == 1):
                token_id = o.get("onChainId") or o.get("positionId")
                break
            if not is_yes and (name in ("no", "down") or idx_set == 2):
                token_id = o.get("onChainId") or o.get("positionId")
                break
        if not token_id and outcomes:
            # fallback by index
            o = outcomes[0 if is_yes else min(1, len(outcomes) - 1)]
            token_id = o.get("onChainId") or o.get("positionId")
        if not token_id:
            raise RuntimeError(f"no onChainId for market {market_id} is_yes={is_yes}")

        fee_bps = int(mkt.get("feeRateBps") or 200)
        is_neg = bool(mkt.get("isNegRisk"))
        is_yield = bool(mkt.get("isYieldBearing"))
        precision = int(mkt.get("decimalPrecision") or 2)

        # Round price to market precision
        price = round(float(price), precision)
        if not (0 < price < 1):
            raise RuntimeError(f"price out of range: {price}")

        # Min buy value 1 USDT
        if side == "BUY" and shares * price < 1.0:
            # bump shares to meet min notional
            shares = max(shares, (1.0 / price) * 1.01)
            shares = round(shares, 4)

        price_wei = int(round(price * 1e18))
        qty_wei = int(round(shares * 1e18))

        # If user funds via predict.fun UI, money sits on Predict Account (smart wallet).
        # Privy-exported key only signs; maker/signer must be the account address.
        predict_account = (getattr(self.settings, "predict_account", None) or "").strip()
        try:
            from predict_sdk import OrderBuilderOptions
        except ImportError:
            OrderBuilderOptions = None  # type: ignore

        if predict_account and OrderBuilderOptions is not None:
            builder = OrderBuilder.make(
                ChainId.BNB_MAINNET,
                pk,
                OrderBuilderOptions(predict_account=predict_account),
            )
            logger.info("Predict order via smart account %s", predict_account)
        elif predict_account:
            # older sdk kwargs style
            try:
                builder = OrderBuilder.make(
                    ChainId.BNB_MAINNET, pk, predict_account=predict_account
                )
            except TypeError:
                builder = OrderBuilder.make(ChainId.BNB_MAINNET, pk)
                logger.warning(
                    "SDK does not accept predict_account — upgrade predict-sdk. "
                    "Orders may fail if funds are on smart wallet."
                )
            else:
                logger.info("Predict order via smart account %s", predict_account)
        else:
            builder = OrderBuilder.make(ChainId.BNB_MAINNET, pk)
            logger.warning(
                "PREDICT_ACCOUNT not set — using EOA. "
                "If Portfolio is funded on predict.fun UI, set PREDICT_ACCOUNT to deposit address."
            )

        sdk_side = SdkSide.BUY if side == "BUY" else SdkSide.SELL

        amounts = builder.get_limit_order_amounts(
            LimitHelperInput(
                side=sdk_side,
                price_per_share_wei=price_wei,
                quantity_wei=qty_wei,
            )
        )
        price_per_share = amounts.price_per_share if hasattr(amounts, "price_per_share") else price_wei
        maker_amount = str(amounts.maker_amount)
        taker_amount = str(amounts.taker_amount)

        order = builder.build_order(
            "LIMIT",
            BuildOrderInput(
                side=sdk_side,
                token_id=str(token_id),
                maker_amount=maker_amount,
                taker_amount=taker_amount,
                fee_rate_bps=fee_bps,
            ),
        )
        typed = builder.build_typed_data(
            order,
            is_neg_risk=is_neg,
            is_yield_bearing=is_yield,
        )
        signed = builder.sign_typed_data_order(typed)
        order_hash = builder.build_typed_data_hash(typed)

        # signed may be dataclass — normalize to dict
        if hasattr(signed, "__dict__"):
            signed_dict = {
                k: getattr(signed, k)
                for k in (
                    "salt", "maker", "signer", "taker", "token_id", "tokenId",
                    "maker_amount", "makerAmount", "taker_amount", "takerAmount",
                    "expiration", "nonce", "fee_rate_bps", "feeRateBps",
                    "side", "signature_type", "signatureType", "signature", "hash",
                )
                if hasattr(signed, k)
            }
        elif isinstance(signed, dict):
            signed_dict = dict(signed)
        else:
            signed_dict = dict(signed)

        # Normalize field names to API camelCase
        def g(*keys, default=None):
            for k in keys:
                if k in signed_dict and signed_dict[k] is not None:
                    return signed_dict[k]
            return default

        side_val = g("side")
        if hasattr(side_val, "value"):
            side_val = side_val.value
        sig_type = g("signatureType", "signature_type", default=0)
        if hasattr(sig_type, "value"):
            sig_type = sig_type.value

        order_payload = {
            "hash": str(g("hash") or order_hash),
            "salt": str(g("salt")),
            "maker": str(g("maker")),
            "signer": str(g("signer")),
            "taker": str(g("taker") or "0x0000000000000000000000000000000000000000"),
            "tokenId": str(g("tokenId", "token_id", default=token_id)),
            "makerAmount": str(g("makerAmount", "maker_amount", default=maker_amount)),
            "takerAmount": str(g("takerAmount", "taker_amount", default=taker_amount)),
            "expiration": str(g("expiration") or "4102444800"),
            "nonce": str(g("nonce") or "0"),
            "feeRateBps": str(g("feeRateBps", "fee_rate_bps", default=fee_bps)),
            "side": int(side_val if side_val is not None else (0 if side == "BUY" else 1)),
            "signatureType": int(sig_type if sig_type is not None else 0),
            "signature": str(g("signature") or ""),
        }

        body = {
            "data": {
                "order": order_payload,
                "pricePerShare": str(price_per_share),
                "strategy": "LIMIT",
            }
        }
        logger.info(
            "Predict LIVE %s %s market=%s shares=%.4f @ %.4f token=%s…",
            side,
            "YES" if is_yes else "NO",
            market_id,
            shares,
            price,
            str(token_id)[:12],
        )
        data = await self._request("POST", "/v1/orders", json_body=body, with_jwt=True)
        if not data:
            return {"status": "REJECTED", "reason": self.last_error or "empty response"}
        return self._normalize_order_resp(data, shares, price)

    def _normalize_order_resp(self, data: Any, shares: float, price: float) -> dict:
        if not isinstance(data, dict):
            return {"status": "SUBMITTED", "raw": data, "filledShares": 0.0, "avgPrice": price}
        # create-order returns {code, orderId, orderHash} under data (already unwrapped)
        status = str(data.get("status") or data.get("state") or "").upper()
        code = str(data.get("code") or "").upper()
        filled = _num(data.get("filledShares") or data.get("filled") or data.get("quantityFilled") or data.get("amountFilled")) or 0.0
        avg = _num(data.get("avgPrice") or data.get("price")) or price
        oid = str(data.get("id") or data.get("orderId") or data.get("order_id") or "")
        ohash = str(data.get("orderHash") or data.get("hash") or data.get("order_hash") or "")
        if status in ("FILLED", "MATCHED", "COMPLETE", "COMPLETED") or filled >= shares * 0.99:
            status = "FILLED"
            if filled <= 0:
                filled = shares
        elif status in ("REJECTED", "FAILED", "CANCELLED", "CANCELED"):
            status = "REJECTED"
        elif (oid or ohash) and (code in ("OK", "SUCCESS", "") or status in ("", "OPEN", "LIVE", "ACCEPTED", "SUBMITTED")):
            status = "SUBMITTED"
        elif not status:
            status = "SUBMITTED" if (oid or ohash) else "REJECTED"
        return {
            "status": status,
            "orderId": oid,
            "orderHash": ohash,
            "filledShares": filled,
            "avgPrice": avg,
            "usd": round(filled * avg, 4),
            "raw": data,
        }


    async def wait_order_fill(
        self,
        order_id: str,
        shares: float,
        price: float,
        timeout: float = 12.0,
        order_hash: str = "",
        is_buy: bool = True,
        fee_bps: int = 200,
    ) -> dict:
        """Poll order status until FILLED/CANCELLED or timeout.

        Docs: GET /v1/orders/{hash} — path is the order **hash**, not numeric id.
        amountFilled is in wei (1e18). Brief 404 right after create is normal.
        """
        import asyncio as _asyncio

        last = {
            "status": "SUBMITTED",
            "orderId": order_id or "",
            "orderHash": order_hash or "",
            "filledShares": 0.0,
            "avgPrice": price,
        }
        if not order_hash and not order_id:
            return {**last, "status": "REJECTED", "reason": "no order id/hash"}

        deadline = time.time() + timeout

        def _parse_order(data: dict) -> dict:
            st = str(data.get("status") or "").upper()
            raw_filled = data.get("amountFilled") or data.get("filledShares") or data.get("amount") or 0
            try:
                amt = float(raw_filled)
            except (TypeError, ValueError):
                amt = 0.0
            # wei → shares
            if amt > shares * 10:
                amt = amt / 1e18
            oid = str(data.get("id") or order_id or "")
            oh = ""
            order_obj = data.get("order") if isinstance(data.get("order"), dict) else {}
            if order_obj:
                oh = str(order_obj.get("hash") or "")
            oh = oh or str(data.get("orderHash") or order_hash or "")

            # Real fill price (not the limit we submitted)
            avg = None
            for k in ("avgPrice", "averagePrice", "fillPrice", "executedPrice", "price"):
                v = data.get(k)
                if v is None and order_obj:
                    v = order_obj.get(k)
                try:
                    if v is not None and float(v) > 0:
                        avg = float(v)
                        # wei price? (e.g. 490000000000000000 = 0.49)
                        if avg > 1.5:
                            avg = avg / 1e18
                        if 0 < avg < 1.5:
                            break
                        avg = None
                except (TypeError, ValueError):
                    avg = None
            # Derive from maker/taker amounts when available
            if avg is None:
                try:
                    maker = float(data.get("makerAmount") or order_obj.get("makerAmount") or 0)
                    taker = float(data.get("takerAmount") or order_obj.get("takerAmount") or 0)
                    if maker > 1e12:  # wei
                        maker /= 1e18
                        taker /= 1e18
                    # BUY: maker=USD, taker=shares → price = maker/taker
                    if is_buy and taker > 0 and maker > 0:
                        avg = maker / taker
                    elif (not is_buy) and maker > 0 and taker > 0:
                        avg = taker / maker  # SELL: maker=shares, taker=USD
                    if avg is not None and not (0 < avg < 1.5):
                        avg = None
                except (TypeError, ValueError, ZeroDivisionError):
                    avg = None
            if avg is None:
                avg = float(price) if price else 0.5

            out = {
                "status": st or "SUBMITTED",
                "orderId": oid,
                "orderHash": oh,
                "filledShares": amt,
                "avgPrice": round(float(avg), 4),
                "raw": data,
            }
            if st in ("FILLED", "MATCHED", "COMPLETE", "COMPLETED"):
                out["status"] = "FILLED"
                if out["filledShares"] <= 0:
                    out["filledShares"] = shares
                # Taker BUY: fee is taken in shares → position < amountFilled
                if is_buy and fee_bps > 0 and out["filledShares"] > 0:
                    net = out["filledShares"] * (1.0 - float(fee_bps) / 10000.0)
                    out["filledShares"] = round(net, 4)
                    out["fee_bps"] = fee_bps
            elif st in ("CANCELLED", "CANCELED", "REJECTED", "FAILED", "EXPIRED", "INVALIDATED"):
                out["status"] = "REJECTED"
                out["reason"] = st
            elif out["filledShares"] >= shares * 0.95:
                out["status"] = "FILLED"
                if is_buy and fee_bps > 0:
                    out["filledShares"] = round(out["filledShares"] * (1.0 - float(fee_bps) / 10000.0), 4)
                    out["fee_bps"] = fee_bps
            return out

        while time.time() < deadline:
            data = None
            # Primary: by hash
            if order_hash:
                data = await self._request("GET", f"/v1/orders/{order_hash}", with_jwt=True)
            # Fallback: list FILLED then OPEN and match id
            if data is None:
                for st_filter in ("FILLED", "OPEN"):
                    page = await self._request(
                        "GET",
                        "/v1/orders",
                        params={"status": st_filter, "first": "50"},
                        with_jwt=True,
                    )
                    rows = page if isinstance(page, list) else (
                        (page or {}).get("data") if isinstance(page, dict) else None
                    )
                    if not isinstance(rows, list):
                        # _request already unwraps data sometimes — page may be list of orders
                        if isinstance(page, dict) and isinstance(page.get("data"), list):
                            rows = page["data"]
                        else:
                            rows = []
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        rid = str(row.get("id") or "")
                        rhash = ""
                        if isinstance(row.get("order"), dict):
                            rhash = str(row["order"].get("hash") or "")
                        rhash = rhash or str(row.get("orderHash") or "")
                        if (order_id and rid == str(order_id)) or (
                            order_hash and rhash.lower() == order_hash.lower()
                        ):
                            data = row
                            break
                    if data is not None:
                        break

            if isinstance(data, dict):
                last = _parse_order(data)
                if last["status"] in ("FILLED", "REJECTED"):
                    return last
            await _asyncio.sleep(0.7)
        return last

    async def cancel_order(self, order_id: str, order_hash: str = "") -> bool:
        """Remove order from book: POST /v1/orders/remove (not DELETE)."""
        body = None
        if order_id:
            body = {"data": {"ids": [str(order_id)]}}
            path = "/v1/orders/remove"
        elif order_hash:
            body = {"data": {"hashes": [str(order_hash)]}}
            path = "/v1/orders/remove-by-hash"
        else:
            return False
        data = await self._request("POST", path, json_body=body, with_jwt=True)
        logger.info("PF cancel id=%s hash=%s → %s", order_id, (order_hash or "")[:16], data)
        return data is not None


    async def get_order(self, order_id: str) -> Optional[dict]:
        if not self.settings.is_live:
            return None
        await self.ensure_jwt()
        return await self._request("GET", f"/v1/orders/{order_id}", with_jwt=True)

    async def paper_buy(self, market_id: str, shares: float, price: float, is_yes: bool = True) -> dict:
        return await self.place_order(market_id, "BUY", shares, price, is_yes=is_yes)

    async def paper_sell(self, market_id: str, shares: float, price: float, is_yes: bool = True) -> dict:
        return await self.place_order(market_id, "SELL", shares, price, is_yes=is_yes)
