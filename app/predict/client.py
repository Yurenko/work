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
        """Fetch / refresh JWT using private key signature.

        Uses eth_account when available. If private_key is empty or signing
        fails, returns None (public endpoints still work).
        """
        if self._jwt and time.time() < self._jwt_exp - 60:
            return self._jwt
        if not self.private_key:
            return None
        try:
            from eth_account import Account
            from eth_account.messages import encode_defunct
        except ImportError:
            logger.warning("eth_account not installed — cannot obtain JWT")
            return None

        msg_data = await self._request("GET", "/v1/auth/message")
        if not msg_data:
            return None
        message = msg_data.get("message") if isinstance(msg_data, dict) else None
        if not message:
            message = str(msg_data)
        try:
            acct = Account.from_key(self.private_key)
            signed = acct.sign_message(encode_defunct(text=message))
            body = {
                "signer": acct.address,
                "signature": signed.signature.hex(),
                "message": message,
            }
            token_data = await self._request("POST", "/v1/auth", json_body=body)
            if not token_data:
                return None
            token = token_data.get("token") if isinstance(token_data, dict) else None
            if token:
                self._jwt = token
                # JWT typically lasts hours; refresh conservatively
                self._jwt_exp = time.time() + 3600
                logger.info("Predict JWT obtained for %s", acct.address)
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
        status = str(data.get("status") or data.get("state") or "SUBMITTED").upper()
        filled = _num(data.get("filledShares") or data.get("filled") or data.get("quantityFilled")) or 0.0
        avg = _num(data.get("avgPrice") or data.get("price")) or price
        oid = str(data.get("id") or data.get("orderId") or data.get("order_id") or "")
        if status in ("FILLED", "MATCHED", "COMPLETE", "COMPLETED") or filled >= shares * 0.99:
            status = "FILLED"
            if filled <= 0:
                filled = shares
        elif status in ("REJECTED", "FAILED", "CANCELLED", "CANCELED"):
            status = "REJECTED"
        return {
            "status": status,
            "orderId": oid,
            "filledShares": filled,
            "avgPrice": avg,
            "usd": round(filled * avg, 4),
            "raw": data,
        }

    async def cancel_order(self, order_id: str) -> bool:
        if not self.settings.is_live:
            return True
        await self.ensure_jwt()
        data = await self._request("DELETE", f"/v1/orders/{order_id}", with_jwt=True)
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
