"""Hedge strategy: opposite legs on Predict.fun + Polymarket BTC 5m.

Rules:
1. Watch Up/Down asks on both venues.
2. If either opposite pair sums to < ENTRY_SUM_MAX, buy that pair.
   Never the same direction on both exchanges.
3. Exit when the two bids sum to >= EXIT_SUM_MIN.
4. Same share count on both venues (min ~5 on Polymarket).
5. Buy Predict first; Polymarket after Predict accepts, for the filled size.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from app.core.config import get_settings
from app.core.logging import logger, get_recent_logs
from app.polymarket.client import PolymarketClient
from app.polymarket.markets import updown_slug
from app.predict.client import PredictClient
from app.predict.ws import PredictOrderbookWS
from app.strategy.fills import Fill, fill_from_pm_order, fill_from_predict
from app.strategy.recovery import RecoveryBook, RecoveryConfig


@dataclass
class Leg:
    venue: str  # predict | polymarket
    direction: str  # up | down
    token_or_market: str
    shares: float = 0.0
    entry_price: float = 0.0
    status: str = "PENDING"  # PENDING|SUBMITTED|FILLED|REJECTED|CLOSED
    exit_price: float = 0.0
    exit_usd: float = 0.0
    order_id: str = ""
    requested_shares: float = 0.0
    filled_shares: float = 0.0
    entry_usd: float = 0.0
    reject_reason: str = ""


@dataclass
class Hedge:
    id: str
    opened_at: float
    market_end: float
    pf_leg: Leg  # Predict.fun (was bn_leg)
    pm_leg: Leg
    entry_sum: float = 0.0
    status: str = "OPENING"  # OPENING|PARTIAL|OPEN|CLOSING|SETTLING|CLOSED
    closed_at: float = 0.0
    exit_sum: float = 0.0
    pnl: float = 0.0
    close_reason: str = ""  # EXIT | SETTLE
    retries: int = 0
    last_retry_at: float = 0.0
    # aliases for recovery/tests that still say bn
    @property
    def bn_leg(self) -> Leg:
        return self.pf_leg


class HedgeEngine:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.pf = PredictClient()
        self.pm = PolymarketClient()
        self.pf_ws = PredictOrderbookWS()
        self.running = False
        self.hedges: list[Hedge] = []
        self.pf_markets: list[dict] = []
        self.pm_market: Optional[dict] = None
        self._task: Optional[asyncio.Task] = None
        self._last_scan = 0.0
        self.last_error: str = ""
        self._bal_pf: float | None = None
        self._bal_pm: float | None = None
        self._bal_ts: float = 0.0
        self.pf_entry_cooldown_until: float = 0.0
        self._last_entry_attempt: float = 0.0
        self.status_text: str = "idle"
        cfg = RecoveryConfig(
            base_shares=self.settings.base_shares,
            shares_step=self.settings.shares_step,
            max_shares=self.settings.max_shares,
            max_debt_usd=self.settings.max_recovery_debt_usd,
        )
        self.recovery = RecoveryBook(cfg, Path("data/recovery.json"))
        self._entry_busy = False

    # ── lifecycle ───────────────────────────────────────────────

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.status_text = "starting"
        self._task = asyncio.create_task(self._loop(), name="hedge-engine")
        logger.info("HedgeEngine started mode=%s", self.settings.mode)

    async def stop(self) -> None:
        self.running = False
        self.status_text = "stopped"
        self.pf_ws.stop()
        if self._task:
            self._task.cancel()
            self._task = None
        await self.pf.close()
        await self.pm.close()
        self._entry_busy = False
        logger.info("HedgeEngine stopped")

    async def _loop(self) -> None:
        while self.running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.last_error = str(e)
                logger.exception("engine tick: %s", e)
            await asyncio.sleep(0.5)

    async def _tick(self) -> None:
        now = time.time()
        # Force faster rescan near expiry / when no active live market
        live = self._live_pf_markets(now)
        entry_ok = self._active_pf_markets(now)
        need_scan = self._last_scan == 0
        if not need_scan:
            # Market discovery only (books stream via WS — do not poll orderbook)
            interval = 30.0 if live else 8.0
            if live:
                soonest = min(float(m.get("end") or 0) for m in live)
                if soonest - now < 45:
                    interval = 5.0
            pm_end = float((self.pm_market or {}).get("end") or 0)
            if pm_end:
                left = pm_end - now
                if left < 20 or left > 280:
                    interval = min(interval, 5.0)
            if now - self._last_scan > interval:
                need_scan = True
        if need_scan:
            await self._refresh_markets(now)
            self._last_scan = now
            live = self._live_pf_markets(now)
            entry_ok = self._active_pf_markets(now)

        await self._manage_open(now)
        open_count = sum(1 for h in self.hedges if h.status in ("OPENING", "PARTIAL", "OPEN", "CLOSING"))
        if not live or not self.pm_market:
            self.status_text = "waiting for live BTC 5m market"
        elif open_count < self.settings.max_open_hedges:
            block = self.recovery.entry_block()
            if not block:
                await self._refresh_balances(now)
                await self._try_entry(now)
            else:
                self.status_text = block
        else:
            self.status_text = f"max open hedges ({open_count})"

    async def _refresh_markets(self, now: float) -> None:
        try:
            self.pf_markets = await self.pf.find_btc_5m_markets(now)
            live = [m for m in self.pf_markets if float(m.get("end") or 0) > now - 5]
            # Keep markets referenced by open hedges so books stay available until settle
            needed = set()
            for h in self.hedges:
                if h.status not in ("CLOSED",) and h.pf_leg.token_or_market:
                    needed.add(str(h.pf_leg.token_or_market))
            by_id = {str(m["id"]): m for m in self.pf_markets}
            for mid in needed:
                if mid not in {str(m["id"]) for m in live} and mid in by_id:
                    live.append(by_id[mid])
            self.pf_markets = live
            # Subscribe only current + next window (not every distant market)
            live_sorted = sorted(live, key=lambda m: float(m.get("end") or 0))
            ws_targets = live_sorted[:2]
            for h in self.hedges:
                if h.status not in ("CLOSED",) and h.pf_leg.token_or_market:
                    mid = str(h.pf_leg.token_or_market)
                    if mid not in {str(x["id"]) for x in ws_targets}:
                        meta = by_id.get(mid) or {"id": mid, "end": h.market_end}
                        ws_targets.append(meta)
            prev_ids = set(self.pf_ws._market_ids)
            self.pf_ws.set_markets(ws_targets)
            if not self.pf_ws._running:
                self.pf_ws.start()
            # REST seed for new/empty books (WS snapshot may lag a moment after switch)
            for m in ws_targets[:2]:
                mid = str(m["id"])
                ub, ua = self.pf_ws.best(mid, "up")
                age = now - float((self.pf_ws.books.get(mid) or {}).get("updated") or 0)
                is_new = mid not in prev_ids
                need_rest = is_new or (ub is None and ua is None) or age > 5.0
                if need_rest:
                    book = await self.pf.get_orderbook(mid)
                    if book:
                        self.pf_ws.apply_rest_book(mid, book, force=True)
                if ub is None and ua is None and (m.get("up_bid") is not None or m.get("up_ask") is not None):
                    from app.predict.ws import _empty_book, _write_side
                    b = self.pf_ws.books.setdefault(mid, _empty_book())
                    _write_side(b, "up", m.get("up_bid"), m.get("up_ask"))
                    b["updated"] = now
            if self.pf_markets:
                logger.info(
                    "PF live markets: %s",
                    ", ".join(f"{m.get('id')} ends_in={int(float(m.get('end') or 0)-now)}s" for m in self.pf_markets[:4]),
                )
        except Exception as e:
            logger.warning("PF markets refresh: %s", e)

        try:
            prev_slug = (self.pm_market or {}).get("slug")
            self.pm_market = await self.pm.current_btc_5m(now)
            if self.pm_market:
                slug = self.pm_market.get("slug")
                if slug and slug != prev_slug:
                    logger.info("PM window switched %s → %s", prev_slug, slug)
                    # drop stale token quotes from previous window
                    if hasattr(self.pm, "books"):
                        self.pm.books.clear()
                tokens = [
                    t
                    for t in (
                        self.pm_market.get("up_token_id") or self.pm_market.get("up_token"),
                        self.pm_market.get("down_token_id") or self.pm_market.get("down_token"),
                    )
                    if t
                ]
                await self.pm.refresh_quotes(tokens)
                self.pm.start_ws(tokens)
        except Exception as e:
            logger.warning("PM market refresh: %s", e)

        self.status_text = (
            f"PF markets={len(self.pf_markets)} PM={'yes' if self.pm_market else 'no'}"
        )

    # ── quotes ──────────────────────────────────────────────────

    def _pf_best(self, market_id: str, direction: str) -> tuple[Optional[float], Optional[float]]:
        return self.pf_ws.best(str(market_id), direction)

    def _pm_best(self, direction: str) -> tuple[Optional[float], Optional[float]]:
        if not self.pm_market:
            return None, None
        if direction == "up":
            tid = self.pm_market.get("up_token_id") or self.pm_market.get("up_token") or ""
        else:
            tid = self.pm_market.get("down_token_id") or self.pm_market.get("down_token") or ""
        if not tid:
            return None, None
        return self.pm.best(str(tid))

    def _live_pf_markets(self, now: float) -> list[dict]:
        """Markets whose window has not ended yet (for quotes / display / WS)."""
        out = []
        for m in self.pf_markets:
            end = float(m.get("end") or 0)
            if end and end > now:
                out.append(m)
        out.sort(key=lambda m: float(m.get("end") or 0))
        return out

    def _active_pf_markets(self, now: float) -> list[dict]:
        """Live markets with enough time left to OPEN a new hedge."""
        min_left = float(self.settings.min_time_to_expiry_sec)
        return [m for m in self._live_pf_markets(now) if float(m.get("end") or 0) - now >= min_left]

    def _pick_pf_market(self, now: float, *, for_entry: bool) -> Optional[dict]:
        """Pick Predict market aligned with Polymarket window.

        for_entry=True  → only windows with min_time_to_expiry left
        for_entry=False → current live window until it actually ends
        """
        pool = self._active_pf_markets(now) if for_entry else self._live_pf_markets(now)
        if not pool:
            return None
        pm_end = float((self.pm_market or {}).get("end") or 0)
        if pm_end and pm_end > now:
            scored = []
            for m in pool:
                end = float(m.get("end") or 0)
                scored.append((abs(end - pm_end), end, m))
            scored.sort(key=lambda x: (x[0], x[1]))
            if scored[0][0] <= 120:
                return scored[0][2]
        # earliest ending = current live round
        return pool[0]

    def _pick_pf_for_pm(self, now: float) -> Optional[dict]:
        """Back-compat: entry-eligible pick."""
        return self._pick_pf_market(now, for_entry=True)


    # ── entry ───────────────────────────────────────────────────

    async def _refresh_balances(self, now: float) -> None:
        if now - self._bal_ts < 30.0:
            return
        self._bal_ts = now
        try:
            self._bal_pf = await self.pf.get_collateral_balance()
        except Exception:
            pass
        try:
            if hasattr(self.pm, "get_collateral_balance"):
                self._bal_pm = await self.pm.get_collateral_balance()
        except Exception:
            pass

    async def _try_entry(self, now: float) -> None:
        # Don't spam LIVE rejects (e.g. geo 403)
        if now < self.pf_entry_cooldown_until:
            self.status_text = f"PF cooldown {int(self.pf_entry_cooldown_until - now)}s"
            return
        if now - self._last_entry_attempt < 8.0:
            return
        self._last_entry_attempt = now
        if not self.pm_market:
            return
        pm_end = float(self.pm_market.get("end") or 0)
        if not pm_end or pm_end - now < self.settings.min_time_to_expiry_sec:
            self.status_text = "pm market near expiry — skip entry"
            return

        pf_m = self._pick_pf_for_pm(now)
        if not pf_m:
            self.status_text = "no live predict market"
            return

        pf_end = float(pf_m.get("end") or 0)
        if not pf_end or pf_end - now < self.settings.min_time_to_expiry_sec:
            self.status_text = "predict market near expiry — skip entry"
            return

        # Windows must roughly match (same 5m slot)
        if abs(pf_end - pm_end) > 90:
            self.status_text = f"window mismatch PF={int(pf_end-now)}s PM={int(pm_end-now)}s"
            return

        # Collect asks: PF up/down, PM up/down
        pf_up_bid, pf_up_ask = self._pf_best(str(pf_m["id"]), "up")
        pf_dn_bid, pf_dn_ask = self._pf_best(str(pf_m["id"]), "down")
        pm_up_bid, pm_up_ask = self._pm_best("up")
        pm_dn_bid, pm_dn_ask = self._pm_best("down")

        candidates: list[tuple[float, str, str]] = []
        # opposite pairs only
        if pf_up_ask and pm_dn_ask:
            s = pf_up_ask + pm_dn_ask
            if s < self.settings.entry_sum_max:
                candidates.append((s, "up", "down"))
        if pf_dn_ask and pm_up_ask:
            s = pf_dn_ask + pm_up_ask
            if s < self.settings.entry_sum_max:
                candidates.append((s, "down", "up"))

        if not candidates:
            self.status_text = "no entry sum < threshold"
            return
        candidates.sort(key=lambda x: x[0])
        entry_sum, pf_dir, pm_dir = candidates[0]

        if self._entry_busy:
            return
        shares = self.recovery.shares_for_entry()
        # already have open hedge on this window?
        window_end = min(pm_end, pf_end)
        for h in self.hedges:
            if h.status in ("OPENING", "PARTIAL", "OPEN", "CLOSING") and abs(h.market_end - window_end) < 30:
                return

        self._entry_busy = True
        logger.info(
            "ENTRY signal sum=%.3f PF %s + PM %s shares=%.1f pf_id=%s ends_in=%ds",
            entry_sum, pf_dir, pm_dir, shares, pf_m.get("id"), int(window_end - now),
        )
        await self._open_pair(pf_m, pf_dir, pm_dir, shares, entry_sum, now, window_end)

    async def _open_pair(
        self,
        pf_m: dict,
        pf_dir: str,
        pm_dir: str,
        shares: float,
        entry_sum: float,
        now: float,
        market_end: float,
    ) -> None:
        _, pf_ask = self._pf_best(pf_m["id"], pf_dir)
        _, pm_ask = self._pm_best(pm_dir)
        if not pf_ask or not pm_ask:
            return

        pm_token = ""
        if self.pm_market:
            if pm_dir == "up":
                pm_token = self.pm_market.get("up_token_id") or self.pm_market.get("up_token") or ""
            else:
                pm_token = self.pm_market.get("down_token_id") or self.pm_market.get("down_token") or ""
        if not pm_token:
            logger.warning("No PM token for %s", pm_dir)
            return

        hedge = Hedge(
            id=str(uuid.uuid4())[:8],
            opened_at=now,
            market_end=market_end,
            pf_leg=Leg(
                venue="predict",
                direction=pf_dir,
                token_or_market=str(pf_m["id"]),
                requested_shares=shares,
            ),
            pm_leg=Leg(
                venue="polymarket",
                direction=pm_dir,
                token_or_market=str(pm_token),
                requested_shares=shares,
            ),
            entry_sum=entry_sum,
            status="OPENING",
        )
        self.hedges.append(hedge)

        # 1) Predict first — aggressive LIMIT (ask + slip) so it takes liquidity like a market buy.
        # Order size is inflated by taker fee so *net* shares ≈ target after fee (~2%).
        fee_bps = int(getattr(self, "_pf_fee_bps", 200) or 200)
        fee_mult = 1.0 - fee_bps / 10000.0
        order_shares = round(shares / fee_mult, 4) if fee_mult > 0 else shares
        # Cross the book: pay up to +3¢ vs best ask (still limit, fills as taker when book has size)
        pf_px = min(0.99, round(float(pf_ask) + 0.03, 2))
        logger.info(
            "PF BUY target_net=%.2f order_size=%.4f limit=%.2f (ask=%.2f +slip) fee_bps=%d",
            shares, order_shares, pf_px, pf_ask, fee_bps,
        )
        pf_resp = await self.pf.place_order(
            pf_m["id"],
            "BUY",
            order_shares,
            pf_px,
            is_yes=(pf_dir == "up"),
        )
        pf_fill = fill_from_predict(pf_resp, order_shares)

        # Predict POST /orders often returns SUBMITTED (201) before match — wait for fill
        if pf_fill.status == "SUBMITTED" and pf_fill.order_id and self.settings.is_live:
            logger.info("PF order %s submitted — waiting for fill…", pf_fill.order_id[:16])
            lim_px = float((pf_resp or {}).get("avgPrice") or pf_px or 0) or 0.5
            polled = await self.pf.wait_order_fill(
                pf_fill.order_id, order_shares, lim_px, timeout=20.0,
                order_hash=getattr(pf_fill, "order_hash", "") or "",
                is_buy=True,
                fee_bps=fee_bps,
            )
            st = str(polled.get("status") or "").upper()
            sh = float(polled.get("filledShares") or 0)
            if st == "FILLED" or sh > 0.5:
                px = float(polled.get("avgPrice") or lim_px or 0) or lim_px
                if sh <= 0:
                    sh = shares  # fallback target net
                pf_fill = Fill(
                    status="FILLED", shares=sh, price=px,
                    usd=round(sh * px, 4),
                    order_id=pf_fill.order_id,
                    order_hash=getattr(pf_fill, "order_hash", "") or str(polled.get("orderHash") or ""),
                )
                logger.info("PF order filled net_shares=%.4f @ %.4f (target %.2f)", sh, px, shares)
            elif st == "REJECTED":
                pf_fill = Fill(status="REJECTED", reason=polled.get("reason") or "cancelled", order_id=pf_fill.order_id)
            else:
                # still open — cancel; if any partial fill keep it, else reject
                logger.warning("PF order still open after wait — cancel")
                try:
                    await self.pf.cancel_order(pf_fill.order_id, getattr(pf_fill, "order_hash", "") or "")
                except Exception:
                    pass
                if sh > 0.5:
                    px = float(polled.get("avgPrice") or lim_px or 0) or lim_px
                    pf_fill = Fill(status="FILLED", shares=sh, price=px, usd=round(sh * px, 4),
                                   order_id=pf_fill.order_id,
                                   order_hash=getattr(pf_fill, "order_hash", "") or "")
                    logger.info("PF partial kept net_shares=%.4f", sh)
                else:
                    self._entry_busy = False
                    pf_fill = Fill(status="REJECTED", reason="not filled in time", order_id=pf_fill.order_id)

        self._apply_fill(hedge.pf_leg, pf_fill)

        # Reconcile PF shares + REAL avg cost (prefer cost_usd / activity VWAP, not min tick)
        if pf_fill.status == "FILLED" and self.settings.is_live:
            try:
                import asyncio as _asyncio
                lim = float(pf_fill.price or 0)
                real_sh = float(pf_fill.shares or 0)
                real_px = 0.0
                real_usd = 0.0
                for attempt in range(3):
                    await _asyncio.sleep(0.55 + attempt * 0.45)
                    pos = await self.pf.get_position_for_market(pf_m["id"])
                    if pos and pos.get("shares", 0) > 0:
                        real_sh = float(pos["shares"]) or real_sh
                        ap = float(pos.get("avg_price") or 0)
                        cu = float(pos.get("cost_usd") or 0)
                        if cu > 0 and real_sh > 0:
                            # cost basis is the most trustworthy
                            real_usd = cu
                            real_px = cu / real_sh
                        elif 0 < ap < 1.0 and (lim <= 0 or ap <= lim + 0.01):
                            # only accept pos avg if not suspiciously far below limit
                            # (API sometimes returns stale/wrong low avg)
                            if real_px <= 0 or abs(ap - lim) < abs(real_px - lim):
                                real_px = ap
                    act = await self.pf.get_fill_price_from_activity(
                        pf_m["id"], order_id=pf_fill.order_id or "", limit_price=lim,
                    )
                    if act:
                        ap = float(act.get("avg_price") or 0)
                        cu = float(act.get("cost_usd") or 0)
                        if cu > 0 and real_sh > 0:
                            # activity total value preferred
                            if real_usd <= 0 or cu > real_usd * 0.95:
                                real_usd = cu
                                real_px = cu / max(real_sh, float(act.get("shares") or real_sh) or 1)
                        elif 0 < ap < 1.0:
                            # VWAP from activity — do not take min of sources
                            if real_px <= 0:
                                real_px = ap
                            elif abs(ap - lim) < abs(real_px - lim):
                                # closer to limit is often more accurate than a suspiciously low tick
                                real_px = ap
                    if real_px > 0 and (lim <= 0 or real_px <= lim + 0.01 or attempt == 2):
                        break
                if real_px <= 0:
                    real_px = float(pf_fill.price or 0)
                # Guard: never use avg that is >15% below limit without USD proof
                if lim > 0 and real_px > 0 and real_px < lim * 0.85 and real_usd <= 0:
                    logger.warning(
                        "PF reconcile suspicious avg %.4f << limit %.4f without cost_usd — keep limit-based",
                        real_px, lim,
                    )
                    real_px = float(pf_fill.price or lim)
                if real_usd <= 0 and real_sh > 0 and real_px > 0:
                    real_usd = real_sh * real_px
                if real_sh > 0 and real_px > 0:
                    logger.info(
                        "PF reconcile market=%s shares=%.4f→%.4f avg=%.4f→%.4f usd=%.4f (limit was %.4f)",
                        pf_m["id"], pf_fill.shares, real_sh, pf_fill.price, real_px, real_usd, lim,
                    )
                    pf_fill = Fill(
                        status="FILLED",
                        shares=round(real_sh, 4),
                        price=round(real_px, 4),
                        usd=round(real_usd, 4),
                        order_id=pf_fill.order_id,
                        order_hash=getattr(pf_fill, "order_hash", "") or "",
                    )
                    self._apply_fill(hedge.pf_leg, pf_fill)
            except Exception as e:
                logger.warning("PF position reconcile failed: %s", e)

        if pf_fill.status == "REJECTED":
            hedge.status = "CLOSED"
            hedge.closed_at = time.time()
            reason = (pf_fill.reason or "").lower()
            self._entry_busy = False
            logger.warning("PF reject %s — abort pair", pf_fill.reason)
            if "jurisdiction" in reason or "403" in reason or "forbidden" in reason:
                self.pf_entry_cooldown_until = now + 3600.0
                self.last_error = "Predict 403: not available in your jurisdiction"
                logger.error(
                    "Predict blocked by jurisdiction (403). "
                    "LIVE orders will not work from this IP until you change network/VPN. "
                    "Cooldown/WS still work; switch MODE=PAPER or use allowed region."
                )
            elif "insufficient" in reason or "collateral" in reason or "balance" in reason:
                self.pf_entry_cooldown_until = now + 120.0
                self.last_error = "Predict insufficient USDT balance/allowance"
            else:
                # brief cooldown so we refresh quotes before retry
                self.pf_entry_cooldown_until = now + 8.0
            return

        # PM size = *actual net* PF shares (equal share hedge, not equal $)
        filled = float(pf_fill.shares) if pf_fill.shares > 0 else shares
        hedge.pm_leg.requested_shares = filled
        if hedge.pm_leg.order_id and hedge.pm_leg.status in ("FILLED", "SUBMITTED"):
            logger.warning("PM leg already has order %s — skip second place", hedge.pm_leg.order_id[:18])
            return
        # 2) Polymarket FOK for the same share count (aggressive +2¢)
        pm_px = min(0.99, round(float(pm_ask) + 0.02, 2))
        logger.info("PM BUY match PF net=%.4f @ limit %.2f (ask=%.2f)", filled, pm_px, pm_ask)
        pm_resp = await self.pm.place_market_buy(str(pm_token), filled, pm_px)
        pm_fill = fill_from_pm_order(pm_resp, filled)
        self._apply_fill(hedge.pm_leg, pm_fill)

        # If PM only resting (LIVE/SUBMITTED), cancel immediately — never leave open size
        # that a retry would double.
        if pm_fill.status == "SUBMITTED" and pm_fill.order_id:
            logger.warning(
                "PM order %s not filled (resting) — cancel, mark REJECTED to avoid double size",
                pm_fill.order_id[:18],
            )
            try:
                await self.pm.cancel_order(pm_fill.order_id)
            except Exception:
                pass
            # one aggressive retry at a slightly worse price
            worse = min(0.99, round(float(pm_ask) + 0.02, 2))
            pm_resp2 = await self.pm.place_market_buy(str(pm_token), filled, worse)
            pm_fill = fill_from_pm_order(pm_resp2, filled)
            self._apply_fill(hedge.pm_leg, pm_fill)
            if pm_fill.status == "SUBMITTED" and pm_fill.order_id:
                try:
                    await self.pm.cancel_order(pm_fill.order_id)
                except Exception:
                    pass
                pm_fill = Fill(status="REJECTED", reason="PM resting after retry", order_id=pm_fill.order_id)
                self._apply_fill(hedge.pm_leg, pm_fill)

        if pf_fill.status == "FILLED" and pm_fill.status == "FILLED":
            hedge.status = "OPEN"
            self._entry_busy = False
            # Real entry sum from actual fill prices (not pre-trade quote sum)
            hedge.entry_sum = round(
                float(hedge.pf_leg.entry_price or 0) + float(hedge.pm_leg.entry_price or 0), 4
            )
            logger.info(
                "Hedge %s OPEN PF %.2f@%.3f + PM %.2f@%.3f entry_sum=%.3f",
                hedge.id,
                pf_fill.shares, hedge.pf_leg.entry_price,
                pm_fill.shares, hedge.pm_leg.entry_price,
                hedge.entry_sum,
            )
        elif pf_fill.status == "FILLED" or pm_fill.status == "FILLED":
            hedge.status = "PARTIAL"
            self._entry_busy = False
            logger.info("Hedge %s PARTIAL PF=%s PM=%s", hedge.id, pf_fill.status, pm_fill.status)
        else:
            hedge.status = "OPENING"
            self._entry_busy = False

    def _apply_fill(self, leg: Leg, fill: Fill) -> None:
        leg.order_id = fill.order_id or leg.order_id
        leg.reject_reason = fill.reason
        if fill.status == "FILLED":
            leg.status = "FILLED"
            leg.filled_shares = fill.shares
            leg.shares = fill.shares
            leg.entry_price = fill.price
            # Prefer explicit USD from venue (PM makingAmount); else shares×price
            if fill.usd and fill.usd > 0:
                leg.entry_usd = float(fill.usd)
            elif fill.shares > 0 and fill.price > 0:
                leg.entry_usd = round(fill.shares * fill.price, 4)
            # Re-derive avg if usd known (more accurate than limit)
            if leg.entry_usd > 0 and leg.filled_shares > 0:
                leg.entry_price = round(leg.entry_usd / leg.filled_shares, 4)
        elif fill.status == "REJECTED":
            leg.status = "REJECTED"
        elif fill.status == "SUBMITTED":
            leg.status = "SUBMITTED"

    # ── manage open ─────────────────────────────────────────────

    async def _manage_open(self, now: float) -> None:
        for hedge in list(self.hedges):
            if hedge.status in ("CLOSED",):
                continue
            if hedge.status in ("OPENING", "PARTIAL"):
                await self._retry_partial(hedge, now)
            # Past window end → settlement only (no more sells on dead markets)
            if hedge.status in ("OPEN", "PARTIAL", "CLOSING") and now >= hedge.market_end:
                if hedge.status == "CLOSING":
                    logger.info(
                        "Hedge %s CLOSING past end → SETTLE (stop sell spam on closed market)",
                        hedge.id,
                    )
                hedge.status = "SETTLING"
                await self._settle_pair(hedge, now)
                continue
            if hedge.status == "OPEN":
                await self._try_exit(hedge, now)
            if hedge.status == "CLOSING":
                await self._finish_exit(hedge, now)
            if hedge.status == "SETTLING":
                await self._settle_pair(hedge, now)

    async def _retry_partial(self, hedge: Hedge, now: float) -> None:
        if hedge.retries >= self.settings.leg_retry_max:
            return
        if now - hedge.last_retry_at < self.settings.leg_retry_sec:
            return
        if now >= hedge.market_end:
            return
        hedge.last_retry_at = now
        hedge.retries += 1

        if hedge.pf_leg.status != "FILLED":
            _, ask = self._pf_best(hedge.pf_leg.token_or_market, hedge.pf_leg.direction)
            if ask:
                resp = await self.pf.place_order(
                    hedge.pf_leg.token_or_market,
                    "BUY",
                    hedge.pf_leg.requested_shares,
                    ask,
                    is_yes=(hedge.pf_leg.direction == "up"),
                )
                self._apply_fill(hedge.pf_leg, fill_from_predict(resp, hedge.pf_leg.requested_shares))

        if hedge.pm_leg.status != "FILLED" and hedge.pf_leg.status == "FILLED":
            if (hedge.pm_leg.filled_shares or 0) > 0:
                logger.info("PM already has %.2f filled — skip retry buy", hedge.pm_leg.filled_shares)
            _, ask = self._pm_best(hedge.pm_leg.direction)
            size = hedge.pf_leg.filled_shares or hedge.pf_leg.requested_shares
            if ask and size > 0 and (hedge.pm_leg.filled_shares or 0) <= 0:
                # Cancel any previous resting PM order first
                if hedge.pm_leg.order_id and hedge.pm_leg.status == "SUBMITTED":
                    try:
                        await self.pm.cancel_order(hedge.pm_leg.order_id)
                        logger.info("PM cancel before retry %s", hedge.pm_leg.order_id[:18])
                    except Exception as e:
                        logger.warning("PM cancel before retry: %s", e)
                # bump limit so notional stays >= $1 even if ask is tiny
                px = min(0.99, round(float(ask) + 0.03, 2))
                resp = await self.pm.place_market_buy(hedge.pm_leg.token_or_market, size, px)
                fill = fill_from_pm_order(resp, size)
                if fill.status == "SUBMITTED" and fill.order_id:
                    try:
                        await self.pm.cancel_order(fill.order_id)
                    except Exception:
                        pass
                    fill = Fill(status="REJECTED", reason="PM resting on retry", order_id=fill.order_id)
                self._apply_fill(hedge.pm_leg, fill)

        if hedge.pf_leg.status == "FILLED" and hedge.pm_leg.status == "FILLED":
            hedge.status = "OPEN"
            logger.info("Hedge %s promoted PARTIAL→OPEN", hedge.id)

    async def _try_exit(self, hedge: Hedge, now: float) -> None:
        """Early exit when sum of bids >= exit_sum_min (sell both legs at market).

        Exit prices are live bids in (0,1), NOT 0/1 settlement. Example:
        UP bought @0.40 can sell @0.11 if market moved — that is intentional.
        """
        # Don't early-exit in last 15s — let settlement handle it
        if now >= hedge.market_end - 15:
            return
        pf_bid, _ = self._pf_best(hedge.pf_leg.token_or_market, hedge.pf_leg.direction)
        pm_bid, _ = self._pm_best(hedge.pm_leg.direction)
        if not pf_bid or not pm_bid:
            return
        # Ignore absurd one-sided books (stale/empty)
        if pf_bid < 0.02 or pm_bid < 0.02:
            return
        s = pf_bid + pm_bid
        if s < self.settings.exit_sum_min:
            return
        logger.info(
            "EXIT signal hedge=%s sum=%.3f pf_bid=%.3f pm_bid=%.3f (market sell, not settle)",
            hedge.id, s, pf_bid, pm_bid,
        )
        hedge.exit_sum = s
        hedge.close_reason = "EXIT"
        hedge.status = "CLOSING"
        await self._sell_both(hedge)

    async def _sell_both(self, hedge: Hedge) -> None:
        await asyncio.gather(
            self._sell_leg(hedge.pf_leg, "PF"),
            self._sell_leg(hedge.pm_leg, "PM"),
        )

    async def _sell_leg(self, leg: Leg, label: str) -> None:
        if leg.status == "CLOSED" or leg.filled_shares <= 0:
            leg.status = "CLOSED"
            return
        # Sell slightly under tracked size — PF fee/rounding leaves balance < filled_shares
        size = float(leg.filled_shares)
        if leg.venue == "predict":
            size = max(0.01, round(size * 0.97, 4))  # PF fee leaves ~2% less than tracked
        if leg.venue == "predict":
            bid, _ = self._pf_best(leg.token_or_market, leg.direction)
            # aggressive sell limit (cross book)
            px = max(0.01, min(float(bid or 0.01), max(0.01, leg.entry_price * 0.9)))
            if bid:
                px = max(0.01, round(float(bid) - 0.02, 2))
            resp = await self.pf.place_order(
                leg.token_or_market,
                "SELL",
                size,
                px,
                is_yes=(leg.direction == "up"),
            )
            fill = fill_from_predict(resp, size)
        else:
            bid, _ = self._pm_best(leg.direction)
            px = bid or max(0.01, leg.entry_price * 0.9)
            resp = await self.pm.place_market_sell(leg.token_or_market, size, px)
            fill = fill_from_pm_order(resp, size)
        if fill.status == "FILLED":
            leg.status = "CLOSED"
            leg.exit_price = fill.price
            leg.exit_usd = fill.usd
        elif fill.status == "REJECTED":
            reason = (fill.reason or "").lower()
            logger.warning("%s sell rejected: %s", label, fill.reason)
            # No shares left to sell → stop retrying, wait for SETTLE at window end
            if "insufficient" in reason and "share" in reason:
                leg.status = "CLOSED"
                leg.exit_price = leg.exit_price or 0.0
                logger.info("%s sell: insufficient shares — mark CLOSED, settle at end", label)

    async def _finish_exit(self, hedge: Hedge, now: float) -> None:
        # Throttle sell attempts (avoid 1Hz spam when market closed)
        last = float(getattr(hedge, "last_exit_try_at", 0) or 0)
        if now - last < 3.0:
            return
        hedge.last_exit_try_at = now
        hedge.exit_tries = int(getattr(hedge, "exit_tries", 0) or 0) + 1

        if hedge.pf_leg.status != "CLOSED":
            await self._sell_leg(hedge.pf_leg, "PF")
        if hedge.pm_leg.status != "CLOSED":
            await self._sell_leg(hedge.pm_leg, "PM")

        # Dead market / too many fails → wait for settlement at end
        for leg in (hedge.pf_leg, hedge.pm_leg):
            reason = (leg.reject_reason or "").lower()
            if "not currently trading" in reason or "market_not_trading" in reason:
                logger.warning(
                    "Hedge %s exit blocked (market not trading) — will SETTLE at end",
                    hedge.id,
                )
                # keep CLOSING until market_end flips to SETTLING in _manage_open
                return
        if hedge.exit_tries >= 8:
            logger.warning("Hedge %s exit tries exhausted — hold to SETTLE", hedge.id)
            return

        if hedge.pf_leg.status == "CLOSED" and hedge.pm_leg.status == "CLOSED":
            def _leg_pnl_exit(leg: Leg) -> float:
                sh = float(leg.filled_shares or 0)
                if sh <= 0:
                    return 0.0
                cost = float(leg.entry_usd or 0) or float(leg.entry_price or 0) * sh
                proceeds = float(leg.exit_usd or 0) or float(leg.exit_price or 0) * sh
                return proceeds - cost
            pf_pnl = _leg_pnl_exit(hedge.pf_leg)
            pm_pnl = _leg_pnl_exit(hedge.pm_leg)
            hedge.pnl = pf_pnl + pm_pnl
            hedge.exit_sum = float(hedge.pf_leg.exit_price or 0) + float(hedge.pm_leg.exit_price or 0)
            hedge.close_reason = hedge.close_reason or "EXIT"
            hedge.status = "CLOSED"
            hedge.closed_at = now
            self.recovery.on_pair_closed(
                hedge.id, pf_pnl, pm_pnl, 0.0, hedge.pf_leg.filled_shares or 0
            )
            logger.info(
                "Hedge %s EXIT pf=%.3f→%.3f pm=%.3f→%.3f exit_sum=%.3f pnl=%.4f",
                hedge.id,
                hedge.pf_leg.entry_price,
                hedge.pf_leg.exit_price,
                hedge.pm_leg.entry_price,
                hedge.pm_leg.exit_price,
                hedge.exit_sum,
                hedge.pnl,
            )

    async def _settle_pair(self, hedge: Hedge, now: float) -> None:
        """At expiry: each leg settles to 0 or 1 — opposite legs are complementary.

        One shared outcome for the window (Up won / Down won). Then:
          direction==up  → exit 1 if Up won else 0
          direction==down → exit 0 if Up won else 1
        So opposite PF/PM legs always sum to 1.0 (never both 0 or both 1).
        """
        if now < hedge.market_end:
            return

        # Prefer extreme quotes near expiry; else mid >= 0.5
        ub, ua = self._pf_best(hedge.pf_leg.token_or_market, "up")
        pf_mid = None
        if ub is not None and ua is not None:
            pf_mid = 0.5 * (ub + ua)
        elif ub is not None:
            pf_mid = ub
        elif ua is not None:
            pf_mid = ua

        pm_ub, pm_ua = self._pm_best("up")
        pm_mid = None
        if pm_ub is not None and pm_ua is not None:
            pm_mid = 0.5 * (pm_ub + pm_ua)
        elif pm_ub is not None:
            pm_mid = pm_ub
        elif pm_ua is not None:
            pm_mid = pm_ua

        def decide_up_won() -> bool:
            # Strong signals first
            for bid, ask in ((ub, ua), (pm_ub, pm_ua)):
                if ask is not None and ask <= 0.08:
                    return False
                if bid is not None and bid >= 0.92:
                    return True
            # Prefer clearer mid (farther from 0.5)
            candidates = [m for m in (pf_mid, pm_mid) if m is not None]
            if not candidates:
                return True  # default Up if totally unknown
            # pick extreme-most mid
            best = max(candidates, key=lambda x: abs(x - 0.5))
            return best >= 0.5

        up_won = decide_up_won()

        for leg in (hedge.pf_leg, hedge.pm_leg):
            # Keep real market-sell exit if already closed via EXIT
            if leg.status == "CLOSED" and leg.exit_price is not None:
                # only keep if it looks like a real trade (not default entry echo)
                if abs(leg.exit_price - leg.entry_price) > 1e-9 or hedge.close_reason == "EXIT":
                    if hedge.close_reason == "EXIT":
                        continue
            if (leg.filled_shares or 0) <= 0:
                leg.status = "CLOSED"
                leg.exit_price = leg.exit_price or 0.0
                continue
            if leg.direction == "up":
                leg.exit_price = 1.0 if up_won else 0.0
            else:
                leg.exit_price = 0.0 if up_won else 1.0
            leg.exit_usd = leg.exit_price * leg.filled_shares
            leg.status = "CLOSED"

        def _leg_pnl(leg: Leg) -> float:
            sh = float(leg.filled_shares or 0)
            if sh <= 0:
                return 0.0
            entry_cost = float(leg.entry_usd or 0)
            if entry_cost <= 0 and leg.entry_price:
                entry_cost = float(leg.entry_price) * sh
            exit_val = float(leg.exit_usd or 0)
            if exit_val <= 0 and leg.exit_price is not None:
                exit_val = float(leg.exit_price) * sh
            return exit_val - entry_cost

        pf_pnl = _leg_pnl(hedge.pf_leg)
        pm_pnl = _leg_pnl(hedge.pm_leg)
        hedge.pnl = pf_pnl + pm_pnl
        hedge.exit_sum = float(hedge.pf_leg.exit_price or 0) + float(hedge.pm_leg.exit_price or 0)
        if hedge.close_reason != "EXIT":
            hedge.close_reason = "SETTLE"
        hedge.status = "CLOSED"
        hedge.closed_at = now
        self.recovery.on_pair_closed(hedge.id, pf_pnl, pm_pnl, 0.0, hedge.pf_leg.filled_shares or 0)
        logger.info(
            "Hedge %s %s up_won=%s pf=%s@%.3f→%.3f pm=%s@%.3f→%.3f exit_sum=%.3f pnl=%.4f",
            hedge.id,
            hedge.close_reason,
            up_won,
            hedge.pf_leg.direction,
            hedge.pf_leg.entry_price,
            hedge.pf_leg.exit_price,
            hedge.pm_leg.direction,
            hedge.pm_leg.entry_price,
            hedge.pm_leg.exit_price,
            hedge.exit_sum,
            hedge.pnl,
        )


    async def test_buy(self, usd: float = 1.0) -> dict:
        """One Predict buy, no Polymarket. Proves the order path."""
        if usd < 0.01 or usd > 5:
            return {"status": "REJECTED", "reason": "test buy must be from $0.01 to $5"}
        if not self.settings.is_live:
            return {"status": "REJECTED", "reason": "test buy only runs in LIVE (set MODE=LIVE)"}
        if not self.pf_markets:
            await self._refresh_markets(time.time())
        if not self.pf_markets:
            return {"status": "REJECTED", "reason": "no PF markets"}
        m = self.pf_markets[0]
        _, ask = self._pf_best(m["id"], "up")
        if not ask:
            book = await self.pf.get_orderbook(m["id"])
            ask = self.pf.parse_book_top(book)[1]
        if not ask:
            return {"status": "REJECTED", "reason": "no ask"}
        shares = max(1.0, usd / ask)
        resp = await self.pf.place_order(m["id"], "BUY", shares, ask, is_yes=True)
        return resp

    def snapshot(self) -> dict:
        import time as _time
        now = _time.time()

        def leg_snap(leg: Leg) -> dict:
            sh = float(leg.filled_shares or leg.shares or 0)
            entry_usd = float(getattr(leg, "entry_usd", 0) or 0)
            if entry_usd <= 0 and sh > 0 and leg.entry_price:
                entry_usd = round(sh * float(leg.entry_price), 4)
            exit_usd = float(leg.exit_usd or 0)
            if exit_usd <= 0 and sh > 0 and leg.exit_price is not None:
                exit_usd = round(sh * float(leg.exit_price), 4)
            return {
                "venue": leg.venue,
                "direction": leg.direction,
                "token": leg.token_or_market,
                "status": leg.status,
                "shares": sh,
                "entry": leg.entry_price,
                "exit": leg.exit_price,
                "entry_price": leg.entry_price,
                "exit_price": leg.exit_price,
                "filled_shares": sh,
                "entry_usd": entry_usd,
                "exit_usd": exit_usd,
                "pnl": round(exit_usd - entry_usd, 4) if sh > 0 else 0.0,
            }

        # Active PF market = soonest ending with a book
        # Display the CURRENT live window until it ends (do not jump early)
        pf_active = self._pick_pf_market(now, for_entry=False)

        pf_quotes = {"up_bid": None, "up_ask": None, "down_bid": None, "down_ask": None}
        pf_age = None
        if pf_active:
            mid = str(pf_active["id"])
            ub, ua = self.pf_ws.best(mid, "up")
            db, da = self.pf_ws.best(mid, "down")
            pf_quotes = {"up_bid": ub, "up_ask": ua, "down_bid": db, "down_ask": da}
            book = self.pf_ws.books.get(mid) or {}
            if book.get("updated"):
                pf_age = now - float(book["updated"])

        pm_quotes = {"up_bid": None, "up_ask": None, "down_bid": None, "down_ask": None}
        pm_age = None
        if self.pm_market:
            for d, key in (
                ("up", "up_token_id"),
                ("down", "down_token_id"),
            ):
                tid = self.pm_market.get(key) or self.pm_market.get(key.replace("_id", ""))
                if tid:
                    bid, ask = self.pm.best(str(tid))
                    pm_quotes[f"{d}_bid"] = bid
                    pm_quotes[f"{d}_ask"] = ask

        market_end = None
        if pf_active:
            market_end = float(pf_active.get("end") or 0) or None
        if self.pm_market and self.pm_market.get("end"):
            market_end = float(self.pm_market["end"])

        time_left = max(0.0, (market_end or 0) - now) if market_end else 0.0
        slug = ""
        if self.pm_market:
            slug = str(self.pm_market.get("slug") or "")
        if not slug and pf_active:
            slug = str(pf_active.get("title") or pf_active.get("id") or "")

        open_hedges = []
        history = []
        for h in self.hedges:
            row = {
                "id": h.id,
                "status": h.status,
                "entry_sum": h.entry_sum,
                "exit_sum": h.exit_sum,
                "pnl": h.pnl,
                "bn": leg_snap(h.pf_leg),  # UI still uses bn column for Predict leg
                "pf": leg_snap(h.pf_leg),
                "pm": leg_snap(h.pm_leg),
                "opened_at": h.opened_at,
                "market_end": h.market_end,
                "slug": slug,
            }
            if h.status in ("CLOSED",):
                history.append(row)
            else:
                open_hedges.append(row)

        return {
            "running": self.running,
            "mode": self.settings.mode,
            "status": self.status_text,
            "last_error": self.last_error or self.pf.last_error or self.pm.last_error,
            "settings": {
                "entry_sum_max": self.settings.entry_sum_max,
                "exit_sum_min": self.settings.exit_sum_min,
            },
            "recovery": self.recovery.snapshot(),
            "realized_pnl": sum(h.pnl for h in self.hedges if h.status == "CLOSED"),
            "balances": {
                "predict": self._bal_pf,
                "polymarket": self._bal_pm,
                "total": (None if self._bal_pf is None and self._bal_pm is None
                          else round((self._bal_pf or 0) + (self._bal_pm or 0), 4)),
            },
            "prices": {
                "slug": slug,
                "market_end": market_end,
                "time_left": time_left,
                "bn_age": pf_age,
                "pm_age": pm_age,
                "pf": pf_quotes,
                "predict": pf_quotes,
                "bn": pf_quotes,
                "pm": pm_quotes,
                "polymarket": pm_quotes,
            },
            "open_hedges": open_hedges,
            "history": history[-20:],
            "logs": get_recent_logs(50),
            "pf_markets": [
                {"id": m.get("id"), "title": m.get("title"), "end": m.get("end")}
                for m in self.pf_markets[:5]
            ],
            "pm_market": {
                "slug": (self.pm_market or {}).get("slug"),
                "end": (self.pm_market or {}).get("end"),
            } if self.pm_market else None,
        }



# singleton used by dashboard
_engine: Optional[HedgeEngine] = None


def get_engine() -> HedgeEngine:
    global _engine
    if _engine is None:
        _engine = HedgeEngine()
    return _engine
