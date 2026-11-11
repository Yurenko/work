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
    pm_slug: str = ""  # fixed at open — used for resolution (not current window)
    pf_market_id: str = ""
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

        pm_slug_now = ""
        if self.pm_market:
            pm_slug_now = str(self.pm_market.get("slug") or "")
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
            pm_slug=pm_slug_now,
            pf_market_id=str(pf_m["id"]),
        )
        self.hedges.append(hedge)

        # Simultaneous LIMIT buys on both venues with the SAME share target.
        # PF: aggressive limit (ask+slip), size inflated for ~2% fee so net ≈ target.
        # PM: share-sized FAK limit at ask (NOT dollar-market FOK — that over-fills shares).
        # Wait ~6s for fills; retry the missing leg once.
        import asyncio as _asyncio

        fee_bps = int(getattr(self, "_pf_fee_bps", 200) or 200)
        fee_mult = 1.0 - fee_bps / 10000.0
        order_shares = round(float(shares) / fee_mult, 4) if fee_mult > 0 else float(shares)
        target_net = float(shares)
        pf_px = min(0.99, round(float(pf_ask) + 0.03, 2))
        pm_px = min(0.99, round(float(pm_ask) + 0.01, 2))  # 1¢ slip to cross

        logger.info(
            "LIMIT ENTRY simultaneous target_net=%.2f PF order=%.4f @ %.2f | PM size=%.4f @ %.2f",
            target_net, order_shares, pf_px, target_net, pm_px,
        )

        async def _pf_buy():
            return await self.pf.place_order(
                pf_m["id"], "BUY", order_shares, pf_px, is_yes=(pf_dir == "up"),
            )

        async def _pm_buy(size: float, px: float):
            return await self.pm.place_market_buy(str(pm_token), size, px)

        pf_resp, pm_resp = await _asyncio.gather(_pf_buy(), _pm_buy(target_net, float(pm_ask)))
        pf_fill = fill_from_predict(pf_resp, order_shares)
        pm_fill = fill_from_pm_order(pm_resp, target_net)

        # Wait for SUBMITTED → FILLED (both venues can ack before match)
        wait_deadline = time.time() + 6.5
        while time.time() < wait_deadline:
            need_wait = False
            if pf_fill.status == "SUBMITTED" and pf_fill.order_id:
                need_wait = True
                try:
                    st = await self.pf.get_order(pf_fill.order_id)
                    if st:
                        got = fill_from_predict(st, order_shares)
                        if got.status == "FILLED":
                            pf_fill = got
                except Exception:
                    pass
                # Also check position
                if pf_fill.status != "FILLED":
                    try:
                        pos = await self.pf.get_position_for_market(
                            pf_m["id"], is_yes=(pf_dir == "up"),
                        )
                        if pos and float(pos.get("shares") or 0) >= target_net * 0.9:
                            sh = float(pos["shares"])
                            px = float(pos.get("avg_price") or pf_px)
                            cu = float(pos.get("cost_usd") or 0) or sh * px
                            pf_fill = Fill(
                                status="FILLED", shares=min(sh, order_shares),
                                price=px if 0 < px <= 1 else pf_px,
                                usd=cu, order_id=pf_fill.order_id,
                            )
                    except Exception:
                        pass
            if pm_fill.status == "SUBMITTED" and pm_fill.order_id:
                need_wait = True
                # Cancel resting GTC if any — we use FAK so should be rare
                try:
                    await self.pm.cancel_order(pm_fill.order_id)
                except Exception:
                    pass
            if pf_fill.status == "FILLED" and pm_fill.status in ("FILLED", "REJECTED"):
                break
            if not need_wait and pf_fill.status != "SUBMITTED":
                break
            await _asyncio.sleep(0.6)

        # Cap PF shares to target (positions API can return total market size)
        if pf_fill.status == "FILLED" and pf_fill.shares > target_net * 1.05:
            logger.warning(
                "PF fill shares %.4f > target %.4f — cap to target for PM match",
                pf_fill.shares, target_net,
            )
            # keep price, scale usd
            if pf_fill.shares > 0 and pf_fill.usd:
                px = pf_fill.usd / pf_fill.shares
                pf_fill = Fill(
                    status="FILLED", shares=target_net, price=px,
                    usd=target_net * px, order_id=pf_fill.order_id,
                )
            else:
                pf_fill = Fill(
                    status="FILLED", shares=target_net,
                    price=pf_fill.price or pf_px, usd=target_net * float(pf_fill.price or pf_px),
                    order_id=pf_fill.order_id,
                )

        # Prefer activity/positions avg for PF (limit bias)
        if pf_fill.status == "FILLED":
            try:
                act = await self.pf.get_fill_price_from_activity(
                    pf_m["id"], order_id=pf_fill.order_id or "", limit_price=pf_px,
                )
                if act:
                    cu = float(act.get("cost_usd") or 0)
                    ap = float(act.get("avg_price") or 0)
                    ash = float(act.get("shares") or 0) or float(pf_fill.shares)
                    ash = min(ash, target_net * 1.05)
                    if cu > 0 and ash > 0:
                        if cu / ash > 1.5 and cu > 50:
                            cu = cu / 100.0
                        cand = cu / ash
                        if 0.01 <= cand <= 1.0:
                            pf_fill = Fill(
                                status="FILLED", shares=round(ash, 4),
                                price=round(cand, 4), usd=round(cu, 4),
                                order_id=pf_fill.order_id,
                            )
                    elif 0.01 <= ap <= 1.0:
                        pf_fill = Fill(
                            status="FILLED", shares=round(float(pf_fill.shares), 4),
                            price=round(ap, 4),
                            usd=round(float(pf_fill.shares) * ap, 4),
                            order_id=pf_fill.order_id,
                        )
            except Exception as e:
                logger.debug("PF activity reconcile: %s", e)

        filled_pf = float(pf_fill.shares) if pf_fill.status == "FILLED" else 0.0
        filled_pm = float(pm_fill.shares) if pm_fill.status == "FILLED" else 0.0
        # Match PM size to actual PF net (never more than target)
        match_size = min(target_net, filled_pf) if filled_pf > 0 else target_net
        if match_size <= 0:
            match_size = target_net

        # Retry missing / short legs once after short pause
        if pf_fill.status == "FILLED" and (
            pm_fill.status != "FILLED" or filled_pm < match_size * 0.92
        ):
            await _asyncio.sleep(1.0)
            need = match_size - (filled_pm if pm_fill.status == "FILLED" else 0.0)
            if need >= 0.5:
                logger.info("PM limit retry need=%.4f @ ask=%.4f", need, pm_ask)
                # cancel any resting
                if pm_fill.order_id and pm_fill.status == "SUBMITTED":
                    try:
                        await self.pm.cancel_order(pm_fill.order_id)
                    except Exception:
                        pass
                pm_resp2 = await self.pm.place_market_buy(str(pm_token), need, min(0.99, float(pm_ask) + 0.01))
                pm_fill2 = fill_from_pm_order(pm_resp2, need)
                if pm_fill2.status == "FILLED":
                    if pm_fill.status == "FILLED" and filled_pm > 0:
                        # combine
                        tot_sh = filled_pm + float(pm_fill2.shares)
                        tot_usd = float(pm_fill.usd or 0) + float(pm_fill2.usd or 0)
                        avg = tot_usd / tot_sh if tot_sh else float(pm_fill2.price or 0)
                        pm_fill = Fill(
                            status="FILLED", shares=round(tot_sh, 4),
                            price=round(avg, 4), usd=round(tot_usd, 4),
                            order_id=pm_fill2.order_id or pm_fill.order_id,
                        )
                    else:
                        pm_fill = pm_fill2
                    filled_pm = float(pm_fill.shares)

        if pm_fill.status == "FILLED" and pf_fill.status != "FILLED":
            await _asyncio.sleep(1.0)
            logger.info("PF limit retry after PM filled")
            if pf_fill.order_id:
                try:
                    await self.pf.cancel_order(str(pf_fill.order_id))
                    logger.info("PF cancel resting %s before retry", pf_fill.order_id)
                except Exception as e:
                    logger.warning("PF cancel before retry: %s", e)
            pf_resp2 = await self.pf.place_order(
                pf_m["id"], "BUY", order_shares, min(0.99, pf_px + 0.02),
                is_yes=(pf_dir == "up"),
            )
            pf_fill2 = fill_from_predict(pf_resp2, order_shares)
            if pf_fill2.status == "SUBMITTED" and pf_fill2.order_id:
                await _asyncio.sleep(2.0)
                try:
                    st = await self.pf.get_order(pf_fill2.order_id)
                    if st:
                        pf_fill2 = fill_from_predict(st, order_shares)
                except Exception:
                    pass
            if pf_fill2.status == "FILLED":
                pf_fill = pf_fill2
                filled_pf = float(pf_fill.shares)

        # Cancel any leftover PM resting order
        if pm_fill.status == "SUBMITTED" and pm_fill.order_id:
            try:
                await self.pm.cancel_order(pm_fill.order_id)
            except Exception:
                pass
            pm_fill = Fill(status="REJECTED", reason="PM not filled after wait", order_id=pm_fill.order_id)

        # Apply fills to legs (may be PARTIAL)
        if pf_fill.status == "FILLED":
            self._apply_fill(hedge.pf_leg, pf_fill)
        elif pf_fill.status == "REJECTED":
            self._apply_fill(hedge.pf_leg, pf_fill)
        if pm_fill.status == "FILLED":
            # Cap displayed PM shares if still slightly over
            if pm_fill.shares > target_net * 1.08:
                logger.warning(
                    "PM still over target (%.4f > %.4f) — recording actual fill (limit path)",
                    pm_fill.shares, target_net,
                )
            self._apply_fill(hedge.pm_leg, pm_fill)
        elif pm_fill.status == "REJECTED":
            self._apply_fill(hedge.pm_leg, pm_fill)

        # Reject / jurisdiction handling for PF
        if pf_fill.status == "REJECTED":
            hedge.status = "CLOSED"
            hedge.closed_at = time.time()
            reason = (pf_fill.reason or "").lower()
            self._entry_busy = False
            logger.warning("PF reject %s — abort pair", pf_fill.reason)
            if "jurisdiction" in reason or "403" in reason or "forbidden" in reason:
                self.pf_entry_cooldown_until = time.time() + 3600.0
                self.last_error = "Predict 403: not available in your jurisdiction"
            elif "insufficient" in reason or "collateral" in reason or "balance" in reason:
                self.pf_entry_cooldown_until = time.time() + 120.0
                self.last_error = "Predict insufficient USDT balance/allowance"
            else:
                self.pf_entry_cooldown_until = time.time() + 8.0
            return

        if pm_fill.status != "FILLED" and pf_fill.status == "FILLED":
            hedge.status = "PARTIAL"
            self._entry_busy = False
            logger.info("Hedge %s PARTIAL PF=FILLED PM=%s — will retry PM in manage loop", hedge.id, pm_fill.status)
            return

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
            # Re-pull real avg/shares from venues at T+30s and T+90s (API lag)
            try:
                import asyncio as _aio
                _aio.create_task(self._delayed_reconcile(hedge, delays=(30.0, 90.0), phase="open"))
            except Exception as e:
                logger.warning("schedule delayed reconcile: %s", e)
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
            # Prefer explicit USD from venue (PM makingAmount + optional taker fee)
            if fill.usd and fill.usd > 0:
                leg.entry_usd = float(fill.usd)
            elif fill.shares > 0 and fill.price > 0:
                leg.entry_usd = round(fill.shares * fill.price, 4)
            # Keep market avg price; do NOT fold fee into entry_price (fee lives in entry_usd only)
            if leg.venue in ("pm", "polymarket") and leg.entry_usd and fill.price and fill.shares:
                pre_fee = float(fill.price) * float(fill.shares)
                if leg.entry_usd > pre_fee * 1.005:
                    logger.info(
                        "PM fill cost $%.4f includes ~$%.4f fee (pre-fee $%.4f @ %.4f)",
                        leg.entry_usd, leg.entry_usd - pre_fee, pre_fee, fill.price,
                    )
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
        """Complete a PARTIAL hedge: only fill the missing leg.

        Critical rules:
        - NEVER place a second PF buy without canceling the previous order.
        - Prefer live position check over placing more orders (404 order status is common).
        - If PF already has shares → mark FILLED, only retry PM.
        - Max retries still apply.
        """
        if hedge.retries >= self.settings.leg_retry_max:
            return
        if now - hedge.last_retry_at < self.settings.leg_retry_sec:
            return
        if now >= hedge.market_end:
            return
        hedge.last_retry_at = now
        hedge.retries += 1

        # --- PF leg: detect fill from position; cancel resting; at most ONE new buy ---
        if hedge.pf_leg.status != "FILLED":
            is_yes = hedge.pf_leg.direction == "up"
            mid = hedge.pf_leg.token_or_market
            need = float(hedge.pf_leg.requested_shares or 0)
            # 1) Live position first (order GET often 404 after fill)
            try:
                try:
                    pos = await self.pf.get_position_for_market(mid, is_yes=is_yes)
                except TypeError:
                    pos = await self.pf.get_position_for_market(mid)
                if pos and float(pos.get("shares") or 0) >= max(0.5, need * 0.5):
                    sh = float(pos["shares"])
                    # Cap to this hedge's requested size (other fills may exist)
                    sh = min(sh, need * 1.05) if need > 0 else sh
                    ap = float(pos.get("avg_price") or 0)
                    cu = float(pos.get("cost_usd") or 0) or (sh * ap if ap else 0)
                    if cu > 0 and sh > 0 and (cu / sh) > 1.5 and cu > 50:
                        cu = cu / 100.0
                    px = (cu / sh) if (cu > 0 and sh > 0) else (ap or 0)
                    if 0.01 <= px <= 1.0:
                        self._apply_fill(
                            hedge.pf_leg,
                            Fill(status="FILLED", shares=round(sh, 4), price=round(px, 4),
                                 usd=round(cu or sh * px, 4), order_id=hedge.pf_leg.order_id),
                        )
                        logger.info(
                            "PF retry: position shows FILLED %.4f@%.4f (no new order)",
                            sh, px,
                        )
            except Exception as e:
                logger.debug("PF position check on retry: %s", e)

            # 2) Still not filled → cancel any resting order, then place ONE new buy
            if hedge.pf_leg.status != "FILLED":
                if hedge.pf_leg.order_id:
                    try:
                        await self.pf.cancel_order(str(hedge.pf_leg.order_id))
                        logger.info("PF cancel before retry buy id=%s", hedge.pf_leg.order_id)
                    except Exception as e:
                        logger.warning("PF cancel before retry: %s", e)
                    hedge.pf_leg.order_id = ""
                    hedge.pf_leg.status = "REJECTED"  # cleared resting

                _, ask = self._pf_best(mid, hedge.pf_leg.direction)
                if ask and need > 0:
                    # Only one replacement order; aggressive limit
                    px = min(0.99, round(float(ask) + 0.02, 2))
                    logger.info("PF retry BUY once size=%.4f @ %.2f (after cancel)", need, px)
                    resp = await self.pf.place_order(
                        mid, "BUY", need, px, is_yes=is_yes,
                    )
                    fill = fill_from_predict(resp, need)
                    self._apply_fill(hedge.pf_leg, fill)
                    # If SUBMITTED, wait briefly and check position — do NOT place another
                    if fill.status == "SUBMITTED":
                        import asyncio as _aio
                        await _aio.sleep(1.5)
                        try:
                            try:
                                pos = await self.pf.get_position_for_market(mid, is_yes=is_yes)
                            except TypeError:
                                pos = await self.pf.get_position_for_market(mid)
                            if pos and float(pos.get("shares") or 0) >= max(0.5, need * 0.5):
                                sh = min(float(pos["shares"]), need * 1.05)
                                ap = float(pos.get("avg_price") or px)
                                cu = float(pos.get("cost_usd") or 0) or sh * ap
                                self._apply_fill(
                                    hedge.pf_leg,
                                    Fill(status="FILLED", shares=round(sh, 4),
                                         price=round(ap if 0.01 <= ap <= 1 else px, 4),
                                         usd=round(cu, 4), order_id=fill.order_id),
                                )
                        except Exception:
                            pass

        # --- PM leg: only if PF is filled and PM empty ---
        if hedge.pm_leg.status != "FILLED" and hedge.pf_leg.status == "FILLED":
            if (hedge.pm_leg.filled_shares or 0) > 0.5:
                logger.info("PM already has %.2f filled — skip retry buy", hedge.pm_leg.filled_shares)
            else:
                _, ask = self._pm_best(hedge.pm_leg.direction)
                size = float(hedge.pf_leg.filled_shares or hedge.pf_leg.requested_shares or 0)
                if ask and size > 0:
                    # Do NOT chase a worse PM price after PF already filled.
                    # Abort the pair if combined entry would exceed ENTRY_SUM_MAX + small slack.
                    pf_px = float(hedge.pf_leg.entry_price or 0)
                    projected = pf_px + float(ask)
                    max_ok = float(self.settings.entry_sum_max) + 0.03  # 3¢ slack for fee/slip
                    if pf_px > 0 and projected > max_ok:
                        logger.warning(
                            "Hedge %s ABORT PM retry: projected entry_sum=%.3f "
                            "(pf@%.3f + pm_ask@%.3f) > max %.3f — leave PARTIAL, no chase",
                            hedge.id, projected, pf_px, float(ask), max_ok,
                        )
                        hedge.pm_leg.status = "REJECTED"
                        hedge.pm_leg.reject_reason = (
                            f"retry ask too high ({projected:.3f} > {max_ok:.3f})"
                        )
                        # Mark closed as aborted loss on PF only so recovery sees real PF loss
                        # but do NOT promote to OPEN with bad sum.
                        return
                    if hedge.pm_leg.order_id and hedge.pm_leg.status == "SUBMITTED":
                        try:
                            await self.pm.cancel_order(hedge.pm_leg.order_id)
                            logger.info("PM cancel before retry %s", hedge.pm_leg.order_id[:18])
                        except Exception as e:
                            logger.warning("PM cancel before retry: %s", e)
                    resp = await self.pm.place_market_buy(
                        hedge.pm_leg.token_or_market, size, float(ask),
                    )
                    fill = fill_from_pm_order(resp, size)
                    if fill.status == "SUBMITTED" and fill.order_id:
                        try:
                            await self.pm.cancel_order(fill.order_id)
                        except Exception:
                            pass
                        fill = Fill(status="REJECTED", reason="PM resting on retry", order_id=fill.order_id)
                    self._apply_fill(hedge.pm_leg, fill)

        if hedge.pf_leg.status == "FILLED" and hedge.pm_leg.status == "FILLED":
            self._refresh_entry_sum(hedge)
            max_ok = float(self.settings.entry_sum_max) + 0.05
            if hedge.entry_sum > max_ok:
                logger.warning(
                    "Hedge %s NOT promoting to OPEN: entry_sum=%.3f > %.3f "
                    "(PF %.2f@%.3f + PM %.2f@%.3f) — bad fill, keep PARTIAL",
                    hedge.id, hedge.entry_sum, max_ok,
                    hedge.pf_leg.filled_shares, hedge.pf_leg.entry_price,
                    hedge.pm_leg.filled_shares, hedge.pm_leg.entry_price,
                )
                return
            hedge.status = "OPEN"
            logger.info(
                "Hedge %s promoted PARTIAL→OPEN entry_sum=%.3f PF %.2f@%.3f PM %.2f@%.3f",
                hedge.id, hedge.entry_sum,
                hedge.pf_leg.filled_shares, hedge.pf_leg.entry_price,
                hedge.pm_leg.filled_shares, hedge.pm_leg.entry_price,
            )
            try:
                import asyncio as _aio
                _aio.create_task(self._delayed_reconcile(hedge, delays=(30.0, 90.0), phase="open"))
            except Exception as e:
                logger.warning("schedule delayed reconcile: %s", e)


    async def _delayed_reconcile(self, hedge: Hedge, delays: tuple = (30.0, 90.0), phase: str = "open") -> None:
        """After OPEN/CLOSE, re-fetch venue positions so UI avg/shares match reality."""
        import asyncio as _aio
        for d in delays:
            try:
                await _aio.sleep(d)
            except Exception:
                return
            if hedge.status in ("CLOSED",) and phase == "open":
                # still ok to refresh closed legs for history accuracy
                pass
            if hedge.status not in ("OPEN", "PARTIAL", "CLOSING", "SETTLING", "CLOSED"):
                return
            try:
                await self._reconcile_hedge_from_venues(hedge, reason=f"{phase}+{int(d)}s")
            except Exception as e:
                logger.warning("delayed reconcile %s +%ss: %s", hedge.id, int(d), e)

    async def _reconcile_hedge_from_venues(self, hedge: Hedge, reason: str = "") -> None:
        """Pull PF position cost + shares; keep PM fill as recorded (CLOB has no easy position avg)."""
        if not self.settings.is_live:
            return
        changed = False
        # Predict.fun
        try:
            is_yes = hedge.pf_leg.direction == "up"
            mid = hedge.pf_leg.token_or_market
            try:
                pos = await self.pf.get_position_for_market(mid, is_yes=is_yes)
            except TypeError:
                pos = await self.pf.get_position_for_market(mid)
            if pos and float(pos.get("shares") or 0) > 0:
                sh = float(pos["shares"])
                cu = float(pos.get("cost_usd") or 0)
                ap = float(pos.get("avg_price") or 0)
                if cu <= 0 and ap > 0:
                    cu = sh * ap
                old_sh = float(hedge.pf_leg.filled_shares or 0)
                # Only update PRICE from venue avg; never expand size past this hedge's fill.
                # Position total can include other fills on the same market.
                if old_sh > 0 and sh > old_sh * 1.08 and hedge.status in ("OPEN", "PARTIAL", "CLOSING"):
                    logger.info(
                        "PF re-reconcile [%s] ignore position size %.4f > tracked %.4f (use avg only)",
                        reason, sh, old_sh,
                    )
                    if 0.01 <= (ap or (cu / sh if sh else 0)) <= 1.0:
                        new_px = ap if 0.01 <= ap <= 1.0 else (cu / sh)
                        old_px = float(hedge.pf_leg.entry_price or 0)
                        if abs(old_px - new_px) > 0.01:
                            hedge.pf_leg.entry_price = round(new_px, 4)
                            hedge.pf_leg.entry_usd = round(old_sh * new_px, 4)
                            changed = True
                            logger.info(
                                "PF re-reconcile [%s] avg only %.4f→%.4f (shares stay %.4f)",
                                reason, old_px, new_px, old_sh,
                            )
                elif cu > 0 and sh > 0:
                    new_px = cu / sh
                    if 0.01 <= new_px <= 1.0:
                        old_px = float(hedge.pf_leg.entry_price or 0)
                        # Allow shrink (sold partially) or tiny growth
                        if old_sh <= 0 or sh <= old_sh * 1.08:
                            hedge.pf_leg.filled_shares = sh
                            hedge.pf_leg.shares = sh
                            hedge.pf_leg.entry_usd = cu if sh <= old_sh * 1.08 else round(old_sh * new_px, 4)
                            if sh > old_sh * 1.08:
                                hedge.pf_leg.filled_shares = old_sh
                                hedge.pf_leg.shares = old_sh
                            hedge.pf_leg.entry_price = round(new_px, 4)
                            if abs(old_px - new_px) > 0.005 or abs(old_sh - hedge.pf_leg.filled_shares) > 0.05:
                                changed = True
                                logger.info(
                                    "PF re-reconcile [%s] shares=%.4f→%.4f avg=%.4f→%.4f usd=%.4f",
                                    reason, old_sh, hedge.pf_leg.filled_shares, old_px, new_px,
                                    hedge.pf_leg.entry_usd,
                                )
            # Activity fallback if still looks like limit
            if hedge.pf_leg.entry_price and hedge.pf_leg.entry_price > 0:
                act = await self.pf.get_fill_price_from_activity(
                    mid, order_id=hedge.pf_leg.order_id or "", limit_price=float(hedge.pf_leg.entry_price or 0),
                )
                if act:
                    cu = float(act.get("cost_usd") or 0)
                    ap = float(act.get("avg_price") or 0)
                    ash = float(act.get("shares") or 0) or float(hedge.pf_leg.filled_shares or 0)
                    if cu > 0 and ash > 0:
                        new_px = cu / ash
                        tracked = float(hedge.pf_leg.filled_shares or 0)
                        if 0.01 <= new_px <= 1.0 and (
                            hedge.pf_leg.entry_price <= 0
                            or new_px < float(hedge.pf_leg.entry_price) * 0.98
                            or abs(new_px - float(hedge.pf_leg.entry_price or 0)) > 0.02
                        ):
                            old_px = float(hedge.pf_leg.entry_price or 0)
                            # Activity for this order: use ash only if close to tracked
                            if tracked <= 0 or ash <= tracked * 1.08:
                                hedge.pf_leg.filled_shares = ash
                                hedge.pf_leg.shares = ash
                                hedge.pf_leg.entry_usd = cu
                            else:
                                hedge.pf_leg.entry_usd = round(tracked * new_px, 4)
                            hedge.pf_leg.entry_price = round(new_px, 4)
                            changed = True
                            logger.info(
                                "PF activity re-reconcile [%s] avg=%.4f→%.4f usd=%.4f sh=%.4f",
                                reason, old_px, new_px, hedge.pf_leg.entry_usd, hedge.pf_leg.filled_shares,
                            )
        except Exception as e:
            logger.warning("PF re-reconcile [%s]: %s", reason, e)

        if changed:
            self._refresh_entry_sum(hedge)
            logger.info(
                "Hedge %s entry_sum refreshed → %.3f (PF %.4f + PM %.4f) [%s]",
                hedge.id, hedge.entry_sum,
                hedge.pf_leg.entry_price, hedge.pm_leg.entry_price, reason,
            )

    async def _try_exit(self, hedge: Hedge, now: float) -> None:
        """Early exit when sum of bids >= exit_sum_min AND beats entry (sell both).

        Exit prices are live bids in (0,1), NOT 0/1 settlement.
        """
        # Don't early-exit in last 20s — let settlement handle it
        if now >= hedge.market_end - 20:
            return
        # Min hold 25s — avoid EXIT 1s after fill on noisy books / partial promote
        age = now - float(hedge.opened_at or now)
        if age < 25.0:
            return
        if hedge.status != "OPEN":
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
        # Must improve on actual entry — otherwise we lock a worse loss than holding to settle
        entry = float(hedge.entry_sum or 0)
        if entry > 0 and s < entry + 0.02:
            return
        logger.info(
            "EXIT signal hedge=%s sum=%.3f entry=%.3f pf_bid=%.3f pm_bid=%.3f (market sell)",
            hedge.id, s, entry, pf_bid, pm_bid,
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
        if leg.status == "CLOSED":
            return
        if leg.filled_shares <= 0 and leg.venue != "predict":
            leg.status = "CLOSED"
            return

        size = float(leg.filled_shares)

        if leg.venue == "predict":
            is_yes = leg.direction == "up"
            # Prefer live balance from positions API (exact sellable amount)
            try:
                pos = await self.pf.get_position_for_market(leg.token_or_market, is_yes=is_yes)
                if pos and pos.get("shares") is not None:
                    live = float(pos["shares"])
                    if live > 0:
                        size = live
                        logger.info(
                            "%s sell using live position shares=%.4f (tracked was %.4f)",
                            label, live, leg.filled_shares,
                        )
            except Exception as e:
                logger.debug("%s position lookup for sell: %s", label, e)

            # Floor to 4dp and haircut 3% — PF balance lags + fee dust often rejects 99%
            size = max(0.0, int(size * 0.97 * 10000) / 10000.0)

            # Dust below min notional (~$1 at typical prices) — cannot sell, mark closed
            bid, _ = self._pf_best(leg.token_or_market, leg.direction)
            est_px = float(bid or leg.entry_price or 0.5)
            if size <= 0.0 or size * est_px < 0.50:
                logger.info(
                    "%s sell skip dust shares=%.4f value~$%.2f — mark CLOSED",
                    label, size, size * est_px,
                )
                leg.status = "CLOSED"
                if not leg.exit_price:
                    leg.exit_price = est_px if size > 0 else 0.0
                    leg.exit_usd = size * leg.exit_price
                return

            # Aggressive limit under best bid
            px = max(0.01, round(float(bid) - 0.02, 2)) if bid else max(0.01, leg.entry_price * 0.9)
            resp = await self.pf.place_order(
                leg.token_or_market,
                "SELL",
                size,
                px,
                is_yes=is_yes,
            )
            fill = fill_from_predict(resp, size)

            if fill.status == "FILLED":
                leg.exit_price = fill.price
                leg.exit_usd = fill.usd
                # Re-check residual dust after fill
                try:
                    await asyncio.sleep(0.4)
                    pos2 = await self.pf.get_position_for_market(leg.token_or_market, is_yes=is_yes)
                    left = float((pos2 or {}).get("shares") or 0)
                except Exception:
                    left = 0.0
                if left > 0.05 and left * (bid or 0.5) >= 0.50:
                    logger.info("%s residual %.4f after sell — second pass", label, left)
                    size2 = max(0.0, int(left * 10000) / 10000.0)
                    resp2 = await self.pf.place_order(
                        leg.token_or_market, "SELL", size2, px, is_yes=is_yes,
                    )
                    fill2 = fill_from_predict(resp2, size2)
                    if fill2.status == "FILLED":
                        # Weighted exit
                        total_sh = (leg.exit_usd / fill.price if fill.price else size) + size2
                        total_usd = leg.exit_usd + fill2.usd
                        leg.exit_usd = total_usd
                        leg.exit_price = (total_usd / total_sh) if total_sh > 0 else fill.price
                        left = 0.0
                if left > 0.001:
                    logger.warning(
                        "%s leftover dust %.4f after sell (value ~$%.2f) — mark CLOSED",
                        label, left, left * (bid or 0.5),
                    )
                leg.status = "CLOSED"
            elif fill.status == "REJECTED":
                reason = (fill.reason or "").lower()
                leg.reject_reason = fill.reason or ""
                logger.warning("%s sell rejected: %s", label, fill.reason)
                if "insufficient" in reason and "share" in reason:
                    # Re-read live balance — concurrent sell may have already filled
                    try:
                        is_yes = leg.direction == "up"
                        pos = await self.pf.get_position_for_market(leg.token_or_market, is_yes=is_yes)
                        live = float((pos or {}).get("shares") or 0)
                    except Exception:
                        live = 0.0
                    if live <= 0.05:
                        # Position gone → treat sell as done (order likely filled despite error)
                        leg.status = "CLOSED"
                        if not leg.exit_price:
                            bid, _ = self._pf_best(leg.token_or_market, leg.direction)
                            leg.exit_price = float(bid or leg.entry_price or 0)
                            leg.exit_usd = float(leg.filled_shares or 0) * leg.exit_price
                        logger.info(
                            "%s sell: insufficient but live=0 — mark CLOSED (likely filled)",
                            label,
                        )
                    else:
                        # Shrink to 95% of live for next attempt
                        leg.filled_shares = max(0.0, int(live * 0.95 * 10000) / 10000.0)
                        logger.info(
                            "%s sell: insufficient — live=%.4f shrink tracked to %.4f",
                            label, live, leg.filled_shares,
                        )
            return

        # Polymarket
        bid, _ = self._pm_best(leg.direction)
        px = bid or max(0.01, leg.entry_price * 0.9)
        if size <= 0:
            leg.status = "CLOSED"
            return
        resp = await self.pm.place_market_sell(leg.token_or_market, size, px)
        fill = fill_from_pm_order(resp, size, side="SELL")
        if fill.status == "FILLED":
            leg.status = "CLOSED"
            leg.exit_price = min(1.0, max(0.0, float(fill.price or 0)))
            leg.exit_usd = fill.usd
        elif fill.status == "REJECTED":
            reason = (fill.reason or "").lower()
            leg.reject_reason = fill.reason or ""
            logger.warning("%s sell rejected: %s", label, fill.reason)
            if "insufficient" in reason or "not enough balance" in reason:
                # Chain lag right after buy is common — retry later, do not fake-close
                logger.info("%s sell: balance lag — will retry (not marking CLOSED)", label)

    async def _finish_exit(self, hedge: Hedge, now: float) -> None:
        # Throttle sell attempts (avoid 1Hz spam when market closed)
        last = float(getattr(hedge, "last_exit_try_at", 0) or 0)
        # Back off as tries grow (3s → 8s) to avoid order spam
        gap = 3.0 if int(getattr(hedge, "exit_tries", 0) or 0) < 3 else 8.0
        if now - last < gap:
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
            # Log once every ~30s instead of every 3s
            if hedge.exit_tries == 8 or hedge.exit_tries % 10 == 0:
                logger.warning(
                    "Hedge %s exit tries=%d — hold to SETTLE (PF=%s PM=%s)",
                    hedge.id, hedge.exit_tries,
                    hedge.pf_leg.status, hedge.pm_leg.status,
                )
            # If PF already closed/dust and PM still open, keep trying PM only less often
            if hedge.pf_leg.status == "CLOSED" and hedge.pm_leg.status != "CLOSED":
                if hedge.exit_tries % 3 != 0:
                    return
            else:
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
            # Binary market prices must stay in [0, 1]
            for leg in (hedge.pf_leg, hedge.pm_leg):
                if leg.exit_price is not None:
                    leg.exit_price = min(1.0, max(0.0, float(leg.exit_price)))
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

    def _refresh_entry_sum(self, hedge: Hedge) -> None:
        """Recompute entry_sum from real leg entry_prices after fills/reconcile."""
        pf_px = float(hedge.pf_leg.entry_price or 0)
        pm_px = float(hedge.pm_leg.entry_price or 0)
        if pf_px > 0 and pm_px > 0:
            old = hedge.entry_sum
            hedge.entry_sum = round(pf_px + pm_px, 4)
            if abs(old - hedge.entry_sum) > 1e-4:
                logger.info(
                    "Hedge %s entry_sum %.3f → %.3f (pf@%.3f + pm@%.3f)",
                    hedge.id, old, hedge.entry_sum, pf_px, pm_px,
                )

    async def _settle_pair(self, hedge: Hedge, now: float) -> None:
        """Settle each leg from **venue resolution**, not mid-price guess.

        PF and PM can resolve differently (different oracles). We wait for
        official resolution; if still pending after 90s, fall back to quotes.
        """
        if now < hedge.market_end:
            return

        wait_s = now - float(hedge.market_end or now)
        pf_up_won: bool | None = None
        pm_up_won: bool | None = None
        # Never settle in the first 45s — Gamma/UI often still "determining winner"
        MIN_WAIT_S = 45.0

        # --- Predict.fun resolution ---
        try:
            mid = str(getattr(hedge, "pf_market_id", "") or hedge.pf_leg.token_or_market)
            pf_up_won = await self.pf.market_up_won(mid)
            logger.info(
                "PF settle check hedge=%s market=%s → pf_up_won=%s",
                hedge.id, mid, pf_up_won,
            )
        except Exception as e:
            logger.warning("PF resolution lookup failed: %s", e)

        # --- Polymarket Gamma resolution (ALWAYS the slug from open, not current window) ---
        try:
            slug = str(getattr(hedge, "pm_slug", "") or "")
            if not slug and self.pm_market:
                # last resort only if hedge opened before slug field existed
                slug = str(self.pm_market.get("slug") or "")
            if slug:
                up_px = await self.pm.resolved_leg_price(slug, "up")
                # resolved_leg_price only returns a value when Gamma prices are
                # extreme (0/1). Mid-market quotes return None → keep waiting.
                if up_px is not None:
                    if up_px >= 0.99:
                        pm_up_won = True
                    elif up_px <= 0.01:
                        pm_up_won = False
                    else:
                        pm_up_won = None  # should not happen with strict parser
                logger.info(
                    "PM settle check hedge=%s slug=%s up_px=%s → pm_up_won=%s",
                    hedge.id, slug, up_px, pm_up_won,
                )
        except Exception as e:
            logger.warning("PM resolution lookup failed: %s", e)

        # Not ready yet — wait up to 4 min (Gamma often lags 30–180s).
        # Also enforce MIN_WAIT even when both venues already report.
        if (pf_up_won is None or pm_up_won is None or wait_s < MIN_WAIT_S) and wait_s < 240:
            # Throttle logs + gamma hammering to every 5s
            last_log = float(getattr(hedge, "_last_res_log", 0) or 0)
            if now - last_log >= 5.0:
                hedge._last_res_log = now
                logger.info(
                    "Hedge %s waiting resolution (t+%.0fs) pf=%s pm=%s slug=%s",
                    hedge.id, wait_s,
                    "Up" if pf_up_won else ("Down" if pf_up_won is False else "?"),
                    "Up" if pm_up_won else ("Down" if pm_up_won is False else "?"),
                    getattr(hedge, "pm_slug", "") or "?",
                )
            return

        # Both legs track the SAME BTC 5m event. True venue disagreement is
        # extremely rare; almost always Gamma lag / wrong intermediate prices.
        # NEVER settle opposite outcomes early — that zeros both legs of a hedge.

        # Divergence: wait extra before forcing agreement (Gamma often flips late)
        if (
            pf_up_won is not None
            and pm_up_won is not None
            and pf_up_won != pm_up_won
            and wait_s < 180
        ):
            last_log = float(getattr(hedge, "_last_res_log", 0) or 0)
            if now - last_log >= 5.0:
                hedge._last_res_log = now
                logger.warning(
                    "Hedge %s VENUE DIVERGENCE pf_up=%s pm_up=%s — waiting agreement "
                    "(t+%.0fs/180s) slug=%s",
                    hedge.id, pf_up_won, pm_up_won, wait_s,
                    getattr(hedge, "pm_slug", "") or "?",
                )
            return

        # Timeout / still missing / still diverge → single source of truth
        if pf_up_won is None or pm_up_won is None or pf_up_won != pm_up_won:
            slug = str(getattr(hedge, "pm_slug", "") or "")
            if pm_up_won is None and slug:
                try:
                    up_px = await self.pm.resolved_leg_price(slug, "up")
                    logger.info("PM final resolution slug=%s up_px=%s", slug, up_px)
                    if up_px is not None:
                        if up_px >= 0.95:
                            pm_up_won = True
                        elif up_px <= 0.05:
                            pm_up_won = False
                except Exception as e:
                    logger.warning("PM final resolution error: %s", e)

            # After long wait: fill missing side from the known one; if both known
            # and still diverge, KEEP independent outcomes (true venue split exists).
            if pf_up_won is None and pm_up_won is not None:
                pf_up_won = pm_up_won
                logger.warning(
                    "Hedge %s PF unknown after wait — using PM pm_up=%s for both",
                    hedge.id, pm_up_won,
                )
            elif pm_up_won is None and pf_up_won is not None:
                pm_up_won = pf_up_won
                logger.warning(
                    "Hedge %s PM unknown after wait — using PF pf_up=%s for both",
                    hedge.id, pf_up_won,
                )
            elif pf_up_won is None and pm_up_won is None:
                # last resort: no data at all
                pf_up_won = True
                pm_up_won = True
                logger.warning(
                    "Hedge %s NO resolution data after wait — fallback Up/Up",
                    hedge.id,
                )
            elif pf_up_won != pm_up_won:
                logger.warning(
                    "Hedge %s TRUE DIVERGENCE after wait pf_up=%s pm_up=%s — "
                    "settling each leg on its own venue outcome",
                    hedge.id, pf_up_won, pm_up_won,
                )
            logger.info(
                "Hedge %s resolution final pf_up=%s pm_up=%s (slug=%s t+%.0fs)",
                hedge.id, pf_up_won, pm_up_won, slug, wait_s,
            )

        # Final entry reconcile from positions (avg + shares) before PnL
        self._refresh_entry_sum(hedge)
        if self.settings.is_live:
            try:
                is_yes = hedge.pf_leg.direction == "up"
                try:
                    pos = await self.pf.get_position_for_market(
                        hedge.pf_leg.token_or_market, is_yes=is_yes,
                    )
                except TypeError:
                    pos = await self.pf.get_position_for_market(hedge.pf_leg.token_or_market)
                if pos and pos.get("shares", 0) > 0:
                    sh = float(pos["shares"])
                    ap = float(pos.get("avg_price") or 0)
                    cu = float(pos.get("cost_usd") or 0)
                    if cu <= 0 and ap > 0:
                        cu = sh * ap
                    tracked = float(hedge.pf_leg.filled_shares or 0)
                    if sh > 0 and (ap > 0 or cu > 0):
                        old_usd = float(hedge.pf_leg.entry_usd or 0)
                        # Price/avg only — never expand shares beyond this hedge
                        if cu > 0 and tracked > 0 and sh > tracked * 1.08:
                            new_px = cu / sh
                            if 0.01 <= new_px <= 1.0:
                                hedge.pf_leg.entry_price = new_px
                                hedge.pf_leg.entry_usd = tracked * new_px
                        elif cu > 0 and (old_usd <= 0 or abs(cu - old_usd) / max(old_usd, 1e-9) > 0.05):
                            if tracked <= 0 or sh <= tracked * 1.08:
                                hedge.pf_leg.filled_shares = sh
                                hedge.pf_leg.shares = sh
                                hedge.pf_leg.entry_usd = cu
                                hedge.pf_leg.entry_price = cu / sh
                        elif ap > 0 and old_usd <= 0:
                            hedge.pf_leg.entry_price = ap
                            hedge.pf_leg.entry_usd = (tracked or sh) * ap
                        logger.info(
                            "PF settle-reconcile shares=%.4f avg=%.4f usd=%.4f tracked=%.4f",
                            hedge.pf_leg.filled_shares, hedge.pf_leg.entry_price,
                            hedge.pf_leg.entry_usd, tracked,
                        )
            except Exception as e:
                logger.warning("PF settle-reconcile failed: %s", e)

        for leg in (hedge.pf_leg, hedge.pm_leg):
            if leg.status == "CLOSED" and leg.exit_price is not None:
                if abs(leg.exit_price - leg.entry_price) > 1e-9 or hedge.close_reason == "EXIT":
                    if hedge.close_reason == "EXIT":
                        continue
            if (leg.filled_shares or 0) <= 0:
                leg.status = "CLOSED"
                leg.exit_price = leg.exit_price or 0.0
                continue
            # Per-venue outcome
            won_up = pf_up_won if leg.venue in ("predict", "pf", "bn") else pm_up_won
            if leg.direction == "up":
                leg.exit_price = 1.0 if won_up else 0.0
            else:
                leg.exit_price = 0.0 if won_up else 1.0
            leg.exit_usd = float(leg.exit_price) * float(leg.filled_shares or 0)
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
            "Hedge %s SETTLE pf_up=%s pm_up=%s pf=%s@%.3f→%.3f($%.2f) pm=%s@%.3f→%.3f($%.2f) pnl=%.4f",
            hedge.id,
            pf_up_won,
            pm_up_won,
            hedge.pf_leg.direction,
            hedge.pf_leg.entry_price,
            hedge.pf_leg.exit_price,
            pf_pnl,
            hedge.pm_leg.direction,
            hedge.pm_leg.entry_price,
            hedge.pm_leg.exit_price,
            pm_pnl,
            hedge.pnl,
        )

        try:
            import asyncio as _aio
            _aio.create_task(self._delayed_reconcile(hedge, delays=(30.0, 90.0), phase="close"))
        except Exception:
            pass


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
