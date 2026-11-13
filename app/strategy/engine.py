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
        # PF: aggressive limit (ask+slip). PM: GTC resting limit at ask (not FAK —
        # FAK fails on thin books and left PF naked). Wait ~7s for fills.
        import asyncio as _asyncio

        # Same share count on both venues. Do NOT inflate for fee — that caused
        # confusion and double-fill tracking (6 vs 12). Fee is paid in cash/shares
        # by the venue; we track actual fill after the fact.
        order_shares = round(float(shares), 4)
        target_net = float(shares)
        pf_px = min(0.99, round(float(pf_ask) + 0.03, 2))
        pm_px = min(0.99, round(float(pm_ask) + 0.01, 2))  # 1¢ slip to cross

        logger.info(
            "LIMIT ENTRY simultaneous target_net=%.2f PF order=%.4f @ %.2f | PM size=%.4f @ %.2f",
            target_net, order_shares, pf_px, target_net, pm_px,
        )

        # Pre-check: if this market already has our side filled (stale leftover or
        # prior partial), do NOT place another full size buy.
        pre_live = 0.0
        try:
            try:
                pre_pos = await self.pf.get_position_for_market(pf_m["id"], is_yes=(pf_dir == "up"))
            except TypeError:
                pre_pos = await self.pf.get_position_for_market(pf_m["id"])
            if pre_pos:
                pre_live = float(pre_pos.get("shares") or 0)
                if pre_live >= target_net * 0.85:
                    logger.warning(
                        "PF pre-check: market %s already has %.4f shares on %s — "
                        "will NOT place new buy (prevent double-fill)",
                        pf_m["id"], pre_live, pf_dir,
                    )
        except Exception as e:
            logger.debug("PF pre-check: %s", e)

        async def _pf_buy():
            if pre_live >= target_net * 0.85:
                # Fake a filled response from existing position
                return {
                    "status": "FILLED",
                    "filledShares": min(pre_live, order_shares * 1.08),
                    "avgPrice": float((pre_pos or {}).get("avg_price") or pf_px),
                    "orderId": "pre-existing",
                }
            return await self.pf.place_order(
                pf_m["id"], "BUY", order_shares, pf_px, is_yes=(pf_dir == "up"),
            )

        async def _pm_buy(size: float, px: float):
            return await self.pm.place_market_buy(str(pm_token), size, px)

        # Use pm_px (ask+1¢) so GTC is more likely to match immediately
        pf_resp, pm_resp = await _asyncio.gather(_pf_buy(), _pm_buy(target_net, float(pm_px)))
        pf_fill = fill_from_predict(pf_resp, order_shares)
        pm_fill = fill_from_pm_order(pm_resp, target_net)

        # Wait for SUBMITTED → FILLED. Longer window so GTC can match.
        wait_deadline = time.time() + 15.0
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
                try:
                    st = await self.pm.get_order(pm_fill.order_id)
                    if st:
                        got = fill_from_pm_order(st, target_net)
                        if got.status == "FILLED" and got.shares > 0.5:
                            pm_fill = got
                            logger.info(
                                "PM GTC filled during wait %.4f@%.4f",
                                got.shares, got.price or 0,
                            )
                except Exception:
                    pass
            if pf_fill.status == "FILLED" and pm_fill.status == "FILLED":
                break
            if pf_fill.status == "FILLED" and pm_fill.status == "REJECTED":
                break
            if not need_wait and pf_fill.status != "SUBMITTED" and pm_fill.status != "SUBMITTED":
                break
            await _asyncio.sleep(0.7)

        # After wait: if PM still resting, cancel — but then re-check for partial fill
        # (cancel races with match; first GTC may still fill shares).
        if pm_fill.status == "SUBMITTED" and pm_fill.order_id:
            try:
                st = await self.pm.get_order(pm_fill.order_id)
                if st:
                    got = fill_from_pm_order(st, target_net)
                    if got.status == "FILLED" and got.shares > 0.5:
                        pm_fill = got
                        logger.info(
                            "PM GTC filled at end of wait %.4f@%.4f",
                            got.shares, got.price or 0,
                        )
            except Exception as e:
                logger.debug("PM get_order end-wait: %s", e)
            if pm_fill.status != "FILLED":
                cancelled = False
                try:
                    cancelled = await self.pm.cancel_order(pm_fill.order_id)
                    logger.info(
                        "PM GTC cancel after wait id=%s ok=%s",
                        str(pm_fill.order_id)[:18], cancelled,
                    )
                except Exception as e:
                    logger.warning("PM cancel after wait: %s", e)
                # Race / lag: poll fill again regardless of cancel result
                await _asyncio.sleep(1.0)
                try:
                    st = await self.pm.get_order(pm_fill.order_id)
                    if st:
                        got = fill_from_pm_order(st, target_net)
                        if got.status == "FILLED" and got.shares > 0.5:
                            pm_fill = got
                            logger.info(
                                "PM GTC filled after cancel race %.4f@%.4f",
                                got.shares, got.price or 0,
                            )
                        elif got.status == "SUBMITTED" and not cancelled:
                            # Order still resting — do NOT mark REJECTED; keep SUBMITTED
                            # so manage loop waits instead of placing a second order
                            # that would hit "not enough balance (active orders)".
                            pm_fill = Fill(
                                status="SUBMITTED",
                                shares=0,
                                price=pm_fill.price,
                                order_id=pm_fill.order_id,
                                reason="still resting after cancel fail",
                            )
                            logger.warning(
                                "PM GTC still LIVE after cancel fail id=%s — keep tracking",
                                str(pm_fill.order_id)[:18],
                            )
                except Exception:
                    pass
                if pm_fill.status not in ("FILLED", "SUBMITTED"):
                    try:
                        bal = await self.pm.get_token_balance(str(pm_token))
                        if bal >= target_net * 0.5:
                            pm_fill = Fill(
                                status="FILLED",
                                shares=min(bal, target_net * 1.08),
                                price=float(pm_ask) or 0.5,
                                usd=min(bal, target_net) * float(pm_ask or 0.5),
                                order_id=pm_fill.order_id,
                            )
                            logger.info("PM fill from token balance %.4f", bal)
                    except Exception:
                        pass
                if pm_fill.status not in ("FILLED", "SUBMITTED"):
                    # Must really kill the resting GTC so it cannot fill later
                    try:
                        ok = await self.pm.cancel_order(pm_fill.order_id)
                        if not ok:
                            await self.pm.cancel_all_orders()
                        else:
                            # belt-and-suspenders: clear any sibling GTCs from retries
                            await self.pm.cancel_all_orders()
                        logger.info(
                            "PM force-clear after REJECTED id=%s cancel_ok=%s",
                            str(pm_fill.order_id)[:18], ok,
                        )
                    except Exception as e:
                        logger.warning("PM force-clear after REJECTED: %s", e)
                        try:
                            await self.pm.cancel_all_orders()
                        except Exception:
                            pass
                    pm_fill = Fill(
                        status="REJECTED",
                        reason="GTC unfilled after wait",
                        order_id=pm_fill.order_id,
                        price=pm_fill.price,
                    )

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
            # If original GTC still resting, wait on it instead of placing a 2nd order
            # (2nd order hits "not enough balance / sum of active orders").
            if pm_fill.status == "SUBMITTED" and pm_fill.order_id:
                logger.info(
                    "PM still SUBMITTED id=%s — wait more instead of 2nd order",
                    str(pm_fill.order_id)[:18],
                )
                for _ in range(10):  # ~7s more
                    await _asyncio.sleep(0.7)
                    try:
                        st = await self.pm.get_order(pm_fill.order_id)
                        if st:
                            got = fill_from_pm_order(st, target_net)
                            if got.status == "FILLED" and got.shares > 0.5:
                                pm_fill = got
                                filled_pm = float(pm_fill.shares)
                                logger.info(
                                    "PM GTC filled on extended wait %.4f@%.4f",
                                    got.shares, got.price or 0,
                                )
                                break
                    except Exception:
                        pass
                if pm_fill.status != "FILLED":
                    cancelled = await self.pm.cancel_order(pm_fill.order_id)
                    await _asyncio.sleep(0.8)
                    try:
                        st = await self.pm.get_order(pm_fill.order_id)
                        if st:
                            got = fill_from_pm_order(st, target_net)
                            if got.status == "FILLED" and got.shares > 0.5:
                                pm_fill = got
                                filled_pm = float(pm_fill.shares)
                    except Exception:
                        pass
                    if pm_fill.status != "FILLED" and cancelled:
                        pm_fill = Fill(
                            status="REJECTED",
                            reason="GTC unfilled after extended wait",
                            order_id=pm_fill.order_id,
                            price=pm_fill.price,
                        )
                    need = match_size - (filled_pm if pm_fill.status == "FILLED" else 0.0)
            if need >= 0.5 and pm_fill.status != "FILLED" and pm_fill.status != "SUBMITTED":
                logger.info("PM limit retry need=%.4f @ ask=%.4f", need, pm_ask)
                if pm_fill.order_id:
                    try:
                        ok = await self.pm.cancel_order(pm_fill.order_id)
                        if not ok:
                            await self.pm.cancel_all_orders()
                            await _asyncio.sleep(0.5)
                    except Exception:
                        pass
                pm_resp2 = await self.pm.place_market_buy(
                    str(pm_token), need, min(0.99, float(pm_ask) + 0.01),
                )
                # Balance locked by leftover order?
                if (
                    isinstance(pm_resp2, dict)
                    and "active orders" in str(pm_resp2.get("reason") or pm_resp2.get("error") or "").lower()
                ):
                    logger.warning("PM retry hit active-orders lock — cancel_all + one more try")
                    await self.pm.cancel_all_orders()
                    await _asyncio.sleep(0.8)
                    pm_resp2 = await self.pm.place_market_buy(
                        str(pm_token), need, min(0.99, float(pm_ask) + 0.01),
                    )
                pm_fill2 = fill_from_pm_order(pm_resp2, need)
                if pm_fill2.status == "FILLED":
                    if pm_fill.status == "FILLED" and filled_pm > 0:
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
                elif pm_fill2.status == "SUBMITTED":
                    pm_fill = pm_fill2  # track resting; manage loop continues

        if pm_fill.status == "FILLED" and pf_fill.status != "FILLED":
            # First PF order may already be matching on-chain while GET /orders → 404.
            # Poll position several times BEFORE any cancel/retry to avoid double-buy.
            logger.info("PF limit retry after PM filled — polling live position first")
            live_sh = 0.0
            live_avg = 0.0
            live_usd = 0.0

            async def _poll_pf_live(rounds: int = 4, gap: float = 1.2):
                nonlocal live_sh, live_avg, live_usd
                for _i in range(rounds):
                    await _asyncio.sleep(gap)
                    try:
                        try:
                            pos = await self.pf.get_position_for_market(
                                pf_m["id"], is_yes=(pf_dir == "up")
                            )
                        except TypeError:
                            pos = await self.pf.get_position_for_market(pf_m["id"])
                        if pos:
                            live_sh = float(pos.get("shares") or 0)
                            live_avg = float(pos.get("avg_price") or 0)
                            live_usd = float(pos.get("cost_usd") or 0) or (live_sh * live_avg)
                            if live_sh >= max(0.5, order_shares * 0.85):
                                return True
                    except Exception as e:
                        logger.warning("PF live poll: %s", e)
                return live_sh >= max(0.5, order_shares * 0.5)

            filled_on_chain = await _poll_pf_live(rounds=4, gap=1.2)

            if filled_on_chain and live_sh >= max(0.5, order_shares * 0.85):
                sh = min(live_sh, order_shares * 1.08)
                ap = live_avg if 0.01 <= live_avg <= 1 else float(pf_px)
                cu = live_usd if live_usd > 0 else sh * ap
                pf_fill = Fill(
                    status="FILLED", shares=round(sh, 4),
                    price=round(ap, 4), usd=round(cu, 4),
                    order_id=pf_fill.order_id,
                )
                filled_pf = float(pf_fill.shares)
                logger.info(
                    "PF already filled on-chain %.4f@%.4f after poll (skip retry buy)",
                    sh, ap,
                )
            else:
                # Cancel resting only after polls show we still need shares
                if pf_fill.order_id:
                    try:
                        await self.pf.cancel_order(str(pf_fill.order_id))
                        logger.info("PF cancel resting %s before retry", pf_fill.order_id)
                    except Exception as e:
                        logger.warning("PF cancel before retry: %s", e)
                    # Cancel may race with fill — poll again after cancel
                    await _asyncio.sleep(1.5)
                    try:
                        try:
                            pos = await self.pf.get_position_for_market(
                                pf_m["id"], is_yes=(pf_dir == "up")
                            )
                        except TypeError:
                            pos = await self.pf.get_position_for_market(pf_m["id"])
                        if pos:
                            live_sh = float(pos.get("shares") or 0)
                            live_avg = float(pos.get("avg_price") or 0)
                            live_usd = float(pos.get("cost_usd") or 0) or (live_sh * live_avg)
                    except Exception as e:
                        logger.warning("PF post-cancel live check: %s", e)

                if live_sh >= max(0.5, order_shares * 0.85):
                    sh = min(live_sh, order_shares * 1.08)
                    ap = live_avg if 0.01 <= live_avg <= 1 else float(pf_px)
                    cu = live_usd if live_usd > 0 else sh * ap
                    pf_fill = Fill(
                        status="FILLED", shares=round(sh, 4),
                        price=round(ap, 4), usd=round(cu, 4),
                        order_id=pf_fill.order_id,
                    )
                    filled_pf = float(pf_fill.shares)
                    logger.info(
                        "PF filled during cancel race %.4f@%.4f (no retry buy)",
                        sh, ap,
                    )
                else:
                    need_pf = max(0.0, order_shares - live_sh)
                    if need_pf < 0.5:
                        sh = max(live_sh, 0.0)
                        ap = live_avg if 0.01 <= live_avg <= 1 else float(pf_px)
                        pf_fill = Fill(
                            status="FILLED", shares=round(sh, 4),
                            price=round(ap, 4), usd=round(sh * ap, 4),
                            order_id=pf_fill.order_id,
                        )
                        filled_pf = float(pf_fill.shares)
                        logger.info("PF near target from live %.4f — no new buy", sh)
                    else:
                        # Only shortfall, never full size again
                        px = round(min(0.99, float(pf_px) + 0.02), 2)
                        logger.info(
                            "PF retry buy SHORTFALL need=%.4f @ %.2f (live had %.4f)",
                            need_pf, px, live_sh,
                        )
                        pf_resp2 = await self.pf.place_order(
                            pf_m["id"], "BUY", need_pf, px,
                            is_yes=(pf_dir == "up"),
                        )
                        pf_fill2 = fill_from_predict(pf_resp2, need_pf)
                        if pf_fill2.status == "SUBMITTED" and pf_fill2.order_id:
                            await _asyncio.sleep(2.0)
                            try:
                                st = await self.pf.get_order(pf_fill2.order_id)
                                if st:
                                    pf_fill2 = fill_from_predict(st, need_pf)
                            except Exception:
                                pass
                            if pf_fill2.status != "FILLED":
                                try:
                                    try:
                                        pos2 = await self.pf.get_position_for_market(
                                            pf_m["id"], is_yes=(pf_dir == "up")
                                        )
                                    except TypeError:
                                        pos2 = await self.pf.get_position_for_market(pf_m["id"])
                                    if pos2 and float(pos2.get("shares") or 0) >= max(0.5, order_shares * 0.85):
                                        sh = min(float(pos2["shares"]), order_shares * 1.08)
                                        ap = float(pos2.get("avg_price") or px)
                                        cu = float(pos2.get("cost_usd") or 0) or sh * ap
                                        pf_fill2 = Fill(
                                            status="FILLED", shares=round(sh, 4),
                                            price=round(ap if 0.01 <= ap <= 1 else px, 4),
                                            usd=round(cu, 4), order_id=pf_fill2.order_id,
                                        )
                                except Exception:
                                    pass
                        if pf_fill2.status == "FILLED":
                            if live_sh > 0.1:
                                tot = live_sh + float(pf_fill2.shares)
                                if tot > order_shares * 1.15:
                                    logger.warning(
                                        "PF combined %.4f exceeds target — capping to %.4f",
                                        tot, order_shares * 1.08,
                                    )
                                tot = min(tot, order_shares * 1.08)
                                tot_usd = live_usd + float(pf_fill2.usd or 0)
                                avg = tot_usd / tot if tot else float(pf_fill2.price or 0)
                                pf_fill = Fill(
                                    status="FILLED", shares=round(tot, 4),
                                    price=round(avg, 4), usd=round(tot_usd, 4),
                                    order_id=pf_fill2.order_id or pf_fill.order_id,
                                )
                            else:
                                # Cap single fill to ~target
                                sh = min(float(pf_fill2.shares), order_shares * 1.08)
                                pf_fill = Fill(
                                    status="FILLED", shares=round(sh, 4),
                                    price=float(pf_fill2.price or px),
                                    usd=round(sh * float(pf_fill2.price or px), 4),
                                    order_id=pf_fill2.order_id,
                                )
                            filled_pf = float(pf_fill.shares)
                        else:
                            logger.warning(
                                "PF retry still not filled status=%s — leave unfilled",
                                pf_fill2.status,
                            )

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
        elif pm_fill.status in ("REJECTED", "SUBMITTED"):
            self._apply_fill(hedge.pm_leg, pm_fill)

        # Reject / jurisdiction handling for PF
        if pf_fill.status == "REJECTED":
            hedge.status = "CLOSED"
            hedge.closed_at = time.time()
            reason = (pf_fill.reason or "").lower()
            self._entry_busy = False
            logger.warning("PF reject %s — abort pair", pf_fill.reason)
            # Critical: kill any PM GTC that may still be resting
            try:
                if pm_fill.order_id:
                    await self.pm.cancel_order(pm_fill.order_id)
                await self.pm.cancel_all_orders()
                logger.info("PM cleared after PF reject")
            except Exception as e:
                logger.warning("PM clear after PF reject: %s", e)
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
            logger.info(
                "Hedge %s PARTIAL PF=FILLED PM=%s order=%s — manage loop will wait/retry",
                hedge.id, pm_fill.status, (pm_fill.order_id or "")[:18],
            )
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
            live_sh = 0.0
            live_avg = 0.0
            live_usd = 0.0
            # 1) Live position first (order GET often 404 after fill)
            try:
                try:
                    pos = await self.pf.get_position_for_market(mid, is_yes=is_yes)
                except TypeError:
                    pos = await self.pf.get_position_for_market(mid)
                if pos:
                    live_sh = float(pos.get("shares") or 0)
                    live_avg = float(pos.get("avg_price") or 0)
                    live_usd = float(pos.get("cost_usd") or 0) or (live_sh * live_avg if live_avg else 0)
                    if live_usd > 0 and live_sh > 0 and (live_usd / live_sh) > 1.5 and live_usd > 50:
                        live_usd = live_usd / 100.0
            except Exception as e:
                logger.debug("PF position check on retry: %s", e)

            if live_sh >= max(0.5, need * 0.85):
                # Enough shares already — NEVER buy more (prevents 6→12)
                sh = min(live_sh, need * 1.08) if need > 0 else live_sh
                px = (live_usd / live_sh) if (live_usd > 0 and live_sh > 0) else live_avg
                if not (0.01 <= px <= 1.0):
                    px = live_avg if 0.01 <= live_avg <= 1 else 0.5
                self._apply_fill(
                    hedge.pf_leg,
                    Fill(status="FILLED", shares=round(sh, 4), price=round(px, 4),
                         usd=round(live_usd or sh * px, 4), order_id=hedge.pf_leg.order_id),
                )
                logger.info(
                    "PF retry: position shows FILLED %.4f@%.4f live=%.4f (no new order)",
                    sh, px, live_sh,
                )
            elif getattr(hedge, "_pf_extra_buys", 0) >= 1:
                # Already placed one extra buy this hedge — stop spamming
                logger.warning(
                    "PF retry: already did 1 extra buy, live=%.4f need=%.4f — waiting",
                    live_sh, need,
                )
            else:
                # Cancel resting, buy ONLY the shortfall
                if hedge.pf_leg.order_id:
                    try:
                        await self.pf.cancel_order(str(hedge.pf_leg.order_id))
                        logger.info("PF cancel before retry buy id=%s", hedge.pf_leg.order_id)
                    except Exception as e:
                        logger.warning("PF cancel before retry: %s", e)
                    hedge.pf_leg.order_id = ""

                shortfall = max(0.0, need - live_sh)
                _, ask = self._pf_best(mid, hedge.pf_leg.direction)
                if ask and shortfall >= 0.5:
                    px = min(0.99, round(float(ask) + 0.02, 2))  # 2 decimals max
                    logger.info(
                        "PF retry BUY shortfall=%.4f (live=%.4f need=%.4f) @ %.2f",
                        shortfall, live_sh, need, px,
                    )
                    resp = await self.pf.place_order(
                        mid, "BUY", shortfall, px, is_yes=is_yes,
                    )
                    hedge._pf_extra_buys = getattr(hedge, "_pf_extra_buys", 0) + 1
                    fill = fill_from_predict(resp, shortfall)
                    if fill.status == "FILLED":
                        # Combine with any live shares
                        tot_sh = live_sh + float(fill.shares)
                        tot_usd = live_usd + float(fill.usd or 0)
                        avg = (tot_usd / tot_sh) if tot_sh else float(fill.price or px)
                        self._apply_fill(
                            hedge.pf_leg,
                            Fill(status="FILLED",
                                 shares=round(min(tot_sh, need * 1.08), 4),
                                 price=round(avg, 4),
                                 usd=round(tot_usd, 4),
                                 order_id=fill.order_id),
                        )
                    else:
                        self._apply_fill(hedge.pf_leg, fill)
                        if fill.status == "SUBMITTED":
                            import asyncio as _aio
                            await _aio.sleep(1.5)
                            try:
                                try:
                                    pos = await self.pf.get_position_for_market(mid, is_yes=is_yes)
                                except TypeError:
                                    pos = await self.pf.get_position_for_market(mid)
                                if pos and float(pos.get("shares") or 0) >= max(0.5, need * 0.85):
                                    sh = min(float(pos["shares"]), need * 1.08)
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
                elif live_sh >= 0.5:
                    # Have something but below threshold and no ask — mark what we have
                    px = live_avg if 0.01 <= live_avg <= 1 else 0.5
                    self._apply_fill(
                        hedge.pf_leg,
                        Fill(status="FILLED", shares=round(live_sh, 4), price=round(px, 4),
                             usd=round(live_usd or live_sh * px, 4),
                             order_id=hedge.pf_leg.order_id),
                    )
                    logger.info("PF retry: adopting partial live %.4f (no more buys)", live_sh)

        # --- PM leg: only if PF is filled and PM empty / short ---
        if hedge.pm_leg.status != "FILLED" and hedge.pf_leg.status == "FILLED":
            # If GTC still resting, poll it first — never place a 2nd order on top
            if hedge.pm_leg.status == "SUBMITTED" and hedge.pm_leg.order_id:
                try:
                    st = await self.pm.get_order(hedge.pm_leg.order_id)
                    if st:
                        got = fill_from_pm_order(
                            st, float(hedge.pf_leg.filled_shares or hedge.pm_leg.requested_shares or 0),
                        )
                        if got.status == "FILLED" and got.shares > 0.5:
                            self._apply_fill(hedge.pm_leg, got)
                            logger.info(
                                "PM GTC filled in manage loop %.4f@%.4f",
                                got.shares, got.price or 0,
                            )
                        elif got.status == "SUBMITTED":
                            # still resting — leave it, don't place another
                            logger.info(
                                "PM GTC still resting id=%s — wait, no new order",
                                str(hedge.pm_leg.order_id)[:18],
                            )
                            return
                except Exception as e:
                    logger.debug("PM manage poll: %s", e)

            already = float(hedge.pm_leg.filled_shares or 0)
            need_total = float(hedge.pf_leg.filled_shares or hedge.pf_leg.requested_shares or 0)
            # Live token balance may show fills the order API missed
            try:
                bal = await self.pm.get_token_balance(str(hedge.pm_leg.token_or_market))
                if bal > already:
                    already = bal
            except Exception:
                pass
            if already >= max(0.5, need_total * 0.85):
                px = float(hedge.pm_leg.entry_price or 0) or 0.5
                self._apply_fill(
                    hedge.pm_leg,
                    Fill(
                        status="FILLED",
                        shares=round(min(already, need_total * 1.08), 4),
                        price=round(px, 4),
                        usd=round(min(already, need_total) * px, 4),
                        order_id=hedge.pm_leg.order_id,
                    ),
                )
                logger.info(
                    "PM retry: already have %.2f shares (need %.2f) — no new order",
                    already, need_total,
                )
            elif getattr(hedge, "_pm_extra_buys", 0) >= 1:
                logger.warning(
                    "PM retry: already placed 1 extra buy, have=%.2f need=%.2f — stop",
                    already, need_total,
                )
            else:
                _, ask = self._pm_best(hedge.pm_leg.direction)
                shortfall = max(0.0, need_total - already)
                if ask and shortfall >= 0.5:
                    pf_px = float(hedge.pf_leg.entry_price or 0)
                    projected = pf_px + float(ask)
                    max_ok = float(self.settings.entry_sum_max) + 0.05
                    # Time left in window — only force-unwind near the end
                    ends_in = 999.0
                    try:
                        ends_in = float(
                            (getattr(self, "pf_state", None) or {}).get("live_end_in")
                            or (getattr(self, "pm_state", None) or {}).get("ends_in")
                            or 999
                        )
                    except Exception:
                        ends_in = 999.0
                    if pf_px > 0 and projected > max_ok:
                        # Always clear any resting PM orders first
                        try:
                            if hedge.pm_leg.order_id:
                                await self.pm.cancel_order(hedge.pm_leg.order_id)
                            await self.pm.cancel_all_orders()
                        except Exception as e:
                            logger.warning("PM clear on high-ask: %s", e)
                        if ends_in < 40:
                            logger.warning(
                                "Hedge %s late ABORT PM (ends_in=%.0fs) projected=%.3f "
                                "> max %.3f — UNWIND PF",
                                hedge.id, ends_in, projected, max_ok,
                            )
                            hedge.pm_leg.status = "REJECTED"
                            hedge.pm_leg.reject_reason = (
                                f"retry ask too high ({projected:.3f} > {max_ok:.3f})"
                            )
                            try:
                                await self._unwind_pf_leg(hedge)
                            except Exception as e:
                                logger.error("PF unwind after PM abort failed: %s", e)
                            return
                        logger.info(
                            "Hedge %s PM ask high (proj=%.3f) ends_in=%.0fs — "
                            "KEEP PF, no unwind (PM orders cancelled)",
                            hedge.id, projected, ends_in,
                        )
                        return
                    # Cancel previous resting order only
                    if hedge.pm_leg.order_id:
                        try:
                            await self.pm.cancel_order(hedge.pm_leg.order_id)
                            logger.info("PM cancel before retry %s", str(hedge.pm_leg.order_id)[:18])
                        except Exception as e:
                            logger.debug("PM cancel before retry: %s", e)
                    # Buy ONLY the shortfall, not full size again
                    logger.info(
                        "PM retry BUY shortfall=%.4f (have=%.4f need=%.4f) @ ask=%.3f",
                        shortfall, already, need_total, float(ask),
                    )
                    resp = await self.pm.place_market_buy(
                        hedge.pm_leg.token_or_market, shortfall, float(ask),
                    )
                    hedge._pm_extra_buys = getattr(hedge, "_pm_extra_buys", 0) + 1
                    fill = fill_from_pm_order(resp, shortfall)
                    if fill.status == "SUBMITTED" and fill.order_id:
                        import asyncio as _aio
                        for _ in range(8):
                            await _aio.sleep(0.7)
                            try:
                                st = await self.pm.get_order(fill.order_id)
                                if st:
                                    got = fill_from_pm_order(st, shortfall)
                                    if got.status == "FILLED" and got.shares > 0.5:
                                        fill = got
                                        break
                            except Exception:
                                pass
                        if fill.status != "FILLED":
                            try:
                                await self.pm.cancel_order(fill.order_id)
                                logger.info(
                                    "PM GTC retry unfilled → cancel id=%s",
                                    str(fill.order_id)[:18],
                                )
                            except Exception:
                                pass
                            # post-cancel race check
                            await _aio.sleep(0.5)
                            try:
                                st = await self.pm.get_order(fill.order_id)
                                if st:
                                    got = fill_from_pm_order(st, shortfall)
                                    if got.status == "FILLED" and got.shares > 0.5:
                                        fill = got
                            except Exception:
                                pass
                            if fill.status != "FILLED":
                                fill = Fill(
                                    status="REJECTED",
                                    reason="GTC unfilled on retry",
                                    order_id=fill.order_id,
                                    price=fill.price,
                                )
                    if fill.status == "FILLED" and already > 0:
                        # Combine prior + new
                        tot = already + float(fill.shares)
                        tot_usd = already * float(hedge.pm_leg.entry_price or fill.price or ask) + float(fill.usd or 0)
                        avg = tot_usd / tot if tot else float(fill.price or ask)
                        fill = Fill(
                            status="FILLED",
                            shares=round(min(tot, need_total * 1.08), 4),
                            price=round(avg, 4),
                            usd=round(tot_usd, 4),
                            order_id=fill.order_id,
                        )
                    self._apply_fill(hedge.pm_leg, fill)
                    reason = (getattr(fill, "reason", None) or "") if fill else ""
                    if fill.status == "REJECTED" and (
                        "min-notional" in str(reason).lower()
                        or "min size" in str(reason).lower()
                        or "impossible" in str(reason).lower()
                    ):
                        logger.warning("Hedge %s PM min-notional reject — UNWIND PF", hedge.id)
                        try:
                            await self._unwind_pf_leg(hedge)
                        except Exception as e:
                            logger.error("PF unwind after min-notional: %s", e)
                        return

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
                # If live position is ~2x tracked, this is a double-fill on-chain.
                # Adopt live size so PnL matches reality (do NOT ignore).
                if old_sh > 0 and sh > old_sh * 1.5 and hedge.status in ("OPEN", "PARTIAL", "CLOSING", "SETTLING"):
                    new_px = ap if 0.01 <= ap <= 1.0 else (cu / sh if sh else 0)
                    if not (0.01 <= new_px <= 1.0):
                        new_px = float(hedge.pf_leg.entry_price or 0) or 0.5
                    # Target = PM filled size (or original tracked). Sell excess to rebalance.
                    target = float(hedge.pm_leg.filled_shares or 0) or old_sh
                    excess = max(0.0, sh - target)
                    logger.warning(
                        "PF DOUBLE-FILL detected [%s] live=%.4f tracked=%.4f target=%.4f excess=%.4f",
                        reason, sh, old_sh, target, excess,
                    )
                    if excess >= 0.5 and hedge.status in ("OPEN", "PARTIAL") and not getattr(hedge, "_pf_excess_sold", False):
                        try:
                            bid, _ = self._pf_best(str(mid), hedge.pf_leg.direction)
                            px = max(0.01, round(float(bid or new_px) - 0.02, 2))
                            sell_sz = max(0.0, int(excess * 0.97 * 10000) / 10000.0)
                            if sell_sz * (bid or new_px) >= 0.50:
                                logger.warning(
                                    "PF selling DOUBLE-FILL excess %.4f @ %.2f to match PM %.4f",
                                    sell_sz, px, target,
                                )
                                resp = await self.pf.place_order(
                                    mid, "SELL", sell_sz, px,
                                    is_yes=(hedge.pf_leg.direction == "up"),
                                )
                                fill = fill_from_predict(resp, sell_sz)
                                hedge._pf_excess_sold = True
                                if fill.status == "FILLED":
                                    # reduce cost basis proportionally
                                    remain = max(target, sh - float(fill.shares or sell_sz))
                                    hedge.pf_leg.filled_shares = round(remain, 4)
                                    hedge.pf_leg.shares = round(remain, 4)
                                    hedge.pf_leg.entry_price = round(new_px, 4)
                                    hedge.pf_leg.entry_usd = round(remain * new_px, 4)
                                    logger.info(
                                        "PF excess sold %.4f — remain %.4f@%.4f",
                                        fill.shares, remain, new_px,
                                    )
                                    changed = True
                                else:
                                    # adopt live for accounting; user may need to balance
                                    hedge.pf_leg.filled_shares = round(sh, 4)
                                    hedge.pf_leg.shares = round(sh, 4)
                                    hedge.pf_leg.entry_price = round(new_px, 4)
                                    hedge.pf_leg.entry_usd = round(cu if cu > 0 else sh * new_px, 4)
                                    changed = True
                            else:
                                hedge.pf_leg.filled_shares = round(sh, 4)
                                hedge.pf_leg.shares = round(sh, 4)
                                hedge.pf_leg.entry_price = round(new_px, 4)
                                hedge.pf_leg.entry_usd = round(cu if cu > 0 else sh * new_px, 4)
                                changed = True
                        except Exception as e:
                            logger.warning("PF excess sell failed: %s — adopting live", e)
                            hedge.pf_leg.filled_shares = round(sh, 4)
                            hedge.pf_leg.shares = round(sh, 4)
                            hedge.pf_leg.entry_price = round(new_px, 4)
                            hedge.pf_leg.entry_usd = round(cu if cu > 0 else sh * new_px, 4)
                            changed = True
                    else:
                        hedge.pf_leg.filled_shares = round(sh, 4)
                        hedge.pf_leg.shares = round(sh, 4)
                        hedge.pf_leg.entry_price = round(new_px, 4)
                        hedge.pf_leg.entry_usd = round(cu if cu > 0 else sh * new_px, 4)
                        changed = True
                elif old_sh > 0 and sh > old_sh * 1.08 and hedge.status in ("OPEN", "PARTIAL", "CLOSING"):
                    # mild overfill — keep tracked, update avg only
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

    async def _unwind_pf_leg(self, hedge: Hedge) -> None:
        """Sell the PF leg when PM never filled — avoid naked directional risk.
        First cancel any resting PM GTC so it cannot fill later unhedged.
        """
        if hedge.pf_leg.status not in ("FILLED", "PARTIAL"):
            hedge.status = "CLOSED"
            return
        logger.warning(
            "Hedge %s UNWIND PF only (PM never filled) shares=%.4f @entry=%.4f",
            hedge.id, hedge.pf_leg.filled_shares or 0, hedge.pf_leg.entry_price or 0,
        )
        # Critical: kill resting PM limits so they don't fill after we dump PF
        if hedge.pm_leg.order_id:
            try:
                ok = await self.pm.cancel_order(hedge.pm_leg.order_id)
                logger.info("UNWIND cancel PM order %s ok=%s", str(hedge.pm_leg.order_id)[:18], ok)
            except Exception as e:
                logger.warning("UNWIND cancel PM order: %s", e)
        try:
            await self.pm.cancel_all_orders()
        except Exception as e:
            logger.debug("UNWIND cancel_all: %s", e)

        await self._sell_leg(hedge.pf_leg, "PF")
        # Wait briefly for sell fill / activity proceeds
        if float(hedge.pf_leg.exit_usd or 0) <= 0.01:
            import asyncio as _aio
            for _ in range(6):
                await _aio.sleep(0.8)
                if float(hedge.pf_leg.exit_usd or 0) > 0.01:
                    break
                try:
                    act = await self.pf.get_fill_price_from_activity(
                        hedge.pf_leg.token_or_market,
                        order_id=str(hedge.pf_leg.order_id or ""),
                        limit_price=0.0,
                        for_sell=True,
                    )
                    if act and float(act.get("cost_usd") or 0) > 0:
                        hedge.pf_leg.exit_usd = float(act["cost_usd"])
                        hedge.pf_leg.exit_price = float(act.get("avg_price") or 0)
                        hedge.pf_leg.status = "CLOSED"
                        break
                except Exception:
                    pass

        pf_cost = float(hedge.pf_leg.entry_usd or 0)
        pf_exit = float(hedge.pf_leg.exit_usd or 0)
        pnl = pf_exit - pf_cost
        hedge.pnl = round(pnl, 4)
        hedge.status = "CLOSED"
        hedge.exit_sum = float(hedge.pf_leg.exit_price or 0)
        logger.info(
            "Hedge %s UNWIND CLOSED pf_pnl=%.4f (exit=%.4f cost=%.4f) — recovery will size up",
            hedge.id, pnl, pf_exit, pf_cost,
        )
        try:
            self.recovery.on_pair_closed(
                hedge.id, pnl, 0.0, 0.0, hedge.pf_leg.filled_shares or 0,
            )
        except Exception as e:
            logger.warning("recovery on unwind: %s", e)

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
            mid = leg.token_or_market
            # Prefer live balance from positions API (exact sellable amount)
            live = 0.0
            try:
                pos = await self.pf.get_position_for_market(mid, is_yes=is_yes)
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

            bid, _ = self._pf_best(mid, leg.direction)
            est_px = float(bid or leg.entry_price or 0.5)

            # Already flat (previous sell filled) — recover proceeds from activity
            if size <= 0.0 or size * est_px < 0.50:
                if not leg.exit_usd or leg.exit_usd < 0.01:
                    try:
                        act = await self.pf.get_fill_price_from_activity(
                            mid, order_id=str(leg.order_id or ""), limit_price=0.0,
                            for_sell=True,
                        )
                        if not act or float(act.get("cost_usd") or 0) <= 0:
                            act = await self.pf.get_fill_price_from_activity(
                                mid, order_id="", limit_price=0.0, for_sell=True,
                            )
                        if act and float(act.get("cost_usd") or 0) > 0:
                            # For SELL, cost_usd from activity is proceeds
                            leg.exit_usd = float(act["cost_usd"])
                            ash = float(act.get("shares") or 0) or float(leg.filled_shares or 0)
                            if ash > 0:
                                leg.exit_price = round(leg.exit_usd / ash, 4)
                            logger.info(
                                "%s sell already flat — activity proceeds $%.4f @%.4f",
                                label, leg.exit_usd, leg.exit_price,
                            )
                    except Exception as e:
                        logger.debug("%s activity recovery on dust: %s", label, e)
                if not leg.exit_price:
                    leg.exit_price = est_px if float(leg.filled_shares or 0) > 0 else 0.0
                # CRITICAL: exit_usd must cover FULL position, not residual dust
                if not leg.exit_usd or leg.exit_usd < 0.01:
                    full_sh = float(leg.filled_shares or 0)
                    leg.exit_usd = round(full_sh * float(leg.exit_price or est_px), 4)
                leg.status = "CLOSED"
                logger.info(
                    "%s sell skip dust/flat shares=%.4f — CLOSED exit=%.3f usd=%.4f",
                    label, size, leg.exit_price, leg.exit_usd,
                )
                return

            # Aggressive limit under best bid
            px = max(0.01, round(float(bid) - 0.02, 2)) if bid else max(0.01, (leg.entry_price or 0.5) * 0.9)
            resp = await self.pf.place_order(mid, "SELL", size, px, is_yes=is_yes)
            fill = fill_from_predict(resp, size)

            async def _recover_proceeds_from_position_drop(before_sh: float) -> bool:
                """After place, wait and see if position dropped → sell filled."""
                import asyncio as _aio
                await _aio.sleep(1.2)
                try:
                    pos2 = await self.pf.get_position_for_market(mid, is_yes=is_yes)
                    after = float((pos2 or {}).get("shares") or 0)
                except Exception:
                    after = before_sh
                sold = max(0.0, before_sh - after)
                if sold < 0.5:
                    return False
                # Prefer activity for real avg sell price
                proceeds = 0.0
                avg_px = 0.0
                try:
                    act = await self.pf.get_fill_price_from_activity(
                        mid, order_id=str(fill.order_id or ""), limit_price=0.0,
                        for_sell=True,
                    )
                    if act and float(act.get("cost_usd") or 0) > 0:
                        proceeds = float(act["cost_usd"])
                        ash = float(act.get("shares") or 0) or sold
                        avg_px = proceeds / ash if ash > 0 else 0.0
                except Exception:
                    pass
                if proceeds <= 0:
                    # fallback: use limit px * sold
                    avg_px = float(fill.price or px or est_px)
                    proceeds = sold * avg_px
                leg.exit_usd = round(float(leg.exit_usd or 0) + proceeds, 4)
                # weighted avg exit price
                total_sold = sold
                if leg.exit_price and leg.filled_shares:
                    # recompute from cumulative
                    pass
                leg.exit_price = round(avg_px, 4) if 0.01 <= avg_px <= 1.0 else float(px)
                logger.info(
                    "%s sell filled via position-drop sold=%.4f @%.4f proceeds=$%.4f left=%.4f",
                    label, sold, leg.exit_price, proceeds, after,
                )
                # Keep entry filled_shares for cost basis; residual handled next loop / dust
                if after <= 0.05 or after * (bid or 0.5) < 0.50:
                    leg.status = "CLOSED"
                else:
                    # leave status open so residual can be sold, but do NOT shrink filled_shares
                    pass
                return True

            if fill.status == "FILLED":
                leg.exit_price = float(fill.price or px)
                leg.exit_usd = float(fill.usd or 0) or round(size * leg.exit_price, 4)
                leg.status = "CLOSED"
                logger.info(
                    "%s sell FILLED %.4f @%.4f usd=$%.4f",
                    label, size, leg.exit_price, leg.exit_usd,
                )
                # Check residual
                try:
                    await asyncio.sleep(0.5)
                    pos2 = await self.pf.get_position_for_market(mid, is_yes=is_yes)
                    left = float((pos2 or {}).get("shares") or 0)
                    if left > 0.05 and left * (bid or 0.5) >= 0.50:
                        logger.info("%s residual %.4f after sell — will retry next loop", label, left)
                        # Keep CLOSED for main fill; residual is dust-ish relative to original
                        # Store residual so next exit pass can sell it — but for PnL we have main proceeds
                except Exception:
                    pass
            elif fill.status in ("SUBMITTED", "PENDING") or (fill.order_id and fill.status != "REJECTED"):
                # Limit resting / async fill — wait for position drop
                before = live if live > 0 else size
                ok = await _recover_proceeds_from_position_drop(before)
                if not ok:
                    # cancel resting so we don't leave open sells
                    if fill.order_id:
                        try:
                            await self.pf.cancel_order(str(fill.order_id))
                            logger.info("%s cancel resting sell %s", label, fill.order_id)
                        except Exception as e:
                            logger.warning("%s cancel sell: %s", label, e)
                    logger.warning("%s sell SUBMITTED but no fill yet — will retry", label)
            elif fill.status == "REJECTED":
                reason = (fill.reason or "").lower()
                leg.reject_reason = fill.reason or ""
                logger.warning("%s sell rejected: %s", label, fill.reason)
                if "insufficient" in reason and "share" in reason:
                    try:
                        pos = await self.pf.get_position_for_market(mid, is_yes=is_yes)
                        live2 = float((pos or {}).get("shares") or 0)
                    except Exception:
                        live2 = 0.0
                    if live2 <= 0.05:
                        # Position gone → sell already filled (race). Recover proceeds.
                        if not leg.exit_usd or leg.exit_usd < 0.01:
                            try:
                                act = await self.pf.get_fill_price_from_activity(
                                    mid, order_id="", limit_price=0.0, for_sell=True,
                                )
                                if act and float(act.get("cost_usd") or 0) > 0:
                                    leg.exit_usd = float(act["cost_usd"])
                                    ash = float(act.get("shares") or 0) or float(leg.filled_shares or 0)
                                    leg.exit_price = round(leg.exit_usd / ash, 4) if ash else est_px
                            except Exception:
                                pass
                        if not leg.exit_price:
                            leg.exit_price = est_px
                        if not leg.exit_usd or leg.exit_usd < 0.01:
                            leg.exit_usd = round(float(leg.filled_shares or 0) * float(leg.exit_price), 4)
                        leg.status = "CLOSED"
                        logger.info(
                            "%s sell: insufficient but live=0 — CLOSED exit=%.3f usd=$%.4f",
                            label, leg.exit_price, leg.exit_usd,
                        )
                    else:
                        logger.info(
                            "%s sell: insufficient — live=%.4f (keeping entry shares %.4f)",
                            label, live2, leg.filled_shares,
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
            # Re-pull PF SELL proceeds from activity — residual-dust close often
            # left exit_usd at ~$0.1 while we actually sold most of the position.
            if (hedge.close_reason or "") == "EXIT" and hedge.pf_leg.venue in ("predict", "pf", "bn"):
                try:
                    mid = hedge.pf_leg.token_or_market
                    act = await self.pf.get_fill_price_from_activity(
                        mid, order_id="", limit_price=0.0, for_sell=True,
                    )
                    if act and float(act.get("cost_usd") or 0) > 0.5:
                        sold_usd = float(act["cost_usd"])
                        sold_sh = float(act.get("shares") or 0)
                        if sold_usd > float(hedge.pf_leg.exit_usd or 0) + 0.05:
                            old_u = float(hedge.pf_leg.exit_usd or 0)
                            hedge.pf_leg.exit_usd = sold_usd
                            if sold_sh > 0:
                                hedge.pf_leg.exit_price = round(sold_usd / sold_sh, 4)
                            # Restore display size to what we actually sold (not residual)
                            if sold_sh > float(hedge.pf_leg.filled_shares or 0):
                                hedge.pf_leg.filled_shares = sold_sh
                            logger.info(
                                "PF EXIT activity reconcile proceeds $%.4f → $%.4f @%.4f sh=%.4f",
                                old_u, sold_usd, hedge.pf_leg.exit_price, sold_sh,
                            )
                except Exception as e:
                    logger.warning("PF EXIT activity reconcile failed: %s", e)

            def _leg_pnl_exit(leg: Leg) -> float:
                # Cost = entry_usd (fixed at open). Proceeds = exit_usd (from sell/activity).
                # Never multiply residual filled_shares into cost — that understates loss/profit.
                cost = float(leg.entry_usd or 0)
                if cost <= 0 and leg.entry_price and (leg.filled_shares or 0) > 0:
                    cost = float(leg.entry_price) * float(leg.filled_shares)
                proceeds = float(leg.exit_usd or 0)
                if proceeds <= 0 and leg.exit_price is not None and (leg.filled_shares or 0) > 0:
                    proceeds = float(leg.exit_price) * float(leg.filled_shares)
                return proceeds - cost

            pf_pnl = _leg_pnl_exit(hedge.pf_leg)
            pm_pnl = _leg_pnl_exit(hedge.pm_leg)
            hedge.pnl = pf_pnl + pm_pnl
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

            # After long wait:
            # - missing one side → fill from the known venue (lag)
            # - TRUE disagreement (both known, opposite) → settle EACH leg by its
            #   own venue. Forcing PM onto PF caused fake wins when PF really
            #   resolved the other way (see hedge 6c6d7829: PF Down / PM Up).
            if pf_up_won is None and pm_up_won is not None:
                pf_up_won = pm_up_won
                logger.warning(
                    "Hedge %s PF unknown after wait — using PM pm_up=%s for PF leg only",
                    hedge.id, pm_up_won,
                )
            elif pm_up_won is None and pf_up_won is not None:
                pm_up_won = pf_up_won
                logger.warning(
                    "Hedge %s PM unknown after wait — using PF pf_up=%s for PM leg only",
                    hedge.id, pf_up_won,
                )
            elif pf_up_won is None and pm_up_won is None:
                # Last resort: do not invent wins. Leave None → legs exit 0.
                logger.warning(
                    "Hedge %s NO resolution data after wait — both legs exit 0",
                    hedge.id,
                )
            elif pf_up_won != pm_up_won:
                logger.warning(
                    "Hedge %s TRUE DIVERGENCE after wait pf_up=%s pm_up=%s — "
                    "settle each leg independently (PF by PF, PM by PM)",
                    hedge.id, pf_up_won, pm_up_won,
                )
                # keep both values as-is; per-leg won_up uses venue-specific flag
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
            # Per-venue outcome (None → treat as loss / exit 0 — never invent a win)
            won_up = pf_up_won if leg.venue in ("predict", "pf", "bn") else pm_up_won
            if won_up is None:
                leg.exit_price = 0.0
            elif leg.direction == "up":
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
