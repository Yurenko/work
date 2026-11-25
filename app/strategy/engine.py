"""Lock-in hedge: the same outcome on Predict.fun and Polymarket.

Rules:
1. When an outcome is near $0.70 on both venues, buy it on both, same size.
2. Each venue hedges its own opposite side only when that lock clears
   the net target after fees (about $0.10 per share).
3. A venue that is already locked is held to settlement.
4. Venues do not have to hedge at the same time or the same price.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from app.core.config import get_settings
from app.core.logging import logger, get_recent_logs
from app.polymarket.chainlink import ChainlinkBtcStream
from app.polymarket.client import PolymarketClient
from app.polymarket.markets import updown_slug
from app.predict.client import PredictClient
from app.predict.ws import PredictOrderbookWS
from app.strategy.btc_candle import (
    BtcCandleBook,
    candle_row,
    chart_candle_rows,
    chart_candles_are_current,
    format_new_market,
    loss_recovery_direction,
    parse_polymarket_crypto_price,
    shared_candle_direction,
)
from app.strategy.fills import Fill, fill_from_pm_order, fill_from_predict
from app.strategy.lockin import LockInManager, entry_orders, in_entry_zone, waiting_side
from app.strategy.trade_history import TradeHistory, row_from_hedge
from app.strategy.recovery import (
    RecoveryBook,
    RecoveryConfig,
    allows_immediate_entry,
    exposed_recovery_side,
    normalize_trade_strategy,
    normalize_trade_venues,
    recovery_entry_decision,
    step_limit_price,
    venue_codes,
)


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
    # Target net shares to keep after PF trim (usually = PM size / base_shares)
    target_net: float = 0.0
    # PF limit-sell of intentional excess (entry + offset)
    pf_trim_order_id: str = ""
    pf_trim_shares: float = 0.0       # remaining to sell
    pf_trim_target: float = 0.0       # original excess size
    pf_trim_px: float = 0.0
    pf_trim_status: str = ""          # ""|SUBMITTED|PARTIAL|FILLED|CANCELLED|SKIPPED
    pf_trim_filled: float = 0.0
    pf_trim_proceeds: float = 0.0
    pm_up_token: str = ""
    pm_down_token: str = ""
    pf_hedge_leg: Optional[Leg] = None
    pm_hedge_leg: Optional[Leg] = None
    strategy_mode: str = "BASE"  # BASE | RECOVERY
    limit_ceiling: float = 0.0  # step recovery refuses an ask above this
    cycle_shares: float = 0.0
    venues: str = "both"  # both | predict | polymarket
    candles: Optional[dict] = None
    # aliases for recovery/tests that still say bn
    @property
    def bn_leg(self) -> Leg:
        return self.pf_leg


class HedgeEngine:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.lockin = LockInManager(self.settings)
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
            multiplier=float(self.settings.martingale_multiplier),
            hedging_base=bool(self.settings.hedging_base_mode),
            hedging_recovery=bool(self.settings.hedging_recovery_mode),
        )
        self.recovery = RecoveryBook(cfg, Path("data/recovery.json"))
        self.btc = BtcCandleBook(
            max_age_sec=float(self.settings.btc_last_max_age_sec),
            path=Path("data/btc_candles.json"),
        )
        self.chainlink = ChainlinkBtcStream()
        self._btc_poll_at = 0.0
        self._logged_markets: set[str] = set()
        self._enter_despite_settling = False
        self._entry_busy = False
        self.trade_venues = self._load_trade_venues()
        self.trade_strategy = self._load_trade_strategy()
        self.candle_history: dict = {"predict": [], "polymarket": [], "updated": 0.0}
        self._candle_hist_at = 0.0
        self._candle_task: Optional[asyncio.Task] = None
        self._candle_lock = asyncio.Lock()
        self._position_journal = Path("data/position_candles.json")
        self.trade_history = TradeHistory(Path("data/trade_history.json"))

    # ── lifecycle ───────────────────────────────────────────────

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.status_text = "starting"
        self.chainlink.start()
        self._task = asyncio.create_task(self._loop(), name="hedge-engine")
        self._candle_task = asyncio.create_task(self._candle_loop(), name="candle-history")
        logger.info(
            "HedgeEngine started mode=%s strategy=%s shares=%.0f hedging=%s",
            self.settings.mode,
            self.recovery.mode,
            self.recovery.current_shares,
            "ON" if self.recovery.hedging_enabled() else "OFF",
        )

    async def stop(self) -> None:
        self.running = False
        self.status_text = "stopped"
        self.chainlink.stop()
        self.pf_ws.stop()
        if self._task:
            self._task.cancel()
            self._task = None
        if self._candle_task:
            self._candle_task.cancel()
            self._candle_task = None
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
                # The new window's open lasts a few seconds. Scan it at once.
                if left <= 1.5 or left >= 297:
                    interval = 0.0
                elif left < 20 or left > 280:
                    interval = min(interval, 5.0)
            if now - self._last_scan > interval:
                need_scan = True
        if need_scan:
            await self._refresh_markets(now)
            self._last_scan = now
            live = self._live_pf_markets(now)
            entry_ok = self._active_pf_markets(now)

        if now - self._btc_poll_at >= 2.0:
            self._btc_poll_at = now
            await self._poll_btc_references(now)
        else:
            tick = self.chainlink.latest()
            if tick:
                self.btc.add_tick(*tick)
        await self._manage_open(now)
        open_count = sum(1 for h in self.hedges if h.status in ("OPENING", "PARTIAL", "OPEN", "CLOSING"))
        settling = any(h.status == "SETTLING" for h in self.hedges)
        released = any(
            h.status == "SETTLING" and getattr(h, "_candle_released_entry", False)
            for h in self.hedges
        )
        if not self._step_entry_ready() and (not live or not self.pm_market):
            self.status_text = "waiting for live BTC 5m market"
        elif settling and not released and not self._step_entry_ready():
            self.status_text = "waiting for both platforms to settle"
        elif open_count < self.settings.max_open_hedges:
            block = self.recovery.entry_block()
            if not block:
                await self._refresh_balances(now)
                if self.recovery.mode == "RECOVERY":
                    await self._try_recovery_entry(now)
                else:
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
            target_end = float(getattr(self.recovery, "step_entry_end", 0) or 0)
            if self.trade_strategy == "step" and target_end > now:
                for market in live_sorted:
                    if abs(float(market.get("end") or 0) - target_end) <= 30:
                        if str(market.get("id")) not in {str(item.get("id")) for item in ws_targets}:
                            ws_targets.append(market)
                        break
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
            for m in ws_targets[:3]:
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
        lockin = getattr(self.settings, "lockin_enabled", True)
        if now - self._last_entry_attempt < (1.0 if lockin else 8.0):
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

        if getattr(self.settings, "lockin_enabled", True):
            low = float(self.settings.lock_entry_low)
            high = float(self.settings.lock_entry_high)
            direction, venues = entry_orders(
                pf_up_ask, pf_dn_ask, pm_up_ask, pm_dn_ask,
                target=float(self.settings.lock_entry_price),
                low=low,
                high=high,
            )
            window_end = min(pm_end, pf_end)
            existing = None
            for h in self.hedges:
                if h.status in ("OPENING", "PARTIAL", "OPEN", "CLOSING") and abs(h.market_end - window_end) < 30:
                    existing = h
                    break
            if existing is not None:
                # A venue that never filled can still enter, including after
                # the market flips to the other side.
                await self._complete_lock_entry(existing)
                return
            if direction is None or not venues:
                self.status_text = "no venue in the 0.58–0.82 entry window"
                return
            venues = [code for code in venues if code in self._enabled_codes()]
            if not venues:
                self.status_text = f"selected venue is outside the entry window ({self.trade_venues})"
                return
            def _ask(value: Optional[float]) -> float:
                return float(value) if value else 0.0

            asks = {
                "pf": _ask(pf_up_ask if direction == "up" else pf_dn_ask),
                "pm": _ask(pm_up_ask if direction == "up" else pm_dn_ask),
            }
            if self._entry_busy:
                return
            self._entry_busy = True
            logger.info(
                "ENTRY lock %s venues=%s PF ask=%.3f PM ask=%.3f",
                direction, ",".join(venues), asks["pf"], asks["pm"],
            )
            try:
                await self._open_lock_legs(
                    pf_m, direction, venues, asks, now, window_end,
                )
            finally:
                self._entry_busy = False
            return
        else:
            candidates: list[tuple[float, str, str]] = []

            def _leg_ok(pf_a: float, pm_a: float) -> bool:
                if pf_a is None or pm_a is None:
                    return False
                if pm_a < 0.05 or pm_a > 0.95 or pf_a < 0.05 or pf_a > 0.95:
                    return False
                return True

            if pf_up_ask and pm_dn_ask and _leg_ok(float(pf_up_ask), float(pm_dn_ask)):
                s = float(pf_up_ask) + float(pm_dn_ask)
                if s < self.settings.entry_sum_max:
                    candidates.append((s, "up", "down"))
            if pf_dn_ask and pm_up_ask and _leg_ok(float(pf_dn_ask), float(pm_up_ask)):
                s = float(pf_dn_ask) + float(pm_up_ask)
                if s < self.settings.entry_sum_max:
                    candidates.append((s, "down", "up"))
            if not candidates:
                self.status_text = "no entry sum < threshold (or extreme prices)"
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

    def _step_target_end(self) -> float:
        target = float(getattr(self.recovery, "step_entry_end", 0) or 0)
        if target > 0:
            return target
        anchor = float(getattr(self.recovery, "step_anchor", 0) or 0)
        if anchor > 0:
            self.recovery.step_entry_end = anchor + 600.0
            return self.recovery.step_entry_end
        return 0.0

    def _pf_market_by_end(self, target_end: float) -> Optional[dict]:
        best = None
        best_gap = 45.0
        for market in self.pf_markets:
            end = float(market.get("end") or 0)
            gap = abs(end - target_end)
            if end > 0 and gap < best_gap:
                best = market
                best_gap = gap
        return best

    def _hedge_for_end(self, target_end: float) -> Optional[Hedge]:
        for hedge in self.hedges:
            if hedge.status in ("OPENING", "PARTIAL", "OPEN", "CLOSING") and abs(float(hedge.market_end or 0) - target_end) < 30:
                return hedge
        return None

    def _step_recovery_open(self) -> Optional[Hedge]:
        for hedge in self.hedges:
            if hedge.status not in ("OPENING", "PARTIAL", "OPEN", "CLOSING"):
                continue
            if getattr(hedge, "strategy_mode", "") == "RECOVERY" and float(getattr(hedge, "limit_ceiling", 0) or 0) > 0:
                return hedge
        return None

    async def _pm_market_for_end(self, target_end: float) -> Optional[dict]:
        current = self.pm_market or {}
        if abs(float(current.get("end") or 0) - target_end) <= 30:
            return current
        cached = getattr(self, "_step_pm_market", None) or {}
        if abs(float(cached.get("end") or 0) - target_end) <= 30:
            return cached
        parsed = await self.pm.btc_5m_window(int(target_end - 300))
        if parsed:
            self._step_pm_market = parsed
        return parsed

    def _step_ask(self, hedge: Hedge, venue: str) -> Optional[float]:
        leg = hedge.pf_leg if venue == "pf" else hedge.pm_leg
        if venue == "pf":
            _, ask = self._pf_best(hedge.pf_market_id or leg.token_or_market, leg.direction)
            return ask
        token = hedge.pm_up_token if leg.direction == "up" else hedge.pm_down_token
        if not token:
            return None
        _, ask = self.pm.best(str(token))
        return ask

    async def _try_step_limit_entry(self, now: float) -> None:
        """Rest a 0.50 limit on the market after the skipped one, before it goes live."""
        if now < self.pf_entry_cooldown_until:
            self.status_text = f"PF cooldown {int(self.pf_entry_cooldown_until - now)}s"
            return
        if now - self._last_entry_attempt < 1.0:
            return
        if self._step_result_pending():
            # The recovery position is still settling. Its candle is not the
            # result: that guess posted 294 shares one second before +$73.50.
            self.status_text = "waiting for settlement before the recovery limit"
            return
        self._last_entry_attempt = now
        side = self.recovery.order_side()
        if side not in ("up", "down"):
            self.status_text = "recovery direction unknown — not entering"
            return
        target_end = self._step_target_end()
        if target_end <= 0:
            self.status_text = "step recovery has no entry window"
            return
        existing = self._hedge_for_end(target_end)
        if existing is not None:
            await self._retry_step_limit(existing, now)
            return
        other = self._step_recovery_open()
        if other is not None:
            await self._retry_step_limit(other, now)
            return
        if now >= target_end - 1.5:
            self.recovery.miss_step_window(f"step:{int(target_end)}", target_end)
            self.status_text = "limit 0.50 missed, next entry skips one market"
            return
        enabled = self._enabled_codes()
        pf_m = self._pf_market_by_end(target_end) if "pf" in enabled else None
        pm_market = await self._pm_market_for_end(target_end) if "pm" in enabled else None
        if "pf" in enabled and pf_m is None and "pm" not in enabled:
            self.status_text = "waiting for the predict entry market"
            return
        if "pm" in enabled and pm_market is None and "pf" not in enabled:
            self.status_text = "waiting for the polymarket entry market"
            return
        if pf_m is None and pm_market is None:
            self.status_text = "waiting for the entry market"
            return
        if self._entry_busy:
            return
        market_key = f"step:{int(target_end)}"
        if market_key in self.recovery.entered_markets:
            self.status_text = "recovery already entered this market"
            return
        shares = round(float(self.recovery.shares_for_entry()), 4)
        if not self.recovery.claim_market(market_key):
            return
        self._entry_busy = True
        logger.info(
            "[TRADING STRATEGY]\nShares: %.0f\nHedging: OFF\nPrice Threshold: DISABLED\n"
            "Entry Mode: LIMIT 0.50\nDirection: %s\nWindow end: %.0f\n"
            "[ORDER]\nPredict.fun: BUY %s %.0f shares\nPolymarket: BUY %s %.0f shares",
            shares, side.upper(), target_end, side.upper(), shares, side.upper(), shares,
        )
        try:
            opened = await self._open_step_limit(
                pf_m or {"id": ""}, pm_market, side, shares, now, target_end,
            )
        finally:
            self._entry_busy = False
        if not opened:
            self.recovery.release_market(market_key)

    async def _open_step_limit(
        self,
        pf_m: dict,
        pm_market: Optional[dict],
        direction: str,
        shares: float,
        now: float,
        market_end: float,
    ) -> bool:
        market = pm_market or {}
        up_token = str(market.get("up_token_id") or market.get("up_token") or "")
        down_token = str(market.get("down_token_id") or market.get("down_token") or "")
        pm_token = up_token if direction == "up" else down_token
        pf_id = str(pf_m.get("id") or "")
        hedge = Hedge(
            id=str(uuid.uuid4())[:8],
            opened_at=now,
            market_end=market_end,
            pf_leg=Leg(
                venue="predict",
                direction=direction,
                token_or_market=pf_id,
                requested_shares=shares,
                status="WAITING",
            ),
            pm_leg=Leg(
                venue="polymarket",
                direction=direction,
                token_or_market=pm_token,
                requested_shares=shares,
                status="WAITING",
            ),
            status="OPENING",
            pm_slug=str(market.get("slug") or ""),
            pf_market_id=pf_id,
            target_net=shares,
            pm_up_token=up_token,
            pm_down_token=down_token,
            strategy_mode="RECOVERY",
            cycle_shares=shares,
            limit_ceiling=0.50,
        )
        self.hedges.append(hedge)
        self._apply_trade_scope(hedge)
        started = now >= market_end - 300.0
        for venue in ("pf", "pm"):
            leg = hedge.pf_leg if venue == "pf" else hedge.pm_leg
            if leg.status == "SKIPPED":
                continue
            if venue == "pf" and not pf_id:
                leg.status = "WAITING"
                continue
            if venue == "pm" and not pm_token:
                leg.status = "WAITING"
                continue
            ask = self._step_ask(hedge, venue)
            if started and step_limit_price(ask) is not None:
                await self._buy_recovery_leg(hedge, venue, ask)
            elif self.settings.is_live:
                await self._buy_recovery_leg(hedge, venue, ask, rest=True)
            else:
                leg.status = "SUBMITTED"
                leg.order_id = f"paper-rest-{hedge.id}-{venue}"
                leg.entry_price = 0.50
        alive = [
            leg for leg in (hedge.pf_leg, hedge.pm_leg)
            if leg.status in ("FILLED", "SUBMITTED", "PARTIAL") or float(leg.filled_shares or 0) > 0.5
        ]
        if not alive:
            hedge.status = "CLOSED"
            hedge.closed_at = time.time()
            try:
                self.hedges.remove(hedge)
            except ValueError:
                pass
            self.status_text = "recovery entry rejected"
            return False
        hedge.status = "OPEN"
        self._stamp_position_candles(hedge)
        self._note_recovery_positions(hedge)
        self.status_text = "limit 0.50 resting before the entry market opens"
        logger.info(
            "Hedge %s OPEN step limit %s shares=%.0f ends=%.0f PF %s | PM %s",
            hedge.id, direction.upper(), shares, market_end, hedge.pf_leg.status, hedge.pm_leg.status,
        )
        return True

    async def _retry_step_limit(self, hedge: Hedge, now: float) -> None:
        end = float(hedge.market_end or 0)
        if now >= end - 1.5 and self._step_limit_unfilled(hedge):
            await self._expire_step_limit(hedge)
            return
        if now >= end:
            return
        started = now >= end - 300.0
        for venue in ("pf", "pm"):
            if venue not in venue_codes(getattr(hedge, "venues", "both")):
                continue
            leg = hedge.pf_leg if venue == "pf" else hedge.pm_leg
            if leg.status in ("FILLED", "PARTIAL", "SKIPPED") or float(leg.filled_shares or 0) > 0.5:
                continue
            if venue == "pf" and not (hedge.pf_market_id or leg.token_or_market):
                found = self._pf_market_by_end(end)
                if found:
                    hedge.pf_market_id = str(found.get("id") or "")
                    leg.token_or_market = hedge.pf_market_id
            order_id = str(leg.order_id or "")
            if leg.status == "SUBMITTED" and order_id and not order_id.startswith("paper-rest"):
                await self._poll_recovery_order(hedge, venue)
                continue
            if not started:
                if self.settings.is_live and leg.status == "WAITING":
                    await self._buy_recovery_leg(hedge, venue, self._step_ask(hedge, venue), rest=True)
                continue
            ask = self._step_ask(hedge, venue)
            if step_limit_price(ask) is None:
                continue
            if order_id.startswith("paper-rest"):
                leg.order_id = ""
                leg.status = "WAITING"
            await self._buy_recovery_leg(hedge, venue, ask)
        self._note_recovery_positions(hedge)

    def _step_result_pending(self) -> bool:
        """A step recovery position is waiting for the official dollar result."""
        for hedge in self.hedges:
            if hedge.status != "SETTLING":
                continue
            if (
                getattr(hedge, "strategy_mode", "") == "RECOVERY"
                or float(getattr(hedge, "limit_ceiling", 0) or 0) > 0
            ):
                return True
        return False

    async def _drop_unfilled_step_limits(self) -> None:
        """A plus ends recovery. A resting 0.50 limit from before that plus must go.

        Cancelling does not skip another market. The position never filled,
        so it is not a loss and it does not change the share ladder.
        """
        if self.trade_strategy != "step" or self.recovery.mode == "RECOVERY":
            return
        for hedge in list(self.hedges):
            if hedge.status == "CLOSED" or not self._step_limit_unfilled(hedge):
                continue
            for venue, leg in (("pf", hedge.pf_leg), ("pm", hedge.pm_leg)):
                order_id = str(leg.order_id or "")
                if not order_id or order_id.startswith("paper-rest") or not self.settings.is_live:
                    continue
                try:
                    if venue == "pf":
                        await self.pf.cancel_order(order_id)
                    else:
                        await self.pm.cancel_order(order_id)
                except Exception as exc:
                    logger.warning("step limit cancel %s %s: %s", hedge.id, venue, exc)
            hedge.status = "CLOSED"
            hedge.closed_at = time.time()
            hedge.close_reason = "CANCELLED"
            try:
                self.hedges.remove(hedge)
            except ValueError:
                pass
            self.trade_history.upsert(row_from_hedge(hedge))
            logger.info(
                "Hedge %s step limit cancelled — settlement was a plus, hedging stays on",
                hedge.id,
            )

    def _step_limit_unfilled(self, hedge: Hedge) -> bool:
        if float(getattr(hedge, "limit_ceiling", 0) or 0) <= 0:
            return False
        if getattr(hedge, "strategy_mode", "") != "RECOVERY":
            return False
        for leg in (hedge.pf_leg, hedge.pm_leg):
            if leg.status == "FILLED" or float(leg.filled_shares or 0) > 0.5:
                return False
        return True

    async def _expire_step_limit(self, hedge: Hedge) -> None:
        for venue, leg in (("pf", hedge.pf_leg), ("pm", hedge.pm_leg)):
            order_id = str(leg.order_id or "")
            if not order_id or order_id.startswith("paper-rest") or not self.settings.is_live:
                continue
            try:
                if venue == "pf":
                    await self.pf.cancel_order(order_id)
                else:
                    await self.pm.cancel_order(order_id)
            except Exception as exc:
                logger.warning("step limit cancel %s %s: %s", hedge.id, venue, exc)
        end = float(hedge.market_end or 0)
        self.recovery.miss_step_window(f"step:{int(end)}", end)
        hedge.status = "CLOSED"
        hedge.closed_at = time.time()
        hedge.close_reason = "MISSED"
        try:
            self.hedges.remove(hedge)
        except ValueError:
            pass
        self.status_text = "limit 0.50 missed, next entry skips one market"
        logger.info("Hedge %s step limit missed window %.0f", hedge.id, end)

    async def _try_recovery_entry(self, now: float) -> None:
        """Buy the locked side on both venues as soon as the window is tradable.

        The base-mode 0.58–0.82 band is not applied. Hedging is not opened.
        """
        if self.trade_strategy == "step":
            await self._try_step_limit_entry(now)
            return
        if now < self.pf_entry_cooldown_until:
            self.status_text = f"PF cooldown {int(self.pf_entry_cooldown_until - now)}s"
            return
        if now - self._last_entry_attempt < 1.0:
            return
        self._last_entry_attempt = now
        side = self.recovery.order_side()
        if side not in ("up", "down"):
            self.status_text = "recovery direction unknown — not entering"
            return
        if not self.pm_market:
            return
        pm_end = float(self.pm_market.get("end") or 0)
        if not pm_end or pm_end - now < self.settings.min_time_to_expiry_sec:
            self.status_text = "pm market near expiry — skip recovery entry"
            return
        pf_m = self._pick_pf_for_pm(now)
        if not pf_m:
            self.status_text = "no live predict market"
            return
        pf_end = float(pf_m.get("end") or 0)
        if not pf_end or pf_end - now < self.settings.min_time_to_expiry_sec:
            self.status_text = "predict market near expiry — skip recovery entry"
            return
        if abs(pf_end - pm_end) > 90:
            self.status_text = f"window mismatch PF={int(pf_end - now)}s PM={int(pm_end - now)}s"
            return
        window_end = min(pm_end, pf_end)
        slug = str(self.pm_market.get("slug") or "")
        market_key = f"{pf_m.get('id')}:{slug}"
        existing = None
        for hedge in self.hedges:
            if hedge.status in ("OPENING", "PARTIAL", "OPEN", "CLOSING") and abs(hedge.market_end - window_end) < 30:
                existing = hedge
                break
        if existing is not None:
            await self._retry_recovery(existing, now)
            return
        if market_key in self.recovery.entered_markets:
            self.status_text = "recovery already entered this market"
            return
        _, pf_ask = self._pf_best(str(pf_m["id"]), side)
        _, pm_ask = self._pm_best(side)
        enabled = self._enabled_codes()
        if "pf" not in enabled:
            pf_ask = None
        if "pm" not in enabled:
            pm_ask = None
        if not allows_immediate_entry(pf_ask) and not allows_immediate_entry(pm_ask):
            self.status_text = "recovery waiting for a tradable ask"
            return
        if self._entry_busy:
            return
        shares = round(float(self.recovery.shares_for_entry()), 4)
        if not self.recovery.claim_market(market_key):
            return
        self._entry_busy = True
        logger.info(
            "[TRADING STRATEGY]\nShares: %.0f\nHedging: OFF\nPrice Threshold: DISABLED\n"
            "Entry Mode: IMMEDIATE\nDirection: %s\n"
            "[ORDER]\nPredict.fun: BUY %s %.0f shares\nPolymarket: BUY %s %.0f shares",
            shares,
            side.upper(), side.upper(), shares, side.upper(), shares,
        )
        try:
            opened = await self._open_recovery_legs(pf_m, side, shares, now, window_end, pf_ask, pm_ask)
        finally:
            self._entry_busy = False
        if not opened:
            self.recovery.release_market(market_key)

    async def _open_recovery_legs(
        self,
        pf_m: dict,
        direction: str,
        shares: float,
        now: float,
        market_end: float,
        pf_ask: Optional[float],
        pm_ask: Optional[float],
    ) -> bool:
        market = self.pm_market or {}
        up_token = str(market.get("up_token_id") or market.get("up_token") or "")
        down_token = str(market.get("down_token_id") or market.get("down_token") or "")
        pm_token = up_token if direction == "up" else down_token
        hedge = Hedge(
            id=str(uuid.uuid4())[:8],
            opened_at=now,
            market_end=market_end,
            pf_leg=Leg(
                venue="predict",
                direction=direction,
                token_or_market=str(pf_m["id"]),
                requested_shares=shares,
                status="WAITING",
            ),
            pm_leg=Leg(
                venue="polymarket",
                direction=direction,
                token_or_market=pm_token,
                requested_shares=shares,
                status="WAITING",
            ),
            status="OPENING",
            pm_slug=str(market.get("slug") or ""),
            pf_market_id=str(pf_m["id"]),
            target_net=shares,
            pm_up_token=up_token,
            pm_down_token=down_token,
            strategy_mode="RECOVERY",
            cycle_shares=shares,
            limit_ceiling=0.50 if self.trade_strategy == "step" else 0.0,
        )
        self.hedges.append(hedge)
        self._apply_trade_scope(hedge)
        if allows_immediate_entry(pf_ask) and hedge.pf_leg.status != "SKIPPED":
            await self._buy_recovery_leg(hedge, "pf", float(pf_ask))
        if allows_immediate_entry(pm_ask) and hedge.pm_leg.status != "SKIPPED":
            await self._buy_recovery_leg(hedge, "pm", float(pm_ask))
        alive = [
            leg for leg in (hedge.pf_leg, hedge.pm_leg)
            if leg.status in ("FILLED", "SUBMITTED", "PARTIAL") or float(leg.filled_shares or 0) > 0.5
        ]
        if not alive:
            hedge.status = "CLOSED"
            hedge.closed_at = time.time()
            try:
                self.hedges.remove(hedge)
            except ValueError:
                pass
            self.status_text = "recovery entry rejected"
            return False
        hedge.status = "OPEN"
        self._stamp_position_candles(hedge)
        self._note_recovery_positions(hedge)
        logger.info(
            "Hedge %s OPEN recovery %s shares=%.0f PF %s %.2f@%.3f | PM %s %.2f@%.3f",
            hedge.id, direction.upper(), shares,
            hedge.pf_leg.status, float(hedge.pf_leg.filled_shares or 0), float(hedge.pf_leg.entry_price or 0),
            hedge.pm_leg.status, float(hedge.pm_leg.filled_shares or 0), float(hedge.pm_leg.entry_price or 0),
        )
        return True

    async def _retry_recovery(self, hedge: Hedge, now: float) -> None:
        """Retry the locked side only. Never flips direction and never hedges."""
        if now >= float(hedge.market_end or 0):
            return
        if hedge.retries >= self.settings.leg_retry_max:
            return
        if now - float(hedge.last_retry_at or 0) < self.settings.leg_retry_sec:
            return
        hedge.last_retry_at = now
        for venue in ("pf", "pm"):
            if venue not in venue_codes(getattr(hedge, "venues", "both")):
                continue
            leg = hedge.pf_leg if venue == "pf" else hedge.pm_leg
            if leg.status in ("FILLED", "PARTIAL", "SKIPPED") or float(leg.filled_shares or 0) > 0.5:
                continue
            if leg.status == "SUBMITTED" and leg.order_id:
                await self._poll_recovery_order(hedge, venue)
                continue
            if venue == "pf":
                _, ask = self._pf_best(hedge.pf_market_id or leg.token_or_market, leg.direction)
            else:
                _, ask = self._pm_best(leg.direction)
            if not allows_immediate_entry(ask):
                continue
            hedge.retries += 1
            await self._buy_recovery_leg(hedge, venue, float(ask))
        self._note_recovery_positions(hedge)

    async def _poll_recovery_order(self, hedge: Hedge, venue: str) -> None:
        leg = hedge.pf_leg if venue == "pf" else hedge.pm_leg
        if not leg.order_id:
            return
        try:
            raw = await (self.pf if venue == "pf" else self.pm).get_order(leg.order_id)
        except Exception as exc:
            logger.debug("recovery poll %s %s: %s", hedge.id, venue, exc)
            return
        if not raw:
            return
        fill = (
            fill_from_predict(raw, leg.requested_shares)
            if venue == "pf"
            else fill_from_pm_order(raw, leg.requested_shares)
        )
        if fill.status == "FILLED" or (fill.shares or 0) > float(leg.filled_shares or 0):
            self._apply_fill(leg, fill)
        elif fill.status == "REJECTED":
            leg.status = "WAITING"
            leg.order_id = ""

    async def _buy_recovery_leg(self, hedge: Hedge, venue: str, ask: Optional[float], *, rest: bool = False) -> None:
        leg = hedge.pf_leg if venue == "pf" else hedge.pm_leg
        if leg.status in ("FILLED", "SUBMITTED", "PARTIAL") or float(leg.filled_shares or 0) > 0.5:
            return
        ceiling = float(getattr(hedge, "limit_ceiling", 0) or 0)
        if ceiling > 0:
            limited = step_limit_price(ask, ceiling) if ask else None
            if limited is None and not rest:
                return
            px = limited if limited is not None else round(ceiling, 2)
        else:
            if not allows_immediate_entry(ask):
                return
            # Cross the ask. A late 0.99 print is a missed open, so stop short of it.
            px = min(0.97, round(float(ask) + 0.02, 2))
        shares = float(hedge.cycle_shares or hedge.target_net or self.recovery.shares_for_entry())
        direction = leg.direction
        try:
            if venue == "pf":
                resp = await self.pf.place_order(
                    hedge.pf_market_id or leg.token_or_market,
                    "BUY",
                    shares,
                    px,
                    is_yes=(direction == "up"),
                )
                fill = fill_from_predict(resp, shares)
            else:
                token = hedge.pm_up_token if direction == "up" else hedge.pm_down_token
                if not token:
                    logger.warning("RECOVERY %s no PM token for %s", hedge.id, direction)
                    return
                leg.token_or_market = str(token)
                resp = await self.pm.place_market_buy(str(token), shares, px)
                fill = fill_from_pm_order(resp, shares)
        except Exception as exc:
            logger.warning("RECOVERY %s %s failed: %s", hedge.id, venue, exc)
            return
        self._apply_fill(leg, fill)
        if fill.status != "FILLED" and float(leg.filled_shares or 0) <= 0.5 and fill.status != "SUBMITTED":
            leg.status = "WAITING"
        logger.info(
            "[ORDER]\n%s: BUY %s %.0f shares status=%s @ %.3f",
            "Predict.fun" if venue == "pf" else "Polymarket",
            direction.upper(), shares, leg.status, fill.price or px,
        )

    def _note_recovery_positions(self, hedge: Hedge) -> None:
        for platform, leg in (("predict", hedge.pf_leg), ("polymarket", hedge.pm_leg)):
            self.recovery.note_position(platform, {
                "market_id": hedge.pf_market_id if platform == "predict" else hedge.pm_slug,
                "direction": leg.direction,
                "shares": float(leg.filled_shares or leg.requested_shares or 0),
                "hedge_shares": 0.0,
                "order_status": leg.status,
                "strategy_mode": "RECOVERY",
            })

    async def _open_lock_legs(
        self,
        pf_m: dict,
        direction: str,
        venues: list[str],
        asks: dict[str, float],
        now: float,
        market_end: float,
    ) -> None:
        """Buy each venue that is in the entry window. The other one waits."""
        shares = round(float(self.recovery.shares_for_entry()), 4)
        market = self.pm_market or {}
        up_token = str(market.get("up_token_id") or market.get("up_token") or "")
        down_token = str(market.get("down_token_id") or market.get("down_token") or "")
        pm_token = up_token if direction == "up" else down_token
        hedge = Hedge(
            id=str(uuid.uuid4())[:8],
            opened_at=now,
            market_end=market_end,
            pf_leg=Leg(
                venue="predict",
                direction=direction,
                token_or_market=str(pf_m["id"]),
                requested_shares=shares,
                status="WAITING",
            ),
            pm_leg=Leg(
                venue="polymarket",
                direction=direction,
                token_or_market=pm_token,
                requested_shares=shares,
                status="WAITING",
            ),
            status="OPENING",
            pm_slug=str(market.get("slug") or ""),
            pf_market_id=str(pf_m["id"]),
            target_net=shares,
            pm_up_token=up_token,
            pm_down_token=down_token,
        )
        self.hedges.append(hedge)
        self._apply_trade_scope(hedge)
        if "pf" in venues and hedge.pf_leg.status != "SKIPPED":
            await self._buy_lock_leg(hedge, "pf", asks.get("pf") or 0)
        if "pm" in venues and hedge.pm_leg.status != "SKIPPED":
            await self._buy_lock_leg(hedge, "pm", asks.get("pm") or 0)
        alive = [
            leg for leg in (hedge.pf_leg, hedge.pm_leg)
            if leg.status in ("FILLED", "SUBMITTED", "PARTIAL") or float(leg.filled_shares or 0) > 0.5
        ]
        if not alive:
            hedge.status = "CLOSED"
            hedge.closed_at = time.time()
            try:
                self.hedges.remove(hedge)
            except ValueError:
                pass
            self.status_text = "lock entry rejected"
            return
        hedge.status = "OPEN"
        self._stamp_position_candles(hedge)
        paid = [
            float(leg.entry_price or 0)
            for leg in (hedge.pf_leg, hedge.pm_leg)
            if float(leg.filled_shares or 0) > 0
        ]
        hedge.entry_sum = round(sum(paid), 4) if paid else 0.0
        logger.info(
            "Hedge %s OPEN lock %s PF %s %.2f@%.3f | PM %s %.2f@%.3f",
            hedge.id,
            direction,
            hedge.pf_leg.status,
            float(hedge.pf_leg.filled_shares or 0),
            float(hedge.pf_leg.entry_price or 0),
            hedge.pm_leg.status,
            float(hedge.pm_leg.filled_shares or 0),
            float(hedge.pm_leg.entry_price or 0),
        )

    async def _complete_lock_entry(self, hedge: Hedge) -> None:
        """Buy a venue that is still waiting, on the side now inside the window."""
        if getattr(hedge, "strategy_mode", "BASE") == "RECOVERY":
            return
        low = float(self.settings.lock_entry_low)
        high = float(self.settings.lock_entry_high)
        for venue, leg in (("pf", hedge.pf_leg), ("pm", hedge.pm_leg)):
            if venue not in venue_codes(getattr(hedge, "venues", "both")):
                continue
            if leg.status != "WAITING" or float(leg.filled_shares or 0) > 0.5:
                continue
            if venue == "pf":
                market = hedge.pf_market_id or leg.token_or_market
                _, up_ask = self._pf_best(market, "up")
                _, down_ask = self._pf_best(market, "down")
            else:
                _, up_ask = self._pm_best("up")
                _, down_ask = self._pm_best("down")
            pick = waiting_side(leg.direction, up_ask, down_ask, low=low, high=high)
            if pick is None:
                continue
            side, ask = pick
            if side != leg.direction:
                logger.info(
                    "LOCK ENTRY %s %s reversal %s → %s @ %.3f",
                    hedge.id, venue, leg.direction, side, ask,
                )
                leg.direction = side
                if venue == "pm":
                    token = hedge.pm_up_token if side == "up" else hedge.pm_down_token
                    if token:
                        leg.token_or_market = str(token)
            await self._buy_lock_leg(hedge, venue, ask)

    async def _buy_lock_leg(self, hedge: Hedge, venue: str, ask: float) -> None:
        if getattr(hedge, "strategy_mode", "BASE") == "RECOVERY":
            return
        if not in_entry_zone(
            ask,
            low=float(self.settings.lock_entry_low),
            high=float(self.settings.lock_entry_high),
        ):
            return
        shares = float(hedge.target_net or self.settings.base_shares or 0)
        other = hedge.pm_leg if venue == "pf" else hedge.pf_leg
        if float(other.filled_shares or 0) > 0.5:
            shares = float(other.filled_shares)
        px = min(0.99, round(float(ask) + 0.02, 2))
        leg = hedge.pf_leg if venue == "pf" else hedge.pm_leg
        direction = leg.direction
        try:
            if venue == "pf":
                resp = await self.pf.place_order(
                    hedge.pf_market_id or hedge.pf_leg.token_or_market,
                    "BUY",
                    shares,
                    px,
                    is_yes=(direction == "up"),
                )
                fill = fill_from_predict(resp, shares)
            else:
                token = hedge.pm_up_token if direction == "up" else hedge.pm_down_token
                if not token:
                    logger.warning("LOCK ENTRY %s no PM token for %s", hedge.id, direction)
                    return
                leg.token_or_market = str(token)
                resp = await self.pm.place_market_buy(str(token), shares, px)
                fill = fill_from_pm_order(resp, shares)
        except Exception as exc:
            logger.warning("LOCK ENTRY %s %s failed: %s", hedge.id, venue, exc)
            return
        self._apply_fill(leg, fill)
        if fill.status != "FILLED" and float(leg.filled_shares or 0) <= 0.5:
            leg.status = "WAITING"
        logger.info(
            "LOCK ENTRY %s %s %s status=%s shares=%.2f @ %.3f",
            hedge.id, venue, direction, leg.status, float(fill.shares or 0), fill.price or px,
        )

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
            pm_up_token=str((self.pm_market or {}).get("up_token_id") or (self.pm_market or {}).get("up_token") or ""),
            pm_down_token=str((self.pm_market or {}).get("down_token_id") or (self.pm_market or {}).get("down_token") or ""),
        )
        self.hedges.append(hedge)

        # Simultaneous LIMIT buys: PF oversized, PM at target_net.
        # PF: base + pf_extra_shares (e.g. 10). PM: base (e.g. 6).
        # After PF fill → limit-sell excess at entry + pf_trim_offset.
        import asyncio as _asyncio

        target_net = round(float(shares), 4)
        # Same-direction lock uses one size on both venues. The old extra
        # applies only to the opposite-sum entry.
        if pf_dir == pm_dir:
            extra = 0.0
        else:
            extra = max(0.0, float(self.settings.pf_extra_shares or 0))
        pf_order_shares = round(target_net + extra, 4)
        pm_order_shares = target_net
        order_shares = pf_order_shares  # used by PF path / pre-checks
        hedge.target_net = target_net
        hedge.pf_leg.requested_shares = pf_order_shares
        hedge.pm_leg.requested_shares = pm_order_shares
        pf_px = min(0.99, round(float(pf_ask) + 0.03, 2))
        pm_px = min(0.99, round(float(pm_ask) + 0.01, 2))  # 1¢ slip to cross

        logger.info(
            "LIMIT ENTRY simultaneous target_net=%.2f PF order=%.4f (+%.2f extra) @ %.2f | "
            "PM size=%.4f @ %.2f | trim_offset=%.2f",
            target_net, pf_order_shares, extra, pf_px, pm_order_shares, pm_px,
            float(self.settings.pf_trim_offset or 0),
        )

        # CRITICAL: if PM cannot take target size at this ask (CLOB $1 min-notional),
        # do NOT place PF either — parallel gather used to buy PF naked → -$10 phantom.
        pm_notional = float(pm_px) * float(target_net)
        if pm_notional < 1.0:
            need_sh = 1.05 / max(float(pm_px), 0.001)
            if need_sh > target_net * 1.15:
                logger.warning(
                    "SKIP ENTRY: PM ask=%.3f needs ~%.0f sh for $1 min (target=%.1f) — "
                    "refuse both legs",
                    pm_px, need_sh, target_net,
                )
                hedge.status = "CLOSED"
                hedge.closed_at = time.time()
                hedge.pnl = 0.0
                if hedge in self.hedges:
                    try:
                        self.hedges.remove(hedge)
                    except ValueError:
                        pass
                self._entry_busy = False
                self.pf_entry_cooldown_until = time.time() + 8.0
                self.status_text = "pm min-notional impossible — skip"
                return

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
                # Only skip if already near full PF order size (base+extra)
            if pre_live >= order_shares * 0.85:
                logger.warning(
                    "PF pre-check: market %s already has %.4f shares on %s — "
                    "will NOT place new buy (prevent double-fill)",
                    pf_m["id"], pre_live, pf_dir,
                )
        except Exception as e:
            logger.debug("PF pre-check: %s", e)

        async def _pf_buy():
            if pre_live >= order_shares * 0.85:
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
            # If we already hold this token (leftover from a prior abort), do NOT
            # buy another full size — that caused 6+6=12 PM vs 6 PF.
            try:
                bal = float(await self.pm.get_token_balance(str(pm_token)) or 0)
            except Exception:
                bal = 0.0
            if bal >= size * 0.85:
                logger.warning(
                    "PM pre-check: already hold %.4f of token — skip new buy",
                    bal,
                )
                return {
                    "status": "matched",
                    "takingAmount": str(round(min(bal, size * 1.08), 4)),
                    "makingAmount": str(round(min(bal, size) * float(px), 4)),
                    "orderID": "pre-existing-pm",
                    "success": True,
                }
            need = max(0.0, size - bal)
            if need < 0.5 and bal >= 0.5:
                logger.info("PM pre-check: shortfall %.4f tiny, use balance %.4f", need, bal)
                return {
                    "status": "matched",
                    "takingAmount": str(round(bal, 4)),
                    "makingAmount": str(round(bal * float(px), 4)),
                    "orderID": "pre-existing-pm",
                    "success": True,
                }
            if bal >= 0.5:
                logger.info("PM pre-check: buy shortfall %.4f (already have %.4f)", need, bal)
                size = need
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
                        # Accept when we have most of the oversized PF order (base+extra)
                        if pos and float(pos.get("shares") or 0) >= order_shares * 0.85:
                            sh = float(pos["shares"])
                            px = float(pos.get("avg_price") or pf_px)
                            cu = float(pos.get("cost_usd") or 0) or sh * px
                            pf_fill = Fill(
                                status="FILLED", shares=min(sh, order_shares * 1.05),
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
                        # Enough shares for hedge → cancel remainder immediately
                        if got.shares >= target_net * 0.90:
                            pm_fill = Fill(
                                status="FILLED",
                                shares=round(got.shares, 4),
                                price=got.price,
                                usd=got.usd,
                                order_id=got.order_id,
                            )
                            logger.info(
                                "PM GTC enough during wait %.4f@%.4f (≥90%% target) — will cancel rest",
                                got.shares, got.price or 0,
                            )
                            try:
                                await self.pm.cancel_order(pm_fill.order_id)
                            except Exception:
                                pass
                        elif got.status == "FILLED" and got.shares > 0.5:
                            pm_fill = got
                            logger.info(
                                "PM GTC fully filled during wait %.4f@%.4f",
                                got.shares, got.price or 0,
                            )
                        elif got.shares > 0.5:
                            # partial still resting
                            pm_fill = Fill(
                                status="SUBMITTED",
                                shares=got.shares,
                                price=got.price,
                                usd=got.usd,
                                order_id=got.order_id,
                                reason="partial resting",
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

        # After wait: ALWAYS cancel any resting PM GTC so it cannot keep filling
        # beyond the hedge size (this was the 2.3 tracked vs 9 real bug).
        if pm_fill.order_id and pm_fill.status in ("SUBMITTED", "FILLED"):
            try:
                st = await self.pm.get_order(pm_fill.order_id)
                if st:
                    got = fill_from_pm_order(st, target_net)
                    if got.shares > float(pm_fill.shares or 0):
                        pm_fill = Fill(
                            status="SUBMITTED" if got.status == "SUBMITTED" else got.status,
                            shares=got.shares,
                            price=got.price or pm_fill.price,
                            usd=got.usd,
                            order_id=got.order_id,
                        )
            except Exception as e:
                logger.debug("PM get_order end-wait: %s", e)
            if pm_fill.status != "FILLED" or float(pm_fill.shares or 0) > 0:
                cancelled = False
                try:
                    cancelled = await self.pm.cancel_order(pm_fill.order_id)
                    logger.info(
                        "PM GTC cancel after wait id=%s ok=%s (shares_so_far=%.4f)",
                        str(pm_fill.order_id)[:18], cancelled, float(pm_fill.shares or 0),
                    )
                except Exception as e:
                    logger.warning("PM cancel after wait: %s", e)
                # Race / lag: poll matched size + token balance (source of truth)
                await _asyncio.sleep(1.0)
                final_sh = float(pm_fill.shares or 0)
                final_px = float(pm_fill.price or pm_ask or 0)
                final_usd = float(pm_fill.usd or 0)
                try:
                    st = await self.pm.get_order(pm_fill.order_id)
                    if st:
                        got = fill_from_pm_order(st, target_net)
                        if got.shares > final_sh:
                            final_sh = float(got.shares)
                            final_px = float(got.price or final_px)
                            final_usd = float(got.usd or 0)
                        if got.status == "SUBMITTED" and not cancelled:
                            logger.warning(
                                "PM GTC still LIVE after cancel fail id=%s matched=%.4f",
                                str(pm_fill.order_id)[:18], final_sh,
                            )
                            # force cancel_all
                            try:
                                await self.pm.cancel_all_orders()
                            except Exception:
                                pass
                except Exception:
                    pass
                try:
                    bal = await self.pm.get_token_balance(str(pm_token))
                    if bal > final_sh + 0.05:
                        logger.info(
                            "PM token balance %.4f > order matched %.4f — use balance",
                            bal, final_sh,
                        )
                        final_sh = bal
                except Exception:
                    pass
                if final_sh >= 0.5:
                    if final_usd <= 0 and final_px > 0:
                        final_usd = final_sh * final_px
                    pm_fill = Fill(
                        status="FILLED",
                        shares=round(final_sh, 4),
                        price=round(final_px, 4) if final_px else 0.0,
                        usd=round(final_usd, 4),
                        order_id=pm_fill.order_id,
                    )
                    logger.info(
                        "PM final after cancel: %.4f sh @%.4f usd=$%.4f",
                        pm_fill.shares, pm_fill.price or 0, pm_fill.usd or 0,
                    )
                elif pm_fill.status not in ("FILLED", "SUBMITTED"):
                    pm_fill = Fill(
                        status="REJECTED",
                        reason="GTC unfilled after wait",
                        order_id=pm_fill.order_id,
                        price=pm_fill.price,
                    )
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

        # Cap PF shares to planned order size (base+extra), not target_net —
        # intentional oversize is trimmed later via limit sell.
        if pf_fill.status == "FILLED" and pf_fill.shares > order_shares * 1.12:
            logger.warning(
                "PF fill shares %.4f > planned %.4f — cap to planned order size",
                pf_fill.shares, order_shares,
            )
            if pf_fill.shares > 0 and pf_fill.usd:
                px = pf_fill.usd / pf_fill.shares
                pf_fill = Fill(
                    status="FILLED", shares=order_shares, price=px,
                    usd=order_shares * px, order_id=pf_fill.order_id,
                )
            else:
                pf_fill = Fill(
                    status="FILLED", shares=order_shares,
                    price=pf_fill.price or pf_px,
                    usd=order_shares * float(pf_fill.price or pf_px),
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
                    ash = min(ash, order_shares * 1.12)
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
                # Cancel ALL open PF orders for this market (not just tracked id).
                # Multiple resting limits were the cause of 6→17 shares overfill.
                try:
                    n = await self.pf.cancel_all_open_orders(pf_m["id"])
                    logger.info("PF cancel_all_open before retry market=%s n=%s", pf_m["id"], n)
                except Exception as e:
                    logger.warning("PF cancel_all before retry: %s", e)
                    if pf_fill.order_id:
                        try:
                            await self.pf.cancel_order(str(pf_fill.order_id))
                        except Exception:
                            pass
                # Cancel races with fill — wait and re-read position + activity
                await _asyncio.sleep(2.0)
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
                        # ONE shortfall retry only — never stack full-size limits
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
                            # Keep tracking the retry order as SUBMITTED (not the
                            # original REJECTED) so we don't abort-and-naked the PM leg.
                            if pf_fill2.order_id:
                                pf_fill = Fill(
                                    status="SUBMITTED",
                                    shares=0,
                                    price=px,
                                    order_id=pf_fill2.order_id,
                                    reason="retry resting",
                                )
                        # Mark that entry path already used the one allowed extra buy
                        try:
                            hedge._pf_extra_buys = 1
                        except Exception:
                            pass

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
            # 1) Cancel any resting PM GTC
            try:
                if pm_fill.order_id:
                    await self.pm.cancel_order(pm_fill.order_id)
                await self.pm.cancel_all_orders()
            except Exception as e:
                logger.warning("PM cancel after PF reject: %s", e)
            # 2) If PM already FILLED, must SELL to unwind (cancel does nothing on matched)
            pm_sh = float(pm_fill.shares or 0) if pm_fill.status == "FILLED" else 0.0
            if pm_sh < 0.5:
                try:
                    bal = await self.pm.get_token_balance(str(pm_token))
                    if bal >= 0.5:
                        pm_sh = bal
                except Exception:
                    pass
            if pm_sh >= 0.5:
                try:
                    bid, _ = self._pm_best(pm_dir)
                    sell_px = max(0.01, round(float(bid or 0.01) - 0.02, 2))
                    logger.warning(
                        "PF reject with PM filled %.4f — UNWIND sell PM @ %.2f",
                        pm_sh, sell_px,
                    )
                    await self.pm.place_market_sell(str(pm_token), pm_sh, sell_px)
                    logger.info("PM unwind sell submitted after PF reject")
                except Exception as e:
                    logger.error("PM unwind sell FAILED after PF reject: %s", e)
                    self.last_error = f"PM naked leg after PF reject: {e}"
            else:
                logger.info("PM cleared after PF reject (no filled shares)")
            if "jurisdiction" in reason or "403" in reason or "forbidden" in reason:
                self.pf_entry_cooldown_until = time.time() + 3600.0
                self.last_error = "Predict 403: not available in your jurisdiction"
            elif "insufficient" in reason or "collateral" in reason or "balance" in reason:
                self.pf_entry_cooldown_until = time.time() + 120.0
                self.last_error = "Predict insufficient USDT balance/allowance"
            elif "precision" in reason:
                self.pf_entry_cooldown_until = time.time() + 3.0
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
            pf_s = float(pf_fill.shares or 0)
            pm_s = float(pm_fill.shares or 0)
            # Intentional PF oversize is expected (base+extra vs base).
            # Only treat as error if PM is nearly empty or PF is far above planned size.
            planned_pf = float(order_shares)
            if pf_s >= 0.5 and pm_s >= 0.5:
                # Unexpected: PM overfilled well past target_net
                if pm_s > target_net * 1.25 + 0.5:
                    excess = round(pm_s - target_net, 4)
                    try:
                        bid, _ = self._pm_best(pm_dir)
                        sell_px = max(0.01, round(float(bid or 0.01) - 0.02, 2))
                        logger.warning(
                            "Sell unexpected PM excess %.4f @ %.2f (target_net=%.2f)",
                            excess, sell_px, target_net,
                        )
                        await self.pm.place_market_sell(str(pm_token), excess, sell_px)
                        pm_s = target_net
                        self._apply_fill(
                            hedge.pm_leg,
                            Fill(
                                status="FILLED",
                                shares=round(pm_s, 4),
                                price=float(hedge.pm_leg.entry_price or pm_fill.price or 0),
                                usd=round(pm_s * float(hedge.pm_leg.entry_price or pm_fill.price or 0), 4),
                                order_id=pm_fill.order_id,
                            ),
                        )
                    except Exception as e:
                        logger.error("Failed to sell unexpected PM excess: %s", e)
                # Unexpected: PF far above planned (base+extra) → market-sell the runaway part
                if pf_s > planned_pf * 1.25 + 0.5:
                    runaway = round(pf_s - planned_pf, 4)
                    try:
                        bid, _ = self._pf_best(pf_m["id"], pf_dir)
                        sell_px = max(0.01, round(float(bid or 0.01) - 0.02, 2))
                        logger.warning(
                            "Sell runaway PF %.4f @ %.2f (planned=%.2f)",
                            runaway, sell_px, planned_pf,
                        )
                        await self.pf.place_order(
                            pf_m["id"], "SELL", runaway, sell_px,
                            is_yes=(pf_dir == "up"),
                        )
                        pf_s = planned_pf
                        self._apply_fill(
                            hedge.pf_leg,
                            Fill(
                                status="FILLED",
                                shares=round(pf_s, 4),
                                price=float(hedge.pf_leg.entry_price or pf_fill.price or 0),
                                usd=round(pf_s * float(hedge.pf_leg.entry_price or pf_fill.price or 0), 4),
                                order_id=pf_fill.order_id,
                            ),
                        )
                    except Exception as e:
                        logger.error("Failed to sell runaway PF: %s", e)
                # Catastrophic: PM almost empty while PF filled
                if pm_s < max(1.0, target_net * 0.4) and pf_s >= target_net * 0.8:
                    logger.error(
                        "Hedge %s PM nearly empty (%.2f) while PF=%.2f — UNWIND both",
                        hedge.id, pm_s, pf_s,
                    )
                    try:
                        if pm_s >= 0.5:
                            bid, _ = self._pm_best(pm_dir)
                            await self.pm.place_market_sell(
                                str(pm_token), pm_s,
                                max(0.01, round(float(bid or 0.01) - 0.02, 2)),
                            )
                        if pf_s >= 0.5:
                            bid, _ = self._pf_best(pf_m["id"], pf_dir)
                            await self.pf.place_order(
                                pf_m["id"], "SELL", pf_s,
                                max(0.01, round(float(bid or 0.01) - 0.02, 2)),
                                is_yes=(pf_dir == "up"),
                            )
                    except Exception as e:
                        logger.error("Full unwind after imbalance failed: %s", e)
                    hedge.status = "CLOSED"
                    hedge.closed_at = time.time()
                    self._entry_busy = False
                    self.pf_entry_cooldown_until = time.time() + 15.0
                    return

            hedge.status = "OPEN"
            self._entry_busy = False
            self._stamp_position_candles(hedge)
            # Real entry sum from actual fill prices (not pre-trade quote sum)
            hedge.entry_sum = round(
                float(hedge.pf_leg.entry_price or 0) + float(hedge.pm_leg.entry_price or 0), 4
            )
            logger.info(
                "Hedge %s OPEN PF %.2f@%.3f + PM %.2f@%.3f entry_sum=%.3f target_net=%.2f",
                hedge.id,
                float(hedge.pf_leg.filled_shares or pf_fill.shares),
                hedge.pf_leg.entry_price,
                float(hedge.pm_leg.filled_shares or pm_fill.shares),
                hedge.pm_leg.entry_price,
                hedge.entry_sum,
                target_net,
            )
            # Place PF limit-sell of intentional excess at entry + offset
            try:
                await self._place_pf_trim(hedge)
            except Exception as e:
                logger.error("PF trim place failed: %s", e)
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

    # ── PF trim (limit-sell intentional excess) ─────────────────

    async def _place_pf_trim(self, hedge: Hedge) -> None:
        """After entry: limit-sell PF excess so remaining ≈ target_net (= PM size).

        Sell price = entry_price + pf_trim_offset (configurable, default +0.10).
        Never places a second trim while one is already SUBMITTED/PARTIAL.
        Size = min(configured extra, actual_pf - target_net, actual_pf - pm).
        """
        if hedge.pf_trim_status in ("SUBMITTED", "PARTIAL", "FILLED"):
            return
        if hedge.status not in ("OPEN", "PARTIAL"):
            return

        target = float(hedge.target_net or self.settings.base_shares or 6.0)
        pf_sh = float(hedge.pf_leg.filled_shares or hedge.pf_leg.shares or 0)
        pm_sh = float(hedge.pm_leg.filled_shares or hedge.pm_leg.shares or 0)
        # Keep the smaller of (target_net, PM filled) as the desired remainder
        keep = max(0.0, min(target, pm_sh) if pm_sh >= 0.5 else target)
        excess = round(pf_sh - keep, 4)
        # Cap to configured extra so we never sell past intentional oversize
        max_extra = max(0.0, float(self.settings.pf_extra_shares or 0))
        if max_extra > 0:
            excess = min(excess, max_extra + 0.15)  # tiny slack for fill rounding
        if excess < 0.4:
            hedge.pf_trim_status = "SKIPPED"
            logger.info(
                "Hedge %s PF trim SKIPPED (excess=%.4f pf=%.4f keep=%.4f)",
                hedge.id, excess, pf_sh, keep,
            )
            return

        entry_px = float(hedge.pf_leg.entry_price or 0)
        if entry_px <= 0:
            hedge.pf_trim_status = "SKIPPED"
            logger.warning("Hedge %s PF trim SKIPPED — no entry price", hedge.id)
            return
        offset = float(self.settings.pf_trim_offset or 0.10)
        trim_px = min(0.99, max(0.01, round(entry_px + offset, 2)))
        mid = str(hedge.pf_market_id or hedge.pf_leg.token_or_market)
        is_yes = hedge.pf_leg.direction == "up"

        # Cancel any previous open sells on this market to avoid duplicates
        try:
            await self.pf.cancel_all_open_orders(mid)
        except Exception as e:
            logger.debug("PF trim cancel_all before place: %s", e)

        try:
            resp = await self.pf.place_order(
                mid, "SELL", excess, trim_px, is_yes=is_yes,
            )
        except Exception as e:
            hedge.pf_trim_status = "FAILED"
            logger.error("Hedge %s PF trim place error: %s", hedge.id, e)
            return

        oid = ""
        if isinstance(resp, dict):
            oid = str(
                resp.get("orderId")
                or resp.get("order_id")
                or resp.get("id")
                or (resp.get("data") or {}).get("orderId")
                or (resp.get("data") or {}).get("id")
                or ""
            )
        hedge.pf_trim_order_id = oid
        hedge.pf_trim_shares = excess
        hedge.pf_trim_target = excess
        hedge.pf_trim_px = trim_px
        hedge.pf_trim_status = "SUBMITTED"
        hedge.pf_trim_filled = 0.0
        hedge.pf_trim_proceeds = 0.0
        logger.info(
            "Hedge %s PF TRIM place SELL %.4f @ %.2f (entry=%.3f + %.2f) keep≈%.2f order=%s",
            hedge.id, excess, trim_px, entry_px, offset, keep, (oid or "?")[:16],
        )

    async def _check_pf_trim(self, hedge: Hedge, now: float) -> None:
        """Poll PF trim limit; on (partial) fill reduce tracked PF shares to remainder."""
        if hedge.status not in ("OPEN", "PARTIAL", "CLOSING"):
            return
        if hedge.pf_trim_status not in ("SUBMITTED", "PARTIAL"):
            return

        target = float(hedge.target_net or self.settings.base_shares or 6.0)
        mid = str(hedge.pf_market_id or hedge.pf_leg.token_or_market)
        is_yes = hedge.pf_leg.direction == "up"
        sold = float(hedge.pf_trim_filled or 0)
        need = float(hedge.pf_trim_target or hedge.pf_trim_shares or 0)

        # 1) Order status
        if hedge.pf_trim_order_id:
            try:
                st = await self.pf.get_order(str(hedge.pf_trim_order_id))
                if st:
                    fill = fill_from_predict(st, need)
                    matched = float(fill.shares or 0)
                    if matched > sold + 0.01:
                        delta = matched - sold
                        px = float(fill.price or hedge.pf_trim_px or 0)
                        hedge.pf_trim_filled = matched
                        hedge.pf_trim_proceeds = round(
                            float(hedge.pf_trim_proceeds or 0) + delta * px, 4,
                        )
                        sold = matched
                        logger.info(
                            "Hedge %s PF TRIM fill +%.4f (total sold=%.4f) @ %.3f",
                            hedge.id, delta, sold, px,
                        )
                    if fill.status == "FILLED" or sold >= need * 0.95:
                        hedge.pf_trim_status = "FILLED"
            except Exception as e:
                logger.debug("PF trim order poll: %s", e)

        # 2) Position remainder as truth
        try:
            pos = await self.pf.get_position_for_market(mid, is_yes=is_yes)
            live = float((pos or {}).get("shares") or 0)
            if live > 0 or hedge.pf_trim_status == "FILLED":
                # Update tracked shares to live remainder (never below 0)
                old_sh = float(hedge.pf_leg.filled_shares or 0)
                new_sh = max(0.0, live)
                if abs(new_sh - old_sh) >= 0.05:
                    # Adjust entry_usd: reduce by trim proceeds (fee-aware)
                    old_usd = float(hedge.pf_leg.entry_usd or 0)
                    proceeds = float(hedge.pf_trim_proceeds or 0)
                    if proceeds > 0 and old_usd > 0:
                        new_usd = max(0.0, old_usd - proceeds)
                    elif old_sh > 0 and new_sh < old_sh:
                        new_usd = old_usd * (new_sh / old_sh)
                    else:
                        new_usd = old_usd
                    hedge.pf_leg.filled_shares = round(new_sh, 4)
                    hedge.pf_leg.shares = round(new_sh, 4)
                    hedge.pf_leg.entry_usd = round(new_usd, 4)
                    if new_sh > 0 and new_usd > 0:
                        # Keep original entry avg if we still hold; don't invent new avg
                        pass
                    logger.info(
                        "Hedge %s PF trim reconcile shares %.4f→%.4f usd %.4f→%.4f "
                        "(target_net=%.2f)",
                        hedge.id, old_sh, new_sh, old_usd, new_usd, target,
                    )
                # Done when remainder within tolerance of target_net (or PM size)
                pm_sh = float(hedge.pm_leg.filled_shares or 0)
                keep = min(target, pm_sh) if pm_sh >= 0.5 else target
                if live <= keep + 0.35 or hedge.pf_trim_status == "FILLED":
                    if hedge.pf_trim_status != "FILLED":
                        hedge.pf_trim_status = "FILLED"
                    # Clamp display shares to keep if still slightly above
                    if live > keep + 0.01 and live <= keep + 0.35:
                        hedge.pf_leg.filled_shares = round(keep, 4)
                        hedge.pf_leg.shares = round(keep, 4)
                    logger.info(
                        "Hedge %s PF TRIM done remain=%.4f target=%.2f sold=%.4f proceeds=$%.4f",
                        hedge.id, float(hedge.pf_leg.filled_shares or 0), keep,
                        float(hedge.pf_trim_filled or 0), float(hedge.pf_trim_proceeds or 0),
                    )
        except Exception as e:
            logger.debug("PF trim position poll: %s", e)

        # Heartbeat
        last_hb = float(getattr(hedge, "_pf_trim_hb", 0) or 0)
        if now - last_hb >= 20.0 and hedge.pf_trim_status in ("SUBMITTED", "PARTIAL"):
            hedge._pf_trim_hb = now  # type: ignore[attr-defined]
            logger.info(
                "Hedge %s PF TRIM watching SELL %.2f @ %.2f sold=%.2f order=%s",
                hedge.id,
                float(hedge.pf_trim_target or 0),
                float(hedge.pf_trim_px or 0),
                float(hedge.pf_trim_filled or 0),
                (hedge.pf_trim_order_id or "?")[:14],
            )

    # ── manage open ─────────────────────────────────────────────

    async def _manage_open(self, now: float) -> None:
        for hedge in list(self.hedges):
            if hedge.status in ("CLOSED",):
                continue
            if hedge.status in ("OPENING", "PARTIAL"):
                await self._retry_partial(hedge, now)
            # Watch PF excess trim limit
            if hedge.status in ("OPEN", "PARTIAL") and hedge.pf_trim_status in (
                "SUBMITTED", "PARTIAL", "",
            ):
                if not hedge.pf_trim_status and float(hedge.pf_leg.filled_shares or 0) > 0:
                    try:
                        await self._place_pf_trim(hedge)
                    except Exception as e:
                        logger.debug("lazy PF trim place: %s", e)
                try:
                    await self._check_pf_trim(hedge, now)
                except Exception as e:
                    logger.debug("PF trim check: %s", e)
            # Past window end → settlement only (no more sells on dead markets)
            if hedge.status in ("OPEN", "PARTIAL", "CLOSING") and now >= hedge.market_end:
                if self._step_limit_unfilled(hedge):
                    await self._expire_step_limit(hedge)
                    continue
                if hedge.status == "CLOSING":
                    logger.info(
                        "Hedge %s CLOSING past end → SETTLE (stop sell spam on closed market)",
                        hedge.id,
                    )
                hedge.status = "SETTLING"
                self._release_next_entry(hedge)
                await self._settle_pair(hedge, now)
                continue
            if hedge.status == "OPEN":
                if getattr(hedge, "strategy_mode", "BASE") == "RECOVERY":
                    # Recovery is one side only. The base lock-in hedge stays off.
                    pass
                elif getattr(self.settings, "lockin_enabled", True):
                    try:
                        await self.lockin.manage(self, hedge, now)
                    except Exception as e:
                        logger.exception("lock-in hedge: %s", e)
                else:
                    await self._try_exit(hedge, now)
            if hedge.status == "CLOSING":
                await self._finish_exit(hedge, now)
            if hedge.status == "SETTLING":
                self._release_next_entry(hedge)
                await self._settle_pair(hedge, now)

    async def _retry_partial(self, hedge: Hedge, now: float) -> None:
        """Complete a PARTIAL hedge: only fill the missing leg."""
        if getattr(hedge, "strategy_mode", "BASE") == "RECOVERY":
            await self._retry_recovery(hedge, now)
            return
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
                # Already placed one extra buy this hedge — stop spamming.
                # Still cancel any leftover OPEN limits so they cannot fill later.
                try:
                    await self.pf.cancel_all_open_orders(mid)
                except Exception:
                    pass
                logger.warning(
                    "PF retry: already did 1 extra buy, live=%.4f need=%.4f — waiting (no new order)",
                    live_sh, need,
                )
            else:
                # Cancel ALL open PF orders for market, then at most ONE shortfall buy
                try:
                    n = await self.pf.cancel_all_open_orders(mid)
                    logger.info("PF cancel_all before manage retry market=%s n=%s", mid, n)
                except Exception as e:
                    logger.warning("PF cancel_all manage: %s", e)
                    if hedge.pf_leg.order_id:
                        try:
                            await self.pf.cancel_order(str(hedge.pf_leg.order_id))
                        except Exception:
                            pass
                hedge.pf_leg.order_id = ""
                import asyncio as _aio
                await _aio.sleep(2.0)
                # Re-check position after cancel — fills may have landed
                try:
                    try:
                        pos2 = await self.pf.get_position_for_market(mid, is_yes=is_yes)
                    except TypeError:
                        pos2 = await self.pf.get_position_for_market(mid)
                    if pos2:
                        live_sh = float(pos2.get("shares") or 0)
                        live_usd = float(pos2.get("cost_usd") or 0) or live_usd
                        live_avg = float(pos2.get("avg_price") or live_avg or 0)
                except Exception:
                    pass
                if live_sh >= max(0.5, need * 0.85):
                    sh = min(live_sh, need * 1.08)
                    px = (live_usd / live_sh) if (live_usd > 0 and live_sh > 0) else live_avg
                    if not (0.01 <= px <= 1.0):
                        px = 0.5
                    self._apply_fill(
                        hedge.pf_leg,
                        Fill(status="FILLED", shares=round(sh, 4), price=round(px, 4),
                             usd=round(live_usd or sh * px, 4), order_id=hedge.pf_leg.order_id),
                    )
                    logger.info("PF manage: filled after cancel_all %.4f — no new buy", sh)
                    shortfall = 0.0
                else:
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
            pf_s = float(hedge.pf_leg.filled_shares or 0)
            pm_s = float(hedge.pm_leg.filled_shares or 0)
            if pf_s >= 0.5 and pm_s >= 0.5 and max(pf_s, pm_s) / max(min(pf_s, pm_s), 0.01) > 1.25:
                logger.warning(
                    "Hedge %s promote blocked: imbalance PF=%.2f PM=%.2f — keep PARTIAL",
                    hedge.id, pf_s, pm_s,
                )
                return
            hedge.status = "OPEN"
            if not hedge.candles:
                self._stamp_position_candles(hedge)
            if not hedge.target_net:
                hedge.target_net = float(
                    hedge.pm_leg.filled_shares or self.settings.base_shares or 6.0
                )
            logger.info(
                "Hedge %s promoted PARTIAL→OPEN entry_sum=%.3f PF %.2f@%.3f PM %.2f@%.3f",
                hedge.id, hedge.entry_sum,
                hedge.pf_leg.filled_shares, hedge.pf_leg.entry_price,
                hedge.pm_leg.filled_shares, hedge.pm_leg.entry_price,
            )
            try:
                await self._place_pf_trim(hedge)
            except Exception as e:
                logger.error("PF trim place after promote: %s", e)
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
                # Intentional PF oversize (base+extra) is handled by limit trim —
                # do not market-sell that planned excess as a "double-fill".
                planned = float(hedge.target_net or 0) + float(self.settings.pf_extra_shares or 0)
                if planned <= 0:
                    planned = float(hedge.pf_leg.requested_shares or old_sh or 0)
                if (
                    old_sh > 0
                    and sh > max(old_sh * 1.5, planned * 1.35)
                    and hedge.status in ("OPEN", "PARTIAL", "CLOSING", "SETTLING")
                ):
                    new_px = ap if 0.01 <= ap <= 1.0 else (cu / sh if sh else 0)
                    if not (0.01 <= new_px <= 1.0):
                        new_px = float(hedge.pf_leg.entry_price or 0) or 0.5
                    # Target = planned entry size (base+extra), not PM — trim handles rest
                    target = planned if planned > 0 else (
                        float(hedge.pm_leg.filled_shares or 0) or old_sh
                    )
                    excess = max(0.0, sh - target)
                    logger.warning(
                        "PF DOUBLE-FILL detected [%s] live=%.4f tracked=%.4f planned=%.4f excess=%.4f",
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
        # Wait for sell fill / activity proceeds — PF sells often fill after 2–8s
        import asyncio as _aio
        if float(hedge.pf_leg.exit_usd or 0) <= 0.01:
            for _ in range(12):
                await _aio.sleep(1.0)
                if float(hedge.pf_leg.exit_usd or 0) > 0.01:
                    break
                try:
                    act = await self.pf.get_fill_price_from_activity(
                        hedge.pf_leg.token_or_market,
                        order_id="",
                        limit_price=0.0,
                        for_sell=True,
                    )
                    if act and float(act.get("cost_usd") or 0) > 0.5:
                        hedge.pf_leg.exit_usd = float(act["cost_usd"])
                        ash = float(act.get("shares") or 0) or float(hedge.pf_leg.filled_shares or 0)
                        if ash > 0:
                            hedge.pf_leg.exit_price = round(
                                float(act.get("avg_price") or (hedge.pf_leg.exit_usd / ash)), 4
                            )
                        hedge.pf_leg.status = "CLOSED"
                        logger.info(
                            "UNWIND PF activity sell $%.4f @%.4f sh=%.4f",
                            hedge.pf_leg.exit_usd, hedge.pf_leg.exit_price or 0, ash,
                        )
                        break
                except Exception:
                    pass
                # Also check position went to ~0 → sell filled at bid estimate
                try:
                    pos = await self.pf.get_position_for_market(
                        hedge.pf_leg.token_or_market,
                        is_yes=(hedge.pf_leg.direction == "up"),
                    )
                    live = float((pos or {}).get("shares") or 0)
                    if live < 0.5 and float(hedge.pf_leg.filled_shares or 0) >= 0.5:
                        bid, _ = self._pf_best(
                            hedge.pf_leg.token_or_market, hedge.pf_leg.direction,
                        )
                        px = float(bid or hedge.pf_leg.entry_price or 0.5)
                        sh = float(hedge.pf_leg.filled_shares or 0)
                        hedge.pf_leg.exit_price = px
                        hedge.pf_leg.exit_usd = round(sh * px, 4)
                        hedge.pf_leg.status = "CLOSED"
                        logger.info(
                            "UNWIND PF position flat → estimate exit $%.4f @%.4f",
                            hedge.pf_leg.exit_usd, px,
                        )
                        break
                except Exception:
                    pass

        pf_cost = float(hedge.pf_leg.entry_usd or 0)
        pf_exit = float(hedge.pf_leg.exit_usd or 0)
        # If still no exit after waits, use entry price as conservative estimate
        # (not zero — zero made phantom -$10 losses and inflated recovery)
        if pf_exit <= 0.01 and pf_cost > 0.01:
            bid, _ = self._pf_best(hedge.pf_leg.token_or_market, hedge.pf_leg.direction)
            px = float(bid or hedge.pf_leg.entry_price or 0)
            if px > 0:
                sh = float(hedge.pf_leg.filled_shares or 0)
                pf_exit = round(sh * px, 4)
                hedge.pf_leg.exit_usd = pf_exit
                hedge.pf_leg.exit_price = px
                logger.warning(
                    "UNWIND PF exit still 0 — fallback bid estimate $%.4f @%.4f",
                    pf_exit, px,
                )
        pnl = pf_exit - pf_cost
        hedge.pnl = round(pnl, 4)
        hedge.status = "CLOSED"
        hedge.exit_sum = float(hedge.pf_leg.exit_price or 0)
        logger.info(
            "Hedge %s UNWIND CLOSED pf_pnl=%.4f (exit=$%.4f cost=$%.4f)",
            hedge.id, pnl, pf_exit, pf_cost,
        )
        try:
            # Only count recovery if real economic loss (not phantom exit=0)
            if abs(pnl) > 0.05 or pf_exit > 0.01:
                self._apply_cycle_result(hedge, pnl, 0.0, hedge.pf_leg.filled_shares or 0)
            else:
                logger.warning("UNWIND skip recovery update (no real exit data)")
        except Exception as e:
            logger.warning("recovery on unwind: %s", e)

    async def _sell_both(self, hedge: Hedge) -> None:
        try:
            await self._cancel_pf_trim(hedge, reason="exit")
        except Exception:
            pass
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
            self._apply_cycle_result(hedge, pf_pnl, pm_pnl, hedge.pf_leg.filled_shares or 0)
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

    async def _cancel_pf_trim(self, hedge: Hedge, reason: str = "") -> None:
        """Cancel resting PF excess-sell before exit/settle. Reconcile fills once."""
        if hedge.pf_trim_status not in ("SUBMITTED", "PARTIAL"):
            return
        try:
            await self._check_pf_trim(hedge, time.time())
        except Exception:
            pass
        if hedge.pf_trim_order_id and hedge.pf_trim_status in ("SUBMITTED", "PARTIAL"):
            try:
                await self.pf.cancel_order(str(hedge.pf_trim_order_id))
                logger.info(
                    "Hedge %s PF TRIM cancel (%s) order=%s sold=%.4f",
                    hedge.id, reason or "exit",
                    str(hedge.pf_trim_order_id)[:16],
                    float(hedge.pf_trim_filled or 0),
                )
            except Exception as e:
                logger.debug("PF trim cancel: %s", e)
            hedge.pf_trim_status = "CANCELLED"
        try:
            mid = str(hedge.pf_market_id or hedge.pf_leg.token_or_market)
            await self.pf.cancel_all_open_orders(mid)
        except Exception:
            pass

    async def _settle_pair(self, hedge: Hedge, now: float) -> None:
        """Settle each leg from **venue resolution**, not mid-price guess.

        PF and PM can resolve differently (different oracles). We wait for
        official resolution; if still pending after 90s, fall back to quotes.
        """
        if now < hedge.market_end:
            return

        # Final trim check + cancel so we don't leave resting sells into resolution
        try:
            await self._cancel_pf_trim(hedge, reason="settle")
        except Exception as e:
            logger.debug("settle trim cleanup: %s", e)

        wait_s = now - float(hedge.market_end or now)
        pf_up_won: bool | None = None
        pm_up_won: bool | None = None
        # A definitive result from both venues is enough. The old 45s hold
        # left the new market empty even after both books were already 0/1.

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

        # The new market is already live. Enter from the ending price now.
        # This row keeps settling so the dollar profit can still be exact.
        await self._release_live_entry(hedge, pf_up_won, pm_up_won, now)

        # Missing a venue: keep the hedge in SETTLING. A 0.995 book is enough
        # once it has held for 20s. Gamma's closed flag can lag the whole
        # next window, and that was freezing every new entry.
        traded = venue_codes(getattr(hedge, "venues", "both"))
        if "pm" not in traded:
            pm_up_won = pf_up_won
        if "pf" not in traded:
            pf_up_won = pm_up_won
        both_known = (pf_up_won is not None or "pf" not in traded) and (
            pm_up_won is not None or "pm" not in traded
        )
        pm_official = bool(getattr(self.pm, "last_resolution_closed", False)) or "pm" not in traded
        if both_known and not pm_official and wait_s < 20:
            last_log = float(getattr(hedge, "_last_res_log", 0) or 0)
            if now - last_log >= 5.0:
                hedge._last_res_log = now
                logger.info(
                    "Hedge %s PM book is extreme but not closed yet (t+%.0fs) slug=%s",
                    hedge.id, wait_s, getattr(hedge, "pm_slug", "") or "?",
                )
            return
        if not both_known and wait_s < 240:
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
            "pf" in traded
            and "pm" in traded
            and pf_up_won is not None
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

            # One venue can be RESOLVED while the other is still SETTLING.
            # Do not copy its result across, and do not change Martingale.
            if pf_up_won is None or pm_up_won is None:
                logger.info(
                    "[SETTLEMENT]\nCombined Result: PENDING\n"
                    "Predict.fun: %s\nPolymarket: %s\nMartingale: UNCHANGED",
                    "RESOLVED" if pf_up_won is not None else "SETTLING",
                    "RESOLVED" if pm_up_won is not None else "SETTLING",
                )
                return
            if pf_up_won != pm_up_won:
                logger.warning(
                    "Hedge %s TRUE DIVERGENCE pf_up=%s pm_up=%s — "
                    "settle each leg independently and use the combined PnL",
                    hedge.id, pf_up_won, pm_up_won,
                )
            logger.info(
                "Hedge %s resolution final pf_up=%s pm_up=%s (slug=%s t+%.0fs)",
                hedge.id, pf_up_won, pm_up_won, slug, wait_s,
            )

        if await self._loss_needs_candle(hedge, pf_up_won, pm_up_won, now):
            return

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

        settle_legs = [hedge.pf_leg, hedge.pm_leg]
        if hedge.pf_hedge_leg is not None:
            settle_legs.append(hedge.pf_hedge_leg)
        if hedge.pm_hedge_leg is not None:
            settle_legs.append(hedge.pm_hedge_leg)
        for leg in settle_legs:
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

        pf_pnl = _leg_pnl(hedge.pf_leg) + _leg_pnl(hedge.pf_hedge_leg) if hedge.pf_hedge_leg else _leg_pnl(hedge.pf_leg)
        pm_pnl = _leg_pnl(hedge.pm_leg) + _leg_pnl(hedge.pm_hedge_leg) if hedge.pm_hedge_leg else _leg_pnl(hedge.pm_leg)
        hedge.pnl = pf_pnl + pm_pnl
        hedge.exit_sum = float(hedge.pf_leg.exit_price or 0) + float(hedge.pm_leg.exit_price or 0)
        if hedge.close_reason != "EXIT":
            hedge.close_reason = "SETTLE"
        hedge.status = "CLOSED"
        hedge.closed_at = now
        self._apply_cycle_result(hedge, pf_pnl, pm_pnl, hedge.pf_leg.filled_shares or 0)
        await self._drop_unfilled_step_limits()
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


    def _preview_leg_pnl(self, leg: Optional[Leg], won_up: Optional[bool]) -> float:
        if leg is None:
            return 0.0
        shares = float(leg.filled_shares or 0)
        if shares <= 0:
            return 0.0
        entry_cost = float(leg.entry_usd or 0)
        if entry_cost <= 0 and leg.entry_price:
            entry_cost = float(leg.entry_price) * shares
        if won_up is None:
            exit_px = 0.0
        elif leg.direction == "up":
            exit_px = 1.0 if won_up else 0.0
        else:
            exit_px = 0.0 if won_up else 1.0
        return exit_px * shares - entry_cost

    async def _ingest_hedge_candle(self, hedge: Hedge, now: float) -> None:
        slug = str(getattr(hedge, "pm_slug", "") or "")
        if "updown-5m-" not in slug:
            return
        try:
            start = float(slug.rsplit("-", 1)[-1])
        except ValueError:
            return
        await self._ingest_polymarket_candle(
            {"slug": slug, "start": start, "end": start + 300.0},
            now,
        )

    async def _loss_needs_candle(
        self,
        hedge: Hedge,
        pf_up_won: Optional[bool],
        pm_up_won: Optional[bool],
        now: float,
    ) -> bool:
        """Keep a losing hedge in SETTLING until the BTC candle is known.

        A missing candle used to freeze the next entry in BASE at 6 shares.
        """
        if self.recovery.mode == "RECOVERY" or hedge.id in self.recovery.early_ids:
            return False
        if self._unhedged_side(hedge):
            # Recovery takes the side that lost. The candle is not required.
            return False
        pf_pnl = self._preview_leg_pnl(hedge.pf_leg, pf_up_won)
        pf_pnl += self._preview_leg_pnl(hedge.pf_hedge_leg, pf_up_won)
        pm_pnl = self._preview_leg_pnl(hedge.pm_leg, pm_up_won)
        pm_pnl += self._preview_leg_pnl(hedge.pm_hedge_leg, pm_up_won)
        if pf_pnl + pm_pnl >= -1e-9:
            return False
        candle = self._candle_direction_for(hedge)
        if candle not in ("UP", "DOWN", "NEUTRAL"):
            await self._ingest_hedge_candle(hedge, now)
            candle = self._candle_direction_for(hedge)
        if candle in ("UP", "DOWN", "NEUTRAL"):
            return False
        # A missing candle must not close the loss into a base hedge.
        last_log = float(getattr(hedge, "_last_candle_log", 0) or 0)
        if now - last_log >= 5.0:
            hedge._last_candle_log = now
            logger.info(
                "[SETTLEMENT]\nCombined Result: LOSS\nCandle: %s\n"
                "Martingale: WAITING\nHedge %s stays open until the candle is known.",
                candle or "UNKNOWN",
                hedge.id,
            )
        return True

    def _unhedged_side(self, hedge: Hedge) -> Optional[str]:
        """Side still open on a venue that did not get its hedge.

        Predict can be locked while Polymarket is still a one-way UP. That UP
        is the recovery side. A hedge on the other venue does not change it.
        """
        def opened(leg: Optional[Leg]) -> Optional[str]:
            if not self._leg_filled(leg):
                return None
            return str(leg.direction or "")

        return exposed_recovery_side(
            opened(hedge.pf_leg),
            self._leg_filled(hedge.pf_hedge_leg),
            opened(hedge.pm_leg),
            self._leg_filled(hedge.pm_hedge_leg),
        )

    def _leg_filled(self, leg: Optional[Leg]) -> bool:
        return leg is not None and float(leg.filled_shares or 0) > 0.5

    def _locked_result(self, hedge: Hedge) -> Optional[str]:
        """Win or loss when both sides of a venue are already filled.

        The payout is $1 on one side. Open and the hedge price decide the
        sign before either venue settles.
        """
        locked: list[float] = []
        for main, opposite in (
            (hedge.pf_leg, hedge.pf_hedge_leg),
            (hedge.pm_leg, hedge.pm_hedge_leg),
        ):
            if not self._leg_filled(main):
                continue
            if not self._leg_filled(opposite):
                return None
            matched = min(float(main.filled_shares or 0), float(opposite.filled_shares or 0))
            cost = (float(main.entry_price or 0) + float(opposite.entry_price or 0)) * matched
            locked.append(matched - cost)
        if not locked:
            return None
        return "win" if sum(locked) >= -1e-9 else "loss"

    def _release_win(self, hedge: Hedge) -> None:
        if self.trade_strategy == "step":
            self.recovery.arm_step_win(hedge.id)
        else:
            self.recovery.arm_win(hedge.id)
        hedge._next_entry_decided = True
        hedge._candle_armed = True
        hedge._candle_released_entry = True
        logger.info(
            "[NEXT ENTRY]\nPrevious position: WIN\n"
            "Settlement: still pending, entry is not blocked\n"
            "Shares: %.0f\nHedging: ON",
            self.recovery.current_shares,
        )

    def _recovery_side_for(self, hedge: Hedge) -> Optional[str]:
        """Side to keep. A new loss uses the position's own side."""
        held = self._opened_recovery_side(hedge)
        if held:
            return held
        side = self._unhedged_side(hedge)
        if side in ("UP", "DOWN"):
            return side
        return None

    def _opened_recovery_side(self, hedge: Hedge) -> Optional[str]:
        """Side a recovery entry was bought on. Later candles do not replace it."""
        if getattr(hedge, "strategy_mode", "BASE") != "RECOVERY":
            return None
        for leg in (hedge.pf_leg, hedge.pm_leg):
            side = str(getattr(leg, "direction", "") or "").upper()
            if side in ("UP", "DOWN"):
                return side
        return None

    def _hedge_covered(self, hedge: Hedge) -> bool:
        """True when every filled venue also got its hedge."""
        return self._unhedged_side(hedge) is None and (
            self._leg_filled(hedge.pf_hedge_leg) or self._leg_filled(hedge.pm_hedge_leg)
        )

    def _step_loss_usd(self, hedge: Hedge) -> float:
        """Dollars lost if the open side pays nothing."""
        total = 0.0
        for leg in (hedge.pf_leg, hedge.pm_leg):
            if not self._leg_filled(leg):
                continue
            cost = float(leg.entry_usd or 0)
            if cost <= 0:
                cost = float(leg.entry_price or 0) * float(leg.filled_shares or 0)
            total += cost
        return total

    def _release_loss(self, hedge: Hedge, candle: str) -> None:
        held = self._opened_recovery_side(hedge)
        if self.trade_strategy == "step":
            side = held or self._unhedged_side(hedge) or candle
            if self._hedge_covered(hedge):
                self.recovery.arm_step_stay(hedge.id)
            else:
                self.recovery.arm_step_loss(
                    hedge.id,
                    str(side or ""),
                    self._step_loss_usd(hedge),
                    float(hedge.market_end or 0),
                )
            if hedge.id not in self.recovery.early_ids and hedge.id not in self.recovery.processed_ids:
                return
            hedge._next_entry_decided = True
            hedge._candle_armed = True
            hedge._candle_released_entry = True
            logger.info(
                "[NEXT ENTRY]\nPrevious position: LOSS\nStrategy: step\n"
                "Shares: %.0f\nSkip next market: %s",
                self.recovery.current_shares,
                "yes" if self.recovery.step_skip else "no",
            )
            return
        if candle not in ("UP", "DOWN") and held not in ("UP", "DOWN"):
            return
        shares = float(hedge.cycle_shares or hedge.target_net or hedge.pf_leg.filled_shares or 0)
        event = self.recovery.arm_loss_from_candle(
            hedge.id, candle, shares, keep_direction=held,
        )
        if event is None and hedge.id not in self.recovery.early_ids:
            return
        hedge._next_entry_decided = True
        hedge._candle_armed = True
        hedge._candle_released_entry = True
        logger.info(
            "[NEXT ENTRY]\nPrevious position: LOSS\nRecovery side: %s\n"
            "Same side as the losing position\n"
            "Settlement: still pending, entry is not blocked\nShares: %.0f\nHedging: OFF",
            self.recovery.recovery_direction or candle,
            self.recovery.current_shares,
        )

    def _estimated_win_pnl(self, hedge: Hedge) -> float:
        """Payout if the open side settles at $1, from the prices already filled."""
        side = (
            self._opened_recovery_side(hedge) or self._unhedged_side(hedge) or ""
        ).lower()
        if side not in ("up", "down"):
            return 0.0
        total = 0.0
        for leg in (hedge.pf_leg, hedge.pf_hedge_leg, hedge.pm_leg, hedge.pm_hedge_leg):
            if not self._leg_filled(leg):
                continue
            shares = float(leg.filled_shares or 0)
            cost = float(leg.entry_usd or 0)
            if cost <= 0:
                cost = float(leg.entry_price or 0) * shares
            if str(leg.direction or "").lower() == side:
                total += shares - cost
            else:
                total -= cost
        return total

    def _release_priced_win(self, hedge: Hedge) -> None:
        pnl = self._estimated_win_pnl(hedge)
        if self.trade_strategy == "step":
            if pnl > 1e-9:
                self.recovery.arm_step_win(hedge.id)
            self._release_recovery_open(hedge)
            return
        if self.recovery.mode == "RECOVERY" and pnl > 1e-9:
            self.recovery.arm_priced_win(hedge.id, pnl)
        elif pnl > 1e-9:
            self.recovery.arm_win(hedge.id)
        self._release_recovery_open(hedge)

    def _release_recovery_open(self, hedge: Hedge) -> None:
        """Enter the live market now. Dollar profit still settles later."""
        hedge._next_entry_decided = True
        hedge._candle_armed = True
        hedge._candle_released_entry = True
        logger.info(
            "[NEXT ENTRY]\nPrevious market ending price is in\n"
            "Settlement: still pending, entry is not blocked\n"
            "Shares: %.0f\nHedging: %s",
            self.recovery.current_shares,
            "OFF" if self.recovery.mode == "RECOVERY" else "ON",
        )

    async def _release_live_entry(
        self,
        hedge: Hedge,
        pf_up_won: Optional[bool],
        pm_up_won: Optional[bool],
        now: float,
    ) -> None:
        """Open the market that just went live from the previous close.

        Official settlement keeps running for the exact profit. One venue
        already at 0 or 1 is enough. If neither book has finished, the
        previous window's BTC close versus its open is the same signal.
        """
        if getattr(hedge, "_next_entry_decided", False):
            return
        if self.trade_strategy == "step":
            # Step recovery changes size only from the official combined PnL.
            # The previous candle is not that result: it placed a 294-share
            # limit one second before a +$73.50 settlement, and the limit
            # stayed after the mode returned to hedging.
            return
        held = self._opened_recovery_side(hedge) or self._unhedged_side(hedge)
        decision = recovery_entry_decision(held, pf_up_won, pm_up_won)
        if decision == "loss" and held in ("UP", "DOWN"):
            self._release_loss(hedge, held)
            return
        if decision == "win":
            self._release_priced_win(hedge)
            return
        if decision == "keep":
            self._release_recovery_open(hedge)
            return
        if held in ("UP", "DOWN"):
            candle = self._candle_direction_for(hedge)
            if candle not in ("UP", "DOWN"):
                await self._ingest_hedge_candle(hedge, now)
                candle = self._candle_direction_for(hedge)
            if candle == held:
                # The close is on the position's side. That payout comes off
                # the debt before the order, so a cleared debt enters at 6.
                self._release_priced_win(hedge)
                return
            if candle in ("UP", "DOWN"):
                self._release_loss(hedge, held)
                return
            return
        self._release_next_entry(hedge)

    def _release_next_entry(self, hedge: Hedge) -> None:
        """Start the next market from what is already known.

        A filled hedge has a plus or a minus before Gamma closes. A one-sided
        position uses the previous candle. Either way the old row can keep
        settling without holding the next order.
        """
        if getattr(hedge, "_next_entry_decided", False):
            return
        if self._opened_recovery_side(hedge):
            # The live entry is released from the previous market's ending
            # price. Combined dollars still settle on their own.
            return
        locked = self._locked_result(hedge)
        candle = self._candle_direction_for(hedge)
        if locked == "win":
            self._release_win(hedge)
            return
        if locked == "loss":
            self._release_loss(hedge, candle)
            return
        side = self._unhedged_side(hedge)
        if side is None or candle not in ("UP", "DOWN"):
            return
        if candle == side.upper():
            # The position is on the candle's side. Combined PnL decides.
            return
        # The position lost. The next recovery entry takes that same side.
        self._release_loss(hedge, side.upper())

    def _candle_direction_for(self, hedge: Hedge) -> str:
        book = getattr(self, "btc", None)
        if book is None:
            return "UNKNOWN"
        now = time.time()
        pf_id = str(hedge.pf_market_id or hedge.pf_leg.token_or_market or "")
        pm_id = str(hedge.pm_slug or "")
        predict = book.direction_of("predict", pf_id, now=now) if pf_id else "UNKNOWN"
        polymarket = book.direction_of("polymarket", pm_id, now=now) if pm_id else "UNKNOWN"
        shared = shared_candle_direction(predict, polymarket)
        chosen = loss_recovery_direction(predict, polymarket)
        if (
            chosen in ("UP", "DOWN")
            and chosen != shared
            and not getattr(hedge, "_logged_candle_fallback", False)
        ):
            hedge._logged_candle_fallback = True
            logger.info(
                "[PREVIOUS BTC CANDLE] Predict %s and Polymarket %s disagree. "
                "Using Polymarket %s for the recovery side.",
                predict or "UNKNOWN",
                polymarket or "UNKNOWN",
                chosen,
            )
        return chosen

    def _apply_cycle_result(
        self, hedge: Hedge, pf_pnl: float, pm_pnl: float, shares: float,
    ):
        """Combined PnL of both venues, including hedge legs already folded in.

        Polymarket entry cost includes the taker fee. A hedge leg's PnL is
        part of the venue total passed in. The candle colour is not a win.
        """
        if self.trade_strategy == "step":
            result = self.recovery.confirm_step(
                hedge.id,
                float(pf_pnl) + float(pm_pnl),
                direction=str(self._recovery_side_for(hedge) or ""),
                hedged=self._hedge_covered(hedge),
                anchor_end=float(hedge.market_end or 0),
            )
        else:
            result = self.recovery.on_pair_closed(
                hedge.id,
                pf_pnl,
                pm_pnl,
                0.0,
                shares,
                candle_direction=self._candle_direction_for(hedge),
                pf_confirmed=True,
                pm_confirmed=True,
                keep_direction=self._recovery_side_for(hedge),
                venues=str(getattr(hedge, "venues", "both") or "both"),
            )
        self._write_position_journal(hedge)
        self.trade_history.upsert(row_from_hedge(hedge))
        return result

    async def _poll_btc_references(self, now: float) -> None:
        tick = self.chainlink.latest()
        if tick:
            self.btc.add_tick(*tick)
        market = self.pm_market
        if market:
            await self._ingest_polymarket_candle(market, now)
            start = float(market.get("start") or 0)
            if start:
                prev_start = start - 300.0
                prev_slug = f"btc-updown-5m-{int(prev_start)}"
                prev = self.btc.candle("polymarket", prev_slug)
                if prev is None or prev.close is None:
                    await self._ingest_polymarket_candle(
                        {"slug": prev_slug, "start": prev_start, "end": start},
                        now,
                    )
        for row in list(getattr(self.pf, "updown_windows", []) or []):
            try:
                self.btc.apply_predict_window(row, now)
            except Exception as exc:
                logger.debug("predict btc window: %s", exc)
                continue
        current = self.btc.current_candle("predict")
        if current is not None and self.btc.previous_candle("predict") is not None:
            self._log_new_market("Predict.fun", current.market_id, "predict")

    async def _ingest_polymarket_candle(self, market: dict, now: float) -> None:
        slug = str(market.get("slug") or "")
        start = float(market.get("start") or 0)
        end = float(market.get("end") or 0)
        if not slug or start <= 0 or end <= start:
            return
        rolled = self.btc.upsert_market("polymarket", slug, start, end, now)
        try:
            raw = await self.pm.crypto_window_price(int(start))
        except Exception as exc:
            logger.debug("polymarket crypto-price: %s", exc)
            raw = None
        if raw:
            self.btc.apply_polymarket_crypto(slug, raw, now)
        candle = self.btc.candle("polymarket", slug)
        final_price = market.get("final_price")
        if candle is not None and candle.close is None and final_price is not None:
            self.btc.set_official_close(
                "polymarket", slug, float(final_price), end, "gamma.eventMetadata.finalPrice",
            )
        beat = market.get("price_to_beat")
        if candle is not None and not candle.open_official and beat is not None:
            self.btc.set_official_open(
                "polymarket", slug, float(beat), start, "gamma.eventMetadata.priceToBeat",
            )
        if rolled or self.btc.previous_candle("polymarket") is not None:
            self._log_new_market("Polymarket", slug, "polymarket")

    def _log_new_market(self, platform: str, market_id: str, book_platform: str) -> None:
        key = f"{platform}:{market_id}"
        if not market_id or key in self._logged_markets:
            return
        self._logged_markets.add(key)
        current = self.btc.candle(book_platform, market_id)
        status = current.status if current else "LIVE"
        logger.info(
            "%s",
            format_new_market(
                platform,
                market_id,
                status,
                self.btc.previous_candle(book_platform),
                self.btc.max_age_sec,
            ),
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

    def _load_trade_venues(self) -> str:
        path = Path("data/trade_venues.json")
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return "both"
        if not isinstance(data, dict):
            return "both"
        return normalize_trade_venues(data.get("venues"))

    def _load_trade_strategy(self) -> str:
        path = Path("data/trade_strategy.json")
        if not path.exists():
            return "classic"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return "classic"
        return normalize_trade_strategy(data.get("strategy") if isinstance(data, dict) else None)

    def set_trade_strategy(self, value: str) -> str:
        self.trade_strategy = normalize_trade_strategy(value)
        path = Path("data/trade_strategy.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"strategy": self.trade_strategy}), encoding="utf-8")
        logger.info("Trade strategy: %s", self.trade_strategy)
        return self.trade_strategy

    def set_trade_venues(self, value: str) -> str:
        self.trade_venues = normalize_trade_venues(value)
        path = Path("data/trade_venues.json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"venues": self.trade_venues}), encoding="utf-8")
        logger.info("Trade venues: %s", self.trade_venues)
        return self.trade_venues

    def _enabled_codes(self) -> set[str]:
        return venue_codes(self.trade_venues)

    def _step_entry_ready(self) -> bool:
        """Step recovery can post its limit before the target window is the live one."""
        if self.trade_strategy != "step" or self.recovery.mode != "RECOVERY":
            return False
        enabled = self._enabled_codes()
        if "pf" in enabled:
            return bool(self.pf_markets)
        return "pm" in enabled

    def _apply_trade_scope(self, hedge: Hedge) -> None:
        """New orders follow the dashboard switch. An open hedge keeps its own."""
        hedge.venues = self.trade_venues
        codes = venue_codes(hedge.venues)
        if "pf" not in codes:
            hedge.pf_leg.status = "SKIPPED"
            hedge.pf_leg.requested_shares = 0
        if "pm" not in codes:
            hedge.pm_leg.status = "SKIPPED"
            hedge.pm_leg.requested_shares = 0

    def _stamp_position_candles(self, hedge: Hedge) -> None:
        opened = float(hedge.opened_at or time.time())
        hedge.candles = self._candles_at_open(opened)
        self._write_position_journal(hedge)
        # The 30s candle refresh can still be on the previous window. Fill the
        # bar that just closed without holding the order.
        if self._cache_misses_last_bar(opened):
            asyncio.create_task(
                self._catch_up_position_candles(hedge.id, opened),
                name=f"candles-{hedge.id}",
            )

    @staticmethod
    def _newest_candle_start(rows) -> float:
        return max((float(row.get("start") or 0) for row in (rows or [])), default=0.0)

    def _candles_at_open(self, opened_at: float) -> dict:
        """Completed candles visible when the position opened. The live bar stays out."""
        cutoff = int(float(opened_at) // 300) * 300

        def cut(rows):
            kept = [row for row in (rows or []) if float(row.get("start") or 0) < cutoff]
            return kept[-10:]

        return {
            "predict": cut(self.candle_history.get("predict")),
            "polymarket": cut(self.candle_history.get("polymarket")),
        }

    def _cache_misses_last_bar(self, opened_at: float) -> bool:
        needed = int(float(opened_at) // 300) * 300 - 300
        if needed <= 0:
            return False
        for key in ("predict", "polymarket"):
            if self._newest_candle_start(self.candle_history.get(key)) < needed:
                return True
        return False

    def _save_position_candles(self, hedge_id: str, opened_at: float) -> None:
        fresh = self._candles_at_open(opened_at)
        if (self._newest_candle_start(fresh["predict"]) <= 0
                and self._newest_candle_start(fresh["polymarket"]) <= 0):
            return
        hedge = next((item for item in self.hedges if item.id == hedge_id), None)
        if hedge is not None:
            saved = hedge.candles or {}
            newer = (
                self._newest_candle_start(fresh["predict"]) > self._newest_candle_start(saved.get("predict"))
                or self._newest_candle_start(fresh["polymarket"]) > self._newest_candle_start(saved.get("polymarket"))
            )
            if newer or not saved:
                hedge.candles = fresh
                self._write_position_journal(hedge)
        self.trade_history.update_candles(hedge_id, hedge.candles if hedge is not None and hedge.candles else fresh)

    def _sync_open_candle_snapshots(self) -> None:
        for hedge in list(self.hedges):
            if hedge.status == "CLOSED":
                continue
            opened = float(hedge.opened_at or 0)
            if opened <= 0:
                continue
            fresh = self._candles_at_open(opened)
            saved = hedge.candles or {}
            if (self._candle_rows_changed(saved.get("predict"), fresh["predict"])
                    or self._candle_rows_changed(saved.get("polymarket"), fresh["polymarket"])):
                hedge.candles = fresh
                self._write_position_journal(hedge)
                self.trade_history.update_candles(hedge.id, fresh)
        self.trade_history.refresh_cached_candles(self.candle_history)

    @staticmethod
    def _candle_rows_changed(saved, fresh) -> bool:
        from app.strategy.trade_history import candle_print_changed
        saved = list(saved or [])
        fresh = list(fresh or [])
        if not fresh:
            return False
        if len(saved) != len(fresh):
            return True
        for old, new in zip(saved, fresh):
            try:
                if int(float(old.get("start") or 0)) != int(float(new.get("start") or 0)):
                    return True
            except (TypeError, ValueError):
                return True
            if candle_print_changed(old, new):
                return True
        return False

    async def _catch_up_position_candles(self, hedge_id: str, opened_at: float) -> None:
        try:
            async with self._candle_lock:
                if self._cache_misses_last_bar(opened_at):
                    await self._refresh_candle_history(time.time())
        except Exception as exc:
            logger.warning("position candles %s: %s", hedge_id, exc or type(exc).__name__)
            return
        self._save_position_candles(hedge_id, opened_at)

    def _write_position_journal(self, hedge: Hedge) -> None:
        path = self._position_journal
        rows: list = []
        if path.exists():
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, list):
                    rows = loaded
            except Exception:
                rows = []
        direction = hedge.pf_leg.direction or hedge.pm_leg.direction
        record = {
            "id": hedge.id,
            "opened_at": hedge.opened_at,
            "market_end": hedge.market_end,
            "status": hedge.status,
            "mode": hedge.strategy_mode,
            "venues": hedge.venues,
            "direction": direction,
            "shares": float(hedge.cycle_shares or hedge.target_net or 0),
            "pnl": float(hedge.pnl or 0),
            "candles": hedge.candles or {},
        }
        for index, row in enumerate(rows):
            if isinstance(row, dict) and row.get("id") == hedge.id:
                rows[index] = record
                break
        else:
            rows.append(record)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(rows[-200:], indent=2), encoding="utf-8")

    def _load_position_journal(self) -> list:
        path = self._position_journal
        if not path.exists():
            return []
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        return loaded if isinstance(loaded, list) else []

    async def _candle_loop(self) -> None:
        """Load candle colours off the trading tick.

        A slow Polymarket lookup must not hold the next order, and Predict
        can be shown before Polymarket answers.
        """
        while self.running:
            try:
                async with self._candle_lock:
                    await self._refresh_candle_history(time.time())
                self._sync_open_candle_snapshots()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("candle history: %s", exc or type(exc).__name__)
            await asyncio.sleep(30)

    async def _refresh_candle_history(self, now: float) -> None:
        try:
            pf_rows = await asyncio.wait_for(self._predict_candle_rows(10), timeout=12)
            pf_error = "" if pf_rows else "немає цін закриття"
        except Exception as exc:
            logger.warning("candle history predict: %s", exc or type(exc).__name__)
            pf_rows = list(self.candle_history.get("predict") or [])
            pf_error = "не відповів"
        self.candle_history = {
            "predict": pf_rows,
            "polymarket": list(self.candle_history.get("polymarket") or []),
            "updated": now,
            "predict_error": pf_error,
            "polymarket_error": str(self.candle_history.get("polymarket_error") or ""),
        }

        try:
            # Same 60s TWAP candles the Polymarket chart plots. The open window is dropped.
            # A cached body is an older slice and must not replace the current one.
            candles = await asyncio.wait_for(self.pm.chainlink_candles(timeout=8.0), timeout=12)
            if candles and not chart_candles_are_current(candles, time.time()):
                logger.warning("PM candle response is a stale cache, requesting again")
                candles = await asyncio.wait_for(self.pm.chainlink_candles(timeout=8.0), timeout=12)
            if candles and not chart_candles_are_current(candles, time.time()):
                logger.warning("PM candle response is still stale, keeping the last current chart")
                candles = []
            kept = list(self.candle_history.get("polymarket") or [])
            pm_rows = chart_candle_rows(candles, time.time(), 10) if candles else kept
            pm_error = "" if pm_rows else "не відповів"
        except Exception as exc:
            logger.warning("candle history polymarket: %s", exc or type(exc).__name__)
            pm_rows = list(self.candle_history.get("polymarket") or [])
            pm_error = "не відповів"
        self.candle_history = {
            "predict": pf_rows,
            "polymarket": pm_rows,
            "updated": time.time(),
            "predict_error": pf_error,
            "polymarket_error": pm_error,
        }
        logger.info(
            "Candle history predict=%d polymarket=%d",
            len(pf_rows),
            len(pm_rows),
        )

    async def _predict_candle_rows(self, count: int) -> list:
        from app.predict.client import round_candle

        try:
            chart = await self.pf.btc_chart_candles(count)
        except Exception as exc:
            logger.debug("predict chart candles: %s", exc or type(exc).__name__)
            chart = []
        if chart:
            return chart_candle_rows(chart, time.time(), count)

        rounds = await self.pf.get_series_rounds(
            "btc-updown-5m", first=max(count, 12), status="ENDED",
        )
        rows = []
        for rnd in rounds or []:
            parsed = round_candle(rnd)
            if not parsed:
                continue
            row = candle_row(
                parsed["start"],
                parsed.get("open"),
                parsed.get("close"),
                parsed.get("high"),
                parsed.get("low"),
            )
            if row:
                rows.append(row)
        rows.sort(key=lambda item: item["start"])
        deduped = []
        seen = set()
        for row in rows:
            if row["start"] in seen:
                continue
            seen.add(row["start"])
            deduped.append(row)
        deduped = await self._with_predict_wicks(deduped)
        return deduped[-count:]

    async def _with_predict_wicks(self, rows: list) -> list:
        """Predict publishes open and close only. BTCUSDT 5m high/low match those prints."""
        if not rows:
            return rows
        try:
            import httpx
            start = int(rows[0]["start"]) * 1000
            async with httpx.AsyncClient(timeout=4.0) as http:
                response = await http.get(
                    "https://api.binance.com/api/v3/klines",
                    params={"symbol": "BTCUSDT", "interval": "5m", "startTime": start, "limit": 30},
                )
                response.raise_for_status()
                payload = response.json()
        except Exception as exc:
            logger.debug("predict candle wicks: %s", exc or type(exc).__name__)
            return rows
        wicks = {}
        for candle in payload if isinstance(payload, list) else []:
            try:
                wicks[int(candle[0]) // 1000] = (float(candle[2]), float(candle[3]))
            except (TypeError, ValueError, IndexError):
                continue
        shaped = []
        for row in rows:
            high_low = wicks.get(int(row["start"]))
            if not high_low:
                shaped.append(row)
                continue
            richer = candle_row(row["start"], row["open"], row["close"], high_low[0], high_low[1])
            shaped.append(richer or row)
        return shaped

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
                "requested_shares": float(leg.requested_shares or 0),
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
                "pf_hedge": leg_snap(h.pf_hedge_leg) if h.pf_hedge_leg else None,
                "pm_hedge": leg_snap(h.pm_hedge_leg) if h.pm_hedge_leg else None,
                "opened_at": h.opened_at,
                "market_end": h.market_end,
                "venues": getattr(h, "venues", "both"),
                "strategy_mode": getattr(h, "strategy_mode", "BASE"),
                "candles": getattr(h, "candles", None) or {},
                "slug": slug,
                "target_net": h.target_net,
                "pf_trim": {
                    "status": h.pf_trim_status,
                    "px": h.pf_trim_px,
                    "target": h.pf_trim_target,
                    "filled": h.pf_trim_filled,
                    "proceeds": h.pf_trim_proceeds,
                    "order_id": (h.pf_trim_order_id or "")[:18],
                },
            }
            if h.status not in ("CLOSED",):
                open_hedges.append(row)

        return {
            "running": self.running,
            "mode": self.settings.mode,
            "status": self.status_text,
            "last_error": self.last_error or self.pf.last_error or self.pm.last_error,
            "settings": {
                "entry_sum_max": self.settings.entry_sum_max,
                "exit_sum_min": self.settings.exit_sum_min,
                "base_shares": self.settings.base_shares,
                "trade_venues": self.trade_venues,
                "trade_strategy": self.trade_strategy,
                "pf_extra_shares": self.settings.pf_extra_shares,
                "pf_trim_offset": self.settings.pf_trim_offset,
            },
            "recovery": self.recovery.snapshot(),
            "btc": self.btc.snapshot() if getattr(self, "btc", None) else None,
            "realized_pnl": self.trade_history.realized(),
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
            "history": [],
            "history_total": len(self.trade_history.rows),
            "history_rev": self.trade_history.rev(),
            "trade_venues": self.trade_venues,
            "trade_strategy": self.trade_strategy,
            "candle_history": self.candle_history,
            "candle_journal": self._load_position_journal()[-30:],
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
