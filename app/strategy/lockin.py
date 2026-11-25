"""Same-direction entry, then an independent opposite lock on each venue.

Buy UP (or DOWN) on Predict and Polymarket when that outcome is near $0.70,
in the same share count. Later, if that exchange's opposite ask is cheap
enough to leave at least the target net per share after fees, buy the same
number of opposite shares on that exchange only. The other exchange waits.
A fully hedged exchange is held to settlement.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Optional

from app.core.logging import logger
from app.strategy.fills import fill_from_pm_order, fill_from_predict


@dataclass(frozen=True)
class HedgePlan:
    venue: str  # pf | pm
    action: str  # hedge | wait | hold
    shares: float
    price: float
    net_per_share: float
    reason: str


def in_entry_zone(
    ask: Optional[float],
    *,
    low: float = 0.58,
    high: float = 0.82,
) -> bool:
    """About $0.70 on a fast tape. Venues are not required to match each other."""
    return ask is not None and low - 1e-9 <= float(ask) <= high + 1e-9


def entry_orders(
    pf_up: Optional[float],
    pf_down: Optional[float],
    pm_up: Optional[float],
    pm_down: Optional[float],
    *,
    target: float = 0.70,
    low: float = 0.58,
    high: float = 0.82,
) -> tuple[Optional[str], list[str]]:
    """Side to buy, and which venues are inside the window right now.

    Predict at 0.68 and Polymarket at 0.81 are both entries. Polymarket at
    0.89 has already left the window, so only Predict is bought. A venue
    that points the other way is not dragged along.
    """
    books = {
        "up": (("pf", pf_up), ("pm", pm_up)),
        "down": (("pf", pf_down), ("pm", pm_down)),
    }
    best: Optional[tuple[tuple, str, list[str]]] = None
    for side, legs in books.items():
        hit = [(venue, float(ask)) for venue, ask in legs if in_entry_zone(ask, low=low, high=high)]
        if not hit:
            continue
        avg = sum(price for _, price in hit) / len(hit)
        rank = (-len(hit), abs(avg - target))
        venues = [venue for venue, _ in hit]
        if best is None or rank < best[0]:
            best = (rank, side, venues)
    if best is None:
        return None, []
    return best[1], best[2]


def waiting_side(
    direction: str,
    up_ask: Optional[float],
    down_ask: Optional[float],
    *,
    low: float = 0.58,
    high: float = 0.82,
) -> Optional[tuple[str, float]]:
    """What an unfilled venue should buy now.

    Stay with the side already chosen while its ask is still in the window.
    If that side has left and the other side is now near $0.70, follow the
    reversal. A venue that already filled is not passed here.
    """
    current_ask = up_ask if direction == "up" else down_ask
    if in_entry_zone(current_ask, low=low, high=high):
        return direction, float(current_ask)
    other = "down" if direction == "up" else "up"
    other_ask = down_ask if other == "down" else up_ask
    if in_entry_zone(other_ask, low=low, high=high):
        return other, float(other_ask)
    return None


def choose_direction(
    pf_up: Optional[float],
    pf_down: Optional[float],
    pm_up: Optional[float],
    pm_down: Optional[float],
    *,
    target: float = 0.70,
    band: float = 0.12,
    low: Optional[float] = None,
    high: Optional[float] = None,
) -> Optional[str]:
    """Direction at least one venue is offering near the entry price."""
    if low is None:
        low = target - band
    if high is None:
        high = target + band
    direction, _venues = entry_orders(
        pf_up, pf_down, pm_up, pm_down, target=target, low=low, high=high,
    )
    return direction


def net_per_share(
    entry: float,
    opposite_ask: float,
    venue: str,
    *,
    pf_fee_bps: float = 200.0,
    pm_fee_rate: float = 0.04,
) -> float:
    """Worst locked profit for one share after the venue fee.

    Equal shares pay $1 on exactly one side. Predict takes the fee in shares,
    so the new side pays 1 - fee when it wins. Polymarket adds the fee in cash
    on the buy, and both settlements stay equal.
    """
    if entry <= 0 or opposite_ask <= 0:
        return -1.0
    if venue == "pf":
        fee = max(0.0, pf_fee_bps) / 10000.0
        return 1.0 - fee - entry - opposite_ask
    return 1.0 - entry - opposite_ask * (1.0 + max(0.0, pm_fee_rate))


def sell_net_per_share(
    entry: float,
    bid: float,
    venue: str,
    *,
    pf_fee_bps: float = 200.0,
    pm_fee_rate: float = 0.04,
) -> float:
    if entry <= 0 or bid <= 0:
        return -1.0
    if venue == "pf":
        fee = max(0.0, pf_fee_bps) / 10000.0
        return bid * (1.0 - fee) - entry
    return bid * (1.0 - max(0.0, pm_fee_rate)) - entry


def plan_venue(
    *,
    venue: str,
    entry: float,
    held_shares: float,
    hedged_shares: float,
    opposite_ask: Optional[float],
    min_net: float,
    pf_fee_bps: float,
    pm_fee_rate: float,
    max_shares: float,
    room_usd: float,
) -> HedgePlan:
    if held_shares <= 0.5 or entry <= 0:
        return HedgePlan(venue, "wait", 0.0, 0.0, 0.0, "no entry fill yet")
    gap = held_shares - hedged_shares
    if gap <= 0.5:
        return HedgePlan(venue, "hold", 0.0, 0.0, 0.0, "exchange is locked")
    if opposite_ask is None or opposite_ask <= 0.02 or opposite_ask >= 0.98:
        return HedgePlan(venue, "wait", 0.0, 0.0, 0.0, "no opposite ask")
    net = net_per_share(
        entry, float(opposite_ask), venue, pf_fee_bps=pf_fee_bps, pm_fee_rate=pm_fee_rate,
    )
    if net + 1e-12 < min_net:
        return HedgePlan(
            venue, "wait", 0.0, float(opposite_ask), net,
            f"net {net:.3f} < {min_net:.2f} — wait",
        )
    unit = float(opposite_ask)
    if venue == "pm":
        unit *= 1.0 + max(0.0, pm_fee_rate)
    room_shares = room_usd / unit if unit > 0 else 0.0
    size = min(gap, max_shares - hedged_shares, room_shares)
    if size < 0.5:
        return HedgePlan(venue, "wait", 0.0, float(opposite_ask), net, "size or cash cap")
    return HedgePlan(
        venue, "hedge", round(size, 4), float(opposite_ask), net,
        f"lock {net:.3f}/sh @ {opposite_ask:.3f}",
    )


def four_scenarios(
    pf_up: float,
    pf_down: float,
    pm_up: float,
    pm_down: float,
    cost: float,
) -> dict[str, float]:
    """Payout is $1 on the winning outcome of each venue, independently."""

    def pnl(pf_up_wins: bool, pm_up_wins: bool) -> float:
        payout = (pf_up if pf_up_wins else pf_down) + (pm_up if pm_up_wins else pm_down)
        return round(payout - cost, 4)

    return {
        "pf_up_pm_up": pnl(True, True),
        "pf_up_pm_down": pnl(True, False),
        "pf_down_pm_up": pnl(False, True),
        "pf_down_pm_down": pnl(False, False),
    }


class LockInManager:
    def __init__(self, settings: Any) -> None:
        self.settings = settings

    def _fee_args(self) -> tuple[float, float]:
        return (
            float(getattr(self.settings, "pf_taker_fee_bps", 200.0)),
            float(getattr(self.settings, "pm_taker_fee_rate", 0.04)),
        )

    async def manage(self, engine: Any, hedge: Any, now: float) -> None:
        if hedge.status != "OPEN":
            return
        if now >= float(hedge.market_end or 0):
            return
        await self._poll(engine, hedge)
        plans = self._plans(engine, hedge)
        actionable = [p for p in plans if p.action == "hedge"]
        if not actionable:
            self._log_wait(hedge, plans, now)
            return
        actionable.sort(key=lambda p: p.net_per_share, reverse=True)
        await self._place(engine, hedge, actionable[0])

    def _plans(self, engine: Any, hedge: Any) -> list[HedgePlan]:
        pf_fee, pm_fee = self._fee_args()
        min_net = float(getattr(self.settings, "lock_min_net_per_share", 0.10))
        max_shares = float(getattr(self.settings, "max_shares", 12.0))
        cap = float(getattr(self.settings, "max_total_investment_usd", 50.0))
        spent = _spent(hedge)
        room = max(0.0, cap - spent)
        out: list[HedgePlan] = []
        scope = str(getattr(hedge, "venues", "both") or "both").lower()
        for venue in ("pf", "pm"):
            if scope == "predict" and venue != "pf":
                continue
            if scope == "polymarket" and venue != "pm":
                continue
            initial = hedge.pf_leg if venue == "pf" else hedge.pm_leg
            if str(getattr(initial, "status", "") or "") == "SKIPPED":
                continue
            opp = _opp_leg(hedge, venue)
            direction = _opposite(initial.direction)
            _, ask = _book(engine, hedge, venue, direction)
            out.append(plan_venue(
                venue=venue,
                entry=float(initial.entry_price or 0),
                held_shares=float(initial.filled_shares or initial.shares or 0),
                hedged_shares=float((opp.filled_shares if opp else 0) or 0),
                opposite_ask=ask,
                min_net=min_net,
                pf_fee_bps=pf_fee,
                pm_fee_rate=pm_fee,
                max_shares=max_shares,
                room_usd=room,
            ))
        return out

    def _log_wait(self, hedge: Any, plans: list[HedgePlan], now: float) -> None:
        last = float(getattr(hedge, "_lock_log_at", 0) or 0)
        if now - last < 30:
            return
        hedge._lock_log_at = now
        text = " | ".join(f"{p.venue} {p.action} {p.reason}" for p in plans)
        sc = _scenario_text(hedge)
        logger.info("LOCK %s %s | %s", hedge.id, text, sc)

    async def _place(self, engine: Any, hedge: Any, plan: HedgePlan) -> None:
        existing = _opp_leg(hedge, plan.venue)
        if existing is not None and existing.status == "SUBMITTED":
            return
        initial = hedge.pf_leg if plan.venue == "pf" else hedge.pm_leg
        direction = _opposite(initial.direction)
        px = min(0.99, round(float(plan.price) + 0.01, 2))
        logger.info(
            "LOCK BUY hedge=%s %s %s %.2f @ %.3f (%s)",
            hedge.id, plan.venue, direction, plan.shares, px, plan.reason,
        )
        if plan.venue == "pf":
            resp = await engine.pf.place_order(
                hedge.pf_market_id or hedge.pf_leg.token_or_market,
                "BUY",
                plan.shares,
                px,
                is_yes=(direction == "up"),
            )
            fill = fill_from_predict(resp, plan.shares)
        else:
            token = _pm_token(hedge, direction)
            if not token:
                logger.warning("LOCK %s no PM token for %s", hedge.id, direction)
                return
            resp = await engine.pm.place_market_buy(str(token), plan.shares, px)
            fill = fill_from_pm_order(resp, plan.shares)
        _book_fill(hedge, plan.venue, direction, fill, px, plan.shares)
        sc = _scenario_text(hedge)
        logger.info(
            "LOCK %s %s %s status=%s filled=%.2f @ %.3f | %s",
            hedge.id, plan.venue, direction, fill.status, fill.shares or 0, fill.price or px, sc,
        )

    async def _poll(self, engine: Any, hedge: Any) -> None:
        for venue in ("pf", "pm"):
            leg = _opp_leg(hedge, venue)
            if leg is None or leg.status != "SUBMITTED" or not leg.order_id:
                continue
            try:
                raw = await (engine.pf if venue == "pf" else engine.pm).get_order(leg.order_id)
            except Exception as exc:
                logger.debug("LOCK poll %s %s: %s", hedge.id, venue, exc)
                continue
            if not raw:
                if time.time() - float(getattr(leg, "placed_at", 0) or 0) > 20:
                    try:
                        if venue == "pf":
                            await engine.pf.cancel_order(leg.order_id)
                        else:
                            await engine.pm.cancel_order(leg.order_id)
                    except Exception:
                        pass
                    leg.status = "CANCELLED"
                continue
            fill = (
                fill_from_predict(raw, leg.requested_shares)
                if venue == "pf"
                else fill_from_pm_order(raw, leg.requested_shares)
            )
            if fill.status == "FILLED" or (fill.shares or 0) > float(leg.filled_shares or 0):
                _apply_fill(leg, fill)


def _opposite(direction: str) -> str:
    return "down" if direction == "up" else "up"


def _opp_leg(hedge: Any, venue: str) -> Any:
    return hedge.pf_hedge_leg if venue == "pf" else hedge.pm_hedge_leg


def _pm_token(hedge: Any, direction: str) -> str:
    if direction == "up":
        return str(getattr(hedge, "pm_up_token", "") or "")
    return str(getattr(hedge, "pm_down_token", "") or "")


def _book(engine: Any, hedge: Any, venue: str, direction: str) -> tuple[Optional[float], Optional[float]]:
    if venue == "pf":
        return engine._pf_best(hedge.pf_market_id or hedge.pf_leg.token_or_market, direction)
    token = _pm_token(hedge, direction)
    if token:
        return engine.pm.best(str(token))
    return engine._pm_best(direction)


def _spent(hedge: Any) -> float:
    total = 0.0
    for leg in (hedge.pf_leg, hedge.pm_leg, hedge.pf_hedge_leg, hedge.pm_hedge_leg):
        if leg is None:
            continue
        usd = float(leg.entry_usd or 0)
        if usd <= 0 and leg.entry_price and (leg.filled_shares or leg.shares):
            usd = float(leg.entry_price) * float(leg.filled_shares or leg.shares or 0)
        total += usd
    return total


def _book_fill(
    hedge: Any, venue: str, direction: str, fill: Any, limit_px: float, requested: float,
) -> None:
    from app.strategy.engine import Leg

    leg = _opp_leg(hedge, venue)
    if leg is None:
        token = _pm_token(hedge, direction) if venue == "pm" else str(
            hedge.pf_market_id or hedge.pf_leg.token_or_market
        )
        leg = Leg(
            venue="predict" if venue == "pf" else "polymarket",
            direction=direction,
            token_or_market=token,
            requested_shares=requested,
        )
        if venue == "pf":
            hedge.pf_hedge_leg = leg
        else:
            hedge.pm_hedge_leg = leg
    leg.requested_shares = requested
    leg.order_id = str(fill.order_id or leg.order_id or "")
    leg.placed_at = time.time()  # type: ignore[attr-defined]
    if fill.status == "FILLED":
        _apply_fill(leg, fill)
    elif fill.order_id:
        leg.status = "SUBMITTED"
        leg.entry_price = float(fill.price or limit_px)
    else:
        leg.status = "REJECTED"
        leg.reject_reason = str(getattr(fill, "reason", "") or "rejected")


def _apply_fill(leg: Any, fill: Any) -> None:
    shares = float(fill.shares or 0)
    if shares <= 0:
        return
    price = float(fill.price or leg.entry_price or 0)
    usd = float(fill.usd or 0) or shares * price
    prev = float(leg.filled_shares or 0)
    if shares + 1e-6 < prev:
        return
    leg.filled_shares = shares
    leg.shares = shares
    leg.entry_price = price
    leg.entry_usd = usd
    leg.status = "FILLED" if fill.status == "FILLED" or shares + 0.05 >= float(leg.requested_shares or shares) else "PARTIAL"


def _scenario_text(hedge: Any) -> str:
    def sh(leg: Any, direction: str) -> float:
        if leg is None or leg.direction != direction:
            return 0.0
        return float(leg.filled_shares or leg.shares or 0)

    pf_up = sh(hedge.pf_leg, "up") + sh(hedge.pf_hedge_leg, "up")
    pf_down = sh(hedge.pf_leg, "down") + sh(hedge.pf_hedge_leg, "down")
    pm_up = sh(hedge.pm_leg, "up") + sh(hedge.pm_hedge_leg, "up")
    pm_down = sh(hedge.pm_leg, "down") + sh(hedge.pm_hedge_leg, "down")
    sc = four_scenarios(pf_up, pf_down, pm_up, pm_down, _spent(hedge))
    return " ".join(f"{k}={v:+.2f}" for k, v in sc.items())
