"""Shared BASE / RECOVERY cycle for Predict.fun and Polymarket.

BASE keeps the caller's share count and leaves hedging to the existing lock-in.
The first confirmed combined loss locks the recovery side to the previous BTC
candle. Later losses double the shares and do not touch that side. The first
confirmed combined win returns to base shares and turns hedging back on.

A result is applied once, and only after both platforms are confirmed. One
platform finishing early does not change the mode, the direction, or the size.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from app.core.logging import logger


def _money(value: float) -> float:
    return round(float(value), 6)


def shares_for_remaining_debt(debt: float) -> float:
    """Share count that matches the unpaid recovery debt.

    Debt is dollars. The next size is that amount rounded up to a whole
    share, so a small leftover stays small and a large leftover stays large.
    The base size of 6 is not used while any debt remains.
    """
    remaining = max(0.0, float(debt))
    if remaining <= 1e-9:
        return 0.0
    return float(max(1, math.ceil(remaining - 1e-9)))


def exposed_recovery_side(
    pf_open: Optional[str],
    pf_hedged: bool,
    pm_open: Optional[str],
    pm_hedged: bool,
) -> Optional[str]:
    """Side that is still a one-way bet.

    A hedge on one venue does not hide the other venue. Recovery follows the
    venue that was left without a hedge.
    """
    sides = []
    for side, hedged in ((pf_open, pf_hedged), (pm_open, pm_hedged)):
        text = str(side or "").lower()
        if hedged or text not in ("up", "down"):
            continue
        sides.append(text.upper())
    if len(set(sides)) == 1:
        return sides[0]
    return None


def recovery_entry_decision(
    held: Optional[str],
    pf_up_won: Optional[bool],
    pm_up_won: Optional[bool],
) -> Optional[str]:
    """Next entry from the price the previous market already finished at.

    One venue at 0 or 1 is enough. The other venue's official settle can
    still be running. 'loss' doubles on the same side. 'win' means every
    finished venue paid the position. 'keep' is a split. None means both
    books are still open.
    """
    side = str(held or "").upper()
    if side not in ("UP", "DOWN"):
        return None
    known = [value for value in (pf_up_won, pm_up_won) if value is not None]
    if not known:
        return None

    def won(up_won: bool) -> bool:
        return bool(up_won) if side == "UP" else not bool(up_won)

    results = [won(value) for value in known]
    if len(results) == 2 and results[0] != results[1]:
        return "keep"
    if any(not result for result in results):
        return "loss"
    return "win"


def allows_immediate_entry(ask: Optional[float]) -> bool:
    """Recovery does not wait for the base-mode price band.

    A missing or empty book is still not an order. 0.35 and 0.90 are both
    entries. The base band (about 0.58–0.82) is not consulted.
    """
    if ask is None:
        return False
    price = float(ask)
    # 0.98 and above is a market that already ran away from its open.
    return 0.02 < price < 0.97


def settlement_ready(predict_confirmed: bool, polymarket_confirmed: bool) -> bool:
    return bool(predict_confirmed) and bool(polymarket_confirmed)


def step_recovery_shares(loss_usd: float) -> float:
    """Shares for the skip-one recovery.

    The lost dollars are covered at a 0.50 entry (loss × 2 shares) and 3
    shares are added on top. $3 lost is 9 shares. $3 plus $4.50 is 18.
    """
    loss = max(0.0, float(loss_usd))
    if loss <= 1e-9:
        return 0.0
    return float(int(round(loss * 2)) + 3)


def step_limit_price(ask: Optional[float], ceiling: float = 0.50) -> Optional[float]:
    """Price for a resting recovery buy. 0.49 is taken. 0.85 is not."""
    if ask is None:
        return None
    price = float(ask)
    if price <= 0.02 or price > float(ceiling) + 1e-9:
        return None
    return round(min(price, float(ceiling)), 2)


def normalize_trade_strategy(value: Optional[str]) -> str:
    """classic keeps the current hedge and recovery. step skips one market."""
    text = str(value or "").strip().lower()
    if text in ("step", "skip", "limit", "through", "alternate"):
        return "step"
    return "classic"


def normalize_trade_venues(value: Optional[str]) -> str:
    """both, predict, or polymarket. Anything else stays on both venues."""
    text = str(value or "").strip().lower()
    if text in ("predict", "pf", "bn", "predict.fun"):
        return "predict"
    if text in ("polymarket", "pm", "poly"):
        return "polymarket"
    return "both"


def venue_codes(scope: Optional[str]) -> set[str]:
    chosen = normalize_trade_venues(scope)
    if chosen == "predict":
        return {"pf"}
    if chosen == "polymarket":
        return {"pm"}
    return {"pf", "pm"}


def paid_on_every_venue(pf_pnl: float, pm_pnl: float, venues: Optional[str] = "both") -> bool:
    """A plus on every venue that was actually traded.

    One selected venue is the whole result. Both venues still need both plus.
    """
    scope = normalize_trade_venues(venues)
    if scope == "predict":
        return float(pf_pnl) > 1e-9
    if scope == "polymarket":
        return float(pm_pnl) > 1e-9
    return float(pf_pnl) > 1e-9 and float(pm_pnl) > 1e-9


@dataclass
class RecoveryConfig:
    base_shares: float = 6.0
    shares_step: float = 6.0
    max_shares: float = 12.0
    max_debt_usd: float = 50.0
    multiplier: float = 2.0
    hedging_base: bool = True
    hedging_recovery: bool = False

    def __post_init__(self) -> None:
        if self.base_shares <= 0:
            raise ValueError("BASE_SHARES must be positive")
        if self.multiplier < 2:
            raise ValueError("MARTINGALE_MULTIPLIER must be at least 2")


@dataclass
class RecoveryEvent:
    trade_id: str
    bn_pnl: float
    pm_pnl: float
    fees: float
    pair_pnl: float
    debt_before: float
    debt_after: float
    recovery_level: int
    next_shares: float
    reason: str
    closed_shares: float = 0.0
    previous_mode: str = "BASE"
    mode: str = "BASE"
    direction: str = ""
    direction_changed: bool = False
    hedging: bool = True

    def line(self) -> str:
        direction = self.direction or "RESET"
        if self.reason == "loss" and self.mode == "RECOVERY" and self.previous_mode == "BASE":
            return (
                "[SETTLEMENT]\n"
                "Combined Result: LOSS\n"
                "[STRATEGY UPDATE]\n"
                "Previous Mode: BASE\n"
                "Previous Result: LOSS\n"
                "Recovery Mode: ON\n"
                "[RECOVERY DIRECTION]\n"
                f"Direction: {direction}\n"
                "Reason: Same side as the losing position\n"
                "Direction Locked: TRUE\n"
                "[TRADING STRATEGY]\n"
                f"Shares: {self.next_shares:.0f}\n"
                "Hedging: OFF\n"
                "Price Threshold: DISABLED\n"
                "Entry Mode: IMMEDIATE"
            )
        if self.reason == "loss" and self.mode == "RECOVERY":
            return (
                "[SETTLEMENT]\n"
                "Combined Result: LOSS\n"
                "[STRATEGY UPDATE]\n"
                "Mode: RECOVERY\n"
                f"Recovery Direction: {direction}\n"
                f"Previous Shares: {self.closed_shares:.0f}\n"
                f"Next Shares: {self.next_shares:.0f}\n"
                "Direction Changed: FALSE\n"
                "Hedging: OFF"
            )
        if self.reason == "loss":
            return (
                "[SETTLEMENT]\n"
                "Combined Result: LOSS\n"
                "[STRATEGY UPDATE]\n"
                "Mode: BASE\n"
                "Recovery Mode: OFF\n"
                f"Reason: candle {direction or 'UNKNOWN'}\n"
                f"Shares: {self.next_shares:.0f}\n"
                "Hedging: ON"
            )
        if self.reason == "win" and self.mode == "RECOVERY":
            return (
                "[SETTLEMENT]\n"
                "Combined Result: WIN\n"
                "[STRATEGY UPDATE]\n"
                "Mode: RECOVERY\n"
                f"Recovery Direction: {direction}\n"
                f"Shares: {self.next_shares:.0f}\n"
                f"Recovery debt left: {self.debt_after:.2f}\n"
                "Hedging: OFF"
            )
        if self.reason == "win":
            return (
                "[SETTLEMENT]\n"
                "Combined Result: WIN\n"
                "[STRATEGY UPDATE]\n"
                f"Previous Mode: {self.previous_mode}\n"
                "New Mode: BASE\n"
                "Recovery Direction: RESET\n"
                f"Shares: {self.next_shares:.0f}\n"
                "Hedging: ON"
            )
        if self.reason == "neutral":
            return (
                "[SETTLEMENT]\n"
                "Combined Result: NEUTRAL\n"
                "[STRATEGY UPDATE]\n"
                f"Mode: {self.mode}\n"
                f"Recovery Direction: {direction or 'UNCHANGED'}\n"
                f"Shares: {self.next_shares:.0f}\n"
                "Direction Changed: FALSE\n"
                f"Hedging: {'ON' if self.hedging else 'OFF'}"
            )
        return (
            f"RECOVERY {self.reason} pair {self.pair_pnl:+.4f} "
            f"mode {self.mode} next {self.next_shares:.0f}"
        )


class RecoveryBook:
    """One strategy mode for both platforms. Persisted across restarts."""

    def __init__(self, config: RecoveryConfig, path: Path | None = None) -> None:
        self.config = config
        self.path = path
        self.mode = "BASE"
        self.recovery_direction: Optional[str] = None
        self.direction_locked = False
        self.current_shares = float(config.base_shares)
        self.entry_shares = 0.0
        self.recovery_debt = 0.0
        self.recovery_level = 0
        self.last_trade_id = ""
        self.processed_ids: list[str] = []
        self.entered_markets: list[str] = []
        self.previous_market_id = ""
        self.pending: dict[str, dict] = {}
        self.positions: dict[str, dict] = {}
        self.early_ids: dict[str, dict] = {}
        self.step_loss = 0.0
        self.step_skip = 0
        self.step_anchor = 0.0
        self.step_entry_end = 0.0
        self.step_seen: dict[str, str] = {}
        self.step_missed: list[str] = []
        self.load()

    def reset(self) -> None:
        """Manual return to base size. Processed settlements stay processed."""
        self.mode = "BASE"
        self.recovery_direction = None
        self.direction_locked = False
        self.current_shares = float(self.config.base_shares)
        self.entry_shares = 0.0
        self.recovery_debt = 0.0
        self.recovery_level = 0
        self.pending = {}
        self.early_ids = {}
        self.step_loss = 0.0
        self.step_skip = 0
        self.step_anchor = 0.0
        self.step_entry_end = 0.0
        self.step_seen = {}
        self.step_missed = []
        self.save()

    def shares_for_entry(self) -> float:
        return float(self.current_shares)

    def hedging_enabled(self) -> bool:
        if self.mode == "RECOVERY":
            return bool(self.config.hedging_recovery)
        return bool(self.config.hedging_base)

    def order_side(self) -> Optional[str]:
        if self.mode != "RECOVERY":
            return None
        if self.recovery_direction not in ("UP", "DOWN"):
            return None
        return self.recovery_direction.lower()

    def _collect_win(self, pair_pnl: float) -> None:
        """Pay a both-venue win against the debt.

        Debt left means the next size is that remainder, on the same side.
        Debt cleared means the base hedge of 6 shares.
        """
        remaining = _money(max(0.0, self.recovery_debt - float(pair_pnl)))
        self.entry_shares = 0.0
        if remaining <= 1e-9:
            self.mode = "BASE"
            self.recovery_direction = None
            self.direction_locked = False
            self.recovery_level = 0
            self.recovery_debt = 0.0
            self.current_shares = float(self.config.base_shares)
            return
        self.mode = "RECOVERY"
        self.direction_locked = self.recovery_direction in ("UP", "DOWN")
        self.recovery_debt = remaining
        self.current_shares = shares_for_remaining_debt(remaining)

    def entry_block(self) -> str:
        if self.mode == "RECOVERY" and self.recovery_direction not in ("UP", "DOWN"):
            return "recovery direction unknown — not entering"
        return ""

    def claim_market(self, market_key: str) -> bool:
        """One entry attempt per market id. False when it was already claimed."""
        key = str(market_key or "")
        if not key or key in self.entered_markets:
            return False
        self.entered_markets.append(key)
        if len(self.entered_markets) > 200:
            self.entered_markets = self.entered_markets[-100:]
        self.previous_market_id = key
        self.entry_shares = float(self.current_shares)
        self.save()
        return True

    def release_market(self, market_key: str) -> None:
        """Give the market back when no order was accepted."""
        key = str(market_key or "")
        if key in self.entered_markets:
            self.entered_markets = [item for item in self.entered_markets if item != key]
            self.save()

    def note_position(self, platform: str, payload: dict) -> None:
        self.positions[str(platform)] = dict(payload)
        self.save()

    def note_platform(
        self,
        trade_id: str,
        platform: str,
        pnl: Optional[float],
        confirmed: bool,
    ) -> None:
        """Remember one venue. Does not change shares or direction."""
        if not trade_id or trade_id in self.processed_ids:
            return
        slot = self.pending.setdefault(trade_id, {"pf": None, "pm": None, "pf_ok": False, "pm_ok": False})
        key = "pf" if platform in ("pf", "predict", "bn") else "pm"
        slot[key] = None if pnl is None else _money(pnl)
        slot[f"{key}_ok"] = bool(confirmed)
        self.save()

    def arm_loss_from_candle(
        self,
        trade_id: str,
        candle_direction: str,
        pair_shares: float = 0.0,
        keep_direction: Optional[str] = None,
    ) -> Optional[RecoveryEvent]:
        """Step the ladder from the BTC candle before either venue has settled.

        Used only when the position was one side and that side is opposite the
        candle. Official PnL later confirms the debt and does not double again.
        A position opened in recovery keeps the side it was opened with. A
        later candle, and a book that was briefly returned to base, do not
        replace that side.
        """
        if not trade_id or trade_id in self.processed_ids or trade_id in self.early_ids:
            return None
        candle = str(candle_direction or "").upper()
        held = str(keep_direction or "").upper()
        if held not in ("UP", "DOWN") and candle not in ("UP", "DOWN"):
            return None
        if held in ("UP", "DOWN"):
            closed = float(pair_shares or self.entry_shares or self.current_shares or self.config.base_shares)
        else:
            closed = float(self.entry_shares or self.current_shares or pair_shares or self.config.base_shares)
        self.early_ids[trade_id] = {
            "kind": "loss",
            "candle": held or candle,
            "shares_before": self.current_shares,
            "mode_before": self.mode,
            "direction_before": self.recovery_direction,
            "locked_before": self.direction_locked,
            "level_before": self.recovery_level,
        }
        previous_mode = self.mode
        if self.mode != "RECOVERY" or self.recovery_direction not in ("UP", "DOWN"):
            self.mode = "RECOVERY"
            self.recovery_direction = held if held in ("UP", "DOWN") else candle
            self.direction_locked = True
            self.recovery_level += 1
            self.current_shares = closed * float(self.config.multiplier)
        else:
            self.recovery_level += 1
            self.current_shares = closed * float(self.config.multiplier)
            self.direction_locked = True
        self.entry_shares = 0.0
        event = RecoveryEvent(
            trade_id=trade_id,
            bn_pnl=0.0,
            pm_pnl=0.0,
            fees=0.0,
            pair_pnl=0.0,
            debt_before=self.recovery_debt,
            debt_after=self.recovery_debt,
            recovery_level=self.recovery_level,
            next_shares=self.current_shares,
            reason="loss",
            closed_shares=closed,
            previous_mode=previous_mode,
            mode=self.mode,
            direction=self.recovery_direction or "",
            direction_changed=False,
            hedging=self.hedging_enabled(),
        )
        self.save()
        logger.info("%s", event.line())
        return event

    def arm_win(self, trade_id: str) -> Optional[RecoveryEvent]:
        """The position is already a plus. Next entry is base with hedging.

        Official settlement later records the real PnL and does not reopen
        a recovery for the same market.
        """
        if not trade_id or trade_id in self.processed_ids or trade_id in self.early_ids:
            return None
        previous_mode = self.mode
        shares_before = self.current_shares
        self.early_ids[trade_id] = {
            "kind": "win",
            "shares_before": shares_before,
            "mode_before": previous_mode,
            "direction_before": self.recovery_direction,
            "locked_before": self.direction_locked,
            "level_before": self.recovery_level,
        }
        self.mode = "BASE"
        self.recovery_direction = None
        self.direction_locked = False
        self.recovery_level = 0
        self.recovery_debt = 0.0
        self.current_shares = float(self.config.base_shares)
        self.entry_shares = 0.0
        event = RecoveryEvent(
            trade_id=trade_id,
            bn_pnl=0.0,
            pm_pnl=0.0,
            fees=0.0,
            pair_pnl=0.0,
            debt_before=self.recovery_debt,
            debt_after=0.0,
            recovery_level=0,
            next_shares=self.current_shares,
            reason="win",
            closed_shares=float(shares_before or 0),
            previous_mode=previous_mode,
            mode="BASE",
            direction="",
            direction_changed=False,
            hedging=True,
        )
        self.save()
        logger.info("%s", event.line())
        return event

    def arm_priced_win(self, trade_id: str, pair_pnl: float) -> Optional[RecoveryEvent]:
        """A same-side close already has a payout from the entry prices.

        The profit comes off the debt before the next order. Debt cleared
        is the base hedge of 6. Debt left stays in recovery at that size.
        Official settlement later replaces the estimate with the real sum.
        """
        if not trade_id or trade_id in self.processed_ids or trade_id in self.early_ids:
            return None
        if float(pair_pnl) <= 1e-9:
            return None
        previous_mode = self.mode
        shares_before = self.current_shares
        debt_before = self.recovery_debt
        self.early_ids[trade_id] = {
            "kind": "priced_win",
            "estimated_pnl": float(pair_pnl),
            "shares_before": shares_before,
            "mode_before": previous_mode,
            "direction_before": self.recovery_direction,
            "locked_before": self.direction_locked,
            "level_before": self.recovery_level,
            "debt_before": debt_before,
        }
        self._collect_win(pair_pnl)
        event = RecoveryEvent(
            trade_id=trade_id,
            bn_pnl=0.0,
            pm_pnl=0.0,
            fees=0.0,
            pair_pnl=float(pair_pnl),
            debt_before=debt_before,
            debt_after=self.recovery_debt,
            recovery_level=self.recovery_level,
            next_shares=self.current_shares,
            reason="win",
            closed_shares=float(shares_before or 0),
            previous_mode=previous_mode,
            mode=self.mode,
            direction=self.recovery_direction or "",
            direction_changed=False,
            hedging=self.hedging_enabled(),
        )
        self.save()
        logger.info("%s", event.line())
        return event

    def on_pair_closed(
        self,
        trade_id: str,
        bn_pnl: float,
        pm_pnl: float,
        fees: float = 0.0,
        pair_shares: float = 0.0,
        candle_direction: Optional[str] = None,
        pf_confirmed: bool = True,
        pm_confirmed: bool = True,
        keep_direction: Optional[str] = None,
        venues: str = "both",
    ) -> Optional[RecoveryEvent]:
        """Apply a finished cycle once both venues are confirmed."""
        if trade_id and trade_id in self.processed_ids:
            return None
        if trade_id:
            slot = self.pending.setdefault(
                trade_id, {"pf": None, "pm": None, "pf_ok": False, "pm_ok": False},
            )
            slot["venues"] = normalize_trade_venues(venues)
            # A venue that was not traded is already confirmed at zero.
            if slot["venues"] == "predict":
                self.note_platform(trade_id, "pm", 0.0, True)
            elif slot["venues"] == "polymarket":
                self.note_platform(trade_id, "pf", 0.0, True)
        if trade_id and trade_id in self.early_ids:
            self.note_platform(trade_id, "pf", bn_pnl, pf_confirmed)
            self.note_platform(trade_id, "pm", pm_pnl, pm_confirmed)
            return self._confirm_early(trade_id, fees, candle_direction)
        self.note_platform(trade_id, "pf", bn_pnl, pf_confirmed)
        self.note_platform(trade_id, "pm", pm_pnl, pm_confirmed)
        return self._maybe_apply(trade_id, fees, pair_shares, candle_direction, keep_direction)

    def _confirm_early(
        self,
        trade_id: str,
        fees: float,
        candle_direction: Optional[str],
    ) -> Optional[RecoveryEvent]:
        """Official PnL for a ladder step that the candle already took."""
        slot = self.pending.get(trade_id) or {}
        if not settlement_ready(bool(slot.get("pf_ok")), bool(slot.get("pm_ok"))):
            return None
        snap = self.early_ids.get(trade_id) or {}
        pf_pnl = float(slot.get("pf") or 0.0)
        pm_pnl = float(slot.get("pm") or 0.0)
        pair_pnl = _money(pf_pnl + pm_pnl - float(fees or 0.0))
        previous_mode = self.mode
        armed_win = str(snap.get("kind") or "") == "win"
        venues = str((self.pending.get(trade_id) or {}).get("venues") or "both")
        both_plus = paid_on_every_venue(pf_pnl, pm_pnl, venues)
        if str(snap.get("kind") or "") == "priced_win":
            debt_before = float(snap.get("debt_before") or 0.0)
            direction = str(snap.get("direction_before") or "").upper()
            shares_before = float(snap.get("shares_before") or self.config.base_shares)
            self.recovery_debt = debt_before
            self.current_shares = shares_before
            if direction in ("UP", "DOWN"):
                self.recovery_direction = direction
                self.direction_locked = True
                self.mode = "RECOVERY"
            if pair_pnl > 1e-9 and both_plus:
                self._collect_win(pair_pnl)
                reason = "win"
            elif pair_pnl > 1e-9:
                remaining = _money(max(0.0, debt_before - pair_pnl))
                self.entry_shares = 0.0
                if remaining <= 1e-9:
                    self.mode = "BASE"
                    self.recovery_direction = None
                    self.direction_locked = False
                    self.recovery_level = 0
                    self.recovery_debt = 0.0
                    self.current_shares = float(self.config.base_shares)
                else:
                    self.mode = "RECOVERY"
                    self.recovery_debt = remaining
                    self.current_shares = shares_before
                reason = "win"
            elif pair_pnl < -1e-9:
                self.mode = "RECOVERY"
                self.direction_locked = self.recovery_direction in ("UP", "DOWN")
                self.recovery_level = int(snap.get("level_before") or 0) + 1
                self.recovery_debt = _money(debt_before - pair_pnl)
                self.current_shares = shares_before * float(self.config.multiplier)
                self.entry_shares = 0.0
                reason = "loss"
            else:
                reason = "neutral"
        elif (
            pair_pnl > 1e-9
            and str(snap.get("mode_before") or "") == "RECOVERY"
            and not armed_win
            and not both_plus
        ):
            # The candle doubled a recovery entry that closed in profit.
            # Put the size back. The side stays.
            self.mode = "RECOVERY"
            direction = str(snap.get("direction_before") or "").upper()
            if direction in ("UP", "DOWN"):
                self.recovery_direction = direction
            self.direction_locked = self.recovery_direction in ("UP", "DOWN")
            self.recovery_level = int(snap.get("level_before") or 0)
            self.current_shares = float(snap.get("shares_before") or self.current_shares)
            self.entry_shares = 0.0
            reason = "win"
        elif pair_pnl > 1e-9 and both_plus and (
            self.mode == "RECOVERY" or str(snap.get("mode_before") or "") == "RECOVERY"
        ) and not armed_win:
            direction = str(snap.get("direction_before") or self.recovery_direction or "").upper()
            if direction in ("UP", "DOWN"):
                self.recovery_direction = direction
            self.direction_locked = self.recovery_direction in ("UP", "DOWN")
            self._collect_win(pair_pnl)
            reason = "win"
        elif pair_pnl > 1e-9 or (armed_win and pair_pnl >= -1e-9):
            self.mode = "BASE"
            self.recovery_direction = None
            self.direction_locked = False
            self.recovery_level = 0
            self.recovery_debt = 0.0
            self.current_shares = float(self.config.base_shares)
            self.entry_shares = 0.0
            reason = "win" if pair_pnl > 1e-9 else "neutral"
        elif armed_win and pair_pnl < -1e-9:
            # The book looked locked in profit and the venues paid a loss.
            # The entry already opened stays. The following one recovers.
            closed = float(snap.get("shares_before") or self.config.base_shares)
            candle = str(candle_direction or snap.get("candle") or "").upper()
            self.mode = "RECOVERY"
            if candle in ("UP", "DOWN"):
                self.recovery_direction = candle
            self.direction_locked = self.recovery_direction in ("UP", "DOWN")
            self.recovery_level = int(snap.get("level_before") or 0) + 1
            self.recovery_debt = _money(-pair_pnl)
            self.current_shares = closed * float(self.config.multiplier)
            reason = "loss"
        else:
            if pair_pnl < -1e-9:
                self.recovery_debt = _money(self.recovery_debt - pair_pnl)
            reason = "loss" if pair_pnl < -1e-9 else "neutral"
        event = RecoveryEvent(
            trade_id=trade_id,
            bn_pnl=pf_pnl,
            pm_pnl=pm_pnl,
            fees=float(fees or 0.0),
            pair_pnl=pair_pnl,
            debt_before=float(snap.get("shares_before") or 0.0),
            debt_after=self.recovery_debt,
            recovery_level=self.recovery_level,
            next_shares=self.current_shares,
            reason=reason,
            closed_shares=float(self.current_shares),
            previous_mode=previous_mode,
            mode=self.mode,
            direction=self.recovery_direction or "",
            direction_changed=False,
            hedging=self.hedging_enabled(),
        )
        self.early_ids.pop(trade_id, None)
        self.pending.pop(trade_id, None)
        self._remember(trade_id)
        self.save()
        if reason == "win" and self.mode == "RECOVERY":
            logger.info(
                "[SETTLEMENT]\nCombined Result: WIN\n"
                "Recovery keeps the same side and the same shares.\n"
                "Shares: %.0f\nDirection: %s\nHedging: OFF",
                self.current_shares,
                self.recovery_direction or "",
            )
        elif reason == "win":
            logger.info(
                "[SETTLEMENT]\nCombined Result: WIN\n"
                "Next entry stays BASE.\nShares: %.0f\nHedging: ON",
                self.current_shares,
            )
        else:
            logger.info(
                "[SETTLEMENT]\nCombined Result: %s\n"
                "Combined PnL keeps recovery.\nShares: %.0f\nDirection: %s\nHedging: OFF",
                reason.upper(),
                self.current_shares,
                self.recovery_direction or "",
            )
        return event

    def _maybe_apply(
        self,
        trade_id: str,
        fees: float,
        pair_shares: float,
        candle_direction: Optional[str],
        keep_direction: Optional[str] = None,
    ) -> Optional[RecoveryEvent]:
        if not trade_id or trade_id in self.processed_ids:
            return None
        slot = self.pending.get(trade_id) or {}
        if not settlement_ready(bool(slot.get("pf_ok")), bool(slot.get("pm_ok"))):
            logger.info(
                "[SETTLEMENT]\nCombined Result: PENDING\n"
                "Predict.fun confirmed: %s\nPolymarket confirmed: %s\n"
                "Martingale: UNCHANGED",
                bool(slot.get("pf_ok")),
                bool(slot.get("pm_ok")),
            )
            return None
        pf_pnl = float(slot.get("pf") or 0.0)
        pm_pnl = float(slot.get("pm") or 0.0)
        pair_pnl = _money(pf_pnl + pm_pnl - float(fees or 0.0))
        debt_before = self.recovery_debt
        previous_mode = self.mode
        # Double the strategy size that was used for this entry, not a partial fill.
        closed = float(self.entry_shares or self.current_shares or pair_shares or self.config.base_shares)
        direction_changed = False
        reason = "neutral"

        if pair_pnl > 1e-9:
            reason = "win"
            both_plus = paid_on_every_venue(pf_pnl, pm_pnl, str(slot.get("venues") or "both"))
            in_recovery = (
                self.mode == "RECOVERY" and self.recovery_direction in ("UP", "DOWN")
            )
            if in_recovery and both_plus:
                # Both venues paid. The profit comes off the debt. Shares
                # follow whatever debt is left. Base 6 starts only at zero.
                self._collect_win(pair_pnl)
            elif in_recovery:
                # One venue plus and the other minus. The size stays.
                self.entry_shares = 0.0
            else:
                self.mode = "BASE"
                self.recovery_direction = None
                self.direction_locked = False
                self.recovery_level = 0
                self.recovery_debt = 0.0
                self.current_shares = float(self.config.base_shares)
                self.entry_shares = 0.0
        elif pair_pnl < -1e-9:
            reason = "loss"
            held = str(keep_direction or "").upper()
            candle = held if held in ("UP", "DOWN") else str(candle_direction or "").upper()
            if held in ("UP", "DOWN"):
                closed = float(pair_shares or self.entry_shares or self.current_shares or self.config.base_shares)
            if self.mode != "RECOVERY" and candle not in ("UP", "DOWN", "NEUTRAL"):
                # The candle poll can land a second after the venue result.
                # Do not freeze BASE, and do not mark this settlement done.
                logger.info(
                    "[SETTLEMENT]\nCombined Result: LOSS\nCandle: %s\n"
                    "Martingale: WAITING\n"
                    "Position stays open until the previous BTC candle is known.",
                    candle or "UNKNOWN",
                )
                return None
            self.recovery_debt = _money(self.recovery_debt - pair_pnl)
            if self.mode != "RECOVERY":
                if candle not in ("UP", "DOWN"):
                    # Stay in base. Do not invent a recovery side.
                    self.current_shares = float(self.config.base_shares)
                    self.entry_shares = 0.0
                else:
                    self.mode = "RECOVERY"
                    self.recovery_direction = candle
                    self.direction_locked = True
                    self.recovery_level += 1
                    self.current_shares = closed * float(self.config.multiplier)
                    self.entry_shares = 0.0
            else:
                # Direction was locked at the first loss. Later candles are ignored.
                self.recovery_level += 1
                self.current_shares = closed * float(self.config.multiplier)
                self.entry_shares = 0.0
                self.direction_locked = True
        else:
            reason = "neutral"
            self.entry_shares = 0.0

        event = RecoveryEvent(
            trade_id=trade_id,
            bn_pnl=pf_pnl,
            pm_pnl=pm_pnl,
            fees=float(fees or 0.0),
            pair_pnl=pair_pnl,
            debt_before=debt_before,
            debt_after=self.recovery_debt,
            recovery_level=self.recovery_level,
            next_shares=self.current_shares,
            reason=reason,
            closed_shares=closed,
            previous_mode=previous_mode,
            mode=self.mode,
            direction=self.recovery_direction or "",
            direction_changed=direction_changed,
            hedging=self.hedging_enabled(),
        )
        self._remember(trade_id)
        self.pending.pop(trade_id, None)
        self.save()
        logger.info("%s", event.line())
        return event

    def _reset_step_base(self) -> None:
        self.mode = "BASE"
        self.recovery_direction = None
        self.direction_locked = False
        self.recovery_level = 0
        self.recovery_debt = 0.0
        self.current_shares = float(self.config.base_shares)
        self.entry_shares = 0.0
        self.step_loss = 0.0
        self.step_skip = 0
        self.step_anchor = 0.0
        self.step_entry_end = 0.0

    def _apply_step_loss(self, direction: str, loss_usd: float, anchor_end: float) -> None:
        side = str(direction or "").upper()
        self.step_loss = _money(self.step_loss + max(0.0, float(loss_usd)))
        self.mode = "RECOVERY"
        if side in ("UP", "DOWN"):
            self.recovery_direction = side
        self.direction_locked = self.recovery_direction in ("UP", "DOWN")
        self.recovery_level += 1
        self.current_shares = step_recovery_shares(self.step_loss)
        self.entry_shares = 0.0
        self.step_skip = 1
        self.step_anchor = float(anchor_end or 0)
        # Skip the market that starts when this one ends. The limit belongs
        # on the window after that, and it can be posted before that window opens.
        self.step_entry_end = self.step_anchor + 600.0

    def arm_step_loss(
        self,
        trade_id: str,
        direction: str,
        loss_usd: float,
        anchor_end: float,
    ) -> Optional[RecoveryEvent]:
        """Unhedged loss. Skip the next market and size from the lost dollars."""
        if not trade_id or trade_id in self.processed_ids or trade_id in self.early_ids:
            return None
        if float(loss_usd) <= 1e-9:
            return None
        previous_mode = self.mode
        self.early_ids[trade_id] = {
            "kind": "step_loss",
            "loss": float(loss_usd),
            "mode_before": previous_mode,
            "direction_before": self.recovery_direction,
        }
        self._apply_step_loss(direction, loss_usd, anchor_end)
        event = RecoveryEvent(
            trade_id=trade_id,
            bn_pnl=0.0,
            pm_pnl=0.0,
            fees=0.0,
            pair_pnl=-float(loss_usd),
            debt_before=0.0,
            debt_after=self.step_loss,
            recovery_level=self.recovery_level,
            next_shares=self.current_shares,
            reason="loss",
            previous_mode=previous_mode,
            mode=self.mode,
            direction=self.recovery_direction or "",
            hedging=False,
        )
        self.save()
        logger.info("%s", event.line())
        return event

    def arm_step_win(self, trade_id: str) -> Optional[RecoveryEvent]:
        """A plus returns to the normal hedge and the base share count."""
        if not trade_id or trade_id in self.processed_ids or trade_id in self.early_ids:
            return None
        previous_mode = self.mode
        self.early_ids[trade_id] = {
            "kind": "step_win",
            "mode_before": previous_mode,
            "step_loss": float(self.step_loss),
            "direction_before": self.recovery_direction,
        }
        self._reset_step_base()
        event = RecoveryEvent(
            trade_id=trade_id,
            bn_pnl=0.0,
            pm_pnl=0.0,
            fees=0.0,
            pair_pnl=0.0,
            debt_before=0.0,
            debt_after=0.0,
            recovery_level=0,
            next_shares=self.current_shares,
            reason="win",
            previous_mode=previous_mode,
            mode="BASE",
            hedging=True,
        )
        self.save()
        logger.info("%s", event.line())
        return event

    def arm_step_stay(self, trade_id: str) -> Optional[RecoveryEvent]:
        """A hedged loss stays on the normal hedge. Recovery does not start."""
        if not trade_id or trade_id in self.processed_ids or trade_id in self.early_ids:
            return None
        self.early_ids[trade_id] = {"kind": "step_stay", "mode_before": self.mode}
        event = RecoveryEvent(
            trade_id=trade_id,
            bn_pnl=0.0,
            pm_pnl=0.0,
            fees=0.0,
            pair_pnl=0.0,
            debt_before=self.step_loss,
            debt_after=self.step_loss,
            recovery_level=self.recovery_level,
            next_shares=self.current_shares,
            reason="loss",
            previous_mode=self.mode,
            mode=self.mode,
            direction=self.recovery_direction or "",
            hedging=self.hedging_enabled(),
        )
        self.save()
        logger.info(
            "[SETTLEMENT]\nCombined Result: LOSS\nHedge filled, step recovery stays off.\n"
            "Shares: %.0f\nHedging: %s",
            self.current_shares,
            "ON" if self.hedging_enabled() else "OFF",
        )
        return event

    def confirm_step(
        self,
        trade_id: str,
        pair_pnl: float,
        direction: str = "",
        hedged: bool = False,
        anchor_end: float = 0.0,
    ) -> Optional[RecoveryEvent]:
        """Replace the early estimate with the official sum. Do not step twice."""
        if not trade_id or trade_id in self.processed_ids:
            return None
        snap = self.early_ids.get(trade_id) or {}
        kind = str(snap.get("kind") or "")
        previous_mode = self.mode
        reason = "neutral"
        if kind == "step_loss":
            estimated = float(snap.get("loss") or 0.0)
            if pair_pnl > 1e-9:
                self._reset_step_base()
                reason = "win"
            elif pair_pnl < -1e-9:
                self.step_loss = _money(max(0.0, self.step_loss - estimated - pair_pnl))
                self.mode = "RECOVERY"
                self.current_shares = step_recovery_shares(self.step_loss)
                self.entry_shares = 0.0
                reason = "loss"
            else:
                self.step_loss = _money(max(0.0, self.step_loss - estimated))
                if self.step_loss <= 1e-9:
                    self._reset_step_base()
                else:
                    self.current_shares = step_recovery_shares(self.step_loss)
                reason = "neutral"
        elif kind == "step_win" and pair_pnl < -1e-9 and not hedged:
            # The early win wiped the running total. Put it back, then add
            # this loss. A single-venue miss must not restart the ladder at $0.
            self.step_loss = _money(float(snap.get("step_loss") or 0))
            kept = str(snap.get("direction_before") or "")
            if kept in ("UP", "DOWN"):
                self.recovery_direction = kept
            self._apply_step_loss(direction or kept, -pair_pnl, anchor_end)
            reason = "loss"
        elif kind == "step_stay":
            reason = "loss" if pair_pnl < -1e-9 else ("win" if pair_pnl > 1e-9 else "neutral")
        elif pair_pnl > 1e-9:
            self._reset_step_base()
            reason = "win"
        elif pair_pnl < -1e-9 and not (hedged and self.mode != "RECOVERY"):
            self._apply_step_loss(direction, -pair_pnl, anchor_end)
            reason = "loss"
        elif pair_pnl < -1e-9:
            reason = "loss"
        self.early_ids.pop(trade_id, None)
        event = RecoveryEvent(
            trade_id=trade_id,
            bn_pnl=pair_pnl,
            pm_pnl=0.0,
            fees=0.0,
            pair_pnl=pair_pnl,
            debt_before=0.0,
            debt_after=self.step_loss,
            recovery_level=self.recovery_level,
            next_shares=self.current_shares,
            reason=reason,
            previous_mode=previous_mode,
            mode=self.mode,
            direction=self.recovery_direction or "",
            hedging=self.hedging_enabled(),
        )
        self._remember(trade_id)
        self.save()
        logger.info("%s", event.line())
        return event

    def classify_step_window(self, market_key: str, window_end: float) -> str:
        """skip the market after a loss. enter the one after that."""
        if self.mode != "RECOVERY":
            return "base"
        label = self.step_seen.get(market_key)
        if label:
            return label
        if float(window_end) <= float(self.step_anchor or 0) + 30:
            return "wait"
        if self.step_skip > 0:
            self.step_seen[market_key] = "skip"
            self.step_skip -= 1
            self._trim_step_seen()
            self.save()
            return "skip"
        self.step_seen[market_key] = "enter"
        self._trim_step_seen()
        self.save()
        return "enter"

    def miss_step_window(self, market_key: str, window_end: float) -> None:
        """The entry window never traded at 0.50. Skip one market and try again."""
        if market_key in self.step_missed:
            return
        self.step_missed.append(market_key)
        self.step_missed = self.step_missed[-50:]
        self.step_anchor = float(window_end)
        self.step_entry_end = float(window_end) + 600.0
        self.step_skip = 1
        self.save()

    def _trim_step_seen(self) -> None:
        if len(self.step_seen) <= 50:
            return
        for key in list(self.step_seen)[:-40]:
            self.step_seen.pop(key, None)

    def snapshot(self) -> dict:
        return {
            "mode": self.mode,
            "recovery_direction": self.recovery_direction,
            "direction_locked": self.direction_locked,
            "hedging": self.hedging_enabled(),
            "price_threshold": self.mode != "RECOVERY",
            "entry_mode": "IMMEDIATE" if self.mode == "RECOVERY" else "BASE",
            "recovery_debt": self.recovery_debt,
            "recovery_level": self.recovery_level,
            "current_shares": self.current_shares,
            "entry_shares": self.entry_shares,
            "base_shares": self.config.base_shares,
            "multiplier": self.config.multiplier,
            "last_trade_id": self.last_trade_id,
            "previous_market_id": self.previous_market_id,
            "processed_ids": list(self.processed_ids[-200:]),
            "entered_markets": list(self.entered_markets[-50:]),
            "pending": self.pending,
            "positions": self.positions,
            "early_ids": self.early_ids,
            "step_loss": self.step_loss,
            "step_skip": self.step_skip,
            "step_anchor": self.step_anchor,
            "step_entry_end": self.step_entry_end,
            "step_seen": self.step_seen,
            "step_missed": self.step_missed,
        }

    def load(self) -> None:
        if not self.path or not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        self.recovery_debt = float(data.get("recovery_debt") or 0)
        self.recovery_level = int(data.get("recovery_level") or 0)
        self.current_shares = float(data.get("current_shares") or self.config.base_shares)
        self.entry_shares = float(data.get("entry_shares") or 0)
        self.last_trade_id = str(data.get("last_trade_id") or "")
        self.processed_ids = list(data.get("processed_ids") or [])
        self.entered_markets = [str(x) for x in (data.get("entered_markets") or [])]
        self.previous_market_id = str(data.get("previous_market_id") or "")
        self.pending = dict(data.get("pending") or {})
        self.positions = dict(data.get("positions") or {})
        self.early_ids = dict(data.get("early_ids") or {})
        self.step_loss = float(data.get("step_loss") or 0)
        self.step_skip = int(data.get("step_skip") or 0)
        self.step_anchor = float(data.get("step_anchor") or 0)
        self.step_entry_end = float(data.get("step_entry_end") or 0)
        seen = data.get("step_seen") or {}
        self.step_seen = {str(key): str(value) for key, value in seen.items()} if isinstance(seen, dict) else {}
        self.step_missed = [str(item) for item in (data.get("step_missed") or [])]
        if "mode" in data:
            self.mode = str(data.get("mode") or "BASE")
            direction = data.get("recovery_direction")
            self.recovery_direction = str(direction) if direction in ("UP", "DOWN") else None
            self.direction_locked = bool(data.get("direction_locked")) and self.recovery_direction is not None
        else:
            # Older debt ladder. It has a share count but no BTC side, so it
            # cannot become Recovery Mode. Keep the processed settlement ids
            # and start the new cycle from base.
            legacy = self.recovery_level > 0 or self.current_shares > self.config.base_shares + 1e-9
            self.mode = "BASE"
            self.recovery_direction = None
            self.direction_locked = False
            if legacy:
                logger.warning(
                    "Legacy recovery file has no candle direction "
                    "(level=%s shares=%s debt=%s). Starting in BASE at %.0f shares. "
                    "Already processed settlements are kept.",
                    self.recovery_level,
                    self.current_shares,
                    self.recovery_debt,
                    self.config.base_shares,
                )
                self.current_shares = float(self.config.base_shares)
                self.recovery_level = 0
                self.recovery_debt = 0.0
        if self.mode not in ("BASE", "RECOVERY"):
            self.mode = "BASE"

    def save(self) -> None:
        if not self.path:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.snapshot(), indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def _remember(self, trade_id: str) -> None:
        if not trade_id:
            return
        self.last_trade_id = trade_id
        self.processed_ids.append(trade_id)
        if len(self.processed_ids) > 200:
            self.processed_ids = self.processed_ids[-100:]
