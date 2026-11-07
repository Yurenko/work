"""Recovery sizing: loss steps up shares, wins clear debt."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _money(value: float) -> float:
    return round(float(value), 6)


@dataclass
class RecoveryConfig:
    base_shares: float = 5.0
    shares_step: float = 5.0
    max_shares: float = 20.0
    max_debt_usd: float = 50.0

    def __post_init__(self) -> None:
        if self.base_shares <= 0 or self.shares_step <= 0:
            raise ValueError("BASE_SHARES and SHARES_STEP must be positive")
        if self.max_shares < self.base_shares:
            raise ValueError("MAX_SHARES must be at least BASE_SHARES")
        if self.max_debt_usd <= 0:
            raise ValueError("MAX_RECOVERY_DEBT_USD must be positive")


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

    def line(self) -> str:
        return (
            f"RECOVERY {self.reason} BN {self.bn_pnl:+.4f} PM {self.pm_pnl:+.4f} "
            f"fees {self.fees:.4f} pair {self.pair_pnl:+.4f} "
            f"debt {self.debt_before:.4f} → {self.debt_after:.4f} "
            f"level {self.recovery_level} closed {self.closed_shares:.0f} sh → "
            f"next {self.next_shares:.0f} sh ({self.reason})"
        )


class RecoveryBook:
    """recovery_debt, recovery_level and the share count for the next pair."""

    def __init__(self, config: RecoveryConfig, path: Path | None = None) -> None:
        self.config = config
        self.path = path
        self.recovery_debt = 0.0
        self.recovery_level = 0
        self.current_shares = float(config.base_shares)
        self.last_trade_id = ""
        self.processed_ids: list[str] = []
        self.load()

    def shares_for_entry(self) -> float:
        """Share count for both venues. Frozen by the caller once the pair is open."""
        return float(self.current_shares)

    def entry_block(self) -> str:
        """Empty when a signal may still open. Never a reason to open by itself."""
        if self.current_shares > self.config.max_shares + 1e-9:
            return (
                f"recovery stop: next size {self.current_shares:.0f} sh exceeds "
                f"MAX_SHARES {self.config.max_shares:.0f}"
            )
        if self.recovery_debt >= self.config.max_debt_usd - 1e-9:
            return (
                f"recovery stop: debt ${self.recovery_debt:.2f} reached "
                f"MAX_RECOVERY_DEBT_USD ${self.config.max_debt_usd:.2f}"
            )
        return ""

    def on_pair_closed(
        self,
        trade_id: str,
        bn_pnl: float,
        pm_pnl: float,
        fees: float = 0.0,
        pair_shares: float = 0.0,
    ) -> Optional[RecoveryEvent]:
        """Apply one finished pair. None when this id was already counted."""
        if trade_id and trade_id in self.processed_ids:
            return None
        pair_pnl = _money(bn_pnl + pm_pnl - fees)
        debt_before = self.recovery_debt
        closed = pair_shares or self.current_shares

        if pair_pnl < -1e-9:
            reason = "loss"
            self.recovery_debt = _money(self.recovery_debt - pair_pnl)  # add |loss|
            self.recovery_level += 1
            # step up from the share count of the pair that lost
            self.current_shares = min(
                self.config.max_shares,
                closed + self.config.shares_step,
            )
        elif self.recovery_debt > 0 and pair_pnl > 1e-9:
            if pair_pnl >= self.recovery_debt - 1e-9:
                reason = "full recovery"
                self.recovery_debt = 0.0
                self.recovery_level = 0
                self.current_shares = float(self.config.base_shares)
            else:
                reason = "partial recovery"
                self.recovery_debt = _money(self.recovery_debt - pair_pnl)
                # keep current size while debt remains
        else:
            reason = "flat"
            if self.recovery_debt <= 0:
                self.current_shares = float(self.config.base_shares)
                self.recovery_level = 0

        event = RecoveryEvent(
            trade_id=trade_id,
            bn_pnl=bn_pnl,
            pm_pnl=pm_pnl,
            fees=fees,
            pair_pnl=pair_pnl,
            debt_before=debt_before,
            debt_after=self.recovery_debt,
            recovery_level=self.recovery_level,
            next_shares=self.current_shares,
            reason=reason,
            closed_shares=closed,
        )
        self._remember(trade_id)
        self.save()
        return event

    def snapshot(self) -> dict:
        return {
            "recovery_debt": self.recovery_debt,
            "recovery_level": self.recovery_level,
            "current_shares": self.current_shares,
            "last_trade_id": self.last_trade_id,
            "processed_ids": list(self.processed_ids[-50:]),
        }

    def load(self) -> None:
        if not self.path or not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.recovery_debt = float(data.get("recovery_debt") or 0)
            self.recovery_level = int(data.get("recovery_level") or 0)
            self.current_shares = float(data.get("current_shares") or self.config.base_shares)
            self.last_trade_id = str(data.get("last_trade_id") or "")
            self.processed_ids = list(data.get("processed_ids") or [])
        except Exception:
            pass

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
