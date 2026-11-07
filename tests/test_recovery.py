"""Recovery sizing: loss, partial recovery, full recovery, restart, no double count."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.strategy.engine import Hedge, HedgeEngine, Leg
from app.strategy.recovery import RecoveryBook, RecoveryConfig


def config(**overrides: float) -> RecoveryConfig:
    values = dict(base_shares=5, shares_step=5, max_shares=20, max_debt_usd=50)
    values.update(overrides)
    return RecoveryConfig(**values)


def book(path: Path | None = None, **overrides: float) -> RecoveryBook:
    return RecoveryBook(config(**overrides), path)


class RecoverySequenceTest(unittest.TestCase):
    def test_loss_profit_loss_partial_then_full(self) -> None:
        size = book()
        self.assertEqual(size.shares_for_entry(), 5)
        self.assertEqual(size.entry_block(), "")

        loss = size.on_pair_closed("t1", bn_pnl=-2.5, pm_pnl=-1.5, fees=0)
        self.assertIsNotNone(loss)
        self.assertEqual(loss.reason, "loss")
        self.assertEqual(loss.pair_pnl, -4)
        self.assertEqual(loss.debt_before, 0)
        self.assertEqual(size.recovery_debt, 4)
        self.assertEqual(size.recovery_level, 1)
        self.assertEqual(size.current_shares, 10)
        self.assertIn("debt 0.0000 → 4.0000", loss.line())
        self.assertIn("next 10 sh", loss.line())

        # Profit smaller than the debt keeps the current size.
        partial_first = size.on_pair_closed("t2", bn_pnl=0.6, pm_pnl=0.4, fees=0)
        self.assertEqual(partial_first.reason, "partial recovery")
        self.assertEqual(size.recovery_debt, 3)
        self.assertEqual(size.recovery_level, 1)
        self.assertEqual(size.current_shares, 10)

        again = size.on_pair_closed("t3", bn_pnl=-1.2, pm_pnl=-0.8, fees=0)
        self.assertEqual(again.reason, "loss")
        self.assertEqual(size.recovery_debt, 5)
        self.assertEqual(size.recovery_level, 2)
        self.assertEqual(size.current_shares, 15)

        partial = size.on_pair_closed("t4", bn_pnl=1.25, pm_pnl=0.75, fees=0)
        self.assertEqual(partial.reason, "partial recovery")
        self.assertEqual(partial.debt_before, 5)
        self.assertEqual(size.recovery_debt, 3)
        self.assertEqual(size.recovery_level, 2)
        self.assertEqual(size.current_shares, 15)

        done = size.on_pair_closed("t5", bn_pnl=2, pm_pnl=1, fees=0)
        self.assertEqual(done.reason, "full recovery")
        self.assertEqual(size.recovery_debt, 0)
        self.assertEqual(size.recovery_level, 0)
        self.assertEqual(size.current_shares, 5)

    def test_loss_after_full_recovery_steps_from_the_open_size(self) -> None:
        size = book()
        size.on_pair_closed("loss5", bn_pnl=-4.15, pm_pnl=0, fees=0, pair_shares=5)
        self.assertEqual(size.current_shares, 10)
        size.on_pair_closed("w1", bn_pnl=1.40, pm_pnl=0, fees=0)
        size.on_pair_closed("w2", bn_pnl=1.10, pm_pnl=0, fees=0)
        size.on_pair_closed("w3", bn_pnl=1.10, pm_pnl=0, fees=0)
        cleared = size.on_pair_closed("w4", bn_pnl=1.20, pm_pnl=0, fees=0)
        self.assertEqual(cleared.reason, "full recovery")
        self.assertEqual(size.recovery_debt, 0)
        self.assertEqual(size.current_shares, 5)
        # That 10-share pair was opened before the reset and closed after it.
        lost = size.on_pair_closed("loss10", bn_pnl=-5.1, pm_pnl=-2.7, fees=0, pair_shares=10)
        self.assertEqual(lost.pair_pnl, -7.8)
        self.assertEqual(size.recovery_debt, 7.8)
        self.assertEqual(size.recovery_level, 2)
        self.assertEqual(size.current_shares, 15)

    def test_flat_and_fees_and_cap(self) -> None:
        size = book()
        flat = size.on_pair_closed("flat", bn_pnl=1, pm_pnl=-1, fees=0)
        self.assertEqual(flat.reason, "flat")
        self.assertEqual(size.recovery_debt, 0)
        self.assertEqual(size.current_shares, 5)

        # Fees turn a small gross win into a loss.
        priced = size.on_pair_closed("fee", bn_pnl=1, pm_pnl=1, fees=2.5)
        self.assertEqual(priced.pair_pnl, -0.5)
        self.assertEqual(priced.fees, 2.5)
        self.assertEqual(size.recovery_debt, 0.5)
        self.assertEqual(size.current_shares, 10)

        capped = book()
        for index in range(5):
            capped.on_pair_closed(f"c{index}", bn_pnl=-1, pm_pnl=0, fees=0)
        self.assertEqual(capped.recovery_level, 5)
        self.assertEqual(capped.current_shares, 20)
        self.assertLessEqual(capped.shares_for_entry(), 20)
        self.assertEqual(capped.entry_block(), "")

    def test_debt_limit_and_oversized_state_block_entry(self) -> None:
        size = book(max_debt_usd=5)
        size.on_pair_closed("debt", bn_pnl=-5, pm_pnl=0, fees=0)
        self.assertIn("MAX_RECOVERY_DEBT_USD", size.entry_block())

        below = book(max_debt_usd=5)
        below.on_pair_closed("under", bn_pnl=-4.99, pm_pnl=0, fees=0)
        self.assertEqual(below.entry_block(), "")

        huge = book()
        huge.current_shares = 25
        self.assertIn("MAX_SHARES", huge.entry_block())

    def test_restart_does_not_count_a_trade_twice(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "recovery.json"
            first = book(path)
            first.on_pair_closed("a", bn_pnl=-2.5, pm_pnl=-1.5, fees=0)
            first.on_pair_closed("b", bn_pnl=0.5, pm_pnl=0.5, fees=0)
            self.assertEqual(first.recovery_debt, 3)
            self.assertEqual(first.current_shares, 10)
            self.assertEqual(first.last_trade_id, "b")

            restarted = RecoveryBook(config(), path)
            self.assertEqual(restarted.recovery_debt, 3)
            self.assertEqual(restarted.recovery_level, 1)
            self.assertEqual(restarted.current_shares, 10)
            self.assertEqual(restarted.last_trade_id, "b")

            self.assertIsNone(restarted.on_pair_closed("a", bn_pnl=-100, pm_pnl=0, fees=0))
            self.assertIsNone(restarted.on_pair_closed("b", bn_pnl=-100, pm_pnl=0, fees=0))
            self.assertEqual(restarted.recovery_debt, 3)
            self.assertEqual(restarted.current_shares, 10)

            later = restarted.on_pair_closed("c", bn_pnl=-1, pm_pnl=0, fees=0)
            self.assertEqual(later.reason, "loss")
            self.assertEqual(restarted.recovery_debt, 4)
            self.assertEqual(restarted.current_shares, 15)


def _closed_leg(venue: str, direction: str, entry: float, exit_price: float, shares: float = 5) -> Leg:
    return Leg(
        venue=venue,
        direction=direction,
        token_or_market="t",
        shares=shares,
        entry_price=entry,
        usd=entry * shares,
        status="CLOSED",
        exit_price=exit_price,
        requested_shares=shares,
        filled_shares=shares,
    )


class EngineHookTest(unittest.TestCase):
    def test_record_exit_uses_filled_prices_once(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            engine = HedgeEngine.__new__(HedgeEngine)
            engine.recovery = book(Path(folder) / "recovery.json")
            engine.history = []
            engine.realized_pnl = 0.0
            engine.logs = []
            engine._log = lambda msg: engine.logs.append(msg)
            engine._save_history = lambda: None
            hedge = Hedge(
                id="pair1",
                opened_at=0,
                market_end=1,
                bn_leg=_closed_leg("binance", "up", 0.50, 0.0),
                pm_leg=_closed_leg("polymarket", "down", 0.32, 0.0),
                entry_sum=0.82,
            )
            engine._record_exit(hedge, 2, 0.0, settle="redeem")
            # 5 * (0 - 0.50) + 5 * (0 - 0.32) = -4.10 from the closed prices.
            self.assertAlmostEqual(hedge.pnl, -4.1)
            self.assertEqual(engine.recovery.recovery_debt, 4.1)
            self.assertEqual(engine.recovery.current_shares, 10)
            self.assertTrue(any("RECOVERY pair1" in line for line in engine.logs))

            engine._record_exit(hedge, 3, 0.0, settle="redeem")
            self.assertEqual(engine.recovery.recovery_debt, 4.1)
            self.assertEqual(engine.recovery.recovery_level, 1)

            reloaded = RecoveryBook(config(), engine.recovery.path)
            self.assertIsNone(reloaded.on_pair_closed("pair1", bn_pnl=-4.1, pm_pnl=0, fees=0))
            self.assertEqual(reloaded.recovery_debt, 4.1)

    def test_open_leg_does_not_change_size(self) -> None:
        engine = HedgeEngine.__new__(HedgeEngine)
        engine.recovery = book()
        engine.history = []
        engine.realized_pnl = 0.0
        engine.logs = []
        engine._log = lambda msg: engine.logs.append(msg)
        engine._save_history = lambda: None
        hedge = Hedge(
            id="open",
            opened_at=0,
            market_end=1,
            bn_leg=_closed_leg("binance", "up", 0.5, 0.0),
            pm_leg=Leg("polymarket", "down", "t", 5, 0.32, 1.6, status="FILLED"),
            entry_sum=0.82,
            status="OPEN",
        )
        engine._record_exit(hedge, 2, 0.0)
        self.assertEqual(hedge.status, "CLOSING")
        self.assertEqual(engine.recovery.current_shares, 5)
        self.assertEqual(engine.recovery.recovery_debt, 0)


if __name__ == "__main__":
    unittest.main()
