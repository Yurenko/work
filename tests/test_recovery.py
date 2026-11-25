"""Restart and idempotency of the shared recovery book."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.strategy.recovery import RecoveryBook, RecoveryConfig


def book(path: Path | None = None) -> RecoveryBook:
    return RecoveryBook(
        RecoveryConfig(base_shares=6, shares_step=6, max_shares=12, max_debt_usd=50, multiplier=2),
        path,
    )


class RecoveryPersistenceTest(unittest.TestCase):
    def test_restart_does_not_count_a_trade_twice(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "recovery.json"
            first = book(path)
            first.on_pair_closed("a", bn_pnl=-2.5, pm_pnl=-1.5, fees=0, candle_direction="DOWN")
            first.on_pair_closed("b", bn_pnl=0.4, pm_pnl=-0.4, fees=0, candle_direction="UP")
            self.assertEqual(first.mode, "RECOVERY")
            self.assertEqual(first.recovery_direction, "DOWN")
            self.assertEqual(first.current_shares, 12)

            restarted = book(path)
            self.assertEqual(restarted.recovery_direction, "DOWN")
            self.assertEqual(restarted.current_shares, 12)
            self.assertIsNone(restarted.on_pair_closed("a", bn_pnl=-100, pm_pnl=0, candle_direction="UP"))
            self.assertIsNone(restarted.on_pair_closed("b", bn_pnl=-100, pm_pnl=0, candle_direction="UP"))
            self.assertEqual(restarted.current_shares, 12)

            later = restarted.on_pair_closed("c", bn_pnl=-1, pm_pnl=0, candle_direction="UP")
            self.assertEqual(later.reason, "loss")
            self.assertEqual(restarted.recovery_direction, "DOWN")
            self.assertEqual(restarted.current_shares, 24)

    def test_legacy_debt_file_does_not_invent_a_recovery_side(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "recovery.json"
            path.write_text(
                '{"recovery_debt": 208.86, "recovery_level": 30, '
                '"current_shares": 24.0, "processed_ids": ["5613eb30"]}',
                encoding="utf-8",
            )
            restored = book(path)
            self.assertEqual(restored.mode, "BASE")
            self.assertIsNone(restored.recovery_direction)
            self.assertEqual(restored.current_shares, 6)
            self.assertTrue(restored.hedging_enabled())
            self.assertIsNone(restored.on_pair_closed("5613eb30", -10, -10, candle_direction="UP"))
            self.assertEqual(restored.current_shares, 6)

    def test_legacy_debt_file_does_not_invent_a_recovery_side(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "recovery.json"
            path.write_text(
                '{"recovery_debt": 208.86, "recovery_level": 30, '
                '"current_shares": 24.0, "processed_ids": ["5613eb30"]}',
                encoding="utf-8",
            )
            restored = book(path)
            self.assertEqual(restored.mode, "BASE")
            self.assertIsNone(restored.recovery_direction)
            self.assertEqual(restored.current_shares, 6)
            self.assertTrue(restored.hedging_enabled())
            self.assertIsNone(restored.on_pair_closed("5613eb30", -10, -10, candle_direction="UP"))
            self.assertEqual(restored.current_shares, 6)

    def test_manual_reset_returns_to_base_but_keeps_processed_ids(self) -> None:
        size = book()
        size.on_pair_closed("a", -1, -1, candle_direction="UP")
        size.reset()
        self.assertEqual(size.mode, "BASE")
        self.assertEqual(size.current_shares, 6)
        self.assertIsNone(size.recovery_direction)
        self.assertIsNone(size.on_pair_closed("a", -50, -50, candle_direction="DOWN"))
        self.assertEqual(size.mode, "BASE")
        self.assertEqual(size.current_shares, 6)


if __name__ == "__main__":
    unittest.main()
