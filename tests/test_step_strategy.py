import asyncio
import unittest

from app.strategy.engine import Hedge, HedgeEngine, Leg
from app.strategy.recovery import (
    RecoveryBook,
    RecoveryConfig,
    step_limit_price,
    step_recovery_shares,
)


class StepStrategyTests(unittest.TestCase):
    def test_shares_cover_the_loss_and_add_three(self):
        self.assertEqual(step_recovery_shares(3), 9)
        self.assertEqual(step_recovery_shares(7.5), 18)

    def test_limit_takes_a_better_price_and_refuses_085(self):
        self.assertEqual(step_limit_price(0.49), 0.49)
        self.assertEqual(step_limit_price(0.48), 0.48)
        self.assertEqual(step_limit_price(0.50), 0.50)
        self.assertIsNone(step_limit_price(0.85))

    def test_unhedged_loss_skips_one_market_then_enters(self):
        book = RecoveryBook(RecoveryConfig(), path=None)
        book.arm_step_loss("loss-1", "DOWN", 3, anchor_end=1000)
        self.assertEqual(book.mode, "RECOVERY")
        self.assertEqual(book.current_shares, 9)
        self.assertEqual(book.recovery_direction, "DOWN")
        self.assertEqual(book.hedging_enabled(), False)
        self.assertEqual(book.step_entry_end, 1600)
        self.assertEqual(book.classify_step_window("m-next", 1300), "skip")
        self.assertEqual(book.classify_step_window("m-after", 1600), "enter")

    def test_second_loss_uses_the_sum(self):
        book = RecoveryBook(RecoveryConfig(), path=None)
        book.arm_step_loss("loss-1", "UP", 3, anchor_end=1000)
        book.confirm_step("loss-1", -3, direction="UP", anchor_end=1000)
        book.arm_step_loss("loss-2", "UP", 4.5, anchor_end=1600)
        self.assertEqual(book.step_loss, 7.5)
        self.assertEqual(book.current_shares, 18)
        self.assertEqual(book.step_skip, 1)

    def test_win_returns_to_base_hedge(self):
        book = RecoveryBook(RecoveryConfig(), path=None)
        book.arm_step_loss("loss-1", "UP", 3, anchor_end=1000)
        book.arm_step_win("win-1")
        self.assertEqual(book.mode, "BASE")
        self.assertEqual(book.current_shares, 6)
        self.assertEqual(book.step_loss, 0)
        self.assertEqual(book.step_entry_end, 0)
        self.assertTrue(book.hedging_enabled())

    def test_hedged_loss_does_not_start_recovery(self):
        book = RecoveryBook(RecoveryConfig(), path=None)
        book.arm_step_stay("hedged-loss")
        self.assertEqual(book.mode, "BASE")
        self.assertEqual(book.current_shares, 6)
        book.confirm_step("hedged-loss", -2.5, hedged=True)
        self.assertEqual(book.mode, "BASE")
        self.assertEqual(book.current_shares, 6)

    def test_early_win_that_was_a_loss_keeps_the_running_total(self):
        book = RecoveryBook(RecoveryConfig(), path=None)
        book.arm_step_loss("loss-1", "DOWN", 3.78, anchor_end=1000)
        book.arm_step_loss("loss-2", "DOWN", 5.39, anchor_end=1600)
        self.assertEqual(book.step_loss, 9.17)
        self.assertEqual(book.current_shares, 21)
        book.arm_step_win("false-win")
        self.assertEqual(book.mode, "BASE")
        self.assertEqual(book.current_shares, 6)
        book.confirm_step("false-win", -9.87, direction="DOWN", anchor_end=2200)
        self.assertEqual(book.mode, "RECOVERY")
        self.assertEqual(book.step_loss, 19.04)
        self.assertEqual(book.current_shares, 41)
        self.assertEqual(book.recovery_direction, "DOWN")
        self.assertFalse(book.hedging_enabled())

    def test_missed_limit_skips_another_market(self):
        book = RecoveryBook(RecoveryConfig(), path=None)
        book.arm_step_loss("loss-1", "DOWN", 3, anchor_end=1000)
        book.classify_step_window("m-next", 1300)
        self.assertEqual(book.classify_step_window("m-entry", 1600), "enter")
        book.miss_step_window("m-entry", 1600)
        self.assertEqual(book.step_entry_end, 2200)
        self.assertEqual(book.classify_step_window("m-after-miss", 1900), "skip")
        self.assertEqual(book.classify_step_window("m-retry", 2200), "enter")

    def test_official_plus_is_base_and_drops_the_resting_limit(self):
        """ec160776 paid +73.50. The 294-share limit must not stay open."""
        book = RecoveryBook(RecoveryConfig(), path=None)
        book.arm_step_loss("earlier", "UP", 72, anchor_end=1000)
        book.confirm_step("winner", 73.50, direction="UP", anchor_end=1600)
        self.assertEqual(book.mode, "BASE")
        self.assertEqual(book.current_shares, 6)
        self.assertTrue(book.hedging_enabled())
        self.assertEqual(book.step_entry_end, 0)

        resting = Hedge(
            id="dd085fcd",
            opened_at=0,
            market_end=2200,
            pf_leg=Leg(
                venue="predict",
                direction="up",
                token_or_market="m",
                shares=294,
                requested_shares=294,
                entry_price=0.50,
                status="SUBMITTED",
                order_id="paper-rest-dd085fcd-pf",
            ),
            pm_leg=Leg(venue="polymarket", direction="up", token_or_market="", status="SKIPPED"),
            status="OPEN",
            strategy_mode="RECOVERY",
            limit_ceiling=0.50,
        )
        settling = Hedge(
            id="ec160776",
            opened_at=0,
            market_end=1600,
            pf_leg=Leg(venue="predict", direction="up", token_or_market="m", status="FILLED", filled_shares=147),
            pm_leg=Leg(venue="polymarket", direction="up", token_or_market="", status="SKIPPED"),
            status="SETTLING",
            strategy_mode="RECOVERY",
            limit_ceiling=0.50,
        )
        saved = []
        host = type("Host", (), {})()
        host._step_limit_unfilled = HedgeEngine._step_limit_unfilled.__get__(host, type(host))
        host.hedges = [settling, resting]
        host.trade_strategy = "step"
        host.recovery = book
        host.settings = type("S", (), {"is_live": False})()
        host.trade_history = type("H", (), {"upsert": lambda self, row: saved.append(row)})()
        self.assertTrue(HedgeEngine._step_result_pending(host))
        asyncio.run(HedgeEngine._drop_unfilled_step_limits(host))
        self.assertEqual([item.id for item in host.hedges], ["ec160776"])
        self.assertEqual(resting.close_reason, "CANCELLED")
        self.assertEqual(saved[0]["id"], "dd085fcd")


if __name__ == "__main__":
    unittest.main()
