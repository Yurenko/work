"""BTC candle colour and the shared recovery cycle.

These cover the 14 scenarios required before any live order:
direction while the previous market is still settling, locked recovery side,
doubling, immediate entry, combined PnL, restart, and duplicate settlements.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.polymarket.markets import resolved_updown
from app.predict.client import parse_updown_prices
from app.strategy.btc_candle import (
    BtcCandleBook,
    candle_color,
    candle_row,
    chart_candle_rows,
    chart_candles_are_current,
    completed_window_starts,
    format_new_market,
    loss_recovery_direction,
    parse_chainlink_frame,
    parse_polymarket_crypto_price,
    shared_candle_direction,
)
from app.strategy.lockin import in_entry_zone, plan_venue
from app.strategy.recovery import (
    RecoveryBook,
    RecoveryConfig,
    allows_immediate_entry,
    recovery_entry_decision,
    exposed_recovery_side,
    paid_on_every_venue,
)


SOURCE = "polymarket.ws.crypto_prices_chainlink"


def config() -> RecoveryConfig:
    return RecoveryConfig(
        base_shares=6,
        shares_step=6,
        max_shares=12,
        max_debt_usd=50,
        multiplier=2,
        hedging_base=True,
        hedging_recovery=False,
    )


def book(path: Path | None = None) -> RecoveryBook:
    return RecoveryBook(config(), path)


class CandleDirectionTests(unittest.TestCase):
    def _window(self, folder: Path, last: float, open_price: float = 67000.0) -> BtcCandleBook:
        candles = BtcCandleBook(max_age_sec=20, path=folder / "btc.json")
        start, end = 1_000_000.0, 1_000_300.0
        candles.upsert_market("predict", "prev", start, end, now=start + 5)
        candles.upsert_market("polymarket", "btc-updown-5m-prev", start, end, now=start + 5)
        candles.set_official_open(
            "predict", "prev", open_price, start, "predict.variantDetails.upDown.startPrice",
        )
        candles.set_official_open(
            "polymarket", "btc-updown-5m-prev", open_price, start,
            "polymarket.crypto-price.openPrice",
        )
        candles.add_tick(last, end - 1, SOURCE)
        now = end + 1
        candles.upsert_market("predict", "live", end, end + 300, now=now)
        candles.upsert_market("polymarket", "btc-updown-5m-live", end, end + 300, now=now)
        return candles

    def test_1_green_candle_is_known_while_previous_market_is_settling(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            candles = self._window(Path(folder), 67500.0)
            previous = candles.previous_candle("predict")
            self.assertIsNotNone(previous)
            self.assertEqual(previous.status, "SETTLING")
            self.assertEqual(previous.settlement, "PENDING")
            self.assertIsNone(previous.close)
            self.assertEqual(previous.direction(20, now=1_000_301), "UP")
            self.assertEqual(previous.color(20, now=1_000_301), "GREEN")
            text = format_new_market(
                "Predict.fun", "live", "LIVE", previous, 20, now=1_000_301,
            )
            self.assertIn("Direction: UP", text)
            self.assertIn("Color: GREEN", text)
            self.assertIn("Status: SETTLING", text)
            self.assertIn("Settlement: PENDING", text)

    def test_2_red_candle_is_known_while_previous_market_is_settling(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            candles = self._window(Path(folder), 67000.0, open_price=67500.0)
            previous = candles.previous_candle("polymarket")
            self.assertEqual(previous.status, "SETTLING")
            self.assertEqual(previous.direction(20, now=1_000_301), "DOWN")
            self.assertEqual(candle_color("DOWN"), "RED")
            self.assertIsNone(previous.close)

    def test_official_close_does_not_recolour_the_candle(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            candles = self._window(Path(folder), 67500.0)
            candles.set_official_close(
                "predict", "prev", 66000.0, 1_000_300.0, "predict.variantDetails.upDown.endPrice",
            )
            previous = candles.previous_candle("predict")
            self.assertEqual(previous.close.price, 66000.0)
            self.assertEqual(previous.direction(20, now=1_000_301), "UP")
            self.assertEqual(previous.status, "RESOLVED")


class RecoveryCycleTests(unittest.TestCase):
    def test_3_base_loss_on_a_green_candle_locks_up_and_disables_the_hedge(self) -> None:
        size = book()
        self.assertEqual(size.shares_for_entry(), 6)
        self.assertTrue(size.hedging_enabled())
        event = size.on_pair_closed("base-loss", -1.5, -1.0, fees=0, candle_direction="UP")
        self.assertEqual(event.reason, "loss")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.recovery_direction, "UP")
        self.assertTrue(size.direction_locked)
        self.assertEqual(size.current_shares, 12)
        self.assertFalse(size.hedging_enabled())
        self.assertEqual(size.order_side(), "up")
        self.assertIn("Recovery Mode: ON", event.line())
        self.assertIn("Hedging: OFF", event.line())
        self.assertIn("Price Threshold: DISABLED", event.line())
        self.assertIn("Entry Mode: IMMEDIATE", event.line())

    def test_4_and_5_later_losses_double_without_changing_direction(self) -> None:
        size = book()
        size.on_pair_closed("l1", -1, -1, candle_direction="UP")
        self.assertEqual(size.current_shares, 12)
        second = size.on_pair_closed("l2", -1, -1, candle_direction="UP")
        self.assertEqual(size.recovery_direction, "UP")
        self.assertEqual(size.current_shares, 24)
        self.assertIn("Previous Shares: 12", second.line())
        self.assertIn("Next Shares: 24", second.line())
        self.assertIn("Direction Changed: FALSE", second.line())
        size.on_pair_closed("l3", -1, -1, candle_direction="UP")
        self.assertEqual(size.recovery_direction, "UP")
        self.assertEqual(size.current_shares, 48)
        self.assertFalse(size.hedging_enabled())

    def test_6_recovery_entry_does_not_wait_for_the_base_price_band(self) -> None:
        self.assertTrue(allows_immediate_entry(0.35))
        self.assertTrue(allows_immediate_entry(0.60))
        self.assertTrue(allows_immediate_entry(0.90))
        self.assertFalse(in_entry_zone(0.35, low=0.58, high=0.82))
        self.assertFalse(in_entry_zone(0.90, low=0.58, high=0.82))
        self.assertFalse(allows_immediate_entry(None))
        self.assertFalse(allows_immediate_entry(0.0))
        self.assertFalse(allows_immediate_entry(0.99))
        self.assertFalse(allows_immediate_entry(0.98))

    def test_ending_price_releases_the_next_entry_before_both_venues_settle(self) -> None:
        # 798dcbb6: Predict already finished Down, Polymarket was still blank.
        # The UP position lost. The next market enters doubled, same side.
        self.assertEqual(recovery_entry_decision("UP", False, None), "loss")
        # One venue already paid the position. That is a win, not a double.
        self.assertEqual(recovery_entry_decision("DOWN", False, None), "win")
        # Both venues finished on opposite sides. Keep the size and enter.
        self.assertEqual(recovery_entry_decision("DOWN", False, True), "keep")
        self.assertEqual(recovery_entry_decision("UP", True, True), "win")
        self.assertEqual(recovery_entry_decision("UP", False, False), "loss")
        self.assertIsNone(recovery_entry_decision("UP", None, None))
        self.assertIsNone(recovery_entry_decision(None, False, False))

    def test_a_priced_win_that_clears_the_debt_enters_at_base(self) -> None:
        size = book()
        size.on_pair_closed("loss", -460.80, -960.0, candle_direction="DOWN")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.current_shares, 12)
        self.assertAlmostEqual(size.recovery_debt, 1420.80)
        # 1536 shares bought at 0.50 and 0.31 pay 1827.84 if that side wins.
        won = size.arm_priced_win("848e8b63", 1827.84)
        self.assertEqual(won.reason, "win")
        self.assertEqual(size.mode, "BASE")
        self.assertEqual(size.current_shares, 6)
        self.assertEqual(size.recovery_debt, 0)
        self.assertTrue(size.hedging_enabled())
        confirmed = size.on_pair_closed("848e8b63", 768.0, 1059.84)
        self.assertEqual(size.mode, "BASE")
        self.assertEqual(size.current_shares, 6)
        self.assertEqual(size.recovery_debt, 0)
        self.assertEqual(confirmed.reason, "win")

    def test_one_selected_venue_plus_clears_the_debt(self) -> None:
        size = book()
        size.on_pair_closed("loss", -4.0, 0.0, candle_direction="UP", venues="predict")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.current_shares, 12)
        won = size.on_pair_closed("win", 8.0, 0.0, venues="predict")
        self.assertEqual(won.reason, "win")
        self.assertEqual(size.mode, "BASE")
        self.assertEqual(size.current_shares, 6)
        self.assertEqual(size.recovery_debt, 0)
        self.assertTrue(size.hedging_enabled())
        self.assertFalse(paid_on_every_venue(8.0, 0.0, "both"))
        self.assertTrue(paid_on_every_venue(8.0, 0.0, "predict"))

    def test_last_ten_completed_windows_are_oldest_first(self) -> None:
        starts = completed_window_starts(1_700_000_100, 10)
        self.assertEqual(len(starts), 10)
        self.assertEqual(starts[1] - starts[0], 300)
        self.assertLess(starts[-1], 1_700_000_100)
        green = candle_row(starts[0], 100.0, 101.0)
        red = candle_row(starts[1], 101.0, 100.0)
        self.assertEqual(green["color"], "GREEN")
        self.assertEqual(red["color"], "RED")
        self.assertIsNone(candle_row(starts[2], None, 100.0))
        wick = candle_row(starts[0], 100.0, 99.0, high_price=110.0, low_price=98.0)
        self.assertEqual(wick["color"], "RED")
        self.assertEqual(wick["high"], 110.0)
        self.assertEqual(wick["low"], 98.0)
        now = starts[-1] + 300 + 10
        drawn = chart_candle_rows(
            [
                {"time": starts[-1], "open": 10, "high": 12, "low": 9, "close": 11},
                {"time": starts[-1] + 300, "open": 11, "high": 15, "low": 11, "close": 14},
            ],
            now,
            10,
        )
        self.assertEqual([row["start"] for row in drawn], [starts[-1]])
        self.assertEqual(drawn[0]["color"], "GREEN")
        self.assertTrue(chart_candles_are_current(
            [{"time": starts[-1] + 300, "open": 1, "close": 2}],
            now,
        ))
        self.assertFalse(chart_candles_are_current(
            [{"time": starts[0], "open": 1, "close": 2}],
            now,
        ))

    def test_7_a_new_red_candle_does_not_flip_a_locked_up_recovery(self) -> None:
        size = book()
        size.on_pair_closed("l1", -1, -1, candle_direction="UP")
        size.on_pair_closed("l2", -1, -1, candle_direction="DOWN")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.recovery_direction, "UP")
        self.assertEqual(size.order_side(), "up")
        self.assertEqual(size.current_shares, 24)
        self.assertFalse(size.direction_locked is False)

    def test_8_a_recovery_plus_keeps_the_same_shares_and_side(self) -> None:
        size = book()
        size.on_pair_closed("l1", -1, -1, candle_direction="DOWN")
        size.on_pair_closed("l2", -1, -1, candle_direction="UP")
        self.assertEqual(size.current_shares, 24)
        self.assertEqual(size.recovery_direction, "DOWN")
        # One venue plus, the other minus, and the sum is positive.
        won = size.on_pair_closed("split-plus", 8.28, -4.20, candle_direction="UP")
        self.assertEqual(won.reason, "win")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.recovery_direction, "DOWN")
        self.assertEqual(size.current_shares, 24)
        self.assertFalse(size.hedging_enabled())
        lost = size.on_pair_closed("minus", -10, -5, candle_direction="UP")
        self.assertEqual(lost.reason, "loss")
        self.assertEqual(size.recovery_direction, "DOWN")
        self.assertEqual(size.current_shares, 48)

    def test_both_venues_plus_returns_to_base_hedge(self) -> None:
        size = book()
        size.on_pair_closed("loss", -1.6, -1.5, candle_direction="UP", keep_direction="UP")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.current_shares, 12)
        won = size.on_pair_closed("2d218b6d", 4.56, 4.08)
        self.assertEqual(won.reason, "win")
        self.assertEqual(size.mode, "BASE")
        self.assertIsNone(size.recovery_direction)
        self.assertEqual(size.current_shares, 6)
        self.assertEqual(size.recovery_debt, 0)
        self.assertTrue(size.hedging_enabled())
        self.assertIn("New Mode: BASE", won.line())
        self.assertIn("Hedging: ON", won.line())

    def test_both_venues_plus_with_debt_left_sizes_the_next_shares(self) -> None:
        size = book()
        size.on_pair_closed("c8afdc8a", -2.70, 0.0, candle_direction="UP")
        size.on_pair_closed("a4f25566", -6.66, -6.66)
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.current_shares, 24)
        self.assertAlmostEqual(size.recovery_debt, 16.02)
        won = size.on_pair_closed("c8e00dc7", 6.96, 5.28)
        self.assertEqual(won.reason, "win")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.recovery_direction, "UP")
        self.assertAlmostEqual(size.recovery_debt, 3.78)
        self.assertEqual(size.current_shares, 4)
        self.assertFalse(size.hedging_enabled())
        cleared = size.on_pair_closed("next-win", 2.0, 2.0)
        self.assertEqual(size.mode, "BASE")
        self.assertEqual(size.current_shares, 6)
        self.assertEqual(size.recovery_debt, 0)
        self.assertTrue(size.hedging_enabled())
        self.assertEqual(cleared.reason, "win")

    def test_tiny_both_venues_plus_does_not_drop_to_base(self) -> None:
        size = book()
        size.on_pair_closed("big-loss", -100.0, -6.16, candle_direction="UP")
        self.assertEqual(size.current_shares, 12)
        won = size.on_pair_closed("ebcdc18b", 1.92, 1.92)
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.recovery_direction, "UP")
        self.assertAlmostEqual(size.recovery_debt, 102.32)
        self.assertEqual(size.current_shares, 103)
        self.assertFalse(size.hedging_enabled())

    def test_9_one_resolved_venue_does_not_change_martingale(self) -> None:
        size = book()
        size.note_platform("cycle", "pf", 1.0, confirmed=True)
        size.note_platform("cycle", "pm", None, confirmed=False)
        pending = size._maybe_apply("cycle", 0.0, 0.0, "UP")
        self.assertIsNone(pending)
        self.assertEqual(size.mode, "BASE")
        self.assertEqual(size.current_shares, 6)
        self.assertIsNone(size.recovery_direction)
        done = size.on_pair_closed("cycle", 1.0, -3.0, candle_direction="UP")
        self.assertEqual(done.reason, "loss")
        self.assertEqual(done.pair_pnl, -2)
        self.assertEqual(size.current_shares, 12)

    def test_12_equal_open_and_last_does_not_start_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            candles = BtcCandleBook(max_age_sec=20, path=Path(folder) / "btc.json")
            start, end = 1_000_000.0, 1_000_300.0
            candles.upsert_market("predict", "flat", start, end, now=start + 1)
            candles.set_official_open("predict", "flat", 67000, start, "predict.variantDetails.upDown.startPrice")
            candles.add_tick(67000, end - 1, SOURCE)
            self.assertEqual(candles.direction_of("predict", "flat", now=end + 1), "NEUTRAL")
        size = book()
        event = size.on_pair_closed("flat-loss", -1, -1, candle_direction="NEUTRAL")
        self.assertEqual(event.reason, "loss")
        self.assertEqual(size.mode, "BASE")
        self.assertIsNone(size.recovery_direction)
        self.assertEqual(size.current_shares, 6)
        self.assertTrue(size.hedging_enabled())
        stale = book()
        self.assertIsNone(stale.on_pair_closed("stale", -1, -1, candle_direction="STALE"))
        self.assertEqual(stale.mode, "BASE")
        self.assertEqual(stale.current_shares, 6)
        self.assertNotIn("stale", stale.processed_ids)
        later = stale.on_pair_closed("stale", -1, -1, candle_direction="UP")
        self.assertEqual(later.reason, "loss")
        self.assertEqual(stale.mode, "RECOVERY")
        self.assertEqual(stale.current_shares, 12)
        self.assertFalse(stale.hedging_enabled())

    def test_13_combined_pnl_decides_when_the_venues_disagree(self) -> None:
        loss = book()
        event = loss.on_pair_closed("mix-loss", 2.0, -3.0, fees=0.0, candle_direction="DOWN")
        self.assertEqual(event.pair_pnl, -1)
        self.assertEqual(loss.mode, "RECOVERY")
        self.assertEqual(loss.recovery_direction, "DOWN")
        self.assertEqual(loss.current_shares, 12)

        win = book()
        event = win.on_pair_closed("mix-win", 3.0, -1.0, fees=0.0, candle_direction="UP")
        self.assertEqual(event.pair_pnl, 2)
        self.assertEqual(event.reason, "win")
        self.assertEqual(win.mode, "BASE")
        self.assertEqual(win.current_shares, 6)

        # Fees already counted in the venue PnL can still be passed explicitly.
        charged = book()
        event = charged.on_pair_closed("fees", 1.0, 1.0, fees=2.5, candle_direction="UP")
        self.assertAlmostEqual(event.pair_pnl, -0.5)
        self.assertEqual(charged.mode, "RECOVERY")

    def test_candle_loss_opens_recovery_before_settlement_without_a_second_double(self) -> None:
        size = book()
        armed = size.arm_loss_from_candle("h1", "DOWN")
        self.assertEqual(armed.reason, "loss")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.recovery_direction, "DOWN")
        self.assertEqual(size.current_shares, 12)
        self.assertFalse(size.hedging_enabled())
        confirmed = size.on_pair_closed("h1", -4.5, -4.32, candle_direction="DOWN")
        self.assertEqual(confirmed.reason, "loss")
        self.assertEqual(size.current_shares, 12)
        self.assertAlmostEqual(size.recovery_debt, 8.82)
        self.assertIsNone(size.on_pair_closed("h1", -4.5, -4.32, candle_direction="DOWN"))

    def test_second_candle_loss_keeps_the_locked_side(self) -> None:
        size = book()
        size.arm_loss_from_candle("h1", "UP")
        size.on_pair_closed("h1", -1, -1, candle_direction="UP")
        size.arm_loss_from_candle("h2", "DOWN")
        self.assertEqual(size.recovery_direction, "UP")
        self.assertEqual(size.current_shares, 24)

    def test_split_venues_stay_in_recovery_when_the_sum_is_negative(self) -> None:
        size = book()
        size.on_pair_closed("first", -1, -1, candle_direction="DOWN")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.current_shares, 12)
        # Predict pays the position, Polymarket does not. The sum is still a loss.
        event = size.on_pair_closed("split", 4.20, -7.68, candle_direction="DOWN")
        self.assertEqual(event.reason, "loss")
        self.assertEqual(event.pair_pnl, -3.48)
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.recovery_direction, "DOWN")
        self.assertEqual(size.current_shares, 24)
        self.assertFalse(size.hedging_enabled())

    def test_known_win_opens_the_next_base_entry_before_settlement(self) -> None:
        size = book()
        event = size.arm_win("h1")
        self.assertEqual(event.reason, "win")
        self.assertEqual(size.mode, "BASE")
        self.assertEqual(size.current_shares, 6)
        self.assertTrue(size.hedging_enabled())
        confirmed = size.on_pair_closed("h1", 0.4, 0.46)
        self.assertEqual(confirmed.reason, "win")
        self.assertEqual(size.mode, "BASE")
        self.assertEqual(size.current_shares, 6)
        self.assertIsNone(size.on_pair_closed("h1", 0.4, 0.46))

    def test_official_loss_after_a_known_win_recovers_the_following_entry(self) -> None:
        size = book()
        size.arm_win("h1")
        event = size.on_pair_closed("h1", -2, -1, candle_direction="DOWN")
        self.assertEqual(event.reason, "loss")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.recovery_direction, "DOWN")
        self.assertEqual(size.current_shares, 12)
        self.assertFalse(size.hedging_enabled())

    def test_recovery_plus_undoes_an_early_double_and_keeps_the_side(self) -> None:
        # 625cb9f3 was already UP. The candle was DOWN, so the size doubled
        # before the venues summed. The sum was a plus, so the size goes back
        # and the side stays.
        size = book()
        size.arm_loss_from_candle("first", "UP", pair_shares=6)
        self.assertEqual(size.current_shares, 12)
        size.arm_loss_from_candle("625cb9f3", "DOWN", pair_shares=12)
        self.assertEqual(size.current_shares, 24)
        win = size.on_pair_closed("625cb9f3", 20, -5, candle_direction="DOWN")
        self.assertEqual(win.reason, "win")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.recovery_direction, "UP")
        self.assertEqual(size.current_shares, 12)
        self.assertFalse(size.hedging_enabled())

    def test_first_loss_locks_the_position_side_not_the_candle(self) -> None:
        size = book()
        event = size.on_pair_closed(
            "04c811e5", -3.84, -3.72, candle_direction="UP", keep_direction="DOWN",
        )
        self.assertEqual(event.reason, "loss")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.recovery_direction, "DOWN")
        self.assertEqual(size.current_shares, 12)
        self.assertIn("Same side as the losing position", event.line())

    def test_unhedged_venue_sets_the_recovery_side(self) -> None:
        # 22080c9d: Predict DOWN was hedged, Polymarket stayed UP and lost.
        self.assertEqual(exposed_recovery_side("down", True, "up", False), "UP")
        self.assertEqual(exposed_recovery_side("up", False, "down", True), "UP")
        self.assertEqual(exposed_recovery_side("down", False, "down", False), "DOWN")
        self.assertIsNone(exposed_recovery_side("down", True, "up", True))
        self.assertIsNone(exposed_recovery_side("up", False, "down", False))

    def test_official_win_after_a_candle_loss_resets_the_following_entry(self) -> None:
        size = book()
        size.arm_loss_from_candle("h1", "UP")
        event = size.on_pair_closed("h1", 5, 5, candle_direction="UP")
        self.assertEqual(event.reason, "win")
        self.assertEqual(size.mode, "BASE")
        self.assertEqual(size.current_shares, 6)
        self.assertTrue(size.hedging_enabled())

    def test_extreme_book_is_a_result_before_the_closed_flag(self) -> None:
        event = {
            "closed": False,
            "markets": [{
                "closed": False,
                "outcomes": ["Up", "Down"],
                "outcomePrices": ["0.995", "0.005"],
            }],
        }
        resolved = resolved_updown(event)
        self.assertEqual(resolved["up"], 1.0)
        self.assertEqual(resolved["down"], 0.0)
        self.assertEqual(resolved["_official"], 0.0)
        event["closed"] = True
        event["markets"][0]["closed"] = True
        self.assertEqual(resolved_updown(event)["_official"], 1.0)

    def test_official_close_fills_direction_when_last_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            candles = BtcCandleBook(max_age_sec=20, path=Path(folder) / "btc.json")
            start, end = 1_000_000.0, 1_000_300.0
            candles.upsert_market("polymarket", "done", start, end, now=start + 1)
            candles.set_official_open(
                "polymarket", "done", 67000.0, start, "polymarket.crypto-price.openPrice",
            )
            candles.set_official_close(
                "polymarket", "done", 67100.0, end, "polymarket.crypto-price.closePrice",
            )
            self.assertEqual(candles.direction_of("polymarket", "done", now=end + 60), "UP")

    def test_14_the_same_settlement_does_not_double_twice(self) -> None:
        size = book()
        first = size.on_pair_closed("same", -1, -1, candle_direction="UP")
        second = size.on_pair_closed("same", -1, -1, candle_direction="UP")
        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertEqual(size.current_shares, 12)
        self.assertEqual(size.recovery_level, 1)

    def test_neutral_keeps_the_recovery_ladder(self) -> None:
        size = book()
        size.on_pair_closed("l1", -1, -1, candle_direction="UP")
        event = size.on_pair_closed("flat", 1.0, -1.0, candle_direction="DOWN")
        self.assertEqual(event.reason, "neutral")
        self.assertEqual(size.mode, "RECOVERY")
        self.assertEqual(size.recovery_direction, "UP")
        self.assertEqual(size.current_shares, 12)
        self.assertIn("Direction Changed: FALSE", event.line())

    def test_10_restart_restores_the_candle_and_blocks_a_second_entry(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            candles = BtcCandleBook(max_age_sec=20, path=root / "btc.json")
            start, end = 1_000_000.0, 1_000_300.0
            candles.upsert_market("polymarket", "old", start, end, now=start + 1)
            candles.set_official_open(
                "polymarket", "old", 67000, start, "polymarket.crypto-price.openPrice",
            )
            candles.add_tick(67500, end - 1, SOURCE)
            candles.upsert_market("polymarket", "new", end, end + 300, now=end + 1)
            candles.save()

            restored = BtcCandleBook(max_age_sec=20, path=root / "btc.json")
            previous = restored.previous_candle("polymarket")
            self.assertEqual(previous.open.price, 67000)
            self.assertEqual(previous.last.price, 67500)
            self.assertEqual(previous.direction(20, now=end + 2), "UP")
            # A snapshot replay older than the stored last must not move it.
            restored.add_tick(67100, end - 5, SOURCE)
            self.assertEqual(restored.previous_candle("polymarket").last.price, 67500)

            size = book(root / "recovery.json")
            self.assertTrue(size.claim_market("pf-1:btc-updown-5m-new"))
            reloaded = RecoveryBook(config(), root / "recovery.json")
            self.assertFalse(reloaded.claim_market("pf-1:btc-updown-5m-new"))
            self.assertEqual(reloaded.previous_market_id, "pf-1:btc-updown-5m-new")

    def test_11_restart_after_two_recovery_losses_restores_direction_and_size(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "recovery.json"
            size = book(path)
            size.on_pair_closed("l1", -1, -1, candle_direction="UP")
            size.on_pair_closed("l2", -1, -1, candle_direction="DOWN")
            self.assertEqual(size.current_shares, 24)
            restored = RecoveryBook(config(), path)
            self.assertEqual(restored.mode, "RECOVERY")
            self.assertEqual(restored.recovery_direction, "UP")
            self.assertTrue(restored.direction_locked)
            self.assertEqual(restored.current_shares, 24)
            self.assertFalse(restored.hedging_enabled())
            self.assertIsNone(restored.on_pair_closed("l1", -100, -100, candle_direction="DOWN"))
            self.assertIsNone(restored.on_pair_closed("l2", -100, -100, candle_direction="DOWN"))
            self.assertEqual(restored.current_shares, 24)
            restored.on_pair_closed("l3", -1, -1, candle_direction="DOWN")
            self.assertEqual(restored.recovery_direction, "UP")
            self.assertEqual(restored.current_shares, 48)


class SourceParsingTests(unittest.TestCase):
    def test_predict_updown_details_are_the_open_and_close(self) -> None:
        payload = {
            "variantDetails": {
                "upDown": {
                    "priceFeedProvider": "CHAINLINK",
                    "priceFeedId": "btc-usd",
                    "priceFeedSymbol": "BTC/USD",
                    "startPrice": 67000,
                    "endPrice": None,
                }
            },
            "markets": [{
                "variantData": {
                    "priceFeedProvider": "CHAINLINK",
                    "startPrice": 1,
                }
            }],
        }
        parsed = parse_updown_prices(payload)
        self.assertEqual(parsed["start_price"], 67000)
        self.assertIsNone(parsed["end_price"])
        self.assertEqual(parsed["provider"], "CHAINLINK")
        self.assertEqual(parsed["source"], "predict.variantDetails.upDown")

    def test_polymarket_crypto_price_payload(self) -> None:
        live = parse_polymarket_crypto_price({
            "openPrice": 63674.704217466206,
            "closePrice": None,
            "timestamp": 1795076784710,
            "completed": False,
            "incomplete": True,
            "cached": True,
        })
        self.assertAlmostEqual(live["open"], 63674.704217466206)
        self.assertIsNone(live["close"])
        self.assertFalse(live["completed"])
        done = parse_polymarket_crypto_price({
            "openPrice": 63694.28999874938,
            "closePrice": 63674.704217466206,
            "timestamp": 1795076764036,
            "completed": True,
            "incomplete": False,
            "cached": True,
        })
        self.assertTrue(done["completed"])
        self.assertAlmostEqual(done["close"], 63674.704217466206)

    def test_chainlink_socket_update_and_empty_ack(self) -> None:
        self.assertEqual(parse_chainlink_frame(""), [])
        self.assertEqual(parse_chainlink_frame("not-json"), [])
        ticks = parse_chainlink_frame(
            '{"topic":"crypto_prices_chainlink","type":"update",'
            '"payload":{"full_accuracy_value":"63662422063001625000000",'
            '"symbol":"btc/usd","timestamp":1795076859000,"value":63662.42206300163}}'
        )
        self.assertEqual(len(ticks), 1)
        price, ts, source = ticks[0]
        self.assertAlmostEqual(price, 63662.42206300163)
        self.assertAlmostEqual(ts, 1795076859.0)
        self.assertEqual(source, SOURCE)
        history = parse_chainlink_frame(
            '{"payload":{"data":[{"timestamp":1795076800000,"value":63672.5},'
            '{"timestamp":1795076801000,"value":63670.0}]}}'
        )
        self.assertEqual([round(item[0], 1) for item in history], [63672.5, 63670.0])

    def test_platforms_must_agree_before_a_side_is_chosen(self) -> None:
        self.assertEqual(shared_candle_direction("UP", "UP"), "UP")
        self.assertEqual(shared_candle_direction("UP", "UNKNOWN"), "UP")
        self.assertEqual(shared_candle_direction("DOWN", "UP"), "UNKNOWN")
        self.assertEqual(shared_candle_direction("NEUTRAL", "UNKNOWN"), "NEUTRAL")
        self.assertEqual(shared_candle_direction("STALE", "STALE"), "UNKNOWN")

    def test_loss_uses_polymarket_when_predict_open_disagrees(self) -> None:
        # bf9b0ebd: Predict last was under its own open (DOWN) while the
        # Polymarket open/last was UP. Combined PnL was negative, so the
        # next entry follows Polymarket instead of resetting to 6 shares.
        self.assertEqual(loss_recovery_direction("DOWN", "UP"), "UP")
        self.assertEqual(loss_recovery_direction("UP", "DOWN"), "DOWN")
        self.assertEqual(loss_recovery_direction("UNKNOWN", "DOWN"), "DOWN")
        self.assertEqual(loss_recovery_direction("UP", "UP"), "UP")
        self.assertEqual(loss_recovery_direction("STALE", "STALE"), "UNKNOWN")
        self.assertEqual(loss_recovery_direction("NEUTRAL", "UNKNOWN"), "NEUTRAL")

    def test_base_mode_hedge_plan_still_locks_the_opposite_side(self) -> None:
        plan = plan_venue(
            venue="pf",
            entry=0.70,
            held_shares=6,
            hedged_shares=0,
            opposite_ask=0.15,
            min_net=0.10,
            pf_fee_bps=200,
            pm_fee_rate=0.04,
            max_shares=12,
            room_usd=50,
        )
        self.assertEqual(plan.action, "hedge")
        self.assertGreater(plan.shares, 0)


if __name__ == "__main__":
    unittest.main()
