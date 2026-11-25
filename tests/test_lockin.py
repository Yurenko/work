"""Lock-in entry and per-exchange opposite hedge."""

from __future__ import annotations

import unittest

from app.polymarket.client import polymarket_buy_order
from app.strategy.lockin import (
    choose_direction,
    entry_orders,
    four_scenarios,
    net_per_share,
    plan_venue,
    waiting_side,
)


class LockInTests(unittest.TestCase):
    def test_entry_is_the_same_direction_near_70(self) -> None:
        self.assertEqual(
            choose_direction(0.71, 0.30, 0.69, 0.32),
            "up",
        )
        self.assertEqual(
            choose_direction(0.40, 0.68, 0.35, 0.72),
            "down",
        )
        self.assertEqual(choose_direction(0.70, 0.30, 0.50, 0.50), "up")
        self.assertIsNone(choose_direction(0.90, 0.10, 0.91, 0.09))

    def test_venues_enter_on_their_own_when_prices_diverge(self) -> None:
        # PF 0.68 and PM 0.81 are both still an entry. They do not have to
        # sit within 6 cents of each other.
        direction, venues = entry_orders(0.68, 0.33, 0.81, 0.20)
        self.assertEqual(direction, "up")
        self.assertEqual(venues, ["pf", "pm"])
        # PM has already run to 0.89. Buy Predict, do not chase Polymarket.
        direction, venues = entry_orders(0.72, 0.29, 0.89, 0.12)
        self.assertEqual(direction, "up")
        self.assertEqual(venues, ["pf"])
        # Nothing near 0.70.
        direction, venues = entry_orders(0.40, 0.61, 0.90, 0.11)
        self.assertEqual(direction, "down")
        self.assertEqual(venues, ["pf"])

    def test_unfilled_venue_follows_a_reversal_into_the_window(self) -> None:
        # Opened UP. That ask left the window. DOWN is now 0.60, so buy DOWN.
        self.assertEqual(waiting_side("up", 0.40, 0.60), ("down", 0.60))
        # Original side is still an entry. Do not flip.
        self.assertEqual(waiting_side("up", 0.68, 0.33), ("up", 0.68))
        self.assertIsNone(waiting_side("up", 0.90, 0.12))

    def test_ten_cent_opposite_locks_profit_thirty_does_not(self) -> None:
        cheap = net_per_share(0.70, 0.10, "pf")
        even = net_per_share(0.70, 0.30, "pf")
        self.assertGreater(cheap, 0.15)
        self.assertLess(even, 0.0)
        pm_cheap = net_per_share(0.70, 0.10, "pm")
        self.assertGreater(pm_cheap, 0.15)

    def test_hedge_the_cheap_exchange_and_wait_on_the_other(self) -> None:
        pf = plan_venue(
            venue="pf", entry=0.70, held_shares=6, hedged_shares=0,
            opposite_ask=0.30, min_net=0.10, pf_fee_bps=200, pm_fee_rate=0.04,
            max_shares=12, room_usd=40,
        )
        pm = plan_venue(
            venue="pm", entry=0.70, held_shares=6, hedged_shares=0,
            opposite_ask=0.10, min_net=0.10, pf_fee_bps=200, pm_fee_rate=0.04,
            max_shares=12, room_usd=40,
        )
        self.assertEqual(pf.action, "wait")
        self.assertEqual(pm.action, "hedge")
        self.assertAlmostEqual(pm.shares, 6.0, delta=0.01)
        self.assertGreater(pm.net_per_share, pf.net_per_share)

    def test_full_hedge_stops(self) -> None:
        done = plan_venue(
            venue="pf", entry=0.70, held_shares=6, hedged_shares=6,
            opposite_ask=0.05, min_net=0.10, pf_fee_bps=200, pm_fee_rate=0.04,
            max_shares=12, room_usd=40,
        )
        self.assertEqual(done.action, "hold")

    def test_four_settlements_stay_independent(self) -> None:
        # UP 6 @ 0.70 on both, plus PF DOWN 6 @ 0.10. PM still open.
        cost = 6 * 0.70 + 6 * 0.70 + 6 * 0.10
        sc = four_scenarios(6, 6, 6, 0, cost)
        self.assertAlmostEqual(sc["pf_up_pm_up"], 12 - cost)
        self.assertAlmostEqual(sc["pf_down_pm_up"], 12 - cost)
        self.assertAlmostEqual(sc["pf_up_pm_down"], 6 - cost)
        self.assertAlmostEqual(sc["pf_down_pm_down"], 6 - cost)
        self.assertNotAlmostEqual(sc["pf_up_pm_up"], sc["pf_up_pm_down"])


class PolymarketMinNotionalTests(unittest.TestCase):
    def test_cheap_hedge_is_raised_to_at_least_1_19(self) -> None:
        limit, size = polymarket_buy_order(6, 0.08)
        self.assertEqual(limit, 0.08)
        self.assertGreater(size, 6)
        self.assertGreaterEqual(round(size * limit, 2), 1.19)

    def test_normal_ask_keeps_the_target_size(self) -> None:
        limit, size = polymarket_buy_order(6, 0.82)
        self.assertEqual((limit, size), (0.82, 6.0))

    def test_one_cent_ask_is_refused(self) -> None:
        self.assertEqual(polymarket_buy_order(6, 0.01), (0.0, 0.0))


if __name__ == "__main__":
    unittest.main()
