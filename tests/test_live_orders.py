"""Binance SAS is not retried, and a Polymarket buy is not padded above the ask."""

from __future__ import annotations

import unittest

from app.binance.client import explain_binance_reject, is_permanent_bn_reject
from app.polymarket.client import polymarket_buy_order


class BinanceRejectTest(unittest.TestCase):
    def test_sas_is_permanent(self) -> None:
        raw = (
            'Binance 400: {"code":-31003,"msg":"SAS authorization required. '
            'Please complete SAS authorization on Binance App first."}'
        )
        self.assertTrue(is_permanent_bn_reject(raw))
        text = explain_binance_reject(raw)
        self.assertIn("-31003", text)
        self.assertIn("Prediction Trading", text)

    def test_ordinary_reject_can_be_retried(self) -> None:
        raw = "Binance 400: insufficient balance"
        self.assertFalse(is_permanent_bn_reject(raw))
        self.assertEqual(explain_binance_reject(raw), raw)


class PolymarketBuySizeTest(unittest.TestCase):
    def test_five_shares_stay_five_at_the_ask(self) -> None:
        limit, size = polymarket_buy_order(5, 0.32)
        self.assertEqual(limit, 0.32)
        self.assertEqual(size, 5)
        # The live fill was 5.50 because 5 × 0.33 / 0.30. Do not build that budget.
        self.assertNotEqual(round(size * limit, 2), 1.65)

    def test_cheaper_book_uses_that_ask(self) -> None:
        limit, size = polymarket_buy_order(5, 0.30)
        self.assertEqual((limit, size), (0.30, 5))
        self.assertAlmostEqual(size * limit / 0.30, 5)

    def test_price_between_ticks_does_not_round_the_budget_up(self) -> None:
        limit, size = polymarket_buy_order(5, 0.326)
        self.assertLessEqual(limit, 0.326)
        self.assertLessEqual(round(size * limit, 2), round(5 * 0.326, 2))


if __name__ == "__main__":
    unittest.main()
