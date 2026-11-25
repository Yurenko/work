import unittest

from app.strategy.trade_history import history_page, parse_trade_log


SAMPLE = """
2026-11-20 15:40:03 | INFO | hedge | Hedge 2f55e9f5 OPEN recovery DOWN shares=12 PF FILLED 12.00@0.500 | PM FILLED 12.00@0.360
2026-11-20 15:40:04 | INFO | hedge | PM settle check hedge=2f55e9f5 slug=btc-updown-5m-1795182000 up_px=0
2026-11-20 15:45:23 | INFO | hedge | Hedge 2f55e9f5 entry_sum 0.860 → 0.860 (pf@0.500 + pm@0.360)
2026-11-20 15:45:23 | INFO | hedge | Hedge 2f55e9f5 SETTLE pf_up=True pm_up=True pf=down@0.500→0.000($-6.00) pm=down@0.360→0.000($-4.32) pnl=-10.3200
2026-11-20 15:50:02 | INFO | hedge | Hedge aa11bb22 OPEN lock up PF FILLED 6.00@0.620 | PM FILLED 6.00@0.660
2026-11-20 15:51:02 | INFO | hedge | Hedge aa11bb22 SETTLE pf_up=True pm_up=True pf=up@0.620→1.000($2.28) pm=up@0.660→1.000($2.04) pnl=4.3200
"""


class TradeHistoryTests(unittest.TestCase):
    def test_log_rebuilds_closed_rows_oldest_first(self):
        rows = parse_trade_log(SAMPLE)
        self.assertEqual([row["id"] for row in rows], ["2f55e9f5", "aa11bb22"])
        first = rows[0]
        self.assertEqual(first["pnl"], -10.32)
        self.assertEqual(first["strategy_mode"], "RECOVERY")
        self.assertEqual(first["pf"]["direction"], "down")
        self.assertEqual(first["pf"]["shares"], 12)
        self.assertEqual(first["pm"]["entry_price"], 0.36)
        self.assertEqual(first["exit_sum"], 0)
        self.assertEqual(first["market_end"], 1795182300)
        self.assertEqual(rows[1]["strategy_mode"], "BASE")
        self.assertEqual(rows[1]["pm"]["direction"], "up")
        self.assertAlmostEqual(rows[1]["entry_sum"], 1.28)

    def test_page_is_newest_first_and_clamped(self):
        rows = [{"id": str(i)} for i in range(25)]
        page = history_page(rows, 10, 1)
        self.assertEqual(page["total"], 25)
        self.assertEqual(page["pages"], 3)
        self.assertEqual([row["id"] for row in page["rows"]], [str(i) for i in range(24, 14, -1)])
        last = history_page(rows, 10, 99)
        self.assertEqual(last["page"], 3)
        self.assertEqual([row["id"] for row in last["rows"]], ["4", "3", "2", "1", "0"])
        self.assertEqual(history_page(rows, 7, 1)["limit"], 20)

    def test_revised_chart_close_replaces_the_saved_bar(self):
        import tempfile
        from pathlib import Path
        from app.strategy.trade_history import TradeHistory

        folder = Path(tempfile.mkdtemp())
        history = TradeHistory(
            folder / "history.json",
            log_path=folder / "missing.log",
            journal_path=folder / "missing.json",
        )
        history.rows = [{
            "id": "pos",
            "opened_at": 3000,
            "candles": {
                "predict": [{"start": 2700, "open": 10, "high": 12, "low": 9, "close": 11, "color": "GREEN", "direction": "UP"}],
                "polymarket": [{"start": 2700, "open": 10, "high": 11, "low": 8, "close": 9, "color": "RED", "direction": "DOWN"}],
            },
        }]
        history.refresh_cached_candles({
            "predict": [{"start": 2700, "open": 10, "high": 12, "low": 9, "close": 11, "color": "GREEN", "direction": "UP"}],
            "polymarket": [
                {"start": 2700, "open": 10, "high": 14, "low": 9, "close": 13, "color": "GREEN", "direction": "UP"},
                {"start": 3000, "open": 13, "high": 15, "low": 12, "close": 14, "color": "GREEN", "direction": "UP"},
            ],
        })
        saved = history.rows[0]["candles"]
        self.assertEqual(saved["polymarket"][0]["color"], "GREEN")
        self.assertEqual(saved["polymarket"][0]["close"], 13)
        self.assertEqual([row["start"] for row in saved["polymarket"]], [2700])
        self.assertEqual(saved["predict"][0]["close"], 11)


if __name__ == "__main__":
    unittest.main()
