"""Closed hedges kept on disk so History survives a restart."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

LIMITS = (10, 20, 50, 100)
MAX_ROWS = 500

_OPEN_RECOVERY = re.compile(
    r"Hedge (\w+) OPEN recovery (\w+) shares=(\d+(?:\.\d+)?) "
    r"PF (\S+) ([\d.]+)@([\d.]+) \| PM (\S+) ([\d.]+)@([\d.]+)"
)
_OPEN_LOCK = re.compile(
    r"Hedge (\w+) OPEN lock (\w+) PF (\S+) ([\d.]+)@([\d.]+) \| PM (\S+) ([\d.]+)@([\d.]+)"
)
_OPEN_PLAIN = re.compile(
    r"Hedge (\w+) OPEN PF ([\d.]+)@([\d.]+) \+ PM ([\d.]+)@([\d.]+) "
    r"entry_sum=([\d.]+) target_net=([\d.]+)"
)
_SETTLE = re.compile(
    r"Hedge (\w+) SETTLE pf_up=\S+ pm_up=\S+ "
    r"pf=(\w+)@([\d.]+)→([\d.]+)\(\$(-?[\d.]+)\) "
    r"pm=(\w+)@([\d.]+)→([\d.]+)\(\$(-?[\d.]+)\) pnl=(-?[\d.]+)"
)
_EXIT = re.compile(
    r"Hedge (\w+) EXIT pf=([\d.]+)→([\d.]+) pm=([\d.]+)→([\d.]+) "
    r"exit_sum=([\d.]+) pnl=(-?[\d.]+)"
)
_SLUG = re.compile(r"hedge=(\w+) slug=btc-updown-5m-(\d+)")
_ENTRY_SUM = re.compile(r"Hedge (\w+) entry_sum ([\d.]+) → ([\d.]+)")


def _stamp(line: str) -> float:
    try:
        return datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S").timestamp()
    except ValueError:
        return 0.0


def _leg(
    direction: str,
    shares: float,
    entry: float,
    exit_price: float,
    pnl: float,
    status: str = "CLOSED",
) -> dict:
    entry_usd = round(float(shares) * float(entry), 4) if shares and entry else 0.0
    exit_usd = round(float(shares) * float(exit_price), 4) if shares else 0.0
    return {
        "direction": (direction or "").lower(),
        "status": status or "CLOSED",
        "shares": float(shares or 0),
        "filled_shares": float(shares or 0),
        "entry_price": float(entry or 0),
        "exit_price": float(exit_price or 0),
        "entry": float(entry or 0),
        "exit": float(exit_price or 0),
        "entry_usd": entry_usd,
        "exit_usd": exit_usd,
        "pnl": round(float(pnl or 0), 4),
    }


def _blank() -> dict:
    return {
        "opened_at": 0.0,
        "market_end": 0.0,
        "mode": "BASE",
        "direction": "",
        "pf_shares": 0.0,
        "pm_shares": 0.0,
        "pf_entry": 0.0,
        "pm_entry": 0.0,
        "pf_status": "FILLED",
        "pm_status": "FILLED",
        "entry_sum": 0.0,
        "exit_sum": 0.0,
    }


def parse_trade_log(text: str) -> list:
    """Rebuild closed rows from bot.log. Newest trades stay last."""
    opens: dict[str, dict] = {}
    closed: dict[str, dict] = {}
    order: list[str] = []
    for line in text.splitlines():
        slug = _SLUG.search(line)
        if slug:
            slot = opens.setdefault(slug.group(1), _blank())
            slot["market_end"] = float(slug.group(2)) + 300.0
        summed = _ENTRY_SUM.search(line)
        if summed:
            slot = opens.setdefault(summed.group(1), _blank())
            slot["entry_sum"] = float(summed.group(3))
        recovery = _OPEN_RECOVERY.search(line)
        lock = _OPEN_LOCK.search(line)
        plain = _OPEN_PLAIN.search(line)
        if plain and not recovery and not lock:
            slot = opens.setdefault(plain.group(1), _blank())
            slot["opened_at"] = _stamp(line) or slot["opened_at"]
            slot["pf_shares"] = float(plain.group(2))
            slot["pf_entry"] = float(plain.group(3))
            slot["pm_shares"] = float(plain.group(4))
            slot["pm_entry"] = float(plain.group(5))
            slot["entry_sum"] = float(plain.group(6))
        if recovery:
            slot = opens.setdefault(recovery.group(1), _blank())
            slot["opened_at"] = _stamp(line) or slot["opened_at"]
            slot["mode"] = "RECOVERY"
            slot["direction"] = recovery.group(2).lower()
            slot["pf_status"] = recovery.group(4)
            slot["pf_shares"] = float(recovery.group(5))
            slot["pf_entry"] = float(recovery.group(6))
            slot["pm_status"] = recovery.group(7)
            slot["pm_shares"] = float(recovery.group(8))
            slot["pm_entry"] = float(recovery.group(9))
        elif lock:
            slot = opens.setdefault(lock.group(1), _blank())
            slot["opened_at"] = _stamp(line) or slot["opened_at"]
            slot["mode"] = "BASE"
            slot["direction"] = lock.group(2).lower()
            slot["pf_status"] = lock.group(3)
            slot["pf_shares"] = float(lock.group(4))
            slot["pf_entry"] = float(lock.group(5))
            slot["pm_status"] = lock.group(6)
            slot["pm_shares"] = float(lock.group(7))
            slot["pm_entry"] = float(lock.group(8))
        settle = _SETTLE.search(line)
        exited = _EXIT.search(line)
        if not settle and not exited:
            continue
        if settle:
            hedge_id = settle.group(1)
            pf_dir, pf_entry, pf_exit, pf_pnl = settle.group(2), float(settle.group(3)), float(settle.group(4)), float(settle.group(5))
            pm_dir, pm_entry, pm_exit, pm_pnl = settle.group(6), float(settle.group(7)), float(settle.group(8)), float(settle.group(9))
            pnl = float(settle.group(10))
            reason = "SETTLE"
            exit_sum = pf_exit + pm_exit
        else:
            hedge_id = exited.group(1)
            pf_dir = pm_dir = ""
            pf_entry, pf_exit = float(exited.group(2)), float(exited.group(3))
            pm_entry, pm_exit = float(exited.group(4)), float(exited.group(5))
            exit_sum = float(exited.group(6))
            pnl = float(exited.group(7))
            pf_pnl = pm_pnl = 0.0
            reason = "EXIT"
        slot = opens.get(hedge_id) or _blank()
        pf_shares = float(slot["pf_shares"] or 0)
        pm_shares = float(slot["pm_shares"] or 0)
        if not pf_dir:
            pf_dir = slot["direction"]
        if not pm_dir:
            pm_dir = slot["direction"]
        when = _stamp(line)
        row = {
            "id": hedge_id,
            "status": "CLOSED",
            "close_reason": reason,
            "entry_sum": float(slot["entry_sum"] or (pf_entry + pm_entry)),
            "exit_sum": float(slot["exit_sum"] or exit_sum),
            "pnl": pnl,
            "opened_at": float(slot["opened_at"] or when),
            "market_end": float(slot["market_end"] or when),
            "venues": "both",
            "strategy_mode": slot["mode"] or "BASE",
            "asset": "btc",
            "tf": "5m",
            "candles": {},
            "pf": _leg(pf_dir, pf_shares, pf_entry or slot["pf_entry"], pf_exit, pf_pnl, slot["pf_status"]),
            "pm": _leg(pm_dir, pm_shares, pm_entry or slot["pm_entry"], pm_exit, pm_pnl, slot["pm_status"]),
        }
        row["bn"] = row["pf"]
        if hedge_id not in closed:
            order.append(hedge_id)
        closed[hedge_id] = row
    return [closed[hedge_id] for hedge_id in order][-MAX_ROWS:]


def candle_print_changed(saved: dict, live: dict) -> bool:
    """True when the chart revised open, high, low, close, or colour."""
    if saved.get("color") != live.get("color") or saved.get("direction") != live.get("direction"):
        return True
    for key in ("open", "high", "low", "close"):
        try:
            if abs(float(saved.get(key) or 0) - float(live.get(key) or 0)) > 0.01:
                return True
        except (TypeError, ValueError):
            return True
    return False


def history_page(rows: list, limit: int, page: int) -> dict:
    size = limit if limit in LIMITS else 20
    total = len(rows or [])
    pages = max(1, (total + size - 1) // size) if total else 1
    current = min(max(1, int(page or 1)), pages)
    newest = list(reversed(rows or []))
    start = (current - 1) * size
    return {
        "rows": newest[start:start + size],
        "total": total,
        "limit": size,
        "page": current,
        "pages": pages,
    }


class TradeHistory:
    def __init__(self, path: Path, log_path: Optional[Path] = None, journal_path: Optional[Path] = None) -> None:
        self.path = Path(path)
        self.log_path = Path(log_path or "logs/bot.log")
        self.journal_path = Path(journal_path or "data/position_candles.json")
        self.rows: list = []
        self._load()

    def _load(self) -> None:
        loaded = self._read(self.path)
        log_newer = False
        if self.log_path.exists():
            if not loaded:
                log_newer = True
            else:
                try:
                    log_newer = self.log_path.stat().st_mtime > self.path.stat().st_mtime
                except OSError:
                    log_newer = False
        if loaded and not log_newer:
            self.rows = loaded[-MAX_ROWS:]
            return
        text = ""
        if log_newer:
            try:
                text = self.log_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                text = ""
        by_id = {}
        order = []
        for row in parse_trade_log(text):
            by_id[row["id"]] = row
            order.append(row["id"])
        for row in loaded:
            if isinstance(row, dict) and row.get("id"):
                by_id[row["id"]] = row
                if row["id"] not in order:
                    order.append(row["id"])
        candles = self._candles_by_id()
        self.rows = []
        for hedge_id in order:
            row = by_id[hedge_id]
            if not row.get("candles"):
                saved = candles.get(hedge_id)
                if saved:
                    row["candles"] = saved
            self.rows.append(row)
        self.rows = self.rows[-MAX_ROWS:]
        if self.rows:
            self._write()

    def _candles_by_id(self) -> dict:
        found = {}
        for row in self._read(self.journal_path):
            if isinstance(row, dict) and row.get("id"):
                found[row["id"]] = row.get("candles") or {}
        return found

    @staticmethod
    def _read(path: Path) -> list:
        if not path.exists():
            return []
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        return loaded if isinstance(loaded, list) else []

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.rows[-MAX_ROWS:], indent=2), encoding="utf-8")

    def upsert(self, row: dict) -> None:
        hedge_id = row.get("id")
        if not hedge_id:
            return
        for index, old in enumerate(self.rows):
            if isinstance(old, dict) and old.get("id") == hedge_id:
                self.rows[index] = row
                break
        else:
            self.rows.append(row)
        self.rows = self.rows[-MAX_ROWS:]
        self._write()

    def update_candles(self, hedge_id: str, candles: dict) -> None:
        """Replace candles on a closed row once the last bar has arrived."""
        if not hedge_id or not candles:
            return
        for row in self.rows:
            if isinstance(row, dict) and row.get("id") == hedge_id:
                row["candles"] = candles
                self._write()
                return

    def refresh_cached_candles(self, cache: dict) -> None:
        """Keep saved bars on the same print the live chart is showing.

        A 5-minute close can move after the window ends and flip a small body.
        Bars from later windows are not added.
        """
        fresh = {
            "predict": list((cache or {}).get("predict") or []),
            "polymarket": list((cache or {}).get("polymarket") or []),
        }
        changed = False
        for row in self.rows:
            if not isinstance(row, dict):
                continue
            try:
                opened = float(row.get("opened_at") or 0)
            except (TypeError, ValueError):
                continue
            if opened <= 0:
                continue
            cutoff = int(opened // 300) * 300
            saved = row.get("candles") or {}
            updated = {}
            row_changed = False
            for key in ("predict", "polymarket"):
                by_start = {}
                for candle in fresh[key]:
                    if not isinstance(candle, dict):
                        continue
                    try:
                        by_start[int(float(candle.get("start") or 0))] = candle
                    except (TypeError, ValueError):
                        continue
                merged = []
                seen = set()
                for candle in saved.get(key) or []:
                    if not isinstance(candle, dict):
                        continue
                    try:
                        start = int(float(candle.get("start") or 0))
                    except (TypeError, ValueError):
                        row_changed = True
                        continue
                    if start <= 0 or start >= cutoff or start in seen:
                        row_changed = True
                        continue
                    seen.add(start)
                    live = by_start.get(start)
                    if live is not None and candle_print_changed(candle, live):
                        merged.append(live)
                        row_changed = True
                    else:
                        merged.append(candle)
                newest = max(seen) if seen else 0
                extra = [
                    candle for start, candle in sorted(by_start.items())
                    if newest < start < cutoff
                ]
                if extra:
                    merged.extend(extra)
                    row_changed = True
                updated[key] = merged[-10:]
            if row_changed:
                row["candles"] = updated
                changed = True
        if changed:
            self._write()

    def page(self, limit: int, page: int) -> dict:
        return history_page(self.rows, limit, page)

    def realized(self) -> float:
        return round(sum(float(row.get("pnl") or 0) for row in self.rows), 4)

    def rev(self) -> str:
        last = self.rows[-1] if self.rows else {}
        return f"{len(self.rows)}:{last.get('id', '')}:{last.get('pnl', 0)}"


def row_from_hedge(hedge) -> dict:
    """Dashboard row for a hedge that just closed."""

    def leg(item) -> Optional[dict]:
        if item is None:
            return None
        shares = float(getattr(item, "filled_shares", 0) or getattr(item, "shares", 0) or 0)
        entry = float(getattr(item, "entry_price", 0) or 0)
        exit_price = float(getattr(item, "exit_price", 0) or 0)
        entry_usd = float(getattr(item, "entry_usd", 0) or 0)
        if entry_usd <= 0 and shares > 0 and entry:
            entry_usd = round(shares * entry, 4)
        exit_usd = float(getattr(item, "exit_usd", 0) or 0)
        if exit_usd <= 0 and shares > 0:
            exit_usd = round(shares * exit_price, 4)
        return {
            "venue": getattr(item, "venue", ""),
            "direction": getattr(item, "direction", "") or "",
            "status": getattr(item, "status", "") or "CLOSED",
            "shares": shares,
            "filled_shares": shares,
            "requested_shares": float(getattr(item, "requested_shares", 0) or 0),
            "entry_price": entry,
            "exit_price": exit_price,
            "entry": entry,
            "exit": exit_price,
            "entry_usd": entry_usd,
            "exit_usd": exit_usd,
            "pnl": round(exit_usd - entry_usd, 4) if shares > 0 else 0.0,
            "reject_reason": getattr(item, "reject_reason", "") or "",
        }

    pf = leg(hedge.pf_leg)
    pm = leg(hedge.pm_leg)
    return {
        "id": hedge.id,
        "status": "CLOSED",
        "close_reason": getattr(hedge, "close_reason", "") or "",
        "entry_sum": float(getattr(hedge, "entry_sum", 0) or 0),
        "exit_sum": float(getattr(hedge, "exit_sum", 0) or 0),
        "pnl": float(getattr(hedge, "pnl", 0) or 0),
        "opened_at": float(getattr(hedge, "opened_at", 0) or 0),
        "market_end": float(getattr(hedge, "market_end", 0) or 0),
        "venues": getattr(hedge, "venues", "both") or "both",
        "strategy_mode": getattr(hedge, "strategy_mode", "BASE") or "BASE",
        "asset": "btc",
        "tf": "5m",
        "candles": getattr(hedge, "candles", None) or {},
        "pf": pf,
        "bn": pf,
        "pm": pm,
        "pf_hedge": leg(getattr(hedge, "pf_hedge_leg", None)),
        "pm_hedge": leg(getattr(hedge, "pm_hedge_leg", None)),
    }
