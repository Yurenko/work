"""BTC open / last / close for one 5-minute window, per platform.

Candle direction uses Open and Last only. The platform's final close is stored
separately and never decides the candle colour. Predict.fun and Polymarket
keep their own windows: they can settle at different times and their official
open prices are different fields.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


def candle_direction(open_price: Optional[float], last_price: Optional[float]) -> str:
    """UP when last is above open, DOWN when below, NEUTRAL when equal."""
    if open_price is None or last_price is None:
        return "UNKNOWN"
    if abs(float(open_price) - float(last_price)) <= 1e-6:
        return "NEUTRAL"
    return "UP" if float(last_price) > float(open_price) else "DOWN"


def candle_color(direction: str) -> str:
    return {"UP": "GREEN", "DOWN": "RED", "NEUTRAL": "GRAY"}.get(direction, "UNKNOWN")


def completed_window_starts(now: float, count: int = 10) -> list[int]:
    """Start times of the last completed 5-minute windows, oldest first."""
    live = int(float(now) // 300) * 300
    count = max(1, int(count))
    return [live - 300 * (count - index) for index in range(count)]


def candle_row(
    start: float,
    open_price: Optional[float],
    close_price: Optional[float],
    high_price: Optional[float] = None,
    low_price: Optional[float] = None,
) -> Optional[dict]:
    """One finished 5-minute candle. Colour is close versus open, as on the chart."""
    if open_price is None or close_price is None:
        return None
    if float(open_price) <= 0 or float(close_price) <= 0:
        return None
    direction = candle_direction(open_price, close_price)
    if direction not in ("UP", "DOWN", "NEUTRAL"):
        return None
    start = float(start)
    open_price = float(open_price)
    close_price = float(close_price)
    high = max(open_price, close_price) if high_price is None else float(high_price)
    low = min(open_price, close_price) if low_price is None else float(low_price)
    high = max(high, open_price, close_price)
    low = min(low, open_price, close_price)
    return {
        "start": start,
        "end": start + 300.0,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close_price,
        "direction": direction,
        "color": candle_color(direction),
    }


def chart_candle_rows(candles: list, now: float, count: int = 10) -> list:
    """Last completed chart candles, oldest first.

    The window that is still forming is left out. Its candle on the exchange
    chart keeps moving until the window ends.
    """
    live = int(float(now) // 300) * 300
    rows = []
    seen = set()
    for candle in candles or []:
        if not isinstance(candle, dict):
            continue
        try:
            start = int(candle.get("time") or 0)
        except (TypeError, ValueError):
            continue
        if start > 10_000_000_000:
            start //= 1000
        if start <= 0 or start >= live or start in seen:
            continue
        row = candle_row(
            start,
            candle.get("open"),
            candle.get("close"),
            candle.get("high"),
            candle.get("low"),
        )
        if row is None:
            continue
        seen.add(start)
        rows.append(row)
    rows.sort(key=lambda item: item["start"])
    return rows[-max(1, int(count)):]


def newest_chart_start(candles: list) -> int:
    """Latest bucket start in a raw chart payload. Milliseconds are reduced."""
    newest = 0
    for candle in candles or []:
        if not isinstance(candle, dict):
            continue
        try:
            start = int(candle.get("time") or candle.get("start") or 0)
        except (TypeError, ValueError):
            continue
        if start > 10_000_000_000:
            start //= 1000
        if start > newest:
            newest = start
    return newest


def chart_candles_are_current(candles: list, now: float) -> bool:
    """True when the payload still reaches the live window or the one just closed.

    A cached reply can be a few hours old. That slice is a different shape
    from the candles on the exchange, so it must not replace the chart.
    """
    live = int(float(now) // 300) * 300
    return newest_chart_start(candles) >= live - 300


def shared_candle_direction(predict: str, polymarket: str) -> str:
    """One recovery direction for the cycle. Never pick a side at random.

    Both platforms must agree when both have a direction. A missing side does
    not outvote a known side. Disagreement, or no known side, is not tradable.
    """
    known = [d for d in (predict, polymarket) if d in ("UP", "DOWN")]
    if not known:
        if "NEUTRAL" in (predict, polymarket):
            return "NEUTRAL"
        return "UNKNOWN"
    if len(set(known)) > 1:
        return "UNKNOWN"
    return known[0]


def loss_recovery_direction(predict: str, polymarket: str) -> str:
    """Side for a confirmed combined loss.

    Agreement still wins, and a missing side does not outvote a known one.
    Predict's official open often sits on the other side of the same
    Chainlink last, so a raw disagreement is not a reason to open the next
    market as a base hedge. Polymarket's open against that last is the
    candle. No known side stays unknown.
    """
    shared = shared_candle_direction(predict, polymarket)
    if shared in ("UP", "DOWN", "NEUTRAL"):
        return shared
    if polymarket in ("UP", "DOWN"):
        return polymarket
    if predict in ("UP", "DOWN"):
        return predict
    return "UNKNOWN"


def _secs(ts: float) -> float:
    value = float(ts)
    if value > 1e12:
        value /= 1000.0
    return value


@dataclass
class PricePoint:
    price: float
    ts: float
    source: str

    def to_json(self) -> dict[str, Any]:
        return {"price": self.price, "ts": self.ts, "source": self.source}

    @classmethod
    def from_json(cls, raw: Any) -> Optional["PricePoint"]:
        if not isinstance(raw, dict) or raw.get("price") is None:
            return None
        try:
            return cls(float(raw["price"]), float(raw.get("ts") or 0), str(raw.get("source") or ""))
        except (TypeError, ValueError):
            return None


@dataclass
class MarketCandle:
    platform: str
    market_id: str
    start_ts: float
    end_ts: float
    status: str = "LIVE"  # LIVE | SETTLING | RESOLVED
    open: Optional[PricePoint] = None
    last: Optional[PricePoint] = None
    close: Optional[PricePoint] = None
    open_official: bool = False
    settlement: str = "PENDING"  # PENDING | CONFIRMED

    def direction(self, max_age_sec: float, now: Optional[float] = None) -> str:
        if self.open is None:
            return "UNKNOWN"
        if self.last is not None and self.last_is_fresh(max_age_sec, now=now):
            return candle_direction(self.open.price, self.last.price)
        # A finished window with no fresh Last still has an official close.
        # That close fills the gap. It does not replace a fresh Last.
        moment = time.time() if now is None else float(now)
        if self.close is not None and moment >= self.end_ts:
            return candle_direction(self.open.price, self.close.price)
        if self.last is None:
            return "UNKNOWN"
        return "STALE"

    def color(self, max_age_sec: float, now: Optional[float] = None) -> str:
        return candle_color(self.direction(max_age_sec, now=now))

    def last_is_fresh(self, max_age_sec: float, now: Optional[float] = None) -> bool:
        """Last must sit inside the window and, once it has ended, near the end."""
        if self.last is None:
            return False
        if self.last.ts < self.start_ts or self.last.ts >= self.end_ts:
            return False
        moment = self.end_ts if now is None else float(now)
        if moment >= self.end_ts and (self.end_ts - self.last.ts) > float(max_age_sec):
            return False
        if moment < self.end_ts and (moment - self.last.ts) > float(max_age_sec):
            return False
        return True

    def refresh_status(self, now: float) -> None:
        if self.close is not None or self.settlement == "CONFIRMED":
            self.status = "RESOLVED"
        elif now >= self.end_ts:
            self.status = "SETTLING"
        else:
            self.status = "LIVE"

    def to_json(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "market_id": self.market_id,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "status": self.status,
            "open": self.open.to_json() if self.open else None,
            "last": self.last.to_json() if self.last else None,
            "close": self.close.to_json() if self.close else None,
            "open_official": self.open_official,
            "settlement": self.settlement,
        }

    @classmethod
    def from_json(cls, raw: dict) -> "MarketCandle":
        candle = cls(
            platform=str(raw.get("platform") or ""),
            market_id=str(raw.get("market_id") or ""),
            start_ts=float(raw.get("start_ts") or 0),
            end_ts=float(raw.get("end_ts") or 0),
            status=str(raw.get("status") or "LIVE"),
            open=PricePoint.from_json(raw.get("open")),
            last=PricePoint.from_json(raw.get("last")),
            close=PricePoint.from_json(raw.get("close")),
            open_official=bool(raw.get("open_official")),
            settlement=str(raw.get("settlement") or "PENDING"),
        )
        return candle


def parse_chainlink_frame(raw: Any) -> list[tuple[float, float, str]]:
    """Ticks from Polymarket's public Chainlink BTC/USD socket.

    Snapshot: {"payload": {"data": [{"timestamp": ms, "value": price}, ...]}}
    Update:   {"topic": "crypto_prices_chainlink", "type": "update",
               "payload": {"symbol": "btc/usd", "timestamp": ms, "value": price,
                           "full_accuracy_value": "..."}}
    An empty frame is the subscribe ack and carries no price.
    """
    source = "polymarket.ws.crypto_prices_chainlink"
    if raw is None:
        return []
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if not isinstance(raw, str):
        return []
    text = raw.strip()
    if not text:
        return []
    try:
        msg = json.loads(text)
    except json.JSONDecodeError:
        return []
    payload = msg.get("payload") if isinstance(msg, dict) else None
    if not isinstance(payload, dict):
        return []
    symbol = str(payload.get("symbol") or "")
    if symbol and symbol.lower() not in ("btc/usd", "btc-usd", "btcusd"):
        return []
    out: list[tuple[float, float, str]] = []
    rows = payload.get("data")
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict) or row.get("value") is None:
                continue
            try:
                out.append((float(row["value"]), _secs(row.get("timestamp") or 0), source))
            except (TypeError, ValueError):
                continue
        return out
    if payload.get("value") is None:
        return []
    try:
        out.append((float(payload["value"]), _secs(payload.get("timestamp") or msg.get("timestamp") or 0), source))
    except (TypeError, ValueError):
        return []
    return out


def parse_polymarket_crypto_price(body: Any) -> dict[str, Any]:
    """Fields actually returned by /api/crypto/crypto-price?variant=fiveminute.

    openPrice is the window open (price to beat). closePrice is null while
    incomplete, and the official close once completed. timestamp is when this
    payload was produced, in milliseconds.
    """
    result = {
        "open": None,
        "close": None,
        "completed": False,
        "observed_ts": None,
    }
    if not isinstance(body, dict):
        return result
    if body.get("openPrice") is not None:
        try:
            result["open"] = float(body["openPrice"])
        except (TypeError, ValueError):
            pass
    if body.get("closePrice") is not None:
        try:
            result["close"] = float(body["closePrice"])
        except (TypeError, ValueError):
            pass
    result["completed"] = bool(body.get("completed")) and not bool(body.get("incomplete"))
    if body.get("timestamp") is not None:
        try:
            result["observed_ts"] = _secs(body["timestamp"])
        except (TypeError, ValueError):
            pass
    return result


@dataclass
class BtcCandleBook:
    """Current and previous window per platform, plus the Chainlink tape."""

    max_age_sec: float = 20.0
    path: Optional[Path] = None
    markets: dict[tuple[str, str], MarketCandle] = field(default_factory=dict)
    current: dict[str, str] = field(default_factory=dict)
    previous: dict[str, str] = field(default_factory=dict)
    ticks: list[PricePoint] = field(default_factory=list)
    _last_save: float = 0.0

    def __post_init__(self) -> None:
        self.load()

    def add_tick(self, price: float, ts: float, source: str) -> None:
        if price <= 0 or ts <= 0:
            return
        point = PricePoint(float(price), _secs(ts), source)
        self.ticks.append(point)
        if len(self.ticks) > 180:
            self.ticks = self.ticks[-120:]
        for candle in self.markets.values():
            self._apply_tick(candle, point)
        self._maybe_save()

    def upsert_market(
        self,
        platform: str,
        market_id: str,
        start_ts: float,
        end_ts: float,
        now: Optional[float] = None,
    ) -> bool:
        """Track a window. Returns True when this platform rolled to a new id."""
        now = time.time() if now is None else float(now)
        market_id = str(market_id)
        start_ts = float(start_ts)
        end_ts = float(end_ts)
        key = (platform, market_id)
        rolled = False
        candle = self.markets.get(key)
        current_id = self.current.get(platform)
        current = self.markets.get((platform, current_id)) if current_id else None
        if candle is None and current is not None and start_ts + 1 < current.start_ts:
            candle = MarketCandle(platform, market_id, start_ts, end_ts)
            self.markets[key] = candle
            if not self.previous.get(platform):
                self.previous[platform] = market_id
        elif candle is None:
            candle = MarketCandle(platform, market_id, start_ts, end_ts)
            self.markets[key] = candle
            if current_id and current_id != market_id:
                self.previous[platform] = current_id
                rolled = True
            self.current[platform] = market_id
        else:
            candle.start_ts = start_ts
            candle.end_ts = end_ts
        for point in self.ticks:
            self._apply_tick(candle, point)
        self._refresh_all(now)
        self._trim()
        if rolled:
            self.save()
        return rolled

    def set_official_open(
        self, platform: str, market_id: str, price: Optional[float], ts: float, source: str,
    ) -> None:
        candle = self.markets.get((platform, str(market_id)))
        if candle is None or price is None or price <= 0:
            return
        if candle.open_official and candle.open is not None:
            return
        candle.open = PricePoint(float(price), float(ts or candle.start_ts), source)
        candle.open_official = True
        self.save()

    def set_official_close(
        self, platform: str, market_id: str, price: Optional[float], ts: float, source: str,
    ) -> None:
        """Final platform price. Does not replace Last and does not recolour."""
        candle = self.markets.get((platform, str(market_id)))
        if candle is None or price is None or price <= 0:
            return
        price = float(price)
        if candle.close is not None and abs(candle.close.price - price) <= 1e-9:
            return
        candle.close = PricePoint(price, float(ts or candle.end_ts), source)
        candle.settlement = "CONFIRMED"
        candle.refresh_status(time.time())
        self.save()

    def apply_polymarket_crypto(self, market_id: str, body: Any, now: Optional[float] = None) -> None:
        parsed = parse_polymarket_crypto_price(body)
        now = time.time() if now is None else float(now)
        if parsed["open"] is not None:
            candle = self.markets.get(("polymarket", str(market_id)))
            start = candle.start_ts if candle else now
            self.set_official_open(
                "polymarket", market_id, parsed["open"], start,
                "polymarket.crypto-price.openPrice",
            )
        if parsed["close"] is not None and parsed["completed"]:
            self.set_official_close(
                "polymarket", market_id, parsed["close"],
                float(parsed["observed_ts"] or now),
                "polymarket.crypto-price.closePrice",
            )
        self._refresh_all(now)

    def apply_predict_window(self, row: dict, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else float(now)
        market_id = str(row.get("market_id") or "")
        start = float(row.get("start") or 0)
        end = float(row.get("end") or 0)
        if not market_id or start <= 0 or end <= start:
            return False
        rolled = self.upsert_market("predict", market_id, start, end, now)
        source = str(row.get("source") or "predict.variantDetails.upDown")
        if row.get("start_price") is not None:
            self.set_official_open(
                "predict", market_id, float(row["start_price"]), start, source + ".startPrice",
            )
        if row.get("end_price") is not None:
            self.set_official_close(
                "predict", market_id, float(row["end_price"]), end, source + ".endPrice",
            )
        self._refresh_all(now)
        return rolled

    def previous_candle(self, platform: str) -> Optional[MarketCandle]:
        market_id = self.previous.get(platform)
        if not market_id:
            return None
        return self.markets.get((platform, market_id))

    def current_candle(self, platform: str) -> Optional[MarketCandle]:
        market_id = self.current.get(platform)
        if not market_id:
            return None
        return self.markets.get((platform, market_id))

    def candle(self, platform: str, market_id: str) -> Optional[MarketCandle]:
        return self.markets.get((platform, str(market_id)))

    def direction_of(self, platform: str, market_id: str, now: Optional[float] = None) -> str:
        candle = self.candle(platform, market_id)
        if candle is None:
            return "UNKNOWN"
        return candle.direction(self.max_age_sec, now=now)

    def snapshot(self) -> dict[str, Any]:
        def row(candle: Optional[MarketCandle]) -> Optional[dict]:
            if candle is None:
                return None
            direction = candle.direction(self.max_age_sec)
            return {
                "market_id": candle.market_id,
                "status": candle.status,
                "settlement": candle.settlement,
                "open": candle.open.price if candle.open else None,
                "open_ts": candle.open.ts if candle.open else None,
                "open_source": candle.open.source if candle.open else "",
                "last": candle.last.price if candle.last else None,
                "last_ts": candle.last.ts if candle.last else None,
                "last_source": candle.last.source if candle.last else "",
                "close": candle.close.price if candle.close else None,
                "close_source": candle.close.source if candle.close else "",
                "direction": direction,
                "color": candle_color(direction),
            }

        return {
            "predict": row(self.current_candle("predict")),
            "polymarket": row(self.current_candle("polymarket")),
            "predict_previous": row(self.previous_candle("predict")),
            "polymarket_previous": row(self.previous_candle("polymarket")),
        }

    def load(self) -> None:
        if not self.path or not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        self.markets = {}
        for raw in data.get("markets") or []:
            if not isinstance(raw, dict):
                continue
            candle = MarketCandle.from_json(raw)
            if candle.platform and candle.market_id:
                self.markets[(candle.platform, candle.market_id)] = candle
        self.current = {str(k): str(v) for k, v in (data.get("current") or {}).items()}
        self.previous = {str(k): str(v) for k, v in (data.get("previous") or {}).items()}
        self.ticks = []
        for raw in data.get("ticks") or []:
            point = PricePoint.from_json(raw)
            if point is not None:
                self.ticks.append(point)

    def save(self) -> None:
        if not self.path:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "markets": [c.to_json() for c in self.markets.values()],
            "current": self.current,
            "previous": self.previous,
            "ticks": [p.to_json() for p in self.ticks[-120:]],
        }
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(self.path)
        self._last_save = time.time()

    def _apply_tick(self, candle: MarketCandle, point: PricePoint) -> None:
        if point.ts < candle.start_ts or point.ts >= candle.end_ts:
            return
        if candle.last is None or point.ts >= candle.last.ts:
            candle.last = point
        if candle.open is None and not candle.open_official and point.ts <= candle.start_ts + 3:
            candle.open = point

    def _refresh_all(self, now: float) -> None:
        for candle in self.markets.values():
            candle.refresh_status(now)

    def _trim(self) -> None:
        if len(self.markets) <= 24:
            return
        ranked = sorted(self.markets.values(), key=lambda c: c.start_ts)
        keep_ids = {(c.platform, c.market_id) for c in ranked[-16:]}
        for platform, market_id in list(self.current.items()):
            keep_ids.add((platform, market_id))
        for platform, market_id in list(self.previous.items()):
            keep_ids.add((platform, market_id))
        self.markets = {key: value for key, value in self.markets.items() if key in keep_ids}

    def _maybe_save(self) -> None:
        if not self.path:
            return
        if time.time() - self._last_save >= 5:
            self.save()


def format_new_market(
    platform: str,
    market_id: str,
    status: str,
    previous: Optional[MarketCandle],
    max_age_sec: float,
    now: Optional[float] = None,
) -> str:
    if previous is None:
        direction = "UNKNOWN"
        color = "UNKNOWN"
        open_px = "n/a"
        last_px = "n/a"
        prev_status = "n/a"
        settlement = "n/a"
    else:
        direction = previous.direction(max_age_sec, now=now)
        color = candle_color(direction)
        open_px = f"{previous.open.price:.4f}" if previous.open else "n/a"
        last_px = f"{previous.last.price:.4f}" if previous.last else "n/a"
        prev_status = previous.status
        settlement = "PENDING" if previous.settlement != "CONFIRMED" else "CONFIRMED"
    return (
        "[NEW MARKET]\n"
        f"Platform: {platform}\n"
        f"Market ID: {market_id}\n"
        f"Status: {status}\n"
        "[PREVIOUS BTC CANDLE]\n"
        f"Open: {open_px}\n"
        f"Last: {last_px}\n"
        f"Direction: {direction}\n"
        f"Color: {color}\n"
        "[PREVIOUS MARKET]\n"
        f"Status: {prev_status}\n"
        f"Settlement: {settlement}"
    )
