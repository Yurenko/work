"""BTC 5m Up/Down slug helpers — from polymarket project app/updown/markets.py."""

from __future__ import annotations

import json
from typing import Any, Optional, Tuple

WINDOWS = {"5m": 300, "15m": 900}


def window_bounds(now: float, tf: str = "5m") -> Tuple[int, int]:
    dur = WINDOWS[tf]
    start = int(now) // dur * dur
    return start, start + dur


def updown_slug(asset: str = "btc", tf: str = "5m", now: float | None = None) -> str:
    import time

    if now is None:
        now = time.time()
    start, _ = window_bounds(now, tf)
    return f"{asset.lower()}-updown-{tf}-{start}"


def _loads(raw: Any) -> list:
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, str):
        try:
            return list(json.loads(raw))
        except json.JSONDecodeError:
            return [part.strip().strip('"') for part in raw.strip("[]").split(",") if part.strip()]
    return []


def resolved_updown(event: dict) -> Optional[dict[str, float]]:
    """Return {up: 0|1, down: 0|1} when market is fully resolved, else None."""
    if not event:
        return None
    markets = event.get("markets") or []
    # Sometimes gamma returns the market object itself
    if not markets and (event.get("outcomePrices") or event.get("outcomes")):
        markets = [event]
    if not markets:
        return None
    market = markets[0] if isinstance(markets[0], dict) else {}
    status = str(
        market.get("umaResolutionStatus")
        or market.get("resolutionStatus")
        or event.get("umaResolutionStatus")
        or ""
    ).lower()
    closed = market.get("closed") is True or str(market.get("closed") or "").lower() == "true"
    closed = closed or event.get("closed") is True or str(event.get("closed") or "").lower() == "true"
    # Also treat automaticallyResolved + prices at extremes as resolved
    auto = market.get("automaticallyResolved") is True
    outcomes = [str(x).lower() for x in _loads(market.get("outcomes") or event.get("outcomes"))]
    raw_prices = _loads(market.get("outcomePrices") or event.get("outcomePrices"))
    if len(outcomes) < 2 or len(raw_prices) < 2:
        return None
    # Parse prices first — if already 0/1 we can trust even before closed flag flips
    out: dict[str, float] = {}
    ambiguous = False
    for name, raw in zip(outcomes, raw_prices):
        try:
            price = float(raw)
        except (TypeError, ValueError):
            return None
        if price >= 0.95:
            price = 1.0
        elif price <= 0.05:
            price = 0.0
        else:
            ambiguous = True
            continue
        if name in ("up", "yes"):
            out["up"] = price
        elif name in ("down", "no"):
            out["down"] = price
    if ambiguous or "up" not in out or "down" not in out:
        return None
    if abs(out["up"] + out["down"] - 1.0) > 0.05:
        return None
    # Require closed/resolved OR both legs extreme (0 and 1)
    if not (closed or status in {"resolved", "settled"} or auto):
        # prices already extreme — accept (gamma lag on closed flag)
        if not (out["up"] in (0.0, 1.0) and out["down"] in (0.0, 1.0)):
            return None
    return out



def parse_updown_market(event: dict) -> Optional[dict]:
    markets = event.get("markets") or []
    if not markets:
        return None
    market = markets[0]
    outcomes = [str(x) for x in _loads(market.get("outcomes"))]
    token_ids = [
        str(x) for x in _loads(market.get("clobTokenIds") or market.get("clob_token_ids"))
    ]
    if len(outcomes) < 2 or len(token_ids) < 2:
        return None
    up_i = down_i = None
    for i, name in enumerate(outcomes):
        n = name.lower()
        if n == "up":
            up_i = i
        elif n == "down":
            down_i = i
    if up_i is None or down_i is None:
        up_i, down_i = 0, 1
    end_raw = (
        market.get("endDate")
        or market.get("endDateIso")
        or event.get("endDate")
        or event.get("end_date")
        or market.get("closedTime")
    )
    end_ts = None
    if end_raw is not None:
        try:
            end_ts = float(end_raw)
            if end_ts > 1e12:
                end_ts /= 1000.0
        except (TypeError, ValueError):
            if isinstance(end_raw, str) and "T" in end_raw:
                try:
                    from datetime import datetime
                    end_ts = datetime.fromisoformat(end_raw.replace("Z", "+00:00")).timestamp()
                except Exception:
                    end_ts = None
    # Slug btc-updown-5m-{startUnix} → end = start + 300
    slug = str(event.get("slug") or market.get("slug") or "")
    if end_ts is None and "updown-5m-" in slug:
        try:
            start_u = int(slug.rsplit("-", 1)[-1])
            if start_u > 1e9:
                end_ts = float(start_u + 300)
        except ValueError:
            pass
    up_t = token_ids[up_i]
    down_t = token_ids[down_i]
    return {
        "condition_id": str(market.get("conditionId") or market.get("condition_id") or ""),
        "question": str(market.get("question") or event.get("title") or ""),
        "slug": str(event.get("slug") or market.get("slug") or ""),
        "up_token": up_t,
        "down_token": down_t,
        "up_token_id": up_t,
        "down_token_id": down_t,
        "market_id": str(market.get("id") or ""),
        "end": end_ts,
        "outcomes": outcomes,
    }
