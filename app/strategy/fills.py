"""Turn venue order responses into a fill, a reject, or still-working."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


def _num(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass
class Fill:
    status: str  # FILLED | REJECTED | SUBMITTED
    shares: float = 0.0
    price: float = 0.0
    usd: float = 0.0
    order_id: str = ""
    order_hash: str = ""
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "FILLED" and self.shares > 0


def fill_from_predict(resp: Any, requested: float = 0.0) -> Fill:
    if not isinstance(resp, dict):
        return Fill(status="REJECTED", reason="empty response")
    if "orderId" not in resp and isinstance(resp.get("data"), dict):
        resp = {**resp, **resp["data"]}
    status = str(resp.get("status") or resp.get("state") or "").upper()
    code = str(resp.get("code") or "").upper()
    shares = _num(resp.get("filledShares") or resp.get("filled") or resp.get("quantityFilled") or resp.get("amountFilled")) or 0.0
    # amountFilled may be wei
    if shares > requested * 10 and requested > 0:
        shares = shares / 1e18
    price = _num(resp.get("avgPrice") or resp.get("price") or resp.get("entry_price")) or 0.0
    oid = str(resp.get("orderId") or resp.get("orderID") or resp.get("id") or "")
    ohash = str(resp.get("orderHash") or resp.get("hash") or "")
    reason = str(resp.get("reason") or resp.get("error") or resp.get("message") or "")
    if status in ("FILLED", "MATCHED", "COMPLETE", "COMPLETED"):
        if shares <= 0:
            shares = requested
        return Fill(
            status="FILLED",
            shares=round(shares, 4),
            price=price,
            usd=round(shares * (price or 0), 4),
            order_id=oid,
            order_hash=ohash,
        )
    if status in ("REJECTED", "FAILED", "CANCELLED", "CANCELED"):
        return Fill(status="REJECTED", reason=reason or status, order_id=oid, order_hash=ohash)
    if (oid or ohash) and (code in ("OK", "SUCCESS", "") or status in ("", "OPEN", "LIVE", "SUBMITTED", "ACCEPTED")):
        return Fill(status="SUBMITTED", shares=shares, price=price, order_id=oid, order_hash=ohash, reason=reason)
    if not oid and not ohash and reason:
        return Fill(status="REJECTED", reason=reason)
    return Fill(status="SUBMITTED", shares=shares, price=price, order_id=oid, order_hash=ohash, reason=reason)


fill_from_binance_place = fill_from_predict
fill_from_binance_row = fill_from_predict


def fill_from_pm_order(resp: Any, requested: float = 0.0, side: str = "BUY") -> Fill:
    """Parse Polymarket CLOB post_order response.

    BUY  matched: takingAmount=shares received, makingAmount=USDC spent
                  price = making/taking, shares=taking, usd=making
    SELL matched: makingAmount=shares sold,     takingAmount=USDC received
                  price = taking/making, shares=making, usd=taking
    """
    if not isinstance(resp, dict):
        return Fill(status="REJECTED", reason="empty response")
    status = str(resp.get("status") or "").upper()
    oid = str(resp.get("orderID") or resp.get("orderId") or resp.get("id") or "")
    reason = str(resp.get("reason") or resp.get("error") or resp.get("errorMsg") or "")
    side_u = (side or "BUY").upper()

    taking = _num(resp.get("takingAmount") or resp.get("size_matched") or resp.get("filledShares") or resp.get("filled"))
    making = _num(resp.get("makingAmount") or resp.get("usd"))
    limit_px = _num(resp.get("avgPrice") or resp.get("price")) or 0.0

    shares = 0.0
    price = limit_px
    usd = 0.0
    if taking and taking > 0 and making and making > 0:
        if side_u == "SELL":
            # maker gives shares, taker gives USDC
            shares = making
            usd = taking
            price = taking / making if making else 0.0
        else:
            shares = taking
            # CLOB makingAmount is pre-fee; wallet usually pays more (taker fee).
            try:
                from app.core.config import get_settings
                fee_rate = float(getattr(get_settings(), "pm_taker_fee_rate", 0.0) or 0.0)
            except Exception:
                fee_rate = 0.04
            fee_rate = max(0.0, min(0.15, fee_rate))
            usd = making * (1.0 + fee_rate)
            price = making / taking if taking else 0.0  # mid/fill price stays pre-fee
        # Safety: outcome prices live in (0, 1.5]; if inverted, swap interpretation
        if price > 1.5 and side_u == "BUY":
            # probably was a sell-shaped response
            shares, usd = making, taking
            price = taking / making if making else price
        if price > 1.5 and side_u == "SELL":
            shares, usd = taking, making
            price = making / taking if taking else price
    elif taking and taking > 0:
        shares = taking
        if price > 0:
            usd = shares * price
    elif making and making > 0 and side_u == "SELL":
        shares = making

    # Clamp display price into sane band for binary markets
    if price > 1.5:
        price = min(price, 1.5)

    # original_size / remaining help detect partial MATCHED still resting
    original = _num(
        resp.get("original_size")
        or resp.get("originalSize")
        or resp.get("size")
        or resp.get("makerAmount")
        or 0
    )
    # Some APIs put requested shares in "size" and matched in size_matched
    size_matched = _num(resp.get("size_matched") or resp.get("sizeMatched") or 0)
    if size_matched > shares:
        shares = size_matched

    if status in ("FILLED", "MATCHED"):
        if shares <= 0:
            shares = requested
            if price > 0:
                usd = shares * price
        # If we requested a larger GTC (min-notional bump) and only part matched,
        # treat as SUBMITTED so the engine keeps polling / can cancel remainder.
        # Only mark FILLED when matched ≥ 90% of requested OR order is fully done.
        if requested > 0 and shares > 0 and shares < requested * 0.90:
            return Fill(
                status="SUBMITTED",
                shares=round(shares, 4),
                price=round(price, 4) if price else 0.0,
                usd=round(usd or shares * (price or 0), 4),
                order_id=oid,
                reason=f"partial {shares:.4f}/{requested:.4f}",
            )
        return Fill(
            status="FILLED",
            shares=round(shares, 4),
            price=round(price, 4),
            usd=round(usd or shares * price, 4),
            order_id=oid,
        )
    if status in ("REJECTED", "FAILED", "CANCELLED", "CANCELED") or (
        reason and "couldn't be fully filled" in reason.lower()
    ):
        # Cancelled order may still have partial fills — report them as FILLED if any
        if shares >= 0.5:
            return Fill(
                status="FILLED",
                shares=round(shares, 4),
                price=round(price, 4) if price else 0.0,
                usd=round(usd or shares * (price or 0), 4),
                order_id=oid,
                reason="partial after cancel",
            )
        return Fill(status="REJECTED", reason=reason or status, order_id=oid)
    if status in ("LIVE", "OPEN", "SUBMITTED"):
        return Fill(
            status="SUBMITTED",
            shares=round(shares, 4) if shares else 0.0,
            price=round(price, 4) if price else 0.0,
            usd=round(usd or (shares * price if shares and price else 0), 4),
            order_id=oid,
            reason=reason,
        )
    if reason:
        return Fill(status="REJECTED", reason=reason, order_id=oid)
    return Fill(status="SUBMITTED", shares=shares, price=price, order_id=oid)


fill_from_polymarket = fill_from_pm_order
