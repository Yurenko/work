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


def fill_from_pm_order(resp: Any, requested: float = 0.0) -> Fill:
    """Parse Polymarket CLOB post_order response.

    Matched example:
      takingAmount = shares received (BUY)
      makingAmount = USDC spent
      avg = making / taking
    """
    if not isinstance(resp, dict):
        return Fill(status="REJECTED", reason="empty response")
    status = str(resp.get("status") or "").upper()
    oid = str(resp.get("orderID") or resp.get("orderId") or resp.get("id") or "")
    reason = str(resp.get("reason") or resp.get("error") or resp.get("errorMsg") or "")

    taking = _num(resp.get("takingAmount") or resp.get("size_matched") or resp.get("filledShares") or resp.get("filled"))
    making = _num(resp.get("makingAmount") or resp.get("usd"))
    limit_px = _num(resp.get("avgPrice") or resp.get("price")) or 0.0

    shares = taking or 0.0
    price = limit_px
    usd = 0.0
    if taking and taking > 0 and making and making > 0:
        # Real average fill
        price = making / taking
        shares = taking
        usd = making
    elif shares > 0 and price > 0:
        usd = shares * price

    if status in ("FILLED", "MATCHED"):
        if shares <= 0:
            shares = requested
            if price > 0:
                usd = shares * price
        return Fill(
            status="FILLED",
            shares=round(shares, 4),
            price=round(price, 4),
            usd=round(usd or shares * price, 4),
            order_id=oid,
        )
    if status in ("REJECTED", "FAILED") or (reason and "couldn't be fully filled" in reason.lower()):
        return Fill(status="REJECTED", reason=reason or status, order_id=oid)
    if status in ("LIVE", "OPEN", "SUBMITTED"):
        return Fill(status="SUBMITTED", shares=shares, price=price, order_id=oid, reason=reason)
    if reason:
        return Fill(status="REJECTED", reason=reason, order_id=oid)
    return Fill(status="SUBMITTED", shares=shares, price=price, order_id=oid)


fill_from_polymarket = fill_from_pm_order
