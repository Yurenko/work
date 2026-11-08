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
    reason: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "FILLED" and self.shares > 0


def fill_from_predict(resp: Any, requested: float = 0.0) -> Fill:
    if not isinstance(resp, dict):
        return Fill(status="REJECTED", reason="empty response")
    # Unwrap nested data if present
    if "orderId" not in resp and isinstance(resp.get("data"), dict):
        resp = {**resp, **resp["data"]}
    status = str(resp.get("status") or resp.get("state") or "").upper()
    code = str(resp.get("code") or "").upper()
    shares = _num(resp.get("filledShares") or resp.get("filled") or resp.get("quantityFilled") or resp.get("amountFilled")) or 0.0
    price = _num(resp.get("avgPrice") or resp.get("price") or resp.get("entry_price")) or 0.0
    oid = str(resp.get("orderId") or resp.get("orderID") or resp.get("id") or resp.get("orderHash") or "")
    reason = str(resp.get("reason") or resp.get("error") or resp.get("message") or "")
    if status in ("FILLED", "MATCHED", "COMPLETE", "COMPLETED"):
        if shares <= 0:
            shares = requested
        return Fill(status="FILLED", shares=shares, price=price, usd=round(shares * (price or 0), 4), order_id=oid)
    if status in ("REJECTED", "FAILED", "CANCELLED", "CANCELED"):
        return Fill(status="REJECTED", reason=reason or status, order_id=oid)
    # POST /v1/orders 201 → {code, orderId, orderHash} with no fill yet
    if oid and (code in ("OK", "SUCCESS", "") or status in ("", "OPEN", "LIVE", "SUBMITTED", "ACCEPTED")):
        return Fill(status="SUBMITTED", shares=shares, price=price, order_id=oid, reason=reason)
    if not oid and reason:
        return Fill(status="REJECTED", reason=reason)
    return Fill(status="SUBMITTED", shares=shares, price=price, order_id=oid, reason=reason)


# Backwards-compatible aliases used by older tests / engine names
fill_from_binance_place = fill_from_predict
fill_from_binance_row = fill_from_predict


def fill_from_pm_order(resp: Any, requested: float = 0.0) -> Fill:
    if not isinstance(resp, dict):
        return Fill(status="REJECTED", reason="empty response")
    status = str(resp.get("status") or "").upper()
    shares = _num(resp.get("filledShares") or resp.get("size_matched") or resp.get("filled")) or 0.0
    price = _num(resp.get("avgPrice") or resp.get("price")) or 0.0
    oid = str(resp.get("orderID") or resp.get("orderId") or resp.get("id") or "")
    reason = str(resp.get("reason") or resp.get("error") or "")
    if status in ("FILLED", "MATCHED"):
        if shares <= 0:
            shares = requested
        return Fill(status="FILLED", shares=shares, price=price, usd=round(shares * price, 4), order_id=oid)
    if status in ("REJECTED", "FAILED"):
        return Fill(status="REJECTED", reason=reason or status, order_id=oid)
    return Fill(status="SUBMITTED", shares=shares, price=price, order_id=oid, reason=reason)


fill_from_polymarket = fill_from_pm_order
