#!/usr/bin/env python3
"""
One-time: approve USDT + outcome tokens for Predict.fun exchanges.

IMPORTANT: If you deposit via the predict.fun website, funds live on a
*Predict Account* (smart wallet). You must set PREDICT_ACCOUNT in .env to that
deposit address. PREDICT_PRIVATE_KEY is the Privy-exported key that *signs*.

Usage:
  python scripts/set_approvals.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")


def main() -> int:
    pk = (os.getenv("PREDICT_PRIVATE_KEY") or "").strip()
    account = (os.getenv("PREDICT_ACCOUNT") or "").strip()
    if not pk:
        print("ERROR: PREDICT_PRIVATE_KEY missing in .env")
        return 1
    if not pk.startswith("0x"):
        pk = "0x" + pk

    try:
        from eth_account import Account
        from predict_sdk import OrderBuilder, ChainId
    except ImportError as e:
        print("ERROR: pip install predict-sdk eth-account")
        print(e)
        return 1

    eoa = Account.from_key(pk).address
    print(f"Privy / signer EOA: {eoa}")
    if account:
        print(f"Predict Account:    {account}")
    else:
        print("WARNING: PREDICT_ACCOUNT not set.")
        print("  If Portfolio $ is on the website, open Settings → copy deposit address")
        print("  and put it in .env as PREDICT_ACCOUNT=0x...")
        print("  Continuing as pure EOA (will fail if funds are on smart wallet).")

    print("Setting approvals (on-chain txs, needs a little BNB on the signer)...")

    try:
        try:
            from predict_sdk import OrderBuilderOptions
            if account:
                builder = OrderBuilder.make(
                    ChainId.BNB_MAINNET, pk, OrderBuilderOptions(predict_account=account)
                )
            else:
                builder = OrderBuilder.make(ChainId.BNB_MAINNET, pk)
        except Exception:
            # fallbacks for older/newer enum names
            chain = getattr(ChainId, "BNB_MAINNET", None) or getattr(ChainId, "BnbMainnet")
            if account:
                try:
                    builder = OrderBuilder.make(chain, pk, predict_account=account)
                except TypeError:
                    builder = OrderBuilder.make(chain, pk)
            else:
                builder = OrderBuilder.make(chain, pk)

        if hasattr(builder, "set_approvals"):
            result = builder.set_approvals()
        elif hasattr(builder, "setApprovals"):
            result = builder.setApprovals()
        else:
            print("ERROR: no set_approvals on OrderBuilder")
            return 1

        success = getattr(result, "success", None)
        if success is None and isinstance(result, dict):
            success = result.get("success")
        print("success:", success)
        print("result:", result)
        if success:
            print("\nOK — restart bot with MODE=LIVE")
            return 0
        print("\nFailed — check BNB for gas on signer, and PREDICT_ACCOUNT")
        return 2
    except Exception as e:
        print("ERROR:", type(e).__name__, e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
