#!/usr/bin/env python3
"""Quick balance check for Predict wallet (BNB + USDT on BNB Chain)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

# BNB Chain USDT (common)
USDT = "0x55d398326f99059fF775485246999027B3197955"
RPC = os.getenv("BNB_RPC", "https://bsc-dataseed.binance.org")


def main() -> int:
    pk = (os.getenv("PREDICT_PRIVATE_KEY") or "").strip()
    if not pk:
        print("ERROR: PREDICT_PRIVATE_KEY missing")
        return 1
    if not pk.startswith("0x"):
        pk = "0x" + pk

    from eth_account import Account
    from web3 import Web3

    acct = Account.from_key(pk)
    w3 = Web3(Web3.HTTPProvider(RPC))
    if not w3.is_connected():
        print("ERROR: cannot connect RPC", RPC)
        return 1

    addr = Web3.to_checksum_address(acct.address)
    bnb = w3.eth.get_balance(addr) / 1e18
    print(f"Wallet: {addr}")
    print(f"BNB:    {bnb:.6f}")

    abi = [
        {"name": "balanceOf", "type": "function", "stateMutability": "view",
         "inputs": [{"name": "a", "type": "address"}], "outputs": [{"type": "uint256"}]},
        {"name": "allowance", "type": "function", "stateMutability": "view",
         "inputs": [{"name": "o", "type": "address"}, {"name": "s", "type": "address"}],
         "outputs": [{"type": "uint256"}]},
        {"name": "decimals", "type": "function", "stateMutability": "view",
         "inputs": [], "outputs": [{"type": "uint8"}]},
    ]
    usdt = w3.eth.contract(address=Web3.to_checksum_address(USDT), abi=abi)
    try:
        dec = usdt.functions.decimals().call()
    except Exception:
        dec = 18
    bal = usdt.functions.balanceOf(addr).call() / (10 ** dec)
    print(f"USDT:   {bal:.4f}")
    if bnb < 0.001:
        print("WARN: low BNB — need gas for set_approvals")
    if bal < 1:
        print("WARN: low USDT for trading")
    print("\nNext: python scripts/set_approvals.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
