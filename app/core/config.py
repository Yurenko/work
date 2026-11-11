"""Application settings from .env."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # MODE
    mode: Literal["PAPER", "LIVE"] = "PAPER"

    # Strategy
    entry_sum_max: float = 0.80
    exit_sum_min: float = 1.10
    pm_shares: float = 6.0
    base_shares: float = 6.0
    shares_step: float = 6.0
    max_shares: float = 20.0
    max_recovery_debt_usd: float = 50.0
    bn_min_usd: float = 1.0  # kept name for recovery sizing; means PF min notional
    max_open_hedges: int = 2
    min_time_to_expiry_sec: int = 60
    leg_retry_sec: float = 3.0
    leg_retry_max: int = 5

    # Predict.fun
    predict_api_key: str = ""
    predict_private_key: str = ""
    # Predict smart wallet (deposit address from Settings) — required for web-app funded accounts
    predict_account: str = ""
    predict_rest_base_url: str = "https://api.predict.fun"
    predict_wss_url: str = "wss://ws.predict.fun/ws"
    predict_chain_id: int = 56
    predict_min_request_interval_ms: int = 250

    # Polymarket
    private_key: str = ""
    funder_address: str = ""
    signature_type: int = 0
    # Polymarket taker fee on top of CLOB makingAmount (wallet debit > making).
    # Empirically ~4% (e.g. $2.46 → $2.56). Set 0 to disable.
    pm_taker_fee_rate: float = 0.04

    # Server
    host: str = "0.0.0.0"
    port: int = 8888
    log_level: str = "INFO"

    @property
    def is_live(self) -> bool:
        return self.mode.upper() == "LIVE"

    def predict_auth(self) -> tuple[str, str]:
        """Return (api_key, private_key) stripped."""
        return (
            (self.predict_api_key or "").strip().strip('"').strip("'"),
            (self.predict_private_key or "").strip().strip('"').strip("'"),
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
