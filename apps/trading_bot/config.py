from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml
from dotenv import load_dotenv


@dataclass
class MarketCfg:
    epic: str
    pip_size: float


@dataclass
class RiskCfg:
    per_trade_pct: float
    max_concurrent_positions: int
    daily_drawdown_kill_pct: float
    max_consecutive_losses: int


@dataclass
class StrategyCfg:
    ema_fast: int
    ema_mid: int
    ema_slow: int
    macd_fast: int
    macd_slow: int
    macd_signal: int
    atr_period: int
    stop_atr_mult: float
    take_profit_atr_mult: float
    breakeven_at_atr_mult: float
    require_macd_above_zero_for_long: bool


@dataclass
class SessionCfg:
    trade_only_during_overlap: bool
    overlap_utc_start: str
    overlap_utc_end: str


@dataclass
class Credentials:
    env: str
    api_key: str
    identifier: str
    password: str

    @property
    def base_url(self) -> str:
        if self.env == "live":
            return "https://api-capital.backend-capital.com"
        return "https://demo-api-capital.backend-capital.com"


@dataclass
class BotConfig:
    mode: str
    poll_seconds: int
    candles_lookback: int
    resolution: str
    risk: RiskCfg
    strategy: StrategyCfg
    session: SessionCfg
    markets: List[MarketCfg] = field(default_factory=list)


def load_credentials(env_path: Path | None = None) -> Credentials:
    if env_path is None:
        env_path = Path(__file__).parent / ".env"
    load_dotenv(env_path)
    env = os.getenv("CAPITAL_ENV", "demo").strip().lower()
    api_key = os.getenv("CAPITAL_API_KEY", "").strip()
    identifier = os.getenv("CAPITAL_IDENTIFIER", "").strip()
    password = os.getenv("CAPITAL_PASSWORD", "").strip()
    missing = [k for k, v in {
        "CAPITAL_API_KEY": api_key,
        "CAPITAL_IDENTIFIER": identifier,
        "CAPITAL_PASSWORD": password,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")
    return Credentials(env=env, api_key=api_key, identifier=identifier, password=password)


def load_config(yaml_path: Path | None = None) -> BotConfig:
    if yaml_path is None:
        yaml_path = Path(__file__).parent / "config.yaml"
    with open(yaml_path) as f:
        raw = yaml.safe_load(f)
    return BotConfig(
        mode=raw["mode"],
        poll_seconds=int(raw["poll_seconds"]),
        candles_lookback=int(raw["candles_lookback"]),
        resolution=raw["resolution"],
        risk=RiskCfg(**raw["risk"]),
        strategy=StrategyCfg(**raw["strategy"]),
        session=SessionCfg(**raw["session"]),
        markets=[MarketCfg(**m) for m in raw["markets"]],
    )
