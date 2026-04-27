"""Main trading bot loop.

Run with:
    python -m apps.trading_bot.bot

It will:
  1) load .env + config.yaml,
  2) login to Capital.com (demo by default),
  3) every poll_seconds: pull recent candles for each market, evaluate the
     strategy, manage open positions (breakeven move), and place new orders
     respecting risk caps and kill switches.

This is intentionally cautious: max 1 position by default, demo by default,
and the daily DD kill switch halts new entries for the rest of the day.
"""
from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from .capital_client import CapitalAPIError, CapitalClient, Candle
from .config import BotConfig, Credentials, MarketCfg, load_config, load_credentials
from .risk import RiskManager, compute_size, market_dealing_constraints
from .strategy import Direction, Signal, breakeven_stop, evaluate

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

log = logging.getLogger("trading_bot")


def _setup_logging() -> None:
    handler_console = logging.StreamHandler(sys.stdout)
    handler_console.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s :: %(message)s"
    ))
    handler_file = logging.FileHandler(LOG_DIR / "bot.log")
    handler_file.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s :: %(message)s"
    ))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler_console)
    root.addHandler(handler_file)


def _journal(event: str, **fields) -> None:
    rec = {"ts": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
    with open(LOG_DIR / "trades.jsonl", "a") as f:
        f.write(json.dumps(rec, default=str) + "\n")
    log.info("%s :: %s", event, {k: v for k, v in fields.items() if k != "candles"})


def _within_session(cfg: BotConfig, now_utc: datetime) -> bool:
    if not cfg.session.trade_only_during_overlap:
        return True
    h, m = [int(x) for x in cfg.session.overlap_utc_start.split(":")]
    start = now_utc.replace(hour=h, minute=m, second=0, microsecond=0)
    h, m = [int(x) for x in cfg.session.overlap_utc_end.split(":")]
    end = now_utc.replace(hour=h, minute=m, second=0, microsecond=0)
    return start <= now_utc <= end


def _candles_to_lists(candles: List[Candle]):
    highs = [c.high for c in candles]
    lows = [c.low for c in candles]
    closes = [c.close for c in candles]
    return highs, lows, closes


class TradingBot:
    def __init__(self, creds: Credentials, cfg: BotConfig) -> None:
        self.cfg = cfg
        self.client = CapitalClient(creds)
        self.risk = RiskManager(cfg.risk)
        self._running = True
        self._market_constraints: Dict[str, Dict[str, float]] = {}

    def stop(self, *_: object) -> None:
        log.info("stop signal received")
        self._running = False

    # --- helpers ----------------------------------------------------------

    def _ensure_market_constraints(self, epic: str) -> Dict[str, float]:
        if epic in self._market_constraints:
            return self._market_constraints[epic]
        info = self.client.get_market(epic)
        cons = market_dealing_constraints(info)
        self._market_constraints[epic] = cons
        log.info("market %s constraints: %s", epic, cons)
        return cons

    def _open_positions_for(self, epic: str) -> List[Dict]:
        out = []
        for p in self.client.list_positions():
            mkt = p.get("market", {})
            if mkt.get("epic") == epic:
                out.append(p)
        return out

    # --- core actions -----------------------------------------------------

    def _place_entry(self, market: MarketCfg, equity: float, signal: Signal) -> None:
        cons = self._ensure_market_constraints(market.epic)
        size = compute_size(
            equity=equity,
            risk_pct=self.cfg.risk.per_trade_pct,
            entry_price=signal.entry_price,
            stop_price=signal.stop_level,
            contract_size=cons["contract_size"],
            min_size=cons["min_size"],
            size_step=cons["size_step"],
        )
        if size <= 0:
            _journal(
                "entry_skipped_size_zero",
                epic=market.epic,
                equity=equity,
                signal=signal.__dict__,
                constraints=cons,
                reason="risk per trade does not cover broker minimum size at this stop distance",
            )
            return
        try:
            resp = self.client.open_position(
                epic=market.epic,
                direction=signal.direction.value,
                size=size,
                stop_level=round(signal.stop_level, 5),
                profit_level=round(signal.take_profit, 5),
            )
        except CapitalAPIError as e:
            _journal("entry_error", epic=market.epic, error=str(e), signal=signal.__dict__)
            return
        _journal(
            "entry_placed",
            epic=market.epic,
            direction=signal.direction.value,
            size=size,
            entry_price=signal.entry_price,
            stop_level=signal.stop_level,
            take_profit=signal.take_profit,
            atr=signal.atr_value,
            rationale=signal.rationale,
            broker_response=resp,
        )

    def _manage_open(self, market: MarketCfg, last_price: float, atr_value: float) -> None:
        positions = self._open_positions_for(market.epic)
        for p in positions:
            pos = p.get("position", {})
            deal_id = pos.get("dealId")
            direction = pos.get("direction", "").upper()
            entry = float(pos.get("level", 0) or 0)
            current_stop = pos.get("stopLevel")
            if not deal_id or entry <= 0 or direction not in ("BUY", "SELL"):
                continue
            new_stop = breakeven_stop(
                Direction(direction),
                entry_price=entry,
                current_price=last_price,
                atr_value=atr_value,
                breakeven_at_atr_mult=self.cfg.strategy.breakeven_at_atr_mult,
            )
            if new_stop is None:
                continue
            already_at_or_better = (
                current_stop is not None and (
                    (direction == "BUY" and float(current_stop) >= entry) or
                    (direction == "SELL" and float(current_stop) <= entry)
                )
            )
            if already_at_or_better:
                continue
            try:
                self.client.update_position(deal_id, stop_level=round(new_stop, 5))
                _journal(
                    "stop_moved_breakeven",
                    epic=market.epic,
                    deal_id=deal_id,
                    direction=direction,
                    entry=entry,
                    new_stop=new_stop,
                    last_price=last_price,
                )
            except CapitalAPIError as e:
                _journal("stop_update_error", epic=market.epic, deal_id=deal_id, error=str(e))

    # --- main loop --------------------------------------------------------

    def run(self) -> None:
        log.info("logging in to Capital.com (%s)...", self.client._creds.env)
        info = self.client.login()
        log.info("logged in. account currency=%s", info.get("currencyIsoCode") or info.get("currency"))

        while self._running:
            try:
                self._tick()
            except CapitalAPIError as e:
                msg = str(e)
                if "401" in msg or "client_token" in msg.lower() or "errorCode\":\"error.invalid.session" in msg:
                    log.warning("session expired, re-logging: %s", msg)
                    try:
                        self.client.login()
                    except Exception as e2:
                        log.error("re-login failed: %s", e2)
                else:
                    log.error("API error: %s", msg)
            except Exception as e:  # noqa: BLE001
                log.exception("unexpected error: %s", e)
            time.sleep(self.cfg.poll_seconds)

    def _tick(self) -> None:
        now = datetime.now(timezone.utc)
        equity = self.client.account_balance()
        self.risk.ensure_day(now.date(), equity)

        if self.risk.is_halted():
            log.info("trading halted: %s", self.risk.halt_reason())
            return

        if not _within_session(self.cfg, now):
            log.debug("outside session window, skipping")
            return

        positions_total = len(self.client.list_positions())

        for market in self.cfg.markets:
            candles = self.client.get_prices(
                market.epic, self.cfg.resolution, self.cfg.candles_lookback
            )
            if len(candles) < self.cfg.candles_lookback // 2:
                log.warning("only %d candles for %s, skipping", len(candles), market.epic)
                continue

            highs, lows, closes = _candles_to_lists(candles)
            last_close = closes[-1]

            # Manage existing positions for this market (breakeven move).
            from .indicators import atr as _atr_fn
            atr_series = _atr_fn(highs, lows, closes, self.cfg.strategy.atr_period)
            atr_now = atr_series[-1] if atr_series and atr_series[-1] == atr_series[-1] else 0.0
            if atr_now > 0:
                self._manage_open(market, last_close, atr_now)

            # New entries only if under cap and no position on this epic.
            if positions_total >= self.cfg.risk.max_concurrent_positions:
                continue
            if self._open_positions_for(market.epic):
                continue

            sig = evaluate(self.cfg.strategy, highs, lows, closes)
            if sig is None:
                continue

            self._place_entry(market, equity, sig)
            positions_total += 1


def main() -> int:
    _setup_logging()
    creds = load_credentials()
    cfg = load_config()
    bot = TradingBot(creds, cfg)
    signal.signal(signal.SIGINT, bot.stop)
    signal.signal(signal.SIGTERM, bot.stop)
    bot.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
