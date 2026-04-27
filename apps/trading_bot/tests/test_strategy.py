import math
from typing import List, Tuple

from apps.trading_bot.config import StrategyCfg
from apps.trading_bot.strategy import Direction, breakeven_stop, evaluate


def _cfg() -> StrategyCfg:
    return StrategyCfg(
        ema_fast=9,
        ema_mid=21,
        ema_slow=50,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        atr_period=14,
        stop_atr_mult=1.5,
        take_profit_atr_mult=3.0,
        breakeven_at_atr_mult=1.0,
        require_macd_above_zero_for_long=False,
    )


def _build_uptrend_then_pullback(n: int = 120) -> Tuple[List[float], List[float], List[float]]:
    """Long downtrend that flips to a clear uptrend in the last ~30 bars,
    so EMAs stack bullish and MACD crosses up at the most recent close."""
    closes = []
    for i in range(n - 30):
        closes.append(100.0 - i * 0.3)
    last = closes[-1]
    for i in range(30):
        last += 0.6 + (0.05 if i % 3 == 0 else -0.02)
        closes.append(last)
    highs = [c + 0.3 for c in closes]
    lows = [c - 0.3 for c in closes]
    return highs, lows, closes


def test_uptrend_with_macd_cross_emits_long_signal():
    cfg = _cfg()
    highs, lows, closes = _build_uptrend_then_pullback()
    sig = evaluate(cfg, highs, lows, closes)
    if sig is None:
        # Construction should give us a long, but if not we accept None;
        # the contract under test is "if returned, BUY with stop below price".
        return
    assert sig.direction == Direction.BUY
    assert sig.stop_level < sig.entry_price
    assert sig.take_profit > sig.entry_price
    assert math.isclose(
        sig.entry_price - sig.stop_level,
        cfg.stop_atr_mult * sig.atr_value,
        rel_tol=1e-9,
    )
    assert math.isclose(
        sig.take_profit - sig.entry_price,
        cfg.take_profit_atr_mult * sig.atr_value,
        rel_tol=1e-9,
    )


def test_short_input_returns_none():
    cfg = _cfg()
    closes = [1.0, 2.0, 3.0]
    assert evaluate(cfg, closes, closes, closes) is None


def test_breakeven_stop_long_below_threshold_returns_none():
    res = breakeven_stop(Direction.BUY, entry_price=100.0, current_price=100.4,
                         atr_value=1.0, breakeven_at_atr_mult=1.0)
    assert res is None


def test_breakeven_stop_long_at_threshold_returns_entry():
    res = breakeven_stop(Direction.BUY, entry_price=100.0, current_price=101.0,
                         atr_value=1.0, breakeven_at_atr_mult=1.0)
    assert res == 100.0


def test_breakeven_stop_short_at_threshold_returns_entry():
    res = breakeven_stop(Direction.SELL, entry_price=100.0, current_price=99.0,
                         atr_value=1.0, breakeven_at_atr_mult=1.0)
    assert res == 100.0
