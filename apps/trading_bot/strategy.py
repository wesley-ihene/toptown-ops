"""Entry / exit signal generation.

Strategy:
- Trend filter: EMA9, EMA21, EMA50 stacked in trend direction.
- Trigger: MACD line crosses signal line in the trend direction on the most
  recent closed bar.
- Stop: 1.5 * ATR(14) below entry (long) / above entry (short).
- Take profit: 3.0 * ATR(14) target (== 2R given a 1.5*ATR stop).
- Move to breakeven once price has moved 1.0 * ATR in profit.

Only price closes are used for signal evaluation; we treat the most
recently completed candle as the decision bar.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Sequence

from .config import StrategyCfg
from .indicators import atr, ema, macd


class Direction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class Signal:
    direction: Direction
    entry_price: float
    stop_level: float
    take_profit: float
    atr_value: float
    rationale: str


def _last_valid(seq: Sequence[float]) -> Optional[float]:
    for v in reversed(seq):
        if v == v:  # not NaN
            return v
    return None


def evaluate(
    cfg: StrategyCfg,
    highs: List[float],
    lows: List[float],
    closes: List[float],
) -> Optional[Signal]:
    """Return a Signal if entry conditions met on the most recent closed bar."""
    n = len(closes)
    if n < max(cfg.ema_slow, cfg.macd_slow + cfg.macd_signal, cfg.atr_period) + 2:
        return None

    e_fast = ema(closes, cfg.ema_fast)
    e_mid = ema(closes, cfg.ema_mid)
    e_slow = ema(closes, cfg.ema_slow)
    macd_res = macd(closes, cfg.macd_fast, cfg.macd_slow, cfg.macd_signal)
    atr_series = atr(highs, lows, closes, cfg.atr_period)

    # Most recent closed bar = index n-1
    i = n - 1
    prev = i - 1

    f, m, s = e_fast[i], e_mid[i], e_slow[i]
    macd_now, sig_now = macd_res.macd[i], macd_res.signal[i]
    macd_prev, sig_prev = macd_res.macd[prev], macd_res.signal[prev]
    atr_now = atr_series[i]
    close_now = closes[i]

    if any(v != v for v in (f, m, s, macd_now, sig_now, macd_prev, sig_prev, atr_now)):
        return None

    cross_up = macd_prev <= sig_prev and macd_now > sig_now
    cross_dn = macd_prev >= sig_prev and macd_now < sig_now

    long_trend = f > m > s
    short_trend = f < m < s

    long_macd_ok = (not cfg.require_macd_above_zero_for_long) or macd_now > 0
    short_macd_ok = (not cfg.require_macd_above_zero_for_long) or macd_now < 0

    if long_trend and cross_up and long_macd_ok:
        stop = close_now - cfg.stop_atr_mult * atr_now
        tp = close_now + cfg.take_profit_atr_mult * atr_now
        return Signal(
            direction=Direction.BUY,
            entry_price=close_now,
            stop_level=stop,
            take_profit=tp,
            atr_value=atr_now,
            rationale=(
                f"long: EMA{cfg.ema_fast}>{cfg.ema_mid}>{cfg.ema_slow} "
                f"({f:.5f}>{m:.5f}>{s:.5f}); MACD cross up "
                f"({macd_prev:.5f}->{macd_now:.5f} vs sig {sig_prev:.5f}->{sig_now:.5f}); "
                f"ATR={atr_now:.5f}"
            ),
        )

    if short_trend and cross_dn and short_macd_ok:
        stop = close_now + cfg.stop_atr_mult * atr_now
        tp = close_now - cfg.take_profit_atr_mult * atr_now
        return Signal(
            direction=Direction.SELL,
            entry_price=close_now,
            stop_level=stop,
            take_profit=tp,
            atr_value=atr_now,
            rationale=(
                f"short: EMA{cfg.ema_fast}<{cfg.ema_mid}<{cfg.ema_slow} "
                f"({f:.5f}<{m:.5f}<{s:.5f}); MACD cross down "
                f"({macd_prev:.5f}->{macd_now:.5f} vs sig {sig_prev:.5f}->{sig_now:.5f}); "
                f"ATR={atr_now:.5f}"
            ),
        )

    return None


def breakeven_stop(
    direction: Direction,
    entry_price: float,
    current_price: float,
    atr_value: float,
    breakeven_at_atr_mult: float,
) -> Optional[float]:
    """Return the new stop level (= entry) once price has moved breakeven_at_atr_mult * ATR
    in our favour. None means leave the stop where it is."""
    move = breakeven_at_atr_mult * atr_value
    if direction == Direction.BUY and current_price - entry_price >= move:
        return entry_price
    if direction == Direction.SELL and entry_price - current_price >= move:
        return entry_price
    return None
