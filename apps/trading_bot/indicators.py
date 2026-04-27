"""Pure-Python technical indicators (no numpy dependency).

EMA / MACD / ATR are computed in the standard textbook way:
- EMA seeded with SMA over the first `period` samples.
- MACD = EMA(fast) - EMA(slow); signal = EMA(MACD, signal_period); hist = MACD - signal.
- ATR uses Wilder's smoothing (RMA) over True Range.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence


def ema(values: Sequence[float], period: int) -> List[float]:
    if period <= 0:
        raise ValueError("period must be > 0")
    n = len(values)
    out: List[float] = [float("nan")] * n
    if n < period:
        return out
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    k = 2.0 / (period + 1.0)
    prev = seed
    for i in range(period, n):
        cur = (values[i] - prev) * k + prev
        out[i] = cur
        prev = cur
    return out


@dataclass
class MACDResult:
    macd: List[float]
    signal: List[float]
    hist: List[float]


def macd(values: Sequence[float], fast: int = 12, slow: int = 26, signal: int = 9) -> MACDResult:
    if fast >= slow:
        raise ValueError("fast period must be < slow period")
    fast_ema = ema(values, fast)
    slow_ema = ema(values, slow)
    macd_line = [
        (f - s) if (f == f and s == s) else float("nan")  # nan-safe
        for f, s in zip(fast_ema, slow_ema)
    ]
    # signal EMA only over the valid (non-NaN) tail
    valid_start = next((i for i, v in enumerate(macd_line) if v == v), len(macd_line))
    valid = macd_line[valid_start:]
    sig_tail = ema(valid, signal)
    sig_line = [float("nan")] * valid_start + sig_tail
    hist = [
        (m - s) if (m == m and s == s) else float("nan")
        for m, s in zip(macd_line, sig_line)
    ]
    return MACDResult(macd=macd_line, signal=sig_line, hist=hist)


def true_range(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float]) -> List[float]:
    n = len(closes)
    if not (len(highs) == len(lows) == n):
        raise ValueError("highs, lows, closes must have same length")
    tr: List[float] = [float("nan")] * n
    for i in range(n):
        if i == 0:
            tr[i] = highs[i] - lows[i]
        else:
            a = highs[i] - lows[i]
            b = abs(highs[i] - closes[i - 1])
            c = abs(lows[i] - closes[i - 1])
            tr[i] = max(a, b, c)
    return tr


def atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int = 14) -> List[float]:
    """Wilder's ATR (RMA of true range)."""
    n = len(closes)
    out: List[float] = [float("nan")] * n
    if n < period:
        return out
    tr = true_range(highs, lows, closes)
    seed = sum(tr[:period]) / period
    out[period - 1] = seed
    prev = seed
    for i in range(period, n):
        prev = (prev * (period - 1) + tr[i]) / period
        out[i] = prev
    return out
