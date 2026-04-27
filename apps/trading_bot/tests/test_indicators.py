import math

from apps.trading_bot.indicators import atr, ema, macd, true_range


def test_ema_short_input_returns_nans():
    out = ema([1.0, 2.0], period=5)
    assert len(out) == 2
    assert all(math.isnan(v) for v in out)


def test_ema_seeded_with_sma_then_smoothed():
    values = [10.0, 12.0, 14.0, 13.0, 15.0, 16.0]
    period = 3
    out = ema(values, period)
    assert math.isnan(out[0]) and math.isnan(out[1])
    assert out[2] == sum(values[:3]) / 3
    k = 2.0 / (period + 1)
    assert math.isclose(out[3], (values[3] - out[2]) * k + out[2])
    assert math.isclose(out[4], (values[4] - out[3]) * k + out[3])
    assert math.isclose(out[5], (values[5] - out[4]) * k + out[4])


def test_macd_lengths_and_hist_definition():
    values = [float(i) + (i % 4) * 0.5 for i in range(80)]
    res = macd(values, fast=12, slow=26, signal=9)
    assert len(res.macd) == len(values)
    assert len(res.signal) == len(values)
    assert len(res.hist) == len(values)
    for m, s, h in zip(res.macd, res.signal, res.hist):
        if not (math.isnan(m) or math.isnan(s)):
            assert math.isclose(h, m - s, rel_tol=1e-9, abs_tol=1e-12)


def test_true_range_first_bar_is_high_minus_low():
    h = [10.0, 11.0]
    l = [9.0, 10.0]
    c = [9.5, 10.5]
    tr = true_range(h, l, c)
    assert tr[0] == 1.0
    assert tr[1] == max(11 - 10, abs(11 - 9.5), abs(10 - 9.5))


def test_atr_wilder_rma():
    h = [2.0, 2.5, 3.0, 2.8, 3.2, 3.5]
    l = [1.0, 1.5, 2.0, 1.9, 2.1, 2.5]
    c = [1.5, 2.2, 2.5, 2.3, 3.0, 3.4]
    period = 3
    out = atr(h, l, c, period)
    tr = true_range(h, l, c)
    expected_seed = sum(tr[:period]) / period
    assert math.isclose(out[period - 1], expected_seed)
    expected_next = (expected_seed * (period - 1) + tr[period]) / period
    assert math.isclose(out[period], expected_next)
