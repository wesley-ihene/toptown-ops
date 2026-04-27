from datetime import date

from apps.trading_bot.config import RiskCfg
from apps.trading_bot.risk import RiskManager, compute_size, market_dealing_constraints


def _cfg() -> RiskCfg:
    return RiskCfg(
        per_trade_pct=0.02,
        max_concurrent_positions=1,
        daily_drawdown_kill_pct=0.10,
        max_consecutive_losses=3,
    )


def test_compute_size_respects_risk_amount():
    # equity 1000, risk 2% = $20 risk. Stop distance 0.0050, contract size 10000
    # => loss/unit = 0.0050 * 10000 = $50. size = 20/50 = 0.4 → step 0.01 → 0.4
    size = compute_size(
        equity=1000.0, risk_pct=0.02, entry_price=1.1000, stop_price=1.0950,
        contract_size=10000.0, min_size=0.01, size_step=0.01,
    )
    assert abs(size - 0.4) < 1e-9


def test_compute_size_returns_zero_below_min():
    # equity 30, risk 2% = $0.60 risk. Stop 0.0050, contract 10000 → loss/unit $50.
    # size = 0.012, min 0.5 → returns 0 (refuse to over-risk).
    size = compute_size(
        equity=30.0, risk_pct=0.02, entry_price=1.1000, stop_price=1.0950,
        contract_size=10000.0, min_size=0.5, size_step=0.5,
    )
    assert size == 0.0


def test_compute_size_rounds_down_to_step():
    size = compute_size(
        equity=10000.0, risk_pct=0.02, entry_price=1.1000, stop_price=1.0950,
        contract_size=10000.0, min_size=0.01, size_step=0.10,
    )
    # raw = 4.0 → step 0.10 → 4.0
    assert abs(size - 4.0) < 1e-9


def test_compute_size_zero_when_no_stop_distance():
    assert compute_size(100, 0.02, 1.0, 1.0, 10000, 0.01, 0.01) == 0.0


def test_dailystate_drawdown_kill_switch():
    rm = RiskManager(_cfg())
    rm.reset_day(date(2026, 4, 27), equity=100.0)
    rm.record_trade_result(-5.0)
    assert not rm.is_halted()
    rm.record_trade_result(-6.0)  # cumulative -11 = 11% > 10%
    assert rm.is_halted()
    assert "daily drawdown" in rm.halt_reason()


def test_dailystate_consecutive_losses_kill_switch():
    rm = RiskManager(_cfg())
    rm.reset_day(date(2026, 4, 27), equity=10000.0)
    for _ in range(3):
        rm.record_trade_result(-1.0)
    assert rm.is_halted()
    assert "consecutive losses" in rm.halt_reason()


def test_dailystate_win_resets_consecutive_counter():
    rm = RiskManager(_cfg())
    rm.reset_day(date(2026, 4, 27), equity=10000.0)
    rm.record_trade_result(-1.0)
    rm.record_trade_result(-1.0)
    rm.record_trade_result(+5.0)
    rm.record_trade_result(-1.0)
    rm.record_trade_result(-1.0)
    assert not rm.is_halted()


def test_market_dealing_constraints_extracts_fields():
    resp = {
        "instrument": {"contractSize": 10000},
        "dealingRules": {
            "minDealSize": {"value": 0.01},
            "minSizeIncrement": {"value": 0.01},
        },
    }
    out = market_dealing_constraints(resp)
    assert out["contract_size"] == 10000.0
    assert out["min_size"] == 0.01
    assert out["size_step"] == 0.01
