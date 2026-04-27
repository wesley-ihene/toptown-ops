"""Risk and position sizing.

Computes a position size in *broker units* (lots/units depending on the
instrument's contract size) such that, if the stop is hit, the loss equals
risk_pct * equity.

For Capital.com FX, position `size` is in lots-equivalent where 1.0 size on
EURUSD ≈ 1 mini lot (10,000 EUR notional → ~$1 per pip). The dealing rules
returned by /api/v1/markets/{epic} include `minDealSize` and `dealingRules`
which we honour as a clamp.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, Optional

from .config import RiskCfg


@dataclass
class DailyState:
    day: date
    starting_equity: float
    realised_pnl: float = 0.0
    consecutive_losses: int = 0
    halted_reason: Optional[str] = None


@dataclass
class RiskManager:
    cfg: RiskCfg
    state: Optional[DailyState] = None

    def reset_day(self, today: date, equity: float) -> None:
        self.state = DailyState(day=today, starting_equity=equity)

    def ensure_day(self, today: date, equity: float) -> None:
        if self.state is None or self.state.day != today:
            self.reset_day(today, equity)

    def record_trade_result(self, pnl: float) -> None:
        if self.state is None:
            return
        self.state.realised_pnl += pnl
        if pnl < 0:
            self.state.consecutive_losses += 1
        elif pnl > 0:
            self.state.consecutive_losses = 0
        self._update_halt()

    def _update_halt(self) -> None:
        if self.state is None or self.state.halted_reason is not None:
            return
        if self.state.starting_equity > 0:
            dd = -self.state.realised_pnl / self.state.starting_equity
            if dd >= self.cfg.daily_drawdown_kill_pct:
                self.state.halted_reason = (
                    f"daily drawdown kill switch hit "
                    f"({dd:.2%} >= {self.cfg.daily_drawdown_kill_pct:.2%})"
                )
                return
        if self.state.consecutive_losses >= self.cfg.max_consecutive_losses:
            self.state.halted_reason = (
                f"{self.state.consecutive_losses} consecutive losses "
                f">= {self.cfg.max_consecutive_losses}"
            )

    def is_halted(self) -> bool:
        return self.state is not None and self.state.halted_reason is not None

    def halt_reason(self) -> Optional[str]:
        return self.state.halted_reason if self.state else None


def compute_size(
    equity: float,
    risk_pct: float,
    entry_price: float,
    stop_price: float,
    contract_size: float,
    min_size: float,
    size_step: float,
) -> float:
    """Return position size in broker units.

    risk_amount = equity * risk_pct
    loss_per_unit_size = |entry - stop| * contract_size  (currency)
    size = risk_amount / loss_per_unit_size
    Then clamp to >= min_size and round down to nearest size_step.
    """
    if equity <= 0 or risk_pct <= 0:
        return 0.0
    stop_distance = abs(entry_price - stop_price)
    if stop_distance <= 0 or contract_size <= 0:
        return 0.0
    risk_amount = equity * risk_pct
    loss_per_unit = stop_distance * contract_size
    raw_size = risk_amount / loss_per_unit
    if raw_size < min_size:
        # Sub-minimum risk: do not trade rather than over-risk.
        return 0.0
    if size_step > 0:
        # Add a tiny epsilon to absorb float repr error (e.g. 0.4/0.01 -> 39.999...).
        steps = int(raw_size / size_step + 1e-9)
        return round(steps * size_step, 8)
    return raw_size


def market_dealing_constraints(market_resp: Dict) -> Dict[str, float]:
    """Extract minDealSize, sizeStep, contractSize from /markets/{epic} response."""
    instrument = market_resp.get("instrument", {})
    rules = market_resp.get("dealingRules", {})
    contract_size = float(instrument.get("contractSize", 1.0) or 1.0)

    min_node = rules.get("minDealSize", {}) or {}
    min_size = float(min_node.get("value", 0.01) or 0.01)

    step_node = rules.get("minControlledRiskStopDistance", {}) or {}  # not always present
    # Capital.com surfaces step via dealingRules.minSizeIncrement on some assets.
    incr_node = rules.get("minSizeIncrement") or rules.get("sizeIncrement") or {}
    size_step = float(incr_node.get("value", min_size) or min_size)

    return {
        "contract_size": contract_size,
        "min_size": min_size,
        "size_step": size_step,
    }
