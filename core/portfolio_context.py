from dataclasses import dataclass, field
from typing import Dict, List, Any


@dataclass
class PortfolioContext:
    """
    Immutable snapshot of portfolio state passed to PositionSizer and RiskManager.

    Centralising these fields into one object means the ABC signatures for
    PositionSizer.size_order() and RiskManager.evaluate() never need to change
    when new portfolio attributes are added — callers just extend this dataclass.
    """

    current_cash: float
    initial_capital: float
    current_holdings: Dict[str, Any] = field(default_factory=dict)
    all_holdings: List[Dict[str, Any]] = field(default_factory=list)
    margin_used: Dict[str, float] = field(default_factory=dict)

    @property
    def total_equity(self) -> float:
        """Latest total portfolio value (cash + market value of positions)."""
        if self.all_holdings:
            return self.all_holdings[-1].get('total', self.current_cash)
        return self.current_cash

    @property
    def peak_equity(self) -> float:
        """Maximum total equity seen so far."""
        if not self.all_holdings:
            return self.initial_capital
        return max(h.get('total', 0.0) for h in self.all_holdings)

    @property
    def current_drawdown(self) -> float:
        """Current drawdown from peak as a positive fraction (e.g. 0.15 = 15%)."""
        peak = self.peak_equity
        if peak <= 0:
            return 0.0
        return max(0.0, (peak - self.total_equity) / peak)

    @property
    def open_position_count(self) -> int:
        """Number of symbols with a non-zero position."""
        return sum(1 for h in self.current_holdings.values() if h.get('quantity', 0) != 0)
