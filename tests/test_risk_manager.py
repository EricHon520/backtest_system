"""
Unit tests for RiskManager implementations.

Covers:
- NullRiskManager: pass-through
- MaxDrawdownRiskManager: blocks BUY when drawdown >= threshold, allows SELL
- MaxPositionSizeRiskManager: trims quantity, rejects when full, passes SELL
- MaxOpenPositionsRiskManager: blocks new BUY when at cap, allows existing symbol, allows SELL
- CompositeRiskManager: chains correctly, first rejection wins
"""

import pytest
from datetime import datetime
from core.event import OrderEvent
from core.portfolio_context import PortfolioContext
from risk.risk_manager import (
    NullRiskManager,
    MaxDrawdownRiskManager,
    MaxPositionSizeRiskManager,
    MaxOpenPositionsRiskManager,
    CompositeRiskManager,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DT = datetime(2024, 1, 1, 9, 30)


def _order(symbol='AAPL', qty=100, direction='BUY', limit_price=None):
    return OrderEvent(
        symbol=symbol, quantity=qty, direction=direction,
        datetime=_DT, limit_price=limit_price,
    )


def _ctx(cash=100_000, initial=100_000, holdings=None, all_holdings=None,
         margin_used=None):
    """Build a PortfolioContext.  all_holdings[-1]['total'] drives total_equity."""
    return PortfolioContext(
        current_cash=cash,
        initial_capital=initial,
        current_holdings=holdings or {},
        all_holdings=all_holdings or [{'total': cash}],
        margin_used=margin_used or {},
    )


def _ctx_with_drawdown(drawdown_pct: float):
    """Context where current equity is (1 - drawdown_pct) * initial."""
    initial = 100_000
    current = initial * (1 - drawdown_pct)
    # peak is initial; all_holdings drives total_equity
    return PortfolioContext(
        current_cash=current,
        initial_capital=initial,
        current_holdings={},
        all_holdings=[{'total': initial}, {'total': current}],
        margin_used={},
    )


# ---------------------------------------------------------------------------
# NullRiskManager
# ---------------------------------------------------------------------------

class TestNullRiskManager:

    def test_passes_buy_through(self):
        rm = NullRiskManager()
        o = _order()
        assert rm.evaluate(o, _ctx()) is o

    def test_passes_sell_through(self):
        rm = NullRiskManager()
        o = _order(direction='SELL')
        assert rm.evaluate(o, _ctx()) is o


# ---------------------------------------------------------------------------
# MaxDrawdownRiskManager
# ---------------------------------------------------------------------------

class TestMaxDrawdownRiskManager:

    def test_blocks_buy_when_drawdown_exceeds_threshold(self):
        rm = MaxDrawdownRiskManager(max_drawdown=0.20)
        ctx = _ctx_with_drawdown(0.25)   # 25% drawdown > 20% limit
        assert rm.evaluate(_order(direction='BUY'), ctx) is None

    def test_allows_buy_when_drawdown_below_threshold(self):
        rm = MaxDrawdownRiskManager(max_drawdown=0.20)
        ctx = _ctx_with_drawdown(0.10)   # 10% drawdown < 20% limit
        result = rm.evaluate(_order(direction='BUY'), ctx)
        assert result is not None

    def test_allows_buy_at_exact_threshold(self):
        # drawdown == threshold: NOT blocked (condition is >=)
        rm = MaxDrawdownRiskManager(max_drawdown=0.20)
        ctx = _ctx_with_drawdown(0.20)
        # exactly 20% — should be blocked
        assert rm.evaluate(_order(direction='BUY'), ctx) is None

    def test_always_allows_sell_regardless_of_drawdown(self):
        rm = MaxDrawdownRiskManager(max_drawdown=0.10)
        ctx = _ctx_with_drawdown(0.50)   # severe drawdown
        result = rm.evaluate(_order(direction='SELL'), ctx)
        assert result is not None

    def test_invalid_max_drawdown_raises(self):
        with pytest.raises(ValueError):
            MaxDrawdownRiskManager(max_drawdown=0.0)
        with pytest.raises(ValueError):
            MaxDrawdownRiskManager(max_drawdown=1.0)


# ---------------------------------------------------------------------------
# MaxPositionSizeRiskManager
# ---------------------------------------------------------------------------

class TestMaxPositionSizeRiskManager:

    def test_trims_quantity_when_over_limit(self):
        """
        Equity=100000, max_pct=0.30 → max notional=30000.
        Existing AAPL: 200 shares @ 100 = 20000 already used.
        Remaining = 30000 - 20000 = 10000; price=100 → max_qty=100.
        Order qty=200 → trimmed to 100.
        """
        rm = MaxPositionSizeRiskManager(max_position_pct=0.30)
        holdings = {'AAPL': {'quantity': 200, 'avg_cost': 100.0}}
        all_holdings = [{'total': 100_000, 'AAPL_value': 20_000.0}]
        ctx = _ctx(cash=80_000, holdings=holdings, all_holdings=all_holdings)
        result = rm.evaluate(_order('AAPL', qty=200), ctx)
        assert result is not None
        assert result.quantity == 100

    def test_rejects_when_position_already_full(self):
        """Existing AAPL 300 shares @ 100 = 30000 = 30% of 100000 → no room."""
        rm = MaxPositionSizeRiskManager(max_position_pct=0.30)
        holdings = {'AAPL': {'quantity': 300, 'avg_cost': 100.0}}
        all_holdings = [{'total': 100_000, 'AAPL_value': 30_000.0}]
        ctx = _ctx(cash=70_000, holdings=holdings, all_holdings=all_holdings)
        result = rm.evaluate(_order('AAPL', qty=100), ctx)
        assert result is None

    def test_allows_sell_unconditionally(self):
        rm = MaxPositionSizeRiskManager(max_position_pct=0.10)
        result = rm.evaluate(_order(direction='SELL'), _ctx())
        assert result is not None

    def test_passes_through_when_no_price_info(self):
        """No existing position and no limit_price → cannot cap, pass through."""
        rm = MaxPositionSizeRiskManager(max_position_pct=0.30)
        ctx = _ctx()   # empty holdings, all_holdings has no symbol_value
        result = rm.evaluate(_order('AAPL', qty=100), ctx)
        assert result is not None

    def test_invalid_pct_raises(self):
        with pytest.raises(ValueError):
            MaxPositionSizeRiskManager(max_position_pct=0.0)


# ---------------------------------------------------------------------------
# MaxOpenPositionsRiskManager
# ---------------------------------------------------------------------------

class TestMaxOpenPositionsRiskManager:

    def test_blocks_new_buy_when_at_cap(self):
        """2 symbols open, max=2 → third new BUY blocked."""
        rm = MaxOpenPositionsRiskManager(max_positions=2)
        holdings = {
            'AAPL': {'quantity': 100},
            'GOOG': {'quantity': 50},
        }
        ctx = _ctx(holdings=holdings)
        result = rm.evaluate(_order('MSFT', direction='BUY'), ctx)
        assert result is None

    def test_allows_buy_when_below_cap(self):
        rm = MaxOpenPositionsRiskManager(max_positions=3)
        holdings = {'AAPL': {'quantity': 100}}
        ctx = _ctx(holdings=holdings)
        result = rm.evaluate(_order('GOOG', direction='BUY'), ctx)
        assert result is not None

    def test_allows_adding_to_existing_position(self):
        """Symbol already open → adding more is not a NEW position, allowed."""
        rm = MaxOpenPositionsRiskManager(max_positions=1)
        holdings = {'AAPL': {'quantity': 100}}
        ctx = _ctx(holdings=holdings)
        result = rm.evaluate(_order('AAPL', direction='BUY'), ctx)
        assert result is not None

    def test_always_allows_sell(self):
        rm = MaxOpenPositionsRiskManager(max_positions=0)
        ctx = _ctx()
        result = rm.evaluate(_order(direction='SELL'), ctx)
        assert result is not None

    def test_allows_first_position_when_empty(self):
        rm = MaxOpenPositionsRiskManager(max_positions=1)
        ctx = _ctx()
        result = rm.evaluate(_order(direction='BUY'), ctx)
        assert result is not None


# ---------------------------------------------------------------------------
# CompositeRiskManager
# ---------------------------------------------------------------------------

class TestCompositeRiskManager:

    def test_passes_when_all_approve(self):
        rm = CompositeRiskManager([NullRiskManager(), NullRiskManager()])
        o = _order()
        assert rm.evaluate(o, _ctx()) is o

    def test_first_rejection_wins(self):
        rm = CompositeRiskManager([
            MaxDrawdownRiskManager(max_drawdown=0.10),  # will block
            NullRiskManager(),
        ])
        ctx = _ctx_with_drawdown(0.20)
        assert rm.evaluate(_order(), ctx) is None

    def test_second_rejection_wins(self):
        rm = CompositeRiskManager([
            NullRiskManager(),
            MaxOpenPositionsRiskManager(max_positions=0),  # blocks all new buys
        ])
        assert rm.evaluate(_order(), _ctx()) is None

    def test_order_trimmed_by_middle_manager(self):
        """Size manager trims, then null passes it through."""
        rm = CompositeRiskManager([
            NullRiskManager(),
            MaxPositionSizeRiskManager(max_position_pct=0.30),
            NullRiskManager(),
        ])
        holdings = {'AAPL': {'quantity': 200, 'avg_cost': 100.0}}
        all_holdings = [{'total': 100_000, 'AAPL_value': 20_000.0}]
        ctx = _ctx(cash=80_000, holdings=holdings, all_holdings=all_holdings)
        result = rm.evaluate(_order('AAPL', qty=200), ctx)
        assert result is not None
        assert result.quantity == 100
