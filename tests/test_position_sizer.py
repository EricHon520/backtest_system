"""
Unit tests for PositionSizer implementations.

Covers:
- FixedQuantityPositionSizer: quantity, lot rounding, confidence scaling, cash check
- PercentOfEquityPositionSizer: allocation math, lot rounding
- EqualWeightPositionSizer: slot math, lot rounding
- EXIT order: long → SELL, short → BUY, no position → None
- EXIT with T+1 available cap: sells only settled shares
- LONG/SHORT direction mapping
"""

import pytest
from datetime import datetime
from core.position_sizer import (
    FixedQuantityPositionSizer,
    PercentOfEquityPositionSizer,
    EqualWeightPositionSizer,
)
from core.event import SignalEvent
from core.portfolio_context import PortfolioContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DT = datetime(2024, 1, 1, 9, 30)


def _signal(symbol='AAPL', signal_type='LONG', confidence=1.0):
    return SignalEvent(symbol=symbol, datetime=_DT,
                       signal_type=signal_type, confidence=confidence)


def _bar(close=100.0):
    return {'open': close, 'high': close * 1.01, 'low': close * 0.99,
            'close': close, 'volume': 1_000_000}


def _ctx(cash=100_000, holdings=None, all_holdings=None):
    return PortfolioContext(
        current_cash=cash,
        initial_capital=100_000,
        current_holdings=holdings or {},
        all_holdings=all_holdings or [{'total': cash}],
        margin_used={},
    )


# ---------------------------------------------------------------------------
# FixedQuantityPositionSizer
# ---------------------------------------------------------------------------

class TestFixedQuantitySizer:

    def test_returns_fixed_qty(self):
        sizer = FixedQuantityPositionSizer(quantity=100)
        order = sizer.size_order(_signal(), _ctx(), _bar(), 1, 1)
        assert order.quantity == 100

    def test_confidence_scales_quantity(self):
        sizer = FixedQuantityPositionSizer(quantity=100)
        order = sizer.size_order(_signal(confidence=0.5), _ctx(), _bar(), 1, 1)
        assert order.quantity == 50

    def test_lot_rounding_applied(self):
        sizer = FixedQuantityPositionSizer(quantity=150)
        order = sizer.size_order(_signal(), _ctx(), _bar(close=10.0), 1, 100)
        # 150 rounded down to 100
        assert order.quantity == 100

    def test_returns_none_when_insufficient_cash(self):
        sizer = FixedQuantityPositionSizer(quantity=100)
        # bar close=200, need 100*200=20000 but only 1000 cash
        order = sizer.size_order(_signal(), _ctx(cash=1_000), _bar(close=200.0), 1, 1)
        assert order is None

    def test_long_direction_is_buy(self):
        sizer = FixedQuantityPositionSizer(quantity=100)
        order = sizer.size_order(_signal(signal_type='LONG'), _ctx(), _bar(), 1, 1)
        assert order.direction == 'BUY'

    def test_short_direction_is_sell(self):
        sizer = FixedQuantityPositionSizer(quantity=100)
        order = sizer.size_order(_signal(signal_type='SHORT'), _ctx(), _bar(), 1, 1)
        assert order.direction == 'SELL'

    def test_futures_multiplier_used_in_cash_check(self):
        # 1 lot @ 5000 * multiplier 10 = 50000; cash=40000 → reject
        sizer = FixedQuantityPositionSizer(quantity=1)
        order = sizer.size_order(_signal(), _ctx(cash=40_000),
                                 _bar(close=5000.0), 10, 1)
        assert order is None


# ---------------------------------------------------------------------------
# PercentOfEquityPositionSizer
# ---------------------------------------------------------------------------

class TestPercentOfEquitySizer:

    def test_allocates_correct_quantity(self):
        # 10% of 100000 cash = 10000; price=100 → 100 shares
        sizer = PercentOfEquityPositionSizer(percent=0.10)
        order = sizer.size_order(_signal(), _ctx(cash=100_000), _bar(close=100.0), 1, 1)
        assert order.quantity == 100

    def test_confidence_scales_allocation(self):
        # 10% * 0.5 confidence = 5%; 100000*0.05/100 = 50 shares
        sizer = PercentOfEquityPositionSizer(percent=0.10)
        order = sizer.size_order(_signal(confidence=0.5), _ctx(cash=100_000),
                                 _bar(close=100.0), 1, 1)
        assert order.quantity == 50

    def test_lot_rounding_applied(self):
        # 10% of 100000 = 10000; price=110 → 90 shares; lot=100 → 0? no: 90//100=0
        # Use price=95 → 10000/95=105 → 100 (lot=100)
        sizer = PercentOfEquityPositionSizer(percent=0.10)
        order = sizer.size_order(_signal(), _ctx(cash=100_000),
                                 _bar(close=95.0), 1, 100)
        assert order.quantity == 100

    def test_returns_none_when_price_zero(self):
        sizer = PercentOfEquityPositionSizer(percent=0.10)
        order = sizer.size_order(_signal(), _ctx(), _bar(close=0.0), 1, 1)
        assert order is None

    def test_invalid_percent_raises(self):
        with pytest.raises(ValueError):
            PercentOfEquityPositionSizer(percent=0.0)
        with pytest.raises(ValueError):
            PercentOfEquityPositionSizer(percent=1.5)

    def test_futures_contract_multiplier(self):
        # 10% of 500000 = 50000; price=5000, multiplier=10 → 50000/(5000*10)=1 lot
        sizer = PercentOfEquityPositionSizer(percent=0.10)
        order = sizer.size_order(_signal(), _ctx(cash=500_000),
                                 _bar(close=5000.0), 10, 1)
        assert order.quantity == 1


# ---------------------------------------------------------------------------
# EqualWeightPositionSizer
# ---------------------------------------------------------------------------

class TestEqualWeightSizer:

    def test_divides_equity_into_slots(self):
        # cash=100000, n=5 → slot=20000; price=100 → 200 shares
        sizer = EqualWeightPositionSizer(n_positions=5)
        order = sizer.size_order(_signal(), _ctx(cash=100_000), _bar(close=100.0), 1, 1)
        assert order.quantity == 200

    def test_lot_rounding_applied(self):
        # cash=100000, n=5 → slot=20000; price=110 → 181 → lot=100 → 100
        sizer = EqualWeightPositionSizer(n_positions=5)
        order = sizer.size_order(_signal(), _ctx(cash=100_000),
                                 _bar(close=110.0), 1, 100)
        assert order.quantity == 100

    def test_invalid_n_positions_raises(self):
        with pytest.raises(ValueError):
            EqualWeightPositionSizer(n_positions=0)


# ---------------------------------------------------------------------------
# EXIT order handling (shared across all sizers)
# ---------------------------------------------------------------------------

class TestExitOrder:

    @pytest.mark.parametrize("SizerClass,kwargs", [
        (FixedQuantityPositionSizer, {'quantity': 100}),
        (PercentOfEquityPositionSizer, {'percent': 0.10}),
        (EqualWeightPositionSizer, {'n_positions': 5}),
    ])
    def test_exit_long_emits_sell(self, SizerClass, kwargs):
        sizer = SizerClass(**kwargs)
        holdings = {'AAPL': {'quantity': 200, 'avg_cost': 10.0, 'available': 200}}
        ctx = _ctx(holdings=holdings)
        order = sizer.size_order(_signal(signal_type='EXIT'), ctx, _bar(), 1, 1)
        assert order is not None
        assert order.direction == 'SELL'
        assert order.quantity == 200

    @pytest.mark.parametrize("SizerClass,kwargs", [
        (FixedQuantityPositionSizer, {'quantity': 100}),
        (PercentOfEquityPositionSizer, {'percent': 0.10}),
        (EqualWeightPositionSizer, {'n_positions': 5}),
    ])
    def test_exit_short_emits_buy(self, SizerClass, kwargs):
        sizer = SizerClass(**kwargs)
        holdings = {'AAPL': {'quantity': -100, 'avg_cost': 10.0}}
        ctx = _ctx(holdings=holdings)
        order = sizer.size_order(_signal(signal_type='EXIT'), ctx, _bar(), 1, 1)
        assert order is not None
        assert order.direction == 'BUY'
        assert order.quantity == 100

    @pytest.mark.parametrize("SizerClass,kwargs", [
        (FixedQuantityPositionSizer, {'quantity': 100}),
        (PercentOfEquityPositionSizer, {'percent': 0.10}),
        (EqualWeightPositionSizer, {'n_positions': 5}),
    ])
    def test_exit_no_position_returns_none(self, SizerClass, kwargs):
        sizer = SizerClass(**kwargs)
        order = sizer.size_order(_signal(signal_type='EXIT'), _ctx(), _bar(), 1, 1)
        assert order is None

    def test_exit_t1_caps_sell_to_available(self):
        """T+1: hold 200, only 100 available (settled) → EXIT sells only 100."""
        sizer = FixedQuantityPositionSizer(quantity=200)
        holdings = {'AAPL': {'quantity': 200, 'avg_cost': 10.0, 'available': 100}}
        ctx = _ctx(holdings=holdings)
        order = sizer.size_order(_signal(signal_type='EXIT'), ctx, _bar(), 1, 1)
        assert order is not None
        assert order.quantity == 100

    def test_exit_t1_zero_available_returns_none(self):
        """T+1: hold 200, available=0 (all unsettled) → cannot sell → None."""
        sizer = FixedQuantityPositionSizer(quantity=200)
        holdings = {'AAPL': {'quantity': 200, 'avg_cost': 10.0, 'available': 0}}
        ctx = _ctx(holdings=holdings)
        order = sizer.size_order(_signal(signal_type='EXIT'), ctx, _bar(), 1, 1)
        assert order is None
