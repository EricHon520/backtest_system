"""
Unit tests for SimulatedExecutionModel.

Covers:
- MARKET order fill at bar open
- fill_on_next_bar queuing vs immediate execution
- LIMIT order: filled when price crosses, not filled when it doesn't
- fill uses bar datetime (not order datetime)
- commission uses contract_multiplier
- invalid lot-size order returns rejected FillEvent
"""

import pytest
from datetime import datetime
from execution.execution_handler import SimulatedExecutionModel
from core.event import OrderEvent
from core.instrument import InstrumentRegistry, Stock, Future
from core.market_rule import MarketRule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bar(open_=100.0, high=105.0, low=95.0, close=102.0, volume=100_000,
         dt='2024-01-02 09:30:00'):
    return {
        'ticker': 'AAPL',
        'timestamp': 1,
        'datetime_local': dt,
        'open': open_, 'high': high, 'low': low, 'close': close,
        'volume': volume,
    }


class MockDataHandler:
    def __init__(self, bars):
        self._bars = bars          # list, index 0 = oldest
        self._idx = 0

    def advance(self):
        self._idx += 1

    def get_latest_bar(self, symbol):
        if self._idx < len(self._bars):
            return self._bars[self._idx]
        return None

    def get_latest_bars(self, symbol, num_bars):
        end = self._idx + 1
        start = max(0, end - num_bars)
        return self._bars[start:end]


def _us_registry():
    rule = MarketRule('us_stock')
    reg = InstrumentRegistry()
    reg.register(Stock(symbol='AAPL', market_rule=rule, currency='USD'))
    return reg


def _china_registry():
    rule = MarketRule('china_a')
    reg = InstrumentRegistry()
    reg.register(Stock(symbol='000001.SZ', market_rule=rule, currency='CNY'))
    return reg


def _futures_registry(multiplier=10):
    import market_rules.china_future_rule
    rule = MarketRule('china_future')
    reg = InstrumentRegistry()
    from core.instrument import Future
    reg.register(Future(
        symbol='IF2401', market_rule=rule,
        contract_multiplier=multiplier, currency='CNY',
        expiry_date=9999999999,
    ))
    return reg


def _order(symbol='AAPL', qty=100, direction='BUY',
           order_type='MARKET', limit_price=None):
    return OrderEvent(
        symbol=symbol,
        quantity=qty,
        direction=direction,
        datetime=datetime(2024, 1, 1, 15, 0),
        order_type=order_type,
        limit_price=limit_price,
    )


# ---------------------------------------------------------------------------
# fill_on_next_bar semantics
# ---------------------------------------------------------------------------

class TestFillOnNextBar:

    def test_fill_on_next_bar_queues_order(self):
        dh = MockDataHandler([_bar()])
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=True)
        result = eh.execute(_order())
        assert result is None
        assert len(eh.pending_orders) == 1

    def test_fill_on_same_bar_fills_immediately(self):
        dh = MockDataHandler([_bar()])
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=False)
        result = eh.execute(_order())
        assert result is not None
        assert len(eh.pending_orders) == 0

    def test_on_new_bar_drains_pending_orders(self):
        bars = [_bar(open_=100.0, dt='2024-01-01 09:30:00'),
                _bar(open_=101.5, dt='2024-01-02 09:30:00')]
        dh = MockDataHandler(bars)
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=True)
        eh.execute(_order())          # queued at bar 0
        dh.advance()                  # move to bar 1
        fills = eh.on_new_bar()
        assert len(fills) == 1
        assert len(eh.pending_orders) == 0

    def test_fill_uses_next_bar_open_price(self):
        bars = [_bar(open_=100.0), _bar(open_=101.5)]
        dh = MockDataHandler(bars)
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=True)
        eh.execute(_order())
        dh.advance()
        fills = eh.on_new_bar()
        # US stock: no slippage by default (model='none'), fill at open
        assert fills[0].fill_price == pytest.approx(101.5, rel=0.01)

    def test_fill_datetime_is_bar_datetime_not_order_datetime(self):
        bar_dt = '2024-01-02 09:30:00'
        bars = [_bar(), _bar(dt=bar_dt)]
        dh = MockDataHandler(bars)
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=True)
        eh.execute(_order())
        dh.advance()
        fills = eh.on_new_bar()
        assert fills[0].datetime == bar_dt


# ---------------------------------------------------------------------------
# MARKET order fill price
# ---------------------------------------------------------------------------

class TestMarketOrder:

    def test_market_order_fills_at_open(self):
        dh = MockDataHandler([_bar(open_=99.0)])
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=False)
        fill = eh.execute(_order(order_type='MARKET'))
        # US stock slippage model is 'none' → fill at open
        assert fill.fill_price == pytest.approx(99.0, rel=0.01)

    def test_market_order_not_rejected(self):
        dh = MockDataHandler([_bar()])
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=False)
        fill = eh.execute(_order())
        assert fill.rejected is False


# ---------------------------------------------------------------------------
# LIMIT order logic
# ---------------------------------------------------------------------------

class TestLimitOrder:

    def test_buy_limit_fills_when_low_crosses(self):
        # BUY limit=98; bar low=95 → crosses → fill at limit price 98
        dh = MockDataHandler([_bar(low=95.0)])
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=False)
        fill = eh.execute(_order(order_type='LIMIT', limit_price=98.0))
        assert fill is not None
        assert fill.fill_price == pytest.approx(98.0)

    def test_buy_limit_not_filled_when_low_above(self):
        # BUY limit=90; bar low=95 → does NOT cross → no fill
        dh = MockDataHandler([_bar(low=95.0)])
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=False)
        fill = eh.execute(_order(order_type='LIMIT', limit_price=90.0))
        assert fill is None

    def test_sell_limit_fills_when_high_crosses(self):
        # SELL limit=104; bar high=105 → crosses → fill at 104
        dh = MockDataHandler([_bar(high=105.0)])
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=False)
        fill = eh.execute(_order(direction='SELL', order_type='LIMIT', limit_price=104.0))
        assert fill is not None
        assert fill.fill_price == pytest.approx(104.0)

    def test_sell_limit_not_filled_when_high_below(self):
        # SELL limit=110; bar high=105 → does NOT cross
        dh = MockDataHandler([_bar(high=105.0)])
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=False)
        fill = eh.execute(_order(direction='SELL', order_type='LIMIT', limit_price=110.0))
        assert fill is None


# ---------------------------------------------------------------------------
# Commission uses contract_multiplier
# ---------------------------------------------------------------------------

class TestCommission:

    def test_stock_commission_no_multiplier(self):
        """US stock: commission = trade_value * rate, multiplier=1."""
        dh = MockDataHandler([_bar(open_=100.0)])
        eh = SimulatedExecutionModel(dh, instrument_registry=_us_registry(),
                                     fill_on_next_bar=False)
        fill = eh.execute(_order('AAPL', qty=100, direction='BUY'))
        rule = MarketRule('us_stock')
        expected = max(100 * fill.fill_price * rule.commission_rate,
                       rule.min_commission)
        assert fill.commission == pytest.approx(expected, rel=0.01)

    def test_futures_commission_includes_multiplier(self):
        """Futures multiplier=10: commission on qty*price*10, not qty*price."""
        import market_rules.china_future_rule
        dh = MockDataHandler([{
            'ticker': 'IF2401', 'timestamp': 1,
            'datetime_local': '2024-01-02 09:30:00',
            'open': 5000.0, 'high': 5100.0, 'low': 4900.0,
            'close': 5000.0, 'volume': 50_000,
        }])
        eh = SimulatedExecutionModel(dh, instrument_registry=_futures_registry(10),
                                     fill_on_next_bar=False)
        order = OrderEvent(symbol='IF2401', quantity=1, direction='BUY',
                           datetime=datetime(2024, 1, 1))
        fill = eh.execute(order)
        rule = MarketRule('china_future')
        # commission = max(qty * price * multiplier * rate, min_commission)
        expected = max(1 * fill.fill_price * 10 * rule.commission_rate,
                       rule.min_commission)
        assert fill.commission == pytest.approx(expected, rel=0.01)


# ---------------------------------------------------------------------------
# Invalid lot size → rejected
# ---------------------------------------------------------------------------

class TestOrderValidation:

    def test_invalid_lot_size_returns_rejected_fill(self):
        """A-share lot size = 100; order qty=50 is invalid."""
        dh = MockDataHandler([{
            'ticker': '000001.SZ', 'timestamp': 1,
            'datetime_local': datetime(2024, 1, 2, 10, 0),
            'open': 10.0, 'high': 10.5, 'low': 9.5,
            'close': 10.0, 'volume': 1_000_000,
        }])
        eh = SimulatedExecutionModel(dh, instrument_registry=_china_registry(),
                                     fill_on_next_bar=False)
        order = OrderEvent(symbol='000001.SZ', quantity=50, direction='BUY',
                           datetime=datetime(2024, 1, 2, 10, 0))
        fill = eh.execute(order)
        assert fill is not None
        assert fill.rejected is True

    def test_valid_lot_size_not_rejected(self):
        dh = MockDataHandler([{
            'ticker': '000001.SZ', 'timestamp': 1,
            'datetime_local': datetime(2024, 1, 2, 10, 0),
            'open': 10.0, 'high': 10.5, 'low': 9.5,
            'close': 10.0, 'volume': 1_000_000,
        }])
        eh = SimulatedExecutionModel(dh, instrument_registry=_china_registry(),
                                     fill_on_next_bar=False)
        order = OrderEvent(symbol='000001.SZ', quantity=100, direction='BUY',
                           datetime=datetime(2024, 1, 2, 10, 0))
        fill = eh.execute(order)
        assert fill.rejected is False

    def test_no_bar_data_returns_none(self):
        dh = MockDataHandler([])   # empty — no bar
        eh = SimulatedExecutionModel(dh, fill_on_next_bar=False)
        fill = eh.execute(_order())
        assert fill is None
