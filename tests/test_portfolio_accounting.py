"""
Unit tests for Portfolio accounting correctness.

Covers:
- Stock BUY / SELL cash flow and PnL
- Stock partial close and full close
- Stock reversal (long → short)
- Futures long open / MTM daily settlement / partial close / full close
- Futures short open / MTM / close
- Futures reversal
- T+1 available quantity settlement
- Commission double-count guard (commission deducted once, not in PnL)
"""

import pytest
from datetime import datetime
from portfolio.portfolio import Portfolio
from core.instrument import InstrumentRegistry, Stock, Future
from core.market_rule import MarketRule
from core.event import FillEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fill(symbol, direction, quantity, fill_price, commission=0.0,
               dt=None):
    return FillEvent(
        symbol=symbol,
        exchange='TEST',
        quantity=quantity,
        direction=direction,
        fill_price=fill_price,
        datetime=dt or datetime(2024, 1, 1),
        rejected=False,
        commission=commission,
    )


def _make_market_event(dt=None):
    from core.event import MarketEvent
    return MarketEvent(datetime=dt or datetime(2024, 1, 2), symbols=[])


def _stock_portfolio(initial=100_000):
    """Portfolio with a single T+0 stock (contract_multiplier=1)."""
    rule = MarketRule('us_stock')
    registry = InstrumentRegistry()
    registry.register(Stock(symbol='AAPL', market_rule=rule, currency='USD'))
    return Portfolio(initial_capital=initial, instrument_registry=registry)


def _china_a_portfolio(initial=100_000):
    """Portfolio with a single T+1 A-share stock."""
    rule = MarketRule('china_a')
    registry = InstrumentRegistry()
    registry.register(Stock(symbol='000001.SZ', market_rule=rule, currency='CNY'))
    return Portfolio(initial_capital=initial, instrument_registry=registry)


def _futures_portfolio(initial=500_000, multiplier=10):
    """Portfolio with a daily-settlement futures contract."""
    import market_rules.china_future_rule  # ensure preset is registered
    rule = MarketRule('china_future')
    registry = InstrumentRegistry()
    registry.register(Future(
        symbol='IF2401',
        market_rule=rule,
        contract_multiplier=multiplier,
        currency='CNY',
        expiry_date=9999999999,
    ))
    return Portfolio(initial_capital=initial, instrument_registry=registry)


# ---------------------------------------------------------------------------
# Stock: BUY then SELL (full close)
# ---------------------------------------------------------------------------

class TestStockBuySell:

    def test_buy_deducts_notional_plus_commission(self):
        p = _stock_portfolio(100_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY', 100, 10.0, commission=5.0))
        # cash = 100000 - 100*10 - 5 = 98995
        assert p.current_cash == pytest.approx(98_995.0)

    def test_buy_sets_quantity_and_avg_cost(self):
        p = _stock_portfolio(100_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY', 100, 10.0, commission=5.0))
        h = p.current_holdings['AAPL']
        assert h['quantity'] == 100
        assert h['avg_cost'] == pytest.approx(10.0)

    def test_sell_full_close_cash_and_pnl(self):
        p = _stock_portfolio(100_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY', 100, 10.0, commission=5.0))
        p.process_fill_event(_make_fill('AAPL', 'SELL', 100, 12.0, commission=3.0))
        # cash after sell = 98995 + 100*12 - 3 = 98995 + 1197 = 100192
        assert p.current_cash == pytest.approx(100_192.0)
        # realized PnL = 100*(12-10) = 200 (commission NOT included in pnl)
        assert p.total_realized_pnl == pytest.approx(200.0)
        # position is flat
        assert p.current_holdings['AAPL']['quantity'] == 0

    def test_commission_not_double_counted_in_pnl(self):
        """PnL must equal price gain only; commission is deducted from cash separately."""
        p = _stock_portfolio(100_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY',  100, 10.0, commission=10.0))
        p.process_fill_event(_make_fill('AAPL', 'SELL', 100, 10.0, commission=10.0))
        # PnL = 0 (bought and sold at same price)
        assert p.total_realized_pnl == pytest.approx(0.0)
        # Net cash change = -10 (buy comm) - 10 (sell comm) = -20
        assert p.current_cash == pytest.approx(100_000 - 20.0)


# ---------------------------------------------------------------------------
# Stock: partial close
# ---------------------------------------------------------------------------

class TestStockPartialClose:

    def test_partial_sell_reduces_quantity(self):
        p = _stock_portfolio(100_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY',  200, 10.0))
        p.process_fill_event(_make_fill('AAPL', 'SELL', 100, 12.0))
        assert p.current_holdings['AAPL']['quantity'] == 100

    def test_partial_sell_pnl_proportional(self):
        p = _stock_portfolio(100_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY',  200, 10.0))
        p.process_fill_event(_make_fill('AAPL', 'SELL', 100, 12.0))
        # pnl for 100 shares = 100*(12-10) = 200
        assert p.total_realized_pnl == pytest.approx(200.0)

    def test_avg_cost_unchanged_after_partial_sell(self):
        p = _stock_portfolio(100_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY',  200, 10.0))
        p.process_fill_event(_make_fill('AAPL', 'SELL', 100, 12.0))
        # avg_cost should still reflect the entry price, not the sell price
        assert p.current_holdings['AAPL']['avg_cost'] == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# Stock: add to position (avg cost update)
# ---------------------------------------------------------------------------

class TestStockAddToPosition:

    def test_add_position_updates_avg_cost(self):
        p = _stock_portfolio(200_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY', 100, 10.0))
        p.process_fill_event(_make_fill('AAPL', 'BUY', 100, 12.0))
        # avg_cost = (100*10 + 100*12) / 200 = 11.0
        assert p.current_holdings['AAPL']['avg_cost'] == pytest.approx(11.0)
        assert p.current_holdings['AAPL']['quantity'] == 200

    def test_add_position_cash_deducted_twice(self):
        p = _stock_portfolio(200_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY', 100, 10.0, commission=5.0))
        p.process_fill_event(_make_fill('AAPL', 'BUY', 100, 12.0, commission=5.0))
        # cash = 200000 - 1000 - 5 - 1200 - 5 = 197790
        assert p.current_cash == pytest.approx(197_790.0)


# ---------------------------------------------------------------------------
# Stock: reversal (long → short)
# ---------------------------------------------------------------------------

class TestStockReversal:

    def test_reversal_long_to_short(self):
        p = _stock_portfolio(200_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY',  100, 10.0, commission=0.0))
        # SELL 150 reverses to short 50
        p.process_fill_event(_make_fill('AAPL', 'SELL', 150, 12.0, commission=0.0))
        assert p.current_holdings['AAPL']['quantity'] == -50
        assert p.current_holdings['AAPL']['avg_cost'] == pytest.approx(12.0)

    def test_reversal_pnl_only_covers_closing_leg(self):
        p = _stock_portfolio(200_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY',  100, 10.0, commission=0.0))
        p.process_fill_event(_make_fill('AAPL', 'SELL', 150, 12.0, commission=0.0))
        # pnl = 100*(12-10) = 200 (only the 100-share closing leg)
        assert p.total_realized_pnl == pytest.approx(200.0)

    def test_reversal_cash_correct(self):
        """cash after reversal: proceeds from closing long - new short margin."""
        p = _stock_portfolio(200_000)
        p.process_fill_event(_make_fill('AAPL', 'BUY',  100, 10.0, commission=0.0))
        # cash after buy = 200000 - 1000 = 199000
        p.process_fill_event(_make_fill('AAPL', 'SELL', 150, 12.0, commission=0.0))
        # close long: cash += 100*12 = +1200 → 200200
        # open short 50: cash -= 50*12 = -600  → 199600
        assert p.current_cash == pytest.approx(199_600.0)


# ---------------------------------------------------------------------------
# T+1 settlement (China A-share)
# ---------------------------------------------------------------------------

class TestT1Settlement:

    def test_buy_creates_pending_settlement(self):
        p = _china_a_portfolio()
        p.process_fill_event(_make_fill(
            '000001.SZ', 'BUY', 100, 10.0,
            dt=datetime(2024, 1, 2, 9, 30)
        ))
        # available should still be 0 immediately after buy
        assert p.current_holdings['000001.SZ']['available'] == 0
        assert len(p.pending_settlements) == 1

    def test_available_released_after_1_trading_day(self):
        p = _china_a_portfolio()
        p.process_fill_event(_make_fill(
            '000001.SZ', 'BUY', 100, 10.0,
            dt=datetime(2024, 1, 2, 9, 30)   # Tuesday
        ))
        me = _make_market_event(dt=datetime(2024, 1, 3))  # Wednesday T+1
        p.update_timeindex(market_event=me, current_prices={'000001.SZ': 10.0})
        assert p.current_holdings['000001.SZ']['available'] == 100

    def test_same_day_not_yet_available(self):
        p = _china_a_portfolio()
        p.process_fill_event(_make_fill(
            '000001.SZ', 'BUY', 100, 10.0,
            dt=datetime(2024, 1, 2, 9, 30)
        ))
        # update_timeindex on the SAME day → T+1 not yet reached
        me = _make_market_event(dt=datetime(2024, 1, 2))
        p.update_timeindex(market_event=me, current_prices={'000001.SZ': 10.0})
        assert p.current_holdings['000001.SZ']['available'] == 0


# ---------------------------------------------------------------------------
# Futures: daily settlement (MTM)
# ---------------------------------------------------------------------------

class TestFuturesMTM:

    def test_long_open_deducts_full_margin(self):
        p = _futures_portfolio(500_000, multiplier=10)
        p.process_fill_event(_make_fill('IF2401', 'BUY', 1, 5000.0, commission=10.0))
        # margin = 1 * 5000 * 10 = 50000, commission = 10
        assert p.current_cash == pytest.approx(500_000 - 50_000 - 10)
        assert p.margin_used.get('IF2401') == pytest.approx(50_000)

    def test_mtm_credits_gain_to_cash(self):
        p = _futures_portfolio(500_000, multiplier=10)
        p.process_fill_event(_make_fill('IF2401', 'BUY', 1, 5000.0, commission=0.0,
                                        dt=datetime(2024, 1, 2)))
        me = _make_market_event(dt=datetime(2024, 1, 3))
        p.update_timeindex(market_event=me, current_prices={'IF2401': 5100.0})
        # MTM gain = (5100-5000)*1*10 = 1000
        # cash after buy = 500000 - 50000 = 450000
        # cash after MTM = 450000 + 1000 = 451000
        assert p.current_cash == pytest.approx(451_000.0)

    def test_mtm_debits_loss_from_cash(self):
        p = _futures_portfolio(500_000, multiplier=10)
        p.process_fill_event(_make_fill('IF2401', 'BUY', 1, 5000.0, commission=0.0))
        me = _make_market_event(dt=datetime(2024, 1, 3))
        p.update_timeindex(market_event=me, current_prices={'IF2401': 4900.0})
        # MTM loss = (4900-5000)*1*10 = -1000
        assert p.current_cash == pytest.approx(449_000.0)

    def test_last_settle_price_updated_after_mtm(self):
        p = _futures_portfolio(500_000, multiplier=10)
        p.process_fill_event(_make_fill('IF2401', 'BUY', 1, 5000.0, commission=0.0))
        me = _make_market_event(dt=datetime(2024, 1, 3))
        p.update_timeindex(market_event=me, current_prices={'IF2401': 5100.0})
        assert p.current_holdings['IF2401']['last_settle_price'] == pytest.approx(5100.0)


# ---------------------------------------------------------------------------
# Futures: close long — PnL must NOT double-count MTM gains
# ---------------------------------------------------------------------------

class TestFuturesClosePnL:

    def test_realized_pnl_uses_last_settle_price(self):
        """
        Day 1: BUY 1 lot @ 5000.  MTM settles @ 5100 (cash += 1000).
        Day 2: SELL 1 lot @ 5200.
        Realized PnL must be (5200-5100)*1*10 = 1000,
        NOT (5200-5000)*1*10 = 2000 (which would double-count the settled 1000).
        """
        p = _futures_portfolio(500_000, multiplier=10)
        p.process_fill_event(_make_fill('IF2401', 'BUY', 1, 5000.0, commission=0.0,
                                        dt=datetime(2024, 1, 2)))
        # MTM at day 1 close
        me = _make_market_event(dt=datetime(2024, 1, 3))
        p.update_timeindex(market_event=me, current_prices={'IF2401': 5100.0})
        cash_after_mtm = p.current_cash  # 450000 + 1000 = 451000

        # Close on day 2
        p.process_fill_event(_make_fill('IF2401', 'SELL', 1, 5200.0, commission=0.0,
                                        dt=datetime(2024, 1, 3)))
        # released margin = 50000
        # pnl (from last_settle=5100) = (5200-5100)*1*10 = 1000
        # cash = 451000 + 50000 + 1000 = 502000
        assert p.current_cash == pytest.approx(502_000.0)
        assert p.total_realized_pnl == pytest.approx(1_000.0)

    def test_partial_close_releases_proportional_margin(self):
        """
        Hold 3 lots.  SELL 1 lot.  Must release exactly 1/3 of margin_used.
        """
        p = _futures_portfolio(500_000, multiplier=10)
        p.process_fill_event(_make_fill('IF2401', 'BUY', 3, 5000.0, commission=0.0))
        # margin_used = 3*5000*10 = 150000; cash = 350000
        me = _make_market_event(dt=datetime(2024, 1, 3))
        p.update_timeindex(market_event=me, current_prices={'IF2401': 5000.0})
        # no price change, cash stays 350000

        p.process_fill_event(_make_fill('IF2401', 'SELL', 1, 5000.0, commission=0.0,
                                        dt=datetime(2024, 1, 3)))
        # released = 150000 * (1/3) = 50000; pnl = 0
        assert p.margin_used.get('IF2401') == pytest.approx(100_000.0)
        assert p.current_cash == pytest.approx(400_000.0)

    def test_full_close_releases_all_margin(self):
        p = _futures_portfolio(500_000, multiplier=10)
        p.process_fill_event(_make_fill('IF2401', 'BUY', 2, 5000.0, commission=0.0))
        me = _make_market_event(dt=datetime(2024, 1, 3))
        p.update_timeindex(market_event=me, current_prices={'IF2401': 5000.0})
        p.process_fill_event(_make_fill('IF2401', 'SELL', 2, 5000.0, commission=0.0,
                                        dt=datetime(2024, 1, 3)))
        assert p.margin_used.get('IF2401', 0) == pytest.approx(0.0)
        assert p.current_cash == pytest.approx(500_000.0)


# ---------------------------------------------------------------------------
# Futures: reversal — long → short
# ---------------------------------------------------------------------------

class TestFuturesReversal:

    def test_reversal_releases_full_long_margin(self):
        """
        Long 2 lots.  SELL 3 lots → reverses to short 1 lot.
        The full long margin (2 lots) must be released; new short margin (1 lot) deducted.
        """
        p = _futures_portfolio(500_000, multiplier=10)
        p.process_fill_event(_make_fill('IF2401', 'BUY', 2, 5000.0, commission=0.0))
        # margin_used = 100000; cash = 400000
        me = _make_market_event(dt=datetime(2024, 1, 3))
        p.update_timeindex(market_event=me, current_prices={'IF2401': 5000.0})

        p.process_fill_event(_make_fill('IF2401', 'SELL', 3, 5000.0, commission=0.0,
                                        dt=datetime(2024, 1, 3)))
        # close 2 long: released = 100000, pnl = 0 → cash = 400000 + 100000 = 500000
        # open short 1: new_margin = 50000 → cash = 450000
        assert p.current_holdings['IF2401']['quantity'] == -1
        assert p.margin_used.get('IF2401') == pytest.approx(50_000.0)
        assert p.current_cash == pytest.approx(450_000.0)
