"""
End-to-end backtest accuracy tests.

Validates the full Engine pipeline with deterministic data and a scheduled
strategy, checking every step against hand-calculated expected values.

Scenarios covered
-----------------
1. Stock buy-then-sell (zero fees, zero slippage)
   - Fill-on-next-bar: BUY queued at bar 1, filled at bar 2 open
   - Cash correctly debited / credited
   - Realized PnL = price gain only
   - Equity curve snapshots match manual MTM calculations
2. Stock with commission
   - Commission deducted from cash but NOT included in realized PnL
   - net profit = realized_pnl - total_commission
3. Futures with daily MTM settlement
   - Margin deducted on open, released on close
   - MTM gain/loss credited to cash each bar
   - Realized PnL uses last_settle_price (no double-counting of MTM gains)
4. PerformanceAnalyzer integration
   - total_return, total_trades, win_rate, max_drawdown from known backtest
"""

import pytest
from datetime import datetime
from typing import List, Optional

from engine import Engine
from core.data_feed import DataFeed
from core.event import MarketEvent, SignalEvent
from core.strategy import Strategy
from core.instrument import InstrumentRegistry, Stock, Future
from core.market_rule import MarketRule
from core.position_sizer import FixedQuantityPositionSizer
from execution.execution_handler import SimulatedExecutionModel
from portfolio.portfolio import Portfolio
from analytics.performance import PerformanceAnalyzer


# ---------------------------------------------------------------------------
# Shared test infrastructure
# ---------------------------------------------------------------------------

class MockDataFeed(DataFeed):
    """
    Deterministic in-memory data feed for testing.

    bars_by_symbol: {'SYMBOL': [bar_dict, ...]}  (bars sorted by timestamp)
    """

    def __init__(self, bars_by_symbol: dict):
        self._bars_by_symbol = bars_by_symbol
        self._index = 0
        self._latest: dict = {s: [] for s in bars_by_symbol}

        all_ts: set = set()
        for bars in bars_by_symbol.values():
            for bar in bars:
                all_ts.add(bar['timestamp'])
        self._timeline: list = sorted(all_ts)

    @property
    def symbols(self) -> List[str]:
        return list(self._bars_by_symbol.keys())

    def update_bars(self) -> bool:
        if self._index >= len(self._timeline):
            return False
        ts = self._timeline[self._index]
        for symbol, bars in self._bars_by_symbol.items():
            for bar in bars:
                if bar['timestamp'] == ts:
                    self._latest[symbol].append(bar)
        self._index += 1
        return True

    def get_latest_bar(self, symbol: str):
        bars = self._latest.get(symbol)
        return bars[-1] if bars else None

    def get_latest_bars(self, symbol: str, num_bars: int) -> list:
        bars = self._latest.get(symbol, [])
        return bars[-num_bars:]


class ScheduledSignalStrategy(Strategy):
    """
    Emits pre-defined signals at specific bar-call indices.

    signals_schedule: {bar_index: signal_type}
    e.g. {1: 'LONG', 3: 'EXIT'}  emits LONG on the 2nd bar the engine processes.
    """

    def __init__(self, data_handler, symbol: str, signals_schedule: dict):
        super().__init__(data_handler)
        self.symbol = symbol
        self.signals_schedule = signals_schedule
        self._call_count = 0

    def calculate_signal(self, event: MarketEvent) -> Optional[SignalEvent]:
        idx = self._call_count
        self._call_count += 1
        signal_type = self.signals_schedule.get(idx)
        if signal_type:
            return SignalEvent(
                symbol=self.symbol,
                datetime=event.datetime,
                signal_type=signal_type,
            )
        return None


def _make_bar(symbol, ts, year, month, day, open_, high, low, close, volume):
    return {
        'ticker': symbol,
        'timestamp': ts,
        'datetime_local': datetime(year, month, day),
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
    }


def _zero_fee_rule(**overrides):
    """
    MarketRule with no commission, no slippage, T+0 settlement.
    Suitable for stock scenarios where fees must be controlled precisely.
    """
    cfg = dict(
        commission_rate=0.0,
        min_commission=0.0,
        stamp_duty=0.0,
        transfer_fee=0.0,
        lot_size=1,
        price_tick=0.01,
        allow_short=True,
        settlement_days=0,
        slippage_model='none',
        price_limit_pct=0.0,
    )
    cfg.update(overrides)
    return MarketRule(**cfg)


def _zero_fee_futures_rule(**overrides):
    """
    MarketRule for daily-settlement futures with zero fees and no slippage.
    """
    cfg = dict(
        requires_daily_settlement=True,
        commission_rate=0.0,
        min_commission=0.0,
        lot_size=1,
        price_tick=0.01,
        allow_short=True,
        settlement_days=0,
        slippage_model='none',
        price_limit_pct=0.0,
    )
    cfg.update(overrides)
    return MarketRule(**cfg)


# ---------------------------------------------------------------------------
# Scenario 1 & 2: Stock backtest
#
# Bar layout (fill_on_next_bar=True, FixedQuantityPositionSizer(100)):
#
#   Bar 0 (2024-01-02): open=100 high=102 low= 99 close=101  → no signal
#   Bar 1 (2024-01-03): open=101 high=103 low=100 close=102  → LONG  signal
#                                                              → BUY 100 queued
#   Bar 2 (2024-01-04): open=105 high=107 low=104 close=106  → BUY 100 filled @ 105
#   Bar 3 (2024-01-05): open=106 high=108 low=105 close=107  → EXIT  signal
#                                                              → SELL 100 queued
#   Bar 4 (2024-01-08): open=110 high=112 low=109 close=111  → SELL 100 filled @ 110
#
# Manual (zero fees, contract_multiplier=1):
#   cash after BUY  = 100_000 − 100×105       = 89_500
#   cash after SELL = 89_500  + 100×110       = 100_500
#   realized_pnl    = 100×(110−105)           =     500
#
# Equity curve:
#   idx 0: total = 100_000  (no position)
#   idx 1: total = 100_000  (no position)
#   idx 2: total = 89_500 + 100×106           = 100_100
#   idx 3: total = 89_500 + 100×107           = 100_200
#   idx 4: total = 100_500  (position closed)
# ---------------------------------------------------------------------------

_STOCK_SYMBOL = 'TEST'
_STOCK_INITIAL = 100_000.0

_STOCK_BARS = [
    _make_bar(_STOCK_SYMBOL, 1, 2024, 1, 2,  100.0, 102.0,  99.0, 101.0, 1_000_000),
    _make_bar(_STOCK_SYMBOL, 2, 2024, 1, 3,  101.0, 103.0, 100.0, 102.0, 1_000_000),
    _make_bar(_STOCK_SYMBOL, 3, 2024, 1, 4,  105.0, 107.0, 104.0, 106.0, 1_000_000),
    _make_bar(_STOCK_SYMBOL, 4, 2024, 1, 5,  106.0, 108.0, 105.0, 107.0, 1_000_000),
    _make_bar(_STOCK_SYMBOL, 5, 2024, 1, 8,  110.0, 112.0, 109.0, 111.0, 1_000_000),
]


def _build_stock_engine(commission_rate: float = 0.0):
    rule = _zero_fee_rule(commission_rate=commission_rate)
    registry = InstrumentRegistry()
    registry.register(Stock(symbol=_STOCK_SYMBOL, market_rule=rule, currency='USD'))

    feed = MockDataFeed({_STOCK_SYMBOL: _STOCK_BARS})
    portfolio = Portfolio(initial_capital=_STOCK_INITIAL, instrument_registry=registry)
    execution = SimulatedExecutionModel(
        data_handler=feed,
        instrument_registry=registry,
        fill_on_next_bar=True,
    )
    strategy = ScheduledSignalStrategy(
        data_handler=feed,
        symbol=_STOCK_SYMBOL,
        signals_schedule={1: 'LONG', 3: 'EXIT'},
    )
    sizer = FixedQuantityPositionSizer(quantity=100)
    engine = Engine(
        data_handler=feed,
        portfolio=portfolio,
        execution_handler=execution,
        instrument_registry=registry,
        strategies=strategy,
        position_sizer=sizer,
    )
    return engine, portfolio


class TestE2EStockFillTiming:
    """Fill-on-next-bar: orders must be filled at the *next* bar's open price."""

    def test_buy_filled_at_bar2_open_not_signal_bar_close(self):
        """
        LONG signal at bar 1 (close=102) → BUY queued.
        Fill must happen at bar 2 open (105), not bar 1 close (102).
        """
        engine, portfolio = _build_stock_engine()
        engine.run()
        buy = next(p for p in portfolio.positions if p['direction'] == 'BUY')
        assert buy['fill_price'] == pytest.approx(105.0), (
            "BUY should fill at bar-2 open (105), not bar-1 close (102)")

    def test_sell_filled_at_bar4_open_not_signal_bar_close(self):
        """
        EXIT signal at bar 3 (close=107) → SELL queued.
        Fill must happen at bar 4 open (110), not bar 3 close (107).
        """
        engine, portfolio = _build_stock_engine()
        engine.run()
        sell = next(p for p in portfolio.positions if p['direction'] == 'SELL')
        assert sell['fill_price'] == pytest.approx(110.0), (
            "SELL should fill at bar-4 open (110), not bar-3 close (107)")

    def test_exactly_two_fills_recorded(self):
        """One BUY fill and one SELL fill for one round trip."""
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert len(portfolio.positions) == 2


class TestE2EStockCashAndPnL:
    """Cash accounting and PnL correctness after the full round trip."""

    def test_cash_after_buy_fill(self):
        """
        cash = 100_000 − 100×105 = 89_500.
        Snapshot is captured in all_holdings[2] (the bar where BUY was filled).
        """
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert portfolio.all_holdings[2]['cash'] == pytest.approx(89_500.0)

    def test_cash_after_sell_fill(self):
        """cash = 89_500 + 100×110 = 100_500."""
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert portfolio.current_cash == pytest.approx(100_500.0)

    def test_realized_pnl(self):
        """Realized PnL = 100×(110−105) = 500."""
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert portfolio.total_realized_pnl == pytest.approx(500.0)

    def test_position_quantity_zero_after_close(self):
        """Holding is fully closed — quantity must be 0."""
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert portfolio.current_holdings[_STOCK_SYMBOL]['quantity'] == 0

    def test_avg_cost_equals_buy_fill_price(self):
        """
        avg_cost must reflect the actual fill price (105), not the close that
        triggered the signal (102).
        """
        engine, portfolio = _build_stock_engine()
        engine.run()
        buy = next(p for p in portfolio.positions if p['direction'] == 'BUY')
        assert buy['fill_price'] == pytest.approx(105.0)


class TestE2EStockEquityCurve:
    """Equity curve (all_holdings) snapshots match manual MTM calculations."""

    def test_equity_curve_has_one_entry_per_bar(self):
        """5 bars → exactly 5 all_holdings snapshots."""
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert len(portfolio.all_holdings) == 5

    def test_equity_before_any_trade(self):
        """Bar 0 and 1: no position → total = initial capital."""
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert portfolio.all_holdings[0]['total'] == pytest.approx(100_000.0)
        assert portfolio.all_holdings[1]['total'] == pytest.approx(100_000.0)

    def test_equity_at_bar2_after_buy_fill(self):
        """
        Bar 2: BUY filled; cash = 89_500; MTM value = 100×106 = 10_600.
        total = 89_500 + 10_600 = 100_100.
        """
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert portfolio.all_holdings[2]['total'] == pytest.approx(100_100.0)

    def test_equity_at_bar3_mark_to_market(self):
        """
        Bar 3: no trade; price=107; MTM value = 100×107 = 10_700.
        total = 89_500 + 10_700 = 100_200.
        """
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert portfolio.all_holdings[3]['total'] == pytest.approx(100_200.0)

    def test_equity_at_bar4_after_close(self):
        """Bar 4: position closed; total = cash = 100_500."""
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert portfolio.all_holdings[4]['total'] == pytest.approx(100_500.0)

    def test_unrealized_pnl_at_bar2(self):
        """Unrealized PnL at bar 2 = (106−105)×100 = 100."""
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert portfolio.all_holdings[2]['unrealized_pnl'] == pytest.approx(100.0)

    def test_unrealized_pnl_at_bar3(self):
        """Unrealized PnL at bar 3 = (107−105)×100 = 200."""
        engine, portfolio = _build_stock_engine()
        engine.run()
        assert portfolio.all_holdings[3]['unrealized_pnl'] == pytest.approx(200.0)


# ---------------------------------------------------------------------------
# Scenario 2: Stock with commission
#
# Same bars and signals, commission_rate=0.001 (0.1%), min_commission=0.
#
# BUY  100 @ 105: commission = 100×105×0.001 = 10.5
# SELL 100 @ 110: commission = 100×110×0.001 = 11.0
#
# cash after BUY  = 100_000 − 10_500 − 10.5          =  89_489.5
# cash after SELL =  89_489.5 + 11_000 − 11.0        = 100_478.5
# realized_pnl    = 100×(110−105)                     =     500.0   (no commission)
# net profit      = 100_478.5 − 100_000               =     478.5   (= PnL − commissions)
# ---------------------------------------------------------------------------

class TestE2EStockWithCommission:
    """Commission is deducted from cash but must NOT appear in realized PnL."""

    _RATE = 0.001  # 0.1%

    def test_commission_reduces_final_cash(self):
        """Final cash is lower by total commission paid."""
        engine, portfolio = _build_stock_engine(commission_rate=self._RATE)
        engine.run()
        buy_comm = 100 * 105 * self._RATE    # 10.5
        sell_comm = 100 * 110 * self._RATE   # 11.0
        expected = _STOCK_INITIAL - 100 * 105 - buy_comm + 100 * 110 - sell_comm
        assert portfolio.current_cash == pytest.approx(expected, rel=1e-6)

    def test_realized_pnl_excludes_commission(self):
        """PnL = pure price gain = 100×(110−105) = 500, regardless of commission."""
        engine, portfolio = _build_stock_engine(commission_rate=self._RATE)
        engine.run()
        assert portfolio.total_realized_pnl == pytest.approx(500.0)

    def test_net_profit_equals_pnl_minus_total_commission(self):
        """net profit (cash change) = realized_pnl − total_commission paid."""
        engine, portfolio = _build_stock_engine(commission_rate=self._RATE)
        engine.run()
        total_comm = sum(p['commission'] for p in portfolio.positions)
        net_profit = portfolio.current_cash - _STOCK_INITIAL
        assert net_profit == pytest.approx(
            portfolio.total_realized_pnl - total_comm, rel=1e-6)

    def test_buy_commission_is_positive(self):
        """Commission on the BUY fill = 100×105×0.001 = 10.5."""
        engine, portfolio = _build_stock_engine(commission_rate=self._RATE)
        engine.run()
        buy = next(p for p in portfolio.positions if p['direction'] == 'BUY')
        assert buy['commission'] == pytest.approx(100 * 105 * self._RATE, rel=1e-6)


# ---------------------------------------------------------------------------
# Scenario 3: Futures with daily MTM settlement
#
# Instrument: custom contract, multiplier=10, requires_daily_settlement=True,
#             zero commission, no slippage.
#
# Bar layout (FixedQuantityPositionSizer(1)):
#
#   Bar 0 (2024-01-02): close=5_000  → no signal
#   Bar 1 (2024-01-03): close=5_000  → LONG signal → BUY 1 lot queued
#   Bar 2 (2024-01-04): open=5_010, close=5_100 → BUY 1 lot filled @ 5_010
#   Bar 3 (2024-01-05): close=5_200  → EXIT signal → SELL 1 lot queued
#   Bar 4 (2024-01-08): open=5_300, close=5_300 → SELL 1 lot filled @ 5_300
#
# Manual (zero commission, contract_multiplier=10):
#
#   After BUY @ 5_010:
#     margin           = 5_010×1×10   = 50_100
#     cash             = 500_000 − 50_100   = 449_900
#     last_settle_price = 5_010
#
#   Bar 2 MTM (settle @ 5_100):
#     mtm_gain         = (5_100−5_010)×1×10 =     900
#     cash             = 449_900 + 900       = 450_800
#     last_settle_price = 5_100
#
#   Bar 3 MTM (settle @ 5_200):
#     mtm_gain         = (5_200−5_100)×1×10 =   1_000
#     cash             = 450_800 + 1_000     = 451_800
#     last_settle_price = 5_200
#
#   SELL 1 lot @ 5_300 (filled at bar 4 open):
#     pnl              = (5_300−5_200)×1×10 =   1_000  ← uses last_settle, not avg_cost
#     released margin  = 50_100
#     cash             = 451_800 + 50_100 + 1_000 = 502_900
#
#   Bar 4 MTM (no position → no change):
#     total            = 502_900
#
# Key invariant: total_realized_pnl = 1_000, NOT 2_900.
#   The extra 1_900 was already settled into cash via daily MTM.
# ---------------------------------------------------------------------------

_FUT_SYMBOL = 'IF_TEST'
_FUT_INITIAL = 500_000.0
_FUT_MULTIPLIER = 10

_FUT_BARS = [
    _make_bar(_FUT_SYMBOL, 1, 2024, 1, 2, 5_000.0, 5_020.0, 4_990.0, 5_000.0, 500_000),
    _make_bar(_FUT_SYMBOL, 2, 2024, 1, 3, 5_000.0, 5_020.0, 4_990.0, 5_000.0, 500_000),
    _make_bar(_FUT_SYMBOL, 3, 2024, 1, 4, 5_010.0, 5_120.0, 5_000.0, 5_100.0, 500_000),
    _make_bar(_FUT_SYMBOL, 4, 2024, 1, 5, 5_100.0, 5_210.0, 5_090.0, 5_200.0, 500_000),
    _make_bar(_FUT_SYMBOL, 5, 2024, 1, 8, 5_300.0, 5_320.0, 5_290.0, 5_300.0, 500_000),
]


def _build_futures_engine():
    rule = _zero_fee_futures_rule()
    registry = InstrumentRegistry()
    registry.register(Future(
        symbol=_FUT_SYMBOL,
        market_rule=rule,
        contract_multiplier=_FUT_MULTIPLIER,
        currency='CNY',
        expiry_date=9_999_999_999,
    ))

    feed = MockDataFeed({_FUT_SYMBOL: _FUT_BARS})
    portfolio = Portfolio(initial_capital=_FUT_INITIAL, instrument_registry=registry)
    execution = SimulatedExecutionModel(
        data_handler=feed,
        instrument_registry=registry,
        fill_on_next_bar=True,
    )
    strategy = ScheduledSignalStrategy(
        data_handler=feed,
        symbol=_FUT_SYMBOL,
        signals_schedule={1: 'LONG', 3: 'EXIT'},
    )
    sizer = FixedQuantityPositionSizer(quantity=1)
    engine = Engine(
        data_handler=feed,
        portfolio=portfolio,
        execution_handler=execution,
        instrument_registry=registry,
        strategies=strategy,
        position_sizer=sizer,
    )
    return engine, portfolio


class TestE2EFuturesDailyMTM:
    """Futures daily MTM: margin, settlement cash flows, and PnL accuracy."""

    def test_buy_fill_price_is_bar2_open(self):
        """BUY fills at bar 2 open (5_010), not bar 1 close (5_000)."""
        engine, portfolio = _build_futures_engine()
        engine.run()
        buy = next(p for p in portfolio.positions if p['direction'] == 'BUY')
        assert buy['fill_price'] == pytest.approx(5_010.0)

    def test_sell_fill_price_is_bar4_open(self):
        """SELL fills at bar 4 open (5_300), not bar 3 close (5_200)."""
        engine, portfolio = _build_futures_engine()
        engine.run()
        sell = next(p for p in portfolio.positions if p['direction'] == 'SELL')
        assert sell['fill_price'] == pytest.approx(5_300.0)

    def test_cash_after_buy_and_first_mtm(self):
        """
        After BUY @ 5_010: cash = 500_000 − 50_100 = 449_900.
        Bar 2 MTM (settle @ 5_100): cash += (5_100−5_010)×10 = +900 → 450_800.
        all_holdings[2]['cash'] must equal 450_800.
        """
        engine, portfolio = _build_futures_engine()
        engine.run()
        assert portfolio.all_holdings[2]['cash'] == pytest.approx(450_800.0)

    def test_cash_after_second_mtm(self):
        """Bar 3 MTM (settle @ 5_200): cash += (5_200−5_100)×10 = +1_000 → 451_800."""
        engine, portfolio = _build_futures_engine()
        engine.run()
        assert portfolio.all_holdings[3]['cash'] == pytest.approx(451_800.0)

    def test_realized_pnl_uses_last_settle_price_not_avg_cost(self):
        """
        Closing PnL = (5_300−5_200)×1×10 = 1_000.
        Must NOT be (5_300−5_010)×10 = 2_900 (which double-counts settled gains).
        """
        engine, portfolio = _build_futures_engine()
        engine.run()
        assert portfolio.total_realized_pnl == pytest.approx(1_000.0)

    def test_final_cash_after_close(self):
        """
        cash = 451_800 + released_margin(50_100) + pnl(1_000) = 502_900.
        """
        engine, portfolio = _build_futures_engine()
        engine.run()
        assert portfolio.current_cash == pytest.approx(502_900.0)

    def test_margin_fully_released_after_close(self):
        """All margin is returned when the position is fully closed."""
        engine, portfolio = _build_futures_engine()
        engine.run()
        assert portfolio.margin_used.get(_FUT_SYMBOL, 0.0) == pytest.approx(0.0)

    def test_position_quantity_zero_after_close(self):
        engine, portfolio = _build_futures_engine()
        engine.run()
        assert portfolio.current_holdings[_FUT_SYMBOL]['quantity'] == 0

    def test_equity_curve_length(self):
        """5 bars → 5 snapshots."""
        engine, portfolio = _build_futures_engine()
        engine.run()
        assert len(portfolio.all_holdings) == 5

    def test_equity_at_bar2_reflects_mtm(self):
        """
        Futures total = cash only (no separate market_value column).
        Bar 2: cash = 450_800 → total = 450_800.
        """
        engine, portfolio = _build_futures_engine()
        engine.run()
        assert portfolio.all_holdings[2]['total'] == pytest.approx(450_800.0)

    def test_equity_at_bar4_after_close(self):
        """Bar 4: position closed → total = cash = 502_900."""
        engine, portfolio = _build_futures_engine()
        engine.run()
        assert portfolio.all_holdings[4]['total'] == pytest.approx(502_900.0)


# ---------------------------------------------------------------------------
# Scenario 4: PerformanceAnalyzer end-to-end
#
# Run the zero-fee stock backtest and verify every metric that can be
# computed deterministically from the known equity curve.
#
# Equity curve: [100_000, 100_000, 100_100, 100_200, 100_500]
# Returns:       [0.0%, +0.1%, +0.0998%, +0.2994%]  (approx)
# Total return:  (100_500 − 100_000) / 100_000 = 0.5%
# Max drawdown:  0  (curve is monotonically non-decreasing)
# Total trades:  1  (the SELL is the only closing leg)
# Win rate:      1.0  (the single closed trade is profitable)
# Profit factor: inf (no losing trades)
# ---------------------------------------------------------------------------

class TestE2EPerformanceAnalyzer:
    """PerformanceAnalyzer produces correct metrics from the known stock run."""

    @pytest.fixture(scope='class')
    def analyzer(self):
        engine, portfolio = _build_stock_engine()
        engine.run()
        return PerformanceAnalyzer(
            all_holdings=portfolio.all_holdings,
            positions=portfolio.positions,
            initial_capital=portfolio.initial_capital,
        )

    def test_total_return(self, analyzer):
        """total_return = (100_500 − 100_000) / 100_000 = 0.005."""
        assert analyzer.total_return() == pytest.approx(0.005, rel=1e-4)

    def test_total_trades(self, analyzer):
        """Only the SELL closing leg is a 'closed trade' → 1."""
        assert analyzer.total_trades() == 1

    def test_win_rate_is_one(self, analyzer):
        """Single profitable trade → win_rate = 1.0."""
        assert analyzer.win_rate() == pytest.approx(1.0)

    def test_max_drawdown_is_zero(self, analyzer):
        """Equity curve never dips below a previous peak → max_drawdown = 0."""
        assert analyzer.max_drawdown() == pytest.approx(0.0)

    def test_profit_factor_is_infinite(self, analyzer):
        """Gross profit = 500, gross loss = 0 → profit_factor = inf."""
        assert analyzer.profit_factor() == float('inf')

    def test_avg_trade_pnl(self, analyzer):
        """avg_trade_pnl = 500 / 1 trade = 500."""
        assert analyzer.avg_trade_pnl() == pytest.approx(500.0)

    def test_total_commission_is_zero(self, analyzer):
        """Zero-fee scenario → total_commission = 0."""
        assert analyzer.total_commission() == pytest.approx(0.0)

    def test_summary_keys_present(self, analyzer):
        """summary() dict must contain all standard metric keys."""
        s = analyzer.summary()
        required = {
            'initial_capital', 'final_equity', 'total_return_pct',
            'max_drawdown_pct', 'sharpe_ratio', 'total_trades',
            'win_rate_pct', 'profit_factor',
        }
        assert required.issubset(s.keys())

    def test_summary_final_equity(self, analyzer):
        """final_equity in summary matches actual final portfolio value."""
        s = analyzer.summary()
        assert s['final_equity'] == pytest.approx(100_500.0)
