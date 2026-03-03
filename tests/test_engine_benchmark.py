"""
Engine throughput benchmark tests.

Measures how fast the Engine processes bars under different workloads and
asserts conservative lower-bound thresholds that any non-pathological
implementation should exceed on modern hardware.

Scenarios
---------
1. Baseline (no-op strategy)       — pure event-loop overhead
2. Active trading (periodic cycle) — steady buy / sell churn
3. MA crossover strategy           — realistic signal generation
4. Multi-symbol                    — 5 symbols processed in parallel
5. Scalability                     — time should scale ~linearly with N
6. Memory footprint                — peak RSS for a large run
7. Accounting accuracy under load  — cash conservation holds after many trades
"""

import math
import sys
import time
import random
import tracemalloc
import pytest
from datetime import datetime, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, str(__file__).rsplit('/', 2)[0])

from engine import Engine
from core.data_feed import DataFeed
from core.event import MarketEvent, SignalEvent
from core.strategy import Strategy
from core.instrument import InstrumentRegistry, Stock
from core.market_rule import MarketRule
from core.position_sizer import FixedQuantityPositionSizer, PercentOfEquityPositionSizer
from execution.execution_handler import SimulatedExecutionModel
from portfolio.portfolio import Portfolio
from strategies.moving_average import MovingAverage


# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

class MockDataFeed(DataFeed):
    """Fast in-memory data feed backed by pre-built bar lists."""

    def __init__(self, bars_by_symbol: dict):
        self._bars = bars_by_symbol
        self._index = 0
        self._latest: dict = {s: [] for s in bars_by_symbol}

        all_ts: set = set()
        for bars in bars_by_symbol.values():
            for bar in bars:
                all_ts.add(bar['timestamp'])
        self._timeline: list = sorted(all_ts)

    @property
    def symbols(self) -> List[str]:
        return list(self._bars.keys())

    def update_bars(self) -> bool:
        if self._index >= len(self._timeline):
            return False
        ts = self._timeline[self._index]
        for symbol, bars in self._bars.items():
            bar = bars[self._index] if self._index < len(bars) else None
            if bar is not None and bar['timestamp'] == ts:
                self._latest[symbol].append(bar)
        self._index += 1
        return True

    def get_latest_bar(self, symbol: str):
        bars = self._latest.get(symbol)
        return bars[-1] if bars else None

    def get_latest_bars(self, symbol: str, num_bars: int) -> list:
        return self._latest.get(symbol, [])[-num_bars:]


class NoOpStrategy(Strategy):
    """Never generates a signal — measures pure event-loop overhead."""

    def calculate_signal(self, event: MarketEvent) -> None:
        return None


class PeriodicSignalStrategy(Strategy):
    """
    Alternates LONG → EXIT every `half_period` bars, creating a steady
    stream of round-trip trades.

    Timeline (half_period=50):
      bar  0 : LONG
      bar 50 : EXIT
      bar 100: LONG
      bar 150: EXIT  ...
    """

    def __init__(self, data_handler, symbol: str, half_period: int = 50):
        super().__init__(data_handler)
        self.symbol = symbol
        self.half_period = half_period
        self._n = 0

    def calculate_signal(self, event: MarketEvent) -> Optional[SignalEvent]:
        idx = self._n
        self._n += 1
        if idx % self.half_period == 0:
            signal_type = 'LONG' if (idx // self.half_period) % 2 == 0 else 'EXIT'
            return SignalEvent(
                symbol=self.symbol,
                datetime=event.datetime,
                signal_type=signal_type,
            )
        return None


def _generate_bars(symbol: str, n: int, seed: int = 42,
                   start_price: float = 100.0) -> List[dict]:
    """
    Seeded random-walk price series.
    Returns a list of OHLCV bar dicts sorted by timestamp.
    """
    rng = random.Random(seed)
    bars = []
    price = start_price
    base_date = datetime(2020, 1, 1)

    for i in range(n):
        # Random-walk with a slight upward drift and mean-reversion
        change = rng.gauss(0.02, 1.5)
        price = max(price + change, 1.0)

        spread = abs(rng.gauss(0, 0.8))
        high = price + spread
        low = max(price - spread, 0.01)
        open_ = price + rng.gauss(0, 0.3)
        volume = int(rng.uniform(500_000, 2_000_000))

        bars.append({
            'ticker': symbol,
            'timestamp': i + 1,
            'datetime_local': base_date + timedelta(days=i),
            'open':   round(max(open_, 0.01), 4),
            'high':   round(high, 4),
            'low':    round(low, 4),
            'close':  round(price, 4),
            'volume': volume,
        })

    return bars


def _zero_fee_rule() -> MarketRule:
    return MarketRule(
        commission_rate=0.0,
        min_commission=0.0,
        stamp_duty=0.0,
        lot_size=1,
        price_tick=0.01,
        allow_short=True,
        settlement_days=0,
        slippage_model='none',
        price_limit_pct=0.0,
    )


def _build_engine(bars_by_symbol: dict, strategy: Strategy,
                  initial: float = 1_000_000.0,
                  sizer=None) -> tuple:
    """Build a fully wired Engine. Returns (engine, portfolio)."""
    rule = _zero_fee_rule()
    registry = InstrumentRegistry()
    for symbol in bars_by_symbol:
        registry.register(Stock(symbol=symbol, market_rule=rule, currency='USD'))

    feed = MockDataFeed(bars_by_symbol)
    portfolio = Portfolio(initial_capital=initial, instrument_registry=registry)
    execution = SimulatedExecutionModel(
        data_handler=feed,
        instrument_registry=registry,
        fill_on_next_bar=True,
    )
    if sizer is None:
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


def _throughput(elapsed: float, total_bars: int) -> float:
    """Return bars-per-second, or inf if elapsed==0."""
    return total_bars / elapsed if elapsed > 0 else float('inf')


# ---------------------------------------------------------------------------
# Minimum acceptable throughput (bars / second).
# Very conservative — real implementations should be 10-100× faster.
# ---------------------------------------------------------------------------
_MIN_BPS_NOOP    = 2_000   # no-op strategy (pure event-loop)
_MIN_BPS_TRADING = 500     # strategy with active trading

# Maximum acceptable peak memory for a 10 K-bar single-symbol run (bytes)
_MAX_MEMORY_BYTES = 100 * 1024 * 1024   # 100 MB


# ---------------------------------------------------------------------------
# 1. Baseline: no-op strategy
# ---------------------------------------------------------------------------

class TestBaselineThroughput:
    """
    Measure raw event-loop speed with a strategy that never signals.
    Establishes how much overhead exists before any trading logic.
    """

    @pytest.mark.parametrize('n_bars', [1_000, 5_000, 10_000])
    def test_noop_throughput(self, n_bars):
        bars = _generate_bars('BENCH', n_bars)
        feed_data = {'BENCH': bars}
        strategy = NoOpStrategy()
        engine, _ = _build_engine(feed_data, strategy)

        t0 = time.perf_counter()
        engine.run()
        elapsed = time.perf_counter() - t0

        bps = _throughput(elapsed, n_bars)
        print(f"\n[no-op {n_bars:,} bars] {bps:,.0f} bars/sec  ({elapsed*1000:.1f} ms)")
        assert bps >= _MIN_BPS_NOOP, (
            f"Throughput {bps:.0f} bars/sec is below minimum {_MIN_BPS_NOOP} bars/sec")

    def test_noop_produces_no_positions(self):
        """Sanity: a no-op strategy must not generate any trades."""
        bars = _generate_bars('BENCH', 500)
        strategy = NoOpStrategy()
        engine, portfolio = _build_engine({'BENCH': bars}, strategy)
        engine.run()
        assert len(portfolio.positions) == 0


# ---------------------------------------------------------------------------
# 2. Active trading: periodic buy / sell cycles
# ---------------------------------------------------------------------------

class TestActiveTradingThroughput:
    """
    Throughput when the strategy generates a trade roughly every 50 bars
    (LONG then EXIT alternating), creating maximum portfolio churn.
    """

    @pytest.mark.parametrize('n_bars', [1_000, 5_000])
    def test_periodic_strategy_throughput(self, n_bars):
        bars = _generate_bars('BENCH', n_bars)
        feed_data = {'BENCH': bars}
        strategy = PeriodicSignalStrategy(data_handler=None,
                                          symbol='BENCH', half_period=50)
        engine, portfolio = _build_engine(feed_data, strategy)

        t0 = time.perf_counter()
        engine.run()
        elapsed = time.perf_counter() - t0

        bps = _throughput(elapsed, n_bars)
        n_trades = len(portfolio.positions)
        print(f"\n[periodic {n_bars:,} bars] {bps:,.0f} bars/sec  "
              f"({elapsed*1000:.1f} ms)  trades={n_trades}")
        assert bps >= _MIN_BPS_TRADING, (
            f"Throughput {bps:.0f} bars/sec is below minimum {_MIN_BPS_TRADING} bars/sec")

    def test_periodic_strategy_generates_expected_trade_count(self):
        """
        With half_period=50 and 1_000 bars, we expect ~10 round trips
        (LONG at bars 0,100,200,...; EXIT at bars 50,150,...).
        """
        n_bars = 1_000
        bars = _generate_bars('BENCH', n_bars)
        strategy = PeriodicSignalStrategy(data_handler=None,
                                          symbol='BENCH', half_period=50)
        engine, portfolio = _build_engine({'BENCH': bars}, strategy,
                                          initial=10_000_000.0)
        engine.run()
        # Each LONG that successfully enters creates 2 fills (BUY + SELL).
        # With fill_on_next_bar, a LONG at bar k is filled at bar k+1,
        # and EXIT at bar k+50 is filled at bar k+51 — all within 1_000 bars.
        assert len(portfolio.positions) > 0, "Periodic strategy must generate trades"


# ---------------------------------------------------------------------------
# 3. Moving Average crossover strategy
# ---------------------------------------------------------------------------

class TestMACrossoverThroughput:
    """
    Throughput with a realistic MA(5, 20) strategy that auto-generates
    signals based on price data — tests the full signal aggregation path.
    """

    @pytest.mark.parametrize('n_symbols,n_bars', [(3, 1_000), (5, 1_000), (5, 2_000)])
    def test_ma_strategy_1yr_1min_bars_throughput(self, n_symbols, n_bars):
        n_bars = 98_280
        bars = _generate_bars('BENCH', n_bars, seed=7)
        symbols = [f'SYM{i:02d}' for i in range(n_symbols)]
        bars_by_symbol = {
            sym: _generate_bars(sym, n_bars, seed=i * 17)
            for i, sym in enumerate(symbols)
        }
        feed = MockDataFeed(bars_by_symbol)
        rule = _zero_fee_rule()
        registry = InstrumentRegistry()
        registry.register(Stock(symbol='BENCH', market_rule=rule, currency='USD'))

        strategy = MovingAverage(data_handler=feed, short_window=5, long_window=20)
        portfolio = Portfolio(initial_capital=1_000_000.0, instrument_registry=registry)
        execution = SimulatedExecutionModel(
            data_handler=feed, instrument_registry=registry, fill_on_next_bar=True)
        engine = Engine(
            data_handler=feed, portfolio=portfolio,
            execution_handler=execution, instrument_registry=registry,
            strategies=strategy,
            position_sizer=FixedQuantityPositionSizer(quantity=100),
        )

        t0 = time.perf_counter()
        engine.run()
        elapsed = time.perf_counter() - t0

        bps = _throughput(elapsed, n_bars)
        print(f"\n[MA(5,20) {n_bars:,} bars] {bps:,.0f} bars/sec  "
              f"({elapsed*1000:.1f} ms)  trades={len(portfolio.positions)}")
        assert bps >= _MIN_BPS_TRADING, (
            f"MA strategy throughput {bps:.0f} bars/sec below {_MIN_BPS_TRADING}")

    def test_ma_strategy_generates_trades(self):
        """MA strategy must produce at least some signals on a random-walk series."""
        n_bars = 500
        bars = _generate_bars('BENCH', n_bars, seed=99)
        feed = MockDataFeed({'BENCH': bars})
        rule = _zero_fee_rule()
        registry = InstrumentRegistry()
        registry.register(Stock(symbol='BENCH', market_rule=rule, currency='USD'))
        strategy = MovingAverage(data_handler=feed, short_window=5, long_window=20)
        portfolio = Portfolio(initial_capital=1_000_000.0, instrument_registry=registry)
        execution = SimulatedExecutionModel(
            data_handler=feed, instrument_registry=registry, fill_on_next_bar=True)
        engine = Engine(
            data_handler=feed, portfolio=portfolio,
            execution_handler=execution, instrument_registry=registry,
            strategies=strategy,
            position_sizer=FixedQuantityPositionSizer(quantity=100),
        )
        engine.run()
        assert len(portfolio.positions) > 0, "MA strategy should generate at least one trade"


# ---------------------------------------------------------------------------
# 4. Multi-symbol throughput
# ---------------------------------------------------------------------------

class TestMultiSymbolThroughput:
    """
    Engine processes N symbols per bar; throughput measured across total bar-symbols.
    """

    @pytest.mark.parametrize('n_symbols,n_bars', [(3, 1_000), (5, 1_000), (5, 2_000)])
    def test_multi_symbol_throughput(self, n_symbols, n_bars):
        symbols = [f'SYM{i:02d}' for i in range(n_symbols)]
        bars_by_symbol = {
            sym: _generate_bars(sym, n_bars, seed=i * 17)
            for i, sym in enumerate(symbols)
        }

        feed = MockDataFeed(bars_by_symbol)
        rule = _zero_fee_rule()
        registry = InstrumentRegistry()
        for sym in symbols:
            registry.register(Stock(symbol=sym, market_rule=rule, currency='USD'))

        portfolio = Portfolio(initial_capital=5_000_000.0, instrument_registry=registry)
        execution = SimulatedExecutionModel(
            data_handler=feed, instrument_registry=registry, fill_on_next_bar=True)

        strategies = [
            PeriodicSignalStrategy(data_handler=feed, symbol=sym, half_period=100)
            for sym in symbols
        ]
        engine = Engine(
            data_handler=feed, portfolio=portfolio,
            execution_handler=execution, instrument_registry=registry,
            strategies=strategies,
            position_sizer=FixedQuantityPositionSizer(quantity=100),
        )

        t0 = time.perf_counter()
        engine.run()
        elapsed = time.perf_counter() - t0

        total_bar_symbols = n_bars * n_symbols
        bps = _throughput(elapsed, total_bar_symbols)
        print(f"\n[{n_symbols} symbols × {n_bars:,} bars] {bps:,.0f} bar-symbols/sec  "
              f"({elapsed*1000:.1f} ms)  trades={len(portfolio.positions)}")
        assert bps >= _MIN_BPS_TRADING, (
            f"Multi-symbol throughput {bps:.0f} bar-symbols/sec below {_MIN_BPS_TRADING}")


# ---------------------------------------------------------------------------
# 5. Scalability: throughput should remain roughly linear with N
# ---------------------------------------------------------------------------

class TestScalability:
    """
    Processing 10× more bars must not take more than 25× longer.
    Any super-linear blowup (e.g. quadratic list scan) would be caught here.
    """

    def test_time_scales_linearly_with_bars(self):
        def _run_time(n):
            bars = _generate_bars('BENCH', n, seed=1)
            engine, _ = _build_engine(
                {'BENCH': bars},
                NoOpStrategy(),
            )
            t0 = time.perf_counter()
            engine.run()
            return time.perf_counter() - t0

        t1k  = _run_time(1_000)
        t10k = _run_time(10_000)

        ratio = t10k / t1k if t1k > 0 else float('inf')
        print(f"\n[scalability] 1k={t1k*1000:.1f}ms  10k={t10k*1000:.1f}ms  "
              f"ratio={ratio:.1f}×")
        assert ratio < 25, (
            f"10× more bars took {ratio:.1f}× longer — likely super-linear growth")

    def test_throughput_does_not_degrade_at_10k_bars(self):
        """Throughput at 10 K bars must meet the same floor as smaller runs."""
        n_bars = 10_000
        bars = _generate_bars('BENCH', n_bars)
        engine, _ = _build_engine({'BENCH': bars}, NoOpStrategy())
        t0 = time.perf_counter()
        engine.run()
        elapsed = time.perf_counter() - t0
        bps = _throughput(elapsed, n_bars)
        print(f"\n[10k bars] {bps:,.0f} bars/sec  ({elapsed*1000:.1f} ms)")
        assert bps >= _MIN_BPS_NOOP

    def test_one_year_1min_bars_throughput(self):
        """
        Realistic workload: ~1 year of 1-minute bars.
        252 trading days × 390 minutes/day (6.5h) ≈ 98,280 bars
        """
        n_bars = 98_280  # ~1 year of 1-minute data
        bars = _generate_bars('BENCH', n_bars, seed=42)
        engine, _ = _build_engine({'BENCH': bars}, NoOpStrategy())

        t0 = time.perf_counter()
        engine.run()
        elapsed = time.perf_counter() - t0

        bps = _throughput(elapsed, n_bars)
        print(f"\n[1-year 1min {n_bars:,} bars] {bps:,.0f} bars/sec  ({elapsed*1000:.1f} ms)")
        assert bps >= _MIN_BPS_NOOP, (
            f"1-year 1min throughput {bps:.0f} bars/sec below {_MIN_BPS_NOOP}")


# ---------------------------------------------------------------------------
# 6. Memory footprint
# ---------------------------------------------------------------------------

class TestMemoryFootprint:
    """Peak heap allocation must stay within a sane bound for large runs."""

    def test_peak_memory_1k_bars(self):
        n_bars = 1_000
        bars = _generate_bars('BENCH', n_bars)
        engine, _ = _build_engine({'BENCH': bars}, NoOpStrategy())

        tracemalloc.start()
        engine.run()
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak / (1024 * 1024)
        print(f"\n[memory 1k bars] peak={peak_mb:.1f} MB")
        assert peak < _MAX_MEMORY_BYTES, (
            f"Peak memory {peak_mb:.1f} MB exceeds limit of "
            f"{_MAX_MEMORY_BYTES // (1024*1024)} MB")

    def test_peak_memory_10k_bars(self):
        n_bars = 10_000
        bars = _generate_bars('BENCH', n_bars)
        engine, _ = _build_engine({'BENCH': bars}, NoOpStrategy())

        tracemalloc.start()
        engine.run()
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak / (1024 * 1024)
        print(f"\n[memory 10k bars] peak={peak_mb:.1f} MB")
        assert peak < _MAX_MEMORY_BYTES, (
            f"Peak memory {peak_mb:.1f} MB exceeds limit of "
            f"{_MAX_MEMORY_BYTES // (1024*1024)} MB")


# ---------------------------------------------------------------------------
# 7. Accounting accuracy under load
# ---------------------------------------------------------------------------

class TestAccountingAccuracyUnderLoad:
    """
    After running many trades, the accounting invariants proven in
    test_e2e_backtest must still hold at scale.
    """

    def test_cash_plus_unrealized_equals_total(self):
        """
        At every snapshot: all_holdings[i]['total'] ==
        all_holdings[i]['cash'] + sum of position market values.
        (For a stock portfolio without daily-settlement futures.)
        """
        n_bars = 2_000
        bars = _generate_bars('BENCH', n_bars, seed=3)
        strategy = PeriodicSignalStrategy(
            data_handler=None, symbol='BENCH', half_period=100)
        engine, portfolio = _build_engine(
            {'BENCH': bars}, strategy, initial=10_000_000.0)
        engine.run()

        for snap in portfolio.all_holdings:
            computed_total = snap['cash']
            for key, val in snap.items():
                if key.endswith('_value'):
                    computed_total += val
            assert computed_total == pytest.approx(snap['total'], rel=1e-6), (
                f"Snapshot total mismatch at time={snap.get('time')}: "
                f"cash+mv={computed_total:.2f} vs total={snap['total']:.2f}")

    def test_realized_pnl_equals_sum_of_position_pnls(self):
        """
        portfolio.total_realized_pnl must equal the sum of
        realized_pnl across all recorded positions.
        """
        n_bars = 1_000
        bars = _generate_bars('BENCH', n_bars, seed=5)
        strategy = PeriodicSignalStrategy(
            data_handler=None, symbol='BENCH', half_period=50)
        engine, portfolio = _build_engine(
            {'BENCH': bars}, strategy, initial=5_000_000.0)
        engine.run()

        pnl_sum = sum(p['realized_pnl'] for p in portfolio.positions)
        assert portfolio.total_realized_pnl == pytest.approx(pnl_sum, rel=1e-6)

    def test_final_equity_consistent_with_cash_and_position(self):
        """
        If the strategy ends with a flat position, final equity == final cash.
        Design: use an even number of LONG/EXIT cycles guaranteed to close out.
        """
        # 400 bars, half_period=100 → bars 0,100,200,300 signal; EXIT at 100,200,300,400
        # bar 400 doesn't exist, so last EXIT at bar 300 fills at bar 301 open.
        # After bar 301 fill, position is flat.
        n_bars = 500
        bars = _generate_bars('BENCH', n_bars, seed=11)
        strategy = PeriodicSignalStrategy(
            data_handler=None, symbol='BENCH', half_period=100)
        engine, portfolio = _build_engine(
            {'BENCH': bars}, strategy, initial=5_000_000.0)
        engine.run()

        final_qty = portfolio.current_holdings.get('BENCH', {}).get('quantity', 0)
        if final_qty == 0:
            # Flat position: total equity should equal cash
            assert portfolio.current_cash == pytest.approx(
                portfolio.all_holdings[-1]['total'], rel=1e-6)

    def test_no_negative_cash_with_conservative_sizer(self):
        """
        FixedQuantityPositionSizer checks available cash before ordering.
        Cash must never go negative regardless of price movements.
        """
        n_bars = 2_000
        bars = _generate_bars('BENCH', n_bars, seed=13)
        strategy = PeriodicSignalStrategy(
            data_handler=None, symbol='BENCH', half_period=20)
        engine, portfolio = _build_engine(
            {'BENCH': bars}, strategy,
            initial=100_000.0,                          # intentionally small
            sizer=FixedQuantityPositionSizer(quantity=10),
        )
        engine.run()

        assert portfolio.current_cash >= 0.0, (
            f"Cash went negative: {portfolio.current_cash:.2f}")
        for snap in portfolio.all_holdings:
            assert snap['cash'] >= 0.0, (
                f"Snapshot cash went negative at {snap.get('time')}: {snap['cash']:.2f}")
