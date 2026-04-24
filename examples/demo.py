"""
Backtest Engine — Complete Demo
================================
Self-contained walkthrough of every major feature.
No external database required — synthetic OHLCV data is generated in-memory.

Run from the project root:
    python examples/demo.py

Sections
--------
1. Quick Start         — simplest possible backtest in ~10 lines
2. Custom Strategy     — write your own signal logic
3. Position Sizing     — Fixed / Percent-of-equity / Equal-weight
4. Risk Management     — MaxDrawdown + MaxOpenPositions guards
5. Multi-Strategy      — two strategies, configurable signal aggregation
6. BacktestBuilder API — fluent builder vs. manual assembly
7. Reading the Report  — how to interpret PerformanceAnalyzer output
"""

import random
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime, timedelta
from typing import List, Optional

# Core imports
from engine import Engine
from builder import BacktestBuilder
from core.data_feed import DataFeed
from core.event import MarketEvent, SignalEvent
from core.strategy import Strategy
from core.instrument import InstrumentRegistry, Stock
from core.market_rule import MarketRule
from core.position_sizer import (
    FixedQuantityPositionSizer,
    PercentOfEquityPositionSizer,
    EqualWeightPositionSizer,
)
from core.signal_aggregator import MajorityVoteAggregator
from execution.execution_handler import SimulatedExecutionModel
from portfolio.portfolio import Portfolio
from risk.risk_manager import (
    CompositeRiskManager,
    MaxDrawdownRiskManager,
    MaxOpenPositionsRiskManager,
)
from analytics.performance import PerformanceAnalyzer
from strategies.moving_average import MovingAverage


# ===========================================================================
# Shared helpers
# ===========================================================================

def generate_bars(symbol: str, n: int, seed: int = 42,
                  start_price: float = 100.0) -> List[dict]:
    """
    Generate N daily OHLCV bars for *symbol* using a seeded random walk.
    Returns a list of bar dicts compatible with DataFeed.get_latest_bar().
    """
    rng = random.Random(seed)
    bars = []
    price = start_price
    base = datetime(2023, 1, 2)   # Monday

    for i in range(n):
        price = max(price + rng.gauss(0.05, 1.2), 1.0)
        spread = abs(rng.gauss(0, 0.6))
        bars.append({
            'ticker':         symbol,
            'timestamp':      i + 1,
            'datetime_local': base + timedelta(days=i),
            'open':           round(price + rng.gauss(0, 0.3), 2),
            'high':           round(price + spread, 2),
            'low':            round(max(price - spread, 0.01), 2),
            'close':          round(price, 2),
            'volume':         int(rng.uniform(500_000, 3_000_000)),
        })
    return bars


class SyntheticDataFeed(DataFeed):
    """In-memory DataFeed backed by pre-generated bar lists."""

    def __init__(self, bars_by_symbol: dict):
        self._bars = bars_by_symbol
        self._idx = 0
        self._latest = {s: [] for s in bars_by_symbol}
        all_ts = set()
        for bars in bars_by_symbol.values():
            for b in bars:
                all_ts.add(b['timestamp'])
        self._timeline = sorted(all_ts)

    @property
    def symbols(self) -> List[str]:
        return list(self._bars.keys())

    def update_bars(self) -> bool:
        if self._idx >= len(self._timeline):
            return False
        ts = self._timeline[self._idx]
        for sym, bars in self._bars.items():
            if self._idx < len(bars) and bars[self._idx]['timestamp'] == ts:
                self._latest[sym].append(bars[self._idx])
        self._idx += 1
        return True

    def get_latest_bar(self, symbol: str):
        b = self._latest.get(symbol)
        return b[-1] if b else None

    def get_latest_bars(self, symbol: str, num_bars: int) -> list:
        return self._latest.get(symbol, [])[-num_bars:]


def _zero_fee_rule(**overrides) -> MarketRule:
    """MarketRule with no commission and no slippage — good for demos."""
    cfg = dict(
        commission_rate=0.0, min_commission=0.0, stamp_duty=0.0,
        lot_size=1, price_tick=0.01, allow_short=True,
        settlement_days=0, slippage_model='none', price_limit_pct=0.0,
    )
    cfg.update(overrides)
    return MarketRule(**cfg)


def _section(title: str):
    width = 60
    print(f"\n{'=' * width}")
    print(f"  {title}")
    print('=' * width)


def _print_summary(portfolio: Portfolio, label: str = ''):
    analyzer = PerformanceAnalyzer(
        all_holdings=portfolio.all_holdings,
        positions=portfolio.positions,
        initial_capital=portfolio.initial_capital,
    )
    if label:
        print(f"\n  [{label}]")
    s = analyzer.summary()
    print(f"  Initial capital : {s['initial_capital']:>12,.2f}")
    print(f"  Final equity    : {s['final_equity']:>12,.2f}")
    print(f"  Total return    : {s['total_return_pct']:>11.2f}%")
    print(f"  Max drawdown    : {s['max_drawdown_pct']:>11.2f}%")
    print(f"  Sharpe ratio    : {s['sharpe_ratio']:>12.4f}")
    print(f"  Total trades    : {s['total_trades']:>12}")
    print(f"  Win rate        : {s['win_rate_pct']:>11.2f}%")
    print(f"  Profit factor   : {s['profit_factor']:>12.4f}")


# ===========================================================================
# Section 1 — Quick Start
# ===========================================================================
# The bare minimum to run a backtest:
#   data feed → strategy → engine → run → report
# ===========================================================================

def demo_quick_start():
    _section("1. Quick Start")

    # 1a. Data: 252 daily bars for one symbol
    feed = SyntheticDataFeed({'AAPL': generate_bars('AAPL', 252, seed=1)})

    # 1b. Instrument registry (zero fees for clarity)
    rule = _zero_fee_rule()
    registry = InstrumentRegistry()
    registry.register(Stock(symbol='AAPL', market_rule=rule, currency='USD'))

    # 1c. Portfolio
    portfolio = Portfolio(initial_capital=100_000, instrument_registry=registry)

    # 1d. Execution model (fill-on-next-bar, no look-ahead bias)
    execution = SimulatedExecutionModel(
        data_handler=feed, instrument_registry=registry, fill_on_next_bar=True)

    # 1e. Strategy: built-in Moving Average crossover
    strategy = MovingAverage(data_handler=feed, short_window=5, long_window=20)

    # 1f. Engine wires everything together
    engine = Engine(
        data_handler=feed,
        portfolio=portfolio,
        execution_handler=execution,
        instrument_registry=registry,
        strategies=strategy,
        position_sizer=FixedQuantityPositionSizer(quantity=100),
    )

    engine.run()
    _print_summary(portfolio, 'MA(5,20) fixed 100 shares')

    print(f"\n  Trades recorded : {len(portfolio.positions)}")
    print(f"  Equity snapshots: {len(portfolio.all_holdings)}")


# ===========================================================================
# Section 2 — Writing a Custom Strategy
# ===========================================================================
# Subclass Strategy and implement calculate_signal().
# Return a SignalEvent (or list) or None.
#
# Signal types:
#   'LONG'  → PositionSizer will size a BUY order
#   'SHORT' → PositionSizer will size a SELL order (if allow_short=True)
#   'EXIT'  → PositionSizer flattens the current position
# ===========================================================================

class MomentumStrategy(Strategy):
    """
    Simple momentum: buy when the last bar closed higher than N bars ago,
    sell (exit) when it closed lower.

    Parameters
    ----------
    lookback : how many bars to look back for the momentum comparison
    """

    def __init__(self, data_handler: DataFeed, symbol: str, lookback: int = 10):
        super().__init__(data_handler)
        self.symbol = symbol
        self.lookback = lookback

    def calculate_signal(self, event: MarketEvent) -> Optional[SignalEvent]:
        bars = self.data_handler.get_latest_bars(self.symbol, self.lookback + 1)
        if len(bars) < self.lookback + 1:
            return None          # not enough history yet

        current_close = bars[-1]['close']
        past_close    = bars[0]['close']

        if current_close > past_close:
            return SignalEvent(symbol=self.symbol, datetime=event.datetime,
                               signal_type='LONG', confidence=1.0)
        elif current_close < past_close:
            return SignalEvent(symbol=self.symbol, datetime=event.datetime,
                               signal_type='EXIT', confidence=1.0)
        return None


def demo_custom_strategy():
    _section("2. Custom Strategy — Momentum")

    feed = SyntheticDataFeed({'MSFT': generate_bars('MSFT', 252, seed=2)})
    rule = _zero_fee_rule()
    registry = InstrumentRegistry()
    registry.register(Stock(symbol='MSFT', market_rule=rule, currency='USD'))

    portfolio = Portfolio(initial_capital=100_000, instrument_registry=registry)
    execution = SimulatedExecutionModel(
        data_handler=feed, instrument_registry=registry, fill_on_next_bar=True)

    strategy = MomentumStrategy(data_handler=feed, symbol='MSFT', lookback=10)

    engine = Engine(
        data_handler=feed, portfolio=portfolio,
        execution_handler=execution, instrument_registry=registry,
        strategies=strategy,
        position_sizer=FixedQuantityPositionSizer(quantity=50),
    )
    engine.run()
    _print_summary(portfolio, 'Momentum(10) fixed 50 shares')


# ===========================================================================
# Section 3 — Position Sizing
# ===========================================================================
# Three built-in sizers:
#   FixedQuantityPositionSizer  — always trades a fixed number of units
#   PercentOfEquityPositionSizer — allocates a % of current equity per trade
#   EqualWeightPositionSizer     — divides equity equally across N slots
# ===========================================================================

def demo_position_sizing():
    _section("3. Position Sizing Comparison")

    def _run(sizer, label):
        feed = SyntheticDataFeed({'SPY': generate_bars('SPY', 252, seed=3)})
        rule = _zero_fee_rule()
        registry = InstrumentRegistry()
        registry.register(Stock(symbol='SPY', market_rule=rule, currency='USD'))
        portfolio = Portfolio(initial_capital=100_000, instrument_registry=registry)
        execution = SimulatedExecutionModel(
            data_handler=feed, instrument_registry=registry, fill_on_next_bar=True)
        strategy = MovingAverage(data_handler=feed, short_window=5, long_window=20)
        engine = Engine(
            data_handler=feed, portfolio=portfolio,
            execution_handler=execution, instrument_registry=registry,
            strategies=strategy, position_sizer=sizer)
        engine.run()
        _print_summary(portfolio, label)

    # Fixed: always 50 shares regardless of price or equity
    _run(FixedQuantityPositionSizer(quantity=50),
         'Fixed: 50 shares per trade')

    # Percent: 20% of current equity per trade
    _run(PercentOfEquityPositionSizer(percent=0.20),
         'Percent: 20% of equity per trade')

    # Equal weight: divide equity across 3 concurrent position slots
    _run(EqualWeightPositionSizer(n_positions=3),
         'Equal-weight: 3 position slots')


# ===========================================================================
# Section 4 — Risk Management
# ===========================================================================
# Risk managers sit between PositionSizer and ExecutionModel.
# They can approve, adjust, or reject proposed orders.
#
# Built-in managers:
#   MaxDrawdownRiskManager      — blocks new BUYs after X% drawdown from peak
#   MaxOpenPositionsRiskManager — caps number of simultaneous open positions
#   MaxPositionSizeRiskManager  — caps single-symbol notional as % of equity
#   CompositeRiskManager        — chains multiple managers (AND logic)
# ===========================================================================

def demo_risk_management():
    _section("4. Risk Management")

    def _run(risk_mgr, label):
        feed = SyntheticDataFeed({'TSLA': generate_bars('TSLA', 504, seed=4,
                                                         start_price=200.0)})
        rule = _zero_fee_rule()
        registry = InstrumentRegistry()
        registry.register(Stock(symbol='TSLA', market_rule=rule, currency='USD'))
        portfolio = Portfolio(initial_capital=100_000, instrument_registry=registry)
        execution = SimulatedExecutionModel(
            data_handler=feed, instrument_registry=registry, fill_on_next_bar=True)
        strategy = MovingAverage(data_handler=feed, short_window=5, long_window=20)
        engine = Engine(
            data_handler=feed, portfolio=portfolio,
            execution_handler=execution, instrument_registry=registry,
            strategies=strategy,
            position_sizer=FixedQuantityPositionSizer(quantity=10),
            risk_manager=risk_mgr,
        )
        engine.run()
        _print_summary(portfolio, label)

    # No risk manager (default pass-through)
    _run(None, 'No risk manager')

    # Block new buys if drawdown exceeds 15%
    _run(MaxDrawdownRiskManager(max_drawdown=0.15),
         'MaxDrawdown 15%')

    # Combined: 15% drawdown guard AND max 1 concurrent position
    _run(
        CompositeRiskManager([
            MaxDrawdownRiskManager(max_drawdown=0.15),
            MaxOpenPositionsRiskManager(max_positions=1),
        ]),
        'MaxDrawdown 15% + MaxPositions 1',
    )


# ===========================================================================
# Section 5 — Multi-Strategy with Signal Aggregation
# ===========================================================================
# Multiple strategies can run simultaneously on the same symbol.
# When they disagree, the SignalAggregator resolves the conflict.
#
# Built-in aggregators:
#   WeightedAggregator    (default) — net confidence score determines direction
#   MajorityVoteAggregator          — majority wins; EXIT > LONG > SHORT on ties
#   FirstWinsAggregator             — first strategy's signal is kept
#   LastWinsAggregator              — last strategy's signal is kept
#   VetoOnConflictAggregator        — do nothing if any two strategies disagree
# ===========================================================================

def demo_multi_strategy():
    _section("5. Multi-Strategy + Signal Aggregation")

    def _run(short_fast, long_fast, short_slow, long_slow, aggregator, label):
        feed = SyntheticDataFeed({'NVDA': generate_bars('NVDA', 504, seed=5,
                                                         start_price=300.0)})
        rule = _zero_fee_rule()
        registry = InstrumentRegistry()
        registry.register(Stock(symbol='NVDA', market_rule=rule, currency='USD'))
        portfolio = Portfolio(initial_capital=100_000, instrument_registry=registry)
        execution = SimulatedExecutionModel(
            data_handler=feed, instrument_registry=registry, fill_on_next_bar=True)

        fast_ma = MovingAverage(data_handler=feed,
                                short_window=short_fast, long_window=long_fast)
        slow_ma = MovingAverage(data_handler=feed,
                                short_window=short_slow, long_window=long_slow)

        engine = Engine(
            data_handler=feed, portfolio=portfolio,
            execution_handler=execution, instrument_registry=registry,
            strategies=[fast_ma, slow_ma],
            position_sizer=FixedQuantityPositionSizer(quantity=10),
            signal_aggregator=aggregator,
        )
        engine.run()
        _print_summary(portfolio, label)

    _run(5, 20, 10, 40, None,
         'Fast MA(5,20) + Slow MA(10,40)  [weighted, default]')

    _run(5, 20, 10, 40, MajorityVoteAggregator(),
         'Fast MA(5,20) + Slow MA(10,40)  [majority vote]')


# ===========================================================================
# Section 6 — BacktestBuilder API
# ===========================================================================
# BacktestBuilder is a fluent builder that wires every component in one chain.
# It auto-injects data_handler into strategies that accept it.
# ===========================================================================

def demo_builder_api():
    _section("6. BacktestBuilder — Fluent API")

    # --- 6a. Fluent chain style ---
    print("\n  Style A: fully chained")

    feed = SyntheticDataFeed({'GOOG': generate_bars('GOOG', 252, seed=6,
                                                     start_price=150.0)})
    rule = _zero_fee_rule()
    registry = InstrumentRegistry()
    registry.register(Stock(symbol='GOOG', market_rule=rule, currency='USD'))

    engine = (
        BacktestBuilder()
        .set_data_feed(feed)
        .set_instrument_registry(registry)
        .set_capital(100_000)
        .add_strategy(MovingAverage, short_window=5, long_window=20)
        .add_strategy(MovingAverage, short_window=10, long_window=40)
        .set_position_sizer('percent', percent=0.15)
        .add_risk_manager('max_drawdown',  max_drawdown=0.20)
        .add_risk_manager('max_positions', max_positions=2)
        .set_signal_aggregator('majority')
        .build()
    )
    engine.run()
    _print_summary(engine.portfolio, 'Builder chained')

    # --- 6b. Step-by-step style (same result) ---
    print("\n  Style B: step-by-step")

    feed2 = SyntheticDataFeed({'GOOG': generate_bars('GOOG', 252, seed=6,
                                                      start_price=150.0)})
    registry2 = InstrumentRegistry()
    registry2.register(Stock(symbol='GOOG', market_rule=rule, currency='USD'))

    builder = BacktestBuilder()
    builder.set_data_feed(feed2)
    builder.set_instrument_registry(registry2)
    builder.set_capital(100_000)
    builder.add_strategy(MovingAverage, short_window=5,  long_window=20)
    builder.add_strategy(MovingAverage, short_window=10, long_window=40)
    builder.set_position_sizer('percent', percent=0.15)
    builder.add_risk_manager('max_drawdown',  max_drawdown=0.20)
    builder.add_risk_manager('max_positions', max_positions=2)
    builder.set_signal_aggregator('majority')
    engine2 = builder.build()
    engine2.run()
    _print_summary(engine2.portfolio, 'Builder step-by-step')


# ===========================================================================
# Section 7 — Reading the Performance Report
# ===========================================================================

def demo_performance_report():
    _section("7. Full Performance Report")

    feed = SyntheticDataFeed({'AMZN': generate_bars('AMZN', 504, seed=7,
                                                     start_price=180.0)})
    rule = _zero_fee_rule(commission_rate=0.001)   # 0.1% commission
    registry = InstrumentRegistry()
    registry.register(Stock(symbol='AMZN', market_rule=rule, currency='USD'))
    portfolio = Portfolio(initial_capital=200_000, instrument_registry=registry)
    execution = SimulatedExecutionModel(
        data_handler=feed, instrument_registry=registry, fill_on_next_bar=True)
    strategy = MovingAverage(data_handler=feed, short_window=5, long_window=20)
    engine = Engine(
        data_handler=feed, portfolio=portfolio,
        execution_handler=execution, instrument_registry=registry,
        strategies=strategy,
        position_sizer=PercentOfEquityPositionSizer(percent=0.10),
    )
    engine.run()

    analyzer = PerformanceAnalyzer(
        all_holdings=portfolio.all_holdings,
        positions=portfolio.positions,
        initial_capital=portfolio.initial_capital,
    )

    print()
    analyzer.print_report()

    # --- Accessing individual metrics programmatically ---
    s = analyzer.summary()
    print("\n  Key metrics (programmatic access):")
    print(f"    total_return    = {s['total_return_pct']:.2f}%")
    print(f"    max_drawdown    = {s['max_drawdown_pct']:.2f}%")
    print(f"    sharpe_ratio    = {s['sharpe_ratio']:.4f}")
    print(f"    total_trades    = {s['total_trades']}")
    print(f"    win_rate        = {s['win_rate_pct']:.1f}%")
    print(f"    profit_factor   = {s['profit_factor']:.4f}")
    print(f"    total_commission= {s['total_commission']:.2f}")

    # --- Inspecting individual trade records ---
    print(f"\n  Last 3 trades:")
    for t in portfolio.positions[-3:]:
        print(f"    {t['direction']:4s}  {t['quantity']:>6} @ {t['fill_price']:>8.2f}"
              f"  pnl={t['realized_pnl']:>8.2f}  comm={t['commission']:.2f}"
              f"  time={t['time']}")


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == '__main__':
    print("\nBacktest Engine — Feature Demo")
    print("(All prices are synthetic — no external data required)\n")

    demo_quick_start()
    demo_custom_strategy()
    demo_position_sizing()
    demo_risk_management()
    demo_multi_strategy()
    demo_builder_api()
    demo_performance_report()

    print(f"\n{'=' * 60}")
    print("  Demo complete.")
    print('=' * 60)
