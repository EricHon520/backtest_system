"""
How to build a backtest with BacktestBuilder
============================================
Run from the project root:
    python examples/demo_builder.py

Every example uses synthetic in-memory price data so no database or
external data source is required.  Swap `SyntheticDataFeed` for a real
`DataHandler` when you have live data.

Examples
--------
1. Minimal backtest      — 5 lines with BacktestBuilder
2. Choose a position sizer
3. Add risk managers
4. Run multiple strategies with a signal aggregator
5. Custom strategy       — write your own, plug straight into the builder
"""

import random
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from datetime import datetime, timedelta
from typing import List, Optional

from builder import BacktestBuilder
from core.data_feed import DataFeed
from core.event import MarketEvent, SignalEvent
from core.strategy import Strategy
from analytics.performance import PerformanceAnalyzer
from strategies.moving_average import MovingAverage


# ---------------------------------------------------------------------------
# Synthetic data — replace with DataHandler for real data
# ---------------------------------------------------------------------------

class SyntheticDataFeed(DataFeed):
    """Deterministic in-memory data feed driven by a seeded random walk."""

    def __init__(self, bars_by_symbol: dict):
        self._bars   = bars_by_symbol
        self._idx    = 0
        self._latest = {s: [] for s in bars_by_symbol}
        all_ts = sorted({b['timestamp'] for bars in bars_by_symbol.values()
                         for b in bars})
        self._timeline = all_ts

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


def make_feed(symbols: List[str], n_bars: int = 252,
              seed: int = 0, start_price: float = 100.0) -> SyntheticDataFeed:
    """Return a SyntheticDataFeed with *n_bars* daily bars for each symbol."""
    bars_by_symbol = {}
    base = datetime(2023, 1, 2)
    for i, sym in enumerate(symbols):
        rng   = random.Random(seed + i * 31)
        price = start_price
        bars  = []
        for j in range(n_bars):
            price = max(price + rng.gauss(0.05, 1.2), 1.0)
            spread = abs(rng.gauss(0, 0.5))
            bars.append({
                'ticker':         sym,
                'timestamp':      j + 1,
                'datetime_local': base + timedelta(days=j),
                'open':  round(price + rng.gauss(0, 0.25), 2),
                'high':  round(price + spread, 2),
                'low':   round(max(price - spread, 0.01), 2),
                'close': round(price, 2),
                'volume': int(rng.uniform(500_000, 3_000_000)),
            })
        bars_by_symbol[sym] = bars
    return SyntheticDataFeed(bars_by_symbol)


def print_report(portfolio, label: str = ''):
    """Print a compact summary from PerformanceAnalyzer."""
    a = PerformanceAnalyzer(
        all_holdings=portfolio.all_holdings,
        positions=portfolio.positions,
        initial_capital=portfolio.initial_capital,
    )
    s = a.summary()
    if label:
        print(f"\n  {'─' * 44}")
        print(f"  {label}")
        print(f"  {'─' * 44}")
    print(f"  Capital : {s['initial_capital']:>12,.2f}  →  "
          f"Final equity : {s['final_equity']:>12,.2f}")
    print(f"  Return  : {s['total_return_pct']:>+8.2f}%   "
          f"Max drawdown : {s['max_drawdown_pct']:>7.2f}%")
    print(f"  Sharpe  : {s['sharpe_ratio']:>8.4f}        "
          f"Trades       : {s['total_trades']:>6}   "
          f"Win rate : {s['win_rate_pct']:.1f}%")


# ===========================================================================
# Example 1 — Minimal backtest
# ===========================================================================
#
#  BacktestBuilder().set_data_feed(...)     ← your data source
#                   .set_market(...)        ← market rules + instrument registry
#                   .set_capital(...)       ← starting cash
#                   .add_strategy(...)      ← one or more strategies
#                   .build()               ← assemble the Engine
#
# Defaults applied automatically:
#   • SimulatedExecutionModel  (fill_on_next_bar=True, no look-ahead bias)
#   • FixedQuantityPositionSizer(quantity=1)
#   • NullRiskManager (pass-through)
#   • WeightedAggregator (resolves multi-strategy conflicts by confidence)
# ===========================================================================

def example_1_minimal():
    print("\n" + "=" * 50)
    print("  Example 1 — Minimal backtest")
    print("=" * 50)

    engine = (
        BacktestBuilder()
        .set_data_feed(make_feed(['AAPL'], n_bars=252, seed=1))
        .set_market('us_stock')
        .set_capital(100_000)
        .add_strategy(MovingAverage, short_window=5, long_window=20)
        .build()
    )

    engine.run()
    print_report(engine.portfolio, 'MA(5,20) · fixed 1 share · no risk manager')


# ===========================================================================
# Example 2 — Choose a position sizer
# ===========================================================================
#
#  Available shortcut strings for set_position_sizer():
#    'fixed'    → FixedQuantityPositionSizer  (trades a constant number of units)
#    'percent'  → PercentOfEquityPositionSizer (allocates X% of equity per trade)
#    'equal'    → EqualWeightPositionSizer     (divides equity across N slots)
# ===========================================================================

def example_2_position_sizing():
    print("\n" + "=" * 50)
    print("  Example 2 — Position sizing")
    print("=" * 50)

    configs = [
        ('fixed',   dict(quantity=50),     'Fixed 50 shares'),
        ('percent', dict(percent=0.10),     '10% of equity per trade'),
        ('equal',   dict(n_positions=3),    'Equal-weight (3 slots)'),
    ]

    for sizer_key, sizer_kw, label in configs:
        engine = (
            BacktestBuilder()
            .set_data_feed(make_feed(['SPY'], n_bars=252, seed=2))
            .set_market('us_stock')
            .set_capital(100_000)
            .add_strategy(MovingAverage, short_window=5, long_window=20)
            .set_position_sizer(sizer_key, **sizer_kw)
            .build()
        )
        engine.run()
        print_report(engine.portfolio, label)


# ===========================================================================
# Example 3 — Risk management
# ===========================================================================
#
#  add_risk_manager() chains managers (AND logic — all must approve an order):
#    'max_drawdown'    → block new BUYs once portfolio drawdown ≥ threshold
#    'max_positions'   → block new BUYs when N symbols already open
#    'max_position_pct'→ trim order so no single symbol exceeds X% of equity
# ===========================================================================

def example_3_risk_management():
    print("\n" + "=" * 50)
    print("  Example 3 — Risk management")
    print("=" * 50)

    # No risk guards (baseline)
    engine = (
        BacktestBuilder()
        .set_data_feed(make_feed(['TSLA'], n_bars=504, seed=3, start_price=200.0))
        .set_market('us_stock')
        .set_capital(100_000)
        .add_strategy(MovingAverage, short_window=5, long_window=20)
        .set_position_sizer('fixed', quantity=10)
        .build()
    )
    engine.run()
    print_report(engine.portfolio, 'No risk manager (baseline)')

    # MaxDrawdown guard: pause new buys if drawdown hits 15%
    engine = (
        BacktestBuilder()
        .set_data_feed(make_feed(['TSLA'], n_bars=504, seed=3, start_price=200.0))
        .set_market('us_stock')
        .set_capital(100_000)
        .add_strategy(MovingAverage, short_window=5, long_window=20)
        .set_position_sizer('fixed', quantity=10)
        .add_risk_manager('max_drawdown', max_drawdown=0.15)
        .build()
    )
    engine.run()
    print_report(engine.portfolio, 'MaxDrawdown 15%')

    # Composite: MaxDrawdown 15% AND max 1 open position at a time
    engine = (
        BacktestBuilder()
        .set_data_feed(make_feed(['TSLA'], n_bars=504, seed=3, start_price=200.0))
        .set_market('us_stock')
        .set_capital(100_000)
        .add_strategy(MovingAverage, short_window=5, long_window=20)
        .set_position_sizer('fixed', quantity=10)
        .add_risk_manager('max_drawdown',  max_drawdown=0.15)
        .add_risk_manager('max_positions', max_positions=1)
        .build()
    )
    engine.run()
    print_report(engine.portfolio, 'MaxDrawdown 15% + MaxPositions 1')


# ===========================================================================
# Example 4 — Multiple strategies + signal aggregation
# ===========================================================================
#
#  add_strategy() can be called multiple times.
#  When two strategies disagree on the same symbol, set_signal_aggregator()
#  decides which signal wins:
#    'weighted'         (default) — net confidence-score determines direction
#    'majority'                   — majority vote; ties go to EXIT > LONG > SHORT
#    'first_wins'                 — first strategy's signal is kept
#    'last_wins'                  — last strategy's signal is kept
#    'veto_on_conflict'           — do nothing if any two strategies disagree
# ===========================================================================

def example_4_multi_strategy():
    print("\n" + "=" * 50)
    print("  Example 4 — Multiple strategies + aggregation")
    print("=" * 50)

    for agg, label in [
        ('weighted', 'Weighted (default)'),
        ('majority', 'Majority vote'),
        ('veto_on_conflict', 'Veto on conflict'),
    ]:
        engine = (
            BacktestBuilder()
            .set_data_feed(make_feed(['NVDA'], n_bars=504, seed=4, start_price=300.0))
            .set_market('us_stock')
            .set_capital(100_000)
            .add_strategy(MovingAverage, short_window=5,  long_window=20)
            .add_strategy(MovingAverage, short_window=10, long_window=40)
            .set_position_sizer('fixed', quantity=10)
            .add_risk_manager('max_drawdown', max_drawdown=0.20)
            .set_signal_aggregator(agg)
            .build()
        )
        engine.run()
        print_report(engine.portfolio,
                     f'Fast MA(5,20) + Slow MA(10,40)  [{label}]')


# ===========================================================================
# Example 5 — Plug in a custom strategy
# ===========================================================================
#
#  1. Subclass Strategy.
#  2. Implement calculate_signal(event) → SignalEvent | list[SignalEvent] | None
#     Signal types: 'LONG', 'SHORT', 'EXIT'
#  3. Pass the class (not an instance) to add_strategy().
#     The builder auto-injects data_handler at build() time.
# ===========================================================================

class BreakoutStrategy(Strategy):
    """
    Buy when close breaks above the N-bar high.
    Exit when close falls below the N-bar low.
    """

    def __init__(self, data_handler: DataFeed, symbol: str, window: int = 20):
        super().__init__(data_handler)
        self.symbol = symbol
        self.window = window

    def calculate_signal(self, event: MarketEvent) -> Optional[SignalEvent]:
        bars = self.data_handler.get_latest_bars(self.symbol, self.window + 1)
        if len(bars) < self.window + 1:
            return None

        lookback  = bars[:-1]           # exclude the current bar
        current   = bars[-1]['close']
        period_hi = max(b['high']  for b in lookback)
        period_lo = min(b['low']   for b in lookback)

        if current > period_hi:
            return SignalEvent(symbol=self.symbol, datetime=event.datetime,
                               signal_type='LONG')
        if current < period_lo:
            return SignalEvent(symbol=self.symbol, datetime=event.datetime,
                               signal_type='EXIT')
        return None


def example_5_custom_strategy():
    print("\n" + "=" * 50)
    print("  Example 5 — Custom strategy (20-bar Breakout)")
    print("=" * 50)

    engine = (
        BacktestBuilder()
        .set_data_feed(make_feed(['GOOG'], n_bars=504, seed=5, start_price=140.0))
        .set_market('us_stock')
        .set_capital(100_000)
        # Pass the class + any constructor kwargs (data_handler is injected automatically)
        .add_strategy(BreakoutStrategy, symbol='GOOG', window=20)
        .set_position_sizer('percent', percent=0.15)
        .add_risk_manager('max_drawdown', max_drawdown=0.20)
        .build()
    )

    engine.run()
    print_report(engine.portfolio, 'Breakout(20) · 15% equity · MaxDD 20%')

    # Show the last few trades
    print(f"\n  Last 5 trades:")
    header = f"  {'Dir':4}  {'Qty':>5}  {'FillPrice':>10}  {'PnL':>10}  {'Date'}"
    print(header)
    for t in engine.portfolio.positions[-5:]:
        print(f"  {t['direction']:4}  {t['quantity']:>5}  "
              f"{t['fill_price']:>10.2f}  {t['realized_pnl']:>10.2f}  "
              f"{t['time'].strftime('%Y-%m-%d') if hasattr(t['time'], 'strftime') else t['time']}")


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == '__main__':
    print("\nBacktestBuilder — Runnable Demo")
    print("(Synthetic OHLCV data — no database required)")

    example_1_minimal()
    example_2_position_sizing()
    example_3_risk_management()
    example_4_multi_strategy()
    example_5_custom_strategy()

    print("\n" + "=" * 50)
    print("  Done.")
    print("=" * 50)
