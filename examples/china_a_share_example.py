"""
Example: Backtesting on China A-Share Market using BacktestBuilder

Demonstrates two equivalent ways to set up the same backtest:

Style A — fully chained (one expression):
    Build everything in a single fluent chain.

Style B — set-attribute style (step by step):
    Create the builder, configure each component separately, then call build().

Both styles produce an identical Engine.
"""

import sys
sys.path.append('..')

from builder import BacktestBuilder
from strategies.moving_average import MovingAverage
from analytics.performance import PerformanceAnalyzer


# ------------------------------------------------------------------ #
# Style A: fully chained
# ------------------------------------------------------------------ #
def china_a_share_backtest_chained():
    engine = (
        BacktestBuilder()
        .set_data(
            symbols=['600519.SS'],
            start='2024-01-01',
            end='2024-12-31',
            frequency='1d',
            timezone='Asia/Shanghai',
            source='stock',
        )
        .set_market('china_a')
        .set_capital(100_000)
        .add_strategy(MovingAverage, short_window=5,  long_window=20)
        .add_strategy(MovingAverage, short_window=10, long_window=50)
        .set_position_sizer('percent', percent=0.10)
        .add_risk_manager('max_drawdown',  max_drawdown=0.20)
        .add_risk_manager('max_positions', max_positions=3)
        .build()
    )

    engine.run()

    analyzer = PerformanceAnalyzer(
        all_holdings=engine.portfolio.all_holdings,
        positions=engine.portfolio.positions,
        initial_capital=engine.portfolio.initial_capital,
    )
    analyzer.print_report()

    return engine.portfolio, analyzer


# ------------------------------------------------------------------ #
# Style B: set-attribute style
# ------------------------------------------------------------------ #
def china_a_share_backtest_stepwise():
    builder = BacktestBuilder()

    builder.set_data(
        symbols=['600519.SS'],
        start='2024-01-01',
        end='2024-12-31',
        frequency='1d',
        timezone='Asia/Shanghai',
        source='stock',
    )
    builder.set_market('china_a')
    builder.set_capital(100_000)

    builder.add_strategy(MovingAverage, short_window=5,  long_window=20)
    builder.add_strategy(MovingAverage, short_window=10, long_window=50)

    builder.set_position_sizer('percent', percent=0.10)

    builder.add_risk_manager('max_drawdown',  max_drawdown=0.20)
    builder.add_risk_manager('max_positions', max_positions=3)

    engine = builder.build()
    engine.run()

    analyzer = PerformanceAnalyzer(
        all_holdings=engine.portfolio.all_holdings,
        positions=engine.portfolio.positions,
        initial_capital=engine.portfolio.initial_capital,
    )
    analyzer.print_report()

    return engine.portfolio, analyzer


if __name__ == '__main__':
    china_a_share_backtest_chained()
