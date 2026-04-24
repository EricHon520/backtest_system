"""
Unit tests for PerformanceAnalyzer.

Covers:
- total_return exact value
- max_drawdown exact value and peak update
- CAGR with known time span
- Sharpe ratio numerical trace (zero std → 0)
- win_rate, profit_factor, avg_trade_pnl only count closed trades
- total_commission sums all positions
- drawdown_series and max_drawdown_duration
- _detect_periods_per_year: daily / weekly / monthly
- Edge cases: empty equity curve, single bar
"""

import pytest
import math
from datetime import datetime, timedelta
from analytics.performance import PerformanceAnalyzer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _holdings(values, start=datetime(2024, 1, 1)):
    """Build all_holdings list with 'total' and 'time' keys."""
    return [
        {'total': v, 'time': start + timedelta(days=i)}
        for i, v in enumerate(values)
    ]


def _pos(pnl, commission=0.0, direction='SELL'):
    return {'realized_pnl': pnl, 'commission': commission, 'direction': direction}


def _analyzer(values, positions=None, initial=100_000,
               risk_free=0.0, ppy=252):
    return PerformanceAnalyzer(
        all_holdings=_holdings(values),
        positions=positions or [],
        initial_capital=initial,
        risk_free_rate=risk_free,
        periods_per_year=ppy,   # pin so tests are deterministic
    )


# ---------------------------------------------------------------------------
# total_return
# ---------------------------------------------------------------------------

class TestTotalReturn:

    def test_positive_return(self):
        a = _analyzer([100_000, 110_000])
        assert a.total_return() == pytest.approx(0.10)

    def test_negative_return(self):
        a = _analyzer([100_000, 90_000])
        assert a.total_return() == pytest.approx(-0.10)

    def test_flat_return(self):
        a = _analyzer([100_000, 100_000])
        assert a.total_return() == pytest.approx(0.0)

    def test_empty_curve_returns_zero(self):
        a = PerformanceAnalyzer([], [], 100_000, periods_per_year=252)
        assert a.total_return() == 0.0


# ---------------------------------------------------------------------------
# max_drawdown
# ---------------------------------------------------------------------------

class TestMaxDrawdown:

    def test_simple_drawdown(self):
        # peak 120k, trough 96k → dd = (120k-96k)/120k = 0.20
        a = _analyzer([100_000, 120_000, 96_000, 100_000])
        assert a.max_drawdown() == pytest.approx(0.20)

    def test_no_drawdown_returns_zero(self):
        a = _analyzer([100_000, 110_000, 120_000])
        assert a.max_drawdown() == pytest.approx(0.0)

    def test_peak_updates_correctly(self):
        # Two troughs: first 80k from 100k (20%), second 90k from 130k (≈30.8%)
        a = _analyzer([100_000, 80_000, 130_000, 90_000])
        assert a.max_drawdown() == pytest.approx((130_000 - 90_000) / 130_000)


# ---------------------------------------------------------------------------
# CAGR
# ---------------------------------------------------------------------------

class TestCAGR:

    def test_cagr_one_year_doubling(self):
        start = datetime(2024, 1, 1)
        end = datetime(2025, 1, 1)
        days = (end - start).days
        holdings = [
            {'total': 100_000, 'time': start},
            {'total': 200_000, 'time': end},
        ]
        a = PerformanceAnalyzer(holdings, [], 100_000, periods_per_year=252)
        # ~1 year doubling → CAGR ≈ 100%
        assert a.cagr() == pytest.approx(1.0, rel=0.01)

    def test_cagr_flat(self):
        start = datetime(2024, 1, 1)
        end = datetime(2025, 1, 1)
        holdings = [
            {'total': 100_000, 'time': start},
            {'total': 100_000, 'time': end},
        ]
        a = PerformanceAnalyzer(holdings, [], 100_000, periods_per_year=252)
        assert a.cagr() == pytest.approx(0.0, abs=0.001)


# ---------------------------------------------------------------------------
# Sharpe ratio
# ---------------------------------------------------------------------------

class TestSharpeRatio:

    def test_zero_std_returns_zero(self):
        # All returns identical → std=0 → Sharpe=0
        a = _analyzer([100_000, 101_000, 102_000, 103_000])
        # returns are approx equal but not exactly; use truly flat
        a2 = _analyzer([100_000] * 10)
        assert a2.sharpe_ratio() == pytest.approx(0.0)

    def test_positive_sharpe_for_steady_gains(self):
        # Monotonically increasing equity → all returns positive, std small
        vals = [100_000 + i * 100 for i in range(50)]
        a = _analyzer(vals, ppy=252)
        assert a.sharpe_ratio() > 0

    def test_single_bar_returns_zero(self):
        a = _analyzer([100_000])
        assert a.sharpe_ratio() == pytest.approx(0.0)

    def test_risk_free_rate_reduces_sharpe(self):
        vals = [100_000 + i * 200 for i in range(50)]
        a_no_rf = _analyzer(vals, risk_free=0.0, ppy=252)
        a_with_rf = _analyzer(vals, risk_free=0.10, ppy=252)
        assert a_with_rf.sharpe_ratio() < a_no_rf.sharpe_ratio()


# ---------------------------------------------------------------------------
# Trade-level metrics
# ---------------------------------------------------------------------------

class TestTradeMetrics:

    def test_win_rate_correct(self):
        positions = [_pos(200), _pos(100), _pos(-50), _pos(0)]
        a = _analyzer([100_000, 100_200], positions=positions)
        # closed trades = pnl != 0 → 3 trades; wins = 2
        assert a.win_rate() == pytest.approx(2 / 3)

    def test_win_rate_zero_for_no_trades(self):
        a = _analyzer([100_000, 100_000])
        assert a.win_rate() == pytest.approx(0.0)

    def test_profit_factor(self):
        positions = [_pos(300), _pos(200), _pos(-100), _pos(-50)]
        a = _analyzer([100_000], positions=positions)
        # gross_profit=500, gross_loss=150
        assert a.profit_factor() == pytest.approx(500 / 150)

    def test_profit_factor_no_losing_trades(self):
        positions = [_pos(100), _pos(200)]
        a = _analyzer([100_000], positions=positions)
        assert a.profit_factor() == float('inf')

    def test_avg_trade_pnl(self):
        positions = [_pos(100), _pos(200), _pos(-50)]
        a = _analyzer([100_000], positions=positions)
        assert a.avg_trade_pnl() == pytest.approx(250 / 3)

    def test_total_commission(self):
        positions = [_pos(100, commission=5.0), _pos(200, commission=3.0),
                     _pos(0, commission=2.0)]
        a = _analyzer([100_000], positions=positions)
        assert a.total_commission() == pytest.approx(10.0)

    def test_zero_pnl_trades_excluded_from_closed(self):
        """Trades with pnl=0 are entry-only records; must not count as closed."""
        positions = [_pos(0), _pos(0), _pos(100)]
        a = _analyzer([100_000], positions=positions)
        assert a.total_trades() == 1
        assert a.win_rate() == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Drawdown series and duration
# ---------------------------------------------------------------------------

class TestDrawdownDetails:

    def test_drawdown_series_length(self):
        a = _analyzer([100_000, 90_000, 95_000])
        series = a.drawdown_series()
        assert len(series) == 3

    def test_drawdown_series_values(self):
        a = _analyzer([100_000, 80_000, 90_000])
        series = a.drawdown_series()
        assert series[0] == pytest.approx(0.0)
        assert series[1] == pytest.approx(0.20)
        assert series[2] == pytest.approx(0.10)

    def test_max_drawdown_duration(self):
        # 100→110 (peak), then 3 bars below peak, then recovery
        a = _analyzer([100_000, 110_000, 100_000, 95_000, 90_000, 110_000])
        assert a.max_drawdown_duration() == 3

    def test_no_drawdown_duration_zero(self):
        a = _analyzer([100_000, 105_000, 110_000])
        assert a.max_drawdown_duration() == 0


# ---------------------------------------------------------------------------
# _detect_periods_per_year
# ---------------------------------------------------------------------------

class TestDetectPeriodsPerYear:

    def test_daily_bars_detected(self):
        start = datetime(2024, 1, 1)
        holdings = [{'total': 100_000, 'time': start + timedelta(days=i)}
                    for i in range(10)]
        a = PerformanceAnalyzer(holdings, [], 100_000)
        assert a._detect_periods_per_year() == 252

    def test_weekly_bars_detected(self):
        start = datetime(2024, 1, 1)
        holdings = [{'total': 100_000, 'time': start + timedelta(weeks=i)}
                    for i in range(10)]
        a = PerformanceAnalyzer(holdings, [], 100_000)
        assert a._detect_periods_per_year() == 52

    def test_monthly_bars_detected(self):
        start = datetime(2024, 1, 1)
        # ~30 day spacing
        holdings = [{'total': 100_000, 'time': start + timedelta(days=30 * i)}
                    for i in range(10)]
        a = PerformanceAnalyzer(holdings, [], 100_000)
        assert a._detect_periods_per_year() == 12

    def test_fallback_when_no_times(self):
        holdings = [{'total': 100_000}] * 5  # no 'time' key
        a = PerformanceAnalyzer(holdings, [], 100_000)
        assert a._detect_periods_per_year() == 252
