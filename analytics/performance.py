"""
Performance analytics for backtesting results.

Usage:
    from analytics.performance import PerformanceAnalyzer

    analyzer = PerformanceAnalyzer(
        all_holdings=portfolio.all_holdings,
        positions=portfolio.positions,
        initial_capital=portfolio.initial_capital,
    )
    report = analyzer.summary()
    analyzer.print_report()
"""

import math
from datetime import datetime, timedelta
from typing import List, Dict, Optional


class PerformanceAnalyzer:
    """
    Computes standard backtesting performance metrics from
    portfolio.all_holdings (time-series) and portfolio.positions (trades).
    """

    def __init__(
        self,
        all_holdings: List[dict],
        positions: List[dict],
        initial_capital: float,
        risk_free_rate: float = 0.0,
        periods_per_year: Optional[int] = None,
    ):
        """
        Args:
            all_holdings:      List of dicts with keys 'time', 'total', 'cash'.
            positions:         List of trade dicts with 'realized_pnl', 'direction'.
            initial_capital:   Starting capital.
            risk_free_rate:    Annual risk-free rate (default 0.0).
            periods_per_year:  Override auto-detection of bar frequency
                               (e.g. 252 for daily, 52 for weekly, 12 for monthly).
        """
        self.all_holdings = all_holdings
        self.positions = positions
        self.initial_capital = initial_capital
        self.risk_free_rate = risk_free_rate
        self._periods_per_year = periods_per_year

        self._equity_curve = [h['total'] for h in all_holdings if 'total' in h]
        self._returns = self._compute_returns()

    # ------------------------------------------------------------------
    # Core metrics
    # ------------------------------------------------------------------

    def total_return(self) -> float:
        """Total return as a decimal (e.g. 0.25 = 25%)."""
        if not self._equity_curve or self.initial_capital == 0:
            return 0.0
        return (self._equity_curve[-1] / self.initial_capital) - 1.0

    def cagr(self) -> float:
        """Compound Annual Growth Rate."""
        years = self._years_elapsed()
        if years <= 0 or self.initial_capital == 0:
            return 0.0
        ending = self._equity_curve[-1] if self._equity_curve else self.initial_capital
        return (ending / self.initial_capital) ** (1.0 / years) - 1.0

    def max_drawdown(self) -> float:
        """Maximum peak-to-trough drawdown as a positive decimal (e.g. 0.20 = 20%)."""
        if not self._equity_curve:
            return 0.0
        peak = self._equity_curve[0]
        max_dd = 0.0
        for val in self._equity_curve:
            if val > peak:
                peak = val
            dd = (peak - val) / peak if peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd
        return max_dd

    def sharpe_ratio(self) -> float:
        """Annualised Sharpe Ratio."""
        if len(self._returns) < 2:
            return 0.0
        ppy = self._periods_per_year or self._detect_periods_per_year()
        mean_r = sum(self._returns) / len(self._returns)
        variance = sum((r - mean_r) ** 2 for r in self._returns) / (len(self._returns) - 1)
        std_r = math.sqrt(variance)
        if std_r == 0:
            return 0.0
        daily_rf = self.risk_free_rate / ppy
        return (mean_r - daily_rf) / std_r * math.sqrt(ppy)

    def sortino_ratio(self) -> float:
        """Annualised Sortino Ratio (downside deviation only)."""
        if len(self._returns) < 2:
            return 0.0
        ppy = self._periods_per_year or self._detect_periods_per_year()
        daily_rf = self.risk_free_rate / ppy
        mean_r = sum(self._returns) / len(self._returns)
        downside = [min(r - daily_rf, 0) ** 2 for r in self._returns]
        downside_std = math.sqrt(sum(downside) / len(downside))
        if downside_std == 0:
            return 0.0
        return (mean_r - daily_rf) / downside_std * math.sqrt(ppy)

    def calmar_ratio(self) -> float:
        """CAGR / Max Drawdown."""
        mdd = self.max_drawdown()
        if mdd == 0:
            return 0.0
        return self.cagr() / mdd

    # ------------------------------------------------------------------
    # Trade-level metrics
    # ------------------------------------------------------------------

    def _closed_trades(self) -> list:
        """Positions that represent a closing leg (realized_pnl is non-zero)."""
        return [p for p in self.positions if p.get('realized_pnl', 0) != 0]

    def win_rate(self) -> float:
        """Fraction of closed trades with positive realized PnL."""
        closed = self._closed_trades()
        if not closed:
            return 0.0
        wins = sum(1 for p in closed if p['realized_pnl'] > 0)
        return wins / len(closed)

    def profit_factor(self) -> float:
        """Gross profit / Gross loss (closed trades only)."""
        closed = self._closed_trades()
        gross_profit = sum(p['realized_pnl'] for p in closed if p['realized_pnl'] > 0)
        gross_loss = abs(sum(p['realized_pnl'] for p in closed if p['realized_pnl'] < 0))
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
        return gross_profit / gross_loss

    def avg_trade_pnl(self) -> float:
        """Average realized PnL per closed trade."""
        closed = self._closed_trades()
        if not closed:
            return 0.0
        return sum(p['realized_pnl'] for p in closed) / len(closed)

    def total_trades(self) -> int:
        """Number of closing legs (entries with non-zero realized PnL)."""
        return len(self._closed_trades())

    def total_commission(self) -> float:
        return sum(p.get('commission', 0) for p in self.positions)

    # ------------------------------------------------------------------
    # Drawdown details
    # ------------------------------------------------------------------

    def drawdown_series(self) -> List[float]:
        """Per-bar drawdown from peak, as positive decimals."""
        series = []
        peak = self._equity_curve[0] if self._equity_curve else 0
        for val in self._equity_curve:
            if val > peak:
                peak = val
            dd = (peak - val) / peak if peak > 0 else 0.0
            series.append(dd)
        return series

    def max_drawdown_duration(self) -> int:
        """Number of bars spent in drawdown (below the previous peak)."""
        if not self._equity_curve:
            return 0
        peak = self._equity_curve[0]
        in_dd = False
        current_dur = 0
        max_dur = 0
        for val in self._equity_curve:
            if val >= peak:
                peak = val
                in_dd = False
                current_dur = 0
            else:
                in_dd = True
                current_dur += 1
                max_dur = max(max_dur, current_dur)
        return max_dur

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summary(self) -> Dict:
        """Return all metrics as a dict."""
        return {
            'initial_capital':        self.initial_capital,
            'final_equity':           self._equity_curve[-1] if self._equity_curve else self.initial_capital,
            'total_return_pct':       round(self.total_return() * 100, 4),
            'cagr_pct':               round(self.cagr() * 100, 4),
            'max_drawdown_pct':       round(self.max_drawdown() * 100, 4),
            'max_drawdown_duration':  self.max_drawdown_duration(),
            'sharpe_ratio':           round(self.sharpe_ratio(), 4),
            'sortino_ratio':          round(self.sortino_ratio(), 4),
            'calmar_ratio':           round(self.calmar_ratio(), 4),
            'total_trades':           self.total_trades(),
            'win_rate_pct':           round(self.win_rate() * 100, 2),
            'profit_factor':          round(self.profit_factor(), 4),
            'avg_trade_pnl':          round(self.avg_trade_pnl(), 4),
            'total_commission':       round(self.total_commission(), 4),
        }

    def print_report(self):
        """Pretty-print the performance summary."""
        s = self.summary()
        width = 40
        print('=' * width)
        print('  Backtest Performance Report')
        print('=' * width)
        print(f"  Initial Capital      : {s['initial_capital']:>14,.2f}")
        print(f"  Final Equity         : {s['final_equity']:>14,.2f}")
        print(f"  Total Return         : {s['total_return_pct']:>13.2f}%")
        print(f"  CAGR                 : {s['cagr_pct']:>13.2f}%")
        print('-' * width)
        print(f"  Max Drawdown         : {s['max_drawdown_pct']:>13.2f}%")
        print(f"  Max DD Duration      : {s['max_drawdown_duration']:>11} bars")
        print('-' * width)
        print(f"  Sharpe Ratio         : {s['sharpe_ratio']:>14.4f}")
        print(f"  Sortino Ratio        : {s['sortino_ratio']:>14.4f}")
        print(f"  Calmar Ratio         : {s['calmar_ratio']:>14.4f}")
        print('-' * width)
        print(f"  Total Trades         : {s['total_trades']:>14}")
        print(f"  Win Rate             : {s['win_rate_pct']:>13.2f}%")
        print(f"  Profit Factor        : {s['profit_factor']:>14.4f}")
        print(f"  Avg Trade PnL        : {s['avg_trade_pnl']:>14.4f}")
        print(f"  Total Commission     : {s['total_commission']:>14.4f}")
        print('=' * width)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _compute_returns(self) -> List[float]:
        """Period-over-period returns from equity curve."""
        if len(self._equity_curve) < 2:
            return []
        returns = []
        for i in range(1, len(self._equity_curve)):
            prev = self._equity_curve[i - 1]
            curr = self._equity_curve[i]
            if prev > 0:
                returns.append((curr - prev) / prev)
        return returns

    def _detect_periods_per_year(self) -> int:
        """
        Heuristically infer the bar frequency from the holdings timestamps.
        Falls back to 252 (daily) if timestamps are unavailable or ambiguous.
        """
        times = [h.get('time') for h in self.all_holdings if h.get('time') is not None]
        if len(times) < 2:
            return 252

        def _to_dt(t):
            if isinstance(t, datetime):
                return t
            if isinstance(t, str):
                try:
                    return datetime.fromisoformat(t.split(' +')[0].split(' UTC')[0])
                except Exception:
                    return None
            return None

        dts = [_to_dt(t) for t in times[:min(20, len(times))]]
        dts = [d for d in dts if d is not None]
        if len(dts) < 2:
            return 252

        gaps = [(dts[i] - dts[i - 1]).total_seconds() for i in range(1, len(dts))]
        avg_gap = sum(gaps) / len(gaps)

        seconds_per_year = 365.25 * 24 * 3600
        ppy = seconds_per_year / avg_gap if avg_gap > 0 else 252

        # Pick the standard period whose geometric midpoint is closest to ppy.
        # Using geometric midpoints avoids the asymmetric threshold problem with
        # the original cascade (e.g. monthly ~24 bars/yr being mapped to 26).
        standards = [252, 52, 26, 12, 4, 2, 1]
        best = standards[0]
        best_dist = abs(math.log(ppy / best)) if ppy > 0 else float('inf')
        for standard in standards[1:]:
            dist = abs(math.log(ppy / standard)) if ppy > 0 else float('inf')
            if dist < best_dist:
                best_dist = dist
                best = standard
        return best

    def _years_elapsed(self) -> float:
        times = [h.get('time') for h in self.all_holdings if h.get('time') is not None]
        if len(times) < 2:
            return len(self._equity_curve) / 252.0

        def _to_dt(t):
            if isinstance(t, datetime):
                return t
            if isinstance(t, str):
                try:
                    return datetime.fromisoformat(t.split(' +')[0].split(' UTC')[0])
                except Exception:
                    return None
            return None

        first = _to_dt(times[0])
        last = _to_dt(times[-1])
        if first is None or last is None:
            return len(self._equity_curve) / 252.0
        delta = (last - first).total_seconds()
        return delta / (365.25 * 24 * 3600)
