"""
C++ extension wrapper with fallback to pure Python.
 
If C++ extensions fail to load or crash, automatically fall back to Python implementations.
"""

import logging
from math import sqrt
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)

try:
    import market_rule_ext 
    HAS_MARKET_RULE_EXT = True
except ImportError:
    HAS_MARKET_RULE_EXT = False
    logger.warning("C++ extension 'market_rule_ext' not available, using Python fallback")

try:
    import indicators_ext
    HAS_INDICATORS_EXT = True
except ImportError:
    HAS_INDICATORS_EXT = False
    logger.warning("C++ extension 'indicators_ext' not available, using Python fallback")

try:
    import portfolio_ext
    HAS_PORTFOLIO_EXT = True
except ImportError:
    HAS_PORTFOLIO_EXT = False
    logger.warning("portfolio_ext not available, using Python fallback")

def normalize_price(price: float, tick_size: float) -> float:
    if HAS_MARKET_RULE_EXT:
        try:
            return market_rule_ext.normalize_price(price, tick_size)
        except Exception as e:
            logger.warning(f"market_rule_ext.normalize_price failed: {e}, using Python fallback")
    return round(price / tick_size) * tick_size

def calculate_commission(
    symbol: str, quantity: int, price: float,
    direction: str, contract_multiplier: int, commission_rate: float,
    min_commission: float, stamp_duty_on_sell_only: bool, stamp_duty: float,
    transfer_fee: float, trading_fee: float
) -> float:
    if HAS_MARKET_RULE_EXT:
        try:
            return market_rule_ext.calculate_commission(
                symbol, quantity, price,
                direction, contract_multiplier, commission_rate,
                min_commission, stamp_duty_on_sell_only, stamp_duty,
                transfer_fee, trading_fee
            )
        except Exception as e:
            logger.warning(f"market_rule_ext.calculate_commission failed: {e}, using Python fallback")
    
    trade_value = quantity * price * contract_multiplier

    commission = max(trade_value * commission_rate, min_commission)

    if stamp_duty_on_sell_only:
        cal_stamp_duty = trade_value * stamp_duty if direction == "SELL" else 0.0
    else:
        cal_stamp_duty = trade_value * stamp_duty

    cal_transfer_fee = trade_value * transfer_fee

    cal_trading_fee = trade_value * trading_fee

    return commission + cal_stamp_duty + cal_transfer_fee + cal_trading_fee

def calculate_slippage(
    symbol: str, quantity: int, price: float,
    direction: str, bar_volume: float, bar_high: float,
    bar_low: float, slippage_model: str, slippage_bps: float,
    volume_slippage_factor: float
) -> float:
    if HAS_MARKET_RULE_EXT:
        try:
            return market_rule_ext.calculate_slippage(
                symbol, quantity, price,
                direction, bar_volume, bar_high,
                bar_low, slippage_model, slippage_bps,
                volume_slippage_factor
            )
        except Exception as e:
            logger.warning(f"market_rule_ext.calculate_slippage failed: {e}, using Python fallback")
    
    if slippage_model == "none":
        return price
    
    if slippage_model == "fixed":
        slippage_pct = slippage_bps / 10000.0
        if direction == "BUY":
            return price * (1.0 + slippage_pct)
        else:
            return price * (1.0 - slippage_pct)

    elif slippage_model == "volume_based":
        if bar_volume <= 0:
            slippage_pct = 0.001
        else:
            order_volume_pct = quantity / bar_volume
            spread_pct = (bar_high - bar_low) / price
            slippage_pct = volume_slippage_factor * sqrt(order_volume_pct) * spread_pct
            slippage_pct = min(slippage_pct, 0.01)
        
        if direction == "BUY":
            return price * (1.0 + slippage_pct)
        else:
            return price * (1.0 - slippage_pct)

    elif slippage_model == "spread_based":
        spread_pct = (bar_high - bar_low) / price
        slippage_pct = spread_pct * 0.5
        
        if direction == "BUY":
            return price * (1.0 + slippage_pct)
        else:
            return price * (1.0 - slippage_pct)

def compute_mtm(
    symbols: List[str], avg_costs: List[float],
    quantities: List[int], current_prices: List[float],
    contract_multipliers: List[int], last_settle_prices: List[float],
    is_daily_settlement: List[bool]
) -> Tuple[List[float], float]:
    if HAS_PORTFOLIO_EXT:
        try:
            return portfolio_ext.compute_mtm(
                symbols, avg_costs,
                quantities, current_prices,
                contract_multipliers, last_settle_prices, 
                is_daily_settlement
            )
        except Exception as e:
            logger.warning(f"portfolio_ext.compute_mtm failed: {e}, using Python fallback")
    
    unrealized_pnl = []
    total_market_value = 0.0

    for i in range(len(symbols)):
        
        if is_daily_settlement[i]:
            prev_price = last_settle_prices[i]
            if prev_price == 0.0:
                prev_price = avg_costs[i]
            unrealized = (current_prices[i] - prev_price) * quantities[i] * contract_multipliers[i]
        else:
            unrealized = (current_prices[i] - avg_costs[i]) * quantities[i] * contract_multipliers[i]

        unrealized_pnl.append(unrealized)
        total_market_value += current_prices[i] * quantities[i] * contract_multipliers[i]

    return unrealized_pnl, total_market_value


# Indicator wrapper classes
class RollingMA:
    """Wrapper for C++ RollingMA with Python fallback."""
    
    def __init__(self, window_size: int):
        if HAS_INDICATORS_EXT:
            try:
                self._cpp = indicators_ext.RollingMA(window_size)
                self._use_cpp = True
            except Exception as e:
                logger.warning(f"indicators_ext.RollingMA failed: {e}, using Python fallback")
                self._use_cpp = False
                self._window = []
                self._window_size = window_size
        else:
            self._use_cpp = False
            self._window = []
            self._window_size = window_size
    
    def update(self, value: float):
        if self._use_cpp:
            self._cpp.update(value)
        else:
            self._window.append(value)
            if len(self._window) > self._window_size:
                self._window.pop(0)
    
    def calculate(self) -> float:
        if self._use_cpp:
            return self._cpp.calculate()
        else:
            if not self._window:
                return 0.0
            return sum(self._window) / len(self._window)


class RollingEMA:
    """Wrapper for C++ RollingEMA with Python fallback."""
    
    def __init__(self, window_size: int):
        if HAS_INDICATORS_EXT:
            try:
                self._cpp = indicators_ext.RollingEMA(window_size)
                self._use_cpp = True
            except Exception as e:
                logger.warning(f"indicators_ext.RollingEMA failed: {e}, using Python fallback")
                self._use_cpp = False
                self._window = []
                self._window_size = window_size
                self._multiplier = 2.0 / (window_size + 1)
                self._ema_value = 0.0
        else:
            self._use_cpp = False
            self._window = []
            self._window_size = window_size
            self._multiplier = 2.0 / (window_size + 1)
            self._ema_value = 0.0
    
    def update(self, value: float):
        if self._use_cpp:
            self._cpp.update(value)
        else:
            if self._ema_value == 0.0:
                self._ema_value = value
            else:
                self._ema_value = value * self._multiplier + self._ema_value * (1.0 - self._multiplier)
            self._window.append(value)
            if len(self._window) > self._window_size:
                self._window.pop(0)
    
    def calculate(self) -> float:
        if self._use_cpp:
            return self._cpp.calculate()
        else:
            return self._ema_value