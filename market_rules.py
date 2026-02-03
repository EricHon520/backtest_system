from abc import ABC, abstractmethod
from datetime import datetime, time
from typing import Optional, Dict, Any
import pytz
import math

class MarketRules(ABC):
    """
    Abstract base class for market-specific trading rules
    """
    
    def __init__(self):
        self.market_name = "GENERIC"
        self.timezone = "UTC"
        self.commission_rate = 0.0
        self.min_commission = 0.0
        self.stamp_duty = 0.0
        self.transfer_fee = 0.0
        self.lot_size = 1
        self.price_tick = 0.01
        self.allow_short = True
        self.settlement_days = 0  # T+0, T+1, T+2, etc.
        
        # Slippage model parameters
        self.slippage_model = "volume_based"  # "fixed", "volume_based", "spread_based"
        self.fixed_slippage_bps = 0  # basis points (1 bps = 0.01%)
        self.volume_slippage_factor = 0.1  # impact factor for volume-based model
        
    @abstractmethod
    def validate_order(self, symbol: str, quantity: int, price: float, direction: str, current_time: datetime) -> tuple[bool, str]:
        """
        Validate if an order is allowed under market rules
        Returns: (is_valid, error_message)
        """
        pass
    
    @abstractmethod
    def is_trading_time(self, dt: datetime) -> bool:
        """
        Check if the given datetime is within trading hours
        """
        pass
    
    @abstractmethod
    def apply_price_limit(self, symbol: str, price: float, prev_close: float, direction: str) -> float:
        """
        Apply price limit rules (e.g., circuit breaker, price limit up/down)
        Returns the adjusted price
        """
        pass
    
    @abstractmethod
    def calculate_commission(self, symbol: str, quantity: int, price: float, direction: str) -> float:
        """
        Calculate total commission and fees for a trade
        """
        pass
    
    def normalize_quantity(self, quantity: int) -> int:
        """
        Normalize quantity to comply with lot size requirements
        """
        return (quantity // self.lot_size) * self.lot_size
    
    def normalize_price(self, price: float) -> float:
        """
        Normalize price to comply with tick size requirements
        """
        return round(price / self.price_tick) * self.price_tick
    
    def calculate_slippage(self, symbol: str, quantity: int, price: float, direction: str, 
                          bar_volume: float, bar_high: float, bar_low: float) -> float:
        """
        Calculate slippage based on order size relative to bar volume
        
        Volume-based slippage model:
        - Estimates market impact based on order size as % of bar volume
        - Larger orders relative to volume → more slippage
        - Uses high-low spread as volatility proxy
        
        Args:
            symbol: Trading symbol
            quantity: Order quantity
            price: Base price (typically close price)
            direction: 'BUY' or 'SELL'
            bar_volume: Volume of the current bar
            bar_high: High price of the current bar
            bar_low: Low price of the current bar
        
        Returns:
            Adjusted price after slippage
        """
        if self.slippage_model == "none" or self.slippage_model is None:
            return price
        
        if self.slippage_model == "fixed":
            # Fixed slippage in basis points
            slippage_pct = self.fixed_slippage_bps / 10000.0
            if direction == 'BUY':
                return price * (1 + slippage_pct)
            else:
                return price * (1 - slippage_pct)
        
        elif self.slippage_model == "volume_based":
            if bar_volume <= 0:
                slippage_pct = 0.001  # 0.1% default
            else:
                order_volume_pct = (quantity * price) / (bar_volume * price) if bar_volume > 0 else 0

                spread_pct = (bar_high - bar_low) / price if price > 0 else 0
                
                # Slippage = volume_factor * sqrt(order_volume_pct) * spread_pct
                # Square root to model non-linear market impact
                slippage_pct = self.volume_slippage_factor * math.sqrt(order_volume_pct) * spread_pct
                
                # Cap maximum slippage at 1% to avoid unrealistic values
                slippage_pct = min(slippage_pct, 0.01)
            
            if direction == 'BUY':
                return price * (1 + slippage_pct)
            else:
                return price * (1 - slippage_pct)
        
        elif self.slippage_model == "spread_based":
            # Spread-based slippage (cross the spread)
            spread = bar_high - bar_low
            spread_pct = spread / price if price > 0 else 0
            
            # Assume we pay half the spread
            slippage_pct = spread_pct * 0.5
            
            if direction == 'BUY':
                return price * (1 + slippage_pct)
            else:
                return price * (1 - slippage_pct)
        
        return price


class ChinaAShareRules(MarketRules):
    """
    A-Share market rules (Shanghai/Shenzhen Stock Exchange)
    """
    
    def __init__(self):
        super().__init__()
        self.market_name = "CHINA_A"
        self.timezone = "Asia/Shanghai"
        self.commission_rate = 0.0003  # 0.03%
        self.min_commission = 5.0  # 5 RMB minimum
        self.stamp_duty = 0.001  # 0.1% on sell only
        self.transfer_fee = 0.00002  # 0.002%
        self.lot_size = 100  # 100 shares per lot
        self.price_tick = 0.01
        self.allow_short = False  # No short selling for regular accounts
        self.settlement_days = 1  # T+1
        
        # Slippage parameters for A-share market
        self.slippage_model = "volume_based"
        self.volume_slippage_factor = 0.15  # Higher impact due to less liquidity
        
        # Trading hours (Beijing time)
        self.morning_start = time(9, 30)
        self.morning_end = time(11, 30)
        self.afternoon_start = time(13, 0)
        self.afternoon_end = time(15, 0)
        
    def validate_order(self, symbol: str, quantity: int, price: float, direction: str, current_time: datetime) -> tuple[bool, str]:
        # Check short selling
        if direction == 'SELL' and not self.allow_short:
            return True, ""  # Assume portfolio check handles position validation
        
        # Check lot size
        if quantity % self.lot_size != 0:
            return False, f"Quantity must be multiple of {self.lot_size} shares"
        
        # Check trading hours
        if not self.is_trading_time(current_time):
            return False, "Outside trading hours"
        
        return True, ""
    
    def is_trading_time(self, dt: datetime) -> bool:
        """
        Check if within A-share trading hours
        """
        if dt.tzinfo is None:
            tz = pytz.timezone(self.timezone)
            dt = tz.localize(dt)
        else:
            dt = dt.astimezone(pytz.timezone(self.timezone))
        
        t = dt.time()
        
        # Check if weekday (Monday=0, Sunday=6)
        if dt.weekday() >= 5:
            return False
        
        # Check trading hours
        morning_session = self.morning_start <= t <= self.morning_end
        afternoon_session = self.afternoon_start <= t <= self.afternoon_end
        
        return morning_session or afternoon_session
    
    def apply_price_limit(self, symbol: str, price: float, prev_close: float, direction: str) -> float:
        """
        Apply 10% price limit (20% for ST stocks and ChiNext/STAR Market)
        """
        if prev_close <= 0:
            return price
        
        # Determine limit based on symbol
        if symbol.startswith('ST') or symbol.startswith('*ST'):
            limit_pct = 0.05  # 5% for ST stocks
        elif symbol.startswith('688') or symbol.startswith('300'):
            limit_pct = 0.20  # 20% for STAR Market and ChiNext
        else:
            limit_pct = 0.10  # 10% for main board
        
        max_price = prev_close * (1 + limit_pct)
        min_price = prev_close * (1 - limit_pct)
        
        # Clamp price within limits
        return max(min_price, min(max_price, price))
    
    def calculate_commission(self, symbol: str, quantity: int, price: float, direction: str) -> float:
        """
        Calculate A-share trading fees
        Commission + Stamp Duty (sell only) + Transfer Fee
        """
        trade_value = quantity * price
        
        # Commission
        commission = max(trade_value * self.commission_rate, self.min_commission)
        
        # Stamp duty (only on sell)
        stamp_duty = trade_value * self.stamp_duty if direction == 'SELL' else 0.0
        
        # Transfer fee
        transfer_fee = trade_value * self.transfer_fee
        
        total_fee = commission + stamp_duty + transfer_fee
        
        return total_fee


class USStockRules(MarketRules):
    """
    US Stock market rules (NYSE/NASDAQ)
    """
    
    def __init__(self):
        super().__init__()
        self.market_name = "US_STOCK"
        self.timezone = "America/New_York"
        self.commission_rate = 0.0  # Many brokers offer zero commission
        self.min_commission = 0.0
        self.stamp_duty = 0.0
        self.transfer_fee = 0.0
        self.lot_size = 1  # No lot size requirement
        self.price_tick = 0.01
        self.allow_short = True
        self.settlement_days = 2  # T+2
        
        # Slippage parameters for US market
        self.slippage_model = "volume_based"
        self.volume_slippage_factor = 0.05  # Lower impact due to high liquidity
        
        # Trading hours (ET)
        self.market_open = time(9, 30)
        self.market_close = time(16, 0)
        
    def validate_order(self, symbol: str, quantity: int, price: float, direction: str, current_time: datetime) -> tuple[bool, str]:
        # Check trading hours
        if not self.is_trading_time(current_time):
            return False, "Outside trading hours"
        
        return True, ""
    
    def is_trading_time(self, dt: datetime) -> bool:
        """
        Check if within US market trading hours
        """
        if dt.tzinfo is None:
            tz = pytz.timezone(self.timezone)
            dt = tz.localize(dt)
        else:
            dt = dt.astimezone(pytz.timezone(self.timezone))
        
        t = dt.time()
        
        # Check if weekday
        if dt.weekday() >= 5:
            return False
        
        return self.market_open <= t <= self.market_close
    
    def apply_price_limit(self, symbol: str, price: float, prev_close: float, direction: str) -> float:
        """
        US market has circuit breakers but no daily price limits
        For backtesting, we don't apply circuit breakers
        """
        return price
    
    def calculate_commission(self, symbol: str, quantity: int, price: float, direction: str) -> float:
        """
        Calculate US stock trading fees (typically zero for retail)
        """
        trade_value = quantity * price
        commission = max(trade_value * self.commission_rate, self.min_commission)
        return commission


class HKStockRules(MarketRules):
    """
    Hong Kong Stock Exchange rules
    """
    
    def __init__(self):
        super().__init__()
        self.market_name = "HK_STOCK"
        self.timezone = "Asia/Hong_Kong"
        self.commission_rate = 0.0025  # 0.25%
        self.min_commission = 100.0  # 100 HKD minimum
        self.stamp_duty = 0.0013  # 0.13%
        self.transfer_fee = 0.00002  # 0.002%
        self.lot_size = 100  # Varies by stock, default 100
        self.price_tick = 0.01
        self.allow_short = True
        self.settlement_days = 2  # T+2
        
        # Slippage parameters for HK market
        self.slippage_model = "volume_based"
        self.volume_slippage_factor = 0.10  # Moderate liquidity
        
        # Trading hours (HKT)
        self.morning_start = time(9, 30)
        self.morning_end = time(12, 0)
        self.afternoon_start = time(13, 0)
        self.afternoon_end = time(16, 0)
        
    def validate_order(self, symbol: str, quantity: int, price: float, direction: str, current_time: datetime) -> tuple[bool, str]:
        # Check lot size
        if quantity % self.lot_size != 0:
            return False, f"Quantity must be multiple of {self.lot_size} shares"
        
        # Check trading hours
        if not self.is_trading_time(current_time):
            return False, "Outside trading hours"
        
        return True, ""
    
    def is_trading_time(self, dt: datetime) -> bool:
        """
        Check if within HK market trading hours
        """
        if dt.tzinfo is None:
            tz = pytz.timezone(self.timezone)
            dt = tz.localize(dt)
        else:
            dt = dt.astimezone(pytz.timezone(self.timezone))
        
        t = dt.time()
        
        # Check if weekday
        if dt.weekday() >= 5:
            return False
        
        # Check trading hours
        morning_session = self.morning_start <= t <= self.morning_end
        afternoon_session = self.afternoon_start <= t <= self.afternoon_end
        
        return morning_session or afternoon_session
    
    def apply_price_limit(self, symbol: str, price: float, prev_close: float, direction: str) -> float:
        """
        HK market has no price limits
        """
        return price
    
    def calculate_commission(self, symbol: str, quantity: int, price: float, direction: str) -> float:
        """
        Calculate HK stock trading fees
        """
        trade_value = quantity * price
        
        # Commission
        commission = max(trade_value * self.commission_rate, self.min_commission)
        
        # Stamp duty
        stamp_duty = trade_value * self.stamp_duty
        
        # Transfer fee
        transfer_fee = trade_value * self.transfer_fee
        
        # Trading fee (0.005%)
        trading_fee = trade_value * 0.00005
        
        total_fee = commission + stamp_duty + transfer_fee + trading_fee
        
        return total_fee


class CryptoRules(MarketRules):
    """
    Cryptocurrency market rules
    """
    
    def __init__(self):
        super().__init__()
        self.market_name = "CRYPTO"
        self.timezone = "UTC"
        self.commission_rate = 0.001  # 0.1% typical maker/taker fee
        self.min_commission = 0.0
        self.stamp_duty = 0.0
        self.transfer_fee = 0.0
        self.lot_size = 1  # No lot size, can trade fractional
        self.price_tick = 0.01
        self.allow_short = True
        self.settlement_days = 0  # T+0, instant settlement
        
        # Slippage parameters for crypto market
        self.slippage_model = "volume_based"
        self.volume_slippage_factor = 0.20  # Higher volatility and impact
        
    def validate_order(self, symbol: str, quantity: int, price: float, direction: str, current_time: datetime) -> tuple[bool, str]:
        # Crypto markets have minimal restrictions
        return True, ""
    
    def is_trading_time(self, dt: datetime) -> bool:
        """
        Crypto markets trade 24/7
        """
        return True
    
    def apply_price_limit(self, symbol: str, price: float, prev_close: float, direction: str) -> float:
        """
        No price limits in crypto markets
        """
        return price
    
    def calculate_commission(self, symbol: str, quantity: int, price: float, direction: str) -> float:
        """
        Calculate crypto trading fees
        """
        trade_value = quantity * price
        commission = trade_value * self.commission_rate
        return commission


class MarketRulesFactory:
    """
    Factory class to create appropriate market rules
    """
    
    _rules_map = {
        'china_a': ChinaAShareRules,
        'us_stock': USStockRules,
        'hk_stock': HKStockRules,
        'crypto': CryptoRules,
        'stock': USStockRules,  # Default stock to US
    }
    
    @classmethod
    def create_rules(cls, market_type: str) -> MarketRules:
        """
        Create market rules instance based on market type
        
        Args:
            market_type: 'china_a', 'us_stock', 'hk_stock', 'crypto', etc.
        
        Returns:
            MarketRules instance
        """
        market_type = market_type.lower()
        
        if market_type not in cls._rules_map:
            raise ValueError(f"Unsupported market type: {market_type}. "
                           f"Supported types: {list(cls._rules_map.keys())}")
        
        return cls._rules_map[market_type]()
    
    @classmethod
    def register_rules(cls, market_type: str, rules_class: type):
        """
        Register a custom market rules class
        
        Args:
            market_type: Market identifier
            rules_class: MarketRules subclass
        """
        cls._rules_map[market_type.lower()] = rules_class
