from datetime import datetime, time
from typing import Optional, Dict, Any, List
import pytz
import math
from core.cpp_wrapper import normalize_price, calculate_commission, calculate_slippage


class MarketRule:
    """
    Configurable market trading rules.
    All market differences are driven by attributes — no subclassing needed.

    Usage:
        # Use a preset market configuration
        rule = MarketRule('china_a')

        # Use a preset with overrides
        rule = MarketRule('china_a', commission_rate=0.001)

        # Fully custom
        rule = MarketRule(market_name="MY_MARKET", commission_rate=0.001, lot_size=10, ...)
    """

    _presets = {
        'china_a': {
            'market_name': 'CHINA_A',
            'timezone': 'Asia/Shanghai',
            'currency': 'CNY',
            'commission_rate': 0.0003,
            'min_commission': 5.0,
            'stamp_duty': 0.001,
            'stamp_duty_on_sell_only': True,
            'transfer_fee': 0.00002,
            'lot_size': 100,
            'price_tick': 0.01,
            'allow_short': False,
            'settlement_days': 1,
            'trading_sessions': [(time(9, 30), time(11, 30)), (time(13, 0), time(15, 0))],
            'price_limit_pct': 0.10,
            'price_limit_rules': {'ST': 0.05, '*ST': 0.05, '688': 0.20, '300': 0.20},
            'volume_slippage_factor': 0.15,
        },
        'us_stock': {
            'market_name': 'US_STOCK',
            'timezone': 'America/New_York',
            'currency': 'USD',
            'lot_size': 1,
            'price_tick': 0.01,
            'allow_short': True,
            'settlement_days': 2,
            'trading_sessions': [(time(9, 30), time(16, 0))],
            'volume_slippage_factor': 0.05,
        },
        'hk_stock': {
            'market_name': 'HK_STOCK',
            'timezone': 'Asia/Hong_Kong',
            'currency': 'HKD',
            'commission_rate': 0.0025,
            'min_commission': 100.0,
            'stamp_duty': 0.0013,
            'stamp_duty_on_sell_only': False,
            'transfer_fee': 0.00002,
            'trading_fee': 0.00005,
            'lot_size': 100,
            'price_tick': 0.01,
            'allow_short': True,
            'settlement_days': 2,
            'trading_sessions': [(time(9, 30), time(12, 0)), (time(13, 0), time(16, 0))],
            'volume_slippage_factor': 0.10,
        },
        'crypto': {
            'market_name': 'CRYPTO',
            'timezone': 'UTC',
            'currency': 'USD',
            'commission_rate': 0.001,
            'lot_size': 1,
            'price_tick': 0.01,
            'allow_short': True,
            'settlement_days': 0,
            'is_24h': True,
            'volume_slippage_factor': 0.20,
        },
    }
    # Alias
    _presets['stock'] = _presets['us_stock']

    def __init__(self,
                 market_type: Optional[str] = None,
                 **kwargs):
        # Defaults
        defaults = {
            'market_name': 'GENERIC',
            'timezone': 'UTC',
            'currency': 'USD',
            'commission_rate': 0.0,
            'min_commission': 0.0,
            'stamp_duty': 0.0,
            'stamp_duty_on_sell_only': True,
            'transfer_fee': 0.0,
            'trading_fee': 0.0,
            'lot_size': 1,
            'price_tick': 0.01,
            'allow_short': True,
            'settlement_days': 0,
            'requires_daily_settlement': False,
            'trading_sessions': None,
            'is_24h': False,
            'price_limit_pct': 0.0,
            'price_limit_rules': {},
            'slippage_model': 'volume_based',
            'fixed_slippage_bps': 0,
            'volume_slippage_factor': 0.1,
        }

        # If market_type is provided, overlay preset on top of defaults
        if market_type is not None:
            market_type = market_type.lower()
            if market_type not in self._presets:
                raise ValueError(f"Unknown market_type: '{market_type}'. "
                               f"Available: {list(self._presets.keys())}")
            defaults.update(self._presets[market_type])

        # User kwargs override everything
        defaults.update(kwargs)

        self.market_name = defaults['market_name']
        self.timezone = defaults['timezone']
        self.currency = defaults['currency']
        self.commission_rate = defaults['commission_rate']
        self.min_commission = defaults['min_commission']
        self.stamp_duty = defaults['stamp_duty']
        self.stamp_duty_on_sell_only = defaults['stamp_duty_on_sell_only']
        self.transfer_fee = defaults['transfer_fee']
        self.trading_fee = defaults['trading_fee']
        self.lot_size = defaults['lot_size']
        self.price_tick = defaults['price_tick']
        self.allow_short = defaults['allow_short']
        self.settlement_days = defaults['settlement_days']
        self.requires_daily_settlement = defaults['requires_daily_settlement']
        self.trading_sessions = defaults['trading_sessions']
        self.is_24h = defaults['is_24h']
        self.price_limit_pct = defaults['price_limit_pct']
        self.price_limit_rules = defaults['price_limit_rules']
        self.slippage_model = defaults['slippage_model']
        self.fixed_slippage_bps = defaults['fixed_slippage_bps']
        self.volume_slippage_factor = defaults['volume_slippage_factor']

    @classmethod
    def register_preset(cls, market_type: str, config: dict):
        """
        Register a custom market preset.

        Args:
            market_type: Market identifier (e.g. 'china_future')
            config: Dict of MarketRule attribute overrides
        """
        cls._presets[market_type.lower()] = config

    def validate_order(self, symbol: str, quantity: int, price: float, direction: str, current_time) -> tuple[bool, str]:
        """
        Validate if an order is allowed under market rules
        Returns: (is_valid, error_message)
        """
        # Check lot size
        if self.lot_size > 1 and quantity % self.lot_size != 0:
            return False, f"Quantity must be multiple of {self.lot_size} shares"

        # Coerce current_time to datetime if it arrived as a string
        dt = self._coerce_datetime(current_time)
        if dt is None:
            return True, ""

        # Skip the time-of-day check for daily (and coarser) bars: their
        # datetime is typically midnight (00:00:00) which would fall outside
        # every intraday session and cause all daily orders to be rejected.
        from datetime import time as _time
        is_midnight = (dt.time() == _time(0, 0, 0))
        if not is_midnight and not self.is_trading_time(dt):
            return False, "Outside trading hours"

        return True, ""

    @staticmethod
    def _coerce_datetime(dt) -> Optional[datetime]:
        """Return a timezone-naive datetime from str, datetime, or None."""
        if isinstance(dt, datetime):
            return dt
        if isinstance(dt, str):
            cleaned = dt.rsplit(' ', 1)[0] if ' ' in dt else dt
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                try:
                    return datetime.strptime(cleaned, fmt)
                except ValueError:
                    continue
        return None
    
    def is_trading_time(self, dt: datetime) -> bool:
        """
        Check if the given datetime is within trading hours
        """
        if self.is_24h:
            return True

        if self.trading_sessions is None:
            return True

        if dt.tzinfo is None:
            tz = pytz.timezone(self.timezone)
            dt = tz.localize(dt)
        else:
            dt = dt.astimezone(pytz.timezone(self.timezone))
        
        t = dt.time()
        
        # Check if weekday (Monday=0, Sunday=6)
        if dt.weekday() >= 5:
            return False
        
        # Check against all trading sessions
        for session_start, session_end in self.trading_sessions:
            if session_start <= t <= session_end:
                return True
        
        return False
    
    def apply_price_limit(self, symbol: str, price: float, prev_close: float, direction: str) -> float:
        """
        Apply price limit rules (e.g., circuit breaker, price limit up/down)
        Returns the adjusted price
        """
        if self.price_limit_pct == 0.0 and not self.price_limit_rules:
            return price

        if prev_close <= 0:
            return price
        
        # Check symbol-prefix based overrides first
        limit_pct = self.price_limit_pct
        for prefix, pct in self.price_limit_rules.items():
            if symbol.startswith(prefix):
                limit_pct = pct
                break

        if limit_pct == 0.0:
            return price
        
        max_price = prev_close * (1 + limit_pct)
        min_price = prev_close * (1 - limit_pct)
        
        return max(min_price, min(max_price, price))
    
    def calculate_commission(self, symbol: str, quantity: int, price: float, direction: str,
                              contract_multiplier: int = 1) -> float:
        """
        Calculate total commission and fees for a trade
        """
        return calculate_commission(
            symbol, quantity, price, direction, contract_multiplier,
            self.commission_rate, self.min_commission, self.stamp_duty_on_sell_only,
            self.stamp_duty, self.transfer_fee, self.trading_fee
        )
    
    def normalize_quantity(self, quantity: int) -> int:
        """
        Normalize quantity to comply with lot size requirements
        """
        return (quantity // self.lot_size) * self.lot_size
    
    def normalize_price(self, price: float) -> float:
        """
        Normalize price to comply with tick size requirements
        """
        return normalize_price(price, self.price_tick)
    
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
        return calculate_slippage(
            symbol, quantity, price, direction, bar_volume, bar_high, bar_low,
            self.slippage_model, self.fixed_slippage_bps, self.volume_slippage_factor
        )

    def calculate_margin(self, symbol: str, quantity: int, price: float, contract_multiplier: int):
        return quantity * price * contract_multiplier


