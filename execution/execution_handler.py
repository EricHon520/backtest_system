from data.data_handler import DataHandler
from core.event import FillEvent, OrderEvent
from core.instrument import InstrumentRegistry
import logging

class ExecutionHandler():
    def __init__(self, data_handler: DataHandler, rejection_rate: float, instrument_registry: InstrumentRegistry, commission: float = None, fill_on_next_bar: bool = True):
        self.data_handler = data_handler
        self.rejection_rate = rejection_rate
        self.commission = commission
        self.logger = logging.getLogger(__name__)
        self.fill_on_next_bar = fill_on_next_bar  # Fill on next bar to avoid look-ahead bias
        self.pending_orders = []  # Orders waiting for next bar
        self.instrument_registry = instrument_registry

    def process_order_event(self, order_event: OrderEvent) -> FillEvent:
        if order_event is None:
            return None
        
        if self.fill_on_next_bar:
            self.pending_orders.append(order_event)
            return None
        else:
            return self._execute_order(order_event)
    
    def process_pending_orders(self) -> list:
        """
        Process all pending orders on the new bar
        Called at the beginning of each new bar
        """
        fill_events = []
        
        for order_event in self.pending_orders:
            fill_event = self._execute_order(order_event)
            if fill_event is not None:
                fill_events.append(fill_event)
        
        # Clear pending orders
        self.pending_orders = []
        
        return fill_events
    
    def _execute_order(self, order_event: OrderEvent) -> FillEvent:
        """
        Execute an order and return fill event
        """
        symbol = order_event.symbol
        quantity = order_event.quantity
        direction = order_event.direction
        datetime = order_event.datetime
        
        latest_bar = self.data_handler.get_latest_bar(symbol=symbol)
        if latest_bar is None:
            return None
        
        fill_price = latest_bar.get('open', latest_bar['close'])

        market_rule = self.instrument_registry.get(symbol=symbol).market_rule
        
        # Validate order against market rules
        is_valid, error_msg = market_rule.validate_order(
            symbol=symbol,
            quantity=quantity,
            price=fill_price,
            direction=direction,
            current_time=datetime
        )
        
        if not is_valid:
            self.logger.warning(f"Order rejected by market rules: {error_msg}")
            return FillEvent(
                symbol=symbol,
                exchange=market_rule.market_name,
                quantity=quantity,
                direction=direction,
                fill_price=fill_price,
                datetime=datetime,
                rejected=True,
                commission=0.0,
            )
        
        # Apply price limits (e.g., A-share price limit up/down)
        prev_bar = self.data_handler.get_latest_bars(symbol=symbol, N=2)
        prev_close = prev_bar[-2]['close'] if len(prev_bar) >= 2 else fill_price
        fill_price = market_rule.apply_price_limit(
            symbol=symbol,
            price=fill_price,
            prev_close=prev_close,
            direction=direction
        )
        
        # Normalize price to tick size
        fill_price = market_rule.normalize_price(fill_price)
        
        # Apply slippage based on volume and volatility
        fill_price = market_rule.calculate_slippage(
            symbol=symbol,
            quantity=quantity,
            price=fill_price,
            direction=direction,
            bar_volume=latest_bar.get('volume', 0),
            bar_high=latest_bar.get('high', fill_price),
            bar_low=latest_bar.get('low', fill_price)
        )
        
        # Normalize price again after slippage
        fill_price = market_rule.normalize_price(fill_price)
        
        # Calculate commission based on market rules
        commission = market_rule.calculate_commission(
            symbol=symbol,
            quantity=quantity,
            price=fill_price,
            direction=direction
        )
        
        # Simulate order rejection based on rejection rate
        # rejected = random.random() < self.rejection_rate
        rejected = False

        exchange = market_rule.market_name

        fill_event = FillEvent(
            symbol=symbol,
            exchange=exchange,
            quantity=quantity,
            direction=direction,
            fill_price=fill_price,
            datetime=datetime,
            rejected=rejected,
            commission=commission,
        )

        return fill_event


