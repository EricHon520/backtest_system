from core.data_feed import DataFeed
from core.event import FillEvent, OrderEvent
from core.instrument import InstrumentRegistry
from core.execution_model import ExecutionModel
import logging


class SimulatedExecutionModel(ExecutionModel):
    """
    Simulated execution model for backtesting.

    Supports:
    - fill_on_next_bar (default True): avoids look-ahead bias by filling
      orders at the open of the next bar instead of the signal bar.
    - MARKET orders: fill at next bar open price.
    - LIMIT orders: fill only if bar low (buy) / bar high (sell) crosses limit.
    - Slippage, commission, price limits via MarketRule.
    """

    def __init__(self, data_handler: DataFeed, instrument_registry: InstrumentRegistry = None,
                 rejection_rate: float = 0.0, fill_on_next_bar: bool = True):
        self.data_handler = data_handler
        self.rejection_rate = rejection_rate
        self.logger = logging.getLogger(__name__)
        self.fill_on_next_bar = fill_on_next_bar
        self.pending_orders = []
        self.instrument_registry = instrument_registry

    # ------------------------------------------------------------------
    # ExecutionModel interface
    # ------------------------------------------------------------------

    def execute(self, order_event: OrderEvent) -> FillEvent:
        """Queue or immediately execute a single order."""
        if order_event is None:
            return None
        if self.fill_on_next_bar:
            self.pending_orders.append(order_event)
            return None
        return self._execute_order(order_event)

    def on_new_bar(self) -> list:
        """Flush all pending orders at the start of a new bar."""
        fill_events = []
        remaining_orders = []
        for order_event in self.pending_orders:
            fill_event = self._execute_order(order_event)
            if fill_event is not None:
                fill_events.append(fill_event)
            else:
                remaining_orders.append(order_event)
        self.pending_orders = remaining_orders
        return fill_events

    # ------------------------------------------------------------------
    # Legacy aliases (keep backward compatibility with Engine)
    # ------------------------------------------------------------------

    def process_order_event(self, order_event: OrderEvent) -> FillEvent:
        return self.execute(order_event)

    def process_pending_orders(self) -> list:
        return self.on_new_bar()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _execute_order(self, order_event: OrderEvent) -> FillEvent:
        """
        Execute an order and return fill event
        """
        symbol = order_event.symbol
        quantity = order_event.quantity
        direction = order_event.direction
        order_type = order_event.order_type

        latest_bar = self.data_handler.get_latest_bar(symbol=symbol)
        if latest_bar is None:
            return None

        # Use the bar's own timestamp as the fill time so that positions record
        # when the trade actually executed, not when the order was placed.
        datetime = latest_bar.get('datetime_local', order_event.datetime)

        if order_type == 'LIMIT':
            limit_price = order_event.limit_price
            if direction == 'BUY':
                current_price = latest_bar['low']
                if current_price > limit_price:
                    return None
            elif direction == 'SELL':
                current_price = latest_bar['high']
                if current_price < limit_price:
                    return None
                
        if order_type == 'LIMIT':
            fill_price = order_event.limit_price
        elif order_type == 'MARKET':
            fill_price = latest_bar.get('open', latest_bar['close'])

        if self.instrument_registry is None:
            return FillEvent(
                symbol=symbol,
                exchange='UNKNOWN',
                quantity=quantity,
                direction=direction,
                fill_price=fill_price,
                datetime=datetime,
                rejected=False,
                commission=0.0,
            )

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
        prev_bar = self.data_handler.get_latest_bars(symbol=symbol, num_bars=2)
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
        contract_multiplier = self.instrument_registry.get(symbol=symbol).contract_multiplier
        commission = market_rule.calculate_commission(
            symbol=symbol,
            quantity=quantity,
            price=fill_price,
            direction=direction,
            contract_multiplier=contract_multiplier,
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



# Backward-compatibility alias
ExecutionHandler = SimulatedExecutionModel
