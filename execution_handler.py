import random
from data_handler import DataHandler
from event import FillEvent, OrderEvent

class ExecutionHandler():
    def __init__(self, data_handler: DataHandler, rejection_rate: float, commission: float):
        self.data_handler = data_handler
        self.rejection_rate = rejection_rate
        self.commission = commission

    def process_order_event(self, order_event: OrderEvent) -> FillEvent:
        if order_event is None:
            return None

        rejected = random.random() < self.rejection_rate

        symbol = order_event.symbol
        quantity = order_event.quantity
        direction = order_event.direction
        latest_bar = self.data_handler.get_latest_bar(symbol=symbol)
        if latest_bar is None:
            return None
        fill_price = latest_bar['close']
        datetime = order_event.datetime

        exchange = latest_bar.get('source', '').upper() if isinstance(latest_bar, dict) else ''
        if exchange == 'STOCK':
            exchange = 'STOCK'
        elif exchange == 'CRYPTO':
            exchange = 'CRYPTO'

        fill_event = FillEvent(
            symbol=symbol,
            exchange=exchange,
            quantity=quantity,
            direction=direction,
            fill_price=fill_price,
            datetime=datetime,
            rejected=rejected,
            commission=self.commission,
        )

        return fill_event


