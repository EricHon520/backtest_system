from queue import Queue
from core.event import Event, EventType, MarketEvent, OrderEvent
from data.data_handler import DataHandler
from core.strategy import Strategy
from portfolio.portfolio import Portfolio
from execution.execution_handler import ExecutionHandler
from core.instrument import InstrumentRegistry
from datetime import datetime
from typing import List

class Engine:
    def __init__(self, data_handler: DataHandler, strategy: Strategy, portfolio: Portfolio, execution_handler:ExecutionHandler, instrument_registry: InstrumentRegistry):
        self.data_handler = data_handler
        self.strategy = strategy
        self.portfolio = portfolio
        self.queue = Queue()
        self.execution_handler = execution_handler
        self.instrument_registry = instrument_registry
        self.timezone = self.data_handler._timezone
        self.expired_symbols = set()

    def run(self):
        while self.data_handler.update_bars():
            updated_symbols = []
            for symbol in self.data_handler._symbols:
                bar = self.data_handler.get_latest_bar(symbol=symbol)
                updated_symbols.append(bar['ticker'])
                datetime = bar['datetime_local']
            market_event = MarketEvent(datetime=datetime, symbols=updated_symbols)
            self.queue.put(market_event)

            while not self.queue.empty():
                event = self.queue.get()
                self._process_event(event=event)

    def _process_event(self, event: Event):

        match event.event_type:
            case EventType.MARKET:
                fill_events = self.execution_handler.process_pending_orders()
                for fill_event in fill_events:
                    self.queue.put(fill_event)

                order_events = self._check_expirations(event.datetime)
                if order_events is not None:
                    for order_event in order_events:
                        self.queue.put(order_event)

                self.portfolio.update_timeindex(market_event=event)

                signal_events = self.strategy.calculate_signal(event=event)
                if signal_events is not None:
                    for signal_event in signal_events:
                        self.queue.put(signal_event)

            case EventType.SIGNAL:
                order_event = self.portfolio.process_signal_event(event=event)
                if order_event is not None:
                    self.queue.put(order_event)

            case EventType.ORDER:
                fill_event = self.execution_handler.process_order_event(event=event)
                if fill_event is not None:
                    self.queue.put(fill_event)

            case EventType.FILL:
                self.portfolio.process_fill_event(event=event)

    def _check_expirations(self, current_time: datetime) -> List[OrderEvent]:
        instruments = self.instrument_registry.get_all()

        order_events = []

        timestamp = current_time.timestamp()

        for instrument in instruments:
            if instrument.is_expired(current_time=timestamp):
                if instrument.symbol in self.expired_symbols:
                    continue

                holding = self.portfolio.get_holding(instrument.symbol)

                if holding is None:
                    continue

                quantity = holding['quantity']
                if quantity != 0:
                    direction = 'SELL' if quantity > 0 else 'BUY'

                    order = OrderEvent(
                        symbol=instrument.symbol,
                        quantity=abs(quantity),
                        direction=direction,
                        datetime=current_time
                    )

                    self.expired_symbols.add(instrument.symbol)

                    order_events.append(order)
        
        return order_events

