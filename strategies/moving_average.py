from core.strategy import Strategy
from core.data_feed import DataFeed
from typing import List
from core.event import SignalEvent, MarketEvent
from datetime import datetime

class MovingAverage(Strategy):
    def __init__(self, data_handler: DataFeed, short_window=5, long_window=10):
        super().__init__(data_handler=data_handler)
        self.short_window = short_window
        self.long_window = long_window

    def calculate_signal(self, event: MarketEvent) -> List[SignalEvent]:
        signals = []

        for symbol in event.symbols:
            bars = self.data_handler.get_latest_bars(symbol=symbol, num_bars=self.long_window)

            if len(bars) >= self.long_window:
                short_ma = sum(bar['close'] for bar in bars[-self.short_window:]) / self.short_window
                long_ma  = sum(bar['close'] for bar in bars) / self.long_window

                if short_ma > long_ma:
                    # Emit EXIT first so any existing short position is closed
                    # before the LONG signal is sized into a new position.
                    signals.append(SignalEvent(symbol=symbol, datetime=event.datetime, signal_type='EXIT', strength=1.0))
                    signals.append(SignalEvent(symbol=symbol, datetime=event.datetime, signal_type='LONG', strength=1.0))
                elif short_ma < long_ma:
                    # Emit EXIT first so any existing long position is closed.
                    signals.append(SignalEvent(symbol=symbol, datetime=event.datetime, signal_type='EXIT', strength=1.0))
                    signals.append(SignalEvent(symbol=symbol, datetime=event.datetime, signal_type='SHORT', strength=1.0))
                else:
                    signals.append(SignalEvent(symbol=symbol, datetime=event.datetime, signal_type='EXIT', strength=1.0))

        return signals if signals else None
