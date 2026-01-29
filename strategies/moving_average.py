from strategy import Strategy
from data_handler import DataHandler
from typing import List
from event import SignalEvent, MarketEvent
from datetime import datetime

class MovingAverage(Strategy):
    def __init__(self, data_handler: DataHandler, short_window=5, long_window=10):
        super().__init__(data_handler=data_handler)
        self.short_window = short_window
        self.long_window = long_window

    def calculate_signal(self, event: MarketEvent) -> SignalEvent:
        symbol = event.symbol

        bars = self.data_handler.get_latest_bars(symbol=symbol, num_bars=self.long_window)

        signal = None

        if len(bars) >= self.long_window:
            short_ma = sum([bar['close'] for bar in bars[-self.short_window:]]) / self.short_window
            long_ma = sum([bar['close'] for bar in bars]) / self.long_window

            print(f"[DEBUG] Bars: {len(bars)}, Short MA: {short_ma:.2f}, Long MA: {long_ma:.2f}")

            if short_ma > long_ma:
                signal = SignalEvent(symbol=symbol, datetime=event.datetime, signal_type='LONG', strength=1.0)
                print(f"[SIGNAL] LONG signal generated at {event.datetime}")
            elif short_ma < long_ma:
                signal = SignalEvent(symbol=symbol, datetime=event.datetime, signal_type='SHORT', strength=1.0)
                print(f"[SIGNAL] SHORT signal generated at {event.datetime}")

        return signal
