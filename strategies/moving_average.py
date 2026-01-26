from strategy import Strategy
from data_handler import DataHandler
from typing import List
from event import SignalEvent
from datetime import datetime

class MovingAverage(Strategy):
    def __init__(self, data_handler: DataHandler, symbols: List[str], short_window=10, long_window=30):
        super().__init__(data_handler=data_handler, symbols=symbols)
        self.short_window = short_window
        self.long_window = long_window

    def calculate_signals(self) -> List[SignalEvent]:
        signals = []

        for symbol in symbols:
            bars = self.data_handler.get_latest_bars(symbol=symbol, num_bars=self.long_window)

            if len(bars) >= 0:
                short_ma = sum([bar['close'] for bar in bars[-self.short_window:]]) / self.short_window
                long_ma = sum([bar['close'] for bar in bars]) / self.long_window

                if short_ma > long_ma:
                    signal = SignalEvent(symbol=symbol, datetime=datetime.now(), signal_type='LONG', strength=1.0)
                    signals.append(signal)
                elif short_ma < long_ma:
                    signal = SignalEvent(symbol=symbol, datetime=datetime.now(), signal_type='SHORT', strength=1.0)
                    signals.append(signal)

        return signals
