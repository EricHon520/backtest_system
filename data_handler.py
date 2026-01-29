from data_loader import DataLoader
from typing import List
from datetime import datetime

class DataHandler:
    def __init__(self, symbols: List[str], start_time: datetime, end_time: datetime, frequency: str, timezone: str, source: str):
        self._symbols = symbols
        self._start_time = start_time
        self._end_time = end_time
        self._frequency = frequency
        self._timezone = timezone
        self._source = source
        self._bar_index = 0

        self._data_loader = DataLoader()

        raw_data = self._data_loader.get_historical_data(tickers=symbols, start_time=start_time, end_time=end_time,
        frequency=frequency, timezone=timezone, source=source)

        self._symbols_data = {symbol: [] for symbol in symbols}
        for bar in raw_data:
            symbol = bar["ticker"]
            self._symbols_data[symbol].append(bar)
        for symbol in symbols:
            self._symbols_data[symbol].sort(key=lambda x: x["timestamp"])

        self._latest_symbols_data = {symbol: [] for symbol in symbols}

    def update_bars(self) -> bool:
        has_data = False

        for symbol in self._symbols:
            if len(self._symbols_data[symbol]) > self._bar_index:
                bar = self._symbols_data[symbol][self._bar_index]
                self._latest_symbols_data[symbol].append(bar)
                has_data = True

        if has_data:
            self._bar_index += 1
        
        return has_data
    
    def get_latest_bar(self, symbol: str) -> dict:
        if symbol not in self._latest_symbols_data:
            return None
        if len(self._latest_symbols_data[symbol]) == 0:
            return None
        if len(self._latest_symbols_data[symbol]) == self._bar_index:
            return self._latest_symbols_data[symbol][-1]
        return None

    def get_latest_bars(self, symbol: str, num_bars: int) -> List[dict]:
        if symbol not in self._latest_symbols_data:
            return []
        if len(self._latest_symbols_data[symbol]) < num_bars:
            return []
        if len(self._latest_symbols_data[symbol]) == self._bar_index:
            return self._latest_symbols_data[symbol][-num_bars:]
        return []


    