from abc import abstractmethod, ABC
from typing import List

from market_rule import MarketRule

class Instrument(ABC):
    def __init__(self, symbol: str, market_rule: MarketRule, contract_multiplier: int, currency: str):
        self.symbol = symbol
        self.market_rule = market_rule
        self.contract_multiplier = contract_multiplier
        self.currency = currency

    @abstractmethod
    def is_expired(self, current_time: int) -> bool:
        pass

class InstrumentRegistry:
    def __init__(self):
        self.instrument_map = {}

    def register(self, instrument: Instrument):
        self.instrument_map[instrument.symbol] = instrument

    def get(self, symbol: str) -> Instrument:
        if symbol not in self.instrument_map:
            return None
        
        return self.instrument_map[symbol]

    def get_all(self) -> List[Instrument]:
        all_instruments = []
        for symbol in self.instrument_map:
            all_instruments.append(self.instrument_map[symbol])

        return all_instruments