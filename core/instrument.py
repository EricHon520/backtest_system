from abc import abstractmethod, ABC
from typing import List
from core.market_rule import MarketRule


class Instrument(ABC):
    def __init__(self, symbol: str, market_rule: MarketRule, contract_multiplier: int, currency: str):
        self.symbol = symbol
        self.market_rule = market_rule
        self.contract_multiplier = contract_multiplier
        self.currency = currency

    @abstractmethod
    def is_expired(self, current_time: int) -> bool:
        pass


class Stock(Instrument):
    def __init__(self, symbol: str, market_rule: MarketRule, currency: str):
        super().__init__(
            symbol=symbol, market_rule=market_rule, 
            contract_multiplier=1, currency=currency
        )

    def is_expired(self, current_time: int) -> bool:
        return False


class Future(Instrument):
    def __init__(self, symbol: str, market_rule: MarketRule, contract_multiplier: int, currency: str, expiry_date: int):
        super().__init__(
            symbol=symbol, market_rule=market_rule, 
            contract_multiplier=contract_multiplier, currency=currency
        )
        self.expiry_date = expiry_date

    def is_expired(self, current_time: int) -> bool:
        if current_time >= self.expiry_date:
            return True
        else:
            return False


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

    @classmethod
    def create_default(cls, symbols: List[str], market_type: str) -> 'InstrumentRegistry':
        """
        Create an InstrumentRegistry with default Stock instruments for all symbols.
        For advanced use cases (futures, options, custom instruments), 
        create the registry manually instead.

        Args:
            symbols: List of trading symbols
            market_type: 'china_a', 'us_stock', 'hk_stock', 'crypto', etc.

        Returns:
            InstrumentRegistry with all symbols registered as Stock instruments
        """
        market_rule = MarketRule(market_type)
        currency = market_rule.currency

        registry = cls()
        for symbol in symbols:
            instrument = Stock(symbol=symbol, market_rule=market_rule, currency=currency)
            registry.register(instrument)

        return registry