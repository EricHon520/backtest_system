from core.instrument import Instrument
from core.market_rule import MarketRule

class Stock(Instrument):
    def __init__(self, symbol: str, market_rule: MarketRule, currency: str):
        super().__init__(
            symbol=symbol, market_rule=market_rule, 
            contract_multiplier=1, currency=currency
            )

    def is_expired(self, current_time: int) -> bool:
        return False