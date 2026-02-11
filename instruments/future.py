from core.instrument import Instrument
from core.market_rule import MarketRule

class Future(Instrument):
    def __init__(self, symbol: str, market_rule:MarketRule, contract_multiplier: int, currency: str, expiry_date: int):
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