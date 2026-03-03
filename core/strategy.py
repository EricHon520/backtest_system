from abc import ABC, abstractmethod
from core.data_feed import DataFeed
from typing import List, Optional
from core.event import SignalEvent, MarketEvent

class Strategy(ABC):
    def __init__(self, data_handler: Optional[DataFeed] = None):
        self.data_handler = data_handler

    @abstractmethod
    def calculate_signal(self, event: MarketEvent) -> Optional[List[SignalEvent]]:
        pass
