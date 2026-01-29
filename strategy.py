from abc import ABC, abstractmethod
from data_handler import DataHandler
from typing import List
from event import SignalEvent, MarketEvent

class Strategy(ABC):
    def __init__(self, data_handler: DataHandler):
        self.data_handler = data_handler

    @abstractmethod
    def calculate_signal(self, event: MarketEvent) -> SignalEvent:
        pass
