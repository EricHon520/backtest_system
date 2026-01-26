from abc import ABC, abstractmethod
from data_handler import DataHandler
from typing import List
from event import SignalEvent

class Strategy(ABC):
    def __init__(self, data_handler: DataHandler, symbols: List[str]):
        self.data_handler = data_handler
        self.symbols = symbols

    @abstractmethod
    def calculate_signals(self) -> List[SignalEvent]:
        pass
