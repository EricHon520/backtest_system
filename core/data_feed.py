from abc import ABC, abstractmethod
from typing import List
from core.types import BarData


class DataFeed(ABC):
    """
    Abstract base class for all data sources.

    Decouples Engine / Portfolio / Strategy / ExecutionModel from
    the concrete DataHandler so that:
      - unit tests can inject a lightweight mock
      - live trading can plug in a real-time feed without touching Engine
      - multiple data sources can be composed transparently
    """

    @property
    @abstractmethod
    def symbols(self) -> List[str]:
        """Return the list of symbols this feed covers."""
        pass

    @abstractmethod
    def update_bars(self) -> bool:
        """
        Advance the feed by one bar.
        Returns True if data was emitted, False when the feed is exhausted.
        """
        pass

    @abstractmethod
    def get_latest_bar(self, symbol: str) -> BarData | None:
        """Return the most recently emitted bar for *symbol*, or None."""
        pass

    @abstractmethod
    def get_latest_bars(self, symbol: str, num_bars: int) -> List[BarData]:
        """Return the last *num_bars* bars for *symbol* (oldest first)."""
        pass
