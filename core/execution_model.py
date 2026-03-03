from abc import ABC, abstractmethod
from typing import Optional
from core.event import OrderEvent, FillEvent


class ExecutionModel(ABC):
    """
    Abstract base class for all execution models.

    Each concrete implementation decides how an OrderEvent is translated
    into a FillEvent (price, timing, partial fill, rejection, etc.).
    """

    @abstractmethod
    def execute(self, order_event: OrderEvent) -> Optional[FillEvent]:
        """
        Synchronously execute an order and return a FillEvent.
        Return None if the order cannot be filled this bar.
        """
        pass

    def on_new_bar(self) -> list:
        """
        Called at the start of every new bar.
        Used by fill-on-next-bar models to flush pending orders.
        Returns a list of FillEvents.
        """
        return []
