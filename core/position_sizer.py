from abc import ABC, abstractmethod
from typing import Optional
from core.event import SignalEvent, OrderEvent
from core.portfolio_context import PortfolioContext


class PositionSizer(ABC):
    """
    Abstract base class for position sizing.

    Receives a SignalEvent and portfolio state, returns an OrderEvent
    (or None if no order should be placed).

    Separates capital allocation logic from Portfolio bookkeeping.
    """

    @abstractmethod
    def size_order(
        self,
        signal_event: SignalEvent,
        context: PortfolioContext,
        latest_bar: dict,
        contract_multiplier: int,
        lot_size: int,
    ) -> Optional[OrderEvent]:
        """
        Args:
            signal_event:        The incoming signal.
            context:             Snapshot of current portfolio state.
            latest_bar:          Latest OHLCV bar for the signal's symbol.
            contract_multiplier: Multiplier for futures / leveraged products.
            lot_size:            Minimum tradeable lot (e.g. 100 for A-shares).

        Returns:
            OrderEvent or None.
        """
        pass

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _round_to_lot(quantity: int, lot_size: int) -> int:
        if lot_size <= 1:
            return quantity
        return (quantity // lot_size) * lot_size

    @staticmethod
    def _direction(signal_type: str) -> str:
        return 'BUY' if signal_type == 'LONG' else 'SELL'

    @staticmethod
    def _exit_order(
        signal_event: SignalEvent,
        context: PortfolioContext,
    ) -> Optional[OrderEvent]:
        """
        Build a flat-position (EXIT) order from the current holding.

        For long positions the sell quantity is capped to 'available' so that
        T+1 / T+2 markets cannot sell shares that have not yet settled.
        Returns None if there is no position to close.
        """
        symbol = signal_event.symbol
        holding = context.current_holdings.get(symbol, {})
        held_qty = holding.get('quantity', 0)
        if held_qty == 0:
            return None

        if held_qty > 0:
            direction = 'SELL'
            # For long positions respect the settled (available) quantity so
            # that T+1 / T+2 markets do not sell unsettled shares.
            available = holding.get('available', held_qty)
            qty = min(abs(held_qty), available)
        else:
            direction = 'BUY'
            qty = abs(held_qty)

        if qty == 0:
            return None

        return OrderEvent(
            symbol=symbol,
            quantity=qty,
            direction=direction,
            datetime=signal_event.datetime,
        )


class FixedQuantityPositionSizer(PositionSizer):
    """
    Always trades a fixed number of units (scaled by signal confidence).

    Example:
        sizer = FixedQuantityPositionSizer(quantity=100)
        # confidence=0.5 → trades 50 units (rounded to lot_size)
    """

    def __init__(self, quantity: int):
        self.quantity = quantity

    def size_order(
        self,
        signal_event: SignalEvent,
        context: PortfolioContext,
        latest_bar: dict,
        contract_multiplier: int,
        lot_size: int,
    ) -> Optional[OrderEvent]:
        if signal_event.signal_type == 'EXIT':
            return self._exit_order(signal_event, context)

        raw_qty = int(self.quantity * signal_event.confidence)
        qty = self._round_to_lot(raw_qty, lot_size)
        if qty == 0:
            return None

        price = latest_bar.get('close', 0)
        cost = qty * price * contract_multiplier
        if cost > context.current_cash:
            return None

        return OrderEvent(
            symbol=signal_event.symbol,
            quantity=qty,
            direction=self._direction(signal_event.signal_type),
            datetime=signal_event.datetime,
        )


class PercentOfEquityPositionSizer(PositionSizer):
    """
    Allocates a fixed percentage of total equity per signal,
    further scaled by signal confidence.

    Example:
        sizer = PercentOfEquityPositionSizer(percent=0.10)
        # 10% of equity per signal; confidence=0.5 → 5% allocated
    """

    def __init__(self, percent: float):
        if not 0.0 < percent <= 1.0:
            raise ValueError("percent must be in (0, 1]")
        self.percent = percent

    def size_order(
        self,
        signal_event: SignalEvent,
        context: PortfolioContext,
        latest_bar: dict,
        contract_multiplier: int,
        lot_size: int,
    ) -> Optional[OrderEvent]:
        if signal_event.signal_type == 'EXIT':
            return self._exit_order(signal_event, context)

        price = latest_bar.get('close', 0)
        if price <= 0:
            return None

        alloc = context.current_cash * self.percent * signal_event.confidence
        raw_qty = int(alloc / (price * contract_multiplier))
        qty = self._round_to_lot(raw_qty, lot_size)
        if qty == 0:
            return None

        return OrderEvent(
            symbol=signal_event.symbol,
            quantity=qty,
            direction=self._direction(signal_event.signal_type),
            datetime=signal_event.datetime,
        )


class EqualWeightPositionSizer(PositionSizer):
    """
    Divides equity equally among `n_positions` slots.
    Useful for multi-asset portfolio construction.

    Example:
        sizer = EqualWeightPositionSizer(n_positions=5)
        # Allocates 1/5 of equity to each open signal
    """

    def __init__(self, n_positions: int):
        if n_positions <= 0:
            raise ValueError("n_positions must be > 0")
        self.n_positions = n_positions

    def size_order(
        self,
        signal_event: SignalEvent,
        context: PortfolioContext,
        latest_bar: dict,
        contract_multiplier: int,
        lot_size: int,
    ) -> Optional[OrderEvent]:
        if signal_event.signal_type == 'EXIT':
            return self._exit_order(signal_event, context)

        price = latest_bar.get('close', 0)
        if price <= 0:
            return None

        slot_value = context.current_cash / self.n_positions * signal_event.confidence
        raw_qty = int(slot_value / (price * contract_multiplier))
        qty = self._round_to_lot(raw_qty, lot_size)
        if qty == 0:
            return None

        return OrderEvent(
            symbol=signal_event.symbol,
            quantity=qty,
            direction=self._direction(signal_event.signal_type),
            datetime=signal_event.datetime,
        )
