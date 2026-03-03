from abc import ABC, abstractmethod
from typing import Optional, List
from core.event import OrderEvent
from core.portfolio_context import PortfolioContext


class RiskManager(ABC):
    """
    Abstract base class for risk management.

    Sits between PositionSizer and ExecutionModel.
    Receives a proposed OrderEvent and may:
      - approve it (return as-is)
      - adjust it (reduce quantity, change direction)
      - reject it (return None)
    """

    @abstractmethod
    def evaluate(
        self,
        order_event: OrderEvent,
        context: PortfolioContext,
    ) -> Optional[OrderEvent]:
        """
        Args:
            order_event: The proposed order from PositionSizer.
            context:     Snapshot of current portfolio state.

        Returns:
            Approved/adjusted OrderEvent, or None to reject.
        """
        pass


class NullRiskManager(RiskManager):
    """Pass-through: approves every order unchanged."""

    def evaluate(self, order_event: OrderEvent,
                 context: PortfolioContext) -> Optional[OrderEvent]:
        return order_event


class CompositeRiskManager(RiskManager):
    """
    Chains multiple RiskManager instances.
    An order must pass ALL managers; first rejection wins.

    Example:
        rm = CompositeRiskManager([
            MaxDrawdownRiskManager(max_drawdown=0.20),
            MaxPositionSizeRiskManager(max_position_pct=0.30),
        ])
    """

    def __init__(self, managers: List[RiskManager]):
        self.managers = managers

    def evaluate(self, order_event: OrderEvent,
                 context: PortfolioContext) -> Optional[OrderEvent]:
        current = order_event
        for manager in self.managers:
            current = manager.evaluate(current, context)
            if current is None:
                return None
        return current


class MaxDrawdownRiskManager(RiskManager):
    """
    Blocks all new BUY orders when portfolio drawdown exceeds threshold.
    Existing positions are unaffected (no forced liquidation).

    Args:
        max_drawdown: e.g. 0.20 means block new buys after -20% drawdown
                      from the peak equity seen so far.
    """

    def __init__(self, max_drawdown: float):
        if not 0.0 < max_drawdown < 1.0:
            raise ValueError("max_drawdown must be in (0, 1)")
        self.max_drawdown = max_drawdown

    def evaluate(self, order_event: OrderEvent,
                 context: PortfolioContext) -> Optional[OrderEvent]:
        if context.current_drawdown >= self.max_drawdown and order_event.direction == 'BUY':
            return None
        return order_event


class MaxPositionSizeRiskManager(RiskManager):
    """
    Caps the notional exposure of any single position to a fraction
    of current total equity. Trims quantity to fit within the limit.

    Args:
        max_position_pct: e.g. 0.30 means no single symbol > 30% of equity.
    """

    def __init__(self, max_position_pct: float):
        if not 0.0 < max_position_pct <= 1.0:
            raise ValueError("max_position_pct must be in (0, 1]")
        self.max_position_pct = max_position_pct

    def evaluate(self, order_event: OrderEvent,
                 context: PortfolioContext) -> Optional[OrderEvent]:
        if order_event.direction != 'BUY':
            return order_event

        total_equity = context.total_equity
        if total_equity <= 0:
            return order_event

        max_notional = total_equity * self.max_position_pct
        symbol = order_event.symbol

        existing_notional = 0.0
        if context.all_holdings:
            existing_notional = context.all_holdings[-1].get(symbol + '_value', 0.0)
        remaining = max_notional - existing_notional

        if remaining <= 0:
            return None

        current_qty = context.current_holdings.get(symbol, {}).get('quantity', 0)
        if current_qty != 0 and existing_notional > 0:
            # Derive price from last known notional / quantity
            latest_price = existing_notional / abs(current_qty)
        elif order_event.limit_price is not None and order_event.limit_price > 0:
            latest_price = order_event.limit_price
        else:
            # No price information available — cannot enforce the cap on a
            # first-time entry; the PositionSizer should have already sized
            # the order within equity limits, so pass it through.
            return order_event

        max_qty = int(remaining / latest_price)
        if max_qty <= 0:
            return None

        if order_event.quantity > max_qty:
            return OrderEvent(
                symbol=order_event.symbol,
                quantity=max_qty,
                direction=order_event.direction,
                datetime=order_event.datetime,
                limit_price=order_event.limit_price,
                order_type=order_event.order_type,
            )

        return order_event


class MaxOpenPositionsRiskManager(RiskManager):
    """
    Blocks new BUY orders when the number of symbols with non-zero
    holdings already equals or exceeds `max_positions`.

    Args:
        max_positions: maximum number of concurrent open positions.
    """

    def __init__(self, max_positions: int):
        self.max_positions = max_positions

    def evaluate(self, order_event: OrderEvent,
                 context: PortfolioContext) -> Optional[OrderEvent]:
        if order_event.direction != 'BUY':
            return order_event

        symbol = order_event.symbol
        already_open = context.current_holdings.get(symbol, {}).get('quantity', 0) != 0

        if not already_open and context.open_position_count >= self.max_positions:
            return None

        return order_event
