import calendar
import logging
from queue import Queue
from core.event import Event, EventType, MarketEvent, OrderEvent, SignalEvent
from core.data_feed import DataFeed
from core.strategy import Strategy
from core.position_sizer import PositionSizer, PercentOfEquityPositionSizer
from core.portfolio_context import PortfolioContext
from risk.risk_manager import RiskManager, NullRiskManager
from portfolio.portfolio import Portfolio
from core.execution_model import ExecutionModel
from core.instrument import InstrumentRegistry
from core.signal_aggregator import SignalAggregator
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union


class Engine:
    """
    Event-driven backtest engine.

    Flow per bar:
      MarketEvent
        → ExecutionModel.on_new_bar()   (flush pending orders → FillEvents)
        → _check_expirations()          (auto-close expired futures → OrderEvents)
        → Portfolio.update_timeindex()  (mark-to-market snapshot)
        → Strategy[i].calculate_signal() for each strategy → SignalEvents
      SignalEvent
        → PositionSizer.size_order()    (signal + portfolio state → proposed OrderEvent)
        → RiskManager.evaluate()        (approve / adjust / reject)
        → queue OrderEvent if approved
      OrderEvent
        → ExecutionModel.execute()      (queue for next-bar fill)
      FillEvent
        → Portfolio.process_fill_event()
    """

    def __init__(
        self,
        data_handler: DataFeed,
        portfolio: Portfolio,
        execution_handler: ExecutionModel,
        instrument_registry: InstrumentRegistry,
        strategies: Union[Strategy, List[Strategy]],
        position_sizer: Optional[PositionSizer] = None,
        risk_manager: Optional[RiskManager] = None,
        signal_aggregator: Optional[SignalAggregator] = None,
    ):
        self.data_handler = data_handler
        self.portfolio = portfolio
        self.execution_handler = execution_handler
        self.instrument_registry = instrument_registry

        # Accept a single strategy or a list
        if isinstance(strategies, list):
            self.strategies = strategies
        else:
            self.strategies = [strategies]

        # Default to 10%-of-equity sizing if none provided
        self.position_sizer = position_sizer or PercentOfEquityPositionSizer(percent=0.10)
        self.risk_manager = risk_manager or NullRiskManager()

        self.queue = Queue()
        self.timezone = getattr(self.data_handler, '_timezone', 'UTC')
        self.expired_symbols = set()
        self.logger = logging.getLogger(__name__)
        self.signal_aggregator = signal_aggregator or SignalAggregator()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def run(self):
        while self.data_handler.update_bars():
            updated_symbols = []
            current_datetime = None
            current_prices: Dict[str, float] = {}

            for symbol in self.data_handler.symbols:
                bar = self.data_handler.get_latest_bar(symbol=symbol)
                if bar is not None:
                    updated_symbols.append(bar['ticker'])
                    current_datetime = bar['datetime_local']
                    if 'close' in bar:
                        current_prices[bar['ticker']] = bar['close']

            if current_datetime is None:
                continue

            market_event = MarketEvent(datetime=current_datetime, symbols=updated_symbols)
            market_event.current_prices = current_prices  # carry prices alongside event
            self.queue.put(market_event)

            while not self.queue.empty():
                event = self.queue.get()
                try:
                    self._process_event(event=event)
                except Exception as exc:
                    self.logger.error(
                        "Unhandled exception processing %s event: %s",
                        event.event_type, exc, exc_info=True
                    )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _process_event(self, event: Event):
        match event.event_type:
            case EventType.MARKET:
                # 1. Flush pending orders from the previous bar into the queue.
                #    fill_on_next_bar semantics: these trades execute at the
                #    open of the *current* bar, so their fills must be
                #    processed before the MTM snapshot for this bar.
                fill_events = self.execution_handler.on_new_bar()
                for fill_event in fill_events:
                    self.queue.put(fill_event)

                # 2. Auto-close expired instruments.  Their fills also need to
                #    land before the MTM snapshot.
                order_events = self._check_expirations(event.datetime)
                for order_event in order_events:
                    expiry_fill = self.execution_handler._execute_order(order_event)
                    if expiry_fill is not None:
                        self.queue.put(expiry_fill)

                # 3. Drain every FILL event that is already in the queue so
                #    that update_timeindex sees the correct post-fill holdings.
                #    Non-FILL events stay in the queue and are handled in the
                #    normal event loop below — event-driven order is preserved.
                self._drain_fills()

                # 4. Mark-to-market snapshot (pass prices so Portfolio is data-feed-agnostic)
                current_prices = getattr(event, 'current_prices', None)
                self.portfolio.update_timeindex(market_event=event,
                                                current_prices=current_prices)

                # 5. Run all strategies, collect signals
                raw_signals: List[SignalEvent] = []
                for strategy in self.strategies:
                    try:
                        result = strategy.calculate_signal(event=event)
                    except Exception as exc:
                        self.logger.error(
                            "Strategy %s raised an exception: %s",
                            type(strategy).__name__, exc, exc_info=True
                        )
                        continue
                    if result is None:
                        continue
                    if isinstance(result, list):
                        raw_signals.extend(sig for sig in result if sig is not None)
                    else:
                        raw_signals.append(result)

                # 5. Aggregate signals (resolve conflicts) then enqueue
                aggregated = self.signal_aggregator.aggregate(raw_signals)
                for sig in aggregated:
                    self.queue.put(sig)

            case EventType.SIGNAL:
                self._handle_signal(event)

            case EventType.ORDER:
                self.execution_handler.execute(event)

            case EventType.FILL:
                self.portfolio.process_fill_event(event)

            case _:
                self.logger.warning("Unhandled event type: %s", event.event_type)

    def _handle_signal(self, signal_event: SignalEvent):
        """Route a signal through PositionSizer → RiskManager → queue."""
        # Drop stale signals (compare datetime objects; skip if expiry is set)
        if signal_event.expiry is not None:
            current_dt = self._coerce_datetime(signal_event.datetime)
            if current_dt is not None and signal_event.is_expired(current_dt):
                return

        symbol = signal_event.symbol
        instrument = self.instrument_registry.get(symbol)
        if instrument is None:
            return

        latest_bar = self.data_handler.get_latest_bar(symbol)
        if latest_bar is None:
            return

        market_rule = instrument.market_rule

        # Build immutable portfolio snapshot
        context = PortfolioContext(
            current_cash=self.portfolio.current_cash,
            initial_capital=self.portfolio.initial_capital,
            current_holdings=self.portfolio.current_holdings,
            all_holdings=self.portfolio.all_holdings,
            margin_used=self.portfolio.margin_used,
        )

        # PositionSizer determines quantity
        proposed_order = self.position_sizer.size_order(
            signal_event=signal_event,
            context=context,
            latest_bar=latest_bar,
            contract_multiplier=instrument.contract_multiplier,
            lot_size=market_rule.lot_size,
        )

        if proposed_order is None:
            return

        # RiskManager approves / adjusts / rejects
        final_order = self.risk_manager.evaluate(
            order_event=proposed_order,
            context=context,
        )

        if final_order is not None:
            self.queue.put(final_order)

    def _drain_fills(self) -> None:
        """
        Process every FILL event currently sitting in the queue, leaving all
        other event types in place so they are handled in normal order.

        This is called inside the MARKET handler — after pending fills have
        been queued but before the MTM snapshot — so that update_timeindex
        always reflects post-fill holdings.  FillEvents still travel through
        the queue (event-driven principle is preserved); we simply prioritise
        them relative to the MTM step.
        """
        deferred = []
        while not self.queue.empty():
            ev = self.queue.get()
            if ev.event_type == EventType.FILL:
                try:
                    self.portfolio.process_fill_event(ev)
                except Exception as exc:
                    self.logger.error(
                        "Unhandled exception processing FILL event: %s",
                        exc, exc_info=True
                    )
            else:
                deferred.append(ev)
        for ev in deferred:
            self.queue.put(ev)

    @staticmethod
    def _coerce_datetime(dt) -> Optional[datetime]:
        """Return a timezone-naive datetime from str, datetime, or None."""
        if isinstance(dt, datetime):
            return dt.replace(tzinfo=None)
        if isinstance(dt, str):
            cleaned = dt.rsplit(' ', 1)[0] if ' ' in dt else dt
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                try:
                    return datetime.strptime(cleaned, fmt)
                except ValueError:
                    continue
        return None

    def _check_expirations(self, current_time) -> List[OrderEvent]:
        instruments = self.instrument_registry.get_all()
        order_events = []

        # Normalise to Unix timestamp for Instrument.is_expired(int)
        # Use calendar.timegm so the naive datetime is always treated as UTC,
        # regardless of the host machine's local timezone.
        current_dt = self._coerce_datetime(current_time)
        if current_dt is None:
            return order_events
        timestamp = calendar.timegm(current_dt.timetuple())

        for instrument in instruments:
            if instrument.is_expired(current_time=timestamp):
                if instrument.symbol in self.expired_symbols:
                    continue

                holding = self.portfolio.get_holding(instrument.symbol)

                if holding is None:
                    continue

                quantity = holding['quantity']
                if quantity != 0:
                    direction = 'SELL' if quantity > 0 else 'BUY'

                    order = OrderEvent(
                        symbol=instrument.symbol,
                        quantity=abs(quantity),
                        direction=direction,
                        datetime=current_time
                    )

                    self.expired_symbols.add(instrument.symbol)

                    order_events.append(order)
        
        return order_events

