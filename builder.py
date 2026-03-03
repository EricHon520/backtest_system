"""
BacktestBuilder — fluent builder for the backtest engine.

Typical usage (strategy class, auto-inject data_handler)::

    from builder import BacktestBuilder
    from strategies.my_strategy import MyStrategy

    engine = (
        BacktestBuilder()
        .set_data(symbols=['600519.SS'],
                  start='2024-01-01', end='2024-12-31',
                  frequency='1d', timezone='Asia/Shanghai', source='stock')
        .set_market('china_a')
        .set_capital(100_000)
        .add_strategy(MyStrategy, short_window=5, long_window=20)
        .set_position_sizer('percent', percent=0.10)
        .set_risk_manager('max_drawdown', max_drawdown=0.20)
        .build()
    )
    engine.run()

You can also pass pre-built instances directly::

    sizer = PercentOfEquityPositionSizer(percent=0.10)
    engine = (
        BacktestBuilder()
        ...
        .set_position_sizer(sizer)
        .build()
    )

Available shortcut strings
--------------------------
set_position_sizer  : 'percent'  (PercentOfEquityPositionSizer)
                      'fixed'    (FixedQuantityPositionSizer)
                      'equal'    (EqualWeightPositionSizer)

set_risk_manager    : 'max_drawdown'    (MaxDrawdownRiskManager)
                      'max_positions'   (MaxOpenPositionsRiskManager)
                      'max_position_pct'(MaxPositionSizeRiskManager)
                      'composite'       (CompositeRiskManager — pass managers=[...])
                      'null'            (NullRiskManager)

set_execution_model : 'simulated'  (SimulatedExecutionModel)

set_signal_aggregator: 'weighted'         (WeightedAggregator / default)
                       'first_wins'       (FirstWinsAggregator)
                       'last_wins'        (LastWinsAggregator)
                       'majority'         (MajorityVoteAggregator)
                       'veto_on_conflict' (VetoOnConflictAggregator)
"""

from __future__ import annotations

import inspect
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Type, Union

from engine import Engine
from data.data_handler import DataHandler
from core.data_feed import DataFeed
from core.strategy import Strategy
from core.instrument import InstrumentRegistry
from core.execution_model import ExecutionModel
from core.position_sizer import (
    PositionSizer,
    PercentOfEquityPositionSizer,
    FixedQuantityPositionSizer,
    EqualWeightPositionSizer,
)
from core.signal_aggregator import (
    SignalAggregatorBase,
    SignalAggregator,
    FirstWinsAggregator,
    LastWinsAggregator,
    MajorityVoteAggregator,
    WeightedAggregator,
    VetoOnConflictAggregator,
)
from execution.execution_handler import SimulatedExecutionModel
from portfolio.portfolio import Portfolio
from risk.risk_manager import (
    RiskManager,
    NullRiskManager,
    CompositeRiskManager,
    MaxDrawdownRiskManager,
    MaxOpenPositionsRiskManager,
    MaxPositionSizeRiskManager,
)


# ---------------------------------------------------------------------------
# Shortcut registries
# ---------------------------------------------------------------------------

_POSITION_SIZER_MAP: Dict[str, type] = {
    'percent': PercentOfEquityPositionSizer,
    'fixed':   FixedQuantityPositionSizer,
    'equal':   EqualWeightPositionSizer,
}

_RISK_MANAGER_MAP: Dict[str, type] = {
    'max_drawdown':     MaxDrawdownRiskManager,
    'max_positions':    MaxOpenPositionsRiskManager,
    'max_position_pct': MaxPositionSizeRiskManager,
    'composite':        CompositeRiskManager,
    'null':             NullRiskManager,
}

_SIGNAL_AGGREGATOR_MAP: Dict[str, type] = {
    'weighted':          WeightedAggregator,
    'first_wins':        FirstWinsAggregator,
    'last_wins':         LastWinsAggregator,
    'majority':          MajorityVoteAggregator,
    'veto_on_conflict':  VetoOnConflictAggregator,
}


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------

class BacktestBuilder:
    """
    Fluent builder that assembles a backtest Engine.

    All ``set_*`` / ``add_*`` methods return ``self`` to support chaining.
    Call ``build()`` at the end to get a ready-to-run ``Engine`` instance.
    """

    def __init__(self):
        # --- data ---
        self._data_feed: Optional[DataFeed] = None
        self._data_kwargs: Optional[Dict[str, Any]] = None

        # --- market / instruments ---
        self._market_type: Optional[str] = None
        self._instrument_registry: Optional[InstrumentRegistry] = None

        # --- portfolio ---
        self._initial_capital: float = 100_000.0

        # --- strategies: list of (class_or_instance, kwargs) ---
        self._strategy_entries: List[tuple] = []

        # --- execution model ---
        self._execution_model: Optional[ExecutionModel] = None
        self._execution_kwargs: Dict[str, Any] = {}

        # --- position sizer ---
        self._position_sizer: Optional[PositionSizer] = None

        # --- risk manager ---
        self._risk_manager: Optional[RiskManager] = None

        # --- signal aggregator ---
        self._signal_aggregator: Optional[SignalAggregatorBase] = None

        # --- logging ---
        self._log_level: int = logging.INFO
        self._log_format: str = '%(asctime)s - %(levelname)s - %(message)s'

    # ------------------------------------------------------------------
    # Data
    # ------------------------------------------------------------------

    def set_data(
        self,
        symbols: List[str],
        start: Union[str, datetime],
        end: Union[str, datetime],
        frequency: str = '1d',
        timezone: str = 'UTC',
        source: str = 'stock',
    ) -> 'BacktestBuilder':
        """Configure the data source using DataHandler shortcuts."""
        self._data_kwargs = dict(
            symbols=symbols,
            start_time=_parse_date(start),
            end_time=_parse_date(end),
            frequency=frequency,
            timezone=timezone,
            source=source,
        )
        return self

    def set_data_feed(self, data_feed: DataFeed) -> 'BacktestBuilder':
        """Provide a pre-built DataFeed instance directly."""
        self._data_feed = data_feed
        return self

    # ------------------------------------------------------------------
    # Market / Instruments
    # ------------------------------------------------------------------

    def set_market(self, market_type: str) -> 'BacktestBuilder':
        """
        Set market type shortcut used to build InstrumentRegistry.
        e.g. 'china_a', 'us_stock', 'hk_stock', 'china_future'
        """
        self._market_type = market_type
        return self

    def set_instrument_registry(
        self, registry: InstrumentRegistry
    ) -> 'BacktestBuilder':
        """Provide a pre-built InstrumentRegistry (overrides set_market)."""
        self._instrument_registry = registry
        return self

    # ------------------------------------------------------------------
    # Capital
    # ------------------------------------------------------------------

    def set_capital(self, initial_capital: float) -> 'BacktestBuilder':
        """Set the initial capital for the portfolio."""
        self._initial_capital = initial_capital
        return self

    # ------------------------------------------------------------------
    # Strategies
    # ------------------------------------------------------------------

    def add_strategy(
        self,
        strategy: Union[Type[Strategy], Strategy],
        **kwargs: Any,
    ) -> 'BacktestBuilder':
        """
        Add a strategy.

        Two modes:
        - Pass a **class** + keyword args → Builder instantiates it at
          ``build()`` time and auto-injects ``data_handler`` if the class
          constructor accepts it.
        - Pass an already-instantiated **object** → used as-is; kwargs
          are ignored.

        Can be called multiple times to add multiple strategies.
        """
        self._strategy_entries.append((strategy, kwargs))
        return self

    def set_strategy(
        self,
        strategy: Union[Type[Strategy], Strategy, List],
        **kwargs: Any,
    ) -> 'BacktestBuilder':
        """
        Set strategy/strategies, replacing any previously added ones.

        Accepts a single class, a single instance, or a list of either.
        """
        self._strategy_entries.clear()
        if isinstance(strategy, list):
            for s in strategy:
                self._strategy_entries.append((s, {}))
        else:
            self._strategy_entries.append((strategy, kwargs))
        return self

    # ------------------------------------------------------------------
    # Execution model
    # ------------------------------------------------------------------

    def set_execution_model(
        self,
        execution_model: Union[str, ExecutionModel] = 'simulated',
        **kwargs: Any,
    ) -> 'BacktestBuilder':
        """
        Set execution model.

        Shortcut strings: ``'simulated'``
        Or pass a pre-built ``ExecutionModel`` instance.
        ``fill_on_next_bar`` defaults to ``True`` for the simulated model.
        """
        if isinstance(execution_model, str):
            if execution_model != 'simulated':
                raise ValueError(
                    f"Unknown execution_model shortcut '{execution_model}'. "
                    "Use 'simulated' or pass an ExecutionModel instance."
                )
            self._execution_model = None  # built in build()
            self._execution_kwargs = kwargs
        else:
            self._execution_model = execution_model
        return self

    # ------------------------------------------------------------------
    # Position sizer
    # ------------------------------------------------------------------

    def set_position_sizer(
        self,
        position_sizer: Union[str, PositionSizer],
        **kwargs: Any,
    ) -> 'BacktestBuilder':
        """
        Set position sizer.

        Shortcut strings: ``'percent'``, ``'fixed'``, ``'equal'``
        Or pass a pre-built ``PositionSizer`` instance.
        """
        if isinstance(position_sizer, str):
            cls = _POSITION_SIZER_MAP.get(position_sizer)
            if cls is None:
                raise ValueError(
                    f"Unknown position_sizer shortcut '{position_sizer}'. "
                    f"Available: {list(_POSITION_SIZER_MAP)}"
                )
            self._position_sizer = cls(**kwargs)
        else:
            self._position_sizer = position_sizer
        return self

    # ------------------------------------------------------------------
    # Risk manager
    # ------------------------------------------------------------------

    def set_risk_manager(
        self,
        risk_manager: Union[str, RiskManager],
        **kwargs: Any,
    ) -> 'BacktestBuilder':
        """
        Set a single risk manager or replace the existing one.

        Shortcut strings: ``'max_drawdown'``, ``'max_positions'``,
        ``'max_position_pct'``, ``'composite'``, ``'null'``
        Or pass a pre-built ``RiskManager`` instance.
        """
        if isinstance(risk_manager, str):
            cls = _RISK_MANAGER_MAP.get(risk_manager)
            if cls is None:
                raise ValueError(
                    f"Unknown risk_manager shortcut '{risk_manager}'. "
                    f"Available: {list(_RISK_MANAGER_MAP)}"
                )
            self._risk_manager = cls(**kwargs)
        else:
            self._risk_manager = risk_manager
        return self

    def add_risk_manager(
        self,
        risk_manager: Union[str, RiskManager],
        **kwargs: Any,
    ) -> 'BacktestBuilder':
        """
        Add a risk manager, composing it with any previously set manager
        via ``CompositeRiskManager``.

        Useful for chaining multiple risk checks without manually building
        a ``CompositeRiskManager``::

            builder
            .add_risk_manager('max_drawdown', max_drawdown=0.20)
            .add_risk_manager('max_positions', max_positions=3)
        """
        if isinstance(risk_manager, str):
            cls = _RISK_MANAGER_MAP.get(risk_manager)
            if cls is None:
                raise ValueError(
                    f"Unknown risk_manager shortcut '{risk_manager}'. "
                    f"Available: {list(_RISK_MANAGER_MAP)}"
                )
            new_rm = cls(**kwargs)
        else:
            new_rm = risk_manager

        if self._risk_manager is None:
            self._risk_manager = new_rm
        elif isinstance(self._risk_manager, CompositeRiskManager):
            self._risk_manager.managers.append(new_rm)
        else:
            self._risk_manager = CompositeRiskManager(
                [self._risk_manager, new_rm]
            )
        return self

    # ------------------------------------------------------------------
    # Signal aggregator
    # ------------------------------------------------------------------

    def set_signal_aggregator(
        self,
        aggregator: Union[str, SignalAggregatorBase],
        **kwargs: Any,
    ) -> 'BacktestBuilder':
        """
        Set signal aggregator.

        Shortcut strings: ``'weighted'`` (default), ``'first_wins'``,
        ``'last_wins'``, ``'majority'``, ``'veto_on_conflict'``
        Or pass a pre-built ``SignalAggregatorBase`` instance.
        """
        if isinstance(aggregator, str):
            cls = _SIGNAL_AGGREGATOR_MAP.get(aggregator)
            if cls is None:
                raise ValueError(
                    f"Unknown signal_aggregator shortcut '{aggregator}'. "
                    f"Available: {list(_SIGNAL_AGGREGATOR_MAP)}"
                )
            self._signal_aggregator = cls(**kwargs)
        else:
            self._signal_aggregator = aggregator
        return self

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def set_logging(
        self,
        level: int = logging.INFO,
        fmt: str = '%(asctime)s - %(levelname)s - %(message)s',
    ) -> 'BacktestBuilder':
        """Configure root logger. Called automatically by build() if not set."""
        self._log_level = level
        self._log_format = fmt
        return self

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    @staticmethod
    def list_options() -> None:
        """Print all available shortcut strings for each configurable component."""
        sections = [
            ('set_position_sizer', _POSITION_SIZER_MAP),
            ('set_risk_manager / add_risk_manager', _RISK_MANAGER_MAP),
            ('set_signal_aggregator', _SIGNAL_AGGREGATOR_MAP),
            ('set_execution_model', {'simulated': SimulatedExecutionModel}),
        ]
        for title, mapping in sections:
            print(f"\n{title}:")
            for key, cls in mapping.items():
                print(f"  '{key}' → {cls.__name__}")

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self) -> Engine:
        """
        Validate configuration and assemble the ``Engine``.

        Raises ``ValueError`` if required components are missing.
        """
        logging.basicConfig(
            level=self._log_level,
            format=self._log_format,
            handlers=[logging.StreamHandler()],
        )

        # --- 1. Data feed ---
        data_feed = self._build_data_feed()

        # --- 2. Instrument registry ---
        registry = self._build_registry(data_feed)

        # --- 3. Portfolio ---
        portfolio = Portfolio(
            initial_capital=self._initial_capital,
            instrument_registry=registry,
        )

        # --- 4. Execution model ---
        execution_model = self._build_execution_model(data_feed, registry)

        # --- 5. Strategies (auto-inject data_handler) ---
        strategies = self._build_strategies(data_feed)

        # --- 6. Optional components (use Engine defaults if not set) ---
        position_sizer = self._position_sizer
        risk_manager = self._risk_manager
        signal_aggregator = self._signal_aggregator

        return Engine(
            data_handler=data_feed,
            portfolio=portfolio,
            execution_handler=execution_model,
            instrument_registry=registry,
            strategies=strategies,
            position_sizer=position_sizer,
            risk_manager=risk_manager,
            signal_aggregator=signal_aggregator,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_data_feed(self) -> DataFeed:
        if self._data_feed is not None:
            return self._data_feed
        if self._data_kwargs is not None:
            return DataHandler(**self._data_kwargs)
        raise ValueError(
            "No data source configured. "
            "Call set_data() or set_data_feed() before build()."
        )

    def _build_registry(self, data_feed: DataFeed) -> InstrumentRegistry:
        if self._instrument_registry is not None:
            return self._instrument_registry
        if self._market_type is not None:
            return InstrumentRegistry.create_default(
                symbols=data_feed.symbols,
                market_type=self._market_type,
            )
        raise ValueError(
            "No market/instrument configuration found. "
            "Call set_market() or set_instrument_registry() before build()."
        )

    def _build_execution_model(
        self, data_feed: DataFeed, registry: InstrumentRegistry
    ) -> ExecutionModel:
        if self._execution_model is not None:
            return self._execution_model
        kwargs = {'fill_on_next_bar': True}
        kwargs.update(self._execution_kwargs)
        return SimulatedExecutionModel(
            data_handler=data_feed,
            instrument_registry=registry,
            **kwargs,
        )

    def _build_strategies(self, data_feed: DataFeed) -> List[Strategy]:
        if not self._strategy_entries:
            raise ValueError(
                "No strategy configured. "
                "Call add_strategy() or set_strategy() before build()."
            )

        strategies: List[Strategy] = []
        for entry, kwargs in self._strategy_entries:
            if isinstance(entry, Strategy):
                strategies.append(entry)
            elif inspect.isclass(entry) and issubclass(entry, Strategy):
                init_params = inspect.signature(entry.__init__).parameters
                if 'data_handler' in init_params:
                    strategies.append(entry(data_handler=data_feed, **kwargs))
                else:
                    strategies.append(entry(**kwargs))
            else:
                raise TypeError(
                    f"Expected a Strategy subclass or instance, got {type(entry)}."
                )
        return strategies


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(value: Union[str, datetime]) -> datetime:
    if isinstance(value, datetime):
        return value
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(
        f"Cannot parse date string '{value}'. "
        "Expected 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'."
    )
