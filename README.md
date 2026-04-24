# Backtest System

An event-driven quantitative trading backtesting framework with multi-market support and C++ acceleration extensions.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Components](#core-components)
- [Supported Markets](#supported-markets)
- [Project Structure](#project-structure)
- [Documentation](#documentation)

---

## Features

- **Event-driven engine**: Full event chain — `MarketEvent → SignalEvent → OrderEvent → FillEvent`
- **Multi-market support**: China A-shares, US stocks, HK stocks, and crypto — each market's rules are independently encapsulated
- **C++ acceleration**: Performance-critical components (indicator calculation, market rules, portfolio math) compiled as native extensions via pybind11
- **Flexible position sizing**: Fixed quantity, percent-of-equity, and equal-weight modes
- **Risk management**: Max drawdown, max open positions, max position size guards — combinable via `CompositeRiskManager`
- **Signal aggregation**: Multi-strategy signals resolved via weighted, majority vote, first-wins, last-wins, or veto-on-conflict modes
- **Automated data ingestion**: Integrates yfinance (equities) and Binance (crypto); data cached in local SQLite
- **Futures support**: Auto-tracks expiration dates and closes positions; supports daily mark-to-market
- **Fluent Builder API**: `BacktestBuilder` chain for fast engine assembly
- **Performance analytics**: `PerformanceAnalyzer` reports Sharpe ratio, max drawdown, annualised return, and more

---

## Architecture

```
DataHandler
    │
    ▼ MarketEvent
ExecutionModel.on_new_bar()   ← pending orders from prior bar fill here
    │
    ▼ FillEvent  (fill-on-next-bar semantics)
Portfolio.update_timeindex()  ← mark-to-market snapshot
    │
    ▼
Strategy.calculate_signal()   ← each strategy produces signals
    │
    ▼ SignalEvent
SignalAggregator              ← resolve multi-strategy conflicts
    │
    ▼
PositionSizer                 ← determine order quantity
    │
    ▼
RiskManager                   ← approve / adjust / reject order
    │
    ▼ OrderEvent
ExecutionModel.execute()      ← queue order; fills on next bar
    │
    ▼ FillEvent
Portfolio.process_fill_event()
```

---

## Installation

### Requirements

- Python 3.11+
- Conda (recommended for managing the C++ build environment)
- C++17-compatible compiler (macOS: Xcode Command Line Tools)

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Build C++ extensions

```bash
conda run -n backtest python setup.py build_ext --inplace
```

After building, the following `.so` files will appear in the project root:

- `indicators_ext` — Technical indicators (SMA, etc.)
- `market_rule_ext` — Market rule calculations
- `portfolio_ext` — Portfolio-level computations
- `hello_ext` — Example extension

> C++ extensions are optional. The system falls back to pure-Python implementations if they are not compiled.

---

## Quick Start

### Using BacktestBuilder (recommended)

```python
from builder import BacktestBuilder
from strategies.moving_average import MovingAverage

engine = (
    BacktestBuilder()
    .set_data(
        symbols=['AAPL'],
        start='2023-01-01',
        end='2023-12-31',
        frequency='1d',
        timezone='America/New_York',
        source='stock',
    )
    .set_market('us_stock')
    .set_capital(100_000)
    .add_strategy(MovingAverage, short_window=10, long_window=30)
    .set_position_sizer('percent', percent=0.10)
    .set_risk_manager('max_drawdown', max_drawdown=0.20)
    .build()
)

engine.run()
```

### China A-share example

```python
engine = (
    BacktestBuilder()
    .set_data(
        symbols=['600519.SS'],
        start='2024-01-01',
        end='2024-12-31',
        frequency='1d',
        timezone='Asia/Shanghai',
        source='stock',
    )
    .set_market('china_a')
    .set_capital(500_000)
    .add_strategy(MovingAverage, short_window=5, long_window=20)
    .set_position_sizer('percent', percent=0.10)
    .build()
)

engine.run()
```

More examples are available in the [`examples/`](examples/) directory.

---

## Core Components

### Engine

[`engine.py`](engine.py) — The event loop. Coordinates all components. Supports `run()` for a full backtest and `run_one_bar()` for bar-by-bar execution (suitable for live trading loops).

### BacktestBuilder

[`builder.py`](builder.py) — Fluent API for assembling the engine in a single chain.

| Method | Description |
|---|---|
| `.set_data(...)` | Set data source, symbols, date range, and frequency |
| `.set_market(market_type)` | Set market type (see table below) |
| `.set_capital(amount)` | Set initial capital |
| `.add_strategy(cls, **kwargs)` | Add a strategy (can be called multiple times) |
| `.set_position_sizer(type, ...)` | Set position sizing mode |
| `.set_risk_manager(type, ...)` | Set risk manager |
| `.set_signal_aggregator(type)` | Set signal aggregation mode |
| `.build()` | Build and return the `Engine` instance |

### Position Sizer

| Type string | Description |
|---|---|
| `'percent'` | Allocate a fixed percentage of account equity |
| `'fixed'` | Fixed number of shares/contracts |
| `'equal'` | Equal-weight allocation across all symbols |

### Risk Manager

| Type string | Description |
|---|---|
| `'max_drawdown'` | Reject new orders when drawdown exceeds threshold |
| `'max_positions'` | Cap the number of simultaneously open positions |
| `'max_position_pct'` | Cap a single position as a percentage of equity |
| `'composite'` | Combine multiple risk managers |
| `'null'` | No restrictions (pass-through) |

### Signal Aggregator

| Type string | Description |
|---|---|
| `'weighted'` | Weighted aggregation (default) |
| `'majority'` | Majority vote |
| `'first_wins'` | First signal takes priority |
| `'last_wins'` | Last signal takes priority |
| `'veto_on_conflict'` | Cancel all signals when strategies conflict |

### Custom Strategy

Subclass `Strategy` and implement `calculate_signal()`:

```python
from core.strategy import Strategy
from core.event import SignalEvent

class MyStrategy(Strategy):
    def __init__(self, data_handler, **kwargs):
        super().__init__(data_handler)
        self.window = kwargs.get('window', 20)

    def calculate_signal(self, event):
        for symbol in self.data_handler.symbols:
            bars = self.data_handler.get_latest_bars(symbol, n=self.window)
            if bars is None or len(bars) < self.window:
                continue
            # signal logic ...
            return SignalEvent(symbol=symbol, signal_type='LONG', datetime=event.datetime)
```

---

## Supported Markets

| Market | Type string | Settlement | Lot size | Price limit | Short selling | Commission |
|---|---|---|---|---|---|---|
| China A-shares | `china_a` | T+1 | 100 shares | ±10% / 20% | No | 0.03% |
| US stocks | `us_stock` | T+2 | 1 share | None | Yes | 0% |
| HK stocks | `hk_stock` | T+2 | 100 shares | None | Yes | 0.25% |
| Crypto | `crypto` | T+0 | No minimum | None | Yes | 0.1% |

---

## Project Structure

```
backtest_system/
├── engine.py              # Event-driven backtest engine
├── builder.py             # BacktestBuilder fluent API
├── setup.py               # C++ extension build script
├── requirements.txt       # Python dependencies
│
├── core/                  # Core abstractions and type definitions
│   ├── event.py           # Event types (Market / Signal / Order / Fill)
│   ├── strategy.py        # Strategy abstract base class
│   ├── instrument.py      # Financial instruments (Stock, Future, etc.)
│   ├── market_rule.py     # Market rules (MarketRule, MarketRulesFactory)
│   ├── position_sizer.py  # Position sizing
│   ├── signal_aggregator.py # Signal aggregation
│   ├── data_feed.py       # DataFeed abstract interface
│   ├── portfolio_context.py # Read-only portfolio snapshot
│   └── execution_model.py # Execution model abstract interface
│
├── portfolio/
│   └── portfolio.py       # Holdings management, MTM, T+1 settlement
│
├── execution/
│   └── execution_handler.py # Simulated matching (fill-on-next-bar, slippage)
│
├── risk/
│   └── risk_manager.py    # Risk managers
│
├── analytics/
│   └── performance.py     # PerformanceAnalyzer (Sharpe, drawdown, etc.)
│
├── data/
│   ├── data_loader.py     # yfinance / Binance data fetching + SQLite cache
│   ├── data_handler.py    # Historical data replay
│   └── future_roller.py   # Futures roll handling
│
├── strategies/
│   └── moving_average.py  # Moving average strategy example
│
├── cpp/                   # C++ source files (pybind11 extensions)
│   ├── indicators_ext.cpp
│   ├── market_rule_ext.cpp
│   ├── portfolio_ext.cpp
│   └── hello_ext.cpp
│
├── examples/
│   ├── demo.py                  # Full feature walkthrough
│   ├── demo_builder.py          # Builder API demo
│   └── china_a_share_example.py # China A-share backtest example
│
└── docs/
    ├── ARCHITECTURE.md          # System architecture and design decisions
    ├── MARKET_RULES_GUIDE.md    # Market rules configuration and extension guide
    ├── FILL_ON_NEXT_BAR.md      # Fill-on-next-bar semantics explained
    ├── SLIPPAGE_MODEL.md        # Slippage model implementation
    └── README_TESTS.md          # Test suite overview and how to run
```

---
