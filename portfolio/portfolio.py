from textwrap import fill
import warnings
from datetime import datetime, timedelta
from typing import Dict, Optional
from core import market_rule
from core.event import SignalEvent, OrderEvent, FillEvent, MarketEvent
from core.instrument import InstrumentRegistry

class Portfolio():
    def __init__(self, initial_capital: float, instrument_registry: InstrumentRegistry,
                 data_handler=None):
        self.initial_capital = initial_capital
        self.current_holdings = {}
        self.current_cash = self.initial_capital
        self.positions = []
        self.all_holdings = []
        self.total_realized_pnl = 0
        self.pending_settlements = []  # For T+1, T+2 settlement
        self.instrument_registry = instrument_registry
        self.margin_used = {}
        if data_handler is not None:
            warnings.warn(
                "Portfolio no longer requires data_handler. "
                "Pass current_prices to update_timeindex() instead. "
                "The data_handler argument will be removed in a future version.",
                DeprecationWarning,
                stacklevel=2,
            )
            self._legacy_data_handler = data_handler
        else:
            self._legacy_data_handler = None

    def process_signal_event(self, signal_event: SignalEvent) -> OrderEvent:  # noqa: E501
        warnings.warn(
            "Portfolio.process_signal_event() is deprecated. "
            "Use Engine's PositionSizer + RiskManager pipeline instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if signal_event is None:
            return None

        if signal_event.signal_type == 'LONG':
            direction = 'BUY'
        elif signal_event.signal_type == 'SHORT':
            direction = 'SELL'
        else:
            return None

        symbol = signal_event.symbol
        signal_strength = signal_event.strength

        bar = self._legacy_data_handler.get_latest_bar(symbol)

        instrument = self.instrument_registry.get(symbol=symbol)

        market_rule = instrument.market_rule

        # Normalize quantity to comply with lot size
        quantity = market_rule.normalize_quantity(int(signal_strength))

        margin = market_rule.calculate_margin(symbol=symbol, quantity=quantity, price=bar['close'], contract_multiplier=instrument.contract_multiplier)

        if margin > self.current_cash:
            return None
        
        if quantity == 0:
            return None

        if direction == 'SELL':
            current_quantity = self.current_holdings.get(symbol, {}).get('quantity', 0)

            if current_quantity <= 0 and not market_rule.allow_short:
                return None
            
            # For T+1 markets, check if there are enough available shares to sell
            if market_rule.settlement_days > 0 and current_quantity > 0:
                available = self.current_holdings.get(symbol, {}).get('available', 0)
                if available < quantity:
                    return None

        return OrderEvent(
            symbol=symbol,
            quantity=quantity,
            direction=direction,
            datetime=signal_event.datetime,
        )

    def process_fill_event(self, fill_event: FillEvent) -> None:
        if fill_event.rejected:
            return
        symbol = fill_event.symbol
        fill_price = fill_event.fill_price
        quantity = fill_event.quantity
        commission = fill_event.commission
        direction = fill_event.direction
        time = fill_event.datetime

        market_rule = self.instrument_registry.get(symbol=symbol).market_rule
        contract_multiplier = self.instrument_registry.get(symbol=symbol).contract_multiplier

        if symbol not in self.current_holdings:
            self.current_holdings[symbol] = {'quantity': 0, 'avg_cost': 0, 'available': 0}

        current_quantity = self.current_holdings[symbol]['quantity']
        quantity_change = quantity if direction == 'BUY' else -quantity
        new_quantity = current_quantity + quantity_change

        pnl = self._update_position(symbol=symbol, fill_price=fill_price, quantity=quantity, direction=direction, commission=commission, current_quantity=current_quantity)
        self._update_cash(symbol=symbol, direction=direction, fill_price=fill_price, quantity=quantity, commission=commission, pnl=pnl, market_rule=market_rule, 
                          contract_multiplier=contract_multiplier, current_quantity=current_quantity, new_quantity=new_quantity)
        self._update_settlement(symbol=symbol, direction=direction, quantity=quantity, time=time, market_rule=market_rule)
        self._record_trade(symbol=symbol, fill_price=fill_price, quantity=quantity, commission=commission, direction=direction, time=time, pnl=pnl)

        self.total_realized_pnl += pnl


    def update_timeindex(self, market_event: MarketEvent,
                         current_prices: Optional[Dict[str, float]] = None):
        """
        Take a mark-to-market snapshot of the portfolio.

        Args:
            market_event:   The current MarketEvent (provides timestamp).
            current_prices: Dict mapping symbol → latest close price.
                            If omitted and a legacy data_handler was supplied,
                            prices are fetched from it (deprecated path).
        """
        # Process pending settlements (T+1, T+2)
        self._process_settlements(market_event.datetime)

        # Build price dict via legacy data_handler if caller didn't supply prices
        if current_prices is None:
            if self._legacy_data_handler is not None:
                current_prices = {}
                for sym in self.current_holdings:
                    bar = self._legacy_data_handler.get_latest_bar(sym)
                    if bar:
                        current_prices[sym] = bar['close']
            else:
                current_prices = {}

        holdings = {
            'time': market_event.datetime,
            'cash': self.current_cash,
            'total': self.current_cash,
            'unrealized_pnl': 0
        }

        for symbol, position in self.current_holdings.items():
            if position['quantity'] == 0:
                continue

            current_price = current_prices.get(symbol)
            if current_price is None:
                continue

            market_rule = self.instrument_registry.get(symbol=symbol).market_rule
            quantity = position['quantity']
            avg_cost = position['avg_cost']
            contract_multiplier = self.instrument_registry.get(symbol=symbol).contract_multiplier

            if market_rule.requires_daily_settlement:
                prev_price = position.get('last_settle_price', avg_cost)
                mtm_pnl = (current_price - prev_price) * quantity * contract_multiplier
                self.current_cash += mtm_pnl
                holdings['cash'] += mtm_pnl
                holdings['total'] += mtm_pnl
                position['last_settle_price'] = current_price
                unrealized_pnl = mtm_pnl
            else:
                prev_price = avg_cost

            if not market_rule.requires_daily_settlement:
                market_value = quantity * current_price * contract_multiplier
                unrealized_pnl = (current_price - avg_cost) * quantity * contract_multiplier
                holdings[symbol + '_value'] = market_value
                holdings[symbol + '_pnl'] = unrealized_pnl
                holdings['total'] += market_value
            else:
                holdings[symbol + '_value'] = 0.0
                holdings[symbol + '_pnl'] = unrealized_pnl

            holdings['unrealized_pnl'] += unrealized_pnl

        self.all_holdings.append(holdings)


    def get_holding(self, symbol: str) -> dict:
        if symbol in self.current_holdings:
            return self.current_holdings[symbol]

    def _add_current_holding(self, symbol: str, price: float, quantity: float, commission: float) -> None:
        current_quantity = self.current_holdings[symbol]['quantity']
        current_avg_cost = self.current_holdings[symbol]['avg_cost']

        # avg_cost records only the pure fill price; commission is accounted for
        # separately in cash so it is never double-counted.
        abs_qty = abs(quantity)
        if abs_qty == 0:
            return

        new_quantity = current_quantity + quantity
        self.current_holdings[symbol]['quantity'] = new_quantity
        # Weighted average of old cost basis and new fill price (always positive)
        self.current_holdings[symbol]['avg_cost'] = (
            current_avg_cost * abs(current_quantity) + price * abs_qty
        ) / abs(new_quantity)

    def _close_long_cash(self, symbol: str, market_rule, close_qty: float,
                          position_qty: float, fill_price: float,
                          contract_multiplier: int, pnl: float, commission: float) -> None:
        """Update cash when closing (part of) a long position.

        position_qty: the absolute held quantity *before* this close, used to
                      compute the proportional share of margin to release.
        """
        if market_rule.requires_daily_settlement:
            release_ratio = close_qty / position_qty if position_qty != 0 else 1.0
            released = self.margin_used.get(symbol, 0) * release_ratio
            self.margin_used[symbol] = self.margin_used.get(symbol, 0) - released
            self.current_cash += released + pnl - commission
        else:
            self.current_cash += close_qty * fill_price * contract_multiplier - commission
            self.margin_used.pop(symbol, None)

    def _close_short_cash(self, symbol: str, market_rule, close_qty: float,
                           position_qty: float, fill_price: float,
                           contract_multiplier: int, pnl: float, commission: float) -> None:
        """Update cash when closing (part of) a short position.

        position_qty: the absolute held quantity *before* this close, used to
                      compute the proportional share of margin to release.
        """
        if market_rule.requires_daily_settlement:
            release_ratio = close_qty / position_qty if position_qty != 0 else 1.0
            released = self.margin_used.get(symbol, 0) * release_ratio
            self.margin_used[symbol] = self.margin_used.get(symbol, 0) - released
            self.current_cash += released + pnl - commission
        else:
            # Short stock: pay back shares at current price
            self.current_cash -= close_qty * fill_price * contract_multiplier + commission
            self.margin_used.pop(symbol, None)

    def _reduce_current_holding(self, symbol: str, price: float, quantity: float, commission: float) -> None:
        current_quantity = self.current_holdings[symbol]['quantity']
        
        if current_quantity > 0:
            self.current_holdings[symbol]['quantity'] = current_quantity - quantity
        elif current_quantity < 0:
            self.current_holdings[symbol]['quantity'] = current_quantity + quantity

    def _calculate_pnl(self, symbol: str, price: float, quantity: float, direction: str, commission: float) -> float:
        contract_multiplier = self.instrument_registry.get(symbol).contract_multiplier
        market_rule = self.instrument_registry.get(symbol=symbol).market_rule

        if market_rule.requires_daily_settlement:
            # For daily-settlement futures the MTM process has already credited
            # every day's gain/loss into cash up to last_settle_price.  The
            # realized PnL on a close is therefore only the move from the last
            # settlement price to the fill price — NOT from the original entry
            # price.  Using avg_cost here would double-count all settled gains.
            cost_basis = self.current_holdings[symbol].get('last_settle_price',
                         self.current_holdings[symbol]['avg_cost'])
        else:
            # For non-settlement instruments, cost basis is the average entry price.
            cost_basis = self.current_holdings[symbol]['avg_cost']

        # commission is deducted from cash directly in process_fill_event;
        # do NOT subtract it here (that would be a double-count).
        if direction == 'SELL':
            # Closing a long: profit = (fill_price - cost_basis) * qty
            pnl = abs(quantity) * (price - cost_basis) * contract_multiplier
        else:
            # Closing a short: profit = (cost_basis - fill_price) * qty
            pnl = abs(quantity) * (cost_basis - price) * contract_multiplier

        return pnl
    
    @staticmethod
    def _parse_datetime(dt) -> Optional[datetime]:
        """Normalise a datetime value that may be a str, datetime, or None."""
        if isinstance(dt, datetime):
            return dt.replace(tzinfo=None)   # strip tz for comparison
        if isinstance(dt, str):
            # Strip optional trailing timezone token (e.g. 'UTC+08:00')
            cleaned = dt.rsplit(' ', 1)[0] if ' ' in dt else dt
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                try:
                    return datetime.strptime(cleaned, fmt)
                except ValueError:
                    continue
        return None

    @staticmethod
    def _add_trading_days(dt: datetime, n: int) -> datetime:
        """
        Advance *dt* by *n* trading days (Mon-Fri), skipping weekends.
        A full exchange holiday calendar is not modelled here, but skipping
        weekends already fixes the primary settlement-date error (e.g. US T+2:
        Thursday buy settles Monday, not Saturday).
        """
        count = 0
        result = dt
        while count < n:
            result += timedelta(days=1)
            if result.weekday() < 5:   # 0=Mon … 4=Fri
                count += 1
        return result

    def _process_settlements(self, current_time) -> None:
        """
        Process pending settlements for T+1, T+2 markets.
        Release shares that have reached their settlement date.
        Settlement days are counted in *trading* days, not calendar days.
        """
        if not self.pending_settlements:
            return

        current_dt = self._parse_datetime(current_time)
        if current_dt is None:
            return

        settled = []
        for i, settlement in enumerate(self.pending_settlements):
            buy_dt = self._parse_datetime(settlement['buy_time'])
            if buy_dt is None:
                settled.append(i)   # discard malformed record
                continue

            # Strip intraday time so settlement_date is always at midnight of
            # the settlement day; daily bar timestamps are also midnight, making
            # the >= comparison correct regardless of what time the buy occurred.
            buy_date = buy_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            settlement_date = self._add_trading_days(buy_date, settlement['settlement_days'])
            if current_dt >= settlement_date:
                symbol = settlement['symbol']
                quantity = settlement['quantity']
                if symbol in self.current_holdings:
                    self.current_holdings[symbol]['available'] = (
                        self.current_holdings[symbol].get('available', 0) + quantity
                    )
                settled.append(i)

        for i in reversed(settled):
            self.pending_settlements.pop(i)

    def _update_position(self, symbol: str, fill_price: float,
                         quantity: int, direction: str,
                         commission: float, current_quantity: int) -> float:
        """
        Update holdings for open/add/reduce/close/reverse
        Returns realized PnL
        """
        quantity_change = quantity if direction == "BUY" else -quantity
        new_quantity = current_quantity + quantity_change
        pnl = 0

        # open position
        if current_quantity == 0:
            self._add_current_holding(symbol=symbol, price=fill_price, quantity=new_quantity, commission=commission)

        # add position
        elif current_quantity * quantity_change > 0:
            self._add_current_holding(symbol=symbol, price=fill_price, quantity=quantity_change, commission=commission)

        # reduce position
        elif current_quantity * quantity_change < 0 and abs(current_quantity) > abs(quantity_change):
            self._reduce_current_holding(symbol=symbol, price=fill_price, quantity=quantity, commission=commission)
            pnl = self._calculate_pnl(symbol=symbol, price=fill_price, quantity=quantity, direction=direction, commission=commission)

        # close or reverse position
        elif current_quantity * quantity_change < 0 and abs(current_quantity) <= abs(quantity_change):
            close_qty = abs(current_quantity)
            self._reduce_current_holding(symbol=symbol, price=fill_price, quantity=close_qty, commission=commission)
            pnl = self._calculate_pnl(symbol=symbol, price=fill_price, quantity=close_qty, direction=direction, commission=commission)
            if new_quantity != 0:
                self._add_current_holding(symbol=symbol, price=fill_price, quantity=new_quantity, commission=commission)

        return pnl

    def _update_cash(self, symbol: str, direction: str, fill_price: float,
                     quantity: int, commission: float, pnl: float,
                     market_rule, contract_multiplier: int,
                     current_quantity: int, new_quantity: int) -> None:
        """
        Handle cash-flow accounting for a fill
        """
        margin = market_rule.calculate_margin(symbol=symbol, quantity=quantity, price=fill_price, contract_multiplier=contract_multiplier)

        is_reversal = (current_quantity * (quantity if direction == "BUY" else -quantity) < 0
                       and abs(current_quantity) < abs(quantity))
        
        close_leg_qty = abs(current_quantity) if is_reversal else quantity

        new_leg_qty = abs(new_quantity) if is_reversal else 0

        if direction == "BUY":
            if is_reversal:
                self._close_short_cash(symbol=symbol, market_rule=market_rule, close_qty=close_leg_qty,
                                       position_qty=abs(current_quantity), fill_price=fill_price,
                                       contract_multiplier=contract_multiplier, pnl=pnl, commission=commission)
                new_margin = market_rule.calculate_margin(symbol=symbol, quantity=new_leg_qty, price=fill_price, contract_multiplier=contract_multiplier)
                self.current_cash -= new_margin
                self.margin_used[symbol] = self.margin_used.get(symbol, 0) + new_margin
            else:
                self.current_cash -= (margin + commission)
                self.margin_used[symbol] = self.margin_used.get(symbol, 0) + margin
            self.current_holdings[symbol]['last_settle_price'] = fill_price

        elif direction == 'SELL':
            if is_reversal:
                self._close_long_cash(symbol=symbol, market_rule=market_rule, close_qty=close_leg_qty,
                                      position_qty=abs(current_quantity), fill_price=fill_price,
                                      contract_multiplier=contract_multiplier, pnl=pnl, commission=commission)
                new_margin = market_rule.calculate_margin(symbol=symbol, quantity=new_leg_qty, price=fill_price, contract_multiplier=contract_multiplier)
                self.current_cash -= new_margin
                self.margin_used[symbol] = self.margin_used.get(symbol, 0) + new_margin
                self.current_holdings[symbol]['last_settle_price'] = fill_price
            elif market_rule.requires_daily_settlement:
                if current_quantity > 0:
                    self._close_long_cash(symbol=symbol, market_rule=market_rule, close_qty=quantity,
                                          position_qty=current_quantity, fill_price=fill_price,
                                          contract_multiplier=contract_multiplier, pnl=pnl, commission=commission)
                else:
                    self.current_cash -= (margin + commission)
                    self.margin_used[symbol] = self.margin_used.get(symbol, 0) + margin
                    self.current_holdings[symbol]['last_settle_price'] = fill_price
            else:
                self.current_cash += quantity * fill_price * contract_multiplier - commission
                self.margin_used.pop(symbol, None)
    
    def _update_settlement(self, symbol: str, direction: str, 
                           quantity: int, time, market_rule) -> None:
        """
        Handle T+1/T+2 availability tracking
        """
        if market_rule.settlement_days > 0:
            if direction == "BUY":
                self.pending_settlements.append({
                    'symbol': symbol,
                    'quantity': quantity,
                    'buy_time': time,
                    'settlement_days': market_rule.settlement_days
                })
            elif direction == 'SELL':
                self.current_holdings[symbol]['available'] = max(0, self.current_holdings[symbol].get('available', 0) - quantity)
        else:
            self.current_holdings[symbol]['available'] = self.current_holdings[symbol]['quantity']

    def _record_trade(self, symbol: str, fill_price: float, quantity: int,
                      commission: float, direction: str, time, pnl: float) -> None:
        """
        Append trade record to positions log
        """
        self.positions.append({
            'symbol': symbol,
            'fill_price': fill_price,
            'quantity': quantity,
            'commission': commission,
            'direction': direction,
            'time': time,
            'realized_pnl': pnl
        })