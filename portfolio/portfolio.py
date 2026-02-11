from datetime import datetime, timedelta
from core.event import SignalEvent, OrderEvent, FillEvent, MarketEvent
from data.data_handler import DataHandler
from core.instrument import InstrumentRegistry

class Portfolio():
    def __init__(self, data_handler: DataHandler, initial_capital: float, instrument_registry: InstrumentRegistry):
        self.initial_capital = initial_capital
        self.current_holdings = {}
        self.current_cash = self.initial_capital
        self.positions = []
        self.all_holdings = []
        self.data_handler = data_handler
        self.total_realized_pnl = 0
        self.pending_settlements = []  # For T+1, T+2 settlement 
        self.instrument_registry = instrument_registry
        self.margin_used = {}

    def process_signal_event(self, signal_event: SignalEvent) -> OrderEvent:
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

        bar = self.data_handler.get_latest_bar(symbol)

        instrument = self.instrument_registry.get(symbol=symbol)

        market_rule = instrument.market_rule

        # Normalize quantity to comply with lot size
        quantity = market_rule.normalize_quantity(int(signal_strength))

        margin = market_rule.calculate_margin(symbol=symbol, quantity=quantity, price=bar['close'])

        if margin > self.current_cash:
            return None
        
        if quantity == 0:
            return None
        
        # For T+1 markets, check if we have enough available shares to sell
        if direction == 'SELL' and market_rule.settlement_days > 0:
            if symbol in self.current_holdings:
                available = self.current_holdings[symbol].get('available', 0)
                if available < quantity:
                    # Not enough available shares (T+1 restriction)
                    return None

        return OrderEvent(
            symbol=symbol,
            quantity=quantity,
            direction=direction,
            datetime=signal_event.datetime,
        )

    def process_fill_event(self, fill_event: FillEvent) -> None:
        if not fill_event.rejected:
            fill_price = fill_event.fill_price
            symbol = fill_event.symbol
            quantity = fill_event.quantity
            commission = fill_event.commission
            direction = fill_event.direction
            time = fill_event.datetime

            market_rule = self.instrument_registry.get(symbol=symbol).market_rule

            if symbol not in self.current_holdings:
                self.current_holdings[symbol] = {'quantity': 0, 'avg_cost': 0, 'available': 0}

            current_quantity = self.current_holdings[symbol]['quantity']
            current_avg_cost = self.current_holdings[symbol]['avg_cost']

            quantity_change = quantity if direction == 'BUY' else -quantity
            new_quantity = current_quantity + quantity_change

            pnl = 0

            # 開倉
            if current_quantity == 0:
                self._add_current_holding(symbol=symbol, price=fill_price, quantity=new_quantity, commission=commission)

            # 加倉
            elif current_quantity * quantity_change > 0:
                self._add_current_holding(symbol=symbol, price=fill_price, quantity=quantity_change, commission=commission)
            
            # 減倉        
            elif current_quantity * quantity_change < 0 and abs(current_quantity) > abs(quantity_change):
                self._reduce_current_holding(symbol=symbol, price=fill_price, quantity=quantity, commission=commission)
                pnl = self._calculate_pnl(symbol=symbol, price=fill_price, quantity=quantity, commission=commission)

            # 平倉或轉向
            elif current_quantity * quantity_change < 0 and abs(current_quantity) <= abs(quantity_change):
                self._reduce_current_holding(symbol=symbol, price=fill_price, quantity=abs(current_quantity), commission=commission)
                pnl = self._calculate_pnl(symbol=symbol, price=fill_price, quantity=quantity, commission=commission)
                if new_quantity != 0:
                    self._add_current_holding(symbol=symbol, price=fill_price, quantity=new_quantity, commission=commission)

            contract_multiplier = self.instrument_registry.get(symbol).contract_multiplier

            margin = market_rule.calculate_margin(symbol=symbol, quantity=quantity, price=fill_price)

            # Handle cash flow with commission included in fill_event
            if direction == 'BUY':
                self.current_cash -= (margin + commission)
                self.margin_used[symbol] = self.margin_used.get(symbol, 0) + margin
                self.current_holdings[symbol]['last_settle_price'] = fill_price
            elif direction == 'SELL':
                if symbol in self.margin_used and current_quantity != 0:
                    release_ratio = quantity / abs(current_quantity)
                    released = self.margin_used[symbol] * release_ratio
                    self.margin_used[symbol] -= released
                    self.current_cash += (released - commission)
            
            # Handle T+1, T+2 settlement for availability
            if market_rule.settlement_days > 0:
                if direction == 'BUY':
                    # Add to pending settlements, will be available after settlement_days
                    self.pending_settlements.append({
                        'symbol': symbol,
                        'quantity': quantity,
                        'buy_time': time,
                        'settlement_days': market_rule.settlement_days
                    })
                elif direction == 'SELL':
                    # Reduce available quantity when selling
                    self.current_holdings[symbol]['available'] -= quantity
            else:
                # T+0 market, immediately available
                self.current_holdings[symbol]['available'] = self.current_holdings[symbol]['quantity']

            self.total_realized_pnl += pnl

            position = {
                'symbol': symbol,
                'fill_price': fill_price,
                'quantity': quantity,
                'commission': commission,
                'direction': direction,
                'time': time,
                'realized_pnl': pnl
            }
            self.positions.append(position)

    def update_timeindex(self, market_event: MarketEvent):
        # Process pending settlements (T+1, T+2)
        self._process_settlements(market_event.datetime)
        
        holdings = {
            'time': market_event.datetime,
            'cash': self.current_cash,
            'total': self.current_cash,
            'unrealized_pnl': 0
        }
        
        for symbol, position in self.current_holdings.items():
            if position['quantity'] != 0:
                latest_bar = self.data_handler.get_latest_bar(symbol)
                
                if latest_bar:

                    market_rule = self.instrument_registry.get(symbol=symbol).market_rule
                    current_price = latest_bar['close']
                    quantity = position['quantity']
                    avg_cost = position['avg_cost']

                    contract_multiplier = self.instrument_registry.get(symbol=symbol).contract_multiplier

                    # mark to market
                    if market_rule.requires_daily_settlement:
                        prev_price = position.get('last_settle_price', avg_cost)
                        mtm_pnl = (current_price - prev_price) * quantity * contract_multiplier
                        self.current_cash += mtm_pnl
                        holdings['cash'] += mtm_pnl
                        holdings['total'] += mtm_pnl
                        position['last_settle_price'] = current_price

                    market_value = quantity * current_price * contract_multiplier
                    if market_rule.requires_daily_settlement:
                        unrealized_pnl = (current_price - position['last_settle_price']) * quantity * contract_multiplier
                    else:
                        unrealized_pnl = (current_price - avg_cost) * quantity * contract_multiplier

                    holdings[symbol + '_value'] = market_value
                    holdings[symbol + '_pnl'] = unrealized_pnl
                    holdings['total'] += market_value
        
        self.all_holdings.append(holdings)


    def get_holding(self, symbol: str) -> dict:
        if symbol in self.current_holdings:
            return self.current_holdings[symbol]

    def _add_current_holding(self, symbol: str, price: float, quantity: float, commission: float) -> None:
        current_quantity = self.current_holdings[symbol]['quantity']
        current_avg_cost = self.current_holdings[symbol]['avg_cost']
        
        # Calculate commission per share
        commission_per_share = commission / abs(quantity) if quantity != 0 else 0

        if quantity > 0:
            self.current_holdings[symbol]['quantity'] = current_quantity + quantity
            self.current_holdings[symbol]['avg_cost'] = (current_avg_cost * current_quantity + (price + commission_per_share) * quantity) / (current_quantity + quantity)

        elif quantity < 0:
            self.current_holdings[symbol]['quantity'] = current_quantity + quantity
            self.current_holdings[symbol]['avg_cost'] = (current_avg_cost * current_quantity + (price - commission_per_share) * quantity) / (current_quantity + quantity)

    def _reduce_current_holding(self, symbol: str, price: float, quantity: float, commission: float) -> None:
        current_quantity = self.current_holdings[symbol]['quantity']
        
        if current_quantity > 0:
            self.current_holdings[symbol]['quantity'] = current_quantity - quantity
        elif current_quantity < 0:
            self.current_holdings[symbol]['quantity'] = current_quantity + quantity

    def _calculate_pnl(self, symbol: str, price: float, quantity: float, commission: float) -> float:
        current_avg_cost = self.current_holdings[symbol]['avg_cost']

        contract_multiplier = self.instrument_registry.get(symbol).contract_multiplier
        
        # Commission is already calculated in fill_event, so we just need price difference
        commission_per_share = commission / abs(quantity) if quantity != 0 else 0

        if quantity < 0:
            pnl = abs(quantity) * ((price - commission_per_share) - current_avg_cost) * contract_multiplier

        elif quantity > 0:
            pnl = quantity * (current_avg_cost - (price + commission_per_share)) * contract_multiplier

        return pnl
    
    def _process_settlements(self, current_time: str) -> None:
        """
        Process pending settlements for T+1, T+2 markets
        Release shares that have reached their settlement date
        """
        if not self.pending_settlements:
            return
        
        # Parse current time
        try:
            if isinstance(current_time, str):
                # Remove timezone suffix for parsing
                time_str = current_time.rsplit(' ', 1)[0]
                current_dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
            else:
                current_dt = current_time
        except:
            return
        
        # Check which settlements have matured
        settled = []
        for i, settlement in enumerate(self.pending_settlements):
            buy_time = settlement['buy_time']
            settlement_days = settlement['settlement_days']
            
            try:
                if isinstance(buy_time, str):
                    time_str = buy_time.rsplit(' ', 1)[0]
                    buy_dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                else:
                    buy_dt = buy_time
                
                # Calculate settlement date (T+N trading days)
                # Simplified: use calendar days for now
                settlement_date = buy_dt + timedelta(days=settlement_days)
                
                if current_dt >= settlement_date:
                    # Settlement has matured, make shares available
                    symbol = settlement['symbol']
                    quantity = settlement['quantity']
                    
                    if symbol in self.current_holdings:
                        self.current_holdings[symbol]['available'] = \
                            self.current_holdings[symbol].get('available', 0) + quantity
                    
                    settled.append(i)
            except:
                continue
        
        # Remove settled items (in reverse order to maintain indices)
        for i in reversed(settled):
            self.pending_settlements.pop(i)


