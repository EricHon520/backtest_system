import datetime
from typing import List
import data_handler
from event import SignalEvent, OrderEvent, FillEvent, MarketEvent
from data_handler import DataHandler

class Portfolio():
    def __init__(self, data_handler: DataHandler, initial_capital: float):
        self.initial_capital = initial_capital
        self.current_holdings = {}
        self.current_cash = self.initial_capital
        self.positions = []
        self.all_holdings = []
        self.data_handler = data_handler
        self.total_realized_pnl = 0

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

        return OrderEvent(
            symbol=symbol,
            quantity=int(signal_strength),
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

            if symbol not in self.current_holdings:
                self.current_holdings[symbol] = {'quantity': 0, 'avg_cost': 0}

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
                self._reduce_current_holding(symbol=symbol, price=fill_price, quantity=current_quantity, commission=commission)
                pnl = self._calculate_pnl(symbol=symbol, price=fill_price, quantity=quantity, commission=commission)
                if new_quantity != 0:
                    self._add_current_holding(symbol=symbol, price=fill_price, quantity=new_quantity, commission=commission)

            if direction == 'BUY':
                self.current_cash -= (fill_price * quantity) * (1 + commission)
            elif direction == 'SELL':
                self.current_cash += (fill_price * quantity) * (1 - commission)

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
                    current_price = latest_bar['close']
                    quantity = position['quantity']
                    avg_cost = position['avg_cost']

                    market_value = quantity * current_price
                    unrealized_pnl = (current_price - avg_cost) * quantity

                    holdings[symbol + '_value'] = market_value
                    holdings[symbol + '_pnl'] = unrealized_pnl
                    holdings['total'] += market_value
        
        self.all_holdings.append(holdings)

    def _add_current_holding(self, symbol: str, price: float, quantity: float, commission: float) -> None:
        current_quantity = self.current_holdings[symbol]['quantity']
        current_avg_cost = self.current_holdings[symbol]['avg_cost']

        if quantity > 0:
            self.current_holdings[symbol]['quantity'] = current_quantity + quantity
            self.current_holdings[symbol]['avg_cost'] = (current_avg_cost * current_quantity + price * (1 + commission) * quantity) / (current_quantity + quantity)

        elif quantity < 0:
            self.current_holdings[symbol]['quantity'] = current_quantity + quantity
            self.current_holdings[symbol]['avg_cost'] = (current_avg_cost * current_quantity + price * (1 - commission) * quantity) / (current_quantity + quantity)

    def _reduce_current_holding(self, symbol: str, price: float, quantity: float, commission: float) -> None:
        current_quantity = self.current_holdings[symbol]['quantity']
        
        if current_quantity > 0:
            self.current_holdings[symbol]['quantity'] = current_quantity - quantity
        elif current_quantity < 0:
            self.current_holdings[symbol]['quantity'] = current_quantity + quantity

    def _calculate_pnl(self, symbol: str, price: float, quantity: float, commission: float) -> float:
        current_avg_cost = self.current_holdings[symbol]['avg_cost']

        if quantity < 0:
            pnl = abs(quantity) * (price * (1 - commission) - current_avg_cost)

        elif quantity > 0:
            pnl = quantity * (current_avg_cost - price * (1 + commission))

        return pnl


