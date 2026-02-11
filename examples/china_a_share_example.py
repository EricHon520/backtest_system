"""
Example: Backtesting on China A-Share Market

This example demonstrates how to run a backtest on A-share market
with specific market rules including:
- T+1 settlement
- Price limit (10% for main board, 20% for ChiNext/STAR)
- Lot size requirement (100 shares)
- Stamp duty on sell orders
"""

import sys
sys.path.append('..')

from datetime import datetime
from engine import main as engine_main
from data_handler import DataHandler
from portfolio import Portfolio
from execution_handler import ExecutionHandler
from strategies.moving_average import MovingAverage
import queue
from event import EventType, MarketEvent
import logging

def china_a_share_backtest():
    # A-share symbols (example: Shanghai Stock Exchange)
    symbols = ['600519.SS']  # Moutai (example)
    start_time = datetime(2024, 1, 1)
    end_time = datetime(2024, 12, 31)
    frequency = '1d'
    timezone = 'Asia/Shanghai'
    source = 'stock'
    
    # Specify China A-share market rules
    market_type = 'china_a'
    
    # Initialize components with market rules
    data_handler = DataHandler(
        symbols=symbols,
        start_time=start_time,
        end_time=end_time,
        frequency=frequency,
        timezone=timezone,
        source=source
    )
    
    # Portfolio with A-share rules (T+1, lot size, etc.)
    portfolio = Portfolio(
        data_handler=data_handler,
        initial_capital=100000.00,  # 100,000 RMB
        market_type=market_type
    )
    
    # Execution handler with A-share commission structure
    execution_handler = ExecutionHandler(
        data_handler=data_handler,
        rejection_rate=0.05,
        market_type=market_type
    )
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('china_a_backtest.log'),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    event_queue = queue.Queue()
    strategy = MovingAverage(data_handler=data_handler)
    
    logger.info(f"Starting China A-Share backtest with market rules:")
    logger.info(f"  - Market: {portfolio.market_rules.market_name}")
    logger.info(f"  - Settlement: T+{portfolio.market_rules.settlement_days}")
    logger.info(f"  - Lot size: {portfolio.market_rules.lot_size} shares")
    logger.info(f"  - Commission: {portfolio.market_rules.commission_rate * 100}%")
    logger.info(f"  - Stamp duty: {portfolio.market_rules.stamp_duty * 100}% (sell only)")
    
    # Main backtest loop
    while True:
        if data_handler.update_bars():
            latest_bar = data_handler.get_latest_bar(symbols[0])
            
            # Check if within trading hours
            if not portfolio.market_rules.is_trading_time(datetime.strptime(latest_bar['datetime_local'], '%Y-%m-%d %H:%M:%S %Z')):
                continue
            
            event = MarketEvent(datetime=latest_bar['datetime_local'], symbol=latest_bar['ticker'])
            event_queue.put(event)
        else:
            break
        
        while not event_queue.empty():
            event = event_queue.get()
            
            if event.event_type == EventType.MARKET:
                portfolio.update_timeindex(event)
                signal_event = strategy.calculate_signal(event=event)
                if signal_event is not None:
                    logger.info(f"Signal: {signal_event.signal_type} at {signal_event.datetime}")
                    event_queue.put(signal_event)
            
            elif event.event_type == EventType.SIGNAL:
                order_event = portfolio.process_signal_event(event)
                if order_event is not None:
                    logger.info(f"Order: {order_event.direction} {order_event.quantity} shares")
                    event_queue.put(order_event)
            
            elif event.event_type == EventType.ORDER:
                fill_event = execution_handler.process_order_event(event)
                if fill_event is not None:
                    if fill_event.rejected:
                        logger.warning(f"Order rejected")
                    else:
                        logger.info(f"Fill: {fill_event.direction} {fill_event.quantity} @ {fill_event.fill_price}, commission: {fill_event.commission}")
                    event_queue.put(fill_event)
            
            elif event.event_type == EventType.FILL:
                portfolio.process_fill_event(event)
    
    # Print results
    logger.info(f"\n{'='*50}")
    logger.info(f"Backtest Results")
    logger.info(f"{'='*50}")
    logger.info(f"Initial Capital: {portfolio.initial_capital:,.2f} RMB")
    logger.info(f"Final Equity: {portfolio.all_holdings[-1]['total']:,.2f} RMB")
    logger.info(f"Total Return: {(portfolio.all_holdings[-1]['total'] / portfolio.initial_capital - 1) * 100:.2f}%")
    logger.info(f"Total Realized PnL: {portfolio.total_realized_pnl:,.2f} RMB")
    logger.info(f"Number of Trades: {len(portfolio.positions)}")
    
    return portfolio

if __name__ == '__main__':
    china_a_share_backtest()
