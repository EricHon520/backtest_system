import queue
from data_handler import DataHandler
from datetime import datetime
from event import EventType, MarketEvent
from strategies.moving_average import MovingAverage
import logging
from execution_handler import ExecutionHandler
from portfolio import Portfolio
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def main():
    symbols = ['GOOG']
    start_time = datetime(2025, 1, 1)
    end_time = datetime(2025, 2, 1)
    frequency = '1h'
    timezone = 'Asia/Hong_Kong'
    source = 'stock'
    # Market type: 'china_a', 'us_stock', 'hk_stock', 'crypto'
    market_type = 'us_stock'
    
    data_handler = DataHandler(symbols=symbols, start_time=start_time, end_time=end_time, frequency=frequency, timezone=timezone, source=source)
    portfolio = Portfolio(data_handler=data_handler, initial_capital=10000.00, market_type=market_type)
    execution_handler = ExecutionHandler(data_handler=data_handler, rejection_rate=0.1, market_type=market_type)

    logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data_loader.log'),
                logging.StreamHandler()
            ]
        )

    logger = logging.getLogger(__name__)

    event_queue = queue.Queue()
    strategy = MovingAverage(data_handler=data_handler)

    while True:
        if data_handler.update_bars():
            latest_bar = data_handler.get_latest_bar(symbols[0])
            
            logger.info(f"Received Market event: {latest_bar}")
            
            # Process pending orders from previous bar FIRST
            # This ensures orders are filled on next bar's open price
            pending_fills = execution_handler.process_pending_orders()
            for fill_event in pending_fills:
                logger.info(f"Received Fill event (next bar) with price: {fill_event.fill_price}, quantity: {fill_event.quantity}")
                event_queue.put(fill_event)

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
                    logger.info(f"Received Signal event at {signal_event.datetime}")
                    event_queue.put(signal_event)

            elif event.event_type == EventType.SIGNAL:
                order_event = portfolio.process_signal_event(event)
                if order_event is not None:
                    logger.info(f"Received Order event with quantity: {order_event.quantity} ")
                    event_queue.put(order_event)
            
            elif event.event_type == EventType.ORDER:
                # Order is added to pending queue, will be filled on next bar
                fill_event = execution_handler.process_order_event(event)
                if fill_event is not None:
                    # Only if fill_on_next_bar=False
                    logger.info(f"Received Fill event (same bar) with price: {fill_event.fill_price}, quantity: {fill_event.quantity}")
                    event_queue.put(fill_event)

            elif event.event_type == EventType.FILL:
                portfolio.process_fill_event(event)

            

    if len(portfolio.all_holdings) > 0:
        df = pd.DataFrame(portfolio.all_holdings)
        # 移除時區後綴（如 'HKT'）以避免解析錯誤
        df['time'] = df['time'].astype(str).str.replace(r'\s+[A-Z]{3,4}$', '', regex=True)
        df['time'] = pd.to_datetime(df['time'])
        df = df.set_index('time')

        ax = df['total'].plot(figsize=(12, 6), title='Equity Curve')
        ax.set_xlabel('Time')
        ax.set_ylabel('Equity')

        plt.tight_layout()
        plt.savefig('equity_curve.png', dpi=150)
        plt.close()

    return None

if __name__ == '__main__':
    main()