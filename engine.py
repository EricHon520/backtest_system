import queue
from data_handler import DataHandler
from datetime import datetime
from event import EventType, MarketEvent
from strategies.moving_average import MovingAverage

def main():
    symbols = ['BTCUSDT']
    start_time = datetime(2025,1,1)
    end_time = datetime(2025,2,1)
    frequency = '1h'
    timezone = 'Asia/Hong_Kong'
    source = 'crypto'
    data_handler = DataHandler(symbols=symbols, start_time=start_time, end_time=end_time, frequency=frequency, timezone=timezone, source=source)

    event_queue = queue.Queue()
    strategy = MovingAverage(data_handler=data_handler, symbols=symbols)

    while True:
        if data_handler.update_bars():
            latest_bar = data_handler.get_latest_bar(symbols[0])
            print(f"latest bar: {latest_bar}")

            event = MarketEvent()

            event_queue.put(event)
        else:
            break
        
        while not event_queue.empty():
            event = event_queue.get()
            if event.event_type == EventType.MARKET:
                signal_events = strategy.calculate_signals()
                for signal in signal_events:
                    event_queue.put(signal)
            

    return None

if __name__ == '__main__':
    main()