import os
import sqlite3
import json
from typing import List
from datetime import datetime
import yfinance as yf
from binance.client import Client
import pytz
import time
import logging
import warnings


from core.types import BarData

class DataLoader:

    _BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def __init__(self, db_path: str = None, price_jump_threshold: float = 0.5):
        if db_path is None:
            db_path = os.path.join(self._BASE_DIR, 'db', 'historical_data.db')
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.price_jump_threshold = price_jump_threshold

        self.last_request_time = {'stock': 0, 'crypto': 0}
        self.rate_limit_delay = {'stock': 0.2, 'crypto': 0.1}

        warnings.filterwarnings('ignore', category=FutureWarning, module='yfinance')
        warnings.filterwarnings('ignore', message='.*Timestamp.utcnow.*')

        self.logger = logging.getLogger(__name__)
        logging.getLogger('yfinance').setLevel(logging.CRITICAL)

        # read sql file and create table
        sql_path = os.path.join(self._BASE_DIR, 'historical_data.sql')
        with open(sql_path, 'r') as f:
            sql = f.read()
        self.cursor.execute(sql)
        self.conn.commit()

        self._ranges_path = os.path.join(self._BASE_DIR, 'db', 'fetched_ranges.json')
        self._fetched_ranges = self._load_fetched_ranges()

        self._binance_client = Client()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        return False

    def get_historical_data(self, tickers: List[str], start_time: datetime, end_time: datetime, frequency: str, timezone: str, source: str) -> List[BarData]:
        self._check_edge_case(start_time=start_time, end_time=end_time)

        start_time_utc = self._convert_timezone(dt=start_time, from_tz=timezone, to_tz='UTC')
        end_time_utc = self._convert_timezone(dt=end_time, from_tz=timezone, to_tz='UTC')

        start_ts = int(start_time_utc.timestamp())
        end_ts = int(end_time_utc.timestamp())

        historical_data = []
        
        for ticker in tickers:
            not_exist_timestamps = self._check_exist_data(ticker=ticker, start_time=start_ts, end_time=end_ts, frequency=frequency)
            if not_exist_timestamps:
                for not_exist_timestamp in not_exist_timestamps:
                    check_aggregate = self._get_best_api_interval(target_interval=frequency, source=source)

                    start_time_dt_object_utc = datetime.utcfromtimestamp(not_exist_timestamp[0])
                    end_time_dt_object_utc = datetime.utcfromtimestamp(not_exist_timestamp[1])

                    if check_aggregate[1]:
                        if source == "stock":
                            data = self._get_stock_data(ticker=ticker, start_time=start_time_dt_object_utc, end_time=end_time_dt_object_utc, frequency=check_aggregate[0], timezone=timezone)
                        elif source == "crypto":
                            data = self._get_crypto_data(symbol=ticker, start_time=start_time_dt_object_utc, end_time=end_time_dt_object_utc, frequency=check_aggregate[0], timezone=timezone)
                        data = self._aggregate_data(data=data, aggregate_count=check_aggregate[-1], target_interval=frequency)
                    else:    
                        if source == "stock":
                            data = self._get_stock_data(ticker=ticker, start_time=start_time_dt_object_utc, end_time=end_time_dt_object_utc, frequency=frequency, timezone=timezone)
                        elif source == "crypto":
                            data = self._get_crypto_data(symbol=ticker, start_time=start_time_dt_object_utc, end_time=end_time_dt_object_utc, frequency=frequency, timezone=timezone)
                    
                    cleaned_data = self._data_preprocessing(data)
                    self._store_data(cleaned_data)
                    self._update_fetched_ranges(ticker, frequency, not_exist_timestamp[0], not_exist_timestamp[1])
                
            ticker_data = self._get_from_db(ticker=ticker, start_time=start_ts, end_time=end_ts, frequency=frequency, timezone=timezone)
            historical_data.extend(ticker_data)

        for item in historical_data:
            utc_dt = datetime.utcfromtimestamp(item['timestamp'])
            local_dt = self._convert_timezone(utc_dt, from_tz='UTC', to_tz=timezone)
            item['datetime_local'] = local_dt.strftime('%Y-%m-%d %H:%M:%S %Z')

        return historical_data

    def _get_stock_data(self, ticker: str, start_time: datetime, end_time:datetime, frequency: str, timezone: str) -> List[BarData]:
        self._apply_rate_limit('stock')
        
        try:
            ticker_obj = yf.Ticker(ticker)
            data = ticker_obj.history(start=start_time, end=end_time, interval=frequency)
            
            if data is None or data.empty:
                self.logger.warning(f"No data returned from yfinance for {ticker} from {start_time} to {end_time}")
                return []
        except Exception as e:
            self.logger.error(f"Error fetching data for {ticker} from {start_time} to {end_time}: {str(e)}")
            return []

        result = []
        for timestamp, row in data.iterrows():
            result.append({
                'ticker': ticker,
                'timestamp': int(timestamp.timestamp()),
                'frequency': frequency,
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume'],
                'source': 'stock',
                'timezone': timezone,
                'created_at': int(datetime.now().timestamp())
            }
            )
        return result


    def _get_crypto_data(self, symbol: str, start_time: datetime, end_time: datetime, frequency: str, timezone: str) -> List[BarData]:
        self._apply_rate_limit('crypto')
        
        interval_mapping = {
            '1m': Client.KLINE_INTERVAL_1MINUTE,
            '3m': Client.KLINE_INTERVAL_3MINUTE,
            '5m': Client.KLINE_INTERVAL_5MINUTE,
            '15m': Client.KLINE_INTERVAL_15MINUTE,
            '30m': Client.KLINE_INTERVAL_30MINUTE,
            '1h': Client.KLINE_INTERVAL_1HOUR,
            '2h': Client.KLINE_INTERVAL_2HOUR,
            '4h': Client.KLINE_INTERVAL_4HOUR,
            '6h': Client.KLINE_INTERVAL_6HOUR,
            '8h': Client.KLINE_INTERVAL_8HOUR,
            '12h': Client.KLINE_INTERVAL_12HOUR,
            '1d': Client.KLINE_INTERVAL_1DAY,
            '3d': Client.KLINE_INTERVAL_3DAY,
            '1w': Client.KLINE_INTERVAL_1WEEK,
            '1M': Client.KLINE_INTERVAL_1MONTH,
        }

        interval = interval_mapping.get(frequency)

        if not interval:
            raise ValueError(f"Unsupported frequency: {frequency}")

        klines = self._binance_client.get_historical_klines(
            symbol=symbol, 
            interval=interval,
            start_str=start_time.strftime('%Y-%m-%d %H:%M:%S'),
            end_str=end_time.strftime('%Y-%m-%d %H:%M:%S')
            )
        
        result = []

        for kline in klines:
            result.append({
                'ticker': symbol,
                'timestamp': int(kline[0] / 1000),
                'frequency': frequency,
                'open': float(kline[1]),
                'high': float(kline[2]),
                'low': float(kline[3]),
                'close': float(kline[4]),
                'volume': float(kline[5]),
                'source': 'crypto',
                'timezone': timezone,
                'created_at': int(datetime.now().timestamp())
            })

        return result

    def _data_preprocessing(self, data: List[BarData]) -> List[BarData]:
        cleaned_data = []
        
        prev_close = None
        for i, item in enumerate(data):

            if (item.get('open') is None or
                item.get('high') is None or
                item.get('low') is None or
                item.get('close') is None or
                item.get('volume') is None):
                self._invalidate_data(item)
                cleaned_data.append(item)
                continue

            if item['open'] <= 0 or item['high'] <= 0 or item['low'] <= 0 or item['close'] <= 0:
                self._invalidate_data(item)
                cleaned_data.append(item)
                continue
            elif item['volume'] < 0:
                self._invalidate_data(item)
                cleaned_data.append(item)
                continue
            elif item['high'] < item['low']:
                self._invalidate_data(item)
                cleaned_data.append(item)
                continue
            elif not (item['low'] <= item['open'] <= item['high']) or not (item['low'] <= item['close'] <= item['high']):
                self._invalidate_data(item)
                cleaned_data.append(item)
                continue
            
            if prev_close is not None and item['open'] is not None:
                self._check_price_jump(item, prev_close)
            
            if item['close'] is not None:
                prev_close = item['close']
            
            cleaned_data.append(item)
        
        return cleaned_data

    def _convert_timezone(self, dt: datetime, from_tz: str, to_tz: str) -> datetime:
        from_timezone = pytz.timezone(from_tz)
        to_timezone = pytz.timezone(to_tz)

        if dt.tzinfo is None:
            dt = from_timezone.localize(dt)

        return dt.astimezone(to_timezone)

    def _store_data(self, data: List[BarData]) -> None:

        sql = """
        INSERT OR IGNORE INTO historical_data
        (ticker, timestamp, frequency, open, high, low, close, volume, source, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        list_tuple_data = [
            (d['ticker'], d['timestamp'], d['frequency'], d['open'], d['high'], d['low'],
             d['close'], d['volume'], d['source'], d['created_at'])
            for d in data
        ]

        self.cursor.executemany(sql, list_tuple_data)
        self.conn.commit()

    def _check_exist_data(self, ticker: str, start_time: int, end_time: int, frequency: str) -> List[tuple]:
        key = f"{ticker}__{frequency}"
        fetched = self._fetched_ranges.get(key, [])

        # Use one bar period as the adjacency threshold so that two fetched
        # ranges separated by exactly one bar period are treated as contiguous,
        # rather than the naive ±1 second which is meaningless for daily bars.
        bar_period = self._parse_interval(frequency)

        uncovered = [(start_time, end_time)]
        for f_start, f_end in fetched:
            next_uncovered = []
            for u_start, u_end in uncovered:
                if f_end < u_start or f_start > u_end:
                    next_uncovered.append((u_start, u_end))
                else:
                    if u_start < f_start:
                        next_uncovered.append((u_start, f_start - bar_period))
                    if u_end > f_end:
                        next_uncovered.append((f_end + bar_period, u_end))
            uncovered = [(s, e) for s, e in next_uncovered if s <= e]

        return uncovered

    def _load_fetched_ranges(self) -> dict:
        if os.path.exists(self._ranges_path):
            with open(self._ranges_path, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {}
        return {}

    def _save_fetched_ranges(self) -> None:
        with open(self._ranges_path, 'w') as f:
            json.dump(self._fetched_ranges, f)

    def _update_fetched_ranges(self, ticker: str, frequency: str, start_time: int, end_time: int) -> None:
        key = f"{ticker}__{frequency}"
        ranges = self._fetched_ranges.get(key, [])
        ranges.append([start_time, end_time])
        ranges.sort(key=lambda x: x[0])

        # Use the same bar_period adjacency threshold as _check_exist_data so
        # that two ranges separated by exactly one bar period are merged here
        # and not treated as a gap requiring a re-fetch.
        bar_period = self._parse_interval(frequency)

        merged = [ranges[0]]
        for current in ranges[1:]:
            last = merged[-1]
            if current[0] <= last[1] + bar_period:
                last[1] = max(last[1], current[1])
            else:
                merged.append(current)

        self._fetched_ranges[key] = merged
        self._save_fetched_ranges()

    def _check_edge_case(self, start_time: datetime, end_time: datetime) -> None:
        if start_time > end_time:
            raise ValueError(f"Invalid time range: start time ({start_time}) > end time ({end_time})")

    def _invalidate_data(self, item: BarData) -> None:
        item['open'] = None
        item['high'] = None
        item['low'] = None
        item['close'] = None
        item['volume'] = None

    def _get_from_db(self, ticker: str, start_time: int, end_time: int, frequency: str, timezone: str) -> List[BarData]:
        sql = """
        SELECT ticker, timestamp, frequency, open, high, low, close, volume, source FROM historical_data
        WHERE ticker = ?
        AND frequency = ?
        AND timestamp >= ?
        AND timestamp <= ?
        """

        self.cursor.execute(sql, (ticker, frequency, start_time, end_time))
        rows = self.cursor.fetchall()
        
        result = []
        for row in rows:
            result.append({
                'ticker': row[0],
                'timestamp': row[1],
                'frequency': row[2],
                'open': row[3],
                'high': row[4],
                'low': row[5],
                'close': row[6],
                'volume': row[7],
                'source': row[8],
                'timezone': timezone
            })
        
        return result

    def _get_best_api_interval(self, target_interval: str, source: str) -> tuple[str, bool, int]:
        if source == 'stock':
            if target_interval == 'w':
                target_interval = 'wk'
            supported_intervals = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo']
        elif source == 'crypto':
            supported_intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']

        if target_interval in supported_intervals:
            return (target_interval, False, 1)

        target_interval_sec = self._parse_interval(target_interval)

        best_api_interval = ''
        best_api_interval_sec = 0

        for supported_interval in supported_intervals:
            supported_interval_sec = self._parse_interval(supported_interval)
            if supported_interval_sec < target_interval_sec:
                if target_interval_sec % supported_interval_sec == 0:
                    # Keep the largest divisible interval to minimise API calls
                    if supported_interval_sec > best_api_interval_sec:
                        best_api_interval = supported_interval
                        best_api_interval_sec = supported_interval_sec

        if best_api_interval_sec == 0:
            raise ValueError(
                f"No supported API interval can aggregate into '{target_interval}' "
                f"for source '{source}'. Supported: {supported_intervals}"
            )

        aggregate_count = target_interval_sec // best_api_interval_sec

        return (best_api_interval, True, aggregate_count)

    def _parse_interval(self, interval: str) -> int:
        conversion_table = {
            'm': 60,
            'h': 3600,
            'd': 86400,
            'w': 604800,
            'wk': 604800,
            'mo': 2592000,
            'M': 2592000
        }
        
        if interval.endswith('wk'):
            num = int(interval[:-2])
            unit = 'wk'
        elif interval.endswith('mo'):
            num = int(interval[:-2])
            unit = 'mo'
        else:
            num = int(interval[:-1])
            unit = interval[-1]

        return num * conversion_table[unit]

    def _aggregate_data(self, data: List[BarData], aggregate_count: int, target_interval: str) -> List[BarData]:

        aggregated_data = []
        
        for i in range(0, len(data), aggregate_count):
            group = data[i:i + aggregate_count]

            if len(group) < aggregate_count:
                continue

            valid_bars = [bar for bar in group if bar['high'] is not None and bar['low'] is not None and bar['volume'] is not None]
            if not valid_bars:
                continue

            open_price = next((bar['open'] for bar in group if bar['open'] is not None), None)
            close_price = next((bar['close'] for bar in reversed(group) if bar['close'] is not None), None)

            aggregated_bar = {
                'ticker': group[0]['ticker'],
                'timestamp': group[0]['timestamp'],
                'frequency': target_interval,
                'open': open_price,
                'high': max(bar['high'] for bar in valid_bars),
                'low': min(bar['low'] for bar in valid_bars),
                'close': close_price,
                'volume': sum(bar['volume'] for bar in valid_bars),
                'source': group[0]['source'],
                'timezone': group[0]['timezone'],
                'created_at': group[0]['created_at']
            }

            aggregated_data.append(aggregated_bar)

        return aggregated_data
    
    def _apply_rate_limit(self, source: str) -> None:
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time[source]
        
        if time_since_last_request < self.rate_limit_delay[source]:
            sleep_time = self.rate_limit_delay[source] - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time[source] = time.time()
    
    def _check_price_jump(self, current_bar: BarData, prev_close: float) -> None:
        if current_bar['open'] is None:
            return
        
        price_change = abs(current_bar['open'] - prev_close) / prev_close
        
        if price_change > self.price_jump_threshold:
            self.logger.warning(
                f"Price jump detected for {current_bar['ticker']} at timestamp {current_bar['timestamp']}: "
                f"prev_close={prev_close:.4f}, current_open={current_bar['open']:.4f}, "
                f"change={price_change*100:.2f}%"
            )

    def _check_delisted(self):
        pass