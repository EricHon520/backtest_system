import sqlite3
from typing import List
from datetime import datetime
from pandas.core.generic import dt
import yfinance as yf
from binance.client import Client
import pytz


class DataLoader:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

        # read sql file and create table
        with open('historical_data.sql', 'r') as f:
            sql = f.read()
        self.cursor.execute(sql)
        self.conn.commit()

    def get_historical_data(self, tickers: List[str], start_time: datetime, end_time: datetime, frequency: str, timezone: str, source: str) -> List[dict]:
        self._check_edge_case(start_time=start_time, end_time=end_time)

        start_time_utc = self._convert_timezone(dt=start_time, from_tz=timezone, to_tz='UTC')
        end_time_utc = self._convert_timezone(dt=end_time, from_tz=timezone, to_tz='UTC')

        start_ts = int(start_time_utc.timestamp())
        end_ts = int(end_time_utc.timestamp())

        historical_data = []
        
        for ticker in tickers:
            not_exist_timestamps = self._check_exist_data(ticker=ticker, start_time=start_ts, end_time=end_ts, frequency=frequency)

            for not_exist_timestamp in not_exist_timestamps:
                start_time_dt_object_utc = datetime.utcfromtimestamp(not_exist_timestamp[0])
                end_time_dt_object_utc = datetime.utcfromtimestamp(not_exist_timestamp[1])
                
                if source == "stock":
                    data = self._get_stock_data(ticker=ticker, start_time=start_time_dt_object_utc, end_time=end_time_dt_object_utc, frequency=frequency, timezone=timezone)
                elif source == "crypto":
                    data = self._get_crypto_data(symbol=ticker, start_time=start_time_dt_object_utc, end_time=end_time_dt_object_utc, frequency=frequency, timezone=timezone)
                
                cleaned_data = self._data_preprocessing(data)
                self._store_data(cleaned_data)
                
            ticker_data = self._get_from_db(ticker=ticker, start_time=start_ts, end_time=end_ts, frequency=frequency, timezone=timezone)
            historical_data.extend(ticker_data)

        for item in historical_data:
            utc_dt = datetime.utcfromtimestamp(item['timestamp'])
            local_dt = self._convert_timezone(utc_dt, from_tz='UTC', to_tz=timezone)
            item['datetime_local'] = local_dt

        return historical_data

    def _get_stock_data(self, ticker: str, start_time: datetime, end_time:datetime, frequency: str, timezone: str) -> List[dict]:
        ticker_obj = yf.Ticker(ticker)
        data = ticker_obj.history(start=start_time, end=end_time, interval=frequency)

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


    def _get_crypto_data(self, symbol: str, start_time: datetime, end_time: datetime, frequency: str, timezone: str) -> List[dict]:
        client = Client()
        
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

        klines = client.get_historical_klines(
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

    def _data_preprocessing(self, data: List[dict]) -> List[dict]:
        cleaned_data = []

        for item in data:

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
            elif item['volume'] < 0:
                self._invalidate_data(item)    
            elif item['high'] < item['low']:
                self._invalidate_data(item)
            elif not (item['low'] <= item['open'] <= item['high']) or not (item['low'] <= item['close'] <= item['high']):
                self._invalidate_data(item)
            
            cleaned_data.append(item)
        
        return cleaned_data

    def _convert_timezone(self, dt: datetime, from_tz: str, to_tz: str) -> datetime:
        from_timezone = pytz.timezone(from_tz)
        to_timezone = pytz.timezone(to_tz)

        if dt.tzinfo is None:
            dt = from_timezone.localize(dt)

        return dt.astimezone(to_timezone)

    def _store_data(self, data: List[dict]) -> None:

        sql = """
        INSERT INTO historical_data
        (ticker, timestamp, frequency, open, high, low, close, volume, source, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        list_tuple_data = [
            (d['ticker'], d['timestamp'], d['frequency'], d['open'], d['high'], d['low'],
             d['close'], d['volume'], d['source'], d['created_at'])
            for d in data
        ]

        self.cursor.executemany(sql, list_tuple_data)
        self.conn.commit()

    def _check_exist_data(self, ticker: str, start_time: int, end_time: int, frequency: str) -> List[tuple]:

        sql = """
        SELECT timestamp FROM historical_data
        WHERE ticker = ?
        AND frequency = ?
        AND timestamp >= ?
        AND timestamp <= ?
        ORDER BY timestamp
        """

        self.cursor.execute(sql, (ticker, frequency, start_time, end_time))
        existing_timestamps = self.cursor.fetchall()

        existing_timestamps_list = [existing_timestamp[0] for existing_timestamp in existing_timestamps]

        if not existing_timestamps:
            return [(start_time, end_time)]

        # convert frequecy from string to integer
        unit = frequency[-1]
        interval_str = frequency[0:len(frequency) - 1]
        interval_int = int(interval_str)
        if unit == 'm':
            frequency_seconds = interval_int * 60
        elif unit == 'h':
            frequency_seconds = interval_int * 3600
        else:
            frequency_seconds = interval_int * 86400

        not_exist_timestamps = []

        if existing_timestamps_list[0] > start_time:
            not_exist_timestamps.append((start_time, existing_timestamps_list[0] - frequency_seconds))

        if existing_timestamps_list[-1] < end_time:
            not_exist_timestamps.append((existing_timestamps_list[-1] + frequency_seconds, end_time))

        for i in range(len(existing_timestamps_list ) - 1):
            gap = existing_timestamps_list[i + 1] - existing_timestamps_list[i]
            if gap > frequency_seconds:
                not_exist_start_time = existing_timestamps_list[i] + frequency_seconds
                not_exist_end_time = existing_timestamps_list[i + 1] - frequency_seconds
                not_exist_timestamps.append((not_exist_start_time, not_exist_end_time))

        return not_exist_timestamps

    def _check_edge_case(self, start_time: datetime, end_time: datetime) -> None:
        if start_time > end_time:
            raise ValueError(f"Invalid time range: start time ({start_time}) > end time ({end_time})")

    def _invalidate_data(self, item: dict) -> None:
        item['open'] = None
        item['high'] = None
        item['low'] = None
        item['close'] = None
        item['volume'] = None

    def _get_from_db(self, ticker: str, start_time: int, end_time: int, frequency: str, timezone: str) -> List[dict]:
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
