import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
import sqlite3
from datetime import datetime
import tempfile
import os
import pytz
from data.data_loader import DataLoader


class TestDataLoader(unittest.TestCase):

    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
        self.sql_content = """CREATE TABLE IF NOT EXISTS historical_data (
            ticker TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            frequency TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            source TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (ticker, timestamp, frequency)
        )"""
        
        with patch('builtins.open', mock_open(read_data=self.sql_content)):
            self.loader = DataLoader(self.db_path)

    def tearDown(self):
        if hasattr(self, 'loader'):
            self.loader.conn.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_init_creates_database_connection(self):
        self.assertIsNotNone(self.loader.conn)
        self.assertIsNotNone(self.loader.cursor)
        self.assertEqual(self.loader.db_path, self.db_path)

    def test_init_creates_table(self):
        self.loader.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='historical_data'")
        result = self.loader.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'historical_data')

    def test_convert_timezone_naive_datetime(self):
        dt = datetime(2024, 1, 1, 12, 0, 0)
        result = self.loader._convert_timezone(dt, from_tz='America/New_York', to_tz='UTC')
        
        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.tzinfo.zone, 'UTC')

    def test_convert_timezone_aware_datetime(self):
        tz = pytz.timezone('America/New_York')
        dt = tz.localize(datetime(2024, 1, 1, 12, 0, 0))
        result = self.loader._convert_timezone(dt, from_tz='America/New_York', to_tz='Asia/Hong_Kong')
        
        self.assertEqual(result.tzinfo.zone, 'Asia/Hong_Kong')

    def test_check_edge_case_valid_time_range(self):
        start_time = datetime(2024, 1, 1, 0, 0, 0)
        end_time = datetime(2024, 1, 2, 0, 0, 0)
        
        try:
            self.loader._check_edge_case(start_time, end_time)
        except ValueError:
            self.fail("_check_edge_case raised ValueError unexpectedly")

    def test_check_edge_case_invalid_time_range(self):
        start_time = datetime(2024, 1, 2, 0, 0, 0)
        end_time = datetime(2024, 1, 1, 0, 0, 0)
        
        with self.assertRaises(ValueError) as context:
            self.loader._check_edge_case(start_time, end_time)
        
        self.assertIn("Invalid time range", str(context.exception))

    def test_invalidate_data(self):
        item = {
            'open': 100.0,
            'high': 110.0,
            'low': 90.0,
            'close': 105.0,
            'volume': 1000.0
        }
        
        self.loader._invalidate_data(item)
        
        self.assertIsNone(item['open'])
        self.assertIsNone(item['high'])
        self.assertIsNone(item['low'])
        self.assertIsNone(item['close'])
        self.assertIsNone(item['volume'])

    def test_data_preprocessing_valid_data(self):
        data = [{
            'ticker': 'AAPL',
            'timestamp': 1704067200,
            'frequency': '1d',
            'open': 100.0,
            'high': 110.0,
            'low': 95.0,
            'close': 105.0,
            'volume': 1000.0,
            'source': 'stock',
            'timezone': 'UTC',
            'created_at': 1704067200
        }]
        
        result = self.loader._data_preprocessing(data)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['open'], 100.0)
        self.assertEqual(result[0]['close'], 105.0)

    def test_data_preprocessing_missing_values(self):
        data = [{
            'ticker': 'AAPL',
            'timestamp': 1704067200,
            'frequency': '1d',
            'open': None,
            'high': 110.0,
            'low': 95.0,
            'close': 105.0,
            'volume': 1000.0,
            'source': 'stock',
            'timezone': 'UTC',
            'created_at': 1704067200
        }]
        
        result = self.loader._data_preprocessing(data)
        
        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]['open'])
        self.assertIsNone(result[0]['high'])
        self.assertIsNone(result[0]['low'])
        self.assertIsNone(result[0]['close'])
        self.assertIsNone(result[0]['volume'])

    def test_data_preprocessing_negative_price(self):
        data = [{
            'ticker': 'AAPL',
            'timestamp': 1704067200,
            'frequency': '1d',
            'open': -100.0,
            'high': 110.0,
            'low': 95.0,
            'close': 105.0,
            'volume': 1000.0,
            'source': 'stock',
            'timezone': 'UTC',
            'created_at': 1704067200
        }]
        
        result = self.loader._data_preprocessing(data)
        
        self.assertIsNone(result[0]['open'])

    def test_data_preprocessing_negative_volume(self):
        data = [{
            'ticker': 'AAPL',
            'timestamp': 1704067200,
            'frequency': '1d',
            'open': 100.0,
            'high': 110.0,
            'low': 95.0,
            'close': 105.0,
            'volume': -1000.0,
            'source': 'stock',
            'timezone': 'UTC',
            'created_at': 1704067200
        }]
        
        result = self.loader._data_preprocessing(data)
        
        self.assertIsNone(result[0]['volume'])

    def test_data_preprocessing_high_less_than_low(self):
        data = [{
            'ticker': 'AAPL',
            'timestamp': 1704067200,
            'frequency': '1d',
            'open': 100.0,
            'high': 90.0,
            'low': 95.0,
            'close': 105.0,
            'volume': 1000.0,
            'source': 'stock',
            'timezone': 'UTC',
            'created_at': 1704067200
        }]
        
        result = self.loader._data_preprocessing(data)
        
        self.assertIsNone(result[0]['high'])

    def test_data_preprocessing_open_outside_range(self):
        data = [{
            'ticker': 'AAPL',
            'timestamp': 1704067200,
            'frequency': '1d',
            'open': 120.0,
            'high': 110.0,
            'low': 95.0,
            'close': 105.0,
            'volume': 1000.0,
            'source': 'stock',
            'timezone': 'UTC',
            'created_at': 1704067200
        }]
        
        result = self.loader._data_preprocessing(data)
        
        self.assertIsNone(result[0]['open'])

    def test_data_preprocessing_close_outside_range(self):
        data = [{
            'ticker': 'AAPL',
            'timestamp': 1704067200,
            'frequency': '1d',
            'open': 100.0,
            'high': 110.0,
            'low': 95.0,
            'close': 115.0,
            'volume': 1000.0,
            'source': 'stock',
            'timezone': 'UTC',
            'created_at': 1704067200
        }]
        
        result = self.loader._data_preprocessing(data)
        
        self.assertIsNone(result[0]['close'])

    def test_store_data(self):
        data = [{
            'ticker': 'AAPL',
            'timestamp': 1704067200,
            'frequency': '1d',
            'open': 100.0,
            'high': 110.0,
            'low': 95.0,
            'close': 105.0,
            'volume': 1000.0,
            'source': 'stock',
            'created_at': 1704067200
        }]
        
        self.loader._store_data(data)
        
        self.loader.cursor.execute("SELECT * FROM historical_data WHERE ticker='AAPL'")
        result = self.loader.cursor.fetchone()
        
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'AAPL')
        self.assertEqual(result[1], 1704067200)
        self.assertEqual(result[3], 100.0)

    def test_check_exist_data_no_existing_data(self):
        result = self.loader._check_exist_data(
            ticker='AAPL',
            start_time=1704067200,
            end_time=1704153600,
            frequency='1d'
        )
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], (1704067200, 1704153600))

    def test_check_exist_data_with_existing_data(self):
        data = [{
            'ticker': 'AAPL',
            'timestamp': 1704067200,
            'frequency': '1d',
            'open': 100.0,
            'high': 110.0,
            'low': 95.0,
            'close': 105.0,
            'volume': 1000.0,
            'source': 'stock',
            'created_at': 1704067200
        }]
        self.loader._store_data(data)
        
        result = self.loader._check_exist_data(
            ticker='AAPL',
            start_time=1704067200,
            end_time=1704153600,
            frequency='1d'
        )
        
        self.assertEqual(len(result), 1)

    def test_check_exist_data_with_gaps(self):
        data = [
            {
                'ticker': 'AAPL',
                'timestamp': 1704067200,
                'frequency': '1h',
                'open': 100.0,
                'high': 110.0,
                'low': 95.0,
                'close': 105.0,
                'volume': 1000.0,
                'source': 'stock',
                'created_at': 1704067200
            },
            {
                'ticker': 'AAPL',
                'timestamp': 1704078000,
                'frequency': '1h',
                'open': 105.0,
                'high': 115.0,
                'low': 100.0,
                'close': 110.0,
                'volume': 1200.0,
                'source': 'stock',
                'created_at': 1704067200
            }
        ]
        self.loader._store_data(data)
        
        result = self.loader._check_exist_data(
            ticker='AAPL',
            start_time=1704067200,
            end_time=1704078000,
            frequency='1h'
        )
        
        self.assertGreaterEqual(len(result), 1)

    def test_get_from_db(self):
        data = [{
            'ticker': 'AAPL',
            'timestamp': 1704067200,
            'frequency': '1d',
            'open': 100.0,
            'high': 110.0,
            'low': 95.0,
            'close': 105.0,
            'volume': 1000.0,
            'source': 'stock',
            'created_at': 1704067200
        }]
        self.loader._store_data(data)
        
        result = self.loader._get_from_db(
            ticker='AAPL',
            start_time=1704067200,
            end_time=1704153600,
            frequency='1d',
            timezone='UTC'
        )
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['ticker'], 'AAPL')
        self.assertEqual(result[0]['open'], 100.0)
        self.assertEqual(result[0]['close'], 105.0)

    def test_get_from_db_no_data(self):
        result = self.loader._get_from_db(
            ticker='AAPL',
            start_time=1704067200,
            end_time=1704153600,
            frequency='1d',
            timezone='UTC'
        )
        
        self.assertEqual(len(result), 0)

    @patch('data.data_loader.yf.Ticker')
    def test_get_stock_data(self, mock_ticker):
        mock_df = MagicMock()
        mock_df.empty = False
        mock_df.iterrows.return_value = [
            (datetime(2024, 1, 1, 0, 0, 0, tzinfo=pytz.UTC), {
                'Open': 100.0,
                'High': 110.0,
                'Low': 95.0,
                'Close': 105.0,
                'Volume': 1000.0
            })
        ]
        
        mock_ticker_instance = MagicMock()
        mock_ticker_instance.history.return_value = mock_df
        mock_ticker.return_value = mock_ticker_instance
        
        result = self.loader._get_stock_data(
            ticker='AAPL',
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
            frequency='1d',
            timezone='UTC'
        )
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['ticker'], 'AAPL')
        self.assertEqual(result[0]['open'], 100.0)
        self.assertEqual(result[0]['source'], 'stock')

    def test_get_crypto_data(self):
        mock_client = MagicMock()
        mock_client.get_historical_klines.return_value = [
            [1704067200000, '100.0', '110.0', '95.0', '105.0', '1000.0', 1704067260000, '105000.0', 100, '500.0', '52500.0', '0']
        ]
        self.loader._binance_client = mock_client
        
        result = self.loader._get_crypto_data(
            symbol='BTCUSDT',
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
            frequency='1m',
            timezone='UTC'
        )
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['ticker'], 'BTCUSDT')
        self.assertEqual(result[0]['open'], 100.0)
        self.assertEqual(result[0]['source'], 'crypto')

    @patch('data.data_loader.Client')
    def test_get_crypto_data_invalid_frequency(self, mock_client_class):
        with self.assertRaises(ValueError) as context:
            self.loader._get_crypto_data(
                symbol='BTCUSDT',
                start_time=datetime(2024, 1, 1),
                end_time=datetime(2024, 1, 2),
                frequency='invalid',
                timezone='UTC'
            )
        
        self.assertIn("Unsupported frequency", str(context.exception))

    @patch.object(DataLoader, '_get_stock_data')
    @patch.object(DataLoader, '_check_exist_data')
    def test_get_historical_data_stock(self, mock_check_exist, mock_get_stock):
        mock_check_exist.return_value = [(1704067200, 1704153600)]
        mock_get_stock.return_value = [{
            'ticker': 'AAPL',
            'timestamp': 1704067200,
            'frequency': '1d',
            'open': 100.0,
            'high': 110.0,
            'low': 95.0,
            'close': 105.0,
            'volume': 1000.0,
            'source': 'stock',
            'timezone': 'UTC',
            'created_at': 1704067200
        }]
        
        result = self.loader.get_historical_data(
            tickers=['AAPL'],
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
            frequency='1d',
            timezone='UTC',
            source='stock'
        )
        
        self.assertIsInstance(result, list)
        self.assertTrue(all('datetime_local' in item for item in result))

    @patch.object(DataLoader, '_get_crypto_data')
    @patch.object(DataLoader, '_check_exist_data')
    def test_get_historical_data_crypto(self, mock_check_exist, mock_get_crypto):
        mock_check_exist.return_value = [(1704067200, 1704153600)]
        mock_get_crypto.return_value = [{
            'ticker': 'BTCUSDT',
            'timestamp': 1704067200,
            'frequency': '1h',
            'open': 40000.0,
            'high': 41000.0,
            'low': 39000.0,
            'close': 40500.0,
            'volume': 100.0,
            'source': 'crypto',
            'timezone': 'UTC',
            'created_at': 1704067200
        }]
        
        result = self.loader.get_historical_data(
            tickers=['BTCUSDT'],
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
            frequency='1h',
            timezone='UTC',
            source='crypto'
        )
        
        self.assertIsInstance(result, list)
        self.assertTrue(all('datetime_local' in item for item in result))


if __name__ == '__main__':
    unittest.main()
