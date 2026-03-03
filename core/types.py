from typing import TypedDict
from datetime import datetime


class BarData(TypedDict):
    """K線數據結構"""
    ticker: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class TradeData(TypedDict):
    """成交數據結構"""
    symbol: str
    quantity: int
    price: float
    timestamp: datetime
    direction: str  # 'BUY' or 'SELL'
