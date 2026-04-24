"""
Test script for market rules functionality
"""

from core.market_rule import MarketRule
from datetime import datetime
import pytz

def test_china_a_share_rules():
    print("=" * 60)
    print("Testing China A-Share Market Rules")
    print("=" * 60)
    
    rules = MarketRule('china_a')
    
    # Test 1: Lot size normalization
    print("\n1. Lot Size Normalization (100 shares per lot)")
    quantities = [50, 100, 150, 200, 250]
    for qty in quantities:
        normalized = rules.normalize_quantity(qty)
        print(f"   {qty} shares -> {normalized} shares")
    
    # Test 2: Price limit
    print("\n2. Price Limit (10% for main board)")
    prev_close = 100.0
    test_prices = [90.0, 100.0, 105.0, 110.0, 115.0]
    for price in test_prices:
        adjusted = rules.apply_price_limit('600519.SS', price, prev_close, 'BUY')
        print(f"   Price {price:.2f} -> {adjusted:.2f} (prev close: {prev_close:.2f})")
    
    # Test 3: Commission calculation
    print("\n3. Commission Calculation")
    quantity = 100
    price = 100.0
    for direction in ['BUY', 'SELL']:
        commission = rules.calculate_commission('600519.SS', quantity, price, direction)
        print(f"   {direction} {quantity} shares @ {price:.2f}: Commission = {commission:.2f} RMB")
    
    # Test 4: Trading hours
    print("\n4. Trading Hours Check")
    tz = pytz.timezone('Asia/Shanghai')
    test_times = [
        datetime(2024, 1, 15, 9, 0, 0),   # Before market open
        datetime(2024, 1, 15, 10, 0, 0),  # Morning session
        datetime(2024, 1, 15, 12, 0, 0),  # Lunch break
        datetime(2024, 1, 15, 14, 0, 0),  # Afternoon session
        datetime(2024, 1, 15, 16, 0, 0),  # After market close
        datetime(2024, 1, 13, 10, 0, 0),  # Saturday
    ]
    for dt in test_times:
        dt_local = tz.localize(dt)
        is_trading = rules.is_trading_time(dt_local)
        print(f"   {dt.strftime('%Y-%m-%d %H:%M')} ({dt.strftime('%A')}): {'✓ Trading' if is_trading else '✗ Closed'}")
    
    # Test 5: Order validation
    print("\n5. Order Validation")
    test_cases = [
        ('600519.SS', 100, 100.0, 'BUY', datetime(2024, 1, 15, 10, 0, 0)),
        ('600519.SS', 50, 100.0, 'BUY', datetime(2024, 1, 15, 10, 0, 0)),   # Invalid lot size
        ('600519.SS', 100, 100.0, 'BUY', datetime(2024, 1, 15, 16, 0, 0)),  # After hours
    ]
    for symbol, qty, price, direction, dt in test_cases:
        dt_local = tz.localize(dt)
        is_valid, msg = rules.validate_order(symbol, qty, price, direction, dt_local)
        status = "✓ Valid" if is_valid else f"✗ Invalid: {msg}"
        print(f"   {direction} {qty} shares @ {dt.strftime('%H:%M')}: {status}")

def test_us_stock_rules():
    print("\n" + "=" * 60)
    print("Testing US Stock Market Rules")
    print("=" * 60)
    
    rules = MarketRule('us_stock')
    
    print(f"\nMarket: {rules.market_name}")
    print(f"Settlement: T+{rules.settlement_days}")
    print(f"Lot Size: {rules.lot_size} share(s)")
    print(f"Commission: {rules.commission_rate * 100}%")
    print(f"Allow Short: {rules.allow_short}")
    
    # Test commission
    print("\nCommission Calculation:")
    commission = rules.calculate_commission('AAPL', 100, 150.0, 'BUY')
    print(f"   BUY 100 shares @ $150.00: Commission = ${commission:.2f}")

def test_hk_stock_rules():
    print("\n" + "=" * 60)
    print("Testing Hong Kong Stock Market Rules")
    print("=" * 60)
    
    rules = MarketRule('hk_stock')
    
    print(f"\nMarket: {rules.market_name}")
    print(f"Settlement: T+{rules.settlement_days}")
    print(f"Lot Size: {rules.lot_size} share(s)")
    print(f"Commission Rate: {rules.commission_rate * 100}%")
    print(f"Stamp Duty: {rules.stamp_duty * 100}%")
    
    # Test commission
    print("\nCommission Calculation:")
    for direction in ['BUY', 'SELL']:
        commission = rules.calculate_commission('0700.HK', 100, 300.0, direction)
        print(f"   {direction} 100 shares @ HK$300.00: Total Fees = HK${commission:.2f}")

def test_crypto_rules():
    print("\n" + "=" * 60)
    print("Testing Cryptocurrency Market Rules")
    print("=" * 60)
    
    rules = MarketRule('crypto')
    
    print(f"\nMarket: {rules.market_name}")
    print(f"Settlement: T+{rules.settlement_days} (Instant)")
    print(f"Trading Hours: 24/7")
    print(f"Commission: {rules.commission_rate * 100}%")
    
    # Test 24/7 trading
    print("\nTrading Hours (24/7):")
    test_times = [
        datetime(2024, 1, 15, 0, 0, 0),
        datetime(2024, 1, 15, 12, 0, 0),
        datetime(2024, 1, 13, 10, 0, 0),  # Saturday
    ]
    for dt in test_times:
        is_trading = rules.is_trading_time(dt)
        print(f"   {dt.strftime('%Y-%m-%d %H:%M')} ({dt.strftime('%A')}): {'✓ Trading' if is_trading else '✗ Closed'}")

def compare_markets():
    print("\n" + "=" * 60)
    print("Market Comparison Summary")
    print("=" * 60)
    
    markets = {
        'China A-Share': 'china_a',
        'US Stock': 'us_stock',
        'HK Stock': 'hk_stock',
        'Crypto': 'crypto'
    }
    
    print(f"\n{'Market':<15} {'Settlement':<12} {'Lot Size':<10} {'Commission':<12} {'Short':<8}")
    print("-" * 60)
    
    for name, market_type in markets.items():
        rules = MarketRule(market_type)
        print(f"{name:<15} T+{rules.settlement_days:<11} {rules.lot_size:<10} {rules.commission_rate*100:>6.2f}%{'':<5} {'Yes' if rules.allow_short else 'No':<8}")

if __name__ == '__main__':
    test_china_a_share_rules()
    test_us_stock_rules()
    test_hk_stock_rules()
    test_crypto_rules()
    compare_markets()
    
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Assert-based pytest tests (run via pytest, not __main__)
# ---------------------------------------------------------------------------

import pytest


class TestCommissionCalculation:
    """Verify calculate_commission arithmetic including contract_multiplier."""

    def test_stock_commission_no_multiplier(self):
        rule = MarketRule('us_stock')
        comm = rule.calculate_commission('AAPL', 100, 150.0, 'BUY', contract_multiplier=1)
        trade_value = 100 * 150.0 * 1
        expected = max(trade_value * rule.commission_rate, rule.min_commission)
        assert comm == pytest.approx(expected)

    def test_futures_commission_uses_multiplier(self):
        """With multiplier=10, commission must be 10x larger than with multiplier=1."""
        import market_rules.china_future_rule
        rule = MarketRule('china_future')
        comm_1 = rule.calculate_commission('IF2401', 1, 5000.0, 'BUY',
                                            contract_multiplier=1)
        comm_10 = rule.calculate_commission('IF2401', 1, 5000.0, 'BUY',
                                             contract_multiplier=10)
        assert comm_10 == pytest.approx(comm_1 * 10, rel=0.01)

    def test_futures_commission_correct_absolute_value(self):
        import market_rules.china_future_rule
        rule = MarketRule('china_future')
        # trade_value = 1 * 5000 * 10 = 50000
        trade_value = 1 * 5000.0 * 10
        expected = max(trade_value * rule.commission_rate, rule.min_commission)
        comm = rule.calculate_commission('IF2401', 1, 5000.0, 'BUY',
                                          contract_multiplier=10)
        assert comm == pytest.approx(expected)

    def test_china_a_stamp_duty_on_sell_only(self):
        rule = MarketRule('china_a')
        buy_comm  = rule.calculate_commission('600519.SS', 100, 100.0, 'BUY')
        sell_comm = rule.calculate_commission('600519.SS', 100, 100.0, 'SELL')
        # SELL includes stamp duty; BUY does not → sell_comm > buy_comm
        assert sell_comm > buy_comm

    def test_min_commission_floor_applied(self):
        rule = MarketRule('us_stock')
        # 1 share @ $0.01: trade_value = 0.01; rate * 0.01 << min_commission
        comm = rule.calculate_commission('AAPL', 1, 0.01, 'BUY')
        assert comm == pytest.approx(rule.min_commission)

    def test_lot_size_normalization(self):
        rule = MarketRule('china_a')
        assert rule.normalize_quantity(150) == 100
        assert rule.normalize_quantity(200) == 200
        assert rule.normalize_quantity(50)  == 0

    def test_price_limit_clips_above_max(self):
        rule = MarketRule('china_a')
        capped = rule.apply_price_limit('600519.SS', 115.0, 100.0, 'BUY')
        assert capped == pytest.approx(110.0, rel=1e-6)   # capped at 10% up-limit

    def test_price_limit_clips_below_min(self):
        rule = MarketRule('china_a')
        capped = rule.apply_price_limit('600519.SS', 85.0, 100.0, 'SELL')
        assert capped >= 90.0   # ≥10% down-limit
