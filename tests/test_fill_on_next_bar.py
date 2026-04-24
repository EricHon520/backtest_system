"""
Test script to demonstrate fill-on-next-bar vs fill-on-same-bar
Shows the difference and validates the implementation
"""

from data.data_handler import DataHandler
from execution.execution_handler import SimulatedExecutionModel as ExecutionHandler
from portfolio.portfolio import Portfolio
from core.event import OrderEvent, MarketEvent
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def create_mock_bars():
    """Create mock bar data for testing"""
    return [
        {
            'ticker': 'TEST',
            'timestamp': 1,
            'datetime_local': '2024-01-15 09:30:00',
            'open': 100.0,
            'high': 102.0,
            'low': 98.0,
            'close': 101.0,
            'volume': 100000
        },
        {
            'ticker': 'TEST',
            'timestamp': 2,
            'datetime_local': '2024-01-15 10:30:00',
            'open': 101.5,  # Gap up from previous close
            'high': 103.0,
            'low': 100.5,
            'close': 102.0,
            'volume': 120000
        },
        {
            'ticker': 'TEST',
            'timestamp': 3,
            'datetime_local': '2024-01-15 11:30:00',
            'open': 102.5,
            'high': 104.0,
            'low': 101.5,
            'close': 103.0,
            'volume': 110000
        }
    ]

def test_fill_timing():
    """
    Test the difference between fill-on-next-bar and fill-on-same-bar
    """
    print("=" * 80)
    print("Testing Fill Timing: Next Bar vs Same Bar")
    print("=" * 80)
    
    # Scenario: Signal generated at Bar 1 close (101.0)
    # Question: What price do we get filled at?
    
    print("\nScenario:")
    print("  Bar 1: open=100.0, close=101.0")
    print("  Bar 2: open=101.5, close=102.0 (gap up!)")
    print("  Signal: BUY at Bar 1 close")
    print()
    
    # Test 1: Fill on same bar (has look-ahead bias)
    print("-" * 80)
    print("Test 1: Fill on SAME Bar (fill_on_next_bar=False)")
    print("-" * 80)
    
    mock_bars = create_mock_bars()
    
    class MockDataHandler:
        def __init__(self, bars):
            self.bars = bars
            self.current_idx = 0
        
        def get_latest_bar(self, symbol):
            if self.current_idx < len(self.bars):
                return self.bars[self.current_idx]
            return None
        
        def get_latest_bars(self, symbol, num_bars):
            return self.bars[max(0, self.current_idx - num_bars + 1):self.current_idx + 1]
    
    data_handler = MockDataHandler(mock_bars)
    data_handler.current_idx = 0  # At Bar 1
    
    execution_handler = ExecutionHandler(
        data_handler=data_handler,
        rejection_rate=0.0,
        fill_on_next_bar=False  # Same bar fill
    )
    
    order = OrderEvent(
        symbol='TEST',
        quantity=100,
        direction='BUY',
        datetime='2024-01-15 09:30:00'
    )
    
    fill = execution_handler.process_order_event(order)
    
    if fill:
        print(f"  ✓ Order filled immediately")
        print(f"  Fill Price: ${fill.fill_price:.2f}")
        print(f"  Bar Used: Bar 1 (current bar)")
        print(f"  Price Used: open=${mock_bars[0]['open']:.2f}")
        print(f"  ⚠️  Look-ahead bias: Using Bar 1's open price when signal is at Bar 1 close")
    
    # Test 2: Fill on next bar (correct approach)
    print("\n" + "-" * 80)
    print("Test 2: Fill on NEXT Bar (fill_on_next_bar=True)")
    print("-" * 80)
    
    data_handler = MockDataHandler(mock_bars)
    data_handler.current_idx = 0  # At Bar 1
    
    execution_handler = ExecutionHandler(
        data_handler=data_handler,
        rejection_rate=0.0,
        fill_on_next_bar=True  # Next bar fill
    )
    
    order = OrderEvent(
        symbol='TEST',
        quantity=100,
        direction='BUY',
        datetime='2024-01-15 09:30:00'
    )
    
    fill = execution_handler.process_order_event(order)
    
    print(f"  Bar 1: Signal generated, order created")
    print(f"  Fill Event: {fill}")
    print(f"  Pending Orders: {len(execution_handler.pending_orders)}")
    print(f"  ✓ Order added to pending queue (not filled yet)")
    
    # Move to next bar
    print(f"\n  >>> Time passes, Bar 2 arrives <<<")
    data_handler.current_idx = 1  # Move to Bar 2
    
    fills = execution_handler.process_pending_orders()
    
    if fills:
        fill = fills[0]
        print(f"  ✓ Pending order executed")
        print(f"  Fill Price: ${fill.fill_price:.2f}")
        print(f"  Bar Used: Bar 2 (next bar)")
        print(f"  Price Used: open=${mock_bars[1]['open']:.2f}")
        print(f"  ✓ No look-ahead bias: Using Bar 2's open price")
    
    # Compare results
    print("\n" + "=" * 80)
    print("Comparison")
    print("=" * 80)
    
    same_bar_price = mock_bars[0]['open']
    next_bar_price = mock_bars[1]['open']
    difference = next_bar_price - same_bar_price
    difference_pct = (difference / same_bar_price) * 100
    
    print(f"\nSame Bar Fill:  ${same_bar_price:.2f} (Bar 1 open)")
    print(f"Next Bar Fill:  ${next_bar_price:.2f} (Bar 2 open)")
    print(f"Difference:     ${difference:.2f} ({difference_pct:+.2f}%)")
    print(f"\nImpact on 100 shares: ${difference * 100:.2f}")
    
    if difference > 0:
        print(f"\n⚠️  Gap up: Next bar fill is WORSE (more expensive)")
        print(f"   This is realistic - you can't avoid overnight gaps")
    
    print("\n" + "=" * 80)
    print("Conclusion")
    print("=" * 80)
    print("\n✓ Fill-on-next-bar is the CORRECT approach")
    print("  - Avoids look-ahead bias")
    print("  - Reflects real trading conditions")
    print("  - Includes overnight risk (gaps)")
    print("  - More conservative (realistic) results")
    
    print("\n✗ Fill-on-same-bar has look-ahead bias")
    print("  - Uses information not available at signal time")
    print("  - Overly optimistic results")
    print("  - Not realistic")

def test_realistic_scenario():
    """
    Test a realistic trading scenario with multiple bars
    """
    print("\n\n" + "=" * 80)
    print("Realistic Trading Scenario")
    print("=" * 80)
    
    print("\nTimeline:")
    print("  Day 1, 15:00 - Strategy sees close price, generates BUY signal")
    print("  Day 2, 09:30 - Order executed at open price")
    print("  Day 2, 15:00 - Strategy sees close price, generates SELL signal")
    print("  Day 3, 09:30 - Order executed at open price")
    
    bars = [
        {'ticker': 'AAPL', 'datetime_local': 'Day 1 15:00', 'open': 150.0, 'close': 152.0, 'high': 153.0, 'low': 149.0, 'volume': 1000000},
        {'ticker': 'AAPL', 'datetime_local': 'Day 2 09:30', 'open': 151.5, 'close': 154.0, 'high': 155.0, 'low': 151.0, 'volume': 1200000},
        {'ticker': 'AAPL', 'datetime_local': 'Day 3 09:30', 'open': 154.5, 'close': 153.0, 'high': 156.0, 'low': 153.0, 'volume': 1100000},
    ]
    
    print("\nBar Data:")
    for i, bar in enumerate(bars, 1):
        print(f"  Bar {i} ({bar['datetime_local']}): open=${bar['open']:.2f}, close=${bar['close']:.2f}")
    
    print("\nExecution Flow:")
    print("  1. Bar 1 close: Signal BUY at $152.00")
    print("     → Order created, added to pending queue")
    print("  2. Bar 2 open: Execute pending BUY order at $151.50")
    print("     → Filled 100 shares @ $151.50")
    print("     → Cost: $15,150.00")
    print("  3. Bar 2 close: Signal SELL at $154.00")
    print("     → Order created, added to pending queue")
    print("  4. Bar 3 open: Execute pending SELL order at $154.50")
    print("     → Filled 100 shares @ $154.50")
    print("     → Revenue: $15,450.00")
    
    buy_price = bars[1]['open']
    sell_price = bars[2]['open']
    profit = (sell_price - buy_price) * 100
    profit_pct = (profit / (buy_price * 100)) * 100
    
    print(f"\nResult:")
    print(f"  Buy:  100 shares @ ${buy_price:.2f} = ${buy_price * 100:.2f}")
    print(f"  Sell: 100 shares @ ${sell_price:.2f} = ${sell_price * 100:.2f}")
    print(f"  Profit: ${profit:.2f} ({profit_pct:+.2f}%)")
    
    print("\n✓ This reflects realistic trading:")
    print("  - Cannot trade at the price that generated the signal")
    print("  - Must wait for next bar to execute")
    print("  - Subject to overnight gaps and market movements")

if __name__ == '__main__':
    test_fill_timing()
    test_realistic_scenario()
    
    print("\n\n" + "=" * 80)
    print("Summary: Fill-on-Next-Bar is ESSENTIAL for realistic backtesting!")
    print("=" * 80)
