"""
Test script for slippage model functionality
"""

from core.market_rule import MarketRule
import math

def test_volume_based_slippage():
    print("=" * 70)
    print("Testing Volume-Based Slippage Model")
    print("=" * 70)
    
    # Test with A-share market
    rules = MarketRule('china_a')
    
    print(f"\nMarket: {rules.market_name}")
    print(f"Slippage Model: {rules.slippage_model}")
    print(f"Volume Slippage Factor: {rules.volume_slippage_factor}")
    
    # Scenario 1: Small order relative to volume
    print("\n" + "-" * 70)
    print("Scenario 1: Small Order (Low Market Impact)")
    print("-" * 70)
    
    base_price = 100.0
    quantity = 100  # 100 shares
    bar_volume = 1000000  # 1M shares traded in the bar
    bar_high = 102.0
    bar_low = 98.0
    
    print(f"Base Price: ¥{base_price:.2f}")
    print(f"Order Size: {quantity} shares")
    print(f"Bar Volume: {bar_volume:,} shares")
    print(f"Bar Range: ¥{bar_low:.2f} - ¥{bar_high:.2f}")
    print(f"Order as % of Volume: {(quantity/bar_volume)*100:.4f}%")
    
    for direction in ['BUY', 'SELL']:
        slipped_price = rules.calculate_slippage(
            symbol='600519.SS',
            quantity=quantity,
            price=base_price,
            direction=direction,
            bar_volume=bar_volume,
            bar_high=bar_high,
            bar_low=bar_low
        )
        slippage_bps = abs(slipped_price - base_price) / base_price * 10000
        print(f"  {direction}: ¥{base_price:.2f} → ¥{slipped_price:.2f} (slippage: {slippage_bps:.2f} bps)")
    
    # Scenario 2: Medium order
    print("\n" + "-" * 70)
    print("Scenario 2: Medium Order (Moderate Market Impact)")
    print("-" * 70)
    
    quantity = 10000  # 10K shares
    bar_volume = 100000  # 100K shares
    
    print(f"Base Price: ¥{base_price:.2f}")
    print(f"Order Size: {quantity:,} shares")
    print(f"Bar Volume: {bar_volume:,} shares")
    print(f"Bar Range: ¥{bar_low:.2f} - ¥{bar_high:.2f}")
    print(f"Order as % of Volume: {(quantity/bar_volume)*100:.2f}%")
    
    for direction in ['BUY', 'SELL']:
        slipped_price = rules.calculate_slippage(
            symbol='600519.SS',
            quantity=quantity,
            price=base_price,
            direction=direction,
            bar_volume=bar_volume,
            bar_high=bar_high,
            bar_low=bar_low
        )
        slippage_bps = abs(slipped_price - base_price) / base_price * 10000
        cost_impact = abs(slipped_price - base_price) * quantity
        print(f"  {direction}: ¥{base_price:.2f} → ¥{slipped_price:.2f} (slippage: {slippage_bps:.2f} bps, cost: ¥{cost_impact:.2f})")
    
    # Scenario 3: Large order
    print("\n" + "-" * 70)
    print("Scenario 3: Large Order (High Market Impact)")
    print("-" * 70)
    
    quantity = 50000  # 50K shares
    bar_volume = 100000  # 100K shares
    
    print(f"Base Price: ¥{base_price:.2f}")
    print(f"Order Size: {quantity:,} shares")
    print(f"Bar Volume: {bar_volume:,} shares")
    print(f"Bar Range: ¥{bar_low:.2f} - ¥{bar_high:.2f}")
    print(f"Order as % of Volume: {(quantity/bar_volume)*100:.2f}%")
    
    for direction in ['BUY', 'SELL']:
        slipped_price = rules.calculate_slippage(
            symbol='600519.SS',
            quantity=quantity,
            price=base_price,
            direction=direction,
            bar_volume=bar_volume,
            bar_high=bar_high,
            bar_low=bar_low
        )
        slippage_bps = abs(slipped_price - base_price) / base_price * 10000
        cost_impact = abs(slipped_price - base_price) * quantity
        print(f"  {direction}: ¥{base_price:.2f} → ¥{slipped_price:.2f} (slippage: {slippage_bps:.2f} bps, cost: ¥{cost_impact:.2f})")
    
    # Scenario 4: High volatility (wider spread)
    print("\n" + "-" * 70)
    print("Scenario 4: High Volatility Day (Wider Spread)")
    print("-" * 70)
    
    quantity = 10000
    bar_volume = 100000
    bar_high = 110.0  # 10% range
    bar_low = 90.0
    
    print(f"Base Price: ¥{base_price:.2f}")
    print(f"Order Size: {quantity:,} shares")
    print(f"Bar Volume: {bar_volume:,} shares")
    print(f"Bar Range: ¥{bar_low:.2f} - ¥{bar_high:.2f} (wider spread)")
    print(f"Spread: {((bar_high - bar_low) / base_price * 100):.2f}%")
    
    for direction in ['BUY', 'SELL']:
        slipped_price = rules.calculate_slippage(
            symbol='600519.SS',
            quantity=quantity,
            price=base_price,
            direction=direction,
            bar_volume=bar_volume,
            bar_high=bar_high,
            bar_low=bar_low
        )
        slippage_bps = abs(slipped_price - base_price) / base_price * 10000
        cost_impact = abs(slipped_price - base_price) * quantity
        print(f"  {direction}: ¥{base_price:.2f} → ¥{slipped_price:.2f} (slippage: {slippage_bps:.2f} bps, cost: ¥{cost_impact:.2f})")

def test_market_comparison():
    print("\n" + "=" * 70)
    print("Slippage Comparison Across Markets")
    print("=" * 70)
    
    markets = {
        'China A-Share': 'china_a',
        'US Stock': 'us_stock',
        'HK Stock': 'hk_stock',
        'Crypto': 'crypto'
    }
    
    # Common scenario
    base_price = 100.0
    quantity = 10000
    bar_volume = 100000
    bar_high = 102.0
    bar_low = 98.0
    direction = 'BUY'
    
    print(f"\nScenario: {direction} {quantity:,} units @ {base_price:.2f}")
    print(f"Bar Volume: {bar_volume:,}, Range: {bar_low:.2f} - {bar_high:.2f}")
    print(f"Order as % of Volume: {(quantity/bar_volume)*100:.2f}%")
    print()
    print(f"{'Market':<20} {'Factor':<10} {'Slipped Price':<15} {'Slippage (bps)':<18} {'Cost Impact':<15}")
    print("-" * 90)
    
    for name, market_type in markets.items():
        rules = MarketRule(market_type)
        
        slipped_price = rules.calculate_slippage(
            symbol='TEST',
            quantity=quantity,
            price=base_price,
            direction=direction,
            bar_volume=bar_volume,
            bar_high=bar_high,
            bar_low=bar_low
        )
        
        slippage_bps = abs(slipped_price - base_price) / base_price * 10000
        cost_impact = abs(slipped_price - base_price) * quantity
        
        print(f"{name:<20} {rules.volume_slippage_factor:<10.2f} {slipped_price:<15.4f} {slippage_bps:<18.2f} {cost_impact:<15.2f}")

def test_slippage_formula():
    print("\n" + "=" * 70)
    print("Understanding the Slippage Formula")
    print("=" * 70)
    
    print("\nFormula: slippage = volume_factor × √(order_volume_pct) × spread_pct")
    print("Where:")
    print("  - order_volume_pct = order_size / bar_volume")
    print("  - spread_pct = (high - low) / price")
    print("  - √ (square root) models non-linear market impact")
    
    rules = MarketRule('china_a')
    
    print(f"\nA-Share Market (volume_factor = {rules.volume_slippage_factor})")
    print("\nExample Calculation:")
    
    base_price = 100.0
    quantity = 10000
    bar_volume = 100000
    bar_high = 102.0
    bar_low = 98.0
    
    order_volume_pct = quantity / bar_volume
    spread_pct = (bar_high - bar_low) / base_price
    slippage_pct = rules.volume_slippage_factor * math.sqrt(order_volume_pct) * spread_pct
    
    print(f"  Order: {quantity:,} shares @ ¥{base_price:.2f}")
    print(f"  Bar Volume: {bar_volume:,} shares")
    print(f"  Bar Range: ¥{bar_low:.2f} - ¥{bar_high:.2f}")
    print()
    print(f"  Step 1: order_volume_pct = {quantity:,} / {bar_volume:,} = {order_volume_pct:.4f} ({order_volume_pct*100:.2f}%)")
    print(f"  Step 2: spread_pct = ({bar_high:.2f} - {bar_low:.2f}) / {base_price:.2f} = {spread_pct:.4f} ({spread_pct*100:.2f}%)")
    print(f"  Step 3: √(order_volume_pct) = √{order_volume_pct:.4f} = {math.sqrt(order_volume_pct):.4f}")
    print(f"  Step 4: slippage_pct = {rules.volume_slippage_factor} × {math.sqrt(order_volume_pct):.4f} × {spread_pct:.4f}")
    print(f"         = {slippage_pct:.6f} ({slippage_pct*100:.4f}%)")
    print()
    print(f"  BUY Price: ¥{base_price:.2f} × (1 + {slippage_pct:.6f}) = ¥{base_price * (1 + slippage_pct):.4f}")
    print(f"  SELL Price: ¥{base_price:.2f} × (1 - {slippage_pct:.6f}) = ¥{base_price * (1 - slippage_pct):.4f}")
    print()
    print(f"  Cost Impact (BUY): ¥{base_price * slippage_pct * quantity:.2f}")

def test_edge_cases():
    print("\n" + "=" * 70)
    print("Edge Cases and Limits")
    print("=" * 70)
    
    rules = MarketRule('china_a')
    
    # Case 1: Zero volume
    print("\nCase 1: Zero Volume Bar")
    slipped_price = rules.calculate_slippage(
        symbol='TEST',
        quantity=100,
        price=100.0,
        direction='BUY',
        bar_volume=0,
        bar_high=102.0,
        bar_low=98.0
    )
    print(f"  Result: Uses default 0.1% slippage → ¥{slipped_price:.2f}")
    
    # Case 2: Very large order (capped at 1%)
    print("\nCase 2: Extremely Large Order (Slippage Cap)")
    slipped_price = rules.calculate_slippage(
        symbol='TEST',
        quantity=1000000,  # 1M shares
        price=100.0,
        direction='BUY',
        bar_volume=10000,  # Only 10K volume
        bar_high=110.0,
        bar_low=90.0
    )
    print(f"  Order: 1,000,000 shares, Bar Volume: 10,000")
    print(f"  Result: Capped at 1% maximum → ¥{slipped_price:.2f}")
    
    # Case 3: Disabled slippage
    print("\nCase 3: Disabled Slippage Model")
    rules.slippage_model = "none"
    slipped_price = rules.calculate_slippage(
        symbol='TEST',
        quantity=10000,
        price=100.0,
        direction='BUY',
        bar_volume=100000,
        bar_high=102.0,
        bar_low=98.0
    )
    print(f"  Result: No slippage applied → ¥{slipped_price:.2f}")

if __name__ == '__main__':
    test_volume_based_slippage()
    test_market_comparison()
    test_slippage_formula()
    test_edge_cases()
    
    print("\n" + "=" * 70)
    print("All slippage tests completed!")
    print("=" * 70)
