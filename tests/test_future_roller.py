"""
Unit tests for FutureRoller.

Covers:
- volume roll: highest-volume contract selected each bar
- expiry roll: stays with current contract until N days before expiry, then switches
- unadjusted: no price adjustment at roll
- panama: additive adjustment closes roll gap
- ratio: multiplicative adjustment closes roll gap
- get_active_contract mapping
- edge: single contract, no roll needed
"""

import pytest
from data.future_roller import FutureRoller


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bar(symbol, ts, open_=100.0, high=101.0, low=99.0, close=100.0, volume=1000):
    return {
        'ticker': symbol, 'timestamp': ts,
        'open': open_, 'high': high, 'low': low, 'close': close, 'volume': volume,
    }


# ---------------------------------------------------------------------------
# Volume roll
# ---------------------------------------------------------------------------

class TestVolumeRoll:

    def test_highest_volume_selected_each_bar(self):
        roller = FutureRoller(roll_trigger='volume')
        # ts=1: A volume=2000 > B volume=500 → A active
        # ts=2: B volume=3000 > A volume=100 → B active
        roller.add_contract('A', [
            _bar('A', 1, volume=2000),
            _bar('A', 2, volume=100),
        ])
        roller.add_contract('B', [
            _bar('B', 1, volume=500),
            _bar('B', 2, volume=3000),
        ])
        result = roller.roll(method='unadjusted')
        assert len(result) == 2
        assert result[0]['active_contract'] == 'A'
        assert result[1]['active_contract'] == 'B'

    def test_single_contract_always_active(self):
        roller = FutureRoller(roll_trigger='volume')
        roller.add_contract('A', [_bar('A', 1), _bar('A', 2)])
        result = roller.roll(method='unadjusted')
        assert all(r['active_contract'] == 'A' for r in result)

    def test_get_active_contract(self):
        roller = FutureRoller(roll_trigger='volume')
        roller.add_contract('A', [_bar('A', 1, volume=500)])
        roller.add_contract('B', [_bar('B', 1, volume=1000)])
        roller.roll(method='unadjusted')
        assert roller.get_active_contract(1) == 'B'


# ---------------------------------------------------------------------------
# Expiry roll
# ---------------------------------------------------------------------------

class TestExpiryRoll:

    def test_stays_with_first_contract_before_roll_window(self):
        """Roll N=1 day before expiry.  ts=1 is 5 days before expiry → stay with A."""
        roller = FutureRoller(roll_trigger='expiry')
        expiry_a = 6   # seconds-as-proxy for timestamps
        roller.add_contract('A', [_bar('A', 1), _bar('A', 2), _bar('A', 3)],
                            expiry_date=expiry_a)
        roller.add_contract('B', [_bar('B', 1), _bar('B', 2), _bar('B', 3)],
                            expiry_date=9999)
        result = roller.roll(method='unadjusted', rolling_days_before_expiry=0)
        # At ts=1 and 2, A not yet expired; at ts >= expiry_a roll to B
        actives = [r['active_contract'] for r in result]
        assert actives[0] == 'A'
        assert actives[1] == 'A'

    def test_rolls_to_next_contract_near_expiry(self):
        """ts=5, expiry_a=6, rolling_days=1 (roll_offset=86400s).
        At ts=5: 5 < 6-86400 is False → stays; but here unit is seconds
        so rolling_days_before_expiry=0 means roll at expiry bar itself.
        """
        roller = FutureRoller(roll_trigger='expiry')
        roller.add_contract('A', [_bar('A', 1), _bar('A', 5), _bar('A', 6)],
                            expiry_date=6)
        roller.add_contract('B', [_bar('B', 1), _bar('B', 5), _bar('B', 6)],
                            expiry_date=9999)
        result = roller.roll(method='unadjusted', rolling_days_before_expiry=0)
        by_ts = {r['timestamp']: r['active_contract'] for r in result}
        # at ts=6, A expired → B
        assert by_ts[6] == 'B'


# ---------------------------------------------------------------------------
# Price adjustment: unadjusted
# ---------------------------------------------------------------------------

class TestUnadjusted:

    def test_prices_unchanged(self):
        roller = FutureRoller(roll_trigger='volume')
        roller.add_contract('A', [_bar('A', 1, close=100.0, volume=2000)])
        roller.add_contract('B', [_bar('B', 1, close=110.0, volume=500)])
        result = roller.roll(method='unadjusted')
        assert result[0]['close'] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Price adjustment: panama (additive)
# ---------------------------------------------------------------------------

class TestPanamaAdjustment:

    def test_historical_bar_shifted_by_gap(self):
        """
        ts=1: A close=100 (active, volume=2000)
        ts=2: B close=95  (active, volume=3000)  ← roll occurs between ts=1 and ts=2
        Panama gap = A_close_at_roll - B_close_at_roll = 100 - 95 = +5
        → bar at ts=1 must have close adjusted UP by 5 → 105.
        Bar at ts=2 (new contract) unchanged → 95.
        """
        roller = FutureRoller(roll_trigger='volume')
        roller.add_contract('A', [
            _bar('A', 1, close=100.0, volume=2000),
            _bar('A', 2, close=101.0, volume=100),   # low volume after roll
        ])
        roller.add_contract('B', [
            _bar('B', 1, close=97.0, volume=500),
            _bar('B', 2, close=95.0, volume=3000),   # rolls here
        ])
        result = roller.roll(method='panama')
        by_ts = {r['timestamp']: r for r in result}
        # ts=2: B active, no adjustment (post-roll bar)
        assert by_ts[2]['close'] == pytest.approx(95.0)
        # ts=1: A active, adjusted +5 (100→95 gap)
        assert by_ts[1]['close'] == pytest.approx(100.0 + (100.0 - 95.0))

    def test_no_roll_no_adjustment(self):
        """Single contract → no roll → prices unchanged."""
        roller = FutureRoller(roll_trigger='volume')
        roller.add_contract('A', [_bar('A', 1, close=100.0), _bar('A', 2, close=102.0)])
        result = roller.roll(method='panama')
        assert result[0]['close'] == pytest.approx(100.0)
        assert result[1]['close'] == pytest.approx(102.0)


# ---------------------------------------------------------------------------
# Price adjustment: ratio (multiplicative)
# ---------------------------------------------------------------------------

class TestRatioAdjustment:

    def test_historical_bar_scaled_by_ratio(self):
        """
        Roll: A_close=100 (ts=1), B_close=80 (ts=2).
        ratio = 100/80 = 1.25
        ts=1 bar (pre-roll) close = 100 * 1.25 = 125.
        ts=2 bar (post-roll) close = 80 unchanged.
        """
        roller = FutureRoller(roll_trigger='volume')
        roller.add_contract('A', [
            _bar('A', 1, close=100.0, volume=2000),
            _bar('A', 2, close=101.0, volume=100),
        ])
        roller.add_contract('B', [
            _bar('B', 1, close=90.0, volume=500),
            _bar('B', 2, close=80.0, volume=3000),
        ])
        result = roller.roll(method='ratio')
        by_ts = {r['timestamp']: r for r in result}
        ratio = 100.0 / 80.0
        assert by_ts[1]['close'] == pytest.approx(100.0 * ratio)
        assert by_ts[2]['close'] == pytest.approx(80.0)

    def test_all_ohlc_fields_adjusted(self):
        """All of open/high/low/close must be adjusted, not just close."""
        roller = FutureRoller(roll_trigger='volume')
        roller.add_contract('A', [
            _bar('A', 1, open_=98.0, high=102.0, low=97.0, close=100.0, volume=2000),
            _bar('A', 2, close=101.0, volume=100),
        ])
        roller.add_contract('B', [
            _bar('B', 1, close=90.0, volume=500),
            _bar('B', 2, close=80.0, volume=3000),
        ])
        result = roller.roll(method='ratio')
        by_ts = {r['timestamp']: r for r in result}
        ratio = 100.0 / 80.0
        assert by_ts[1]['open'] == pytest.approx(98.0 * ratio)
        assert by_ts[1]['high'] == pytest.approx(102.0 * ratio)
        assert by_ts[1]['low'] == pytest.approx(97.0 * ratio)
        assert by_ts[1]['close'] == pytest.approx(100.0 * ratio)


# ---------------------------------------------------------------------------
# Output structure
# ---------------------------------------------------------------------------

class TestOutputStructure:

    def test_result_contains_active_contract_key(self):
        roller = FutureRoller(roll_trigger='volume')
        roller.add_contract('A', [_bar('A', 1)])
        result = roller.roll(method='unadjusted')
        assert 'active_contract' in result[0]

    def test_result_timestamps_sorted(self):
        roller = FutureRoller(roll_trigger='volume')
        roller.add_contract('A', [_bar('A', 3), _bar('A', 1), _bar('A', 2)])
        result = roller.roll(method='unadjusted')
        ts = [r['timestamp'] for r in result]
        assert ts == sorted(ts)

    def test_invalid_method_raises(self):
        roller = FutureRoller(roll_trigger='volume')
        roller.add_contract('A', [_bar('A', 1)])
        with pytest.raises(ValueError):
            roller.roll(method='invalid')
