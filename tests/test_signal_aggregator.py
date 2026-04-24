"""
Unit tests for SignalAggregator and its concrete implementations.

Tests cover:
- No signals / single signal pass-through
- No-conflict multi-signal (same symbol, same direction)
- Conflict resolution per aggregation mode
- Confidence-weighted selection within a winning type
- EXIT handling
- Multi-symbol independence
"""

import pytest
from datetime import datetime
from core.event import SignalEvent
from core.signal_aggregator import (
    FirstWinsAggregator,
    LastWinsAggregator,
    MajorityVoteAggregator,
    WeightedAggregator,
    VetoOnConflictAggregator,
    SignalAggregator,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DT = datetime(2024, 1, 1, 9, 30)


def make_signal(symbol: str, signal_type: str, confidence: float = 1.0) -> SignalEvent:
    return SignalEvent(
        symbol=symbol,
        datetime=_DT,
        signal_type=signal_type,
        confidence=confidence,
    )


# ---------------------------------------------------------------------------
# Empty / single-signal pass-through (all aggregators)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("AggClass", [
    FirstWinsAggregator,
    LastWinsAggregator,
    MajorityVoteAggregator,
    WeightedAggregator,
    VetoOnConflictAggregator,
])
def test_empty_input_returns_empty(AggClass):
    agg = AggClass()
    assert agg.aggregate([]) == []


@pytest.mark.parametrize("AggClass", [
    FirstWinsAggregator,
    LastWinsAggregator,
    MajorityVoteAggregator,
    WeightedAggregator,
    VetoOnConflictAggregator,
])
def test_single_signal_passes_through(AggClass):
    agg = AggClass()
    sig = make_signal('AAPL', 'LONG')
    result = agg.aggregate([sig])
    assert result == [sig]


# ---------------------------------------------------------------------------
# FirstWinsAggregator
# ---------------------------------------------------------------------------

class TestFirstWinsAggregator:
    def setup_method(self):
        self.agg = FirstWinsAggregator()

    def test_keeps_first_on_conflict(self):
        first = make_signal('AAPL', 'LONG')
        second = make_signal('AAPL', 'SHORT')
        result = self.agg.aggregate([first, second])
        assert len(result) == 1
        assert result[0].signal_type == 'LONG'

    def test_multiple_symbols_independent(self):
        a = make_signal('AAPL', 'LONG')
        b = make_signal('GOOG', 'SHORT')
        result = self.agg.aggregate([a, b])
        symbols = {r.symbol for r in result}
        assert symbols == {'AAPL', 'GOOG'}

    def test_does_not_mutate_input(self):
        signals = [make_signal('AAPL', 'LONG'), make_signal('AAPL', 'SHORT')]
        original_len = len(signals)
        self.agg.aggregate(signals)
        assert len(signals) == original_len


# ---------------------------------------------------------------------------
# LastWinsAggregator
# ---------------------------------------------------------------------------

class TestLastWinsAggregator:
    def setup_method(self):
        self.agg = LastWinsAggregator()

    def test_keeps_last_on_conflict(self):
        first = make_signal('AAPL', 'LONG')
        second = make_signal('AAPL', 'SHORT')
        result = self.agg.aggregate([first, second])
        assert len(result) == 1
        assert result[0].signal_type == 'SHORT'

    def test_three_signals_same_symbol(self):
        # EXIT + directional signals for the same symbol:
        # EXIT is always forwarded first, then the directional signals are
        # resolved by _aggregate_impl (LastWins → SHORT, the last one).
        signals = [
            make_signal('AAPL', 'LONG'),
            make_signal('AAPL', 'SHORT'),
            make_signal('AAPL', 'EXIT'),
        ]
        result = self.agg.aggregate(signals)
        assert len(result) == 2
        assert result[0].signal_type == 'EXIT'
        assert result[1].signal_type == 'SHORT'


# ---------------------------------------------------------------------------
# MajorityVoteAggregator
# ---------------------------------------------------------------------------

class TestMajorityVoteAggregator:
    def setup_method(self):
        self.agg = MajorityVoteAggregator()

    def test_majority_wins(self):
        signals = [
            make_signal('AAPL', 'LONG'),
            make_signal('AAPL', 'LONG'),
            make_signal('AAPL', 'SHORT'),
        ]
        result = self.agg.aggregate(signals)
        assert len(result) == 1
        assert result[0].signal_type == 'LONG'

    def test_tie_prefers_exit_over_long(self):
        signals = [
            make_signal('AAPL', 'EXIT'),
            make_signal('AAPL', 'LONG'),
        ]
        result = self.agg.aggregate(signals)
        assert result[0].signal_type == 'EXIT'

    def test_tie_prefers_long_over_short(self):
        signals = [
            make_signal('AAPL', 'LONG'),
            make_signal('AAPL', 'SHORT'),
        ]
        result = self.agg.aggregate(signals)
        assert result[0].signal_type == 'LONG'

    def test_highest_confidence_selected_from_winners(self):
        signals = [
            make_signal('AAPL', 'LONG', confidence=0.5),
            make_signal('AAPL', 'LONG', confidence=0.9),
            make_signal('AAPL', 'SHORT', confidence=1.0),
        ]
        result = self.agg.aggregate(signals)
        assert result[0].signal_type == 'LONG'
        assert result[0].confidence == 0.9


# ---------------------------------------------------------------------------
# WeightedAggregator
# ---------------------------------------------------------------------------

class TestWeightedAggregator:
    def setup_method(self):
        self.agg = WeightedAggregator()

    def test_net_positive_emits_long(self):
        signals = [
            make_signal('AAPL', 'LONG', confidence=0.8),
            make_signal('AAPL', 'SHORT', confidence=0.3),
        ]
        result = self.agg.aggregate(signals)
        assert len(result) == 1
        assert result[0].signal_type == 'LONG'

    def test_net_negative_emits_short(self):
        signals = [
            make_signal('AAPL', 'LONG', confidence=0.2),
            make_signal('AAPL', 'SHORT', confidence=0.9),
        ]
        result = self.agg.aggregate(signals)
        assert result[0].signal_type == 'SHORT'

    def test_net_zero_emits_exit_if_present(self):
        signals = [
            make_signal('AAPL', 'LONG', confidence=0.5),
            make_signal('AAPL', 'SHORT', confidence=0.5),
            make_signal('AAPL', 'EXIT', confidence=0.8),
        ]
        result = self.agg.aggregate(signals)
        assert result[0].signal_type == 'EXIT'

    def test_net_zero_no_exit_emits_nothing(self):
        signals = [
            make_signal('AAPL', 'LONG', confidence=0.5),
            make_signal('AAPL', 'SHORT', confidence=0.5),
        ]
        result = self.agg.aggregate(signals)
        assert result == []

    def test_exit_signals_score_zero(self):
        # EXIT alongside a directional signal: EXIT is always forwarded first,
        # then the remaining LONG passes through _aggregate_impl unchanged.
        # The old expectation (LONG only) was the buggy behaviour where EXIT
        # was silently dropped because its score contribution is 0.
        signals = [
            make_signal('AAPL', 'LONG', confidence=0.6),
            make_signal('AAPL', 'EXIT', confidence=1.0),
        ]
        result = self.agg.aggregate(signals)
        assert len(result) == 2
        assert result[0].signal_type == 'EXIT'
        assert result[1].signal_type == 'LONG'

    def test_highest_confidence_long_selected(self):
        signals = [
            make_signal('AAPL', 'LONG', confidence=0.4),
            make_signal('AAPL', 'LONG', confidence=0.9),
            make_signal('AAPL', 'SHORT', confidence=0.1),
        ]
        result = self.agg.aggregate(signals)
        assert result[0].confidence == 0.9

    def test_multi_symbol_independent(self):
        signals = [
            make_signal('AAPL', 'LONG', confidence=0.8),
            make_signal('AAPL', 'SHORT', confidence=0.3),
            make_signal('GOOG', 'SHORT', confidence=0.9),
            make_signal('GOOG', 'LONG', confidence=0.1),
        ]
        result = self.agg.aggregate(signals)
        by_symbol = {r.symbol: r for r in result}
        assert by_symbol['AAPL'].signal_type == 'LONG'
        assert by_symbol['GOOG'].signal_type == 'SHORT'


# ---------------------------------------------------------------------------
# VetoOnConflictAggregator
# ---------------------------------------------------------------------------

class TestVetoOnConflictAggregator:
    def setup_method(self):
        self.agg = VetoOnConflictAggregator()

    def test_conflicting_direction_emits_nothing(self):
        signals = [
            make_signal('AAPL', 'LONG'),
            make_signal('AAPL', 'SHORT'),
        ]
        result = self.agg.aggregate(signals)
        assert result == []

    def test_unanimous_direction_passes_through(self):
        signals = [
            make_signal('AAPL', 'LONG', confidence=0.5),
            make_signal('AAPL', 'LONG', confidence=0.9),
        ]
        result = self.agg.aggregate(signals)
        assert len(result) == 1
        assert result[0].signal_type == 'LONG'
        assert result[0].confidence == 0.9

    def test_exit_with_unanimous_direction_passes(self):
        # EXIT + directional: EXIT always passes through first, then
        # LONG passes through _aggregate_impl (single signal, no conflict).
        signals = [
            make_signal('AAPL', 'LONG', confidence=0.7),
            make_signal('AAPL', 'EXIT', confidence=1.0),
        ]
        result = self.agg.aggregate(signals)
        assert len(result) == 2
        assert result[0].signal_type == 'EXIT'
        assert result[1].signal_type == 'LONG'

    def test_non_conflicting_symbols_unaffected(self):
        signals = [
            make_signal('AAPL', 'LONG'),
            make_signal('AAPL', 'SHORT'),
            make_signal('GOOG', 'LONG'),
        ]
        result = self.agg.aggregate(signals)
        assert len(result) == 1
        assert result[0].symbol == 'GOOG'


# ---------------------------------------------------------------------------
# Default SignalAggregator is WeightedAggregator
# ---------------------------------------------------------------------------

def test_default_signal_aggregator_is_weighted():
    agg = SignalAggregator()
    signals = [
        make_signal('AAPL', 'LONG', confidence=0.8),
        make_signal('AAPL', 'SHORT', confidence=0.3),
    ]
    result = agg.aggregate(signals)
    assert result[0].signal_type == 'LONG'
