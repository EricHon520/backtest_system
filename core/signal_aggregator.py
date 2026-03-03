"""
SignalAggregator — resolves conflicts when multiple strategies emit signals
for the same symbol on the same bar.

Aggregation modes
-----------------
FIRST_WINS   : keep the signal from the first strategy that spoke.
LAST_WINS    : keep the signal from the last strategy that spoke.
MAJORITY     : take the signal_type voted by the majority of strategies.
WEIGHTED     : weighted vote using each signal's confidence; highest net
               score wins.  Ties go to the later signal.
VETO_ON_CONFLICT : if any two signals disagree, emit nothing for that
                   symbol (conservative / do-nothing on disagreement).

The aggregator is plugged into the Engine and is fully replaceable via
the PluggableSignalAggregator ABC.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from core.event import SignalEvent


# ---------------------------------------------------------------------------
# ABC
# ---------------------------------------------------------------------------

class SignalAggregatorBase(ABC):
    """Abstract base class — one method to implement."""

    def aggregate(self, signals: List[SignalEvent]) -> List[SignalEvent]:
        """
        Consume a flat list of SignalEvents (possibly from multiple strategies)
        and return the resolved list that the Engine should enqueue.

        EXIT signals that appear alongside a directional signal for the same
        symbol are always forwarded first (before the directional signal) so
        that existing positions are cleanly closed.  Only when a symbol has
        *exclusively* EXIT signals (no directional companion) does the EXIT
        go through normal aggregation.

        Implementations must not mutate the input list.
        """
        # Separate symbols that have both EXIT and directional signals from
        # those that have only one type.
        groups = _group_by_symbol(signals)
        always_exit: List[SignalEvent] = []       # guaranteed to pass through
        remainder: List[SignalEvent] = []         # goes to _aggregate_impl

        for symbol, group in groups.items():
            has_directional = any(s.signal_type in ('LONG', 'SHORT') for s in group)
            has_exit = any(s.signal_type == 'EXIT' for s in group)
            if has_exit and has_directional:
                # EXIT travels first; directional goes into aggregation.
                exit_sig = max(
                    (s for s in group if s.signal_type == 'EXIT'),
                    key=lambda s: s.confidence,
                )
                always_exit.append(exit_sig)
                remainder.extend(s for s in group if s.signal_type != 'EXIT')
            else:
                remainder.extend(group)

        aggregated = self._aggregate_impl(remainder)
        return always_exit + aggregated

    @abstractmethod
    def _aggregate_impl(self, signals: List[SignalEvent]) -> List[SignalEvent]:
        """
        Core aggregation logic.  Called with signals that have already been
        pre-filtered so no symbol has both EXIT and directional signals in
        this list simultaneously.

        Implementations must not mutate the input list.
        """
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DIRECTION_SCORE: Dict[str, int] = {'LONG': 1, 'SHORT': -1, 'EXIT': 0}


def _group_by_symbol(
    signals: List[SignalEvent],
) -> Dict[str, List[SignalEvent]]:
    groups: Dict[str, List[SignalEvent]] = defaultdict(list)
    for sig in signals:
        groups[sig.symbol].append(sig)
    return groups


def _best_signal_for_type(
    candidates: List[SignalEvent], signal_type: str
) -> Optional[SignalEvent]:
    """Return the candidate with the highest confidence for a given type."""
    typed = [s for s in candidates if s.signal_type == signal_type]
    if not typed:
        return None
    return max(typed, key=lambda s: s.confidence)


# ---------------------------------------------------------------------------
# Concrete implementations
# ---------------------------------------------------------------------------

class FirstWinsAggregator(SignalAggregatorBase):
    """
    For each symbol, keep only the first signal received.
    All subsequent signals for the same symbol are discarded.
    """

    def _aggregate_impl(self, signals: List[SignalEvent]) -> List[SignalEvent]:
        seen: Dict[str, SignalEvent] = {}
        for sig in signals:
            if sig.symbol not in seen:
                seen[sig.symbol] = sig
        return list(seen.values())


class LastWinsAggregator(SignalAggregatorBase):
    """
    For each symbol, keep only the last signal received.
    """

    def _aggregate_impl(self, signals: List[SignalEvent]) -> List[SignalEvent]:
        seen: Dict[str, SignalEvent] = {}
        for sig in signals:
            seen[sig.symbol] = sig
        return list(seen.values())


class MajorityVoteAggregator(SignalAggregatorBase):
    """
    For each symbol, count votes per signal_type.  The most-voted type wins.
    On a tie, prefer EXIT > LONG > SHORT (conservative).
    The winning SignalEvent is the one with the highest confidence among
    the winners.
    """

    _TIE_PRIORITY = {'EXIT': 0, 'LONG': 1, 'SHORT': 2}

    def _aggregate_impl(self, signals: List[SignalEvent]) -> List[SignalEvent]:
        groups = _group_by_symbol(signals)
        result: List[SignalEvent] = []

        for symbol, group in groups.items():
            if len(group) == 1:
                result.append(group[0])
                continue

            votes: Dict[str, int] = defaultdict(int)
            for sig in group:
                votes[sig.signal_type] += 1

            max_votes = max(votes.values())
            winners = [t for t, v in votes.items() if v == max_votes]

            if len(winners) == 1:
                winning_type = winners[0]
            else:
                winners.sort(key=lambda t: self._TIE_PRIORITY.get(t, 99))
                winning_type = winners[0]

            best = _best_signal_for_type(group, winning_type)
            if best is not None:
                result.append(best)

        return result


class WeightedAggregator(SignalAggregatorBase):
    """
    For each symbol, compute a net directional score:
        score = Σ (direction_weight × confidence)
    where LONG=+1, SHORT=-1, EXIT=0.

    If score > 0  → emit the LONG signal with the highest confidence.
    If score < 0  → emit the SHORT signal with the highest confidence.
    If score == 0 → emit the EXIT signal (if any), else emit nothing.
    """

    def _aggregate_impl(self, signals: List[SignalEvent]) -> List[SignalEvent]:
        groups = _group_by_symbol(signals)
        result: List[SignalEvent] = []

        for symbol, group in groups.items():
            if len(group) == 1:
                result.append(group[0])
                continue

            net_score = sum(
                _DIRECTION_SCORE.get(sig.signal_type, 0) * sig.confidence
                for sig in group
            )

            if net_score > 0:
                best = _best_signal_for_type(group, 'LONG')
            elif net_score < 0:
                best = _best_signal_for_type(group, 'SHORT')
            else:
                best = _best_signal_for_type(group, 'EXIT')

            if best is not None:
                result.append(best)

        return result


class VetoOnConflictAggregator(SignalAggregatorBase):
    """
    Conservative: if any two signals for the same symbol disagree on
    direction (ignoring EXIT), emit nothing for that symbol.
    Unanimous signals (or single signals) pass through unchanged.
    EXIT signals are always forwarded if there is no directional conflict.
    """

    def _aggregate_impl(self, signals: List[SignalEvent]) -> List[SignalEvent]:
        groups = _group_by_symbol(signals)
        result: List[SignalEvent] = []

        for symbol, group in groups.items():
            if len(group) == 1:
                result.append(group[0])
                continue

            directional = {
                sig.signal_type for sig in group if sig.signal_type != 'EXIT'
            }
            if len(directional) > 1:
                continue

            best = max(group, key=lambda s: s.confidence)
            result.append(best)

        return result


# ---------------------------------------------------------------------------
# Default aggregator (used by Engine when none is supplied)
# ---------------------------------------------------------------------------

class SignalAggregator(WeightedAggregator):
    """
    Default aggregator used by Engine.

    Uses weighted voting: net directional score from confidence-weighted
    signals determines the winning direction.  Can be swapped out by
    passing a different SignalAggregatorBase subclass to Engine.
    """
    pass
