from typing import List, Optional


class FutureRoller:
    """
    Builds a continuous futures series by rolling across multiple contracts.

    roll_trigger:
        'volume'  — at each bar the contract with highest volume is the active one.
        'expiry'  — roll to the next-expiry contract `rolling_days_before_expiry`
                    calendar days before the current active contract's registered
                    expiry_date.  Requires expiry_date to be registered via
                    add_contract().
    """

    def __init__(self, roll_trigger: str = 'volume', min_volume_ratio: float = 1.2):
        if roll_trigger not in ('volume', 'expiry'):
            raise ValueError(f"Unknown roll_trigger '{roll_trigger}'. Use 'volume' or 'expiry'.")
        self.contracts = {}
        self.expiry_date = {}
        self.roll_trigger = roll_trigger
        self.min_volume_ratio = min_volume_ratio

    def add_contract(self, symbol: str, bars: List, expiry_date: Optional[int] = None):
        self.contracts[symbol] = bars
        if expiry_date is not None:
            self.expiry_date[symbol] = expiry_date

    def roll(self, method: str = 'unadjusted', rolling_days_before_expiry: int = 1) -> List[dict]:
        if method not in ['unadjusted', 'panama', 'ratio']:
            raise ValueError(f"Unknown method: {method}")

        # collect all the timestamps
        all_timestamps = set()
        for bars in self.contracts.values():
            for bar in bars:
                all_timestamps.add(bar['timestamp'])
        sorted_timestamps = sorted(all_timestamps)

        # select main contract
        continuous_data = []
        self._active_contract_map = {}

        roll_offset_seconds = rolling_days_before_expiry * 86400

        for ts in sorted_timestamps:
            # find all contracts at that timestamp
            available = []
            for symbol, bars in self.contracts.items():
                bar = self._find_bar_by_timestamps(bars=bars, timestamp=ts)
                if bar:
                    available.append({'symbol': symbol, 'bar': bar})
            if not available:
                continue

            if self.roll_trigger == 'volume':
                main = max(available, key=lambda x: x['bar']['volume'])
            else:
                # expiry-based: prefer the contract whose expiry is furthest
                # away *but* only switch off the current contract when it is
                # within rolling_days_before_expiry of its expiry.
                prev_active = self._active_contract_map.get(
                    sorted_timestamps[sorted_timestamps.index(ts) - 1]
                ) if sorted_timestamps.index(ts) > 0 else None

                def _expiry_sort_key(item):
                    exp = self.expiry_date.get(item['symbol'])
                    if exp is None:
                        return float('inf')
                    return exp

                # Stay with the previous contract unless it's about to expire
                if prev_active is not None:
                    prev_expiry = self.expiry_date.get(prev_active)
                    still_valid = (
                        prev_expiry is None or ts < prev_expiry - roll_offset_seconds
                    )
                    if still_valid:
                        prev_bar = self._find_bar_by_timestamps(
                            self.contracts.get(prev_active, []), ts
                        )
                        main = {'symbol': prev_active, 'bar': prev_bar} if prev_bar else min(
                            available, key=_expiry_sort_key
                        )
                    else:
                        # Roll: pick next non-expired contract with nearest expiry
                        candidates = [
                            item for item in available
                            if self.expiry_date.get(item['symbol'], float('inf')) > ts
                            and item['symbol'] != prev_active
                        ]
                        main = min(candidates, key=_expiry_sort_key) if candidates else min(
                            available, key=_expiry_sort_key
                        )
                else:
                    main = min(available, key=_expiry_sort_key)

            row = {
                'timestamp': ts,
                'active_contract': main['symbol'],
                **main['bar']
            }
            continuous_data.append(row)
            self._active_contract_map[ts] = main['symbol']

        # ------------------------------------------------------------------
        # Price adjustment: remove roll gaps from the continuous series.
        #
        # panama  — additive: shift all *historical* bars by a constant
        #           so their close aligns with the incoming contract's close
        #           on the roll day.  Preserves absolute price differences.
        # ratio   — multiplicative: scale all historical bars by the ratio
        #           of the two closes on the roll day.  Preserves percentage
        #           returns (use this for return-based analysis).
        # ------------------------------------------------------------------
        if method != 'unadjusted' and len(continuous_data) > 1:
            # Walk forward, detect contract changes, accumulate adjustments.
            cumulative_add = 0.0    # for panama
            cumulative_mul = 1.0    # for ratio

            # We adjust bars *before* the roll so that the series is
            # seamless at the roll point.  Process in reverse so we can
            # apply the cumulative adjustment as we go backwards.
            adjustments_add = [0.0] * len(continuous_data)
            adjustments_mul = [1.0] * len(continuous_data)

            for i in range(len(continuous_data) - 1, 0, -1):
                prev_contract = continuous_data[i - 1]['active_contract']
                curr_contract = continuous_data[i]['active_contract']
                if prev_contract != curr_contract:
                    # Roll detected between bar i-1 and bar i.
                    # Find the close of the outgoing contract on day i-1 and
                    # the close of the incoming contract on day i.
                    old_close = continuous_data[i - 1].get('close', 0)
                    new_close = continuous_data[i].get('close', 0)
                    if old_close > 0 and new_close > 0:
                        gap_add = old_close - new_close          # panama shift
                        gap_mul = old_close / new_close          # ratio scale
                        cumulative_add += gap_add
                        cumulative_mul *= gap_mul
                # All bars at index <= i-1 need cumulative adjustment
                adjustments_add[i - 1] = cumulative_add
                adjustments_mul[i - 1] = cumulative_mul

            price_fields = ('open', 'high', 'low', 'close')
            for i, row in enumerate(continuous_data):
                if method == 'panama':
                    adj = adjustments_add[i]
                    if adj != 0.0:
                        for f in price_fields:
                            if f in row:
                                row[f] = row[f] + adj
                elif method == 'ratio':
                    adj = adjustments_mul[i]
                    if adj != 1.0:
                        for f in price_fields:
                            if f in row:
                                row[f] = row[f] * adj

        return continuous_data

    def _find_bar_by_timestamps(self, bars, timestamp):
        for bar in bars:
            if bar['timestamp'] == timestamp:
                return bar
        return None

    def get_active_contract(self, timestamp):
        return self._active_contract_map.get(timestamp)
        
        
