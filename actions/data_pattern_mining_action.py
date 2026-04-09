"""Data Pattern Mining Action Module.

Discovers frequent patterns, associations, and correlations
in datasets using configurable mining algorithms.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import Counter, defaultdict
import math
import logging

logger = logging.getLogger(__name__)


@dataclass
class Pattern:
    """A discovered pattern in the data."""
    items: Set[Any]
    support: float
    confidence: Optional[float] = None
    lift: Optional[float] = None


class DataPatternMiningAction:
    """Discovers frequent patterns and associations in data.
    
    Implements support-based pattern mining to find itemsets
    that co-occur above a configurable minimum support threshold.
    """

    def __init__(self, min_support: float = 0.01) -> None:
        self.min_support = min_support
        self._patterns: List[Pattern] = []
        self._total_transactions: int = 0

    def mine_frequent_itemsets(
        self,
        transactions: List[List[Any]],
        max_size: int = 3,
    ) -> List[Pattern]:
        """Mine frequent itemsets from a list of transactions.
        
        Args:
            transactions: List of transactions, each a list of items.
            max_size: Maximum itemset size to consider.
        
        Returns:
            List of Pattern objects sorted by support descending.
        """
        self._total_transactions = len(transactions)
        if self._total_transactions == 0:
            return []

        # Count single items
        item_counts: Counter[Any] = Counter()
        for txn in transactions:
            for item in set(txn):
                item_counts[item] += 1

        # Filter by min_support
        min_count = self.min_support * self._total_transactions
        frequent_items = {
            item for item, count in item_counts.items()
            if count >= min_count
        }

        current_frequent: List[Set[Any]] = [{item} for item in frequent_items]
        all_patterns: List[Pattern] = []

        for size in range(1, max_size + 1):
            if not current_frequent:
                break
            all_patterns.extend([
                Pattern(items=itemset, support=len(itemset) / self._total_transactions)
                for itemset in current_frequent
            ])

            if size == max_size:
                break

            # Generate candidates of size+1
            next_frequent: List[Set[Any]] = []
            for i, s1 in enumerate(current_frequent):
                for s2 in current_frequent[i + 1:]:
                    candidate = s1 | s2
                    if len(candidate) != size + 1:
                        continue
                    count = sum(1 for txn in transactions if candidate.issubset(set(txn)))
                    if count >= min_count:
                        if candidate not in [set(x.items) for x in next_frequent]:
                            next_frequent.append(candidate)

            current_frequent = next_frequent

        all_patterns.sort(key=lambda p: -p.support)
        self._patterns = all_patterns
        return all_patterns

    def mine_associations(
        self,
        transactions: List[List[Any]],
        min_confidence: float = 0.5,
    ) -> List[Pattern]:
        """Mine association rules (if A then B).
        
        Args:
            transactions: List of transactions.
            min_confidence: Minimum confidence threshold.
        
        Returns:
            List of patterns with confidence and lift computed.
        """
        frequent = self.mine_frequent_itemsets(transactions, max_size=2)
        association_rules: List[Pattern] = []

        item_supports: Dict[frozenset, float] = {
            frozenset(p.items): p.support for p in frequent
        }
        total = self._total_transactions

        for pattern in frequent:
            if len(pattern.items) < 2:
                continue
            items_list = list(pattern.items)
            for i, consequent in enumerate(items_list):
                antecedent = set(items_list[:i] + items_list[i + 1:])
                antecedent_key = frozenset(antecedent)
                antecedent_support = item_supports.get(antecedent_key, 0)
                if antecedent_support == 0:
                    continue
                confidence = pattern.support / antecedent_support
                if confidence < min_confidence:
                    continue
                # Compute lift
                consequent_key = frozenset([consequent])
                consequent_support = item_supports.get(consequent_key, 0)
                lift = confidence / consequent_support if consequent_support > 0 else 0.0

                association_rules.append(Pattern(
                    items={f"{tuple(antecedent)} -> {consequent}"},
                    support=pattern.support,
                    confidence=round(confidence, 4),
                    lift=round(lift, 4),
                ))

        return association_rules

    def get_top_patterns(self, n: int = 10) -> List[Pattern]:
        """Get the top N patterns by support."""
        return sorted(self._patterns, key=lambda p: -p.support)[:n]

    def get_stats(self) -> Dict[str, Any]:
        """Get pattern mining statistics."""
        return {
            "total_transactions": self._total_transactions,
            "patterns_found": len(self._patterns),
            "min_support": self.min_support,
        }
