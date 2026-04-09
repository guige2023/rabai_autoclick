"""
Entropy analysis module for measuring uncertainty and information content.

Provides entropy calculation, mutual information, and information gain
computations for data analysis and feature selection workflows.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from typing import Optional


@dataclass
class EntropyResult:
    """Result of entropy computation."""
    entropy: float
    normalized_entropy: float
    max_entropy: float
    redundancy: float


class EntropyAnalyzer:
    """
    Computes entropy and related information-theoretic metrics.
    
    Example:
        analyzer = EntropyAnalyzer()
        data = ['a', 'b', 'a', 'c', 'b', 'a']
        result = analyzer.compute_entropy(data)
    """

    def __init__(self, base: float = 2.0) -> None:
        """
        Initialize entropy analyzer.
        
        Args:
            base: Logarithm base for entropy calculation.
                  2.0 = bits, e = nats, 10 = decimal digits.
        """
        self.base = base
        self._log_base = math.log(base)

    def _log(self, x: float) -> float:
        """Compute logarithm in configured base."""
        if x <= 0:
            return 0.0
        return math.log(x) / self._log_base

    def compute_entropy(self, data: list[str | int | float]) -> EntropyResult:
        """
        Compute Shannon entropy of a categorical dataset.
        
        Args:
            data: List of categorical values.
            
        Returns:
            EntropyResult with entropy and related metrics.
        """
        if not data:
            return EntropyResult(entropy=0.0, normalized_entropy=0.0, max_entropy=0.0, redundancy=0.0)

        counter = Counter(data)
        n = len(data)
        
        # Shannon entropy: H = -sum(p * log(p))
        entropy = 0.0
        for count in counter.values():
            p = count / n
            if p > 0:
                entropy -= p * self._log(p)

        # Maximum entropy for uniform distribution
        num_categories = len(counter)
        max_entropy = self._log(num_categories)

        # Normalized entropy (0 to 1)
        normalized = entropy / max_entropy if max_entropy > 0 else 0.0

        # Redundancy (how far from maximum entropy)
        redundancy = 1.0 - normalized

        return EntropyResult(
            entropy=round(entropy, 6),
            normalized_entropy=round(normalized, 6),
            max_entropy=round(max_entropy, 6),
            redundancy=round(redundancy, 6)
        )

    def compute_conditional_entropy(
        self,
        data: list[str | int | float],
        conditions: list[str | int | float]
    ) -> float:
        """
        Compute conditional entropy H(Y|X).
        
        Args:
            data: Target variable values.
            conditions: Condition variable values (same length as data).
            
        Returns:
            Conditional entropy H(Y|X).
        """
        if len(data) != len(conditions):
            raise ValueError("data and conditions must have same length")

        if not data:
            return 0.0

        # Group data by condition values
        groups: dict[str | int | float, list[str | int | float]] = {}
        for d, c in zip(data, conditions):
            if c not in groups:
                groups[c] = []
            groups[c].append(d)

        n = len(data)
        conditional_h = 0.0

        for condition_val, group_data in groups.items():
            p_condition = len(group_data) / n
            group_entropy = self.compute_entropy(group_data).entropy
            conditional_h += p_condition * group_entropy

        return round(conditional_h, 6)

    def compute_mutual_information(
        self,
        x: list[str | int | float],
        y: list[str | int | float]
    ) -> float:
        """
        Compute mutual information I(X;Y).
        
        Args:
            x: First variable values.
            y: Second variable values (same length).
            
        Returns:
            Mutual information in bits/nats (based on configured base).
        """
        if len(x) != len(y):
            raise ValueError("x and y must have same length")

        # I(X;Y) = H(X) + H(Y) - H(X,Y)
        h_x = self.compute_entropy(x).entropy
        h_y = self.compute_entropy(y).entropy
        h_xy = self.compute_joint_entropy(list(zip(x, y)))

        mi = h_x + h_y - h_xy
        return round(max(0.0, mi), 6)

    def compute_joint_entropy(self, pairs: list[tuple]) -> float:
        """
        Compute joint entropy H(X,Y).
        
        Args:
            pairs: List of (x, y) tuples.
            
        Returns:
            Joint entropy H(X,Y).
        """
        counter = Counter(pairs)
        n = len(pairs)
        entropy = 0.0

        for count in counter.values():
            p = count / n
            if p > 0:
                entropy -= p * self._log(p)

        return round(entropy, 6)

    def compute_information_gain(
        self,
        data: list[str | int | float],
        split_values: list[str | int | float]
    ) -> float:
        """
        Compute information gain from splitting data.
        
        Args:
            data: Target variable values.
            split_values: Values used for splitting (same length as data).
            
        Returns:
            Information gain from the split.
        """
        h_before = self.compute_entropy(data).entropy
        h_after = self.compute_conditional_entropy(data, split_values)
        return round(h_before - h_after, 6)

    def compute_gain_ratio(
        self,
        data: list[str | int | float],
        split_values: list[str | int | float]
    ) -> float:
        """
        Compute gain ratio (information gain / split info).
        
        Args:
            data: Target variable values.
            split_values: Values used for splitting.
            
        Returns:
            Gain ratio.
        """
        ig = self.compute_information_gain(data, split_values)
        split_info = self.compute_entropy(split_values).entropy
        
        if split_info == 0:
            return 0.0
        
        return round(ig / split_info, 6)

    def rank_features_by_entropy(
        self,
        features: dict[str, list[str | int | float]],
        target: list[str | int | float]
    ) -> list[tuple[str, float]]:
        """
        Rank features by their information gain with respect to target.
        
        Args:
            features: Dictionary mapping feature names to value lists.
            target: Target variable values.
            
        Returns:
            List of (feature_name, information_gain) sorted descending.
        """
        rankings = []
        for name, values in features.items():
            try:
                ig = self.compute_information_gain(target, values)
                rankings.append((name, ig))
            except (ValueError, ZeroDivisionError):
                continue

        rankings.sort(key=lambda x: x[1], reverse=True)
        return rankings
