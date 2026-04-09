"""
UI state comparison utilities.

Compare UI states to verify automation results.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any


@dataclass
class StateDifference:
    """Represents a difference between two UI states."""
    path: str
    expected: Any
    actual: Any
    severity: str = "medium"


@dataclass
class StateComparisonResult:
    """Result of comparing two UI states."""
    is_match: bool
    differences: list[StateDifference]
    match_percentage: float


class UIStateComparator:
    """Compare UI states for verification."""
    
    def __init__(self, tolerance: float = 0.0):
        self.tolerance = tolerance
    
    def compare(
        self,
        expected: dict,
        actual: dict,
        path: str = ""
    ) -> StateComparisonResult:
        """Compare expected and actual UI states."""
        differences = []
        
        self._compare_values(expected, actual, path, differences)
        
        match_percentage = self._calculate_match_percentage(expected, differences)
        
        return StateComparisonResult(
            is_match=len(differences) == 0,
            differences=differences,
            match_percentage=match_percentage
        )
    
    def _compare_values(
        self,
        expected: Any,
        actual: Any,
        path: str,
        differences: list[StateDifference]
    ) -> None:
        """Compare values recursively."""
        if isinstance(expected, dict) and isinstance(actual, dict):
            all_keys = set(expected.keys()) | set(actual.keys())
            for key in all_keys:
                new_path = f"{path}.{key}" if path else key
                if key not in expected:
                    differences.append(StateDifference(
                        path=new_path,
                        expected=None,
                        actual=actual[key],
                        severity="high"
                    ))
                elif key not in actual:
                    differences.append(StateDifference(
                        path=new_path,
                        expected=expected[key],
                        actual=None,
                        severity="high"
                    ))
                else:
                    self._compare_values(expected[key], actual[key], new_path, differences)
        elif isinstance(expected, list) and isinstance(actual, list):
            for i, (exp, act) in enumerate(zip(expected, actual)):
                self._compare_values(exp, act, f"{path}[{i}]", differences)
            if len(expected) != len(actual):
                differences.append(StateDifference(
                    path=f"{path}.length",
                    expected=len(expected),
                    actual=len(actual),
                    severity="medium"
                ))
        else:
            if self._values_differ(expected, actual):
                differences.append(StateDifference(
                    path=path,
                    expected=expected,
                    actual=actual,
                    severity="medium"
                ))
    
    def _values_differ(self, expected: Any, actual: Any) -> bool:
        """Check if two values differ."""
        if isinstance(expected, float) and isinstance(actual, float):
            return abs(expected - actual) > self.tolerance
        return expected != actual
    
    def _calculate_match_percentage(
        self,
        expected: dict,
        differences: list[StateDifference]
    ) -> float:
        """Calculate percentage of matching values."""
        total_fields = self._count_fields(expected)
        if total_fields == 0:
            return 100.0
        return max(0.0, 100.0 - (len(differences) / total_fields) * 100)
    
    def _count_fields(self, obj: Any) -> int:
        """Count total fields in an object."""
        if isinstance(obj, dict):
            return sum(self._count_fields(v) for v in obj.values()) + len(obj)
        elif isinstance(obj, list):
            return sum(self._count_fields(item) for item in obj) + len(obj)
        return 1


class StateMatcher:
    """Match UI elements against expected state."""
    
    def __init__(self):
        self.comparator = UIStateComparator()
    
    def match_element(
        self,
        element: dict,
        expected: dict
    ) -> StateComparisonResult:
        """Match an element against expected state."""
        return self.comparator.compare(expected, element)
    
    def match_any(
        self,
        elements: list[dict],
        expected: dict
    ) -> tuple[bool, Optional[dict]]:
        """Check if any element matches expected state."""
        for element in elements:
            result = self.match_element(element, expected)
            if result.is_match:
                return True, element
        return False, None
