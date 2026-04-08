"""Compare action module for RabAI AutoClick.

Provides data comparison actions for equality checks,
difference analysis, and fuzzy matching.
"""

import sys
import os
import time
import hashlib
import difflib
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class DiffResult:
    """Result of a diff operation.
    
    Attributes:
        equal: Whether items are equal.
        differences: List of differences.
        similarity: Similarity score (0-1).
    """
    equal: bool
    differences: List[Dict[str, Any]]
    similarity: float


class DataComparer:
    """Compare data structures and values."""
    
    @staticmethod
    def compare_values(a: Any, b: Any, path: str = "") -> List[Dict[str, Any]]:
        """Compare two values recursively.
        
        Args:
            a: First value.
            b: Second value.
            path: Current path for nested comparison.
        
        Returns:
            List of differences.
        """
        differences = []
        
        if type(a) != type(b):
            differences.append({
                "path": path or "root",
                "type": "type_mismatch",
                "a": str(a)[:100],
                "b": str(b)[:100],
                "a_type": type(a).__name__,
                "b_type": type(b).__name__
            })
            return differences
        
        if isinstance(a, dict):
            all_keys = set(a.keys()) | set(b.keys())
            for key in all_keys:
                key_path = f"{path}.{key}" if path else key
                if key not in a:
                    differences.append({"path": key_path, "type": "missing_in_a", "value": b[key]})
                elif key not in b:
                    differences.append({"path": key_path, "type": "missing_in_b", "value": a[key]})
                else:
                    differences.extend(DataComparer.compare_values(a[key], b[key], key_path))
        
        elif isinstance(a, (list, tuple)):
            if len(a) != len(b):
                differences.append({
                    "path": path or "root",
                    "type": "length_mismatch",
                    "a_length": len(a),
                    "b_length": len(b)
                })
            
            max_len = min(len(a), len(b))
            for i in range(max_len):
                item_path = f"{path}[{i}]"
                differences.extend(DataComparer.compare_values(a[i], b[i], item_path))
        
        else:
            if a != b:
                differences.append({
                    "path": path or "root",
                    "type": "value_diff",
                    "a": a,
                    "b": b
                })
        
        return differences
    
    @staticmethod
    def compare_strings(a: str, b: str) -> DiffResult:
        """Compare two strings.
        
        Args:
            a: First string.
            b: Second string.
        
        Returns:
            DiffResult with differences.
        """
        if a == b:
            return DiffResult(equal=True, differences=[], similarity=1.0)
        
        matcher = difflib.SequenceMatcher(None, a, b)
        similarity = matcher.ratio()
        
        differences = []
        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag != 'equal':
                differences.append({
                    "type": tag,
                    "a_segment": a[i1:i2],
                    "b_segment": b[j1:j2],
                    "a_pos": i1,
                    "b_pos": j1
                })
        
        return DiffResult(equal=False, differences=differences, similarity=similarity)
    
    @staticmethod
    def compute_hash(value: Any) -> str:
        """Compute hash of value.
        
        Args:
            value: Value to hash.
        
        Returns:
            SHA256 hash string.
        """
        if isinstance(value, dict):
            value_str = json.dumps(value, sort_keys=True, ensure_ascii=False)
        elif isinstance(value, (list, tuple)):
            value_str = json.dumps(list(value), sort_keys=True, ensure_ascii=False)
        else:
            value_str = str(value)
        
        return hashlib.sha256(value_str.encode()).hexdigest()
    
    @staticmethod
    def fuzzy_match(a: str, b: str, threshold: float = 0.6) -> Dict[str, Any]:
        """Fuzzy match two strings.
        
        Args:
            a: First string.
            b: Second string.
            threshold: Match threshold (0-1).
        
        Returns:
            Match result with score and matched status.
        """
        ratio = difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()
        partial_ratio = difflib.SequenceMatcher(None, a.lower(), b.lower()).quick_ratio()
        
        matched = ratio >= threshold
        
        return {
            "matched": matched,
            "ratio": round(ratio, 3),
            "partial_ratio": round(partial_ratio, 3),
            "threshold": threshold,
            "a": a,
            "b": b
        }


class CompareValuesAction(BaseAction):
    """Compare two values recursively."""
    action_type = "compare_values"
    display_name = "比较值"
    description = "递归比较两个值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compare values.
        
        Args:
            context: Execution context.
            params: Dict with keys: a, b.
        
        Returns:
            ActionResult with comparison result.
        """
        a = params.get('a', None)
        b = params.get('b', None)
        
        try:
            differences = DataComparer.compare_values(a, b)
            equal = len(differences) == 0
            
            return ActionResult(
                success=True,
                message=f"Values are {'equal' if equal else f'different ({len(differences)} differences)'}",
                data={
                    "equal": equal,
                    "differences": differences,
                    "diff_count": len(differences)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Compare error: {str(e)}")


class CompareStringsAction(BaseAction):
    """Compare two strings."""
    action_type = "compare_strings"
    display_name = "比较字符串"
    description = "比较两个字符串"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compare strings.
        
        Args:
            context: Execution context.
            params: Dict with keys: a, b.
        
        Returns:
            ActionResult with comparison result.
        """
        a = params.get('a', '')
        b = params.get('b', '')
        
        try:
            result = DataComparer.compare_strings(a, b)
            
            return ActionResult(
                success=True,
                message=f"Strings are {'equal' if result.equal else f'different (similarity: {result.similarity:.2%})'}",
                data={
                    "equal": result.equal,
                    "similarity": result.similarity,
                    "differences": result.differences
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Compare error: {str(e)}")


class ComputeHashAction(BaseAction):
    """Compute hash of a value."""
    action_type = "compute_hash"
    display_name = "计算哈希"
    description = "计算值哈希"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compute hash.
        
        Args:
            context: Execution context.
            params: Dict with keys: value.
        
        Returns:
            ActionResult with hash.
        """
        value = params.get('value', None)
        
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        try:
            hash_value = DataComparer.compute_hash(value)
            
            return ActionResult(
                success=True,
                message="Hash computed",
                data={"hash": hash_value, "value_preview": str(value)[:50]}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Hash error: {str(e)}")


class FuzzyMatchAction(BaseAction):
    """Fuzzy match two strings."""
    action_type = "fuzzy_match"
    display_name = "模糊匹配"
    description = "模糊匹配两个字符串"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Fuzzy match.
        
        Args:
            context: Execution context.
            params: Dict with keys: a, b, threshold.
        
        Returns:
            ActionResult with match result.
        """
        a = params.get('a', '')
        b = params.get('b', '')
        threshold = params.get('threshold', 0.6)
        
        try:
            result = DataComparer.fuzzy_match(a, b, threshold)
            
            return ActionResult(
                success=True,
                message=f"Fuzzy match: {'matched' if result['matched'] else 'not matched'}",
                data=result
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Fuzzy match error: {str(e)}")


class FindMatchesAction(BaseAction):
    """Find fuzzy matches in a list."""
    action_type = "find_matches"
    display_name = "查找匹配"
    description = "在列表中查找模糊匹配"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Find matches.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, candidates, threshold.
        
        Returns:
            ActionResult with matches.
        """
        value = params.get('value', '')
        candidates = params.get('candidates', [])
        threshold = params.get('threshold', 0.6)
        
        if not candidates:
            return ActionResult(success=False, message="candidates list is required")
        
        try:
            matches = []
            for candidate in candidates:
                result = DataComparer.fuzzy_match(value, candidate, threshold)
                if result['matched']:
                    matches.append(result)
            
            matches.sort(key=lambda x: x['ratio'], reverse=True)
            
            return ActionResult(
                success=True,
                message=f"Found {len(matches)} matches",
                data={"matches": matches, "count": len(matches)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Find matches error: {str(e)}")


class CompareListsAction(BaseAction):
    """Compare two lists."""
    action_type = "compare_lists"
    display_name = "比较列表"
    description = "比较两个列表"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compare lists.
        
        Args:
            context: Execution context.
            params: Dict with keys: a, b.
        
        Returns:
            ActionResult with comparison result.
        """
        a = params.get('a', [])
        b = params.get('b', [])
        
        if not isinstance(a, list) or not isinstance(b, list):
            return ActionResult(success=False, message="a and b must be lists")
        
        try:
            set_a = set(a)
            set_b = set(b)
            
            common = list(set_a & set_b)
            only_in_a = list(set_a - set_b)
            only_in_b = list(set_b - set_a)
            
            return ActionResult(
                success=True,
                message=f"Common: {len(common)}, Only in A: {len(only_in_a)}, Only in B: {len(only_in_b)}",
                data={
                    "common": common,
                    "only_in_a": only_in_a,
                    "only_in_b": only_in_b,
                    "equal": set_a == set_b,
                    "a_count": len(a),
                    "b_count": len(b)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Compare lists error: {str(e)}")


class SimilarityScoreAction(BaseAction):
    """Calculate similarity score between strings."""
    action_type = "similarity_score"
    display_name = "相似度"
    description = "计算字符串相似度"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate similarity.
        
        Args:
            context: Execution context.
            params: Dict with keys: a, b.
        
        Returns:
            ActionResult with similarity score.
        """
        a = params.get('a', '')
        b = params.get('b', '')
        
        try:
            result = DataComparer.compare_strings(a, b)
            
            return ActionResult(
                success=True,
                message=f"Similarity: {result.similarity:.2%}",
                data={
                    "similarity": result.similarity,
                    "equal": result.equal
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Similarity error: {str(e)}")
