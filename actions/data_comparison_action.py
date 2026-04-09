"""Data comparison action module for RabAI AutoClick.

Provides data comparison operations:
- EqualityComparerAction: Compare data for equality
- DifferenceFinderAction: Find differences between datasets
- SimilarityCalculatorAction: Calculate similarity scores
- GroupComparatorAction: Compare grouped data
- VersionComparatorAction: Compare data versions
"""

from typing import Any, Dict, List, Optional, Set
from datetime import datetime
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EqualityComparerAction(BaseAction):
    """Compare data for equality."""
    action_type = "equality_comparer"
    display_name = "相等比较"
    description = "比较数据是否相等"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data1 = params.get("data1")
            data2 = params.get("data2")
            comparison_mode = params.get("mode", "deep")
            
            if comparison_mode == "deep":
                equal = self._deep_equal(data1, data2)
            elif comparison_mode == "shallow":
                equal = data1 == data2
            elif comparison_mode == "by_fields":
                fields = params.get("fields", [])
                equal = self._compare_by_fields(data1, data2, fields)
            else:
                return ActionResult(success=False, message=f"Unknown mode: {comparison_mode}")
            
            return ActionResult(
                success=True,
                message="Equality comparison complete",
                data={
                    "equal": equal,
                    "mode": comparison_mode
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _deep_equal(self, obj1: Any, obj2: Any) -> bool:
        if type(obj1) != type(obj2):
            return False
        
        if isinstance(obj1, dict):
            if set(obj1.keys()) != set(obj2.keys()):
                return False
            return all(self._deep_equal(obj1[k], obj2[k]) for k in obj1)
        elif isinstance(obj1, (list, tuple)):
            if len(obj1) != len(obj2):
                return False
            return all(self._deep_equal(o1, o2) for o1, o2 in zip(obj1, obj2))
        else:
            return obj1 == obj2
    
    def _compare_by_fields(self, data1: Any, data2: Any, fields: List[str]) -> bool:
        if not isinstance(data1, dict) or not isinstance(data2, dict):
            return data1 == data2
        
        return all(data1.get(f) == data2.get(f) for f in fields)


class DifferenceFinderAction(BaseAction):
    """Find differences between datasets."""
    action_type = "difference_finder"
    display_name = "差异查找"
    description = "查找数据集之间的差异"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data1 = params.get("data1", [])
            data2 = params.get("data2", [])
            key_field = params.get("key_field")
            compare_mode = params.get("mode", "symmetric")
            
            if isinstance(data1, list) and isinstance(data2, list):
                result = self._find_list_differences(data1, data2, key_field, compare_mode)
            elif isinstance(data1, dict) and isinstance(data2, dict):
                result = self._find_dict_differences(data1, data2)
            else:
                return ActionResult(success=False, message="data1 and data2 must be same type")
            
            return ActionResult(
                success=True,
                message="Difference analysis complete",
                data=result
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _find_list_differences(self, list1: List, list2: List, key_field: Optional[str], 
                                mode: str) -> Dict:
        if key_field:
            dict1 = {item.get(key_field) if isinstance(item, dict) else item: item 
                    for item in list1}
            dict2 = {item.get(key_field) if isinstance(item, dict) else item: item 
                    for item in list2}
            
            keys1 = set(dict1.keys())
            keys2 = set(dict2.keys())
        else:
            keys1 = set(range(len(list1)))
            keys2 = set(range(len(list2)))
            dict1 = {i: list1[i] for i in keys1}
            dict2 = {i: list2[i] for i in keys2}
        
        if mode == "symmetric":
            only_in_1 = keys1 - keys2
            only_in_2 = keys2 - keys1
            common = keys1 & keys2
            
            different = []
            for key in common:
                if dict1[key] != dict2[key]:
                    different.append({
                        "key": key,
                        "value1": dict1[key],
                        "value2": dict2[key]
                    })
            
            return {
                "only_in_first": [dict1[k] for k in only_in_1],
                "only_in_second": [dict2[k] for k in only_in_2],
                "different": different,
                "same_count": len(common) - len(different)
            }
        else:
            only_in_1 = keys1 - keys2
            
            return {
                "only_in_first": [dict1[k] for k in only_in_1],
                "in_both": [dict1[k] for k in keys1 & keys2]
            }
    
    def _find_dict_differences(self, dict1: Dict, dict2: Dict) -> Dict:
        keys1 = set(dict1.keys())
        keys2 = set(dict2.keys())
        
        only_in_1 = keys1 - keys2
        only_in_2 = keys2 - keys1
        common = keys1 & keys2
        
        different = []
        for key in common:
            if dict1[key] != dict2[key]:
                different.append({
                    "key": key,
                    "value1": dict1[key],
                    "value2": dict2[key]
                })
        
        return {
            "only_in_first": {k: dict1[k] for k in only_in_1},
            "only_in_second": {k: dict2[k] for k in only_in_2},
            "different": different,
            "same_count": len(common) - len(different)
        }


class SimilarityCalculatorAction(BaseAction):
    """Calculate similarity scores."""
    action_type = "similarity_calculator"
    display_name = "相似度计算"
    description = "计算数据相似度"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data1 = params.get("data1")
            data2 = params.get("data2")
            method = params.get("method", "jaccard")
            
            if method == "jaccard":
                similarity = self._jaccard_similarity(data1, data2)
            elif method == "cosine":
                similarity = self._cosine_similarity(data1, data2)
            elif method == "levenshtein":
                similarity = self._levenshtein_similarity(data1, data2)
            elif method == "overlap":
                similarity = self._overlap_coefficient(data1, data2)
            else:
                return ActionResult(success=False, message=f"Unknown method: {method}")
            
            return ActionResult(
                success=True,
                message=f"Similarity calculated using {method}",
                data={
                    "similarity": similarity,
                    "method": method
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _jaccard_similarity(self, set1: Set, set2: Set) -> float:
        if not isinstance(set1, set):
            set1 = set(set1) if hasattr(set1, "__iter__") else {set1}
        if not isinstance(set2, set):
            set2 = set(set2) if hasattr(set2, "__iter__") else {set2}
        
        if not set1 and not set2:
            return 1.0
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return intersection / union if union > 0 else 0.0
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        if len(vec1) != len(vec2):
            return 0.0
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = sum(a * a for a in vec1) ** 0.5
        magnitude2 = sum(b * b for b in vec2) ** 0.5
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)
    
    def _levenshtein_similarity(self, str1: str, str2: str) -> float:
        distance = self._levenshtein_distance(str1, str2)
        max_len = max(len(str1), len(str2))
        
        if max_len == 0:
            return 1.0
        
        return 1.0 - (distance / max_len)
    
    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def _overlap_coefficient(self, set1: Set, set2: Set) -> float:
        if not isinstance(set1, set):
            set1 = set(set1) if hasattr(set1, "__iter__") else {set1}
        if not isinstance(set2, set):
            set2 = set(set2) if hasattr(set2, "__iter__") else {set2}
        
        intersection = len(set1 & set2)
        min_size = min(len(set1), len(set2))
        
        if min_size == 0:
            return 0.0
        
        return intersection / min_size


class GroupComparatorAction(BaseAction):
    """Compare grouped data."""
    action_type = "group_comparator"
    display_name = "分组比较"
    description = "比较分组数据"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data1 = params.get("data1", [])
            data2 = params.get("data2", [])
            group_by = params.get("group_by")
            
            if not group_by:
                return ActionResult(success=False, message="group_by is required")
            
            groups1 = self._group_data(data1, group_by)
            groups2 = self._group_data(data2, group_by)
            
            all_keys = set(groups1.keys()) | set(groups2.keys())
            
            comparison = {}
            for key in all_keys:
                in_first = key in groups1
                in_second = key in groups2
                
                comparison[key] = {
                    "in_first": in_first,
                    "in_second": in_second,
                    "count_first": len(groups1.get(key, [])),
                    "count_second": len(groups2.get(key, [])),
                    "present_in_both": in_first and in_second
                }
            
            groups_in_both = sum(1 for v in comparison.values() if v["present_in_both"])
            
            return ActionResult(
                success=True,
                message="Group comparison complete",
                data={
                    "group_by": group_by,
                    "total_groups": len(all_keys),
                    "groups_in_both": groups_in_both,
                    "only_in_first": len(all_keys) - groups_in_both - sum(1 for v in comparison.values() if not v["in_first"]),
                    "only_in_second": len(all_keys) - groups_in_both - sum(1 for v in comparison.values() if not v["in_second"]),
                    "comparison": comparison
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _group_data(self, data: List[Dict], group_by: str) -> Dict[str, List]:
        groups: Dict[str, List] = defaultdict(list)
        
        for item in data:
            if isinstance(item, dict):
                key = item.get(group_by, "unknown")
                groups[key].append(item)
        
        return groups


class VersionComparatorAction(BaseAction):
    """Compare data versions."""
    action_type = "version_comparator"
    display_name = "版本比较"
    description = "比较数据版本"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            version1 = params.get("version1")
            version2 = params.get("version2")
            
            if not version1 or not version2:
                return ActionResult(success=False, message="version1 and version2 are required")
            
            comparison = self._compare_versions(version1, version2)
            
            return ActionResult(
                success=True,
                message="Version comparison complete",
                data={
                    "version1": version1,
                    "version2": version2,
                    **comparison
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _compare_versions(self, v1: str, v2: str) -> Dict:
        parts1 = [int(x) for x in v1.split(".")]
        parts2 = [int(x) for x in v2.split(".")]
        
        max_len = max(len(parts1), len(parts2))
        parts1.extend([0] * (max_len - len(parts1)))
        parts2.extend([0] * (max_len - len(parts2)))
        
        if parts1 > parts2:
            result = 1
            message = f"{v1} is newer than {v2}"
        elif parts1 < parts2:
            result = -1
            message = f"{v1} is older than {v2}"
        else:
            result = 0
            message = f"{v1} equals {v2}"
        
        return {
            "comparison_result": result,
            "message": message,
            "is_newer": parts1 > parts2,
            "is_older": parts1 < parts2,
            "is_equal": parts1 == parts2
        }
