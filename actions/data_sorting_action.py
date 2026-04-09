"""Data sorting action module for RabAI AutoClick.

Provides data sorting operations:
- QuickSortAction: Quick sort implementation
- MergeSortAction: Merge sort implementation
- HeapSortAction: Heap sort implementation
- TopNSortAction: Get top N elements
- MultiKeySortAction: Sort by multiple keys
"""

from typing import Any, Dict, List, Optional, Callable
import random

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QuickSortAction(BaseAction):
    """Quick sort implementation."""
    action_type = "quick_sort"
    display_name = "快速排序"
    description = "快速排序算法实现"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            reverse = params.get("reverse", False)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            sorted_data = self._quick_sort(list(data), key, reverse)
            
            return ActionResult(
                success=True,
                message="Quick sort complete",
                data={
                    "original_count": len(data),
                    "sorted_count": len(sorted_data),
                    "sorted_data": sorted_data[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _quick_sort(self, data: List, key: Optional[Callable], reverse: bool) -> List:
        if len(data) <= 1:
            return data
        
        pivot_idx = len(data) // 2
        pivot = data[pivot_idx]
        
        if key:
            pivot_val = key(pivot)
            less = [x for x in data if key(x) < pivot_val]
            greater = [x for x in data if key(x) > pivot_val]
            equal = [x for x in data if key(x) == pivot_val and x != pivot]
            less.extend(equal)
            less.extend(greater)
        else:
            less = [x for x in data if x < pivot]
            greater = [x for x in data if x > pivot]
            equal = [x for x in data if x == pivot]
        
        if reverse:
            return self._quick_sort(greater, key, reverse) + equal + self._quick_sort(less, key, reverse)
        else:
            return self._quick_sort(less, key, reverse) + equal + self._quick_sort(greater, key, reverse)


class MergeSortAction(BaseAction):
    """Merge sort implementation."""
    action_type = "merge_sort"
    display_name = "归并排序"
    description = "归并排序算法实现"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            reverse = params.get("reverse", False)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            sorted_data = self._merge_sort(list(data), key, reverse)
            
            return ActionResult(
                success=True,
                message="Merge sort complete",
                data={
                    "original_count": len(data),
                    "sorted_count": len(sorted_data),
                    "sorted_data": sorted_data[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _merge_sort(self, data: List, key: Optional[Callable], reverse: bool) -> List:
        if len(data) <= 1:
            return data
        
        mid = len(data) // 2
        left = self._merge_sort(data[:mid], key, reverse)
        right = self._merge_sort(data[mid:], key, reverse)
        
        return self._merge(left, right, key, reverse)
    
    def _merge(self, left: List, right: List, key: Optional[Callable], reverse: bool) -> List:
        result = []
        i = j = 0
        
        while i < len(left) and j < len(right):
            left_val = key(left[i]) if key else left[i]
            right_val = key(right[j]) if key else right[j]
            
            if (left_val <= right_val and not reverse) or (left_val >= right_val and reverse):
                result.append(left[i])
                i += 1
            else:
                result.append(right[j])
                j += 1
        
        result.extend(left[i:])
        result.extend(right[j:])
        
        return result


class HeapSortAction(BaseAction):
    """Heap sort implementation."""
    action_type = "heap_sort"
    display_name = "堆排序"
    description = "堆排序算法实现"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            reverse = params.get("reverse", False)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            sorted_data = self._heap_sort(list(data), key, reverse)
            
            return ActionResult(
                success=True,
                message="Heap sort complete",
                data={
                    "original_count": len(data),
                    "sorted_count": len(sorted_data),
                    "sorted_data": sorted_data[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _heap_sort(self, data: List, key: Optional[Callable], reverse: bool) -> List:
        n = len(data)
        
        def heapify(arr, n, i):
            largest = i
            left = 2 * i + 1
            right = 2 * i + 2
            
            val_i = key(arr[i]) if key else arr[i]
            val_l = key(arr[left]) if key and left < n else arr[left]
            val_r = key(arr[right]) if key and right < n else arr[right]
            
            if left < n and ((val_l > val_i and not reverse) or (val_l < val_i and reverse)):
                largest = left
            
            if right < n and ((val_r > key(arr[largest]) if key else arr[largest]) and not reverse) or \
                      ((val_r < key(arr[largest]) if key else arr[largest]) and reverse):
                largest = right
            
            if largest != i:
                arr[i], arr[largest] = arr[largest], arr[i]
                heapify(arr, n, largest)
        
        for i in range(n // 2 - 1, -1, -1):
            heapify(data, n, i)
        
        for i in range(n - 1, 0, -1):
            data[0], data[i] = data[i], data[0]
            heapify(data, i, 0)
        
        return data


class TopNSortAction(BaseAction):
    """Get top N elements."""
    action_type = "top_n_sort"
    display_name = "TopN排序"
    description = "获取前N个最大/最小元素"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            n = params.get("n", 10)
            key = params.get("key")
            reverse = params.get("reverse", True)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            n = min(n, len(data))
            
            if key:
                sorted_data = sorted(data, key=key, reverse=reverse)
            else:
                sorted_data = sorted(data, reverse=reverse)
            
            top_n = sorted_data[:n]
            
            return ActionResult(
                success=True,
                message=f"Top {n} elements retrieved",
                data={
                    "n": n,
                    "top_n": top_n,
                    "reverse": reverse
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class MultiKeySortAction(BaseAction):
    """Sort by multiple keys."""
    action_type = "multi_key_sort"
    display_name = "多键排序"
    description = "按多个键排序"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            keys = params.get("keys", [])
            orders = params.get("orders", [])
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not keys:
                return ActionResult(success=False, message="keys is required")
            
            if isinstance(data[0], dict) if data else False:
                sorted_data = self._multi_key_sort_dicts(data, keys, orders)
            else:
                sorted_data = self._multi_key_sort_tuples(data, keys, orders)
            
            return ActionResult(
                success=True,
                message="Multi-key sort complete",
                data={
                    "keys": keys,
                    "orders": orders,
                    "sorted_data": sorted_data[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _multi_key_sort_dicts(self, data: List[Dict], keys: List[str], orders: List[str]) -> List[Dict]:
        def sort_key(item):
            values = []
            for i, k in enumerate(keys):
                order = orders[i] if i < len(orders) else "asc"
                val = item.get(k)
                if order == "desc":
                    val = -val if isinstance(val, (int, float)) else val
                values.append(val)
            return tuple(values)
        
        return sorted(data, key=sort_key)
    
    def _multi_key_sort_tuples(self, data: List, keys: List[int], orders: List[str]) -> List:
        def sort_key(item):
            values = []
            for i, k in enumerate(keys):
                order = orders[i] if i < len(orders) else "asc"
                val = item[k] if k < len(item) else None
                if order == "desc":
                    val = -val if isinstance(val, (int, float)) else val
                values.append(val)
            return tuple(values)
        
        return sorted(data, key=sort_key)
