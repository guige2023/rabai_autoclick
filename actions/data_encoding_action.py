"""Data encoding action module for RabAI AutoClick.

Provides data encoding operations:
- OneHotEncoderAction: One-hot encoding
- LabelEncoderAction: Label encoding
- OrdinalEncoderAction: Ordinal encoding
- TargetEncoderAction: Target encoding
- CountEncoderAction: Count/frequency encoding
"""

from typing import Any, Dict, List, Optional, Set
from collections import defaultdict, Counter

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class OneHotEncoderAction(BaseAction):
    """One-hot encoding for categorical data."""
    action_type = "onehot_encoder"
    display_name = "独热编码"
    description = "对分类数据进行独热编码"
    
    def __init__(self):
        super().__init__()
        self._categories: Dict[str, Set] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field")
            drop_first = params.get("drop_first", False)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field:
                return ActionResult(success=False, message="field is required")
            
            encoded, categories = self._encode(data, field, drop_first)
            
            return ActionResult(
                success=True,
                message="One-hot encoding complete",
                data={
                    "original_count": len(data),
                    "categories": categories,
                    "drop_first": drop_first,
                    "encoded_data": encoded[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _encode(self, data: List[Dict], field: str, drop_first: bool) -> Tuple[List[Dict], List[str]]:
        categories = set()
        for item in data:
            if isinstance(item, dict) and field in item:
                categories.add(str(item[field]))
        
        categories = sorted(list(categories))
        
        if drop_first and categories:
            categories = categories[1:]
        
        for item in data:
            if isinstance(item, dict):
                item_value = str(item.get(field, ""))
                for cat in categories:
                    item[f"{field}_{cat}"] = 1 if item_value == cat else 0
        
        return data, categories


class LabelEncoderAction(BaseAction):
    """Label encoding for categorical data."""
    action_type = "label_encoder"
    display_name = "标签编码"
    description = "对分类数据进行标签编码"
    
    def __init__(self):
        super().__init__()
        self._mappings: Dict[str, Dict[str, int]] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field")
            mapping = params.get("mapping")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field:
                return ActionResult(success=False, message="field is required")
            
            encoded, label_mapping = self._encode(data, field, mapping)
            
            return ActionResult(
                success=True,
                message="Label encoding complete",
                data={
                    "original_count": len(data),
                    "label_mapping": label_mapping,
                    "encoded_data": [{"index": i, field: v} for i, v in enumerate(encoded)]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _encode(self, data: List[Dict], field: str, mapping: Optional[Dict[str, int]]) -> Tuple[List[int], Dict[str, int]]:
        categories = set()
        for item in data:
            if isinstance(item, dict) and field in item:
                categories.add(str(item[field]))
        
        if mapping is None:
            sorted_cats = sorted(list(categories))
            mapping = {cat: i for i, cat in enumerate(sorted_cats)}
        
        self._mappings[field] = mapping
        
        encoded = []
        for item in data:
            if isinstance(item, dict) and field in item:
                value = str(item[field])
                encoded_value = mapping.get(value, -1)
                item[field] = encoded_value
                encoded.append(encoded_value)
            else:
                encoded.append(-1)
        
        return encoded, mapping


class OrdinalEncoderAction(BaseAction):
    """Ordinal encoding with custom ordering."""
    action_type = "ordinal_encoder"
    display_name = "有序编码"
    description = "对分类数据进行有序编码"
    
    def __init__(self):
        super().__init__()
        self._orderings: Dict[str, List[str]] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field")
            order = params.get("order", [])
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field:
                return ActionResult(success=False, message="field is required")
            
            if not order:
                return ActionResult(success=False, message="order is required for ordinal encoding")
            
            encoded = self._encode(data, field, order)
            
            return ActionResult(
                success=True,
                message="Ordinal encoding complete",
                data={
                    "original_count": len(data),
                    "order": order,
                    "encoded_data": [{"index": i, field: v} for i, v in enumerate(encoded)]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _encode(self, data: List[Dict], field: str, order: List[str]) -> List[int]:
        self._orderings[field] = order
        order_map = {val: i for i, val in enumerate(order)}
        
        encoded = []
        for item in data:
            if isinstance(item, dict) and field in item:
                value = str(item[field])
                encoded_value = order_map.get(value, -1)
                item[field] = encoded_value
                encoded.append(encoded_value)
            else:
                encoded.append(-1)
        
        return encoded


class TargetEncoderAction(BaseAction):
    """Target encoding for categorical data."""
    action_type = "target_encoder"
    display_name = "目标编码"
    description = "对分类数据进行目标编码"
    
    def __init__(self):
        super().__init__()
        self._encodings: Dict[str, Dict[str, float]] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field")
            target_field = params.get("target_field")
            smoothing = params.get("smoothing", 1.0)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field or not target_field:
                return ActionResult(success=False, message="field and target_field are required")
            
            encoded, encodings = self._encode(data, field, target_field, smoothing)
            
            return ActionResult(
                success=True,
                message="Target encoding complete",
                data={
                    "original_count": len(data),
                    "field": field,
                    "target_field": target_field,
                    "smoothing": smoothing,
                    "encodings": encodings,
                    "encoded_data": [{"index": i, field: v} for i, v in enumerate(encoded)]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _encode(self, data: List[Dict], field: str, target_field: str, 
                smoothing: float) -> Tuple[List[float], Dict[str, float]]:
        category_values: Dict[str, List[float]] = defaultdict(list)
        
        for item in data:
            if isinstance(item, dict) and field in item and target_field in item:
                cat = str(item[field])
                target = item[target_field]
                if isinstance(target, (int, float)):
                    category_values[cat].append(target)
        
        global_mean = 0
        all_values = []
        for values in category_values.values():
            all_values.extend(values)
        
        if all_values:
            global_mean = sum(all_values) / len(all_values)
        
        encodings = {}
        total_count = len(all_values)
        
        for cat, values in category_values.items():
            count = len(values)
            mean = sum(values) / count if count > 0 else global_mean
            
            smooth_weight = count / (count + smoothing)
            encodings[cat] = smooth_weight * mean + (1 - smooth_weight) * global_mean
        
        self._encodings[field] = encodings
        
        encoded = []
        for item in data:
            if isinstance(item, dict) and field in item:
                cat = str(item[field])
                encoded_value = encodings.get(cat, global_mean)
                item[field] = encoded_value
                encoded.append(encoded_value)
            else:
                encoded.append(global_mean)
        
        return encoded, encodings


class CountEncoderAction(BaseAction):
    """Count/frequency encoding for categorical data."""
    action_type = "count_encoder"
    display_name = "计数编码"
    description = "对分类数据进行计数/频率编码"
    
    def __init__(self):
        super().__init__()
        self._counts: Dict[str, Dict[str, int]] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field")
            normalize = params.get("normalize", False)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field:
                return ActionResult(success=False, message="field is required")
            
            encoded, counts = self._encode(data, field, normalize)
            
            return ActionResult(
                success=True,
                message="Count encoding complete",
                data={
                    "original_count": len(data),
                    "field": field,
                    "normalize": normalize,
                    "counts": counts,
                    "encoded_data": [{"index": i, field: v} for i, v in enumerate(encoded)]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _encode(self, data: List[Dict], field: str, normalize: bool) -> Tuple[List[float], Dict[str, int]]:
        counter: Counter = Counter()
        
        for item in data:
            if isinstance(item, dict) and field in item:
                counter[str(item[field])] += 1
        
        counts = dict(counter)
        total = sum(counts.values())
        
        self._counts[field] = counts
        
        encoded = []
        for item in data:
            if isinstance(item, dict) and field in item:
                cat = str(item[field])
                count = counts.get(cat, 0)
                if normalize and total > 0:
                    item[field] = count / total
                else:
                    item[field] = count
                encoded.append(item[field])
            else:
                encoded.append(0 if not normalize else 0.0)
        
        return encoded, counts


class HashEncoderAction(BaseAction):
    """Hash encoding for categorical data."""
    action_type = "hash_encoder"
    display_name = "哈希编码"
    description = "对分类数据进行哈希编码"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field")
            n_components = params.get("n_components", 8)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not field:
                return ActionResult(success=False, message="field is required")
            
            encoded = self._encode(data, field, n_components)
            
            return ActionResult(
                success=True,
                message="Hash encoding complete",
                data={
                    "original_count": len(data),
                    "field": field,
                    "n_components": n_components,
                    "encoded_data": [{"index": i, f"{field}_hash_{j}": v for j, v in enumerate(h)}] 
                                   for i, h in enumerate(encoded)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _encode(self, data: List[Dict], field: str, n_components: int) -> List[List[int]]:
        import hashlib
        
        encoded = []
        
        for item in data:
            if isinstance(item, dict) and field in item:
                value = str(item[field])
                hash_val = int(hashlib.md5(value.encode()).hexdigest(), 16)
                
                hash_features = []
                for i in range(n_components):
                    bit_pos = (hash_val >> i) & 1
                    hash_features.append(bit_pos)
                
                encoded.append(hash_features)
            else:
                encoded.append([0] * n_components)
        
        return encoded


from typing import Tuple
