"""
Data transformation utilities - mapping, flattening, pivoting, reshaping, encoding.
"""
from typing import Any, Dict, List, Optional, Union, Callable
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _deep_get(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    keys = path.split(".")
    result = d
    for k in keys:
        if isinstance(result, dict):
            result = result.get(k)
        elif isinstance(result, list) and k.isdigit():
            idx = int(k)
            result = result[idx] if 0 <= idx < len(result) else None
        else:
            return default
        if result is None:
            return default
    return result


def _deep_set(d: Dict[str, Any], path: str, value: Any) -> None:
    keys = path.split(".")
    current = d
    for k in keys[:-1]:
        if k not in current:
            current[k] = {}
        current = current[k]
    current[keys[-1]] = value


def _flatten_dict(d: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    items: List[tuple] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten_dict(v, new_key, sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(_flatten_dict(item, f"{new_key}[{i}]", sep).items())
                else:
                    items.append((f"{new_key}[{i}]", item))
        else:
            items.append((new_key, v))
    return dict(items)


def _unflatten_dict(d: Dict[str, Any], sep: str = ".") -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in d.items():
        _deep_set(result, key.replace("[", ".").replace("]", ""), value)
    return result


def _group_by(data: List[Dict[str, Any]], key: str) -> Dict[Any, List[Dict[str, Any]]]:
    groups: Dict[Any, List[Dict[str, Any]]] = {}
    for item in data:
        val = item.get(key)
        if val not in groups:
            groups[val] = []
        groups[val].append(item)
    return groups


def _pivot_data(
    data: List[Dict[str, Any]],
    index: str,
    columns: str,
    values: str,
    aggfunc: str = "first"
) -> Dict[str, Any]:
    groups = _group_by(data, columns)
    result: Dict[str, Any] = {}
    for col_val, rows in groups.items():
        row_map = {r.get(index): r.get(values) for r in rows}
        if aggfunc == "first" or aggfunc == "last":
            result[col_val] = row_map
        elif aggfunc == "count":
            result[col_val] = {k: len(v) for k, v in groups.items()}
        elif aggfunc == "sum" and all(isinstance(r.get(values), (int, float)) for r in rows):
            result[col_val] = sum(r.get(values, 0) for r in rows)
        else:
            result[col_val] = row_map
    return result


def _one_hot_encode(values: List[Any], categories: Optional[List[Any]] = None) -> List[List[int]]:
    cats = categories or sorted(set(values))
    cat_index = {c: i for i, c in enumerate(cats)}
    return [[1 if v == c else 0 for c in cats] for v in values]


def _label_encode(values: List[Any]) -> Dict[Any, int]:
    unique = []
    for v in values:
        if v not in unique:
            unique.append(v)
    return {v: i for i, v in enumerate(unique)}


def _normalize(values: List[float], min_val: Optional[float] = None, max_val: Optional[float] = None) -> List[float]:
    mn = min_val if min_val is not None else min(values)
    mx = max_val if max_val is not None else max(values)
    if mx == mn:
        return [0.0] * len(values)
    return [(v - mn) / (mx - mn) for v in values]


def _standardize(values: List[float]) -> List[float]:
    if not values:
        return []
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std = variance ** 0.5
    if std == 0:
        return [0.0] * len(values)
    return [(v - mean) / std for v in values]


class TransformAction(BaseAction):
    """Data transformation operations.

    Provides flattening, pivoting, mapping, encoding, normalization, standardization.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "map")
        data = params.get("data", [])
        key = params.get("key", "")

        try:
            if operation == "flatten":
                if isinstance(data, dict):
                    result = _flatten_dict(data, sep=params.get("sep", "."))
                    return {"success": True, "data": result}
                return {"success": False, "error": "data must be a dict for flatten"}

            elif operation == "unflatten":
                if isinstance(data, dict):
                    result = _unflatten_dict(data, sep=params.get("sep", "."))
                    return {"success": True, "data": result}
                return {"success": False, "error": "data must be a dict for unflatten"}

            elif operation == "group_by":
                if not key:
                    return {"success": False, "error": "key required"}
                groups = _group_by(data, key)
                return {"success": True, "groups": groups, "group_count": len(groups)}

            elif operation == "pivot":
                index = params.get("index", "")
                columns = params.get("columns", "")
                values = params.get("values", "")
                aggfunc = params.get("aggfunc", "first")
                if not all([index, columns, values]):
                    return {"success": False, "error": "index, columns, values required"}
                result = _pivot_data(data, index, columns, values, aggfunc)
                return {"success": True, "pivot": result}

            elif operation == "map":
                if not key:
                    return {"success": False, "error": "key required"}
                mapping = params.get("mapping", {})
                default = params.get("default")
                results = [mapping.get(item.get(key), default) for item in data]
                return {"success": True, "results": results, "count": len(results)}

            elif operation == "filter":
                if not key:
                    return {"success": False, "error": "key required"}
                value = params.get("value")
                results = [item for item in data if item.get(key) == value]
                return {"success": True, "results": results, "count": len(results)}

            elif operation == "sort_by":
                if not key:
                    return {"success": False, "error": "key required"}
                reverse = params.get("reverse", False)
                results = sorted(data, key=lambda x: x.get(key), reverse=reverse)
                return {"success": True, "data": results, "count": len(results)}

            elif operation == "select":
                keys = params.get("keys", [])
                if not keys:
                    return {"success": False, "error": "keys required"}
                results = [{k: item.get(k) for k in keys} for item in data]
                return {"success": True, "data": results, "count": len(results)}

            elif operation == "rename":
                if not key:
                    return {"success": False, "error": "key required"}
                new_key = params.get("new_key", "")
                results = []
                for item in data:
                    new_item = dict(item)
                    if new_key and new_key != key:
                        new_item[new_key] = new_item.pop(key, None)
                    results.append(new_item)
                return {"success": True, "data": results}

            elif operation == "one_hot":
                values = params.get("values", data if isinstance(data, list) else [])
                categories = params.get("categories")
                encoded = _one_hot_encode(values, categories)
                return {"success": True, "encoded": encoded, "shape": [len(encoded), len(encoded[0]) if encoded else 0]}

            elif operation == "label_encode":
                values = params.get("values", data if isinstance(data, list) else [])
                mapping = _label_encode(values)
                encoded = [mapping.get(v, -1) for v in values]
                return {"success": True, "mapping": mapping, "encoded": encoded}

            elif operation == "normalize":
                values = [float(v) for v in (data if isinstance(data, list) else [])]
                min_val = float(params.get("min_val")) if params.get("min_val") else None
                max_val = float(params.get("max_val")) if params.get("max_val") else None
                result = _normalize(values, min_val, max_val)
                return {"success": True, "data": result}

            elif operation == "standardize":
                values = [float(v) for v in (data if isinstance(data, list) else [])]
                result = _standardize(values)
                return {"success": True, "data": result}

            elif operation == "deduplicate":
                seen = set()
                results = []
                for item in data:
                    key_val = str(item.get(key, item)) if key else str(item)
                    if key_val not in seen:
                        seen.add(key_val)
                        results.append(item)
                return {"success": True, "data": results, "removed": len(data) - len(results)}

            elif operation == "deep_get":
                path = params.get("path", key)
                default_val = params.get("default")
                if isinstance(data, dict):
                    result = _deep_get(data, path, default_val)
                    return {"success": True, "value": result}
                return {"success": False, "error": "data must be a dict for deep_get"}

            elif operation == "deep_set":
                path = params.get("path", "")
                value = params.get("value")
                if isinstance(data, dict) and path:
                    _deep_set(data, path, value)
                    return {"success": True, "data": data}
                return {"success": False, "error": "data must be a dict and path required"}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"TransformAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for transform operations."""
    return TransformAction().execute(context, params)
