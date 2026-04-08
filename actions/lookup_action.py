"""Lookup table action module for RabAI AutoClick.

Provides lookup table operations:
- LookupCreateAction: Create a lookup table
- LookupGetAction: Get value from lookup
- LookupSetAction: Set value in lookup
- LookupJoinAction: Join data with lookup
"""

import threading
from typing import Any, Callable, Dict, List, Optional
from datetime import datetime


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LookupTable:
    """Thread-safe lookup table."""
    def __init__(self, name: str):
        self.name = name
        self._data: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._access_count: Dict[str, int] = {}
        self._last_access: Dict[str, datetime] = {}

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value
            self._access_count[key] = 0
            self._last_access[key] = datetime.utcnow()

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            if key in self._data:
                self._access_count[key] = self._access_count.get(key, 0) + 1
                self._last_access[key] = datetime.utcnow()
                return self._data[key]
            return default

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._data:
                del self._data[key]
                self._access_count.pop(key, None)
                self._last_access.pop(key, None)
                return True
            return False

    def keys(self) -> List[str]:
        with self._lock:
            return list(self._data.keys())

    def items(self) -> List[tuple]:
        with self._lock:
            return list(self._data.items())

    def size(self) -> int:
        with self._lock:
            return len(self._data)

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "size": len(self._data),
                "most_accessed": sorted(self._access_count.items(), key=lambda x: x[1], reverse=True)[:5],
                "recently_accessed": sorted(self._last_access.items(), key=lambda x: x[1], reverse=True)[:5]
            }


_tables: Dict[str, LookupTable] = {}
_tables_lock = threading.Lock()


class LookupCreateAction(BaseAction):
    """Create a lookup table."""
    action_type = "lookup_create"
    display_name = "创建查找表"
    description = "创建查找表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "default")
            initial_data = params.get("initial_data", {})

            with _tables_lock:
                if name in _tables:
                    return ActionResult(success=True, message=f"Lookup table '{name}' already exists")
                table = LookupTable(name=name)
                for k, v in initial_data.items():
                    table.set(k, v)
                _tables[name] = table

            return ActionResult(
                success=True,
                message=f"Lookup table '{name}' created with {len(initial_data)} entries",
                data={"name": name, "size": len(initial_data)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Lookup create failed: {str(e)}")


class LookupGetAction(BaseAction):
    """Get value from lookup."""
    action_type = "lookup_get"
    display_name = "查找获取"
    description = "从查找表获取值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "default")
            key = params.get("key", "")
            default = params.get("default", None)

            if not key:
                return ActionResult(success=False, message="key is required")

            with _tables_lock:
                if name not in _tables:
                    return ActionResult(success=False, message=f"Lookup table '{name}' not found")
                value = _tables[name].get(key, default)

            return ActionResult(
                success=True,
                message=f"Got value for key '{key}' from '{name}'",
                data={"key": key, "value": value, "found": value != default}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Lookup get failed: {str(e)}")


class LookupSetAction(BaseAction):
    """Set value in lookup."""
    action_type = "lookup_set"
    display_name = "查找设置"
    description = "在查找表中设置值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "default")
            key = params.get("key", "")
            value = params.get("value", None)
            auto_create = params.get("auto_create", True)

            if not key:
                return ActionResult(success=False, message="key is required")

            with _tables_lock:
                if name not in _tables:
                    if auto_create:
                        _tables[name] = LookupTable(name=name)
                    else:
                        return ActionResult(success=False, message=f"Lookup table '{name}' not found")
                _tables[name].set(key, value)

            return ActionResult(
                success=True,
                message=f"Set '{key}' = {str(value)[:50]} in '{name}'",
                data={"key": key, "value": value, "table": name}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Lookup set failed: {str(e)}")


class LookupJoinAction(BaseAction):
    """Join data with lookup table."""
    action_type = "lookup_join"
    display_name = "查找表连接"
    description = "将数据与查找表连接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "default")
            data = params.get("data", [])
            key_field = params.get("key_field", "")
            lookup_field = params.get("lookup_field", "value")
            add_fields = params.get("add_fields", [])

            if not data:
                return ActionResult(success=False, message="data is required")
            if not key_field:
                return ActionResult(success=False, message="key_field is required")

            with _tables_lock:
                if name not in _tables:
                    return ActionResult(success=False, message=f"Lookup table '{name}' not found")
                table = _tables[name]

            joined = []
            for record in data:
                if isinstance(record, dict):
                    key = record.get(key_field)
                    if key is not None:
                        lookup_value = table.get(str(key))
                        new_record = dict(record)
                        if lookup_value is not None:
                            if isinstance(lookup_value, dict):
                                for field in add_fields:
                                    new_record[field] = lookup_value.get(field, None)
                                new_record[lookup_field] = lookup_value
                            else:
                                new_record[lookup_field] = lookup_value
                        joined.append(new_record)
                    else:
                        joined.append(record)
                else:
                    joined.append(record)

            return ActionResult(
                success=True,
                message=f"Joined {len(joined)} records with lookup table '{name}'",
                data={"joined": joined, "count": len(joined)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Lookup join failed: {str(e)}")
