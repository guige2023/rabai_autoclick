"""Data projection action module for RabAI AutoClick.

Provides data projection:
- DataProjector: Project data fields
- FieldSelector: Select specific fields
- FieldMapper: Map field names
- ComputedFields: Add computed fields
"""

from typing import Any, Callable, Dict, List, Optional, Set, Union
from dataclasses import dataclass

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataProjector:
    """Project data fields."""

    def __init__(self):
        self._field_mappings: Dict[str, str] = {}
        self._computed_fields: Dict[str, Callable] = {}
        self._excluded_fields: Set[str] = set()

    def select_fields(self, fields: List[str]) -> "DataProjector":
        """Select specific fields."""
        self._excluded_fields = set()
        for field in fields:
            self._field_mappings[field] = field
        return self

    def exclude_fields(self, fields: List[str]) -> "DataProjector":
        """Exclude specific fields."""
        self._excluded_fields.update(fields)
        return self

    def map_field(self, source: str, target: str) -> "DataProjector":
        """Map source field to target."""
        self._field_mappings[source] = target
        return self

    def add_computed(
        self,
        name: str,
        compute_fn: Callable[[Dict], Any],
    ) -> "DataProjector":
        """Add computed field."""
        self._computed_fields[name] = compute_fn
        return self

    def project(self, data: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """Project data."""
        if isinstance(data, list):
            return [self._project_single(item) for item in data]
        return self._project_single(data)

    def _project_single(self, item: Dict) -> Dict:
        """Project single item."""
        result = {}

        for source, target in self._field_mappings.items():
            if source not in self._excluded_fields:
                value = item.get(source)
                if value is not None:
                    result[target] = value

        for name, compute_fn in self._computed_fields.items():
            try:
                result[name] = compute_fn(item)
            except Exception:
                pass

        return result


class FieldSelector:
    """Select specific fields from data."""

    @staticmethod
    def select(data: Union[Dict, List[Dict]], fields: List[str]) -> Union[Dict, List[Dict]]:
        """Select fields."""
        if isinstance(data, list):
            return [{k: v for k, v in item.items() if k in fields} for item in data]
        return {k: v for k, v in data.items() if k in fields}

    @staticmethod
    def exclude(data: Union[Dict, List[Dict]], fields: List[str]) -> Union[Dict, List[Dict]]:
        """Exclude fields."""
        if isinstance(data, list):
            return [{k: v for k, v in item.items() if k not in fields} for item in data]
        return {k: v for k, v in data.items() if k not in fields}


class FieldMapper:
    """Map field names."""

    def __init__(self, mapping: Optional[Dict[str, str]] = None):
        self.mapping = mapping or {}

    def map(self, data: Union[Dict, List[Dict]]) -> Union[Dict, List[Dict]]:
        """Map fields."""
        if isinstance(data, list):
            return [self._map_single(item) for item in data]
        return self._map_single(data)

    def _map_single(self, item: Dict) -> Dict:
        """Map single item."""
        result = {}
        for source, target in self.mapping.items():
            if source in item:
                result[target] = item[source]
        for k, v in item.items():
            if k not in self.mapping:
                result[k] = v
        return result


class DataProjectionAction(BaseAction):
    """Data projection action."""
    action_type = "data_projection"
    display_name = "数据投影"
    description = "数据字段投影和映射"

    def __init__(self):
        super().__init__()
        self._projector = DataProjector()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "project")
            data = params.get("data", [])

            if operation == "project":
                return self._project(data, params)
            elif operation == "select":
                return self._select(data, params)
            elif operation == "exclude":
                return self._exclude(data, params)
            elif operation == "map":
                return self._map(data, params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Projection error: {str(e)}")

    def _project(self, data: List[Dict], params: Dict) -> ActionResult:
        """Project data."""
        mapping = params.get("mapping", {})
        computed = params.get("computed", [])

        projector = DataProjector()

        for source, target in mapping.items():
            projector.map_field(source, target)

        for comp in computed:
            name = comp.get("name")
            expr = comp.get("expression")
            if name and expr:
                try:
                    fn = eval(f"lambda item: {expr}")
                    projector.add_computed(name, fn)
                except Exception:
                    pass

        result = projector.project(data)

        return ActionResult(
            success=True,
            message=f"Projected {len(data)} items",
            data={"data": result, "count": len(result)},
        )

    def _select(self, data: List[Dict], params: Dict) -> ActionResult:
        """Select fields."""
        fields = params.get("fields", [])

        if not fields:
            return ActionResult(success=False, message="fields is required")

        result = FieldSelector.select(data, fields)

        return ActionResult(
            success=True,
            message=f"Selected {len(fields)} fields",
            data={"data": result, "count": len(result)},
        )

    def _exclude(self, data: List[Dict], params: Dict) -> ActionResult:
        """Exclude fields."""
        fields = params.get("fields", [])

        if not fields:
            return ActionResult(success=False, message="fields is required")

        result = FieldSelector.exclude(data, fields)

        return ActionResult(
            success=True,
            message=f"Excluded {len(fields)} fields",
            data={"data": result, "count": len(result)},
        )

    def _map(self, data: List[Dict], params: Dict) -> ActionResult:
        """Map fields."""
        mapping = params.get("mapping", {})

        if not mapping:
            return ActionResult(success=False, message="mapping is required")

        mapper = FieldMapper(mapping)
        result = mapper.map(data)

        return ActionResult(
            success=True,
            message=f"Mapped {len(mapping)} fields",
            data={"data": result, "count": len(result)},
        )
