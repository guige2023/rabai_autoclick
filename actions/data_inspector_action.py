"""Data inspector action module for RabAI AutoClick.

Provides data inspection operations:
- InspectSchemaAction: Inspect schema
- InspectTypesAction: Inspect data types
- InspectSampleAction: Sample data inspection
- InspectStructureAction: Inspect data structure
- InspectValidateAction: Validate data structure
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class InspectSchemaAction(BaseAction):
    """Inspect data schema."""
    action_type = "inspect_schema"
    display_name = "检查Schema"
    description = "检查数据Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            schema = {}
            for item in data:
                for key, value in item.items():
                    if key not in schema:
                        schema[key] = {"type": type(value).__name__, "first_value": value}
                        break

            return ActionResult(
                success=True,
                data={"schema": schema, "field_count": len(schema), "sample": data[0] if data else {}},
                message=f"Schema inspection: {len(schema)} fields detected",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Inspect schema failed: {e}")


class InspectTypesAction(BaseAction):
    """Inspect data types."""
    action_type = "inspect_types"
    display_name = "检查类型"
    description = "检查数据类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            if field:
                types = set(type(item.get(field)).__name__ for item in data)
                return ActionResult(
                    success=True,
                    data={"field": field, "types": list(types), "dominant_type": max(types, key=lambda t: sum(1 for i in data if type(i.get(field)).__name__ == t))},
                    message=f"Types for {field}: {types}",
                )
            else:
                type_map = {}
                for item in data:
                    for k, v in item.items():
                        t = type(v).__name__
                        type_map[k] = type_map.get(k, set())
                        type_map[k].add(t)
                return ActionResult(success=True, data={"type_map": {k: list(v) for k, v in type_map.items()}}, message=f"Type inspection complete")
        except Exception as e:
            return ActionResult(success=False, message=f"Inspect types failed: {e}")


class InspectSampleAction(BaseAction):
    """Sample data inspection."""
    action_type = "inspect_sample"
    display_name = "抽样检查"
    description = "抽样检查数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sample_size = params.get("sample_size", 5)
            method = params.get("method", "first")

            if not data:
                return ActionResult(success=False, message="data is required")

            if method == "first":
                sample = data[:sample_size]
            elif method == "last":
                sample = data[-sample_size:]
            elif method == "random":
                import random
                sample = random.sample(data, min(sample_size, len(data)))
            else:
                sample = data[:sample_size]

            return ActionResult(
                success=True,
                data={"sample": sample, "sample_size": len(sample), "method": method},
                message=f"Sampled {len(sample)} items using {method} method",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Inspect sample failed: {e}")


class InspectStructureAction(BaseAction):
    """Inspect data structure."""
    action_type = "inspect_structure"
    display_name = "检查结构"
    description = "检查数据结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            structures = []
            for i, item in enumerate(data[:10]):
                structures.append({"index": i, "keys": list(item.keys()), "value_types": {k: type(v).__name__ for k, v in item.items()}})

            return ActionResult(
                success=True,
                data={"structures": structures, "uniform": all(set(s["keys"]) == set(structures[0]["keys"]) for s in structures) if structures else False},
                message=f"Structure inspection: {'uniform' if all(set(s['keys']) == set(structures[0]['keys']) for s in structures) else 'mixed'} structure",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Inspect structure failed: {e}")


class InspectValidateAction(BaseAction):
    """Validate data structure."""
    action_type = "inspect_validate"
    display_name = "验证结构"
    description = "验证数据结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            expected_fields = params.get("expected_fields", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            errors = []
            for i, item in enumerate(data):
                missing = [f for f in expected_fields if f not in item]
                if missing:
                    errors.append(f"Row {i}: missing fields {missing}")
                extra = [k for k in item.keys() if k not in expected_fields]
                if extra:
                    errors.append(f"Row {i}: extra fields {extra}")

            is_valid = len(errors) == 0

            return ActionResult(
                success=True,
                data={"is_valid": is_valid, "errors": errors, "error_count": len(errors), "validated_rows": len(data)},
                message=f"Validation: {'PASSED' if is_valid else f'FAILED ({len(errors)} errors)'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Inspect validate failed: {e}")
