"""
API transformer action for request/response transformation.

Provides mapping, filtering, enrichment, and format conversion.
"""

from typing import Any, Callable, Dict, List, Optional, Union
import json
import re


class APITransformerAction:
    """Request and response transformation pipeline."""

    def __init__(self) -> None:
        """Initialize API transformer."""
        self._transformations: Dict[str, Dict[str, Any]] = {}
        self._mappings: Dict[str, Callable] = {}

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute transformation operation.

        Args:
            params: Dictionary containing:
                - operation: 'transform', 'map', 'filter', 'enrich'
                - data: Data to transform
                - transformation: Transformation name
                - field_mappings: Field mapping rules

        Returns:
            Dictionary with transformed data
        """
        operation = params.get("operation", "transform")

        if operation == "transform":
            return self._transform_data(params)
        elif operation == "map":
            return self._map_fields(params)
        elif operation == "filter":
            return self._filter_fields(params)
        elif operation == "enrich":
            return self._enrich_data(params)
        elif operation == "register":
            return self._register_transformation(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _transform_data(self, params: dict[str, Any]) -> dict[str, Any]:
        """Apply registered transformation to data."""
        data = params.get("data", {})
        transformation_name = params.get("transformation", "default")
        field_mappings = params.get("field_mappings", {})

        if not data:
            return {"success": False, "error": "Data is required"}

        result = self._apply_mappings(data, field_mappings)

        transformation_steps = params.get("steps", [])
        for step in transformation_steps:
            step_type = step.get("type", "passthrough")
            if step_type == "rename":
                result = self._rename_fields(result, step.get("mapping", {}))
            elif step_type == "remove":
                result = self._remove_fields(result, step.get("fields", []))
            elif step_type == "add":
                result = self._add_fields(result, step.get("fields", {}))
            elif step_type == "convert":
                result = self._convert_types(result, step.get("conversions", {}))
            elif step_type == "map":
                result = self._map_values(result, step.get("value_map", {}))

        return {"success": True, "data": result, "transformed": True}

    def _map_fields(self, params: dict[str, Any]) -> dict[str, Any]:
        """Map fields from source to destination schema."""
        data = params.get("data", {})
        mappings = params.get("mappings", {})

        if not data:
            return {"success": False, "error": "Data is required"}

        result = {}
        for dest_field, source_field in mappings.items():
            if isinstance(source_field, str):
                result[dest_field] = data.get(source_field)
            elif isinstance(source_field, dict):
                transform_fn = source_field.get("transform")
                if transform_fn and callable(transform_fn):
                    result[dest_field] = transform_fn(data)
                else:
                    result[dest_field] = source_field.get("default")

        return {"success": True, "mapped_data": result}

    def _filter_fields(self, params: dict[str, Any]) -> dict[str, Any]:
        """Filter fields based on include/exclude rules."""
        data = params.get("data", {})
        include = params.get("include", [])
        exclude = params.get("exclude", [])

        if not data:
            return {"success": False, "error": "Data is required"}

        if include:
            result = {k: v for k, v in data.items() if k in include}
        elif exclude:
            result = {k: v for k, v in data.items() if k not in exclude}
        else:
            result = data

        return {"success": True, "filtered_data": result, "field_count": len(result)}

    def _enrich_data(self, params: dict[str, Any]) -> dict[str, Any]:
        """Enrich data with computed fields and lookups."""
        data = params.get("data", {})
        enrichments = params.get("enrichments", {})

        if not data:
            return {"success": False, "error": "Data is required"}

        result = dict(data)

        for field_name, enrichment_config in enrichments.items():
            enrichment_type = enrichment_config.get("type", "static")
            enrichment_value = enrichment_config.get("value")

            if enrichment_type == "static":
                result[field_name] = enrichment_value
            elif enrichment_type == "computed":
                compute_fn = enrichment_config.get("compute")
                if callable(compute_fn):
                    result[field_name] = compute_fn(data)
            elif enrichment_type == "lookup":
                lookup_table = enrichment_config.get("lookup_table", {})
                lookup_key = enrichment_config.get("lookup_key")
                if lookup_key:
                    result[field_name] = lookup_table.get(data.get(lookup_key))
            elif enrichment_type == "concat":
                concat_fields = enrichment_config.get("fields", [])
                separator = enrichment_config.get("separator", "")
                values = [str(data.get(f, "")) for f in concat_fields]
                result[field_name] = separator.join(values)
            elif enrichment_type == "regex_extract":
                source_field = enrichment_config.get("source_field")
                pattern = enrichment_config.get("pattern", "")
                if source_field and source_field in data:
                    match = re.search(pattern, str(data[source_field]))
                    result[field_name] = match.group(1) if match else None

        return {"success": True, "enriched_data": result, "enriched_fields": list(enrichments.keys())}

    def _register_transformation(self, params: dict[str, Any]) -> dict[str, Any]:
        """Register a named transformation."""
        name = params.get("name", "")
        transformation_def = params.get("transformation", {})

        if not name:
            return {"success": False, "error": "Transformation name is required"}

        self._transformations[name] = {
            "definition": transformation_def,
            "registered_at": params.get("timestamp", 0),
        }

        return {"success": True, "transformation": name}

    def _apply_mappings(
        self, data: Dict[str, Any], mappings: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply field mappings to data."""
        result = {}
        for dest, source in mappings.items():
            if isinstance(source, str):
                result[dest] = data.get(source)
            elif isinstance(source, list):
                result[dest] = [data.get(s) for s in source]
        return result

    def _rename_fields(
        self, data: Dict[str, Any], mapping: Dict[str, str]
    ) -> Dict[str, Any]:
        """Rename fields based on mapping."""
        result = {}
        for key, value in data.items():
            new_key = mapping.get(key, key)
            result[new_key] = value
        return result

    def _remove_fields(
        self, data: Dict[str, Any], fields: List[str]
    ) -> Dict[str, Any]:
        """Remove specified fields from data."""
        return {k: v for k, v in data.items() if k not in fields}

    def _add_fields(
        self, data: Dict[str, Any], fields: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Add new fields to data."""
        return {**data, **fields}

    def _convert_types(
        self, data: Dict[str, Any], conversions: Dict[str, str]
    ) -> Dict[str, Any]:
        """Convert field types."""
        result = dict(data)
        for field, type_name in conversions.items():
            if field in result:
                result[field] = self._convert_value(result[field], type_name)
        return result

    def _convert_value(self, value: Any, type_name: str) -> Any:
        """Convert value to specified type."""
        if type_name == "string":
            return str(value)
        elif type_name == "integer":
            return int(value)
        elif type_name == "float":
            return float(value)
        elif type_name == "boolean":
            return bool(value)
        elif type_name == "json":
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return value
        return value

    def _map_values(
        self, data: Dict[str, Any], value_map: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Map field values based on value map."""
        result = dict(data)
        for field, mappings in value_map.items():
            if field in result:
                result[field] = mappings.get(result[field], result[field])
        return result
