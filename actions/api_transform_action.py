"""API transform action module for RabAI AutoClick.

Provides API data transformation operations:
- ResponseTransformAction: Transform API response data structure
- RequestTransformAction: Transform request data before sending
- DataMapperAction: Map fields between different schemas
- SchemaConverterAction: Convert between data schemas
- ResponseEnricherAction: Enrich response with additional data
"""

import json
from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ResponseTransformAction(BaseAction):
    """Transform API response data structure."""
    action_type = "api_response_transform"
    display_name = "响应转换"
    description = "转换API响应数据结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            response = params.get("response", {})
            transform_type = params.get("transform_type", "flatten")
            rename_map = params.get("rename_map", {})
            exclude_fields = params.get("exclude_fields", [])
            include_fields = params.get("include_fields")
            wrap_key = params.get("wrap_key")

            if isinstance(response, str):
                try:
                    response = json.loads(response)
                except Exception:
                    response = {"raw": response}

            if transform_type == "flatten":
                flat = self._flatten_dict(response)
                result = flat
            elif transform_type == "compact":
                result = {k: v for k, v in response.items() if v is not None and v != ""}
            elif transform_type == "expand":
                result = self._expand_dict(response)
            else:
                result = response

            if rename_map:
                result = {rename_map.get(k, k): v for k, v in result.items()}

            if exclude_fields:
                result = {k: v for k, v in result.items() if k not in exclude_fields}

            if include_fields:
                result = {k: v for k, v in result.items() if k in include_fields}

            if wrap_key:
                result = {wrap_key: result}

            return ActionResult(
                success=True,
                data={
                    "transformed": result,
                    "transform_type": transform_type,
                    "original_keys": len(response) if isinstance(response, dict) else 0,
                    "transformed_keys": len(result) if isinstance(result, dict) else 0
                },
                message=f"Transformed response using {transform_type}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Response transform error: {str(e)}")

    def _flatten_dict(self, d: Dict, parent_key: str = "", sep: str = ".") -> Dict:
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(self._flatten_dict(item, f"{new_key}[{i}]", sep=sep).items())
                    else:
                        items.append((f"{new_key}[{i}]", item))
            else:
                items.append((new_key, v))
        return dict(items)

    def _expand_dict(self, d: Dict, sep: str = ".") -> Dict:
        result = {}
        for key, value in d.items():
            parts = key.split(sep)
            current = result
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        return result

    def get_required_params(self) -> List[str]:
        return ["response"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"transform_type": "flatten", "rename_map": {}, "exclude_fields": [], "include_fields": None, "wrap_key": None}


class RequestTransformAction(BaseAction):
    """Transform request data before sending."""
    action_type = "api_request_transform"
    display_name = "请求转换"
    description = "发送前转换请求数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            request_data = params.get("request_data", {})
            transform_rules = params.get("transform_rules", {})
            default_values = params.get("default_values", {})
            coerce_types = params.get("coerce_types", {})

            result = dict(request_data)

            for field, default in default_values.items():
                if field not in result or result[field] is None:
                    result[field] = default

            for field, type_name in coerce_types.items():
                if field in result:
                    try:
                        if type_name == "int":
                            result[field] = int(result[field])
                        elif type_name == "float":
                            result[field] = float(result[field])
                        elif type_name == "str":
                            result[field] = str(result[field])
                        elif type_name == "bool":
                            result[field] = bool(result[field])
                    except (ValueError, TypeError):
                        pass

            for rule_name, rule_config in transform_rules.items():
                transform_type = rule_config.get("type")
                field = rule_config.get("field")
                if field not in result:
                    continue
                if transform_type == "uppercase":
                    result[field] = str(result[field]).upper()
                elif transform_type == "lowercase":
                    result[field] = str(result[field]).lower()
                elif transform_type == "trim":
                    result[field] = str(result[field]).strip()
                elif transform_type == "capitalize":
                    result[field] = str(result[field]).capitalize()

            return ActionResult(
                success=True,
                data={
                    "transformed_request": result,
                    "fields_transformed": list(transform_rules.keys()),
                },
                message=f"Transformed request with {len(transform_rules)} rules"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Request transform error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["request_data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"transform_rules": {}, "default_values": {}, "coerce_types": {}}


class DataMapperAction(BaseAction):
    """Map fields between different schemas."""
    action_type = "api_data_mapper"
    display_name = "数据映射器"
    description = "在不同schema之间映射字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            mapping = params.get("mapping", {})
            reverse = params.get("reverse", False)
            skip_missing = params.get("skip_missing", True)

            if reverse:
                mapping = {v: k for k, v in mapping.items()}

            mapped = {}
            for source_key, target_key in mapping.items():
                value = data.get(source_key)
                if value is not None or not skip_missing:
                    mapped[target_key] = value

            return ActionResult(
                success=True,
                data={
                    "mapped_data": mapped,
                    "fields_mapped": len(mapping),
                },
                message=f"Mapped {len(mapping)} fields"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data mapper error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data", "mapping"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"reverse": False, "skip_missing": True}


class SchemaConverterAction(BaseAction):
    """Convert between data schemas."""
    action_type = "api_schema_converter"
    display_name = "Schema转换器"
    description = "在不同数据schema之间转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            source_schema = params.get("source_schema", "json")
            target_schema = params.get("target_schema", "xml")

            converted = data

            if source_schema == "json" and target_schema == "xml":
                converted = self._json_to_xml(data)
            elif source_schema == "xml" and target_schema == "json":
                converted = self._xml_to_json(data)
            elif source_schema == "json" and target_schema == "csv":
                converted = self._json_to_csv(data)
            elif source_schema == "csv" and target_schema == "json":
                converted = self._csv_to_json(data)

            return ActionResult(
                success=True,
                data={
                    "converted": converted,
                    "source_schema": source_schema,
                    "target_schema": target_schema,
                },
                message=f"Converted from {source_schema} to {target_schema}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema converter error: {str(e)}")

    def _json_to_xml(self, data: Dict) -> str:
        items = []
        for key, value in data.items():
            items.append(f"<{key}>{value}</{key}>")
        return f"<root>{''.join(items)}</root>"

    def _xml_to_json(self, xml_str: str) -> Dict:
        import re
        pattern = r"<(\w+)>([^<]*)</\1>"
        matches = re.findall(pattern, xml_str)
        return {k: v for k, v in matches}

    def _json_to_csv(self, data: Union[Dict, List]) -> str:
        if isinstance(data, dict):
            data = [data]
        if not data:
            return ""
        headers = list(data[0].keys())
        rows = [",".join(str(row.get(h, "")) for h in headers) for row in data]
        return "\n".join([",".join(headers), *rows])

    def _csv_to_json(self, csv_str: str) -> List[Dict]:
        lines = csv_str.strip().split("\n")
        if len(lines) < 2:
            return []
        headers = lines[0].split(",")
        return [dict(zip(headers, line.split(","))) for line in lines[1:]]

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"source_schema": "json", "target_schema": "xml"}


class ResponseEnricherAction(BaseAction):
    """Enrich response with additional data."""
    action_type = "api_response_enricher"
    display_name = "响应富化"
    description = "用附加数据富化响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            response = params.get("response", {})
            enrichments = params.get("enrichments", {})
            merge_strategy = params.get("merge_strategy", "shallow")
            prefix = params.get("prefix", "")

            enriched = dict(response)

            for key, value in enrichments.items():
                final_key = f"{prefix}{key}" if prefix else key
                if merge_strategy == "deep" and isinstance(value, dict) and isinstance(enriched.get(key), dict):
                    enriched[key] = {**enriched[key], **value}
                else:
                    enriched[final_key] = value

            return ActionResult(
                success=True,
                data={
                    "enriched": enriched,
                    "enrichments_added": len(enrichments),
                    "merge_strategy": merge_strategy,
                },
                message=f"Enriched response with {len(enrichments)} additions"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Response enricher error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["response"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"enrichments": {}, "merge_strategy": "shallow", "prefix": ""}
