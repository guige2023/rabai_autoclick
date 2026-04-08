"""API Transform Action Module.

Provides API request/response transformation capabilities including
mapping, conversion, and enrichment.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Callable
from copy import deepcopy

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RequestTransformAction(BaseAction):
    """Transform API request data.
    
    Supports field mapping, value conversion, and request enrichment.
    """
    action_type = "request_transform"
    display_name = "请求转换"
    description = "转换API请求数据，支持字段映射和转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Transform an API request.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - request: Original request dict.
                - mapping: Field mapping dict {src: dst}.
                - transforms: Field transformation functions.
                - defaults: Default values for missing fields.
                - add_fields: Fields to add.
                - remove_fields: Fields to remove.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with transformed request or error.
        """
        request = params.get('request', {})
        mapping = params.get('mapping', {})
        transforms = params.get('transforms', {})
        defaults = params.get('defaults', {})
        add_fields = params.get('add_fields', {})
        remove_fields = params.get('remove_fields', [])
        output_var = params.get('output_var', 'transformed_request')

        if not isinstance(request, dict):
            return ActionResult(
                success=False,
                message=f"Expected dict for request, got {type(request).__name__}"
            )

        try:
            result = {}

            # Apply field mapping
            for src_field, dst_field in mapping.items():
                if src_field in request:
                    result[dst_field] = request[src_field]

            # Add unmapped source fields if requested
            if params.get('keep_unmapped', False):
                mapped_values = set(mapping.values())
                for key, value in request.items():
                    if key not in mapping and key not in result:
                        result[key] = value

            # Apply field transforms
            for field, transform in transforms.items():
                if field in result:
                    result[field] = self._apply_transform(result[field], transform)

            # Add default values
            for field, default in defaults.items():
                if field not in result:
                    result[field] = default

            # Add new fields
            for field, value in add_fields.items():
                computed_value = self._compute_value(value, request, context)
                result[field] = computed_value

            # Remove fields
            for field in remove_fields:
                result.pop(field, None)

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Request transformed: {len(request)} -> {len(result)} fields"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Request transform failed: {str(e)}"
            )

    def _apply_transform(self, value: Any, transform: Any) -> Any:
        """Apply a transformation to a value."""
        if callable(transform):
            return transform(value)
        elif isinstance(transform, str):
            # Built-in transforms
            if transform == 'upper':
                return str(value).upper()
            elif transform == 'lower':
                return str(value).lower()
            elif transform == 'trim':
                return str(value).strip()
            elif transform == 'int':
                return int(value)
            elif transform == 'float':
                return float(value)
            elif transform == 'str':
                return str(value)
            elif transform == 'bool':
                return bool(value)
            elif transform == 'json':
                if isinstance(value, str):
                    return json.loads(value)
                return value
        return value

    def _compute_value(self, value: Any, source: Dict, context: Any) -> Any:
        """Compute a field value from an expression."""
        if isinstance(value, str) and '${' in value:
            # Simple variable interpolation
            import re
            pattern = re.compile(r'\$\{([^}]+)\}')
            def replace(match):
                path = match.group(1)
                parts = path.split('.')
                current = source
                for part in parts:
                    if isinstance(current, dict):
                        current = current.get(part)
                    else:
                        return value
                return str(current) if current is not None else ''
            return pattern.sub(replace, value)
        return value


class ResponseTransformAction(BaseAction):
    """Transform API response data.
    
    Supports response parsing, field extraction, and data normalization.
    """
    action_type = "response_transform"
    display_name: "响应转换"
    description = "转换API响应数据，支持解析和字段提取"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Transform an API response.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - response: Original response (dict, string, or raw).
                - extract_path: Dot-notation path to extract data.
                - mapping: Response field mapping.
                - normalize: Whether to normalize nested data.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with transformed response or error.
        """
        response = params.get('response', {})
        extract_path = params.get('extract_path', '')
        mapping = params.get('mapping', {})
        normalize = params.get('normalize', False)
        output_var = params.get('output_var', 'transformed_response')

        try:
            # Parse response if string
            if isinstance(response, str):
                try:
                    response = json.loads(response)
                except json.JSONDecodeError:
                    return ActionResult(
                        success=False,
                        message="Response is not valid JSON"
                    )

            # Extract data from path
            data = response
            if extract_path:
                data = self._extract_path(response, extract_path)

            # Apply mapping
            if mapping and isinstance(data, dict):
                data = self._apply_mapping(data, mapping)

            # Normalize if requested
            if normalize and isinstance(data, list):
                data = self._normalize_list(data)

            result = {
                'data': data,
                'original_response': response,
                'extracted': bool(extract_path)
            }

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Response transformed successfully"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Response transform failed: {str(e)}"
            )

    def _extract_path(self, data: Any, path: str) -> Any:
        """Extract data using dot notation path."""
        parts = path.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    current = current[int(part)]
                except (ValueError, IndexError):
                    return None
            else:
                return None
            if current is None:
                return None
        return current

    def _apply_mapping(self, data: Dict, mapping: Dict) -> Dict:
        """Apply field mapping to data."""
        result = {}
        for src_field, dst_field in mapping.items():
            if src_field in data:
                result[dst_field] = data[src_field]
        return result

    def _normalize_list(self, data: List) -> List:
        """Normalize a list of dicts to common schema."""
        if not data:
            return []

        # Find common keys
        all_keys = set()
        for item in data:
            if isinstance(item, dict):
                all_keys.update(item.keys())

        # Normalize each item
        normalized = []
        for item in data:
            if isinstance(item, dict):
                normalized_item = {}
                for key in all_keys:
                    normalized_item[key] = item.get(key)
                normalized.append(normalized_item)
            else:
                normalized.append(item)

        return normalized


class DataEnricherAction(BaseAction):
    """Enrich data with additional information.
    
    Supports lookup enrichment, computation, and external data joining.
    """
    action_type: "data_enricher"
    display_name = "数据增强"
    description = "用附加信息增强数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Enrich data with additional information.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data to enrich.
                - enrichments: List of enrichment definitions.
                - lookup_table: Lookup dictionary.
                - computed_fields: Fields to compute.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with enriched data or error.
        """
        data = params.get('data', [])
        enrichments = params.get('enrichments', [])
        lookup_table = params.get('lookup_table', {})
        computed_fields = params.get('computed_fields', [])
        output_var = params.get('output_var', 'enriched')

        if isinstance(data, dict):
            data = [data]

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list or dict for data, got {type(data).__name__}"
            )

        try:
            enriched = []

            for item in data:
                enriched_item = dict(item) if isinstance(item, dict) else item

                # Apply enrichments
                for enrichment in enrichments:
                    enrich_type = enrichment.get('type', 'lookup')
                    source_field = enrichment.get('source_field')
                    target_field = enrichment.get('target_field', source_field)

                    if enrich_type == 'lookup':
                        lookup_key = item.get(source_field) if source_field else None
                        if lookup_key and lookup_key in lookup_table:
                            enriched_item[target_field] = lookup_table[lookup_key]

                    elif enrich_type == 'static':
                        enriched_item[target_field] = enrichment.get('value')

                    elif enrich_type == 'copy':
                        copy_source = enrichment.get('from_field')
                        if copy_source:
                            enriched_item[target_field] = item.get(copy_source)

                # Compute fields
                for comp_field in computed_fields:
                    field_name = comp_field.get('name')
                    expression = comp_field.get('expression')
                    if field_name and expression:
                        enriched_item[field_name] = self._compute_expression(
                            expression, item, context
                        )

                enriched.append(enriched_item)

            context.variables[output_var] = enriched
            return ActionResult(
                success=True,
                data={'enriched': enriched, 'count': len(enriched)},
                message=f"Enriched {len(data)} items with {len(enrichments)} enrichments"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Data enrichment failed: {str(e)}"
            )

    def _compute_expression(self, expression: str, item: Dict, context: Any) -> Any:
        """Compute an expression against item data."""
        try:
            # Simple expression evaluation
            # Format: field1 + field2, field1 * 2, etc.
            import re

            # Replace field references
            def replace_field(match):
                field = match.group(1)
                value = item.get(field, 0)
                return str(value)

            expr = re.sub(r'\{(\w+)\}', replace_field, expression)

            # Try to evaluate
            if re.match(r'^[\d\s\+\-\*\/\.\(\)]+$', expr):
                return eval(expr)
            return expression

        except Exception:
            return None


class SchemaConverterAction(BaseAction):
    """Convert data between different schemas.
    
    Supports schema mapping, type conversion, and validation.
    """
    action_type = "schema_converter"
    display_name = "Schema转换"
    description = "在不同Schema之间转换数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Convert data between schemas.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data.
                - source_schema: Source schema definition.
                - target_schema: Target schema definition.
                - mapping: Schema field mapping.
                - validate: Whether to validate output.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with converted data or error.
        """
        data = params.get('data', {})
        source_schema = params.get('source_schema', {})
        target_schema = params.get('target_schema', {})
        mapping = params.get('mapping', {})
        validate = params.get('validate', False)
        output_var = params.get('output_var', 'converted')

        try:
            # Apply mapping
            converted = self._apply_schema_mapping(data, mapping)

            # Apply type conversions
            for field, field_schema in target_schema.items():
                if field in converted and 'type' in field_schema:
                    converted[field] = self._convert_type(
                        converted[field], field_schema['type']
                    )

            # Add default values for missing required fields
            for field, field_schema in target_schema.items():
                if field not in converted and 'default' in field_schema:
                    converted[field] = field_schema['default']

            # Validate if requested
            validation_result = {'valid': True, 'errors': []}
            if validate:
                validation_result = self._validate_against_schema(converted, target_schema)

            result = {
                'data': converted,
                'validation': validation_result,
                'converted_fields': len(converted)
            }

            context.variables[output_var] = result
            return ActionResult(
                success=validation_result['valid'],
                data=result,
                message=f"Schema conversion completed: {len(converted)} fields"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Schema conversion failed: {str(e)}"
            )

    def _apply_schema_mapping(self, data: Dict, mapping: Dict) -> Dict:
        """Apply schema field mapping."""
        result = {}
        for src_field, dst_field in mapping.items():
            if src_field in data:
                result[dst_field] = data[src_field]

        # Add unmapped fields that exist in target
        for key, value in data.items():
            if key not in mapping and key not in result:
                result[key] = value

        return result

    def _convert_type(self, value: Any, target_type: str) -> Any:
        """Convert value to target type."""
        type_converters = {
            'string': str,
            'str': str,
            'integer': int,
            'int': int,
            'float': float,
            'number': float,
            'boolean': bool,
            'bool': bool,
            'array': list,
            'list': list,
            'object': dict,
            'dict': dict,
        }

        converter = type_converters.get(target_type.lower())
        if converter:
            try:
                return converter(value)
            except (ValueError, TypeError):
                return value
        return value

    def _validate_against_schema(
        self, data: Dict, schema: Dict
    ) -> Dict:
        """Validate data against a schema."""
        errors = []

        for field, field_schema in schema.items():
            required = field_schema.get('required', False)

            if required and field not in data:
                errors.append(f"Missing required field: {field}")
                continue

            if field in data:
                expected_type = field_schema.get('type', 'string')
                value = data[field]

                if not self._matches_type(value, expected_type):
                    errors.append(
                        f"Field '{field}' has wrong type: expected {expected_type}"
                    )

        return {'valid': len(errors) == 0, 'errors': errors}

    def _matches_type(self, value: Any, type_name: str) -> bool:
        """Check if value matches expected type."""
        type_checks = {
            'string': lambda v: isinstance(v, str),
            'integer': lambda v: isinstance(v, int) and not isinstance(v, bool),
            'float': lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
            'boolean': lambda v: isinstance(v, bool),
            'array': lambda v: isinstance(v, list),
            'object': lambda v: isinstance(v, dict),
        }

        check = type_checks.get(type_name.lower())
        return check(value) if check else True
