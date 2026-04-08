"""JSON data processing action module for RabAI AutoClick.

Provides JSON parsing, serialization, querying, and transformation actions.
"""

import json
import copy
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JsonParseAction(BaseAction):
    """Parse JSON string into Python object.
    
    Handles JSON strings with error recovery, strip whitespace,
    and support for nested object extraction.
    """
    action_type = "json_parse"
    display_name = "JSON解析"
    description = "将JSON字符串解析为Python对象"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse a JSON string.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: json_string, path, default_on_error.
        
        Returns:
            ActionResult with parsed object or error message.
        """
        json_string = params.get('json_string', '')
        if not json_string:
            return ActionResult(success=False, message="json_string is required")
        
        path = params.get('path', '')
        default_on_error = params.get('default_on_error', None)
        
        # Strip whitespace
        json_string = json_string.strip()
        
        try:
            parsed = json.loads(json_string)
        except json.JSONDecodeError as e:
            if default_on_error is not None:
                return ActionResult(
                    success=True,
                    message=f"Parse error, returning default: {e}",
                    data=default_on_error
                )
            return ActionResult(
                success=False,
                message=f"JSON parse error at pos {e.pos}: {e.msg}",
                data={'error': str(e), 'json': json_string[:200]}
            )
        
        # Extract path if specified
        if path:
            try:
                for key in path.split('.'):
                    if key.isdigit():
                        parsed = parsed[int(key)]
                    else:
                        parsed = parsed[key]
            except (KeyError, IndexError, TypeError) as e:
                return ActionResult(
                    success=False,
                    message=f"Path '{path}' not found: {e}",
                    data={'parsed': parsed}
                )
        
        return ActionResult(
            success=True,
            message=f"Parsed {type(parsed).__name__}",
            data=parsed
        )


class JsonSerializeAction(BaseAction):
    """Serialize Python object to JSON string.
    
    Supports pretty printing, custom separators, and sort keys option.
    """
    action_type = "json_serialize"
    display_name = "JSON序列化"
    description = "将Python对象序列化为JSON字符串"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Serialize a Python object to JSON string.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: obj, pretty, sort_keys, indent,
                   separators, ensure_ascii.
        
        Returns:
            ActionResult with serialized JSON string.
        """
        obj = params.get('obj')
        if obj is None:
            return ActionResult(success=False, message="obj is required")
        
        pretty = params.get('pretty', False)
        sort_keys = params.get('sort_keys', False)
        indent = params.get('indent', 2)
        separators = params.get('separators', (', ', ': '))
        ensure_ascii = params.get('ensure_ascii', False)
        
        try:
            if pretty:
                json_str = json.dumps(
                    obj,
                    indent=indent,
                    sort_keys=sort_keys,
                    ensure_ascii=ensure_ascii,
                    separators=separators
                )
            else:
                json_str = json.dumps(
                    obj,
                    sort_keys=sort_keys,
                    ensure_ascii=ensure_ascii,
                    separators=(',', ':')
                )
            
            return ActionResult(
                success=True,
                message=f"Serialized {len(json_str)} chars",
                data=json_str
            )
        except (TypeError, ValueError) as e:
            return ActionResult(
                success=False,
                message=f"Serialization error: {e}",
                data={'error': str(e)}
            )


class JsonQueryAction(BaseAction):
    """Query JSON object using dot notation or JSONPath-like expressions.
    
    Supports extracting nested values, array indexing, and filtering.
    """
    action_type = "json_query"
    display_name = "JSON查询"
    description = "使用点号路径查询JSON对象中的值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Query a value from JSON object.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: json_obj, query, default.
        
        Returns:
            ActionResult with queried value or default.
        """
        json_obj = params.get('json_obj')
        query = params.get('query', '')
        default = params.get('default', None)
        
        if json_obj is None:
            return ActionResult(success=False, message="json_obj is required")
        
        if not query:
            return ActionResult(success=True, message="No query, returning whole object", data=json_obj)
        
        # Parse query into path segments
        result = json_obj
        try:
            for segment in query.split('.'):
                # Check for array index [n]
                if '[' in segment and segment.endswith(']'):
                    key, rest = segment.split('[', 1)
                    if key:
                        result = result[key]
                    idx = int(rest[:-1])
                    result = result[idx]
                else:
                    result = result[segment]
            
            return ActionResult(
                success=True,
                message=f"Found value of type {type(result).__name__}",
                data=result
            )
        except (KeyError, IndexError, TypeError) as e:
            if default is not None:
                return ActionResult(
                    success=True,
                    message=f"Query not found, returning default",
                    data=default
                )
            return ActionResult(
                success=False,
                message=f"Query '{query}' not found: {e}",
                data={'json_obj': json_obj}
            )


class JsonTransformAction(BaseAction):
    """Transform JSON object using mapping rules.
    
    Supports renaming keys, filtering keys, adding computed fields,
    and applying transformations to values.
    """
    action_type = "json_transform"
    display_name = "JSON转换"
    description = "根据映射规则转换JSON对象结构"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Transform a JSON object.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: json_obj, rename_keys, filter_keys,
                   add_fields, remove_nulls, deep_copy.
        
        Returns:
            ActionResult with transformed object.
        """
        json_obj = params.get('json_obj')
        if json_obj is None:
            return ActionResult(success=False, message="json_obj is required")
        
        rename_keys = params.get('rename_keys', {})
        filter_keys = params.get('filter_keys', None)
        add_fields = params.get('add_fields', {})
        remove_nulls = params.get('remove_nulls', False)
        deep_copy_flag = params.get('deep_copy', True)
        
        # Work on a copy to avoid mutating original
        result = copy.deepcopy(json_obj) if deep_copy_flag else json_obj
        
        # Rename keys
        if rename_keys and isinstance(result, dict):
            for old_key, new_key in rename_keys.items():
                if old_key in result:
                    result[new_key] = result.pop(old_key)
        
        # Filter keys
        if filter_keys is not None:
            if isinstance(filter_keys, list) and isinstance(result, dict):
                result = {k: v for k, v in result.items() if k in filter_keys}
        
        # Remove nulls
        if remove_nulls and isinstance(result, dict):
            result = {k: v for k, v in result.items() if v is not None}
        
        # Add computed fields
        if add_fields and isinstance(result, dict):
            result.update(add_fields)
        
        return ActionResult(
            success=True,
            message=f"Transformed to {type(result).__name__}",
            data=result
        )
