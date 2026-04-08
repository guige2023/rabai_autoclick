"""Variable action module for RabAI AutoClick.

Provides variable manipulation actions including get, set, increment, and type conversion.
"""

import copy
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class VariableGetAction(BaseAction):
    """Get a variable value from context.
    
    Retrieves variables stored in the execution context.
    """
    action_type = "variable_get"
    display_name = "获取变量"
    description = "从上下文获取变量值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get variable value.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: name, default, from_context.
        
        Returns:
            ActionResult with variable value.
        """
        name = params.get('name', '')
        default = params.get('default', None)
        from_context = params.get('from_context', 'variables')
        
        if not name:
            return ActionResult(success=False, message="name is required")
        
        try:
            if from_context == 'variables':
                value = getattr(context, name, default)
            elif from_context == 'env':
                value = os.environ.get(name, default)
            elif from_context == 'config':
                value = getattr(context, 'config', {}).get(name, default)
            else:
                value = getattr(context, name, default)
            
            return ActionResult(
                success=True,
                message=f"Got {name}",
                data={'name': name, 'value': value, 'type': type(value).__name__}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Get variable error: {e}",
                data={'error': str(e)}
            )


class VariableSetAction(BaseAction):
    """Set a variable value in context.
    
    Stores variables in the execution context for later use.
    """
    action_type = "variable_set"
    display_name = "设置变量"
    description = "在上下文中设置变量值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Set variable value.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: name, value, to_context, permanent.
        
        Returns:
            ActionResult with set status.
        """
        name = params.get('name', '')
        value = params.get('value', None)
        to_context = params.get('to_context', 'variables')
        permanent = params.get('permanent', False)
        
        if not name:
            return ActionResult(success=False, message="name is required")
        
        try:
            if to_context == 'variables':
                setattr(context, name, value)
            elif to_context == 'env':
                os.environ[name] = str(value)
            elif to_context == 'config':
                if not hasattr(context, 'config'):
                    context.config = {}
                context.config[name] = value
            else:
                setattr(context, name, value)
            
            # If permanent, also persist to context storage
            if permanent and hasattr(context, 'storage'):
                context.storage[name] = value
            
            return ActionResult(
                success=True,
                message=f"Set {name} = {value}",
                data={'name': name, 'value': value, 'type': type(value).__name__}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Set variable error: {e}",
                data={'error': str(e)}
            )


class VariableDeleteAction(BaseAction):
    """Delete a variable from context.
    
    Removes variables from the execution context.
    """
    action_type = "variable_delete"
    display_name = "删除变量"
    description = "从上下文中删除变量"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Delete variable.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: name, from_context.
        
        Returns:
            ActionResult with deletion status.
        """
        name = params.get('name', '')
        from_context = params.get('from_context', 'variables')
        
        if not name:
            return ActionResult(success=False, message="name is required")
        
        try:
            if from_context == 'variables':
                if hasattr(context, name):
                    delattr(context, name)
                    deleted = True
                else:
                    deleted = False
            elif from_context == 'env':
                if name in os.environ:
                    del os.environ[name]
                    deleted = True
                else:
                    deleted = False
            else:
                if hasattr(context, name):
                    delattr(context, name)
                    deleted = True
                else:
                    deleted = False
            
            return ActionResult(
                success=deleted,
                message=f"{'Deleted' if deleted else 'Not found'}: {name}",
                data={'name': name, 'deleted': deleted}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Delete variable error: {e}",
                data={'error': str(e)}
            )


class VariableTypeConvertAction(BaseAction):
    """Convert variable to a different type.
    
    Supports int, float, str, bool, list, dict conversions.
    """
    action_type = "variable_convert"
    display_name = "类型转换"
    description = "转换变量类型"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Convert variable type.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: value, target_type, encoding, delimiter.
        
        Returns:
            ActionResult with converted value.
        """
        value = params.get('value', None)
        target_type = params.get('target_type', 'str')
        encoding = params.get('encoding', 'utf-8')
        delimiter = params.get('delimiter', ',')
        
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        try:
            if target_type == 'str':
                result = str(value)
            elif target_type == 'int':
                result = int(value)
            elif target_type == 'float':
                result = float(value)
            elif target_type == 'bool':
                if isinstance(value, str):
                    result = value.lower() in ('true', '1', 'yes', 'on')
                else:
                    result = bool(value)
            elif target_type == 'list':
                if isinstance(value, str):
                    result = [item.strip() for item in value.split(delimiter)]
                elif isinstance(value, (tuple, set)):
                    result = list(value)
                else:
                    result = list(value) if hasattr(value, '__iter__') else [value]
            elif target_type == 'dict':
                import json
                if isinstance(value, str):
                    result = json.loads(value)
                elif isinstance(value, dict):
                    result = value
                else:
                    return ActionResult(
                        success=False,
                        message=f"Cannot convert {type(value).__name__} to dict"
                    )
            elif target_type == 'bytes':
                if isinstance(value, str):
                    result = value.encode(encoding)
                else:
                    result = bytes(value)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown target type: {target_type}"
                )
            
            return ActionResult(
                success=True,
                message=f"Converted to {target_type}",
                data={
                    'original_type': type(value).__name__,
                    'result_type': type(result).__name__,
                    'result': result
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Conversion error: {e}",
                data={'error': str(e), 'target_type': target_type}
            )


class VariableCopyAction(BaseAction):
    """Create a copy of a variable.
    
    Supports deep copy and shallow copy modes.
    """
    action_type = "variable_copy"
    display_name = "复制变量"
    description = "创建变量的副本"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Copy variable.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: value, deep_copy, target_name.
        
        Returns:
            ActionResult with copied value.
        """
        value = params.get('value', None)
        deep_copy = params.get('deep_copy', False)
        target_name = params.get('target_name', None)
        
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        try:
            if deep_copy:
                result = copy.deepcopy(value)
            else:
                result = copy.copy(value)
            
            # Optionally store copy in context
            if target_name and hasattr(context, target_name):
                setattr(context, target_name, result)
            
            return ActionResult(
                success=True,
                message=f"Copied ({'deep' if deep_copy else 'shallow'})",
                data={
                    'type': type(result).__name__,
                    'result': result,
                    'target_name': target_name
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Copy error: {e}",
                data={'error': str(e)}
            )
