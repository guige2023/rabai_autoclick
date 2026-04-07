"""Context manager module for RabAI AutoClick.

Provides the ContextManager class for managing workflow execution state,
variables, and history tracking.
"""

import re
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union


class ContextManager:
    """Manages workflow execution context including variables and history.
    
    Supports variable resolution with {{variable}} syntax, expression
    evaluation, and safe code execution.
    """
    
    def __init__(self, max_history: int = 100) -> None:
        """Initialize the context manager.
        
        Args:
            max_history: Maximum number of history entries to retain.
        """
        self._variables: Dict[str, Any] = {}
        self._history: List[str] = []
        self._max_history: int = max_history
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a variable value.
        
        Args:
            key: Variable name.
            default: Default value if variable not found.
            
        Returns:
            Variable value or default.
        """
        return self._variables.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set a variable value.
        
        Args:
            key: Variable name.
            value: Value to set.
        """
        self._variables[key] = value
        self._add_history(f"SET {key} = {repr(value)[:100]}")
    
    def delete(self, key: str) -> bool:
        """Delete a variable.
        
        Args:
            key: Variable name to delete.
            
        Returns:
            True if deleted, False if not found.
        """
        if key in self._variables:
            del self._variables[key]
            self._add_history(f"DELETE {key}")
            return True
        return False
    
    def clear(self) -> None:
        """Clear all variables."""
        self._variables.clear()
        self._add_history("CLEAR ALL")
    
    def get_all(self) -> Dict[str, Any]:
        """Get a copy of all variables.
        
        Returns:
            Dictionary of all variables.
        """
        return self._variables.copy()
    
    def set_all(self, variables: Dict[str, Any]) -> None:
        """Update multiple variables at once.
        
        Args:
            variables: Dictionary of variables to update.
        """
        self._variables.update(variables)
    
    def resolve_value(self, value: Any) -> Any:
        """Resolve variable references in a value.
        
        Recursively resolves {{variable}} references in strings,
        and processes dict/list values.
        
        Args:
            value: Value to resolve.
            
        Returns:
            Value with variables resolved.
        """
        if isinstance(value, str):
            return self._resolve_string(value)
        elif isinstance(value, dict):
            return {k: self.resolve_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self.resolve_value(item) for item in value]
        return value
    
    def _resolve_string(self, text: str) -> str:
        """Resolve {{variable}} references in a string.
        
        Args:
            text: String with optional {{variable}} references.
            
        Returns:
            String with variables resolved.
        """
        pattern = r'\{\{([^}]+)\}\}'
        
        def replace_var(match) -> str:
            expr = match.group(1).strip()
            return str(self._evaluate_expression(expr))
        
        return re.sub(pattern, replace_var, text)
    
    def _evaluate_expression(self, expr: str) -> Any:
        """Evaluate a simple expression using context variables.
        
        Supports:
        - Direct variable references: variable_name
        - Dot notation: obj.attr or dict.key
        - Basic math and functions: int(), float(), len(), etc.
        
        Args:
            expr: Expression string to evaluate.
            
        Returns:
            Evaluated result or original string if evaluation fails.
        """
        expr = expr.strip()
        
        # Direct variable lookup
        if expr in self._variables:
            return self._variables[expr]
        
        # Dot notation for nested access
        if '.' in expr:
            parts = expr.split('.')
            obj = self._variables.get(parts[0])
            for part in parts[1:]:
                if isinstance(obj, dict):
                    obj = obj.get(part)
                elif hasattr(obj, part):
                    obj = getattr(obj, part)
                else:
                    return expr
            return obj if obj is not None else expr
        
        # Safe expression evaluation
        try:
            allowed_names: Dict[str, Any] = {
                'context': self._variables,
                'int': int,
                'float': float,
                'str': str,
                'len': len,
                'abs': abs,
                'min': min,
                'max': max,
                'sum': sum,
                'round': round,
            }
            allowed_names.update(self._variables)
            result = eval(expr, {"__builtins__": {}}, allowed_names)
            return result
        except Exception:
            return expr
    
    def safe_exec(
        self, 
        code: str, 
        output_var: Optional[str] = None
    ) -> Any:
        """Execute Python code in a sandboxed environment.
        
        Only allows specific builtins and variables from context.
        Sets 'return_value' in local scope for output.
        
        Args:
            code: Python code to execute.
            output_var: Optional variable name to store return_value.
            
        Returns:
            The 'return_value' from execution if set.
            
        Raises:
            Exception: If execution fails.
        """
        allowed_builtins: Dict[str, Any] = {
            'int': int,
            'float': float,
            'str': str,
            'len': len,
            'abs': abs,
            'min': min,
            'max': max,
            'sum': sum,
            'round': round,
            'list': list,
            'dict': dict,
            'tuple': tuple,
            'set': set,
            'bool': bool,
            'range': range,
            'enumerate': enumerate,
            'zip': zip,
            'sorted': sorted,
            'reversed': reversed,
            'print': lambda *args: None,
        }
        
        local_vars: Dict[str, Any] = {'context': self._variables}
        local_vars.update(self._variables)
        
        try:
            exec(code, {'__builtins__': allowed_builtins}, local_vars)
            
            if output_var and 'return_value' in local_vars:
                self.set(output_var, local_vars['return_value'])
            
            return local_vars.get('return_value', None)
        except Exception as e:
            self._add_history(f"EXEC ERROR: {str(e)}")
            raise
    
    def _add_history(self, action: str) -> None:
        """Add an action to the history log.
        
        Args:
            action: Description of the action.
        """
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        self._history.append(f"[{timestamp}] {action}")
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]
    
    def get_history(self) -> List[str]:
        """Get a copy of the action history.
        
        Returns:
            List of history entries.
        """
        return self._history.copy()
    
    def to_json(self) -> str:
        """Export variables as JSON string.
        
        Returns:
            JSON representation of all variables.
        """
        return json.dumps(
            self._variables, 
            ensure_ascii=False, 
            indent=2, 
            default=str
        )
    
    def from_json(self, json_str: str) -> None:
        """Import variables from JSON string.

        Args:
            json_str: JSON string to parse and import.
        """
        self._variables.update(json.loads(json_str))

    def has(self, key: str) -> bool:
        """Check if a variable exists in context.

        Args:
            key: Variable name to check.

        Returns:
            True if variable exists, False otherwise.
        """
        return key in self._variables

    def keys(self) -> List[str]:
        """Get list of all variable names.

        Returns:
            List of variable names.
        """
        return list(self._variables.keys())

    def values(self) -> List[Any]:
        """Get list of all variable values.

        Returns:
            List of variable values.
        """
        return list(self._variables.values())

    def items(self) -> List[Tuple[str, Any]]:
        """Get list of all variable name-value pairs.

        Returns:
            List of (name, value) tuples.
        """
        return list(self._variables.items())

    def update(self, variables: Dict[str, Any]) -> None:
        """Update multiple variables at once (alias for set_all).

        Args:
            variables: Dictionary of variables to update.
        """
        self._variables.update(variables)

    def pop(self, key: str, default: Any = None) -> Any:
        """Remove and return a variable value.

        Args:
            key: Variable name to pop.
            default: Default value if key not found.

        Returns:
            Variable value or default.
        """
        value = self._variables.pop(key, default)
        self._add_history(f"POP {key} = {repr(value)[:100]}")
        return value

    def copy(self) -> Dict[str, Any]:
        """Get a shallow copy of all variables.

        Returns:
            Dictionary copy of all variables.
        """
        return self._variables.copy()

    def merge(self, other: Dict[str, Any], overwrite: bool = True) -> None:
        """Merge another dictionary into context.

        Args:
            other: Dictionary to merge.
            overwrite: If True, overwrite existing variables.
        """
        if overwrite:
            self._variables.update(other)
        else:
            for key, value in other.items():
                if key not in self._variables:
                    self._variables[key] = value
        self._add_history(f"MERGE {len(other)} variables (overwrite={overwrite})")

    def to_dict(self) -> Dict[str, Any]:
        """Export variables as a dictionary.

        Returns:
            Dictionary representation of all variables.
        """
        return self._variables.copy()

    def get_nested(self, key_path: str, default: Any = None) -> Any:
        """Get a nested value using dot notation.

        Args:
            key_path: Dot-separated path (e.g., 'user.profile.name').
            default: Default value if path not found.

        Returns:
            Nested value or default.
        """
        keys = key_path.split('.')
        value = self._variables

        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            elif hasattr(value, key):
                value = getattr(value, key)
            else:
                return default

            if value is None:
                return default

        return value

    def set_nested(self, key_path: str, value: Any) -> bool:
        """Set a nested value using dot notation.

        Args:
            key_path: Dot-separated path (e.g., 'user.profile.name').
            value: Value to set.

        Returns:
            True if successful, False otherwise.
        """
        keys = key_path.split('.')
        current = self._variables

        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        current[keys[-1]] = value
        self._add_history(f"SET_NESTED {key_path} = {repr(value)[:100]}")
        return True

    def size(self) -> int:
        """Get the number of variables.

        Returns:
            Number of variables in context.
        """
        return len(self._variables)

    def is_empty(self) -> bool:
        """Check if context is empty.

        Returns:
            True if no variables, False otherwise.
        """
        return len(self._variables) == 0
