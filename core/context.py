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
            # Fallback: try to parse as int/float if eval failed
            try:
                # Handle expressions like "123" that eval failed on
                return int(expr)
            except ValueError:
                try:
                    return float(expr)
                except ValueError:
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
