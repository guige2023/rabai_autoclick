"""Context manager module for RabAI AutoClick.

Provides the ContextManager class for managing workflow execution state,
variables, and history tracking.
"""

import re
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union, Callable
from functools import lru_cache


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
        self._history: List[Dict[str, Any]] = []
        self._max_history: int = max_history
        self._expression_cache: Dict[str, Any] = {}
        self._snapshot_stack: List[Dict[str, Any]] = []
    
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
        old_value = self._variables.get(key)
        self._variables[key] = value
        # Clear cache when variables change
        self._expression_cache.clear()
        self._add_history({
            "action": "set",
            "key": key,
            "old_value": old_value,
            "new_value": value,
        })
    
    def delete(self, key: str) -> bool:
        """Delete a variable.
        
        Args:
            key: Variable name to delete.
            
        Returns:
            True if deleted, False if not found.
        """
        if key in self._variables:
            old_value = self._variables[key]
            del self._variables[key]
            # Clear cache when variables change
            self._expression_cache.clear()
            self._add_history({
                "action": "delete",
                "key": key,
                "old_value": old_value,
            })
            return True
        return False
    
    def clear(self) -> None:
        """Clear all variables."""
        old_vars = self._variables.copy()
        self._variables.clear()
        # Clear cache when variables change
        self._expression_cache.clear()
        self._add_history({
            "action": "clear",
            "old_variables": old_vars,
        })
    
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
        # Optimization: skip regex check for non-string types
        if not isinstance(value, str):
            if isinstance(value, dict):
                return {k: self.resolve_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [self.resolve_value(item) for item in value]
            return value
        
        return self._resolve_string(value)
    
    def _resolve_string(self, text: str) -> Any:
        """Resolve {{variable}} references in a string.
        
        Args:
            text: String with optional {{variable}} references.
            
        Returns:
            If text is a pure expression {{...}}, returns the expression result
            preserving its type (int, list, dict, bool). Otherwise returns a
            string with all variables resolved.
        """
        # Check if the entire text is a single {{...}} expression
        single_expr_pattern = r'^\s*\{\{([^}]+)\}\}\s*$'
        match = re.match(single_expr_pattern, text)
        
        if match:
            # Pure expression: return result with original type preserved
            expr = match.group(1).strip()
            return self._evaluate_expression(expr)
        
        # Mixed text with variables: resolve and return string
        def replace_var(match) -> str:
            expr = match.group(1).strip()
            result = self._evaluate_expression(expr)
            return str(result) if not isinstance(result, str) else result
        
        return re.sub(r'\{\{([^}]+)\}\}', replace_var, text)
    
    def _evaluate_expression(self, expr: str) -> Any:
        """Evaluate a simple expression using context variables.
        
        Supports:
        - Direct variable references: variable_name
        - Dot notation: obj.attr or dict.key
        - Bracket notation: obj['key'] or obj["key"]
        - Basic math and functions: int(), float(), len(), etc.
        - Expression caching for performance.
        
        Args:
            expr: Expression string to evaluate.
            
        Returns:
            Evaluated result or original string if evaluation fails.
        """
        expr = expr.strip()
        
        # Check cache first
        if expr in self._expression_cache:
            return self._expression_cache[expr]
        
        # Direct variable lookup
        if expr in self._variables:
            result = self._variables[expr]
            self._expression_cache[expr] = result
            return result
        
        # Bracket notation for dict access (e.g., obj['key'] or obj["key"])
        bracket_match = re.match(r"^([^.[]+)\[(['\"])(.+?)\2\]$", expr)
        if bracket_match:
            var_name = bracket_match.group(1)
            quote = bracket_match.group(2)
            key = bracket_match.group(3)
            obj = self._variables.get(var_name)
            if obj is not None and isinstance(obj, dict) and key in obj:
                result = obj[key]
                self._expression_cache[expr] = result
                return result
        
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
                    result = expr
                    self._expression_cache[expr] = result
                    return result
            if obj is not None:
                result = obj
                self._expression_cache[expr] = result
                return result
        
        # Safe expression evaluation with expanded builtins
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
                'bool': bool,
                'list': list,
                'dict': dict,
                'tuple': tuple,
                'range': range,
                'enumerate': enumerate,
                'zip': zip,
                'sorted': sorted,
                'any': any,
                'all': all,
                'isinstance': isinstance,
                'type': type,
            }
            allowed_names.update(self._variables)
            result = eval(expr, {"__builtins__": {}}, allowed_names)
            self._expression_cache[expr] = result
            return result
        except Exception as e:
            # Fallback: try to parse as int/float if eval failed
            try:
                # Handle expressions like "123" that eval failed on
                result = int(expr)
                self._expression_cache[expr] = result
                return result
            except ValueError:
                try:
                    result = float(expr)
                    self._expression_cache[expr] = result
                    return result
                except ValueError:
                    result = expr
                    self._expression_cache[expr] = result
                    return result
    
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
            'any': any,
            'all': all,
            'isinstance': isinstance,
            'type': type,
        }
        
        local_vars: Dict[str, Any] = {'context': self._variables}
        local_vars.update(self._variables)
        
        try:
            exec(code, {'__builtins__': allowed_builtins}, local_vars)
            
            if output_var and 'return_value' in local_vars:
                self.set(output_var, local_vars['return_value'])
            
            return local_vars.get('return_value', None)
        except Exception as e:
            self._add_history({
                "action": "exec_error",
                "code": code,
                "error": str(e),
            })
            raise
    
    def _add_history(self, entry: Dict[str, Any]) -> None:
        """Add an action to the history log.
        
        Args:
            entry: Dictionary with action details including timestamp.
        """
        timestamp = datetime.now().isoformat()
        entry["timestamp"] = timestamp
        self._history.append(entry)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]
    
    def get_history(self) -> List[Dict[str, Any]]:
        """Get a copy of the action history.
        
        Returns:
            List of history entries with timestamps.
        """
        return self._history.copy()
    
    def snapshot(self) -> None:
        """Take a snapshot of the current context state.
        
        Useful for trying expressions without permanently modifying context.
        Snapshots can be nested and restored in reverse order.
        """
        self._snapshot_stack.append({
            "variables": self._variables.copy(),
            "cache": self._expression_cache.copy(),
        })
    
    def restore(self) -> bool:
        """Restore the context to the last snapshot.
        
        Returns:
            True if restored successfully, False if no snapshot exists.
        """
        if not self._snapshot_stack:
            return False
        snapshot = self._snapshot_stack.pop()
        self._variables = snapshot["variables"]
        self._expression_cache = snapshot["cache"]
        return True
    
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
