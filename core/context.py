import re
import json
from typing import Any, Dict, Optional
from datetime import datetime


class ContextManager:
    def __init__(self):
        self._variables: Dict[str, Any] = {}
        self._history: list = []
        self._max_history = 100

    def get(self, key: str, default: Any = None) -> Any:
        return self._variables.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._variables[key] = value
        self._add_history(f"SET {key} = {repr(value)[:100]}")

    def delete(self, key: str) -> bool:
        if key in self._variables:
            del self._variables[key]
            self._add_history(f"DELETE {key}")
            return True
        return False

    def clear(self) -> None:
        self._variables.clear()
        self._add_history("CLEAR ALL")

    def get_all(self) -> Dict[str, Any]:
        return self._variables.copy()

    def set_all(self, variables: Dict[str, Any]) -> None:
        self._variables.update(variables)

    def resolve_value(self, value: Any) -> Any:
        if isinstance(value, str):
            return self._resolve_string(value)
        elif isinstance(value, dict):
            return {k: self.resolve_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self.resolve_value(item) for item in value]
        return value

    def _resolve_string(self, text: str) -> str:
        pattern = r'\{\{([^}]+)\}\}'
        
        def replace_var(match):
            expr = match.group(1).strip()
            return str(self._evaluate_expression(expr))
        
        return re.sub(pattern, replace_var, text)

    def _evaluate_expression(self, expr: str) -> Any:
        expr = expr.strip()
        
        if expr in self._variables:
            return self._variables[expr]
        
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
        
        try:
            allowed_names = {
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

    def safe_exec(self, code: str, output_var: Optional[str] = None) -> Any:
        allowed_builtins = {
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
        
        local_vars = {'context': self._variables}
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
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        self._history.append(f"[{timestamp}] {action}")
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

    def get_history(self) -> list:
        return self._history.copy()

    def to_json(self) -> str:
        return json.dumps(self._variables, ensure_ascii=False, indent=2, default=str)

    def from_json(self, json_str: str) -> None:
        self._variables.update(json.loads(json_str))
