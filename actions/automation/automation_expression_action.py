"""Automation Expression Action Module.

Provides expression evaluation engine for automation workflows
including arithmetic, logical, and string expressions with
function support and context binding.

Example:
    >>> from actions.automation.automation_expression_action import ExpressionEngine
    >>> engine = ExpressionEngine()
    >>> result = engine.evaluate("x + y > 10", context={"x": 5, "y": 7})
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
import operator
import re
import threading


class ExpressionType(Enum):
    """Expression type classifications."""
    ARITHMETIC = "arithmetic"
    LOGICAL = "logical"
    STRING = "string"
    COMPARISON = "comparison"
    FUNCTION = "function"
    TERNARY = "ternary"


@dataclass
class ExpressionResult:
    """Expression evaluation result.
    
    Attributes:
        value: Computed value
        error: Error message if failed
        evaluation_time: Time taken to evaluate
        expression_type: Type of expression
    """
    value: Any = None
    error: Optional[str] = None
    evaluation_time: float = 0.0
    expression_type: ExpressionType = ExpressionType.ARITHMETIC


@dataclass
class FunctionDefinition:
    """User-defined function definition.
    
    Attributes:
        name: Function name
        params: Parameter names
        body: Expression body
        description: Function description
    """
    name: str
    params: List[str]
    body: str
    description: str = ""


class ExpressionEngine:
    """Expression evaluation engine for automation.
    
    Supports arithmetic, logical, comparison, and string
    operations with user-defined functions and context binding.
    
    Attributes:
        _functions: Registered user functions
        _variables: Global variables
        _operators: Operator mappings
        _lock: Thread safety lock
    """
    
    # Supported operators
    OPERATORS = {
        "+": operator.add,
        "-": operator.sub,
        "*": operator.mul,
        "/": operator.truediv,
        "//": operator.floordiv,
        "%": operator.mod,
        "**": operator.pow,
        "==": operator.eq,
        "!=": operator.ne,
        "<": operator.lt,
        ">": operator.gt,
        "<=": operator.le,
        ">=": operator.ge,
        "and": lambda a, b: bool(a) and bool(b),
        "or": lambda a, b: bool(a) or bool(b),
        "not": lambda x: not bool(x),
        "in": lambda a, b: a in b,
        "not in": lambda a, b: a not in b,
    }
    
    def __init__(self) -> None:
        """Initialize expression engine."""
        self._functions: Dict[str, FunctionDefinition] = {}
        self._variables: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._register_builtin_functions()
    
    def _register_builtin_functions(self) -> None:
        """Register built-in functions."""
        # String functions
        self._functions["len"] = FunctionDefinition(
            name="len",
            params=["s"],
            body="len(s)",
            description="Get length of string or list",
        )
        self._functions["lower"] = FunctionDefinition(
            name="lower",
            params=["s"],
            body="s.lower()",
            description="Convert string to lowercase",
        )
        self._functions["upper"] = FunctionDefinition(
            name="upper",
            params=["s"],
            body="s.upper()",
            description="Convert string to uppercase",
        )
        self._functions["trim"] = FunctionDefinition(
            name="trim",
            params=["s"],
            body="s.strip()",
            description="Trim whitespace from string",
        )
        self._functions["replace"] = FunctionDefinition(
            name="replace",
            params=["s", "old", "new"],
            body="s.replace(old, new)",
            description="Replace substring",
        )
        
        # Type conversion
        self._functions["int"] = FunctionDefinition(
            name="int",
            params=["x"],
            body="int(x)",
            description="Convert to integer",
        )
        self._functions["str"] = FunctionDefinition(
            name="str",
            params=["x"],
            body="str(x)",
            description="Convert to string",
        )
        self._functions["float"] = FunctionDefinition(
            name="float",
            params=["x"],
            body="float(x)",
            description="Convert to float",
        )
        self._functions["bool"] = FunctionDefinition(
            name="bool",
            params=["x"],
            body="bool(x)",
            description="Convert to boolean",
        )
        
        # Math functions
        self._functions["abs"] = FunctionDefinition(
            name="abs",
            params=["x"],
            body="abs(x)",
            description="Absolute value",
        )
        self._functions["min"] = FunctionDefinition(
            name="min",
            params=["a", "b"],
            body="min(a, b)",
            description="Minimum of two values",
        )
        self._functions["max"] = FunctionDefinition(
            name="max",
            params=["a", "b"],
            body="max(a, b)",
            description="Maximum of two values",
        )
        self._functions["round"] = FunctionDefinition(
            name="round",
            params=["x", "n"],
            body="round(x, n)",
            description="Round to n decimal places",
        )
        
        # Collection functions
        self._functions["contains"] = FunctionDefinition(
            name="contains",
            params=["coll", "item"],
            body="item in coll",
            description="Check if item in collection",
        )
        self._functions["keys"] = FunctionDefinition(
            name="keys",
            params=["d"],
            body="list(d.keys())",
            description="Get dictionary keys",
        )
        self._functions["values"] = FunctionDefinition(
            name="values",
            params=["d"],
            body="list(d.values())",
            description="Get dictionary values",
        )
    
    def register_function(self, func: FunctionDefinition) -> None:
        """Register a user-defined function.
        
        Args:
            func: Function definition to register
        """
        with self._lock:
            self._functions[func.name] = func
    
    def set_variable(self, name: str, value: Any) -> None:
        """Set a global variable.
        
        Args:
            name: Variable name
            value: Variable value
        """
        with self._lock:
            self._variables[name] = value
    
    def clear_variables(self) -> None:
        """Clear all global variables."""
        with self._lock:
            self._variables.clear()
    
    def evaluate(
        self,
        expression: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> ExpressionResult:
        """Evaluate an expression.
        
        Args:
            expression: Expression string
            context: Variable context
            
        Returns:
            Evaluation result
        """
        start_time = datetime.now()
        context = context or {}
        
        try:
            # Determine expression type
            expr_type = self._classify_expression(expression)
            
            # Combine contexts
            eval_context = {**self._variables, **context}
            
            # Replace functions in expression
            expr = self._expand_functions(expression)
            
            # Replace variables
            expr = self._substitute_variables(expr, eval_context)
            
            # Evaluate
            value = self._safe_eval(expr)
            
            eval_time = (datetime.now() - start_time).total_seconds()
            return ExpressionResult(
                value=value,
                evaluation_time=eval_time,
                expression_type=expr_type,
            )
        
        except Exception as e:
            eval_time = (datetime.now() - start_time).total_seconds()
            return ExpressionResult(
                error=str(e),
                evaluation_time=eval_time,
            )
    
    def _classify_expression(self, expression: str) -> ExpressionType:
        """Classify expression type.
        
        Args:
            expression: Expression string
            
        Returns:
            Expression type
        """
        expr = expression.strip()
        
        if any(op in expr for op in ["==", "!=", "<", ">", "<=", ">="]):
            return ExpressionType.COMPARISON
        if any(op in expr for op in ["and", "or", "not"]):
            return ExpressionType.LOGICAL
        if re.search(r'\b\w+\s*\(', expr):
            return ExpressionType.FUNCTION
        if "?" in expr and ":" in expr:
            return ExpressionType.TERNARY
        if any(op in expr for op in ["+", "-", "*", "/", "//", "%", "**"]):
            return ExpressionType.ARITHMETIC
        return ExpressionType.STRING
    
    def _expand_functions(self, expression: str) -> str:
        """Expand function calls in expression.
        
        Args:
            expression: Expression string
            
        Returns:
            Expanded expression
        """
        def replace_function(match):
            func_name = match.group(1)
            args_str = match.group(2)
            
            if func_name in self._functions:
                func = self._functions[func_name]
                args = [a.strip() for a in args_str.split(",")]
                params = func.params
                if len(args) < len(params):
                    args.extend(["None"] * (len(params) - len(args)))
                result = "(" + func.body
                for p, a in zip(params, args):
                    result = result.replace(p, a)
                return result + ")"
            return match.group(0)
        
        return re.sub(r'(\w+)\s*\(([^)]*)\)', replace_function, expression)
    
    def _substitute_variables(
        self,
        expression: str,
        context: Dict[str, Any],
    ) -> str:
        """Substitute variables in expression.
        
        Args:
            expression: Expression string
            context: Variable context
            
        Returns:
            Substituted expression
        """
        def replace_var(match):
            var_name = match.group(1)
            if var_name in context:
                value = context[var_name]
                if isinstance(value, str):
                    return f'"{value}"'
                return str(value)
            return match.group(0)
        
        # Replace identifiers that aren't keywords
        pattern = r'\b([a-zA-Z_]\w*)\b'
        result = re.sub(pattern, replace_var, expression)
        
        # Don't replace Python keywords
        keywords = {"True", "False", "None", "and", "or", "not", "in"}
        for kw in keywords:
            result = result.replace(f'"{kw}"', kw)
        
        return result
    
    def _safe_eval(self, expression: str) -> Any:
        """Safely evaluate expression.
        
        Args:
            expression: Expression string
            
        Returns:
            Evaluated value
        """
        # Build safe globals
        safe_globals = {
            "__builtins__": {},
            "True": True,
            "False": False,
            "None": None,
        }
        
        return eval(expression, safe_globals, {})
    
    def validate(self, expression: str) -> Dict[str, Any]:
        """Validate an expression without evaluating.
        
        Args:
            expression: Expression string
            
        Returns:
            Validation result with any errors found
        """
        errors: List[str] = []
        
        # Check for balanced parentheses
        if expression.count("(") != expression.count(")"):
            errors.append("Unbalanced parentheses")
        
        # Check for undefined functions
        func_pattern = r'(\w+)\s*\('
        for match in re.finditer(func_pattern, expression):
            func_name = match.group(1)
            if func_name not in self._functions and func_name not in dir(__builtins__):
                errors.append(f"Undefined function: {func_name}")
        
        # Check for syntax errors
        try:
            expanded = self._expand_functions(expression)
            self._substitute_variables(expanded, {})
            self._safe_eval(expanded)
        except Exception as e:
            errors.append(f"Syntax error: {str(e)}")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "type": self._classify_expression(expression),
        }
