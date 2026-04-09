"""Data Filter Action Module.

Applies configurable filter chains with predicates, transformations,
and compound filter expressions to data streams.
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


@dataclass
class FilterExpression:
    field: str
    operator: str
    value: Any
    negate: bool = False


@dataclass
class FilterConfig:
    mode: str = "all"
    case_sensitive: bool = False
    strict: bool = True


class DataFilterAction:
    """Filter and transform data with expression chains."""

    OPERATORS = {
        "eq": lambda a, b: a == b,
        "ne": lambda a, b: a != b,
        "gt": lambda a, b: a > b,
        "ge": lambda a, b: a >= b,
        "lt": lambda a, b: a < b,
        "le": lambda a, b: a <= b,
        "contains": lambda a, b: b in a,
        "startswith": lambda a, b: str(a).startswith(b),
        "endswith": lambda a, b: str(a).endswith(b),
        "matches": lambda a, b: bool(re.search(b, str(a))),
        "in": lambda a, b: a in b,
        "not_in": lambda a, b: a not in b,
        "exists": lambda a, b: a is not None,
        "type": lambda a, b: type(a).__name__ == b,
    }

    def __init__(self, config: Optional[FilterConfig] = None) -> None:
        self._config = config or FilterConfig()
        self._expressions: List[FilterExpression] = []
        self._transformers: Dict[str, Callable] = {}
        self._stats = {"total_items": 0, "filtered": 0, "passed": 0}

    def add_expression(
        self,
        field: str,
        operator: str,
        value: Any,
        negate: bool = False,
    ) -> None:
        if operator not in self.OPERATORS and operator not in (
            "and",
            "or",
            "not",
        ):
            raise ValueError(f"Unknown operator: {operator}")
        expr = FilterExpression(
            field=field, operator=operator, value=value, negate=negate
        )
        self._expressions.append(expr)

    def add_transformer(self, field: str, fn: Callable[[Any], Any]) -> None:
        self._transformers[field] = fn

    def filter(self, data: List[Dict]) -> List[Dict]:
        self._stats["total_items"] += len(data)
        passed = []
        for item in data:
            if self._evaluate_item(item):
                passed.append(item)
                self._stats["filtered"] += 1
        self._stats["passed"] += len(passed)
        return passed

    def filter_one(self, data: Dict) -> bool:
        return self._evaluate_item(data)

    def _evaluate_item(self, item: Dict) -> bool:
        if not self._expressions:
            return True
        if self._config.mode == "all":
            return all(self._evaluate_expr(e, item) for e in self._expressions)
        elif self._config.mode == "any":
            return any(self._evaluate_expr(e, item) for e in self._expressions)
        return True

    def _evaluate_expr(self, expr: FilterExpression, item: Dict) -> bool:
        raw_value = item.get(expr.field)
        if expr.field in self._transformers:
            try:
                raw_value = self._transformers[expr.field](raw_value)
            except Exception:
                return False
        op_fn = self.OPERATORS.get(expr.operator)
        if not op_fn:
            return True
        try:
            result = op_fn(raw_value, expr.value)
            return not result if expr.negate else result
        except Exception:
            if self._config.strict:
                return False
            return True

    def clear_expressions(self) -> None:
        self._expressions.clear()

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._stats,
            "pass_rate": self._stats["filtered"] / self._stats["total_items"]
            if self._stats["total_items"] > 0
            else 0,
        }

    def parse_filter_string(self, filter_str: str) -> None:
        tokens = re.findall(r'(\w+)(==|!=|>=|<=|>|<|contains|matches|in)\s*([^,\s]+)', filter_str)
        for field, op, value in tokens:
            try:
                if value.replace(".", "").replace("-", "").isdigit():
                    parsed_value = float(value) if "." in value else int(value)
                elif value in ("true", "false"):
                    parsed_value = value == "true"
                elif value.startswith("[") and value.endswith("]"):
                    parsed_value = [v.strip() for v in value[1:-1].split(",")]
                else:
                    parsed_value = value.strip('"\'')
                self.add_expression(field, op, parsed_value)
            except Exception as e:
                logger.error(f"Failed to parse filter token: {field} {op} {value}: {e}")
