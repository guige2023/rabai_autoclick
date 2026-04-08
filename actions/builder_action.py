"""Builder action module for RabAI AutoClick.

Provides builder pattern implementation:
- Builder: Abstract builder interface
- ConcreteBuilder: Specific builder implementations
- Director: Director for construction process
- ObjectBuilder: Generic object builder
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


T = TypeVar("T")


@dataclass
class BuildStep:
    """Single build step."""
    name: str
    fn: Callable[[Any], Any]
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)


class Builder(ABC, Generic[T]):
    """Abstract builder interface."""

    @abstractmethod
    def build(self) -> T:
        """Build the object."""
        pass

    def reset(self) -> "Builder":
        """Reset the builder."""
        return self


class ObjectBuilder(Builder[Dict]):
    """Generic object builder."""

    def __init__(self):
        self._data: Dict[str, Any] = {}
        self._steps: List[BuildStep] = []

    def set(self, key: str, value: Any) -> "ObjectBuilder":
        """Set a field."""
        self._data[key] = value
        self._steps.append(BuildStep(name=f"set_{key}", fn=lambda x: x, args=(value,), kwargs={"key": key}))
        return self

    def set_if(self, condition: bool, key: str, value: Any) -> "ObjectBuilder":
        """Set a field conditionally."""
        if condition:
            return self.set(key, value)
        return self

    def update(self, data: Dict) -> "ObjectBuilder":
        """Update multiple fields."""
        self._data.update(data)
        return self

    def remove(self, key: str) -> "ObjectBuilder":
        """Remove a field."""
        if key in self._data:
            del self._data[key]
        return self

    def apply(self, fn: Callable[[Dict], Dict]) -> "ObjectBuilder":
        """Apply a transformation function."""
        self._data = fn(self._data)
        self._steps.append(BuildStep(name="apply", fn=fn))
        return self

    def build(self) -> Dict:
        """Build the object."""
        return self._data.copy()

    def reset(self) -> "ObjectBuilder":
        """Reset the builder."""
        self._data = {}
        self._steps = []
        return self

    def get_steps(self) -> List[str]:
        """Get build steps."""
        return [step.name for step in self._steps]


class ConfigBuilder(ObjectBuilder):
    """Configuration object builder."""

    def __init__(self):
        super().__init__()
        self._required_fields: Set[str] = set()
        self._optional_fields: Set[str] = set()
        self._field_validators: Dict[str, Callable] = {}

    def require(self, key: str) -> "ConfigBuilder":
        """Mark field as required."""
        self._required_fields.add(key)
        return self

    def optional(self, key: str, default: Any = None) -> "ConfigBuilder":
        """Mark field as optional with default."""
        self._optional_fields.add(key)
        if key not in self._data:
            self._data[key] = default
        return self

    def validate_with(self, key: str, validator: Callable[[Any], bool]) -> "ConfigBuilder":
        """Add validator for field."""
        self._field_validators[key] = validator
        return self

    def build(self) -> Dict:
        """Build with validation."""
        for key in self._required_fields:
            if key not in self._data:
                raise ValueError(f"Required field missing: {key}")

        for key, validator in self._field_validators.items():
            if key in self._data:
                if not validator(self._data[key]):
                    raise ValueError(f"Validation failed for field: {key}")

        return super().build()


class QueryBuilder:
    """SQL-like query builder."""

    def __init__(self):
        self._table: str = ""
        self._fields: List[str] = []
        self._conditions: List[str] = []
        self._order_by: List[str] = []
        self._limit_val: Optional[int] = None
        self._offset_val: Optional[int] = None
        self._joins: List[str] = []
        self._group_by: List[str] = []

    def select(self, *fields) -> "QueryBuilder":
        """Add SELECT fields."""
        self._fields = list(fields) if fields else ["*"]
        return self

    def from_table(self, table: str) -> "QueryBuilder":
        """Set FROM table."""
        self._table = table
        return self

    def where(self, condition: str) -> "QueryBuilder":
        """Add WHERE condition."""
        self._conditions.append(condition)
        return self

    def order(self, field: str, direction: str = "ASC") -> "QueryBuilder":
        """Add ORDER BY."""
        self._order_by.append(f"{field} {direction}")
        return self

    def limit(self, count: int) -> "QueryBuilder":
        """Set LIMIT."""
        self._limit_val = count
        return self

    def offset(self, count: int) -> "QueryBuilder":
        """Set OFFSET."""
        self._offset_val = count
        return self

    def join(self, table: str, on: str) -> "QueryBuilder":
        """Add JOIN."""
        self._joins.append(f"JOIN {table} ON {on}")
        return self

    def group(self, *fields) -> "QueryBuilder":
        """Add GROUP BY."""
        self._group_by = list(fields)
        return self

    def build(self) -> str:
        """Build SQL query."""
        parts = []

        fields_str = ", ".join(self._fields) if self._fields else "*"
        parts.append(f"SELECT {fields_str}")

        if self._table:
            parts.append(f"FROM {self._table}")

        if self._joins:
            parts.extend(self._joins)

        if self._conditions:
            parts.append(f"WHERE {' AND '.join(self._conditions)}")

        if self._group_by:
            parts.append(f"GROUP BY {', '.join(self._group_by)}")

        if self._order_by:
            parts.append(f"ORDER BY {', '.join(self._order_by)}")

        if self._limit_val is not None:
            parts.append(f"LIMIT {self._limit_val}")

        if self._offset_val is not None:
            parts.append(f"OFFSET {self._offset_val}")

        return " ".join(parts)


class RequestBuilder:
    """HTTP request builder."""

    def __init__(self):
        self._method: str = "GET"
        self._url: str = ""
        self._headers: Dict[str, str] = {}
        self._body: Any = None
        self._params: Dict[str, str] = {}
        self._timeout: Optional[float] = None
        self._auth: Optional[Dict[str, str]] = None

    def method(self, m: str) -> "RequestBuilder":
        """Set HTTP method."""
        self._method = m.upper()
        return self

    def url(self, url: str) -> "RequestBuilder":
        """Set URL."""
        self._url = url
        return self

    def header(self, key: str, value: str) -> "RequestBuilder":
        """Add header."""
        self._headers[key] = value
        return self

    def headers(self, headers: Dict[str, str]) -> "RequestBuilder":
        """Set headers."""
        self._headers.update(headers)
        return self

    def body(self, data: Any) -> "RequestBuilder":
        """Set body."""
        self._body = data
        return self

    def param(self, key: str, value: str) -> "RequestBuilder":
        """Add query param."""
        self._params[key] = value
        return self

    def timeout(self, seconds: float) -> "RequestBuilder":
        """Set timeout."""
        self._timeout = seconds
        return self

    def auth(self, username: str, password: str) -> "RequestBuilder":
        """Set auth."""
        self._auth = {"username": username, "password": password}
        return self

    def build(self) -> Dict[str, Any]:
        """Build request dict."""
        return {
            "method": self._method,
            "url": self._url,
            "headers": self._headers,
            "body": self._body,
            "params": self._params,
            "timeout": self._timeout,
            "auth": self._auth,
        }


class Director:
    """Director for construction process."""

    def __init__(self, builder: Optional[Builder] = None):
        self._builder = builder

    def set_builder(self, builder: Builder) -> None:
        """Set the builder."""
        self._builder = builder

    def construct(self, *args, **kwargs) -> Any:
        """Construct using builder."""
        if self._builder is None:
            raise RuntimeError("No builder set")
        return self._builder.build()


class BuilderAction(BaseAction):
    """Builder pattern action."""
    action_type = "builder"
    display_name = "建造者模式"
    description = "对象构建器"

    def __init__(self):
        super().__init__()
        self._builders: Dict[str, ObjectBuilder] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "build")

            if operation == "build":
                return self._build_object(params)
            elif operation == "query":
                return self._build_query(params)
            elif operation == "request":
                return self._build_request(params)
            elif operation == "config":
                return self._build_config(params)
            elif operation == "reset":
                return self._reset_builder(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Builder error: {str(e)}")

    def _build_object(self, params: Dict[str, Any]) -> ActionResult:
        """Build an object."""
        builder_id = params.get("builder_id", "default")
        fields = params.get("fields", {})

        if builder_id not in self._builders:
            self._builders[builder_id] = ObjectBuilder()

        builder = self._builders[builder_id]
        builder.update(fields)

        result = builder.build()

        return ActionResult(success=True, message=f"Object built with {len(result)} fields", data={"object": result})

    def _build_query(self, params: Dict[str, Any]) -> ActionResult:
        """Build a query."""
        qb = QueryBuilder()

        if "select" in params:
            qb.select(*params["select"].split(","))
        if "table" in params:
            qb.from_table(params["table"])
        if "where" in params:
            qb.where(params["where"])
        if "order" in params:
            qb.order(params["order"], params.get("direction", "ASC"))
        if "limit" in params:
            qb.limit(params["limit"])
        if "offset" in params:
            qb.offset(params["offset"])

        query = qb.build()

        return ActionResult(success=True, message="Query built", data={"query": query})

    def _build_request(self, params: Dict[str, Any]) -> ActionResult:
        """Build an HTTP request."""
        rb = RequestBuilder()

        if "method" in params:
            rb.method(params["method"])
        if "url" in params:
            rb.url(params["url"])
        if "headers" in params:
            rb.headers(params["headers"])
        if "body" in params:
            rb.body(params["body"])
        if "timeout" in params:
            rb.timeout(params["timeout"])

        request = rb.build()

        return ActionResult(success=True, message="Request built", data={"request": request})

    def _build_config(self, params: Dict[str, Any]) -> ActionResult:
        """Build configuration."""
        required = params.get("required", [])
        optional = params.get("optional", {})
        values = params.get("values", {})

        cb = ConfigBuilder()

        for key in required:
            cb.require(key)

        for key, default in optional.items():
            cb.optional(key, default)

        cb.update(values)

        try:
            config = cb.build()
            return ActionResult(success=True, message="Config built", data={"config": config})
        except ValueError as e:
            return ActionResult(success=False, message=str(e))

    def _reset_builder(self, params: Dict[str, Any]) -> ActionResult:
        """Reset a builder."""
        builder_id = params.get("builder_id", "default")

        if builder_id in self._builders:
            self._builders[builder_id].reset()
            return ActionResult(success=True, message=f"Builder reset: {builder_id}")
        return ActionResult(success=False, message=f"Builder not found: {builder_id}")
