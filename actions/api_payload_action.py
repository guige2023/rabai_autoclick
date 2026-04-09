"""
API payload builder module.

Provides fluent builders for constructing complex API request
payloads with validation and transformation support.

Author: Aito Auto Agent
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional, Union
import json


class PayloadType(Enum):
    """Payload type enumeration."""
    JSON = auto()
    FORM = auto()
    MULTIPART = auto()
    XML = auto()
    GRAPHQL = auto()


@dataclass
class FieldSchema:
    """Schema definition for a payload field."""
    name: str
    field_type: type
    required: bool = False
    default: Any = None
    description: str = ""
    validator: Optional[Callable[[Any], bool]] = None
    transformer: Optional[Callable[[Any], Any]] = None


@dataclass
class ValidationError:
    """Validation error details."""
    field: str
    message: str
    value: Any = None


class PayloadBuilder:
    """
    Fluent builder for API request payloads.

    Example:
        payload = (
            PayloadBuilder()
            .field("name", "John Doe", required=True)
            .field("email", "john@example.com", validator=is_valid_email)
            .field("age", 30, validator=lambda x: 0 <= x <= 150)
            .nested("address")
                .field("city", "New York")
                .field("country", "USA")
                .end()
            .build()
        )
    """

    def __init__(self, payload_type: PayloadType = PayloadType.JSON):
        self._payload_type = payload_type
        self._data: dict[str, Any] = {}
        self._errors: list[ValidationError] = []
        self._schema: dict[str, FieldSchema] = {}
        self._current_path: list[str] = []

    def field(
        self,
        name: str,
        value: Any,
        required: bool = False,
        validator: Optional[Callable[[Any], bool]] = None,
        transformer: Optional[Callable[[Any], Any]] = None
    ) -> PayloadBuilder:
        """
        Add a field to the payload.

        Args:
            name: Field name
            value: Field value
            required: Whether field is required
            validator: Optional validation function
            transformer: Optional transformation function

        Returns:
            Self for chaining
        """
        schema = FieldSchema(
            name=name,
            field_type=type(value),
            required=required,
            validator=validator
        )
        self._schema[name] = schema

        if transformer:
            value = transformer(value)

        if name in self._data and isinstance(self._data[name], dict):
            self._data[name].update(value)
        else:
            self._set_nested(name, value)

        return self

    def _set_nested(self, name: str, value: Any) -> None:
        """Set value at nested path."""
        if not self._current_path:
            self._data[name] = value
        else:
            target = self._data
            for key in self._current_path[:-1]:
                if key not in target:
                    target[key] = {}
                target = target[key]

            if self._current_path[-1] not in target:
                target[self._current_path[-1]] = {}

            target[self._current_path[-1]][name] = value

    def nested(self, name: str) -> PayloadBuilder:
        """
        Create a nested object.

        Args:
            name: Name of nested object

        Returns:
            Self for chaining
        """
        if not self._current_path:
            if name not in self._data:
                self._data[name] = {}
        else:
            target = self._data
            for key in self._current_path:
                if key not in target:
                    target[key] = {}
                target = target[key]

            if name not in target:
                target[name] = {}

        self._current_path.append(name)
        return self

    def end(self) -> PayloadBuilder:
        """
        Exit from nested context.

        Returns:
            Self for chaining
        """
        if self._current_path:
            self._current_path.pop()
        return self

    def array(self, name: str) -> ArrayBuilder:
        """
        Create a nested array.

        Args:
            name: Name of array field

        Returns:
            ArrayBuilder for adding items
        """
        return ArrayBuilder(self, name)

    def validate(self) -> list[ValidationError]:
        """
        Validate the payload.

        Returns:
            List of validation errors
        """
        errors = []

        for name, schema in self._schema.items():
            if schema.required and name not in self._flatten_data():
                errors.append(ValidationError(
                    field=name,
                    message=f"Required field '{name}' is missing"
                ))
                continue

            value = self._get_value(name)
            if value is not None and schema.validator:
                if not schema.validator(value):
                    errors.append(ValidationError(
                        field=name,
                        message=f"Field '{name}' failed validation",
                        value=value
                    ))

        self._errors = errors
        return errors

    def _flatten_data(self) -> dict[str, Any]:
        """Flatten nested data structure."""
        result = {}

        def flatten(obj: Any, prefix: str = ""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{prefix}.{key}" if prefix else key
                    flatten(value, new_key)
            else:
                result[prefix] = obj

        flatten(self._data)
        return result

    def _get_value(self, name: str) -> Any:
        """Get value by dotted path."""
        parts = name.split(".")
        value = self._data

        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None

        return value

    def is_valid(self) -> bool:
        """Check if payload is valid."""
        return len(self.validate()) == 0

    def build(self) -> dict[str, Any]:
        """
        Build and return the payload.

        Returns:
            Payload dictionary
        """
        return self._data.copy()

    def to_json(self, indent: Optional[int] = None) -> str:
        """Convert payload to JSON string."""
        return json.dumps(self._data, indent=indent, default=str)

    def to_form_data(self) -> dict[str, str]:
        """Convert payload to form data format."""
        flat = self._flatten_data()
        return {k: str(v) for k, v in flat.items()}


class ArrayBuilder:
    """Builder for array fields in payload."""

    def __init__(self, parent: PayloadBuilder, name: str):
        self._parent = parent
        self._name = name
        self._items: list[Any] = []

    def add(self, item: Any) -> ArrayBuilder:
        """Add an item to the array."""
        self._items.append(item)
        return self

    def add_object(self) -> ObjectBuilder:
        """Add an object to the array and return builder."""
        return ObjectBuilder(self)

    def end(self) -> PayloadBuilder:
        """Finish array and return to parent."""
        self._parent._set_nested(self._name, self._items)
        return self._parent


class ObjectBuilder:
    """Builder for nested objects within arrays."""

    def __init__(self, parent: ArrayBuilder):
        self._parent = parent
        self._data: dict[str, Any] = {}

    def field(self, name: str, value: Any) -> ObjectBuilder:
        """Add a field to the object."""
        self._data[name] = value
        return self

    def end(self) -> ArrayBuilder:
        """Finish object and return to parent."""
        self._parent._items.append(self._data)
        return self._parent


class GraphQLPayloadBuilder:
    """
    Builder for GraphQL payloads.

    Example:
        payload = (
            GraphQLPayloadBuilder()
            .query("""
                query GetUser($id: ID!) {
                    user(id: $id) {
                        name
                        email
                    }
                }
            """)
            .variable("id", "123")
            .build()
        )
    """

    def __init__(self):
        self._query: str = ""
        self._variables: dict[str, Any] = {}
        self._operation_name: Optional[str] = None

    def query(self, query: str) -> GraphQLPayloadBuilder:
        """Set GraphQL query string."""
        self._query = query
        return self

    def mutation(self, mutation: str) -> GraphQLPayloadBuilder:
        """Set GraphQL mutation string."""
        self._query = mutation
        return self

    def variable(self, name: str, value: Any) -> GraphQLPayloadBuilder:
        """Add a query variable."""
        self._variables[name] = value
        return self

    def operation_name(self, name: str) -> GraphQLPayloadBuilder:
        """Set operation name."""
        self._operation_name = name
        return self

    def build(self) -> dict[str, Any]:
        """Build GraphQL payload."""
        payload: dict[str, Any] = {"query": self._query}

        if self._variables:
            payload["variables"] = self._variables

        if self._operation_name:
            payload["operationName"] = self._operation_name

        return payload

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.build())


class MultipartPayloadBuilder:
    """
    Builder for multipart/form-data payloads.

    Example:
        payload = (
            MultipartPayloadBuilder()
            .field("name", "John Doe")
            .file("avatar", "/path/to/image.png", "image/png")
            .build()
        )
    """

    def __init__(self):
        self._fields: dict[str, tuple[str, str]] = {}
        self._files: list[tuple[str, str, str, str]] = []

    def field(self, name: str, value: str) -> MultipartPayloadBuilder:
        """Add a form field."""
        self._fields[name] = (name, value)
        return self

    def file(
        self,
        name: str,
        filepath: str,
        content_type: str = "application/octet-stream"
    ) -> MultipartPayloadBuilder:
        """Add a file field."""
        self._files.append((name, filepath, content_type, filepath.split("/")[-1]))
        return self

    def build(self) -> tuple[dict[str, str], list[tuple[str, str, str, str]]]:
        """Build multipart payload."""
        return self._fields, self._files


def create_payload_builder(
    payload_type: PayloadType = PayloadType.JSON
) -> PayloadBuilder:
    """Factory to create a PayloadBuilder."""
    return PayloadBuilder(payload_type=payload_type)


def create_graphql_builder() -> GraphQLPayloadBuilder:
    """Factory to create a GraphQLPayloadBuilder."""
    return GraphQLPayloadBuilder()
