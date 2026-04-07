"""
GraphQL Client and Schema Management Utilities.

Provides utilities for querying GraphQL APIs, schema introspection,
query building, and response caching.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import json
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class CacheStrategy(Enum):
    """Caching strategies for GraphQL responses."""
    NO_CACHE = "no_cache"
    IN_MEMORY = "in_memory"
    PERSISTENT = "persistent"


@dataclass
class GraphQLQuery:
    """A GraphQL query definition."""
    query: str
    operation_name: Optional[str] = None
    variables: dict[str, Any] = field(default_factory=dict)
    fragments: list[str] = field(default_factory=list)

    def to_request_body(self) -> dict[str, Any]:
        """Convert to GraphQL request body."""
        body: dict[str, Any] = {
            "query": self.query,
        }

        if self.operation_name:
            body["operationName"] = self.operation_name

        if self.variables:
            body["variables"] = self.variables

        return body


@dataclass
class GraphQLResponse:
    """A GraphQL response."""
    data: Optional[dict[str, Any]] = None
    errors: Optional[list[dict[str, Any]]] = None
    extensions: Optional[dict[str, Any]] = None
    status_code: int = 200
    duration_ms: float = 0.0
    cached: bool = False

    @property
    def has_errors(self) -> bool:
        return self.errors is not None and len(self.errors) > 0

    @property
    def first_error(self) -> Optional[str]:
        if self.errors:
            return self.errors[0].get("message", "Unknown error")
        return None


@dataclass
class GraphQLSchema:
    """GraphQL schema representation."""
    types: list[dict[str, Any]]
    query_type: Optional[dict[str, Any]] = None
    mutation_type: Optional[dict[str, Any]] = None
    subscription_type: Optional[dict[str, Any]] = None
    enums: list[dict[str, Any]] = field(default_factory=list)
    inputs: list[dict[str, Any]] = field(default_factory=list)
    fetched_at: datetime = field(default_factory=datetime.now)


@dataclass
class FieldDefinition:
    """A field definition in a GraphQL type."""
    name: str
    type_name: str
    is_list: bool = False
    is_nullable: bool = True
    args: list[dict[str, Any]] = field(default_factory=list)
    description: Optional[str] = None


@dataclass
class TypeDefinition:
    """A type definition in a GraphQL schema."""
    name: str
    kind: str
    fields: list[FieldDefinition] = field(default_factory=list)
    enum_values: list[str] = field(default_factory=list)
    input_fields: list[FieldDefinition] = field(default_factory=list)
    description: Optional[str] = None


class GraphQLClient:
    """GraphQL API client."""

    def __init__(
        self,
        endpoint: str,
        headers: Optional[dict[str, str]] = None,
        timeout_seconds: float = 30.0,
        cache_strategy: CacheStrategy = CacheStrategy.IN_MEMORY,
    ) -> None:
        self.endpoint = endpoint
        self.headers = headers or {}
        self.timeout_seconds = timeout_seconds
        self.cache_strategy = cache_strategy
        self._cache: dict[str, tuple[Any, float]] = {}
        self._cache_ttl_seconds: float = 300

    def execute(
        self,
        query: GraphQLQuery,
        use_cache: bool = True,
    ) -> GraphQLResponse:
        """Execute a GraphQL query."""
        cache_key = self._get_cache_key(query)

        if use_cache and self.cache_strategy != CacheStrategy.NO_CACHE:
            cached = self._get_from_cache(cache_key)
            if cached:
                cached.cached = True
                return cached

        start_time = time.time()

        try:
            request_body = query.to_request_body()

            request = urllib.request.Request(
                self.endpoint,
                data=json.dumps(request_body).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    **self.headers,
                },
                method="POST",
            )

            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                response_body = json.loads(response.read().decode("utf-8"))
                duration_ms = (time.time() - start_time) * 1000

                result = GraphQLResponse(
                    data=response_body.get("data"),
                    errors=response_body.get("errors"),
                    extensions=response_body.get("extensions"),
                    status_code=response.status,
                    duration_ms=duration_ms,
                )

                if use_cache and self.cache_strategy != CacheStrategy.NO_CACHE and not result.has_errors:
                    self._put_in_cache(cache_key, result)

                return result

        except urllib.error.HTTPError as e:
            duration_ms = (time.time() - start_time) * 1000
            try:
                error_body = json.loads(e.read().decode("utf-8"))
                return GraphQLResponse(
                    status_code=e.code,
                    duration_ms=duration_ms,
                    errors=error_body.get("errors", [{"message": e.reason}]),
                )
            except Exception:
                return GraphQLResponse(
                    status_code=e.code,
                    duration_ms=duration_ms,
                    errors=[{"message": e.reason}],
                )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return GraphQLResponse(
                status_code=500,
                duration_ms=duration_ms,
                errors=[{"message": str(e)}],
            )

    def query(
        self,
        query_string: str,
        variables: Optional[dict[str, Any]] = None,
        operation_name: Optional[str] = None,
        use_cache: bool = True,
    ) -> GraphQLResponse:
        """Execute a GraphQL query string."""
        query = GraphQLQuery(
            query=query_string,
            variables=variables or {},
            operation_name=operation_name,
        )
        return self.execute(query, use_cache)

    def mutate(
        self,
        mutation_string: str,
        variables: Optional[dict[str, Any]] = None,
        operation_name: Optional[str] = None,
    ) -> GraphQLResponse:
        """Execute a GraphQL mutation."""
        query = GraphQLQuery(
            query=mutation_string,
            variables=variables or {},
            operation_name=operation_name,
        )
        return self.execute(query, use_cache=False)

    def introspect_schema(self) -> GraphQLSchema:
        """Introspect the GraphQL schema."""
        introspection_query = """
        {
            __schema {
                types {
                    name
                    kind
                    description
                    fields {
                        name
                        description
                        type {
                            name
                            kind
                            ofType {
                                name
                                kind
                            }
                        }
                        args {
                            name
                            description
                            type {
                                name
                                kind
                            }
                            defaultValue
                        }
                    }
                    enumValues {
                        name
                        description
                    }
                    inputFields {
                        name
                        description
                        type {
                            name
                            kind
                        }
                    }
                }
                queryType { name }
                mutationType { name }
                subscriptionType { name }
            }
        }
        """

        result = self.query(introspection_query, use_cache=False)

        if result.has_errors or not result.data:
            raise RuntimeError(f"Schema introspection failed: {result.first_error}")

        schema_data = result.data["__schema"]

        enums = [t for t in schema_data["types"] if t["kind"] == "ENUM"]
        inputs = [t for t in schema_data["types"] if t["kind"] == "INPUT_OBJECT"]

        return GraphQLSchema(
            types=schema_data["types"],
            query_type=schema_data.get("queryType"),
            mutation_type=schema_data.get("mutationType"),
            subscription_type=schema_data.get("subscriptionType"),
            enums=enums,
            inputs=inputs,
        )

    def _get_cache_key(self, query: GraphQLQuery) -> str:
        """Generate a cache key for a query."""
        content = json.dumps({
            "query": query.query,
            "variables": query.variables,
            "operationName": query.operation_name,
        }, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()

    def _get_from_cache(self, key: str) -> Optional[GraphQLResponse]:
        """Get a response from cache."""
        if key not in self._cache:
            return None

        cached_data, cached_at = self._cache[key]
        age = time.time() - cached_at

        if age > self._cache_ttl_seconds:
            del self._cache[key]
            return None

        return cached_data

    def _put_in_cache(self, key: str, response: GraphQLResponse) -> None:
        """Put a response in cache."""
        self._cache[key] = (response, time.time())

    def clear_cache(self) -> None:
        """Clear the response cache."""
        self._cache.clear()


class QueryBuilder:
    """Builder for constructing GraphQL queries."""

    def __init__(self, operation_type: str = "query") -> None:
        self.operation_type = operation_type
        self._selections: list[str] = []
        self._variables: dict[str, Any] = {}
        self._variable_definitions: list[str] = []
        self._arguments: dict[str, Any] = {}
        self._fragments: list[str] = []
        self._operation_name: Optional[str] = None

    def select(self, *fields: str) -> "QueryBuilder":
        """Add field selections."""
        for field in fields:
            if "." in field:
                parts = field.split(".")
                self._selections.append(self._build_nested_field(parts))
            else:
                self._selections.append(field)
        return self

    def _build_nested_field(self, parts: list[str]) -> str:
        """Build a nested field selection."""
        if len(parts) == 1:
            return parts[0]

        return f"{parts[0]} {{ {self._build_nested_field(parts[1:])} }}"

    def with_args(self, **args: Any) -> "QueryBuilder":
        """Add arguments to the current field."""
        self._arguments.update(args)
        return self

    def variable(self, name: str, type_name: str, default_value: Any = None) -> "QueryBuilder":
        """Define a query variable."""
        var_def = f"${name}: {type_name}"
        if default_value is not None:
            var_def += f" = {self._format_value(default_value)}"
        self._variable_definitions.append(var_def)
        self._variables[name] = default_value
        return self

    def with_fragment(self, fragment: str) -> "QueryBuilder":
        """Add a fragment to the query."""
        self._fragments.append(fragment)
        return self

    def operation_name(self, name: str) -> "QueryBuilder":
        """Set the operation name."""
        self._operation_name = name
        return self

    def build(self) -> GraphQLQuery:
        """Build the GraphQL query."""
        query_parts: list[str] = []

        if self._variable_definitions:
            query_parts.append(f"({' '.join(self._variable_definitions)})")

        query_parts.append("{ " + " ".join(self._selections) + " }")

        query = " ".join(query_parts)

        for fragment in self._fragments:
            query += " " + fragment

        return GraphQLQuery(
            query=query,
            operation_name=self._operation_name,
            variables=self._variables,
            fragments=self._fragments,
        )

    def _format_value(self, value: Any) -> str:
        """Format a value for GraphQL."""
        if isinstance(value, str):
            return f'"{value}"'
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, list):
            return "[" + ", ".join(self._format_value(v) for v in value) + "]"
        elif isinstance(value, dict):
            pairs = [f"{k}: {self._format_value(v)}" for k, v in value.items()]
            return "{" + ", ".join(pairs) + "}"
        else:
            return str(value)


class SchemaExplorer:
    """Explore and analyze GraphQL schemas."""

    def __init__(self, schema: GraphQLSchema) -> None:
        self.schema = schema
        self._type_map: dict[str, TypeDefinition] = {}
        self._build_type_map()

    def _build_type_map(self) -> None:
        """Build a map of type names to type definitions."""
        for type_data in self.schema.types:
            type_name = type_data.get("name")
            if not type_name or type_name.startswith("__"):
                continue

            type_def = TypeDefinition(
                name=type_name,
                kind=type_data.get("kind", ""),
                description=type_data.get("description"),
            )

            if "fields" in type_data and type_data["fields"]:
                for field_data in type_data["fields"]:
                    type_name_str = self._get_type_name(field_data.get("type", {}))
                    type_def.fields.append(FieldDefinition(
                        name=field_data.get("name", ""),
                        type_name=type_name_str,
                        description=field_data.get("description"),
                    ))

            if "enumValues" in type_data:
                type_def.enum_values = [v["name"] for v in type_data["enumValues"]]

            self._type_map[type_name] = type_def

    def _get_type_name(self, type_info: dict[str, Any]) -> str:
        """Extract the type name from type info."""
        name = type_info.get("name")
        if name:
            return name

        of_type = type_info.get("ofType")
        if of_type:
            return self._get_type_name(of_type)

        return "Unknown"

    def get_type(self, name: str) -> Optional[TypeDefinition]:
        """Get a type definition by name."""
        return self._type_map.get(name)

    def list_queries(self) -> list[FieldDefinition]:
        """List all queries in the schema."""
        if not self.schema.query_type:
            return []

        query_type_name = self.schema.query_type.get("name")
        if not query_type_name:
            return []

        type_def = self._type_map.get(query_type_name)
        return type_def.fields if type_def else []

    def list_mutations(self) -> list[FieldDefinition]:
        """List all mutations in the schema."""
        if not self.schema.mutation_type:
            return []

        mutation_type_name = self.schema.mutation_type.get("name")
        if not mutation_type_name:
            return []

        type_def = self._type_map.get(mutation_type_name)
        return type_def.fields if type_def else []

    def find_field(self, type_name: str, field_name: str) -> Optional[FieldDefinition]:
        """Find a field in a type."""
        type_def = self._type_map.get(type_name)
        if not type_def:
            return None

        for field_def in type_def.fields:
            if field_def.name == field_name:
                return field_def

        return None

    def get_field_type_chain(self, type_name: str, field_name: str) -> list[str]:
        """Get the type chain for a field (for nested types)."""
        chain = [type_name]

        current_type = type_name
        for _ in range(5):
            field_def = self.find_field(current_type, field_name)
            if not field_def:
                break

            chain.append(field_def.type_name)
            current_type = field_def.type_name

            if current_type in ("String", "Int", "Float", "Boolean", "ID"):
                break

        return chain
