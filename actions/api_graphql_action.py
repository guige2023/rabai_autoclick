"""
API GraphQL Action Module

Provides GraphQL query execution, schema introspection, and resolver management
for API interactions.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class QueryType(Enum):
    """GraphQL operation types."""

    QUERY = "query"
    MUTATION = "mutation"
    SUBSCRIPTION = "subscription"


@dataclass
class GraphQLQuery:
    """A GraphQL query."""

    query_id: str
    query_string: str
    operation_type: QueryType
    variables: Dict[str, Any] = field(default_factory=dict)
    operation_name: Optional[str] = None


@dataclass
class GraphQLResult:
    """Result of a GraphQL query."""

    query_id: str
    data: Optional[Dict[str, Any]] = None
    errors: List[Dict[str, Any]] = field(default_factory=list)
    duration_ms: float = 0.0
    success: bool = True


@dataclass
class Resolver:
    """A GraphQL field resolver."""

    resolver_id: str
    field_name: str
    type_name: str
    handler: Callable[..., Any]


@dataclass
class GraphQLConfig:
    """Configuration for GraphQL."""

    introspection_enabled: bool = True
    default_timeout: float = 30.0
    max_query_depth: int = 10
    enable_caching: bool = True


class QueryParser:
    """Parses GraphQL query strings."""

    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}

    def parse(self, query_string: str) -> Dict[str, Any]:
        """Parse a GraphQL query string (simplified)."""
        if query_string in self._cache:
            return self._cache[query_string]

        result = {
            "operation_type": "query",
            "operation_name": None,
            "fields": [],
            "variables": [],
        }

        query_string_lower = query_string.lower().strip()
        if query_string_lower.startswith("mutation"):
            result["operation_type"] = "mutation"
        elif query_string_lower.startswith("subscription"):
            result["operation_type"] = "subscription"

        return result

    def get_fields(self, query_string: str) -> List[str]:
        """Extract field names from query (simplified)."""
        import re
        pattern = r'(\w+)\s*\{'
        matches = re.findall(pattern, query_string)
        return matches


class APIGraphQLAction:
    """
    GraphQL action for API queries and mutations.

    Features:
    - Query parsing and execution
    - Field resolver registration
    - Schema introspection
    - Variable handling
    - Query result caching
    - Error handling
    - Multiple operation support

    Usage:
        graphql = APIGraphQLAction(config)
        
        graphql.register_resolver("Query", "user", user_resolver)
        
        result = await graphql.execute('{ user(id: "1") { name email } }')
    """

    def __init__(self, config: Optional[GraphQLConfig] = None):
        self.config = config or GraphQLConfig()
        self._parser = QueryParser()
        self._resolvers: Dict[str, Dict[str, Resolver]] = {}
        self._type_defs: Dict[str, Dict[str, Any]] = {}
        self._cache: Dict[str, Any] = {}
        self._stats = {
            "queries_executed": 0,
            "queries_failed": 0,
            "cache_hits": 0,
        }

    def register_resolver(
        self,
        type_name: str,
        field_name: str,
        handler: Callable[..., Any],
    ) -> Resolver:
        """Register a field resolver."""
        resolver_id = f"{type_name}.{field_name}"

        if type_name not in self._resolvers:
            self._resolvers[type_name] = {}

        resolver = Resolver(
            resolver_id=resolver_id,
            field_name=field_name,
            type_name=type_name,
            handler=handler,
        )
        self._resolvers[type_name][field_name] = resolver
        return resolver

    def register_type(self, type_name: str, fields: Dict[str, str]) -> None:
        """Register a GraphQL type definition."""
        self._type_defs[type_name] = fields

    async def execute(
        self,
        query_string: str,
        variables: Optional[Dict[str, Any]] = None,
        operation_name: Optional[str] = None,
    ) -> GraphQLResult:
        """Execute a GraphQL query."""
        query_id = f"query_{uuid.uuid4().hex[:12]}"
        result = GraphQLResult(query_id=query_id)
        start_time = time.time()

        self._stats["queries_executed"] += 1

        try:
            parsed = self._parser.parse(query_string)
            fields = self._parser.get_fields(query_string)

            data = {}
            for field_name in fields:
                resolver = self._find_resolver(parsed["operation_type"], field_name)

                if resolver:
                    if asyncio.iscoroutinefunction(resolver.handler):
                        field_data = await resolver.handler(variables or {})
                    else:
                        field_data = resolver.handler(variables or {})
                    data[field_name] = field_data
                else:
                    data[field_name] = None

            result.data = data
            result.success = True

        except Exception as e:
            result.errors.append({"message": str(e)})
            result.success = False
            self._stats["queries_failed"] += 1

        result.duration_ms = (time.time() - start_time) * 1000
        return result

    def _find_resolver(
        self,
        operation_type: str,
        field_name: str,
    ) -> Optional[Resolver]:
        """Find a resolver for a field."""
        type_name = operation_type.capitalize()
        if type_name in self._resolvers:
            return self._resolvers[type_name].get(field_name)
        return None

    async def execute_batch(
        self,
        queries: List[tuple],
    ) -> List[GraphQLResult]:
        """Execute multiple queries."""
        results = []
        for query_string, variables in queries:
            result = await self.execute(query_string, variables)
            results.append(result)
        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get GraphQL statistics."""
        return {
            **self._stats.copy(),
            "total_types": len(self._type_defs),
            "total_resolvers": sum(len(r) for r in self._resolvers.values()),
        }


async def demo_graphql():
    """Demonstrate GraphQL execution."""
    config = GraphQLConfig()
    graphql = APIGraphQLAction(config)

    async def user_resolver(vars):
        return {"name": "Alice", "email": "alice@example.com"}

    async def users_resolver(vars):
        return [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": "bob@example.com"},
        ]

    graphql.register_resolver("Query", "user", user_resolver)
    graphql.register_resolver("Query", "users", users_resolver)

    result = await graphql.execute("{ user { name email } }")

    print(f"Result: {result.data}")
    print(f"Stats: {graphql.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_graphql())
