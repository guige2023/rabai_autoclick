"""GraphQL query building, validation, and execution utilities."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "GraphQLQuery",
    "GraphQLClient",
    "build_query",
    "build_mutation",
    "parse_variables",
    "validate_schema",
]


@dataclass
class GraphQLQuery:
    """Represents a GraphQL query or mutation."""

    operation: str  # "query" | "mutation" | "subscription"
    name: str
    fields: list[str]
    variables: dict[str, str] = field(default_factory=dict)
    fragments: dict[str, str] = field(default_factory=dict)
    directives: dict[str, Any] = field(default_factory=dict)

    def render(self) -> str:
        """Render the GraphQL query as a string."""
        var_decl = ""
        if self.variables:
            vars_list = [f"${k}: {v}" for k, v in self.variables.items()]
            var_decl = f"({', '.join(vars_list)})"

        directives_str = ""
        if self.directives:
            dirs = [f'@{k}({json.dumps(v)})' for k, v in self.directives.items()]
            directives_str = f" {' '.join(dirs)}"

        frag_refs = ""
        if self.fragments:
            frag_refs = " " + " ".join(f"...{k}" for k in self.fragments)

        body = "\n".join(f"  {f}" for f in self.fields)
        return f"{self.operation} {self.name}{var_decl}{directives_str} {{\n{body}\n}}{frag_refs}"

    def to_dict(self) -> dict[str, Any]:
        """Convert to a dict suitable for GraphQL POST body."""
        return {
            "operationName": self.name,
            "query": self.render(),
            "variables": {},
        }


class GraphQLClient:
    """Lightweight GraphQL client with basic caching and retry logic."""

    def __init__(
        self,
        endpoint: str,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
        retries: int = 3,
    ) -> None:
        self.endpoint = endpoint
        self.headers = headers or {}
        self.timeout = timeout
        self.retries = retries
        self._cache: dict[str, Any] = {}

    def execute(
        self,
        query: GraphQLQuery | str,
        variables: dict[str, Any] | None = None,
        use_cache: bool = False,
    ) -> dict[str, Any]:
        """Execute a GraphQL query against the endpoint."""
        import urllib.request
        import urllib.error

        if isinstance(query, GraphQLQuery):
            body = query.to_dict()
            body["variables"] = variables or {}
            cache_key = json.dumps(body, sort_keys=True)
        else:
            body = {"query": query, "variables": variables or {}}
            cache_key = json.dumps(body, sort_keys=True)

        if use_cache and cache_key in self._cache:
            return self._cache[cache_key]

        payload = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            self.endpoint,
            data=payload,
            headers={**self.headers, "Content-Type": "application/json"},
            method="POST",
        )

        last_error: Exception | None = None
        for attempt in range(self.retries):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    result = json.loads(resp.read().decode("utf-8"))
                    if "errors" in result:
                        raise GraphQLError(result["errors"])
                    if use_cache:
                        self._cache[cache_key] = result
                    return result
            except (urllib.error.URLError, urllib.error.HTTPError) as e:
                last_error = e
                if attempt < self.retries - 1:
                    import time
                    time.sleep(2 ** attempt)

        raise GraphQLError([{"message": str(last_error)}])

    def query(
        self,
        name: str,
        fields: list[str],
        variables: dict[str, str] | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Convenience method to build and execute a query."""
        q = build_query(name, fields, variables)
        return self.execute(q, **kwargs)

    def mutate(
        self,
        name: str,
        fields: list[str],
        variables: dict[str, str] | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Convenience method to build and execute a mutation."""
        q = build_mutation(name, fields, variables)
        return self.execute(q, **kwargs)


def build_query(
    name: str,
    fields: list[str],
    variables: dict[str, str] | None = None,
) -> GraphQLQuery:
    """Build a GraphQL query."""
    return GraphQLQuery(
        operation="query",
        name=name,
        fields=fields,
        variables=variables or {},
    )


def build_mutation(
    name: str,
    fields: list[str],
    variables: dict[str, str] | None = None,
) -> GraphQLQuery:
    """Build a GraphQL mutation."""
    return GraphQLQuery(
        operation="mutation",
        name=name,
        fields=fields,
        variables=variables or {},
    )


def parse_variables(schema: str) -> dict[str, str]:
    """Parse variable definitions from a GraphQL schema string."""
    pattern = r"\$(\w+):\s*(\w+)"
    return dict(re.findall(pattern, schema))


def validate_schema(schema_str: str) -> list[str]:
    """Basic GraphQL schema validation - returns list of errors."""
    errors: list[str] = []

    open_braces = schema_str.count("{")
    close_braces = schema_str.count("}")
    if open_braces != close_braces:
        errors.append("Mismatched braces")

    if not re.search(r"(query|mutation|subscription)\s+\w+\s*\{", schema_str):
        if "{" in schema_str:
            errors.append("Missing operation definition")

    quotes = schema_str.count('"')
    if quotes % 2 != 0:
        errors.append("Unmatched quotes")

    return errors


class GraphQLError(Exception):
    """GraphQL execution error."""

    def __init__(self, errors: list[dict[str, Any]]) -> None:
        self.errors = errors
        super().__init__(json.dumps(errors, indent=2))
