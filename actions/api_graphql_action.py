"""GraphQL API Action.

Provides GraphQL query/mutation execution with schema introspection.
"""
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field


@dataclass
class GraphQLQuery:
    query: str
    variables: Optional[Dict[str, Any]] = None
    operation_name: Optional[str] = None


@dataclass
class GraphQLResponse:
    data: Optional[Dict[str, Any]]
    errors: List[Dict[str, Any]]
    extensions: Dict[str, Any] = field(default_factory=dict)

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0


class GraphQLAction:
    """Executes GraphQL queries and manages schema."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        fetch_fn: Optional[Callable] = None,
    ) -> None:
        self.endpoint = endpoint
        self.headers = headers or {}
        self.fetch_fn = fetch_fn
        self.schema: Optional[Dict[str, Any]] = None

    def set_fetch_fn(self, fn: Callable) -> None:
        self.fetch_fn = fn

    def query(
        self,
        query: Union[str, GraphQLQuery],
        variables: Optional[Dict[str, Any]] = None,
    ) -> GraphQLResponse:
        gql_query = query if isinstance(query, GraphQLQuery) else GraphQLQuery(
            query=query,
            variables=variables,
        )
        payload: Dict[str, Any] = {"query": gql_query.query}
        if gql_query.variables:
            payload["variables"] = gql_query.variables
        if gql_query.operation_name:
            payload["operationName"] = gql_query.operation_name
        if self.fetch_fn:
            result = self.fetch_fn(self.endpoint or "", headers=self.headers, data=payload)
        else:
            result = self._mock_execute(payload)
        data = result.get("data")
        errors = result.get("errors", [])
        extensions = result.get("extensions", {})
        return GraphQLResponse(data=data, errors=errors, extensions=extensions)

    def _mock_execute(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        query = payload.get("query", "")
        if "IntrospectionQuery" in query and self.schema:
            return {"data": {"__schema": self.schema}}
        return {"data": {}, "errors": []}

    def set_schema(self, schema: Dict[str, Any]) -> None:
        self.schema = schema

    def parse_query(self, query_str: str) -> List[str]:
        import re
        operations = re.findall(r'(?:query|mutation)\s+(\w+)?', query_str)
        return [op if op else "anonymous" for op in operations]
