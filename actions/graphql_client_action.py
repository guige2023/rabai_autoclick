"""
GraphQL Client Action Module.

Provides a flexible GraphQL client with query building, batching,
subscriptions support, and automatic retry logic.
"""

from typing import Optional, Dict, List, Any, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
import json
import logging
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

logger = logging.getLogger(__name__)


class QueryType(Enum):
    """GraphQL operation types."""
    QUERY = "query"
    MUTATION = "mutation"
    SUBSCRIPTION = "subscription"


@dataclass
class GraphQLField:
    """Represents a field in a GraphQL query."""
    name: str
    alias: Optional[str] = None
    arguments: Optional[Dict[str, Any]] = None
    fields: Optional[List["GraphQLField"]] = None
    
    def to_str(self, indent: int = 2) -> str:
        """Convert field to GraphQL string."""
        alias_str = f"{self.alias}: " if self.alias else ""
        args_str = self._format_arguments() if self.arguments else ""
        
        if self.fields:
            subfields = "\n".join(
                f.to_str(indent) for f in self.fields
            )
            return f"{' ' * indent}{alias_str}{self.name}{args_str} {{\n{subfields}\n{' ' * indent}}}"
        return f"{' ' * indent}{alias_str}{self.name}{args_str}"
        
    def _format_arguments(self) -> str:
        """Format arguments as GraphQL string."""
        if not self.arguments:
            return ""
        args = []
        for key, value in self.arguments.items():
            if isinstance(value, str):
                args.append(f'{key}: "{value}"')
            elif isinstance(value, bool):
                args.append(f"{key}: {str(value).lower()}")
            elif isinstance(value, dict):
                args.append(f"{key}: {json.dumps(value)}")
            else:
                args.append(f"{key}: {value}")
        return f"({', '.join(args)})"


@dataclass
class GraphQLQuery:
    """Represents a complete GraphQL query."""
    operation_type: QueryType
    operation_name: Optional[str] = None
    variables: Optional[Dict[str, Any]] = None
    fields: List[GraphQLField] = field(default_factory=list)
    
    def to_string(self) -> str:
        """Convert query to GraphQL string."""
        parts = []
        
        if self.operation_name:
            parts.append(f"{self.operation_type.value} {self.operation_name}")
        else:
            parts.append(self.operation_type.value)
            
        vars_str = self._format_variables()
        
        field_strs = []
        for field in self.fields:
            field_strs.append(field.to_str())
            
        fields_str = "\n".join(field_strs)
        return f"{parts[0]}{vars_str} {{\n{fields_str}\n}}"
        
    def _format_variables(self) -> str:
        """Format variables definition."""
        if not self.variables:
            return ""
        parts = []
        for name, gql_type in self.variables.items():
            parts.append(f"${name}: {gql_type}")
        return f"({', '.join(parts)})"
        
    def get_variable_values(self) -> Dict[str, Any]:
        """Get variable values for execution."""
        if not self.variables:
            return {}
        return {}


@dataclass
class GraphQLResponse:
    """Represents a GraphQL response."""
    data: Optional[Dict[str, Any]] = None
    errors: Optional[List[Dict[str, Any]]] = None
    extensions: Optional[Dict[str, Any]] = None
    
    @property
    def has_errors(self) -> bool:
        return self.errors is not None and len(self.errors) > 0
        
    def get_data(self, path: str, default: Any = None) -> Any:
        """
        Get nested data from response.
        
        Args:
            path: Dot-separated path (e.g., "user.profile.name")
            default: Default value if path not found
        """
        if self.data is None:
            return default
        keys = path.split(".")
        value = self.data
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value


class GraphQLClient:
    """
    GraphQL client with query building and execution.
    
    Example:
        client = GraphQLClient("https://api.example.com/graphql")
        
        query = client.query("GetUser")
        query.select("id").select("name").select("email")
        query.with_field("profile", fields=["avatar", "bio"])
        
        result = await client.execute(query, variables={"id": "123"})
    """
    
    def __init__(
        self,
        endpoint: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 30.0,
        retry_count: int = 3,
        retry_delay: float = 1.0,
    ):
        self.endpoint = endpoint
        self.default_headers = headers or {}
        self.default_headers["Content-Type"] = "application/json"
        self.timeout = timeout
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        
    def build_query(
        self,
        operation_type: QueryType = QueryType.QUERY,
        operation_name: Optional[str] = None,
    ) -> GraphQLQuery:
        """Start building a new GraphQL query."""
        return GraphQLQuery(
            operation_type=operation_type,
            operation_name=operation_name,
        )
        
    def query(self, name: Optional[str] = None) -> GraphQLQueryBuilder:
        """Start building a query."""
        return GraphQLQueryBuilder(self, QueryType.QUERY, name)
        
    def mutation(self, name: Optional[str] = None) -> GraphQLQueryBuilder:
        """Start building a mutation."""
        return GraphQLQueryBuilder(self, QueryType.MUTATION, name)
        
    def execute(
        self,
        query: GraphQLQuery,
        variables: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> GraphQLResponse:
        """
        Execute a GraphQL query.
        
        Args:
            query: GraphQLQuery to execute
            variables: Variable values
            headers: Additional headers
            
        Returns:
            GraphQLResponse with data and errors
        """
        payload = {
            "query": query.to_string(),
        }
        
        if variables:
            payload["variables"] = variables
            
        merged_vars = {**(query.variables or {}), **(variables or {})}
        if merged_vars:
            payload["variables"] = merged_vars
            
        headers = {**self.default_headers, **(headers or {})}
        
        for attempt in range(self.retry_count):
            try:
                response = self._send_request(payload, headers)
                return response
                
            except Exception as e:
                logger.warning(f"GraphQL request attempt {attempt + 1} failed: {e}")
                if attempt < self.retry_count - 1:
                    import time
                    time.sleep(self.retry_delay * (attempt + 1))
                    
        return GraphQLResponse(errors=[{"message": "Max retries exceeded"}])
        
    def _send_request(
        self,
        payload: Dict[str, Any],
        headers: Dict[str, str],
    ) -> GraphQLResponse:
        """Send HTTP request to GraphQL endpoint."""
        data = json.dumps(payload).encode("utf-8")
        request = Request(self.endpoint, data=data, headers=headers)
        
        try:
            with urlopen(request, timeout=self.timeout) as response:
                result = json.loads(response.read().decode("utf-8"))
                return GraphQLResponse(
                    data=result.get("data"),
                    errors=result.get("errors"),
                    extensions=result.get("extensions"),
                )
        except HTTPError as e:
            return GraphQLResponse(errors=[{"message": f"HTTP {e.code}: {e.reason}"}])
        except URLError as e:
            return GraphQLResponse(errors=[{"message": f"URL Error: {e.reason}"}])


class GraphQLQueryBuilder:
    """
    Builder for constructing GraphQL queries fluently.
    
    Example:
        builder = client.query("GetUsers")
        builder.select("id").select("name")
        builder.with_field("posts", fields=["title", "content"])
        query = builder.build()
    """
    
    def __init__(
        self,
        client: GraphQLClient,
        operation_type: QueryType,
        name: Optional[str],
    ):
        self.client = client
        self.operation_type = operation_type
        self.operation_name = name
        self._fields: List[GraphQLField] = []
        
    def select(self, name: str, alias: Optional[str] = None) -> "GraphQLQueryBuilder":
        """Add a simple field to selection."""
        self._fields.append(GraphQLField(name=name, alias=alias))
        return self
        
    def with_field(
        self,
        name: str,
        fields: Optional[List[str]] = None,
        alias: Optional[str] = None,
        arguments: Optional[Dict[str, Any]] = None,
    ) -> "GraphQLQueryBuilder":
        """Add a field with nested subfields."""
        subfields = None
        if fields:
            subfields = [GraphQLField(name=f) for f in fields]
            
        self._fields.append(GraphQLField(
            name=name,
            alias=alias,
            arguments=arguments,
            fields=subfields,
        ))
        return self
        
    def with_typed_field(
        self,
        name: str,
        type_name: str,
        fields: Optional[List[str]] = None,
        alias: Optional[str] = None,
        arguments: Optional[Dict[str, Any]] = None,
    ) -> "GraphQLQueryBuilder":
        """Add a field with type annotation."""
        return self.with_field(name, fields, alias, arguments)
        
    def build(self) -> GraphQLQuery:
        """Build the final GraphQLQuery object."""
        return GraphQLQuery(
            operation_type=self.operation_type,
            operation_name=self.operation_name,
            fields=self._fields.copy(),
        )
        
    def execute(self, variables: Optional[Dict[str, Any]] = None) -> GraphQLResponse:
        """Build and execute the query."""
        return self.client.execute(self.build(), variables)


class GraphQLBatchClient(GraphQLClient):
    """
    GraphQL client with batching support for multiple queries.
    
    Example:
        client = GraphQLBatchClient("https://api.example.com/graphql")
        
        batch = client.create_batch()
        batch.add(query1, variables={"id": "1"})
        batch.add(query2, variables={"id": "2"})
        
        results = await batch.execute()
    """
    
    def execute_batch(
        self,
        queries: List[tuple],
    ) -> List[GraphQLResponse]:
        """
        Execute multiple queries in a single batch request.
        
        Args:
            queries: List of (query, variables) tuples
            
        Returns:
            List of GraphQLResponse in same order as input
        """
        if not queries:
            return []
            
        payload = []
        for query, variables in queries:
            item = {"query": query.to_string()}
            if variables:
                item["variables"] = variables
            payload.append(item)
            
        headers = {**self.default_headers}
        data = json.dumps(payload).encode("utf-8")
        request = Request(self.endpoint, data=data, headers=headers)
        
        try:
            with urlopen(request, timeout=self.timeout) as response:
                results = json.loads(response.read().decode("utf-8"))
                
                if isinstance(results, list):
                    return [
                        GraphQLResponse(
                            data=r.get("data"),
                            errors=r.get("errors"),
                            extensions=r.get("extensions"),
                        )
                        for r in results
                    ]
                return [GraphQLResponse(data=results.get("data"), errors=results.get("errors"))]
                
        except Exception as e:
            logger.error(f"Batch request failed: {e}")
            return [GraphQLResponse(errors=[{"message": str(e)}])] * len(queries)
            
    def create_batch(self) -> "GraphQLBatchBuilder":
        """Create a new batch builder."""
        return GraphQLBatchBuilder(self)


class GraphQLBatchBuilder:
    """Builder for GraphQL batch requests."""
    
    def __init__(self, client: GraphQLBatchClient):
        self.client = client
        self._queries: List[tuple] = []
        
    def add(
        self,
        query: GraphQLQuery,
        variables: Optional[Dict[str, Any]] = None,
    ) -> "GraphQLBatchBuilder":
        """Add a query to the batch."""
        self._queries.append((query, variables))
        return self
        
    def execute(self) -> List[GraphQLResponse]:
        """Execute all queries in batch."""
        return self.client.execute_batch(self._queries)
