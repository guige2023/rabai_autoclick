"""
API GraphQL Federation Module.

Implements GraphQL Federation architecture for composing
multiple GraphQL services into a unified supergraph.
Supports schema federation, entity resolution, and query planning.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class FederationVersion(Enum):
    """Federation version."""
    V1 = "v1"
    V2 = "v2"


@dataclass
class Subgraph:
    """Represents a federated subgraph."""
    name: str
    url: str
    schema: dict[str, Any] = field(default_factory=dict)
    entities: list[str] = field(default_factory=list)
    capabilities: list[str] = field(default_factory=list)
    last_updated: float = field(default_factory=time.time)


@dataclass
class EntityDefinition:
    """Entity definition in federation."""
    name: str
    key_fields: list[str]
    reference_resolver: Optional[Callable] = None
    fields: dict[str, Any] = field(default_factory=dict)
    shareable: bool = False


@dataclass
class QueryPlan:
    """Query plan for federated query."""
    query_id: str
    fragments: list[dict[str, Any]] = field(default_factory=list)
    query_paths: list[dict[str, Any]] = field(default_factory=list)
    estimated_complexity: float = 0.0


@dataclass
class FederationConfig:
    """Configuration for GraphQL Federation."""
    version: FederationVersion = FederationVersion.V2
    enable_directives: bool = True
    query_planning_enabled: bool = True
    entity_caching: bool = True


class GraphQLFederation:
    """
    GraphQL Federation orchestrator.

    Manages federated GraphQL architecture with multiple subgraphs,
    entity resolution, and query planning.

    Example:
        fed = GraphQLFederation(config)
        fed.add_subgraph("users", "http://users-service/graphql")
        fed.add_subgraph("products", "http://products-service/graphql")
        supergraph = fed.compose()
    """

    def __init__(self, config: Optional[FederationConfig] = None) -> None:
        self._config = config or FederationConfig()
        self._subgraphs: dict[str, Subgraph] = {}
        self._entities: dict[str, EntityDefinition] = {}
        self._type_defs: dict[str, str] = {}
        self._resolvers: dict[str, Callable] = {}
        self._entity_cache: dict[str, Any] = {}

    def add_subgraph(
        self,
        name: str,
        url: str,
        schema: Optional[dict[str, Any]] = None,
        entities: Optional[list[str]] = None
    ) -> Subgraph:
        """
        Add a subgraph to the federation.

        Args:
            name: Subgraph name
            url: Subgraph GraphQL endpoint
            schema: Subgraph schema definition
            entities: List of entity names in this subgraph

        Returns:
            Created Subgraph
        """
        subgraph = Subgraph(
            name=name,
            url=url,
            schema=schema or {},
            entities=entities or []
        )
        self._subgraphs[name] = subgraph

        for entity in subgraph.entities:
            if entity not in self._entities:
                self._entities[entity] = EntityDefinition(
                    name=entity,
                    key_fields=[]
                )

        return subgraph

    def remove_subgraph(self, name: str) -> bool:
        """Remove a subgraph from federation."""
        return self._subgraphs.pop(name, None) is not None

    def register_entity(
        self,
        name: str,
        key_fields: list[str],
        fields: Optional[dict[str, Any]] = None,
        reference_resolver: Optional[Callable] = None,
        shareable: bool = False
    ) -> EntityDefinition:
        """
        Register an entity for federation.

        Args:
            name: Entity name
            key_fields: Fields that uniquely identify the entity
            fields: Entity field definitions
            reference_resolver: Function to resolve entity references
            shareable: Whether entity can be shared across subgraphs

        Returns:
            Created EntityDefinition
        """
        entity = EntityDefinition(
            name=name,
            key_fields=key_fields,
            fields=fields or {},
            reference_resolver=reference_resolver,
            shareable=shareable
        )
        self._entities[name] = entity
        return entity

    def add_type_def(
        self,
        type_name: str,
        type_def: str
    ) -> None:
        """Add a type definition to the supergraph."""
        self._type_defs[type_name] = type_def

    def set_resolver(
        self,
        field_path: str,
        resolver: Callable
    ) -> None:
        """
        Set a resolver for a field.

        Args:
            field_path: Dot-separated path (e.g., "User.posts")
            resolver: Resolution function
        """
        self._resolvers[field_path] = resolver

    def compose(self) -> dict[str, Any]:
        """
        Compose all subgraphs into a supergraph schema.

        Returns:
            Supergraph schema definition
        """
        type_defs = []
        type_defs.extend(self._generate_service_definitions())
        type_defs.extend(self._generate_entity_definitions())
        type_defs.extend(self._generate_directives())
        type_defs.extend(list(self._type_defs.values()))

        return {
            "version": self._config.version.value,
            "subgraphs": [s.name for s in self._subgraphs.keys()],
            "schema": "\n".join(type_defs),
            "entities": list(self._entities.keys()),
            "timestamp": time.time()
        }

    def _generate_service_definitions(self) -> list[str]:
        """Generate _service definitions for each subgraph."""
        defs = []
        for name, subgraph in self._subgraphs.items():
            defs.append(f'service {name} {{')
            defs.append(f'  url: "{subgraph.url}"')
            defs.append('}')
        return defs

    def _generate_entity_definitions(self) -> list[str]:
        """Generate entity definitions with @key directives."""
        defs = []
        for entity_name, entity in self._entities.items():
            fields_str = ", ".join(entity.key_fields)
            directives = f'@key(fields: "{fields_str}")'
            if entity.shareable:
                directives += " @shareable"

            defs.append(f'type {entity_name} {directives} {{')
            for field_name, field_type in entity.fields.items():
                defs.append(f'  {field_name}: {field_type}')
            defs.append('}')
        return defs

    def _generate_directives(self) -> list[str]:
        """Generate federation directives."""
        return [
            'directive @key(fields: String!) repeatable on OBJECT | INTERFACE',
            'directive @shareable on OBJECT | FIELD_DEFINITION',
            'directive @external on FIELD_DEFINITION | OBJECT',
            'directive @extends on OBJECT | INTERFACE',
            'directive @resolveFields(on: String!) on FIELD_DEFINITION',
            'directive @provides(fields: String!) on FIELD_DEFINITION',
            'directive @requires(fields: String!) on FIELD_DEFINITION'
        ]

    def plan_query(
        self,
        query: str,
        variables: Optional[dict[str, Any]] = None
    ) -> QueryPlan:
        """
        Plan a federated query execution.

        Args:
            query: GraphQL query string
            variables: Query variables

        Returns:
            QueryPlan with execution strategy
        """
        query_id = hashlib.sha256(query.encode()).hexdigest()[:16]

        fragments: list[dict[str, Any]] = []
        query_paths: list[dict[str, Any]] = []

        referenced_entities = self._extract_entity_references(query)

        for entity_name in referenced_entities:
            if entity_name in self._entities:
                entity = self._entities[entity_name]
                path = {
                    "entity": entity_name,
                    "subgraph": self._find_entity_subgraph(entity_name),
                    "key_fields": entity.key_fields,
                    "required_fields": self._extract_required_fields(query, entity_name)
                }
                query_paths.append(path)

        complexity = self._estimate_query_complexity(query_paths)

        return QueryPlan(
            query_id=query_id,
            fragments=fragments,
            query_paths=query_paths,
            estimated_complexity=complexity
        )

    def _extract_entity_references(self, query: str) -> set[str]:
        """Extract entity type names from query."""
        entities: set[str] = set()
        for entity_name in self._entities.keys():
            if entity_name in query:
                entities.add(entity_name)
        return entities

    def _find_entity_subgraph(self, entity_name: str) -> Optional[str]:
        """Find which subgraph owns an entity."""
        for name, subgraph in self._subgraphs.items():
            if entity_name in subgraph.entities:
                return name
        return None

    def _extract_required_fields(
        self,
        query: str,
        entity_name: str
    ) -> list[str]:
        """Extract fields required for an entity in query."""
        fields: list[str] = []
        if entity_name in query:
            entity_def = self._entities.get(entity_name)
            if entity_def:
                fields.extend(entity_def.key_fields)
        return fields

    def _estimate_query_complexity(self, query_paths: list[dict[str, Any]]) -> float:
        """Estimate query complexity score."""
        complexity = len(query_paths) * 1.0
        for path in query_paths:
            complexity += len(path.get("required_fields", [])) * 0.1
        return complexity

    async def execute_query(
        self,
        plan: QueryPlan
    ) -> dict[str, Any]:
        """
        Execute a federated query plan.

        Args:
            plan: QueryPlan to execute

        Returns:
            Query results from all subgraphs
        """
        results: dict[str, Any] = {"_meta": {"query_id": plan.query_id}}

        for path in plan.query_paths:
            subgraph_name = path["subgraph"]
            subgraph = self._subgraphs.get(subgraph_name)

            if not subgraph:
                continue

            cache_key = f"{path['entity']}:{','.join(str(k) for k in path.get('key_fields', []))}"

            if self._config.entity_caching and cache_key in self._entity_cache:
                results[path["entity"]] = self._entity_cache[cache_key]
                continue

            entity_result = await self._resolve_entity(path)
            if entity_result:
                results[path["entity"]] = entity_result
                if self._config.entity_caching:
                    self._entity_cache[cache_key] = entity_result

        return results

    async def _resolve_entity(
        self,
        path: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        """Resolve an entity using its reference resolver."""
        entity_name = path["entity"]
        entity = self._entities.get(entity_name)

        if entity and entity.reference_resolver:
            return await entity.reference_resolver(path)

        return None

    def get_subgraphs(self) -> list[Subgraph]:
        """Get all registered subgraphs."""
        return list(self._subgraphs.values())

    def get_entities(self) -> list[EntityDefinition]:
        """Get all registered entities."""
        return list(self._entities.values())

    def get_supergraph_schema(self) -> str:
        """Get the composed supergraph schema."""
        composed = self.compose()
        return composed.get("schema", "")

    def clear_cache(self) -> None:
        """Clear the entity cache."""
        self._entity_cache.clear()
