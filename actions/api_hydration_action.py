"""
API Hydration Action Module.

Handles lazy loading and on-demand hydration of API responses,
converting flat or minimal responses into fully populated nested objects.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Type, TypeVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class HydrationStrategy(Enum):
    """Strategies for handling hydration."""
    LAZY = auto()
    EAGER = auto()
    PARTIAL = auto()
    BACKGROUND = auto()


@dataclass
class HydrationConfig:
    """Configuration for hydration behavior."""
    strategy: HydrationStrategy = HydrationStrategy.LAZY
    max_depth: int = 10
    timeout_seconds: float = 30.0
    batch_size: int = 50
    on_missing: str = "skip"  # skip, error, null
    cache_hydrated: bool = True


@dataclass
class HydratedField:
    """Represents a field that needs hydration."""
    name: str
    field_type: str
    related_id: Any
    hydrated: bool = False
    value: Any = None
    error: Optional[str] = None


@dataclass
class HydrationResult:
    """Result of a hydration operation."""
    original: Dict[str, Any]
    hydrated: Dict[str, Any]
    fields_hydrated: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    duration_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "hydrated_count": len(self.fields_hydrated),
            "error_count": len(self.errors),
            "duration_ms": self.duration_ms,
            "hydrated": self.hydrated,
        }


class HydrationRegistry:
    """Registry for hydration handlers."""

    def __init__(self):
        """Initialize the registry."""
        self._handlers: Dict[str, Callable] = {}
        self._resolvers: Dict[str, Callable] = {}

    def register_handler(
        self,
        resource_type: str,
        handler: Callable[[Any], Dict[str, Any]],
    ) -> None:
        """
        Register a hydration handler for a resource type.

        Args:
            resource_type: Type identifier (e.g., "user", "order").
            handler: Async function to hydrate the resource.
        """
        self._handlers[resource_type] = handler
        logger.debug(f"Registered hydration handler for: {resource_type}")

    def register_resolver(
        self,
        field_name: str,
        resolver: Callable[[Any, str], Any],
    ) -> None:
        """
        Register a field resolver.

        Args:
            field_name: Name of the field to resolve.
            resolver: Function to resolve the field value.
        """
        self._resolvers[field_name] = resolver

    def get_handler(self, resource_type: str) -> Optional[Callable]:
        """Get handler for a resource type."""
        return self._handlers.get(resource_type)

    def get_resolver(self, field_name: str) -> Optional[Callable]:
        """Get resolver for a field name."""
        return self._resolvers.get(field_name)


class ApiHydrationAction:
    """
    Handles API response hydration for lazy-loaded relationships.

    This action transforms flat or partially loaded API responses into
    fully populated nested objects by resolving relationships on-demand.

    Example:
        >>> action = ApiHydrationAction()
        >>> action.register_handler("user", load_user_by_id)
        >>> flat_response = {"user_id": 123, "name": "Alice"}
        >>> hydrated = await action.hydrate(flat_response, ["user_id"])
        >>> print(hydrated["user"]["name"])
        Alice
    """

    def __init__(self, config: Optional[HydrationConfig] = None):
        """
        Initialize the API Hydration Action.

        Args:
            config: Optional hydration configuration.
        """
        self.config = config or HydrationConfig()
        self.registry = HydrationRegistry()
        self._cache: Dict[str, Any] = {}
        self._pending: Dict[str, List[Callable]] = {}

    async def hydrate(
        self,
        data: Dict[str, Any],
        relations: List[str],
        resource_type: str = "default",
    ) -> Dict[str, Any]:
        """
        Hydrate a data object with related resources.

        Args:
            data: The flat or partial data to hydrate.
            relations: List of relation field names to hydrate.
            resource_type: Type of the resource for handler lookup.

        Returns:
            Fully hydrated data object.
        """
        result = data.copy()
        fields_to_hydrate = self._identify_fields(data, relations)

        for field_info in fields_to_hydrate:
            try:
                hydrated_value = await self._hydrate_field(
                    field_info,
                    data,
                    resource_type,
                )
                if hydrated_value is not None:
                    result[field_info.name] = hydrated_value
            except Exception as e:
                logger.error(f"Failed to hydrate field {field_info.name}: {e}")
                if self.config.on_missing == "error":
                    raise

        return result

    async def hydrate_batch(
        self,
        items: List[Dict[str, Any]],
        relations: List[str],
        resource_type: str = "default",
    ) -> List[Dict[str, Any]]:
        """
        Hydrate multiple items in batch.

        Args:
            items: List of items to hydrate.
            relations: Relations to hydrate for each item.
            resource_type: Type of resources.

        Returns:
            List of hydrated items.
        """
        handler = self.registry.get_handler(resource_type)

        if handler:
            return await self._batch_hydrate_with_handler(
                items, relations, handler
            )

        results = []
        for item in items:
            hydrated = await self.hydrate(item, relations, resource_type)
            results.append(hydrated)

        return results

    def _identify_fields(
        self,
        data: Dict[str, Any],
        relations: List[str],
    ) -> List[HydratedField]:
        """Identify which fields need hydration."""
        fields = []

        for relation in relations:
            if relation.endswith("_id"):
                base_name = relation[:-3]
                value = data.get(relation)

                if value is not None:
                    fields.append(HydratedField(
                        name=base_name,
                        field_type=base_name,
                        related_id=value,
                    ))

        return fields

    async def _hydrate_field(
        self,
        field_info: HydratedField,
        data: Dict[str, Any],
        resource_type: str,
    ) -> Optional[Dict[str, Any]]:
        """Hydrate a single field."""
        cache_key = f"{field_info.field_type}:{field_info.related_id}"

        if self.config.cache_hydrated and cache_key in self._cache:
            return self._cache[cache_key]

        if self.config.strategy == HydrationStrategy.LAZY:
            return await self._lazy_load(field_info, resource_type)

        return await self._eager_load(field_info, resource_type)

    async def _lazy_load(
        self,
        field_info: HydratedField,
        resource_type: str,
    ) -> Optional[Dict[str, Any]]:
        """Lazy load a related resource."""
        resolver = self.registry.get_resolver(field_info.field_type)

        if resolver:
            return await resolver(field_info.related_id, resource_type)

        handler = self.registry.get_handler(field_info.field_type)
        if handler:
            return await handler(field_info.related_id)

        return None

    async def _eager_load(
        self,
        field_info: HydratedField,
        resource_type: str,
    ) -> Optional[Dict[str, Any]]:
        """Eager load a related resource."""
        return await self._lazy_load(field_info, resource_type)

    async def _batch_hydrate_with_handler(
        self,
        items: List[Dict[str, Any]],
        relations: List[str],
        handler: Callable,
    ) -> List[Dict[str, Any]]:
        """Batch hydrate using a registered handler."""
        fields = self._identify_fields(items[0], relations) if items else []

        if not fields:
            return items

        all_ids = set()
        for item in items:
            for field_info in fields:
                val = item.get(f"{field_info.name}_id")
                if val is not None:
                    all_ids.add(val)

        batch_results = await handler(list(all_ids))

        results = []
        for item in items:
            hydrated = item.copy()
            for field_info in fields:
                related_id = item.get(f"{field_info.name}_id")
                if related_id and related_id in batch_results:
                    hydrated[field_info.name] = batch_results[related_id]
            results.append(hydrated)

        return results

    def prefetch(
        self,
        resource_type: str,
        ids: List[Any],
    ) -> None:
        """
        Prefetch resources to populate cache.

        Args:
            resource_type: Type of resources to prefetch.
            ids: List of resource IDs to prefetch.
        """
        if self.config.cache_hydrated:
            for resource_id in ids:
                cache_key = f"{resource_type}:{resource_id}"
                if cache_key not in self._cache:
                    if resource_type not in self._pending:
                        self._pending[resource_type] = []
                    self._pending[resource_type].append(resource_id)

    def clear_cache(self) -> None:
        """Clear the hydration cache."""
        self._cache.clear()
        self._pending.clear()

    def get_cached(self, resource_type: str, resource_id: Any) -> Optional[Any]:
        """Get a cached hydrated resource."""
        cache_key = f"{resource_type}:{resource_id}"
        return self._cache.get(cache_key)

    def add_to_cache(self, resource_type: str, resource_id: Any, value: Any) -> None:
        """Add a value to the cache."""
        if self.config.cache_hydrated:
            cache_key = f"{resource_type}:{resource_id}"
            self._cache[cache_key] = value


def create_hydration_action(
    strategy: HydrationStrategy = HydrationStrategy.LAZY,
    **kwargs,
) -> ApiHydrationAction:
    """Factory function to create an ApiHydrationAction."""
    config = HydrationConfig(strategy=strategy, **kwargs)
    return ApiHydrationAction(config=config)
