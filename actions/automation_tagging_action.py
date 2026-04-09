"""Automation tagging action for resource tagging and categorization.

Manages tags on automation resources for organization,
filtering, and cost tracking with bulk operations.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class Tag:
    """A tag on a resource."""
    key: str
    value: str
    created_at: float = field(default_factory=lambda: __import__("time").time())
    updated_at: float = field(default_factory=lambda: __import__("time").time())


@dataclass
class TaggedResource:
    """A resource with tags."""
    resource_id: str
    resource_type: str
    tags: dict[str, Tag] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaggingStats:
    """Statistics for tagging operations."""
    total_resources: int = 0
    total_tags: int = 0
    tag_operations: int = 0
    bulk_operations: int = 0


@dataclass
class TaggingResult:
    """Result of a tagging operation."""
    success: bool
    resources_updated: int
    tags_updated: int
    errors: list[str] = field(default_factory=list)


class AutomationTaggingAction:
    """Manage tags on automation resources.

    Example:
        >>> tagging = AutomationTaggingAction()
        >>> tagging.tag_resource("server-1", "server", {"env": "prod", "team": "infra"})
        >>> resources = tagging.filter_by_tags(tags={"env": "prod"})
    """

    def __init__(self) -> None:
        self._resources: dict[str, TaggedResource] = {}
        self._tag_index: dict[str, set[str]] = defaultdict(set)
        self._stats = TaggingStats()

    def tag_resource(
        self,
        resource_id: str,
        resource_type: str,
        tags: dict[str, str],
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Tag a resource with one or more tags.

        Args:
            resource_id: Unique resource identifier.
            resource_type: Type of resource.
            tags: Dictionary of tag key-value pairs.
            metadata: Optional resource metadata.

        Returns:
            True if tagging was successful.
        """
        import time

        if resource_id not in self._resources:
            self._resources[resource_id] = TaggedResource(
                resource_id=resource_id,
                resource_type=resource_type,
            )
            self._stats.total_resources += 1

        resource = self._resources[resource_id]

        for key, value in tags.items():
            old_tag = resource.tags.get(key)
            resource.tags[key] = Tag(key=key, value=value)
            self._tag_index[key].add(resource_id)
            self._stats.tag_operations += 1

            if old_tag:
                if old_tag.value != value:
                    self._update_tag_index(key, resource_id, old_tag.value, value)

        if metadata:
            resource.metadata.update(metadata)

        logger.info(f"Tagged resource {resource_id} with {len(tags)} tags")
        return True

    def untag_resource(
        self,
        resource_id: str,
        tag_keys: list[str],
    ) -> bool:
        """Remove tags from a resource.

        Args:
            resource_id: Resource identifier.
            tag_keys: List of tag keys to remove.

        Returns:
            True if untagging was successful.
        """
        if resource_id not in self._resources:
            return False

        resource = self._resources[resource_id]

        for key in tag_keys:
            if key in resource.tags:
                old_value = resource.tags[key].value
                del resource.tags[key]
                self._tag_index[key].discard(resource_id)
                self._stats.tag_operations += 1

        return True

    def get_tags(self, resource_id: str) -> dict[str, str]:
        """Get all tags for a resource.

        Args:
            resource_id: Resource identifier.

        Returns:
            Dictionary of tag key-value pairs.
        """
        if resource_id not in self._resources:
            return {}

        return {
            key: tag.value
            for key, tag in self._resources[resource_id].tags.items()
        }

    def find_resources_by_tag(
        self,
        tag_key: str,
        tag_value: Optional[str] = None,
    ) -> list[str]:
        """Find resources by tag.

        Args:
            tag_key: Tag key to search.
            tag_value: Optional tag value to match.

        Returns:
            List of matching resource IDs.
        """
        resource_ids = self._tag_index.get(tag_key, set())

        if tag_value is not None:
            return [
                rid for rid in resource_ids
                if self._resources[rid].tags.get(tag_key, Tag("", "")).value == tag_value
            ]

        return list(resource_ids)

    def filter_by_tags(
        self,
        tags: dict[str, str],
        match_all: bool = True,
    ) -> list[TaggedResource]:
        """Filter resources by tags.

        Args:
            tags: Tags to filter by.
            match_all: If True, match all tags; if False, match any.

        Returns:
            List of matching resources.
        """
        result: list[TaggedResource] = []

        for resource in self._resources.values():
            if match_all:
                if all(
                    resource.tags.get(k, Tag("", "")).value == v
                    for k, v in tags.items()
                ):
                    result.append(resource)
            else:
                if any(
                    resource.tags.get(k, Tag("", "")).value == v
                    for k, v in tags.items()
                ):
                    result.append(resource)

        return result

    def bulk_tag(
        self,
        resource_ids: list[str],
        tags: dict[str, str],
    ) -> TaggingResult:
        """Apply tags to multiple resources.

        Args:
            resource_ids: List of resource IDs.
            tags: Tags to apply.

        Returns:
            Tagging result with statistics.
        """
        errors: list[str] = []
        resources_updated = 0
        tags_updated = 0

        for resource_id in resource_ids:
            try:
                if resource_id in self._resources:
                    resource = self._resources[resource_id]
                    before_count = len(resource.tags)

                    for key, value in tags.items():
                        resource.tags[key] = Tag(key=key, value=value)
                        self._tag_index[key].add(resource_id)
                        tags_updated += 1

                    if len(resource.tags) > before_count:
                        resources_updated += 1
                else:
                    self.tag_resource(resource_id, "unknown", tags)
                    resources_updated += 1
                    tags_updated += len(tags)

            except Exception as e:
                errors.append(f"{resource_id}: {str(e)}")

        self._stats.bulk_operations += 1
        self._stats.tag_operations += tags_updated

        return TaggingResult(
            success=len(errors) == 0,
            resources_updated=resources_updated,
            tags_updated=tags_updated,
            errors=errors,
        )

    def get_tag_summary(self) -> dict[str, Any]:
        """Get summary of all tags.

        Returns:
            Tag summary with usage statistics.
        """
        tag_usage: dict[str, int] = {}
        resource_types: dict[str, int] = defaultdict(int)

        for resource in self._resources.values():
            resource_types[resource.resource_type] += 1
            for key in resource.tags.keys():
                tag_usage[key] = tag_usage.get(key, 0) + 1

        return {
            "total_resources": len(self._resources),
            "total_tags": sum(len(r.tags) for r in self._resources.values()),
            "unique_tag_keys": len(tag_usage),
            "tag_usage": tag_usage,
            "resource_types": dict(resource_types),
        }

    def copy_tags(
        self,
        source_resource_id: str,
        target_resource_id: str,
    ) -> bool:
        """Copy tags from one resource to another.

        Args:
            source_resource_id: Source resource.
            target_resource_id: Target resource.

        Returns:
            True if copy was successful.
        """
        if source_resource_id not in self._resources:
            return False

        source_tags = self.get_tags(source_resource_id)
        if not source_tags:
            return False

        if target_resource_id not in self._resources:
            self._resources[target_resource_id] = TaggedResource(
                resource_id=target_resource_id,
                resource_type=self._resources[source_resource_id].resource_type,
            )

        resource = self._resources[target_resource_id]

        for key, value in source_tags.items():
            resource.tags[key] = Tag(key=key, value=value)
            self._tag_index[key].add(target_resource_id)

        return True

    def _update_tag_index(
        self,
        key: str,
        resource_id: str,
        old_value: str,
        new_value: str,
    ) -> None:
        """Update tag index when value changes.

        Args:
            key: Tag key.
            resource_id: Resource ID.
            old_value: Old tag value.
            new_value: New tag value.
        """
        self._tag_index[key].add(resource_id)

    def get_stats(self) -> TaggingStats:
        """Get tagging statistics.

        Returns:
            Current statistics.
        """
        self._stats.total_resources = len(self._resources)
        self._stats.total_tags = sum(len(r.tags) for r in self._resources.values())
        return self._stats
