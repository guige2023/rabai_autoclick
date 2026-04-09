"""Workflow version control action for managing workflow versions.

Provides versioning, diffing, rollback, and branching
for workflow definitions.
"""

import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"


@dataclass
class WorkflowVersion:
    version_id: str
    workflow_id: str
    version_number: int
    definition: dict[str, Any]
    checksum: str
    created_at: float = field(default_factory=time.time)
    created_by: str = "system"
    description: str = ""
    tags: list[str] = field(default_factory=list)


@dataclass
class WorkflowDiff:
    workflow_id: str
    from_version: int
    to_version: int
    changes: list[dict[str, Any]] = field(default_factory=list)


class WorkflowVersionControlAction:
    """Manage workflow versions with versioning and rollback.

    Args:
        max_versions_per_workflow: Maximum versions to retain per workflow.
        auto_create_on_change: Automatically create version on workflow change.
    """

    def __init__(
        self,
        max_versions_per_workflow: int = 50,
        auto_create_on_change: bool = True,
    ) -> None:
        self._versions: dict[str, list[WorkflowVersion]] = {}
        self._workflow_definitions: dict[str, dict[str, Any]] = {}
        self._max_versions = max_versions_per_workflow
        self._auto_create = auto_create_on_change
        self._change_listeners: list[Callable[[str, WorkflowVersion], None]] = []

    def _generate_version_id(self, workflow_id: str, version_number: int) -> str:
        """Generate a unique version ID.

        Args:
            workflow_id: Workflow ID.
            version_number: Version number.

        Returns:
            Version ID string.
        """
        data = f"{workflow_id}:{version_number}:{time.time()}"
        return hashlib.sha256(data.encode()).hexdigest()[:12]

    def _compute_checksum(self, definition: dict[str, Any]) -> str:
        """Compute checksum for a workflow definition.

        Args:
            definition: Workflow definition.

        Returns:
            Checksum string.
        """
        import json
        content = json.dumps(definition, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def create_version(
        self,
        workflow_id: str,
        definition: dict[str, Any],
        description: str = "",
        created_by: str = "system",
        tags: Optional[list[str]] = None,
    ) -> WorkflowVersion:
        """Create a new version of a workflow.

        Args:
            workflow_id: Workflow ID.
            definition: Workflow definition.
            description: Version description.
            created_by: Creator identifier.
            tags: Optional version tags.

        Returns:
            Created workflow version.
        """
        if workflow_id not in self._versions:
            self._versions[workflow_id] = []
            self._workflow_definitions[workflow_id] = {}

        version_number = len(self._versions[workflow_id]) + 1
        version_id = self._generate_version_id(workflow_id, version_number)
        checksum = self._compute_checksum(definition)

        version = WorkflowVersion(
            version_id=version_id,
            workflow_id=workflow_id,
            version_number=version_number,
            definition=definition,
            checksum=checksum,
            created_by=created_by,
            description=description,
            tags=tags or [],
        )

        self._versions[workflow_id].append(version)
        self._workflow_definitions[workflow_id] = definition

        if len(self._versions[workflow_id]) > self._max_versions:
            self._versions[workflow_id].pop(0)

        for listener in self._change_listeners:
            try:
                listener(workflow_id, version)
            except Exception as e:
                logger.error(f"Change listener error: {e}")

        logger.debug(f"Created version {version_number} for workflow {workflow_id}")
        return version

    def get_version(
        self,
        workflow_id: str,
        version_number: Optional[int] = None,
    ) -> Optional[WorkflowVersion]:
        """Get a specific version of a workflow.

        Args:
            workflow_id: Workflow ID.
            version_number: Version number (latest if None).

        Returns:
            Workflow version or None.
        """
        versions = self._versions.get(workflow_id, [])
        if not versions:
            return None

        if version_number is None:
            return versions[-1]

        for v in versions:
            if v.version_number == version_number:
                return v
        return None

    def get_all_versions(self, workflow_id: str) -> list[WorkflowVersion]:
        """Get all versions of a workflow.

        Args:
            workflow_id: Workflow ID.

        Returns:
            List of versions (oldest first).
        """
        return self._versions.get(workflow_id, []).copy()

    def get_latest_version(self, workflow_id: str) -> Optional[WorkflowVersion]:
        """Get the latest version of a workflow.

        Args:
            workflow_id: Workflow ID.

        Returns:
            Latest version or None.
        """
        versions = self._versions.get(workflow_id, [])
        return versions[-1] if versions else None

    def rollback(self, workflow_id: str, version_number: int) -> bool:
        """Rollback a workflow to a previous version.

        Args:
            workflow_id: Workflow ID.
            version_number: Version number to rollback to.

        Returns:
            True if rollback was successful.
        """
        version = self.get_version(workflow_id, version_number)
        if not version:
            return False

        self.create_version(
            workflow_id=workflow_id,
            definition=version.definition,
            description=f"Rollback to version {version_number}",
            created_by="system",
        )
        return True

    def diff(
        self,
        workflow_id: str,
        from_version: int,
        to_version: int,
    ) -> Optional[WorkflowDiff]:
        """Compute diff between two versions.

        Args:
            workflow_id: Workflow ID.
            from_version: Source version number.
            to_version: Target version number.

        Returns:
            Workflow diff or None.
        """
        from_ver = self.get_version(workflow_id, from_version)
        to_ver = self.get_version(workflow_id, to_version)

        if not from_ver or not to_ver:
            return None

        changes = self._compute_definition_diff(from_ver.definition, to_ver.definition)

        return WorkflowDiff(
            workflow_id=workflow_id,
            from_version=from_version,
            to_version=to_version,
            changes=changes,
        )

    def _compute_definition_diff(
        self,
        from_def: dict[str, Any],
        to_def: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Compute differences between two definitions.

        Args:
            from_def: Source definition.
            to_def: Target definition.

        Returns:
            List of changes.
        """
        changes = []
        all_keys = set(from_def.keys()) | set(to_def.keys())

        for key in all_keys:
            if key not in from_def:
                changes.append({
                    "path": key,
                    "type": ChangeType.ADDED.value,
                    "value": to_def[key],
                })
            elif key not in to_def:
                changes.append({
                    "path": key,
                    "type": ChangeType.DELETED.value,
                    "value": from_def[key],
                })
            elif from_def[key] != to_def[key]:
                changes.append({
                    "path": key,
                    "type": ChangeType.MODIFIED.value,
                    "from": from_def[key],
                    "to": to_def[key],
                })

        return changes

    def get_current_definition(self, workflow_id: str) -> Optional[dict[str, Any]]:
        """Get the current definition of a workflow.

        Args:
            workflow_id: Workflow ID.

        Returns:
            Current definition or None.
        """
        return self._workflow_definitions.get(workflow_id)

    def register_change_listener(
        self,
        listener: Callable[[str, WorkflowVersion], None],
    ) -> None:
        """Register a listener for version changes.

        Args:
            listener: Callback function.
        """
        self._change_listeners.append(listener)

    def get_stats(self) -> dict[str, Any]:
        """Get version control statistics.

        Returns:
            Dictionary with stats.
        """
        total_workflows = len(self._versions)
        total_versions = sum(len(v) for v in self._versions.values())

        return {
            "total_workflows": total_workflows,
            "total_versions": total_versions,
            "max_versions_per_workflow": self._max_versions,
            "auto_create_on_change": self._auto_create,
        }

    def delete_workflow(self, workflow_id: str) -> bool:
        """Delete all versions of a workflow.

        Args:
            workflow_id: Workflow ID.

        Returns:
            True if workflow was deleted.
        """
        if workflow_id in self._versions:
            del self._versions[workflow_id]
        if workflow_id in self._workflow_definitions:
            del self._workflow_definitions[workflow_id]
        return True
