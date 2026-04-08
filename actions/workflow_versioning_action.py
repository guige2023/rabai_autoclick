"""
Workflow Versioning Action Module.

Versions workflow definitions, supports rollback to previous versions,
and tracks workflow evolution over time.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class WorkflowVersion:
    """A versioned workflow definition."""
    version: str
    workflow_id: str
    definition: dict[str, Any]
    created_at: float
    created_by: str
    changelog: str


@dataclass
class VersioningResult:
    """Result of versioning operation."""
    success: bool
    version: Optional[str]
    message: str


class WorkflowVersioningAction(BaseAction):
    """Version workflow definitions."""

    def __init__(self) -> None:
        super().__init__("workflow_versioning")
        self._versions: dict[str, list[WorkflowVersion]] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Create or retrieve workflow versions.

        Args:
            context: Execution context
            params: Parameters:
                - action: create, list, rollback, diff
                - workflow_id: Workflow identifier
                - version: Version string
                - definition: Workflow definition
                - changelog: Change description
                - created_by: Author of version

        Returns:
            VersioningResult or version list
        """
        import hashlib
        import time

        action = params.get("action", "create")
        workflow_id = params.get("workflow_id", "default")
        created_by = params.get("created_by", "system")

        if action == "create":
            version = params.get("version", "")
            definition = params.get("definition", {})
            changelog = params.get("changelog", "")

            if not version:
                version = hashlib.md5(str(definition).encode()).hexdigest()[:8]

            wf_version = WorkflowVersion(
                version=version,
                workflow_id=workflow_id,
                definition=definition,
                created_at=time.time(),
                created_by=created_by,
                changelog=changelog
            )

            if workflow_id not in self._versions:
                self._versions[workflow_id] = []
            self._versions[workflow_id].append(wf_version)

            return VersioningResult(True, version, f"Version {version} created").__dict__

        elif action == "list":
            versions = self._versions.get(workflow_id, [])
            return {
                "workflow_id": workflow_id,
                "versions": [vars(v) for v in versions]
            }

        elif action == "rollback":
            version = params.get("version")
            versions = self._versions.get(workflow_id, [])
            target = next((v for v in versions if v.version == version), None)
            if target:
                return {
                    "success": True,
                    "version": target.version,
                    "definition": target.definition,
                    "message": f"Rolled back to version {version}"
                }
            return VersioningResult(False, version, "Version not found").__dict__

        elif action == "diff":
            v1 = params.get("version1")
            v2 = params.get("version2")
            versions = self._versions.get(workflow_id, [])
            ver1 = next((v for v in versions if v.version == v1), None)
            ver2 = next((v for v in versions if v.version == v2), None)
            if ver1 and ver2:
                keys1 = set(ver1.definition.keys())
                keys2 = set(ver2.definition.keys())
                return {
                    "added": list(keys2 - keys1),
                    "removed": list(keys1 - keys2),
                    "common": list(keys1 & keys2)
                }
            return {"error": "Version(s) not found"}

        return {"error": f"Unknown action: {action}"}
