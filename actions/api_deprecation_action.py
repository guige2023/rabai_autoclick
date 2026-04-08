"""
API Deprecation Action Module.

Manages API deprecation lifecycle: sunset dates, migration guides,
breaking change notifications, and compatibility layers.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class DeprecationInfo:
    """API deprecation information."""
    endpoint: str
    deprecated_version: str
    sunset_date: Optional[str]
    migration_guide: str
    alternative: Optional[str]
    severity: str  # info, warning, critical


class APIDeprecationAction(BaseAction):
    """Manage API deprecation lifecycle."""

    def __init__(self) -> None:
        super().__init__("api_deprecation")
        self._deprecated: dict[str, DeprecationInfo] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Manage API deprecations.

        Args:
            context: Execution context
            params: Parameters:
                - action: deprecate, check, list, migrate
                - endpoint: Endpoint to deprecate
                - deprecated_version: Version that deprecated it
                - sunset_date: Sunset date (ISO format)
                - migration_guide: Migration instructions
                - alternative: Alternative endpoint/API
                - severity: info, warning, critical

        Returns:
            Deprecation status or check result
        """
        action = params.get("action", "check")
        endpoint = params.get("endpoint", "")

        if action == "deprecate":
            info = DeprecationInfo(
                endpoint=endpoint,
                deprecated_version=params.get("deprecated_version", ""),
                sunset_date=params.get("sunset_date"),
                migration_guide=params.get("migration_guide", ""),
                alternative=params.get("alternative"),
                severity=params.get("severity", "warning")
            )
            self._deprecated[endpoint] = info
            return {"deprecated": True, "endpoint": endpoint, "severity": info.severity}

        elif action == "check":
            info = self._deprecated.get(endpoint)
            if info:
                return {
                    "deprecated": True,
                    "version": info.deprecated_version,
                    "sunset_date": info.sunset_date,
                    "migration_guide": info.migration_guide,
                    "alternative": info.alternative,
                    "severity": info.severity
                }
            return {"deprecated": False, "endpoint": endpoint}

        elif action == "list":
            return {
                "deprecated_endpoints": [vars(v) for v in self._deprecated.values()]
            }

        elif action == "migrate":
            info = self._deprecated.get(endpoint, {})
            alternative = info.alternative if hasattr(info, 'alternative') else None
            return {
                "migration_guide": info.migration_guide if hasattr(info, 'migration_guide') else "",
                "alternative": alternative
            }

        return {"error": f"Unknown action: {action}"}
