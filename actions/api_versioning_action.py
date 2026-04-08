"""API Versioning action module for RabAI AutoClick.

Provides API versioning operations:
- APIVersionParseAction: Parse version string
- APIVersionCompareAction: Compare versions
- APIVersionRouteAction: Route by API version
- APIVersionMigrateAction: Version migration
"""

from __future__ import annotations

import sys
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APIVersionParseAction(BaseAction):
    """Parse API version string."""
    action_type = "api_version_parse"
    display_name = "API版本解析"
    description = "解析API版本字符串"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute version parsing."""
        version_str = params.get('version', '')
        format_type = params.get('format', 'semver')
        output_var = params.get('output_var', 'parsed_version')

        if not version_str:
            return ActionResult(success=False, message="version is required")

        try:
            resolved_version = context.resolve_value(version_str) if context else version_str

            major, minor, patch = 0, 0, 0
            prerelease = ''
            build = ''

            semver_pattern = r'(\d+)\.?(\d*)\.?(\d*)'
            if '-' in resolved_version or '+' in resolved_version:
                main_part = re.split(r'[-+]', resolved_version)[0]
                match = re.match(semver_pattern, main_part)
                if match:
                    major = int(match.group(1))
                    minor = int(match.group(2)) if match.group(2) else 0
                    patch = int(match.group(3)) if match.group(3) else 0
                if '-' in resolved_version:
                    prerelease = resolved_version.split('-')[1].split('+')[0]
                if '+' in resolved_version:
                    build = resolved_version.split('+')[1]
            else:
                parts = resolved_version.replace('v', '').split('.')
                major = int(parts[0]) if len(parts) > 0 else 0
                minor = int(parts[1]) if len(parts) > 1 else 0
                patch = int(parts[2]) if len(parts) > 2 else 0

            result = {
                'major': major,
                'minor': minor,
                'patch': patch,
                'prerelease': prerelease,
                'build': build,
                'string': resolved_version,
                'tuple': (major, minor, patch),
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Parsed: v{major}.{minor}.{patch}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Version parse error: {e}")


class APIVersionCompareAction(BaseAction):
    """Compare API versions."""
    action_type = "api_version_compare"
    display_name = "API版本比较"
    description = "比较API版本"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute version comparison."""
        version1 = params.get('version1', '')
        version2 = params.get('version2', '')
        output_var = params.get('output_var', 'compare_result')

        if not version1 or not version2:
            return ActionResult(success=False, message="version1 and version2 are required")

        try:
            import packaging.version

            v1 = context.resolve_value(version1) if context else version1
            v2 = context.resolve_value(version2) if context else version2

            try:
                parsed_v1 = packaging.version.parse(v1)
                parsed_v2 = packaging.version.parse(v2)

                if parsed_v1 > parsed_v2:
                    comparison = 1
                elif parsed_v1 < parsed_v2:
                    comparison = -1
                else:
                    comparison = 0
            except Exception:
                v1_parts = [int(x) for x in re.findall(r'\d+', v1)]
                v2_parts = [int(x) for x in re.findall(r'\d+', v2)]
                v1_parts = (v1_parts + [0, 0])[:3]
                v2_parts = (v2_parts + [0, 0])[:3]

                if v1_parts > v2_parts:
                    comparison = 1
                elif v1_parts < v2_parts:
                    comparison = -1
                else:
                    comparison = 0

            result = {
                'version1': v1,
                'version2': v2,
                'comparison': comparison,
                'greater': comparison > 0,
                'equal': comparison == 0,
                'less': comparison < 0,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"{v1} {'>' if comparison > 0 else '<' if comparison < 0 else '='} {v2}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Version compare error: {e}")


class APIVersionRouteAction(BaseAction):
    """Route by API version."""
    action_type = "api_version_route"
    display_name = "API版本路由"
    description = "按API版本路由"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute version routing."""
        version = params.get('version', '')
        routes = params.get('routes', {})
        default_handler = params.get('default', '')
        output_var = params.get('output_var', 'route_result')

        if not version or not routes:
            return ActionResult(success=False, message="version and routes are required")

        try:
            import packaging.version

            resolved_version = context.resolve_value(version) if context else version
            resolved_routes = context.resolve_value(routes) if context else routes
            resolved_default = context.resolve_value(default_handler) if context else default_handler

            matched_handler = resolved_default
            matched_version = None

            for route_version, handler in resolved_routes.items():
                try:
                    route_v = packaging.version.parse(route_version)
                    req_v = packaging.version.parse(resolved_version)
                    if req_v >= route_v:
                        if matched_version is None or route_v >= packaging.version.parse(matched_version):
                            matched_handler = handler
                            matched_version = route_version
                except Exception:
                    if resolved_version.startswith(route_version):
                        matched_handler = handler
                        matched_version = route_version

            result = {
                'requested_version': resolved_version,
                'matched_handler': matched_handler,
                'matched_version': matched_version,
                'routed': matched_handler != resolved_default,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Routed to {matched_handler}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Version route error: {e}")


class APIVersionMigrateAction(BaseAction):
    """Version migration."""
    action_type = "api_version_migrate"
    display_name = "API版本迁移"
    description = "API版本迁移"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute version migration."""
        data = params.get('data', {})
        from_version = params.get('from_version', '')
        to_version = params.get('to_version', '')
        migrations = params.get('migrations', [])
        output_var = params.get('output_var', 'migrated_data')

        if not data or not from_version or not to_version:
            return ActionResult(success=False, message="data, from_version, and to_version are required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_migrations = context.resolve_value(migrations) if context else migrations

            migrated = resolved_data.copy()
            migration_count = 0

            for migration in resolved_migrations:
                mf = migration.get('from')
                mt = migration.get('to')
                transforms = migration.get('transforms', [])

                if mf == from_version and mt == to_version:
                    for transform in transforms:
                        field = transform.get('field')
                        operation = transform.get('operation', 'rename')
                        value = transform.get('value')

                        if operation == 'rename' and field in migrated:
                            migrated[value] = migrated.pop(field)
                            migration_count += 1
                        elif operation == 'remove' and field in migrated:
                            del migrated[field]
                            migration_count += 1
                        elif operation == 'add':
                            migrated[field] = value
                            migration_count += 1
                        elif operation == 'transform':
                            if field in migrated:
                                migrated[field] = value(migrated[field])
                                migration_count += 1

            result = {
                'migrated_data': migrated,
                'from_version': from_version,
                'to_version': to_version,
                'changes_applied': migration_count,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Migrated: {migration_count} changes"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Migration error: {e}")
