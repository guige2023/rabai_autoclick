"""Semver13 action module for RabAI AutoClick.

Provides additional semantic versioning operations:
- SemverParseAction: Parse semantic version
- SemverCompareAction: Compare semantic versions
- SemverBumpMajorAction: Bump major version
- SemverBumpMinorAction: Bump minor version
- SemverBumpPatchAction: Bump patch version
- SemverToStringAction: Convert to string
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SemverParseAction(BaseAction):
    """Parse semantic version."""
    action_type = "semver13_parse"
    display_name = "解析语义版本"
    description = "解析语义版本号"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute semver parse.

        Args:
            context: Execution context.
            params: Dict with version, output_var.

        Returns:
            ActionResult with parsed version.
        """
        version = params.get('version', '1.0.0')
        output_var = params.get('output_var', 'parsed_version')

        try:
            import re

            resolved = context.resolve_value(version)

            # Parse semver format: major.minor.patch[-prerelease][+build]
            pattern = r'^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.-]+))?(?:\+([a-zA-Z0-9.-]+))?$'
            match = re.match(pattern, resolved)

            if not match:
                return ActionResult(
                    success=False,
                    message=f"无效的语义版本: {resolved}"
                )

            result = {
                'major': int(match.group(1)),
                'minor': int(match.group(2)),
                'patch': int(match.group(3)),
                'prerelease': match.group(4) or '',
                'build': match.group(5) or ''
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"解析语义版本: {resolved}",
                data={
                    'original': resolved,
                    'version': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析语义版本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['version']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_version'}


class SemverCompareAction(BaseAction):
    """Compare semantic versions."""
    action_type = "semver13_compare"
    display_name = "比较语义版本"
    description = "比较两个语义版本"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute semver compare.

        Args:
            context: Execution context.
            params: Dict with version1, version2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        version1 = params.get('version1', '1.0.0')
        version2 = params.get('version2', '1.0.0')
        output_var = params.get('output_var', 'compare_result')

        try:
            import re

            resolved1 = context.resolve_value(version1)
            resolved2 = context.resolve_value(version2)

            def parse_semver(v):
                pattern = r'^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.-]+))?(?:\+([a-zA-Z0-9.-]+))?$'
                match = re.match(pattern, v)
                if not match:
                    return None
                return {
                    'major': int(match.group(1)),
                    'minor': int(match.group(2)),
                    'patch': int(match.group(3)),
                    'prerelease': match.group(4) or '',
                    'build': match.group(5) or ''
                }

            v1 = parse_semver(resolved1)
            v2 = parse_semver(resolved2)

            if v1 is None or v2 is None:
                return ActionResult(
                    success=False,
                    message=f"无效的语义版本"
                )

            # Compare versions
            for key in ['major', 'minor', 'patch']:
                if v1[key] > v2[key]:
                    result = 1
                    break
                elif v1[key] < v2[key]:
                    result = -1
                    break
            else:
                # Compare prereleases
                if v1['prerelease'] and not v2['prerelease']:
                    result = -1
                elif not v1['prerelease'] and v2['prerelease']:
                    result = 1
                else:
                    result = 0

            context.set(output_var, result)

            comparison = {
                -1: '小于',
                0: '等于',
                1: '大于'
            }

            return ActionResult(
                success=True,
                message=f"比较结果: {comparison[result]}",
                data={
                    'version1': resolved1,
                    'version2': resolved2,
                    'result': result,
                    'comparison': comparison[result],
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"比较语义版本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['version1', 'version2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class SemverBumpMajorAction(BaseAction):
    """Bump major version."""
    action_type = "semver13_bump_major"
    display_name: "增加主版本号"
    description = "增加主版本号"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bump major.

        Args:
            context: Execution context.
            params: Dict with version, output_var.

        Returns:
            ActionResult with bumped version.
        """
        version = params.get('version', '1.0.0')
        output_var = params.get('output_var', 'bumped_version')

        try:
            import re

            resolved = context.resolve_value(version)

            pattern = r'^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.-]+))?(?:\+([a-zA-Z0-9.-]+))?$'
            match = re.match(pattern, resolved)

            if not match:
                return ActionResult(
                    success=False,
                    message=f"无效的语义版本: {resolved}"
                )

            major = int(match.group(1)) + 1
            minor = 0
            patch = 0
            prerelease = match.group(4) or ''
            build = match.group(5) or ''

            result = f"{major}.{minor}.{patch}"
            if prerelease:
                result += f"-{prerelease}"
            if build:
                result += f"+{build}"

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"增加主版本号: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"增加主版本号失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['version']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bumped_version'}


class SemverBumpMinorAction(BaseAction):
    """Bump minor version."""
    action_type = "semver13_bump_minor"
    display_name = "增加次版本号"
    description = "增加次版本号"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bump minor.

        Args:
            context: Execution context.
            params: Dict with version, output_var.

        Returns:
            ActionResult with bumped version.
        """
        version = params.get('version', '1.0.0')
        output_var = params.get('output_var', 'bumped_version')

        try:
            import re

            resolved = context.resolve_value(version)

            pattern = r'^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.-]+))?(?:\+([a-zA-Z0-9.-]+))?$'
            match = re.match(pattern, resolved)

            if not match:
                return ActionResult(
                    success=False,
                    message=f"无效的语义版本: {resolved}"
                )

            major = int(match.group(1))
            minor = int(match.group(2)) + 1
            patch = 0
            prerelease = match.group(4) or ''
            build = match.group(5) or ''

            result = f"{major}.{minor}.{patch}"
            if prerelease:
                result += f"-{prerelease}"
            if build:
                result += f"+{build}"

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"增加次版本号: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"增加次版本号失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['version']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bumped_version'}


class SemverBumpPatchAction(BaseAction):
    """Bump patch version."""
    action_type = "semver13_bump_patch"
    display_name = "增加修订号"
    description = "增加修订号"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bump patch.

        Args:
            context: Execution context.
            params: Dict with version, output_var.

        Returns:
            ActionResult with bumped version.
        """
        version = params.get('version', '1.0.0')
        output_var = params.get('output_var', 'bumped_version')

        try:
            import re

            resolved = context.resolve_value(version)

            pattern = r'^(\d+)\.(\d+)\.(\d+)(?:-([a-zA-Z0-9.-]+))?(?:\+([a-zA-Z0-9.-]+))?$'
            match = re.match(pattern, resolved)

            if not match:
                return ActionResult(
                    success=False,
                    message=f"无效的语义版本: {resolved}"
                )

            major = int(match.group(1))
            minor = int(match.group(2))
            patch = int(match.group(3)) + 1
            prerelease = match.group(4) or ''
            build = match.group(5) or ''

            result = f"{major}.{minor}.{patch}"
            if prerelease:
                result += f"-{prerelease}"
            if build:
                result += f"+{build}"

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"增加修订号: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"增加修订号失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['version']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bumped_version'}


class SemverToStringAction(BaseAction):
    """Convert to string."""
    action_type = "semver13_tostring"
    display_name = "版本转字符串"
    description = "将版本对象转换为字符串"
    version = "13.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to string.

        Args:
            context: Execution context.
            params: Dict with major, minor, patch, prerelease, build, output_var.

        Returns:
            ActionResult with version string.
        """
        major = params.get('major', 1)
        minor = params.get('minor', 0)
        patch = params.get('patch', 0)
        prerelease = params.get('prerelease', '')
        build = params.get('build', '')
        output_var = params.get('output_var', 'version_string')

        try:
            resolved_major = int(context.resolve_value(major)) if major else 1
            resolved_minor = int(context.resolve_value(minor)) if minor else 0
            resolved_patch = int(context.resolve_value(patch)) if patch else 0
            resolved_prerelease = context.resolve_value(prerelease) if prerelease else ''
            resolved_build = context.resolve_value(build) if build else ''

            result = f"{resolved_major}.{resolved_minor}.{resolved_patch}"
            if resolved_prerelease:
                result += f"-{resolved_prerelease}"
            if resolved_build:
                result += f"+{resolved_build}"

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"版本字符串: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"版本转字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['major', 'minor', 'patch']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'prerelease': '', 'build': '', 'output_var': 'version_string'}