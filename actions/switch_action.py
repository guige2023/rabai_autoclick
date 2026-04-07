"""Switch action module for RabAI AutoClick.

Provides switch/case operations:
- SwitchCaseAction: Switch case
- SwitchMatchAction: Match case
- SwitchDefaultAction: Default case
- SwitchEndAction: End switch
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SwitchCaseAction(BaseAction):
    """Switch case."""
    action_type = "switch_case"
    display_name = "开关情况"
    description = "开关情况分支"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute switch.

        Args:
            context: Execution context.
            params: Dict with value.

        Returns:
            ActionResult with switch value.
        """
        value = params.get('value', '')

        try:
            resolved_value = context.resolve_value(value)
            context.set('_switch_value', resolved_value)
            context.set('_switch_matched', False)

            return ActionResult(
                success=True,
                message=f"开关值: {resolved_value}",
                data={
                    'value': resolved_value,
                    'matched': False
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"开关失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class SwitchMatchAction(BaseAction):
    """Match case."""
    action_type = "switch_match"
    display_name = "匹配情况"
    description = "匹配情况分支"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute match.

        Args:
            context: Execution context.
            params: Dict with match_value.

        Returns:
            ActionResult with match result.
        """
        match_value = params.get('match_value', '')

        try:
            already_matched = context.get('_switch_matched', False)
            if already_matched:
                return ActionResult(
                    success=True,
                    message="已匹配，跳过",
                    data={'matched': False, 'skipped': True}
                )

            switch_value = context.get('_switch_value')
            resolved_match = context.resolve_value(match_value)

            result = (switch_value == resolved_match)

            if result:
                context.set('_switch_matched', True)

            return ActionResult(
                success=True,
                message=f"匹配{'成功' if result else '失败'}",
                data={
                    'match_value': resolved_match,
                    'switch_value': switch_value,
                    'matched': result
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"匹配失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['match_value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class SwitchDefaultAction(BaseAction):
    """Default case."""
    action_type = "switch_default"
    display_name = "默认情况"
    description = "默认情况分支"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute default.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating default.
        """
        already_matched = context.get('_switch_matched', False)

        if already_matched:
            return ActionResult(
                success=True,
                message="已匹配，跳过默认",
                data={'executed': False, 'skipped': True}
            )

        context.set('_switch_matched', True)

        return ActionResult(
            success=True,
            message="执行默认分支",
            data={'executed': True}
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class SwitchEndAction(BaseAction):
    """End switch."""
    action_type = "switch_end"
    display_name = "结束开关"
    description = "结束开关块"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute end switch.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating end.
        """
        context.delete('_switch_value')
        context.delete('_switch_matched')

        return ActionResult(
            success=True,
            message="开关块结束"
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
