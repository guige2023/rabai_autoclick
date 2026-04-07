"""Constant action module for RabAI AutoClick.

Provides constant value operations:
- ConstantPiAction: Get pi
- ConstantEAction: Get e (Euler's number)
- ConstantTauAction: Get tau
- ConstantInfAction: Get infinity
- ConstantNanAction: Get NaN
"""

import math
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConstantPiAction(BaseAction):
    """Get pi."""
    action_type = "constant_pi"
    display_name = "获取PI"
    description = "获取圆周率π"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get pi.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with pi.
        """
        output_var = params.get('output_var', 'pi_value')

        try:
            result = math.pi
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"PI: {result}",
                data={
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取PI失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'pi_value'}


class ConstantEAction(BaseAction):
    """Get e (Euler's number)."""
    action_type = "constant_e"
    display_name = "获取E"
    description = "获取自然常数e"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get e.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with e.
        """
        output_var = params.get('output_var', 'e_value')

        try:
            result = math.e
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"E: {result}",
                data={
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取E失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'e_value'}


class ConstantTauAction(BaseAction):
    """Get tau."""
    action_type = "constant_tau"
    display_name = "获取TAU"
    description = "获取常数τ (2π)"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get tau.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with tau.
        """
        output_var = params.get('output_var', 'tau_value')

        try:
            result = math.tau
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"TAU: {result}",
                data={
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取TAU失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tau_value'}


class ConstantInfAction(BaseAction):
    """Get infinity."""
    action_type = "constant_inf"
    display_name = "获取无穷"
    description = "获取无穷大"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get infinity.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with infinity.
        """
        output_var = params.get('output_var', 'inf_value')

        try:
            result = math.inf
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"无穷: {result}",
                data={
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取无穷失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'inf_value'}


class ConstantNanAction(BaseAction):
    """Get NaN."""
    action_type = "constant_nan"
    display_name = "获取NaN"
    description = "获取非数字"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get NaN.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with NaN.
        """
        output_var = params.get('output_var', 'nan_value')

        try:
            result = math.nan
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"NaN: {result}",
                data={
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取NaN失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'nan_value'}
