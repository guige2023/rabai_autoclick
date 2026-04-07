"""Random2 action module for RabAI AutoClick.

Provides additional random operations:
- RandomIntAction: Random integer in range
- RandomFloatAction: Random float in range
- RandomChoiceAction: Random choice from list
- RandomSampleAction: Random sample from list
- RandomShuffleAction: Shuffle list
"""

import random
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RandomIntAction(BaseAction):
    """Random integer in range."""
    action_type = "random2_int"
    display_name = "随机整数"
    description = "生成指定范围内的随机整数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random int.

        Args:
            context: Execution context.
            params: Dict with min_val, max_val, output_var.

        Returns:
            ActionResult with random int.
        """
        min_val = params.get('min_val', 0)
        max_val = params.get('max_val', 100)
        output_var = params.get('output_var', 'random_int')

        try:
            resolved_min = int(context.resolve_value(min_val))
            resolved_max = int(context.resolve_value(max_val))

            result = random.randint(resolved_min, resolved_max)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机整数: {result}",
                data={
                    'min': resolved_min,
                    'max': resolved_max,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机整数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'min_val': 0, 'max_val': 100, 'output_var': 'random_int'}


class RandomFloatAction(BaseAction):
    """Random float in range."""
    action_type = "random2_float"
    display_name = "随机浮点数"
    description = "生成指定范围内的随机浮点数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random float.

        Args:
            context: Execution context.
            params: Dict with min_val, max_val, output_var.

        Returns:
            ActionResult with random float.
        """
        min_val = params.get('min_val', 0.0)
        max_val = params.get('max_val', 1.0)
        output_var = params.get('output_var', 'random_float')

        try:
            resolved_min = float(context.resolve_value(min_val))
            resolved_max = float(context.resolve_value(max_val))

            result = random.uniform(resolved_min, resolved_max)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机浮点数: {result}",
                data={
                    'min': resolved_min,
                    'max': resolved_max,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机浮点数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'min_val': 0.0, 'max_val': 1.0, 'output_var': 'random_float'}


class RandomChoiceAction(BaseAction):
    """Random choice from list."""
    action_type = "random2_choice"
    display_name = "随机选择"
    description = "从列表中随机选择一个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random choice.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with random choice.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'random_choice')

        try:
            resolved_items = context.resolve_value(items)

            if not resolved_items:
                return ActionResult(
                    success=False,
                    message="选择列表为空"
                )

            result = random.choice(resolved_items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机选择: {result}",
                data={
                    'item': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机选择失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'random_choice'}


class RandomSampleAction(BaseAction):
    """Random sample from list."""
    action_type = "random2_sample"
    display_name = "随机抽样"
    description = "从列表中随机抽取多个不重复的元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random sample.

        Args:
            context: Execution context.
            params: Dict with items, count, output_var.

        Returns:
            ActionResult with random sample.
        """
        items = params.get('items', [])
        count = params.get('count', 1)
        output_var = params.get('output_var', 'random_sample')

        try:
            resolved_items = context.resolve_value(items)
            resolved_count = int(context.resolve_value(count))

            if not resolved_items:
                return ActionResult(
                    success=False,
                    message="抽样列表为空"
                )

            if resolved_count > len(resolved_items):
                resolved_count = len(resolved_items)

            result = random.sample(resolved_items, resolved_count)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机抽样: {len(result)} 项",
                data={
                    'count': resolved_count,
                    'sample': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机抽样失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'random_sample'}


class RandomShuffleAction(BaseAction):
    """Shuffle list."""
    action_type = "random2_shuffle"
    display_name = "随机打乱"
    description = "随机打乱列表顺序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute shuffle.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with shuffled list.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'shuffled_list')

        try:
            resolved_items = context.resolve_value(items)

            if not resolved_items:
                return ActionResult(
                    success=False,
                    message="打乱列表为空"
                )

            result = list(resolved_items)
            random.shuffle(result)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机打乱: {len(result)} 项",
                data={
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机打乱失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'shuffled_list'}
