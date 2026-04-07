"""Random8 action module for RabAI AutoClick.

Provides additional random operations:
- RandomIntAction: Generate random integer
- RandomFloatAction: Generate random float
- RandomChoiceAction: Random choice from list
- RandomSampleAction: Random sample from list
- RandomShuffleAction: Shuffle list
- RandomUUIDAction: Generate random UUID
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RandomIntAction(BaseAction):
    """Generate random integer."""
    action_type = "random8_int"
    display_name = "随机整数"
    description = "生成随机整数"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random int.

        Args:
            context: Execution context.
            params: Dict with min, max, output_var.

        Returns:
            ActionResult with random int.
        """
        min_val = params.get('min', 0)
        max_val = params.get('max', 100)
        output_var = params.get('output_var', 'random_int')

        try:
            import random

            resolved_min = int(context.resolve_value(min_val)) if min_val else 0
            resolved_max = int(context.resolve_value(max_val)) if max_val else 100

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
        return {'min': 0, 'max': 100, 'output_var': 'random_int'}


class RandomFloatAction(BaseAction):
    """Generate random float."""
    action_type = "random8_float"
    display_name = "随机浮点数"
    description = "生成随机浮点数"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random float.

        Args:
            context: Execution context.
            params: Dict with min, max, output_var.

        Returns:
            ActionResult with random float.
        """
        min_val = params.get('min', 0.0)
        max_val = params.get('max', 1.0)
        output_var = params.get('output_var', 'random_float')

        try:
            import random

            resolved_min = float(context.resolve_value(min_val)) if min_val else 0.0
            resolved_max = float(context.resolve_value(max_val)) if max_val else 1.0

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
        return {'min': 0.0, 'max': 1.0, 'output_var': 'random_float'}


class RandomChoiceAction(BaseAction):
    """Random choice from list."""
    action_type = "random8_choice"
    display_name = "随机选择"
    description = "从列表随机选择一个元素"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random choice.

        Args:
            context: Execution context.
            params: Dict with list, output_var.

        Returns:
            ActionResult with random choice.
        """
        list_param = params.get('list', [])
        output_var = params.get('output_var', 'random_choice')

        try:
            import random

            resolved_list = context.resolve_value(list_param)

            if not isinstance(resolved_list, (list, tuple)):
                resolved_list = [resolved_list]

            result = random.choice(resolved_list)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机选择: {result}",
                data={
                    'list': resolved_list,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机选择失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'random_choice'}


class RandomSampleAction(BaseAction):
    """Random sample from list."""
    action_type = "random8_sample"
    display_name = "随机抽样"
    description = "从列表随机抽样"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random sample.

        Args:
            context: Execution context.
            params: Dict with list, count, output_var.

        Returns:
            ActionResult with random sample.
        """
        list_param = params.get('list', [])
        count = params.get('count', 1)
        output_var = params.get('output_var', 'random_sample')

        try:
            import random

            resolved_list = context.resolve_value(list_param)
            resolved_count = int(context.resolve_value(count)) if count else 1

            if not isinstance(resolved_list, (list, tuple)):
                resolved_list = [resolved_list]

            if resolved_count > len(resolved_list):
                resolved_count = len(resolved_list)

            result = random.sample(resolved_list, resolved_count)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机抽样: {len(result)}项",
                data={
                    'list': resolved_list,
                    'count': resolved_count,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机抽样失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'random_sample'}


class RandomShuffleAction(BaseAction):
    """Shuffle list."""
    action_type = "random8_shuffle"
    display_name = "随机打乱"
    description = "随机打乱列表"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute shuffle.

        Args:
            context: Execution context.
            params: Dict with list, output_var.

        Returns:
            ActionResult with shuffled list.
        """
        list_param = params.get('list', [])
        output_var = params.get('output_var', 'shuffled_list')

        try:
            import random

            resolved_list = context.resolve_value(list_param)

            if not isinstance(resolved_list, list):
                resolved_list = list(resolved_list)

            result = list(resolved_list)
            random.shuffle(result)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机打乱: {len(result)}项",
                data={
                    'original': resolved_list,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机打乱失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'shuffled_list'}


class RandomUUIDAction(BaseAction):
    """Generate random UUID."""
    action_type = "random8_uuid"
    display_name = "随机UUID"
    description = "生成随机UUID"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID generation.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with UUID.
        """
        output_var = params.get('output_var', 'random_uuid')

        try:
            import uuid

            result = str(uuid.uuid4())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机UUID: {result}",
                data={
                    'uuid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成UUID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'random_uuid'}