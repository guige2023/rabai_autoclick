"""Random5 action module for RabAI AutoClick.

Provides additional random operations:
- RandomUUIDAction: Generate UUID
- RandomColorAction: Generate random color
- RandomChoiceAction: Random choice from list
- RandomShuffleAction: Shuffle list
- RandomGaussAction: Gaussian random number
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RandomUUIDAction(BaseAction):
    """Generate UUID."""
    action_type = "random5_uuid"
    display_name = "生成UUID"
    description = "生成唯一标识符"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID.

        Args:
            context: Execution context.
            params: Dict with version, output_var.

        Returns:
            ActionResult with UUID.
        """
        version = params.get('version', 4)
        output_var = params.get('output_var', 'uuid')

        try:
            import uuid

            resolved_version = int(context.resolve_value(version)) if version else 4

            if resolved_version == 1:
                result = str(uuid.uuid1())
            elif resolved_version == 4:
                result = str(uuid.uuid4())
            else:
                result = str(uuid.uuid4())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID生成: {result}",
                data={
                    'uuid': result,
                    'version': resolved_version,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"UUID生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'version': 4, 'output_var': 'uuid'}


class RandomColorAction(BaseAction):
    """Generate random color."""
    action_type = "random5_color"
    display_name = "生成随机颜色"
    description = "生成随机颜色"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute color.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with color.
        """
        format_type = params.get('format', 'hex')
        output_var = params.get('output_var', 'color')

        try:
            import random

            resolved_format = context.resolve_value(format_type) if format_type else 'hex'

            r = random.randint(0, 255)
            g = random.randint(0, 255)
            b = random.randint(0, 255)

            if resolved_format == 'hex':
                result = '#{:02x}{:02x}{:02x}'.format(r, g, b)
            elif resolved_format == 'rgb':
                result = f'rgb({r}, {g}, {b})'
            elif resolved_format == 'hsl':
                max_val = max(r, g, b) / 255
                min_val = min(r, g, b) / 255
                l = (max_val + min_val) / 2
                if max_val == min_val:
                    h = s = 0
                else:
                    d = max_val - min_val
                    s = l > 0.5 and d / (2 - max_val - min_val) or d / (max_val + min_val)
                    rn, gn, bn = r / 255, g / 255, b / 255
                    if max_val == rn:
                        h = (gn - bn) / d + (gn < bn and 6 or 0)
                    elif max_val == gn:
                        h = (bn - rn) / d + 2
                    else:
                        h = (rn - gn) / d + 4
                    h /= 6
                result = f'hsl({int(h * 360)}, {int(s * 100)}%, {int(l * 100)}%)'
            else:
                result = '#{:02x}{:02x}{:02x}'.format(r, g, b)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机颜色: {result}",
                data={
                    'color': result,
                    'format': resolved_format,
                    'rgb': (r, g, b),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机颜色生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': 'hex', 'output_var': 'color'}


class RandomChoiceAction(BaseAction):
    """Random choice from list."""
    action_type = "random5_choice"
    display_name = "随机选择"
    description = "从列表随机选择"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute choice.

        Args:
            context: Execution context.
            params: Dict with choices, count, output_var.

        Returns:
            ActionResult with random choice.
        """
        choices = params.get('choices', [])
        count = params.get('count', 1)
        output_var = params.get('output_var', 'random_choice')

        try:
            import random

            resolved = context.resolve_value(choices)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            resolved_count = int(context.resolve_value(count)) if count else 1
            resolved_count = min(resolved_count, len(resolved))

            if resolved_count == 1:
                result = random.choice(resolved)
            else:
                result = random.sample(resolved, resolved_count)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机选择: {result}",
                data={
                    'choices': resolved,
                    'selected': result,
                    'count': resolved_count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机选择失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['choices']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'count': 1, 'output_var': 'random_choice'}


class RandomShuffleAction(BaseAction):
    """Shuffle list."""
    action_type = "random5_shuffle"
    display_name = "随机洗牌"
    description = "随机打乱列表顺序"
    version = "5.0"

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
        input_list = params.get('list', [])
        output_var = params.get('output_var', 'shuffled_list')

        try:
            import random

            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = list(resolved)
            random.shuffle(result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机洗牌: 完成",
                data={
                    'original': resolved,
                    'shuffled': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机洗牌失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'shuffled_list'}


class RandomGaussAction(BaseAction):
    """Gaussian random number."""
    action_type = "random5_gauss"
    display_name = "高斯随机数"
    description = "生成高斯分布随机数"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gauss.

        Args:
            context: Execution context.
            params: Dict with mean, stddev, output_var.

        Returns:
            ActionResult with gaussian random.
        """
        mean = params.get('mean', 0)
        stddev = params.get('stddev', 1)
        output_var = params.get('output_var', 'gauss_value')

        try:
            import random

            resolved_mean = float(context.resolve_value(mean)) if mean else 0
            resolved_stddev = float(context.resolve_value(stddev)) if stddev else 1

            result = random.gauss(resolved_mean, resolved_stddev)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"高斯随机数: {result:.4f}",
                data={
                    'value': result,
                    'mean': resolved_mean,
                    'stddev': resolved_stddev,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"高斯随机数生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mean': 0, 'stddev': 1, 'output_var': 'gauss_value'}