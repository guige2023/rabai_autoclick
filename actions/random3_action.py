"""Random3 action module for RabAI AutoClick.

Provides additional random operations:
- RandomChoiceAction: Random choice from list
- RandomSampleAction: Random sample from list
- RandomShuffleAction: Shuffle list
- RandomGaussAction: Gaussian random
- RandomTriangularAction: Triangular random
"""

import random
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RandomChoiceAction(BaseAction):
    """Random choice from list."""
    action_type = "random3_choice"
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
            resolved = context.resolve_value(items)

            if isinstance(resolved, (list, tuple)):
                if not resolved:
                    return ActionResult(
                        success=False,
                        message="列表不能为空"
                    )
                result = random.choice(resolved)
            else:
                return ActionResult(
                    success=False,
                    message="items 必须是列表"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机选择: {result}",
                data={
                    'items': resolved,
                    'choice': result,
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
    action_type = "random3_sample"
    display_name = "随机抽样"
    description = "从列表中随机抽取多个不重复元素"

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

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_count = int(context.resolve_value(count))

            if resolved_count < 0:
                return ActionResult(
                    success=False,
                    message="count 必须为非负数"
                )

            if resolved_count > len(resolved_items):
                return ActionResult(
                    success=False,
                    message=f"count ({resolved_count}) 不能超过列表长度 ({len(resolved_items)})"
                )

            result = random.sample(resolved_items, resolved_count)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机抽样: {len(result)} 个元素",
                data={
                    'items': resolved_items,
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
    action_type = "random3_shuffle"
    display_name = "随机洗牌"
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

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, list):
                return ActionResult(
                    success=False,
                    message="items 必须是列表"
                )

            shuffled = resolved.copy()
            random.shuffle(shuffled)
            context.set(output_var, shuffled)

            return ActionResult(
                success=True,
                message=f"洗牌完成: {len(shuffled)} 个元素",
                data={
                    'original': resolved,
                    'shuffled': shuffled,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机洗牌失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'shuffled_list'}


class RandomGaussAction(BaseAction):
    """Gaussian random."""
    action_type = "random3_gauss"
    display_name = "高斯随机"
    description = "生成高斯分布随机数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gauss.

        Args:
            context: Execution context.
            params: Dict with mu, sigma, output_var.

        Returns:
            ActionResult with gaussian random.
        """
        mu = params.get('mu', 0)
        sigma = params.get('sigma', 1)
        output_var = params.get('output_var', 'gauss_result')

        try:
            resolved_mu = float(context.resolve_value(mu))
            resolved_sigma = float(context.resolve_value(sigma))

            if resolved_sigma < 0:
                return ActionResult(
                    success=False,
                    message="sigma 必须为非负数"
                )

            result = random.gauss(resolved_mu, resolved_sigma)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"高斯随机: μ={resolved_mu}, σ={resolved_sigma}, result={result}",
                data={
                    'mu': resolved_mu,
                    'sigma': resolved_sigma,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"高斯随机失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mu': 0, 'sigma': 1, 'output_var': 'gauss_result'}


class RandomTriangularAction(BaseAction):
    """Triangular random."""
    action_type = "random3_triangular"
    display_name = "三角随机"
    description = "生成三角分布随机数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute triangular.

        Args:
            context: Execution context.
            params: Dict with low, high, mode, output_var.

        Returns:
            ActionResult with triangular random.
        """
        low = params.get('low', 0)
        high = params.get('high', 1)
        mode = params.get('mode', None)
        output_var = params.get('output_var', 'triangular_result')

        try:
            resolved_low = float(context.resolve_value(low))
            resolved_high = float(context.resolve_value(high))
            resolved_mode = float(context.resolve_value(mode)) if mode is not None else None

            if resolved_low > resolved_high:
                return ActionResult(
                    success=False,
                    message="low 必须小于等于 high"
                )

            result = random.triangular(resolved_low, resolved_high, resolved_mode)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"三角随机: [{resolved_low}, {resolved_high}], mode={resolved_mode}, result={result}",
                data={
                    'low': resolved_low,
                    'high': resolved_high,
                    'mode': resolved_mode,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"三角随机失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'low': 0, 'high': 1, 'mode': None, 'output_var': 'triangular_result'}
