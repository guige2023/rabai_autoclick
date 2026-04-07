"""Random4 action module for RabAI AutoClick.

Provides additional random operations:
- RandomChoiceAction: Random choice from list
- RandomSampleAction: Random sample from list
- RandomShuffleAction: Shuffle list
- RandomGaussAction: Gaussian random
- RandomPasswordAction: Generate random password
"""

import random
import string
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RandomChoiceAction(BaseAction):
    """Random choice from list."""
    action_type = "random4_choice"
    display_name = "随机选择"
    description = "从列表中随机选择一个"
    version = "4.0"

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
            ActionResult with chosen item.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'choice_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(items)

            if not resolved:
                return ActionResult(
                    success=False,
                    message="随机选择失败: 列表为空"
                )

            result = random.choice(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机选择完成",
                data={
                    'items': resolved,
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
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'choice_result'}


class RandomSampleAction(BaseAction):
    """Random sample from list."""
    action_type = "random4_sample"
    display_name = "随机抽样"
    description = "从列表中随机抽取多个"
    version = "4.0"

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
            ActionResult with sampled items.
        """
        items = params.get('items', [])
        count = params.get('count', 1)
        output_var = params.get('output_var', 'sample_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_count = int(context.resolve_value(count))

            if resolved_count > len(resolved_items):
                resolved_count = len(resolved_items)

            result = random.sample(resolved_items, resolved_count)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机抽样完成: {len(result)} 项",
                data={
                    'items': resolved_items,
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
        return ['items', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sample_result'}


class RandomShuffleAction(BaseAction):
    """Shuffle list."""
    action_type = "random4_shuffle"
    display_name = "随机打乱"
    description = "随机打乱列表顺序"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random shuffle.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with shuffled list.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'shuffle_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="随机打乱失败: 输入不是列表"
                )

            result = list(resolved)
            random.shuffle(result)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机打乱完成",
                data={
                    'original': resolved,
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
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'shuffle_result'}


class RandomGaussAction(BaseAction):
    """Gaussian random."""
    action_type = "random4_gauss"
    display_name = "高斯随机"
    description = "生成高斯分布随机数"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gaussian random.

        Args:
            context: Execution context.
            params: Dict with mu, sigma, output_var.

        Returns:
            ActionResult with gaussian random number.
        """
        mu = params.get('mu', 0)
        sigma = params.get('sigma', 1)
        output_var = params.get('output_var', 'gauss_result')

        try:
            resolved_mu = float(context.resolve_value(mu))
            resolved_sigma = float(context.resolve_value(sigma))

            result = random.gauss(resolved_mu, resolved_sigma)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"高斯随机: {result}",
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


class RandomPasswordAction(BaseAction):
    """Generate random password."""
    action_type = "random4_password"
    display_name = "随机密码"
    description = "生成随机密码"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute password generation.

        Args:
            context: Execution context.
            params: Dict with length, use_digits, use_special, output_var.

        Returns:
            ActionResult with generated password.
        """
        length = params.get('length', 16)
        use_digits = params.get('use_digits', True)
        use_special = params.get('use_special', False)
        output_var = params.get('output_var', 'password_result')

        try:
            resolved_length = int(context.resolve_value(length))
            resolved_digits = bool(context.resolve_value(use_digits))
            resolved_special = bool(context.resolve_value(use_special))

            chars = string.ascii_letters
            if resolved_digits:
                chars += string.digits
            if resolved_special:
                chars += string.punctuation

            result = ''.join(random.choice(chars) for _ in range(resolved_length))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机密码生成: {resolved_length} 位",
                data={
                    'password': result,
                    'length': resolved_length,
                    'has_digits': resolved_digits,
                    'has_special': resolved_special,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机密码生成失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'length': 16, 'use_digits': True, 'use_special': False, 'output_var': 'password_result'}