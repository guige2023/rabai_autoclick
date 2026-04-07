"""Random action module for RabAI AutoClick.

Provides random value generation operations:
- RandomIntAction: Generate random integer
- RandomFloatAction: Generate random float
- RandomChoiceAction: Random choice from list
- RandomSampleAction: Random sample from list
- RandomStringAction: Generate random string
- RandomUuidAction: Generate random UUID (alias)
- RandomShuffleAction: Shuffle list
"""

import random
import string
import uuid
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RandomIntAction(BaseAction):
    """Generate random integer."""
    action_type = "random_int"
    display_name = "随机整数"
    description = "生成随机整数"
    version = "1.0"

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

        valid, msg = self.validate_type(min_val, int, 'min_val')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_min = context.resolve_value(min_val)
            resolved_max = context.resolve_value(max_val)

            result = random.randint(resolved_min, resolved_max)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机整数: {result}",
                data={'value': result, 'range': [resolved_min, resolved_max], 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"随机整数生成失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['min_val', 'max_val']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'random_int'}


class RandomFloatAction(BaseAction):
    """Generate random float."""
    action_type = "random_float"
    display_name = "随机浮点数"
    description = "生成随机浮点数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random float.

        Args:
            context: Execution context.
            params: Dict with min_val, max_val, decimals, output_var.

        Returns:
            ActionResult with random float.
        """
        min_val = params.get('min_val', 0.0)
        max_val = params.get('max_val', 1.0)
        decimals = params.get('decimals', 2)
        output_var = params.get('output_var', 'random_float')

        valid, msg = self.validate_type(min_val, (int, float), 'min_val')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_min = context.resolve_value(min_val)
            resolved_max = context.resolve_value(max_val)
            resolved_decimals = context.resolve_value(decimals)

            result = random.uniform(resolved_min, resolved_max)
            result = round(result, resolved_decimals)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机浮点数: {result}",
                data={'value': result, 'range': [resolved_min, resolved_max], 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"随机浮点数生成失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['min_val', 'max_val']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'decimals': 2, 'output_var': 'random_float'}


class RandomChoiceAction(BaseAction):
    """Random choice from list."""
    action_type = "random_choice"
    display_name = "随机选择"
    description = "从列表随机选择"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random choice.

        Args:
            context: Execution context.
            params: Dict with choices, output_var.

        Returns:
            ActionResult with random choice.
        """
        choices = params.get('choices', [])
        output_var = params.get('output_var', 'random_choice')

        valid, msg = self.validate_type(choices, list, 'choices')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_choices = context.resolve_value(choices)

            if not resolved_choices:
                return ActionResult(success=False, message="choices不能为空")

            result = random.choice(resolved_choices)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机选择: {result}",
                data={'value': result, 'choices': resolved_choices, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"随机选择失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['choices']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'random_choice'}


class RandomSampleAction(BaseAction):
    """Random sample from list."""
    action_type = "random_sample"
    display_name = "随机抽样"
    description = "从列表随机抽样"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random sample.

        Args:
            context: Execution context.
            params: Dict with population, count, output_var.

        Returns:
            ActionResult with random sample.
        """
        population = params.get('population', [])
        count = params.get('count', 1)
        output_var = params.get('output_var', 'random_sample')

        valid, msg = self.validate_type(population, list, 'population')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pop = context.resolve_value(population)
            resolved_count = context.resolve_value(count)

            if not resolved_pop:
                return ActionResult(success=False, message="population不能为空")

            count = min(resolved_count, len(resolved_pop))
            result = random.sample(resolved_pop, count)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机抽样: {len(result)} 个",
                data={'sample': result, 'count': len(result), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"随机抽样失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['population', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'random_sample'}


class RandomStringAction(BaseAction):
    """Generate random string."""
    action_type = "random_string"
    display_name = "随机字符串"
    description = "生成随机字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random string.

        Args:
            context: Execution context.
            params: Dict with length, charset, output_var.

        Returns:
            ActionResult with random string.
        """
        length = params.get('length', 16)
        charset = params.get('charset', 'alphanumeric')
        output_var = params.get('output_var', 'random_string')

        valid, msg = self.validate_type(length, int, 'length')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_length = context.resolve_value(length)
            resolved_charset = context.resolve_value(charset)

            if resolved_charset == 'alphanumeric':
                chars = string.ascii_letters + string.digits
            elif resolved_charset == 'alpha':
                chars = string.ascii_letters
            elif resolved_charset == 'digits':
                chars = string.digits
            elif resolved_charset == 'ascii':
                chars = string.ascii_letters + string.digits + string.punctuation
            else:
                chars = resolved_charset

            result = ''.join(random.choice(chars) for _ in range(resolved_length))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机字符串: {result[:8]}...",
                data={'value': result, 'length': resolved_length, 'charset': resolved_charset, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"随机字符串生成失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['length']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'charset': 'alphanumeric', 'output_var': 'random_string'}


class RandomShuffleAction(BaseAction):
    """Shuffle list."""
    action_type = "random_shuffle"
    display_name = "随机洗牌"
    description = "随机打乱列表"
    version = "1.0"

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
        output_var = params.get('output_var', 'shuffled_items')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            shuffled = resolved_items.copy()
            random.shuffle(shuffled)

            context.set(output_var, shuffled)

            return ActionResult(
                success=True,
                message=f"列表已打乱: {len(shuffled)} 个元素",
                data={'shuffled': shuffled, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"随机洗牌失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'shuffled_items'}
