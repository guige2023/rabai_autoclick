"""Random action module for RabAI AutoClick.

Provides random value generation:
- RandomNumberAction: Generate random numbers
- RandomChoiceAction: Randomly choose from options
- RandomStringAction: Generate random strings
- ShuffleAction: Shuffle a list
"""

import random
import string
from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RandomNumberAction(BaseAction):
    """Generate random numbers."""
    action_type = "random_number"
    display_name = "随机数字"
    description = "生成随机数字"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute generating random number.

        Args:
            context: Execution context.
            params: Dict with min_val, max_val, count, output_var.

        Returns:
            ActionResult with random number(s).
        """
        min_val = params.get('min_val', 0)
        max_val = params.get('max_val', 100)
        count = params.get('count', 1)
        output_var = params.get('output_var', 'random_number')

        # Validate min_val
        valid, msg = self.validate_type(min_val, (int, float), 'min_val')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate max_val
        valid, msg = self.validate_type(max_val, (int, float), 'max_val')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate count
        valid, msg = self.validate_type(count, int, 'count')
        if not valid:
            return ActionResult(success=False, message=msg)
        if count < 1:
            return ActionResult(
                success=False,
                message=f"Parameter 'count' must be >= 1, got {count}"
            )

        try:
            if count == 1:
                result = random.randint(int(min_val), int(max_val))
                context.set(output_var, result)
            else:
                result = [
                    random.randint(int(min_val), int(max_val))
                    for _ in range(count)
                ]
                context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成随机数: {min_val}-{max_val}",
                data={
                    'value': result,
                    'min': min_val,
                    'max': max_val,
                    'count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'min_val': 0,
            'max_val': 100,
            'count': 1,
            'output_var': 'random_number'
        }


class RandomChoiceAction(BaseAction):
    """Randomly choose from options."""
    action_type = "random_choice"
    display_name = "随机选择"
    description = "从选项列表中随机选择一个"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute random choice.

        Args:
            context: Execution context.
            params: Dict with choices, count, output_var.

        Returns:
            ActionResult with chosen item(s).
        """
        choices = params.get('choices', [])
        count = params.get('count', 1)
        output_var = params.get('output_var', 'random_choice')

        # Validate choices
        valid, msg = self.validate_type(choices, (list, tuple), 'choices')
        if not valid:
            return ActionResult(success=False, message=msg)

        if len(choices) == 0:
            return ActionResult(
                success=False,
                message="选项列表为空"
            )

        # Validate count
        valid, msg = self.validate_type(count, int, 'count')
        if not valid:
            return ActionResult(success=False, message=msg)
        if count < 1:
            return ActionResult(
                success=False,
                message=f"Parameter 'count' must be >= 1, got {count}"
            )
        if count > len(choices):
            return ActionResult(
                success=False,
                message=f"Parameter 'count' ({count}) exceeds choices length ({len(choices)})"
            )

        try:
            if count == 1:
                result = random.choice(choices)
                context.set(output_var, result)
            else:
                result = random.sample(choices, count)
                context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机选择: {result}",
                data={
                    'value': result,
                    'count': count,
                    'total_choices': len(choices),
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
        return {
            'count': 1,
            'output_var': 'random_choice'
        }


class RandomStringAction(BaseAction):
    """Generate random strings."""
    action_type = "random_string"
    display_name = "随机字符串"
    description = "生成随机字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute generating random string.

        Args:
            context: Execution context.
            params: Dict with length, charset, output_var.

        Returns:
            ActionResult with random string.
        """
        length = params.get('length', 16)
        charset = params.get('charset', 'alphanumeric')
        output_var = params.get('output_var', 'random_string')

        # Validate length
        valid, msg = self.validate_type(length, int, 'length')
        if not valid:
            return ActionResult(success=False, message=msg)
        if length < 1:
            return ActionResult(
                success=False,
                message=f"Parameter 'length' must be >= 1, got {length}"
            )

        valid, msg = self.validate_type(charset, str, 'charset')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            # Select character set
            if charset == 'alphanumeric':
                chars = string.ascii_letters + string.digits
            elif charset == 'alpha':
                chars = string.ascii_letters
            elif charset == 'digits':
                chars = string.digits
            elif charset == 'uppercase':
                chars = string.ascii_uppercase
            elif charset == 'lowercase':
                chars = string.ascii_lowercase
            elif charset == 'letters':
                chars = string.ascii_letters
            else:
                chars = charset  # Use custom charset

            result = ''.join(random.choice(chars) for _ in range(length))

            # Store in context
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成随机字符串: {length} 字符",
                data={
                    'value': result,
                    'length': length,
                    'charset': charset,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'length': 16,
            'charset': 'alphanumeric',  # alphanumeric, alpha, digits, uppercase, lowercase, letters, or custom
            'output_var': 'random_string'
        }


class ShuffleAction(BaseAction):
    """Shuffle a list."""
    action_type = "shuffle"
    display_name = "打乱列表"
    description = "随机打乱列表顺序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute shuffling a list.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with shuffled list.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'shuffled')

        # Validate items
        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        if len(items) == 0:
            return ActionResult(
                success=False,
                message="列表为空"
            )

        try:
            import copy
            result = copy.copy(items)
            random.shuffle(result)

            # Store in context
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"已打乱列表: {len(result)} 项",
                data={
                    'shuffled': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"打乱列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'shuffled'}