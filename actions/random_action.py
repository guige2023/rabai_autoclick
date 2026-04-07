"""Random generation action module for RabAI AutoClick.

Provides random operations:
- RandomIntAction: Random integer
- RandomFloatAction: Random float
- RandomChoiceAction: Random choice from list
- RandomSampleAction: Random sample
- RandomShuffleAction: Shuffle list
- RandomPasswordAction: Generate random password
- RandomUuidAction: Generate UUID
"""

from __future__ import annotations

import random
import string
import sys
import uuid
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RandomIntAction(BaseAction):
    """Generate random integer."""
    action_type = "random_int"
    display_name = "随机整数"
    description = "生成随机整数"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random int."""
        min_val = params.get('min', 0)
        max_val = params.get('max', 100)
        seed = params.get('seed', None)
        output_var = params.get('output_var', 'random_int')

        try:
            if seed is not None:
                resolved_seed = context.resolve_value(seed) if context else seed
                random.seed(resolved_seed)

            resolved_min = context.resolve_value(min_val) if context else min_val
            resolved_max = context.resolve_value(max_val) if context else max_val

            result = random.randint(int(resolved_min), int(resolved_max))
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Random int: {result}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Random int error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'min': 0, 'max': 100, 'seed': None, 'output_var': 'random_int'}


class RandomFloatAction(BaseAction):
    """Generate random float."""
    action_type = "random_float"
    display_name = "随机浮点数"
    description = "生成随机浮点数"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random float."""
        min_val = params.get('min', 0.0)
        max_val = params.get('max', 1.0)
        seed = params.get('seed', None)
        output_var = params.get('output_var', 'random_float')

        try:
            if seed is not None:
                resolved_seed = context.resolve_value(seed) if context else seed
                random.seed(resolved_seed)

            resolved_min = context.resolve_value(min_val) if context else min_val
            resolved_max = context.resolve_value(max_val) if context else max_val

            result = random.uniform(float(resolved_min), float(resolved_max))
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Random float: {result}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Random float error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'min': 0.0, 'max': 1.0, 'seed': None, 'output_var': 'random_float'}


class RandomChoiceAction(BaseAction):
    """Random choice from list."""
    action_type = "random_choice"
    display_name = "随机选择"
    description = "从列表随机选择"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random choice."""
        choices = params.get('choices', [])
        count = params.get('count', 1)
        seed = params.get('seed', None)
        output_var = params.get('output_var', 'random_choice')

        if not choices:
            return ActionResult(success=False, message="choices is required")

        try:
            if seed is not None:
                resolved_seed = context.resolve_value(seed) if context else seed
                random.seed(resolved_seed)

            resolved = context.resolve_value(choices) if context else choices
            resolved_count = context.resolve_value(count) if context else count

            if resolved_count == 1:
                result = random.choice(resolved)
            else:
                result = random.sample(resolved, min(int(resolved_count), len(resolved)))

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Random choice: {result}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Random choice error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['choices']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'count': 1, 'seed': None, 'output_var': 'random_choice'}


class RandomSampleAction(BaseAction):
    """Random sample from list."""
    action_type = "random_sample"
    display_name = "随机抽样"
    description = "随机抽样"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random sample."""
        population = params.get('population', [])
        k = params.get('k', 1)
        seed = params.get('seed', None)
        output_var = params.get('output_var', 'random_sample')

        if not population:
            return ActionResult(success=False, message="population is required")

        try:
            if seed is not None:
                resolved_seed = context.resolve_value(seed) if context else seed
                random.seed(resolved_seed)

            resolved_pop = context.resolve_value(population) if context else population
            resolved_k = context.resolve_value(k) if context else k

            result = random.sample(list(resolved_pop), min(int(resolved_k), len(resolved_pop)))

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Sampled {len(result)} items", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Random sample error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['population']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'k': 1, 'seed': None, 'output_var': 'random_sample'}


class RandomShuffleAction(BaseAction):
    """Shuffle list."""
    action_type = "random_shuffle"
    display_name = "随机打乱"
    description = "随机打乱列表"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute shuffle."""
        items = params.get('items', [])
        seed = params.get('seed', None)
        copy_list = params.get('copy', True)
        output_var = params.get('output_var', 'shuffled_list')

        if not items:
            return ActionResult(success=False, message="items is required")

        try:
            if seed is not None:
                resolved_seed = context.resolve_value(seed) if context else seed
                random.seed(resolved_seed)

            resolved = context.resolve_value(items) if context else items

            if copy_list:
                import copy
                shuffled = copy.copy(resolved)
            else:
                shuffled = list(resolved)

            random.shuffle(shuffled)

            if context:
                context.set(output_var, shuffled)
            return ActionResult(success=True, message=f"Shuffled {len(shuffled)} items", data={'result': shuffled})
        except Exception as e:
            return ActionResult(success=False, message=f"Shuffle error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'seed': None, 'copy': True, 'output_var': 'shuffled_list'}


class RandomPasswordAction(BaseAction):
    """Generate random password."""
    action_type = "random_password"
    display_name = "随机密码"
    description = "生成随机密码"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute password generation."""
        length = params.get('length', 16)
        include_uppercase = params.get('include_uppercase', True)
        include_lowercase = params.get('include_lowercase', True)
        include_digits = params.get('include_digits', True)
        include_special = params.get('include_special', True)
        output_var = params.get('output_var', 'random_password')

        try:
            resolved_len = context.resolve_value(length) if context else length

            chars = ''
            if include_lowercase:
                chars += string.ascii_lowercase
            if include_uppercase:
                chars += string.ascii_uppercase
            if include_digits:
                chars += string.digits
            if include_special:
                chars += string.punctuation

            if not chars:
                chars = string.ascii_letters

            result = ''.join(random.choice(chars) for _ in range(int(resolved_len)))

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Generated password ({len(result)} chars)", data={'password': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Password error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'length': 16, 'include_uppercase': True, 'include_lowercase': True,
            'include_digits': True, 'include_special': True, 'output_var': 'random_password'
        }


class RandomUuidAction(BaseAction):
    """Generate UUID."""
    action_type = "random_uuid"
    display_name = "随机UUID"
    description = "生成随机UUID"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute UUID generation."""
        version = params.get('version', 4)  # 1, 3, 4
        namespace = params.get('namespace', None)
        name = params.get('name', None)
        output_var = params.get('output_var', 'random_uuid')

        try:
            resolved_version = context.resolve_value(version) if context else version

            if int(resolved_version) == 1:
                result = str(uuid.uuid1())
            elif int(resolved_version) == 3:
                ns = context.resolve_value(namespace) if context else namespace
                nm = context.resolve_value(name) if context else name
                if ns and nm:
                    result = str(uuid.uuid3(uuid.UUID(ns), nm))
                else:
                    result = str(uuid.uuid3(uuid.NAMESPACE_DNS, str(name or 'default')))
            else:
                result = str(uuid.uuid4())

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"UUID: {result}", data={'uuid': result})
        except Exception as e:
            return ActionResult(success=False, message=f"UUID error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'version': 4, 'namespace': None, 'name': None, 'output_var': 'random_uuid'}
