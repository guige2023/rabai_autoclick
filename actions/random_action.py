"""Random action module for RabAI AutoClick.

Provides random data generation: UUIDs, passwords,
sample selection, and random distributions.
"""

import sys
import os
import random
import string
import uuid
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class UUIDGenerateAction(BaseAction):
    """Generate various UUID formats.
    
    Support UUID1, UUID4, UUID5, and custom formats.
    """
    action_type = "uuid_generate"
    display_name = "UUID生成"
    description = "生成多种格式的UUID"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate UUID.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - version: int (1/3/4/5)
                - namespace: str (for UUID3/5)
                - name: str (for UUID3/5)
                - count: int (number to generate)
                - format: str (raw/hex/urn)
                - save_to_var: str
        
        Returns:
            ActionResult with generated UUID(s).
        """
        version = params.get('version', 4)
        namespace = params.get('namespace', 'default')
        name = params.get('name', '')
        count = params.get('count', 1)
        fmt = params.get('format', 'raw')
        save_to_var = params.get('save_to_var', 'uuid_result')

        uuids = []

        for i in range(count):
            if version == 1:
                uid = uuid.uuid1()
            elif version == 3:
                ns_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, namespace)
                uid = uuid.uuid3(ns_uuid, name or str(i))
            elif version == 5:
                ns_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, namespace)
                uid = uuid.uuid5(ns_uuid, name or str(i))
            else:
                uid = uuid.uuid4()

            if fmt == 'hex':
                uuids.append(uid.hex)
            elif fmt == 'urn':
                uuids.append(str(uid))
            else:
                uuids.append(str(uid))

        result_single = uuids[0] if count == 1 else uuids

        result = {
            'uuid': result_single,
            'count': count,
            'version': version,
            'format': fmt,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Generated {count} UUIDv{version}"
        )


class PasswordGenerateAction(BaseAction):
    """Generate secure random passwords.
    
    Configurable length, character sets, and formats.
    """
    action_type = "password_generate"
    display_name = "密码生成"
    description = "生成安全的随机密码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate password.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - length: int (default 16)
                - use_uppercase: bool
                - use_lowercase: bool
                - use_digits: bool
                - use_special: bool
                - exclude_chars: str
                - count: int
                - save_to_var: str
        
        Returns:
            ActionResult with generated password(s).
        """
        length = params.get('length', 16)
        use_upper = params.get('use_uppercase', True)
        use_lower = params.get('use_lowercase', True)
        use_digits = params.get('use_digits', True)
        use_special = params.get('use_special', True)
        exclude = params.get('exclude_chars', '')
        count = params.get('count', 1)
        save_to_var = params.get('save_to_var', 'password_result')

        chars = ''
        if use_upper:
            chars += string.ascii_uppercase
        if use_lower:
            chars += string.ascii_lowercase
        if use_digits:
            chars += string.digits
        if use_special:
            chars += string.punctuation

        for c in exclude:
            chars = chars.replace(c, '')

        if not chars:
            return ActionResult(success=False, message="No character sets enabled")

        passwords = []
        for _ in range(count):
            pwd = ''.join(random.choice(chars) for _ in range(length))
            passwords.append(pwd)

        result_single = passwords[0] if count == 1 else passwords

        result = {
            'password': result_single,
            'count': count,
            'length': length,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Generated {count} password(s) of length {length}"
        )


class RandomSampleAction(BaseAction):
    """Randomly sample items from data.
    
    Support uniform random and weighted sampling.
    """
    action_type = "random_sample"
    display_name = "随机抽样"
    description = "从数据中随机抽样，支持加权抽样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Random sample from data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list
                - count: int (number to sample)
                - replace: bool (with replacement)
                - weights: list of float (optional)
                - seed: int (random seed)
                - save_to_var: str
        
        Returns:
            ActionResult with sampled items.
        """
        data = params.get('data', [])
        count = params.get('count', 1)
        replace = params.get('replace', False)
        weights = params.get('weights', None)
        seed = params.get('seed', None)
        save_to_var = params.get('save_to_var', 'sample_result')

        if seed is not None:
            random.seed(seed)

        if not data:
            return ActionResult(success=False, message="No data provided")

        try:
            if weights:
                sample = random.choices(data, weights=weights, k=count)
            else:
                sample = random.choices(data, k=count) if replace else random.sample(data, min(count, len(data)))
        except Exception as e:
            return ActionResult(success=False, message=f"Sampling error: {e}")

        result = {
            'sample': sample,
            'count': len(sample),
            'original_count': len(data),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Sampled {len(sample)} items from {len(data)}"
        )


class RandomNumberAction(BaseAction):
    """Generate random numbers.
    
    Support uniform, normal, and other distributions.
    """
    action_type = "random_number"
    display_name = "随机数生成"
    description = "生成随机数，支持多种分布"

    DISTRIBUTIONS = ['uniform', 'normal', 'gauss', 'exponential', 'triangular', 'betavariate', 'random']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate random number(s).
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - distribution: str
                - min_value: float
                - max_value: float
                - mean: float (for normal)
                - stddev: float (for normal)
                - count: int
                - seed: int
                - save_to_var: str
        
        Returns:
            ActionResult with random number(s).
        """
        distribution = params.get('distribution', 'uniform')
        min_val = params.get('min_value', 0)
        max_val = params.get('max_value', 1)
        mean = params.get('mean', 0)
        stddev = params.get('stddev', 1)
        count = params.get('count', 1)
        seed = params.get('seed', None)
        save_to_var = params.get('save_to_var', 'random_result')

        if seed is not None:
            random.seed(seed)

        numbers = []
        for _ in range(count):
            if distribution == 'uniform':
                numbers.append(random.uniform(min_val, max_val))
            elif distribution in ('normal', 'gauss'):
                numbers.append(random.gauss(mean, stddev))
            elif distribution == 'exponential':
                numbers.append(random.expovariate(1.0 / max_val if max_val > 0 else 1.0))
            elif distribution == 'triangular':
                numbers.append(random.triangular(min_val, max_val))
            elif distribution == 'betavariate':
                alpha = params.get('alpha', 1.0)
                beta = params.get('beta', 1.0)
                numbers.append(random.betavariate(alpha, beta) * (max_val - min_val) + min_val)
            else:
                numbers.append(random.random() * (max_val - min_val) + min_val)

        result_single = numbers[0] if count == 1 else numbers

        result = {
            'number': result_single,
            'distribution': distribution,
            'count': count,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Generated {count} random number(s) from {distribution} distribution"
        )


class RandomChoiceAction(BaseAction):
    """Select random choices from options.
    
    Random selection with optional weights and
    exclusion of previously selected items.
    """
    action_type = "random_choice"
    display_name = "随机选择"
    description = "从选项中随机选择"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Random choice from options.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - options: list
                - count: int
                - weights: list
                - unique: bool
                - seed: int
                - save_to_var: str
        
        Returns:
            ActionResult with chosen item(s).
        """
        options = params.get('options', [])
        count = params.get('count', 1)
        weights = params.get('weights', None)
        unique = params.get('unique', False)
        seed = params.get('seed', None)
        save_to_var = params.get('save_to_var', 'choice_result')

        if seed is not None:
            random.seed(seed)

        if not options:
            return ActionResult(success=False, message="No options provided")

        if unique and count > len(options):
            count = len(options)

        try:
            if weights:
                choices = random.choices(options, weights=weights, k=count)
            elif unique:
                choices = random.sample(options, count)
            else:
                choices = [random.choice(options) for _ in range(count)]
        except Exception as e:
            return ActionResult(success=False, message=f"Choice error: {e}")

        result_single = choices[0] if count == 1 else choices

        result = {
            'choice': result_single,
            'count': len(choices),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Chose {count} item(s) from {len(options)} options"
        )
