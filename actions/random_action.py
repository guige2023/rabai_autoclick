"""Random data generation action module for RabAI AutoClick.

Provides random data generation:
- RandomIntAction: Generate random integer
- RandomFloatAction: Generate random float
- RandomChoiceAction: Random choice from list
- RandomSampleAction: Random sample from list
- RandomShuffleAction: Shuffle list
- RandomStringAction: Generate random string
- RandomUuidAction: Generate random UUID
- RandomBoolAction: Generate random boolean
- RandomDateAction: Generate random date
- RandomIpAction: Generate random IP address
- RandomColorAction: Generate random color
- RandomPasswordAction: Generate random password
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import random
    import string
    import uuid
    from datetime import datetime, timedelta
    RANDOM_AVAILABLE = True
except ImportError:
    RANDOM_AVAILABLE = False


class RandomIntAction(BaseAction):
    """Generate random integer."""
    action_type = "random_int"
    display_name = "随机整数"
    description = "生成随机整数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random integer generation.

        Args:
            context: Execution context.
            params: Dict with min_val, max_val, count, output_var.

        Returns:
            ActionResult with random integer(s).
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="random库不可用")

        min_val = params.get('min_val', 0)
        max_val = params.get('max_val', 100)
        count = params.get('count', 1)
        output_var = params.get('output_var', 'random_result')

        valid, msg = self.validate_type(min_val, int, 'min_val')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(max_val, int, 'max_val')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            if count == 1:
                result = random.randint(min_val, max_val)
            else:
                result = [random.randint(min_val, max_val) for _ in range(count)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成随机整数: {result}",
                data={'value': result, 'min': min_val, 'max': max_val}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机整数失败: {str(e)}"
            )


class RandomFloatAction(BaseAction):
    """Generate random float."""
    action_type = "random_float"
    display_name = "随机浮点数"
    description = "生成随机浮点数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random float generation.

        Args:
            context: Execution context.
            params: Dict with min_val, max_val, decimals, count, output_var.

        Returns:
            ActionResult with random float(s).
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="random库不可用")

        min_val = params.get('min_val', 0.0)
        max_val = params.get('max_val', 1.0)
        decimals = params.get('decimals', None)
        count = params.get('count', 1)
        output_var = params.get('output_var', 'random_result')

        try:
            if count == 1:
                result = random.uniform(min_val, max_val)
                if decimals is not None:
                    result = round(result, decimals)
            else:
                result = []
                for _ in range(count):
                    val = random.uniform(min_val, max_val)
                    if decimals is not None:
                        val = round(val, decimals)
                    result.append(val)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成随机浮点数: {result}",
                data={'value': result, 'min': min_val, 'max': max_val}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机浮点数失败: {str(e)}"
            )


class RandomChoiceAction(BaseAction):
    """Random choice from list."""
    action_type = "random_choice"
    display_name = "随机选择"
    description = "从列表中随机选择一个"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random choice.

        Args:
            context: Execution context.
            params: Dict with items, count, output_var.

        Returns:
            ActionResult with chosen item(s).
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="random库不可用")

        items = params.get('items', [])
        count = params.get('count', 1)
        output_var = params.get('output_var', 'choice_result')

        if not items:
            return ActionResult(success=False, message="列表不能为空")

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            if count == 1:
                result = random.choice(items)
            else:
                if count > len(items):
                    count = len(items)
                result = random.sample(items, count)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机选择: {result}",
                data={'value': result, 'count': count if count > 1 else 1}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机选择失败: {str(e)}"
            )


class RandomSampleAction(BaseAction):
    """Random sample from list."""
    action_type = "random_sample"
    display_name = "随机抽样"
    description = "从列表中随机抽取样本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random sampling.

        Args:
            context: Execution context.
            params: Dict with items, sample_size, output_var.

        Returns:
            ActionResult with sampled items.
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="random库不可用")

        items = params.get('items', [])
        sample_size = params.get('sample_size', 1)
        output_var = params.get('output_var', 'sample_result')

        if not items:
            return ActionResult(success=False, message="列表不能为空")

        try:
            if sample_size > len(items):
                sample_size = len(items)

            result = random.sample(items, sample_size)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"随机抽样 {sample_size} 项",
                data={'sample': result, 'sample_size': sample_size}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机抽样失败: {str(e)}"
            )


class RandomShuffleAction(BaseAction):
    """Shuffle list."""
    action_type = "random_shuffle"
    display_name = "随机打乱"
    description = "随机打乱列表顺序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute list shuffle.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with shuffled list.
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="random库不可用")

        items = params.get('items', [])
        output_var = params.get('output_var', 'shuffle_result')

        if not items:
            return ActionResult(success=False, message="列表不能为空")

        try:
            import copy
            shuffled = copy.deepcopy(items)
            random.shuffle(shuffled)

            context.set(output_var, shuffled)

            return ActionResult(
                success=True,
                message="列表打乱成功",
                data={'shuffled': shuffled}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"随机打乱失败: {str(e)}"
            )


class RandomStringAction(BaseAction):
    """Generate random string."""
    action_type = "random_string"
    display_name = "随机字符串"
    description = "生成随机字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random string generation.

        Args:
            context: Execution context.
            params: Dict with length, charset, count, output_var.

        Returns:
            ActionResult with random string(s).
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="random库不可用")

        length = params.get('length', 16)
        charset = params.get('charset', 'alphanumeric')
        count = params.get('count', 1)
        output_var = params.get('output_var', 'random_string_result')

        valid, msg = self.validate_type(length, int, 'length')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            if charset == 'alphanumeric':
                chars = string.ascii_letters + string.digits
            elif charset == 'alpha':
                chars = string.ascii_letters
            elif charset == 'digits':
                chars = string.digits
            elif charset == 'ascii':
                chars = string.ascii_letters
            elif charset == 'printable':
                chars = string.printable
            else:
                chars = charset

            if count == 1:
                result = ''.join(random.choice(chars) for _ in range(length))
            else:
                result = [
                    ''.join(random.choice(chars) for _ in range(length))
                    for _ in range(count)
                ]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成随机字符串: {result if count == 1 else f'{count} strings'}",
                data={'value': result, 'length': length, 'charset': charset}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机字符串失败: {str(e)}"
            )


class RandomUuidAction(BaseAction):
    """Generate random UUID."""
    action_type = "random_uuid"
    display_name = "随机UUID"
    description = "生成随机UUID"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute UUID generation.

        Args:
            context: Execution context.
            params: Dict with version, count, output_var.

        Returns:
            ActionResult with UUID(s).
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="uuid库不可用")

        version = params.get('version', 4)
        count = params.get('count', 1)
        output_var = params.get('output_var', 'uuid_result')

        try:
            if count == 1:
                if version == 1:
                    result = str(uuid.uuid1())
                elif version == 4:
                    result = str(uuid.uuid4())
                else:
                    result = str(uuid.uuid4())
            else:
                result = []
                for _ in range(count):
                    if version == 1:
                        result.append(str(uuid.uuid1()))
                    elif version == 4:
                        result.append(str(uuid.uuid4()))
                    else:
                        result.append(str(uuid.uuid4()))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成UUID: {result if count == 1 else f'{count} UUIDs'}",
                data={'value': result, 'version': version}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成UUID失败: {str(e)}"
            )


class RandomBoolAction(BaseAction):
    """Generate random boolean."""
    action_type = "random_bool"
    display_name = "随机布尔值"
    description = "生成随机布尔值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random boolean generation.

        Args:
            context: Execution context.
            params: Dict with probability, count, output_var.

        Returns:
            ActionResult with random boolean(s).
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="random库不可用")

        probability = params.get('probability', 0.5)
        count = params.get('count', 1)
        output_var = params.get('output_var', 'bool_result')

        try:
            if count == 1:
                result = random.random() < probability
            else:
                result = [random.random() < probability for _ in range(count)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成随机布尔值: {result}",
                data={'value': result, 'probability': probability}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机布尔值失败: {str(e)}"
            )


class RandomDateAction(BaseAction):
    """Generate random date."""
    action_type = "random_date"
    display_name = "随机日期"
    description = "生成随机日期"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random date generation.

        Args:
            context: Execution context.
            params: Dict with start_date, end_date, format, count, output_var.

        Returns:
            ActionResult with random date(s).
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="datetime库不可用")

        start_date = params.get('start_date', '2020-01-01')
        end_date = params.get('end_date', '2030-12-31')
        fmt = params.get('format', '%Y-%m-%d')
        count = params.get('count', 1)
        output_var = params.get('output_var', 'date_result')

        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')

            delta_seconds = int((end - start).total_seconds())

            if count == 1:
                random_seconds = random.randint(0, delta_seconds)
                random_date = start + timedelta(seconds=random_seconds)
                result = random_date.strftime(fmt)
            else:
                result = []
                for _ in range(count):
                    random_seconds = random.randint(0, delta_seconds)
                    random_date = start + timedelta(seconds=random_seconds)
                    result.append(random_date.strftime(fmt))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成随机日期: {result}",
                data={'value': result, 'start': start_date, 'end': end_date}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机日期失败: {str(e)}"
            )


class RandomIpAction(BaseAction):
    """Generate random IP address."""
    action_type = "random_ip"
    display_name = "随机IP地址"
    description = "生成随机IP地址"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random IP generation.

        Args:
            context: Execution context.
            params: Dict with version, count, output_var.

        Returns:
            ActionResult with random IP(s).
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="random库不可用")

        version = params.get('version', 4)
        count = params.get('count', 1)
        output_var = params.get('output_var', 'ip_result')

        try:
            def gen_ipv4():
                return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"

            def gen_ipv6():
                return ':'.join(f'{random.randint(0, 65535):04x}' for _ in range(8))

            if count == 1:
                if version == 4:
                    result = gen_ipv4()
                else:
                    result = gen_ipv6()
            else:
                result = []
                for _ in range(count):
                    if version == 4:
                        result.append(gen_ipv4())
                    else:
                        result.append(gen_ipv6())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成随机IP: {result}",
                data={'value': result, 'version': version}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机IP失败: {str(e)}"
            )


class RandomColorAction(BaseAction):
    """Generate random color."""
    action_type = "random_color"
    display_name = "随机颜色"
    description = "生成随机颜色"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random color generation.

        Args:
            context: Execution context.
            params: Dict with format, count, output_var.

        Returns:
            ActionResult with random color(s).
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="random库不可用")

        fmt = params.get('format', 'hex')
        count = params.get('count', 1)
        output_var = params.get('output_var', 'color_result')

        try:
            def gen_color():
                r = random.randint(0, 255)
                g = random.randint(0, 255)
                b = random.randint(0, 255)

                if fmt == 'hex':
                    return f'#{r:02x}{g:02x}{b:02x}'.upper()
                elif fmt == 'rgb':
                    return f'rgb({r}, {g}, {b})'
                elif fmt == 'hsl':
                    r_norm = r / 255
                    g_norm = g / 255
                    b_norm = b / 255
                    max_c = max(r_norm, g_norm, b_norm)
                    min_c = min(r_norm, g_norm, b_norm)
                    l = (max_c + min_c) / 2
                    if max_c == min_c:
                        h = s = 0
                    else:
                        d = max_c - min_c
                        s = l > 0.5 and d / (2 - max_c - min_c) or d / (max_c + min_c)
                        if max_c == r_norm:
                            h = (g_norm - b_norm) / d + (g_norm < b_norm and 6 or 0)
                        elif max_c == g_norm:
                            h = (b_norm - r_norm) / d + 2
                        else:
                            h = (r_norm - g_norm) / d + 4
                        h /= 6
                    return f'hsl({int(h * 360)}, {int(s * 100)}%, {int(l * 100)}%)'
                else:
                    return f'#{r:02x}{g:02x}{b:02x}'.upper()

            if count == 1:
                result = gen_color()
            else:
                result = [gen_color() for _ in range(count)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成随机颜色: {result}",
                data={'value': result, 'format': fmt}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机颜色失败: {str(e)}"
            )


class RandomPasswordAction(BaseAction):
    """Generate random password."""
    action_type = "random_password"
    display_name = "随机密码"
    description = "生成随机安全密码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute random password generation.

        Args:
            context: Execution context.
            params: Dict with length, include_uppercase, include_digits, include_special, count, output_var.

        Returns:
            ActionResult with random password(s).
        """
        if not RANDOM_AVAILABLE:
            return ActionResult(success=False, message="random库不可用")

        length = params.get('length', 16)
        include_uppercase = params.get('include_uppercase', True)
        include_digits = params.get('include_digits', True)
        include_special = params.get('include_special', True)
        count = params.get('count', 1)
        output_var = params.get('output_var', 'password_result')

        valid, msg = self.validate_type(length, int, 'length')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            charset = string.ascii_lowercase
            if include_uppercase:
                charset += string.ascii_uppercase
            if include_digits:
                charset += string.digits
            if include_special:
                charset += '!@#$%^&*()_+-=[]{}|;:,.<>?'

            def gen_password():
                password = []
                password.append(random.choice(string.ascii_lowercase))
                if include_uppercase:
                    password.append(random.choice(string.ascii_uppercase))
                if include_digits:
                    password.append(random.choice(string.digits))
                if include_special:
                    password.append(random.choice('!@#$%^&*()_+-=[]{}|;:,.<>?'))
                remaining = length - len(password)
                password.extend(random.choice(charset) for _ in range(remaining))
                random.shuffle(password)
                return ''.join(password)

            if count == 1:
                result = gen_password()
            else:
                result = [gen_password() for _ in range(count)]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"生成随机密码: {'*' * len(result) if count == 1 else f'{count} passwords'}",
                data={'value': result, 'length': length}
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成随机密码失败: {str(e)}"
            )
