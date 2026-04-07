"""Security action module for RabAI AutoClick.

Provides security operations:
- SecurityGeneratePasswordAction: Generate secure password
- SecurityValidatePasswordAction: Validate password strength
- SecurityGenerateTokenAction: Generate security token
"""

import secrets
import string
import re
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SecurityGeneratePasswordAction(BaseAction):
    """Generate secure password."""
    action_type = "security_generate_password"
    display_name = "生成密码"
    description = "生成安全密码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute password generation.

        Args:
            context: Execution context.
            params: Dict with length, use_upper, use_lower, use_digits, use_special, output_var.

        Returns:
            ActionResult with generated password.
        """
        length = params.get('length', 16)
        use_upper = params.get('use_upper', True)
        use_lower = params.get('use_lower', True)
        use_digits = params.get('use_digits', True)
        use_special = params.get('use_special', True)
        output_var = params.get('output_var', 'security_result')

        try:
            resolved_length = context.resolve_value(length)

            chars = ''
            if use_upper:
                chars += string.ascii_uppercase
            if use_lower:
                chars += string.ascii_lowercase
            if use_digits:
                chars += string.digits
            if use_special:
                chars += string.punctuation

            if not chars:
                return ActionResult(
                    success=False,
                    message="必须选择至少一种字符类型"
                )

            result = ''.join(secrets.choice(chars) for _ in range(int(resolved_length)))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"密码已生成: {len(result)} 字符",
                data={
                    'password': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成密码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'length': 16,
            'use_upper': True,
            'use_lower': True,
            'use_digits': True,
            'use_special': True,
            'output_var': 'security_result'
        }


class SecurityValidatePasswordAction(BaseAction):
    """Validate password strength."""
    action_type = "security_validate_password"
    display_name = "验证密码强度"
    description = "验证密码强度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute password validation.

        Args:
            context: Execution context.
            params: Dict with password, min_length, require_upper, require_lower, require_digits, require_special, output_var.

        Returns:
            ActionResult with validation result.
        """
        password = params.get('password', '')
        min_length = params.get('min_length', 8)
        require_upper = params.get('require_upper', True)
        require_lower = params.get('require_lower', True)
        require_digits = params.get('require_digits', True)
        require_special = params.get('require_special', True)
        output_var = params.get('output_var', 'security_result')

        valid, msg = self.validate_type(password, str, 'password')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_password = context.resolve_value(password)

            score = 0
            feedback = []

            if len(resolved_password) >= int(min_length):
                score += 1
            else:
                feedback.append(f"密码长度至少 {min_length} 字符")

            if require_upper and re.search(r'[A-Z]', resolved_password):
                score += 1
            elif require_upper:
                feedback.append("需要包含大写字母")

            if require_lower and re.search(r'[a-z]', resolved_password):
                score += 1
            elif require_lower:
                feedback.append("需要包含小写字母")

            if require_digits and re.search(r'\d', resolved_password):
                score += 1
            elif require_digits:
                feedback.append("需要包含数字")

            if require_special and re.search(r'[!@#$%^&*(),.?":{}|<>]', resolved_password):
                score += 1
            elif require_special:
                feedback.append("需要包含特殊字符")

            strength = {
                0: 'very_weak',
                1: 'weak',
                2: 'fair',
                3: 'strong',
                4: 'very_strong',
                5: 'excellent'
            }.get(score, 'unknown')

            valid_password = score >= 3
            context.set(output_var, valid_password)

            return ActionResult(
                success=True,
                message=f"密码强度: {strength}" if valid_password else f"密码强度不足: {', '.join(feedback)}",
                data={
                    'valid': valid_password,
                    'score': score,
                    'strength': strength,
                    'feedback': feedback,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证密码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['password']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'min_length': 8,
            'require_upper': True,
            'require_lower': True,
            'require_digits': True,
            'require_special': True,
            'output_var': 'security_result'
        }


class SecurityGenerateTokenAction(BaseAction):
    """Generate security token."""
    action_type = "security_generate_token"
    display_name = "生成令牌"
    description = "生成安全令牌"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute token generation.

        Args:
            context: Execution context.
            params: Dict with length, output_var.

        Returns:
            ActionResult with generated token.
        """
        length = params.get('length', 32)
        output_var = params.get('output_var', 'security_result')

        try:
            resolved_length = context.resolve_value(length)
            result = secrets.token_urlsafe(int(resolved_length))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"令牌已生成: {len(result)} 字符",
                data={
                    'token': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成令牌失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'length': 32, 'output_var': 'security_result'}