"""Password generator action module for RabAI AutoClick.

Provides secure password and random string generation
with configurable complexity rules.
"""

import os
import sys
import secrets
import string
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PasswordGenerateAction(BaseAction):
    """Generate secure passwords.
    
    Supports configurable length, character sets,
    and entropy calculation.
    """
    action_type = "password_generate"
    display_name = "密码生成"
    description = "生成安全密码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate password.
        
        Args:
            context: Execution context.
            params: Dict with keys: length, use_upper, use_lower,
                   use_digits, use_special, exclude_ambiguous,
                   save_to_var.
        
        Returns:
            ActionResult with generated password.
        """
        length = params.get('length', 16)
        use_upper = params.get('use_upper', True)
        use_lower = params.get('use_lower', True)
        use_digits = params.get('use_digits', True)
        use_special = params.get('use_special', True)
        exclude_ambiguous = params.get('exclude_ambiguous', False)
        save_to_var = params.get('save_to_var', None)

        if length < 4:
            return ActionResult(success=False, message="Password length must be >= 4")

        # Build character pool
        chars = ''
        if use_upper:
            pool = string.ascii_uppercase
            if exclude_ambiguous:
                pool = pool.replace('O', '').replace('I', '').replace('L', '')
            chars += pool

        if use_lower:
            pool = string.ascii_lowercase
            if exclude_ambiguous:
                pool = pool.replace('o', '').replace('i', '').replace('l', '')
            chars += pool

        if use_digits:
            pool = string.digits
            if exclude_ambiguous:
                pool = pool.replace('0', '').replace('1', '')
            chars += pool

        if use_special:
            chars += '!@#$%^&*()_+-=[]{}|;:,.<>?'

        if not chars:
            return ActionResult(success=False, message="No character types selected")

        # Generate password ensuring at least one from each
        password_chars = []

        # Ensure one char from each selected type
        if use_upper:
            pool = string.ascii_uppercase
            if exclude_ambiguous:
                pool = pool.replace('O', '').replace('I', '').replace('L', '')
            password_chars.append(secrets.choice(pool))

        if use_lower:
            pool = string.ascii_lowercase
            if exclude_ambiguous:
                pool = pool.replace('o', '').replace('i', '').replace('l', '')
            password_chars.append(secrets.choice(pool))

        if use_digits:
            pool = string.digits
            if exclude_ambiguous:
                pool = pool.replace('0', '').replace('1', '')
            password_chars.append(secrets.choice(pool))

        if use_special:
            password_chars.append(secrets.choice('!@#$%^&*()_+-=[]{}|;:,.<>?'))

        # Fill remaining
        while len(password_chars) < length:
            password_chars.append(secrets.choice(chars))

        # Shuffle
        secrets.SystemRandom().shuffle(password_chars)
        password = ''.join(password_chars)

        # Calculate entropy
        pool_size = len(chars)
        entropy = length * (pool_size.bit_length() - 1)

        result_data = {
            'password': password,
            'length': length,
            'entropy_bits': entropy
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"密码已生成: {length}位, {entropy}bits熵",
            data=result_data
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
            'exclude_ambiguous': False,
            'save_to_var': None
        }
