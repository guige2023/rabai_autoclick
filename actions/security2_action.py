"""Security2 action module for RabAI AutoClick.

Provides additional security operations:
- SecuritySanitizeAction: Sanitize string
- SecurityValidateEmailAction: Validate email
- SecurityValidateUrlAction: Validate URL
- SecurityEscapeHtmlAction: Escape HTML
"""

import html
import re
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SecuritySanitizeAction(BaseAction):
    """Sanitize string."""
    action_type = "security2_sanitize"
    display_name = "清理字符串"
    description = "清理字符串中的危险字符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sanitize.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with sanitized string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'sanitized_value')

        try:
            resolved = str(context.resolve_value(value))

            sanitized = resolved.replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&#x27;')

            context.set(output_var, sanitized)

            return ActionResult(
                success=True,
                message=f"字符串已清理",
                data={
                    'original': resolved,
                    'result': sanitized,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清理字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sanitized_value'}


class SecurityValidateEmailAction(BaseAction):
    """Validate email."""
    action_type = "security2_validate_email"
    display_name = "验证邮箱"
    description = "验证邮箱地址格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute validate email.

        Args:
            context: Execution context.
            params: Dict with email, output_var.

        Returns:
            ActionResult with validation result.
        """
        email = params.get('email', '')
        output_var = params.get('output_var', 'email_valid')

        valid, msg = self.validate_type(email, str, 'email')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(email)

            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            is_valid = bool(re.match(pattern, resolved))

            context.set(output_var, is_valid)

            return ActionResult(
                success=True,
                message=f"邮箱验证: {'有效' if is_valid else '无效'}",
                data={
                    'email': resolved,
                    'is_valid': is_valid,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证邮箱失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['email']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'email_valid'}


class SecurityValidateUrlAction(BaseAction):
    """Validate URL."""
    action_type = "security2_validate_url"
    display_name = "验证URL"
    description = "验证URL格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute validate URL.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with validation result.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'url_valid')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(url)

            pattern = r'^https?://[^\s/$.?#].[^\s]*$'
            is_valid = bool(re.match(pattern, resolved))

            context.set(output_var, is_valid)

            return ActionResult(
                success=True,
                message=f"URL验证: {'有效' if is_valid else '无效'}",
                data={
                    'url': resolved,
                    'is_valid': is_valid,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证URL失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_valid'}


class SecurityEscapeHtmlAction(BaseAction):
    """Escape HTML."""
    action_type = "security2_escape_html"
    display_name = "HTML转义"
    description = "转义HTML特殊字符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute escape HTML.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with escaped string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'escaped_html')

        try:
            resolved = str(context.resolve_value(value))

            escaped = html.escape(resolved)
            context.set(output_var, escaped)

            return ActionResult(
                success=True,
                message=f"HTML已转义",
                data={
                    'original': resolved,
                    'result': escaped,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"HTML转义失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'escaped_html'}