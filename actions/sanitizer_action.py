"""Data sanitization action module for RabAI AutoClick.

Provides sanitization operations:
- SanitizeHtmlAction: Strip HTML tags
- SanitizeSqlAction: Escape SQL characters
- SanitizeXmlAction: Escape XML special characters
- SanitizeJsAction: Escape JavaScript strings
- SanitizePathAction: Sanitize file path
- SanitizeEmailAction: Sanitize email addresses
"""

from __future__ import annotations

import html
import re
import sys
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SanitizeHtmlAction(BaseAction):
    """Strip HTML tags."""
    action_type = "sanitize_html"
    display_name = "HTML清理"
    description = "去除HTML标签"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTML sanitization."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'sanitized_html')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            # Strip HTML tags
            result = re.sub(r'<[^>]+>', '', str(resolved))
            # Decode HTML entities
            result = html.unescape(result)
            # Remove extra whitespace
            result = ' '.join(result.split())

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"HTML sanitized", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"HTML sanitize error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sanitized_html'}


class SanitizeSqlAction(BaseAction):
    """Escape SQL characters."""
    action_type = "sanitize_sql"
    display_name = "SQL清理"
    description = "转义SQL字符"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SQL sanitization."""
        value = params.get('value', '')
        method = params.get('method', 'escape')  # escape, parameterized
        output_var = params.get('output_var', 'sanitized_sql')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            resolved_method = context.resolve_value(method) if context else method

            if resolved_method == 'escape':
                # Simple escaping for SQL
                result = str(resolved).replace("'", "''").replace(";", "")
            else:
                result = str(resolved)

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"SQL sanitized", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"SQL sanitize error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'method': 'escape', 'output_var': 'sanitized_sql'}


class SanitizeXmlAction(BaseAction):
    """Escape XML special characters."""
    action_type = "sanitize_xml"
    display_name = "XML清理"
    description = "转义XML字符"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute XML sanitization."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'sanitized_xml')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            import xml.sax.saxutils as saxutils

            resolved = context.resolve_value(value) if context else value
            result = saxutils.escape(str(resolved))

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"XML sanitized", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"XML sanitize error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sanitized_xml'}


class SanitizeJsAction(BaseAction):
    """Escape JavaScript strings."""
    action_type = "sanitize_js"
    display_name = "JS清理"
    description = "转义JavaScript字符串"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JS sanitization."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'sanitized_js')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            import json

            resolved = context.resolve_value(value) if context else value
            result = json.dumps(str(resolved))[1:-1]  # Remove surrounding quotes

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"JS sanitized", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"JS sanitize error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sanitized_js'}


class SanitizePathAction(BaseAction):
    """Sanitize file path."""
    action_type = "sanitize_path"
    display_name = "路径清理"
    description = "清理文件路径"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute path sanitization."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'sanitized_path')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            import urllib.parse

            resolved = context.resolve_value(value) if context else value
            # Remove null bytes
            result = str(resolved).replace('\x00', '')
            # Remove directory traversal
            result = result.replace('..', '').replace('~/', '')
            # URL encode dangerous characters
            result = urllib.parse.quote(result, safe='/.-_')

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Path sanitized", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Path sanitize error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sanitized_path'}


class SanitizeEmailAction(BaseAction):
    """Sanitize email address."""
    action_type = "sanitize_email"
    display_name = "邮箱清理"
    description = "清理邮箱地址"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute email sanitization."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'sanitized_email')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            import re

            resolved = context.resolve_value(value) if context else value
            email = str(resolved).strip().lower()
            # Basic validation
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            valid = bool(re.match(pattern, email))

            result = {'email': email, 'valid': valid}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Email sanitized: {email}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Email sanitize error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sanitized_email'}
