"""Data validator action module for RabAI AutoClick.

Provides data validation actions for strings, numbers, URLs,
email addresses, and custom regex patterns.
"""

import re
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ValidateStringAction(BaseAction):
    """Validate string against various constraints.
    
    Supports length range, regex pattern, allowed values,
    and custom format validation.
    """
    action_type = "validate_string"
    display_name = "验证字符串"
    description = "验证字符串长度、格式、正则表达式匹配"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate a string value.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, min_length, max_length,
                   pattern, allowed_values, trim, save_to_var.
        
        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        min_length = params.get('min_length', None)
        max_length = params.get('max_length', None)
        pattern = params.get('pattern', None)
        allowed_values = params.get('allowed_values', None)
        trim = params.get('trim', False)
        save_to_var = params.get('save_to_var', None)

        if not isinstance(value, str):
            return ActionResult(
                success=False,
                message=f"Value must be string, got {type(value).__name__}"
            )

        original_value = value
        if trim:
            value = value.strip()

        errors = []

        # Check length constraints
        if min_length is not None:
            try:
                min_len = int(min_length)
                if len(value) < min_len:
                    errors.append(f"长度 {len(value)} < 最小值 {min_len}")
            except (ValueError, TypeError):
                errors.append(f"Invalid min_length: {min_length}")

        if max_length is not None:
            try:
                max_len = int(max_length)
                if len(value) > max_len:
                    errors.append(f"长度 {len(value)} > 最大值 {max_len}")
            except (ValueError, TypeError):
                errors.append(f"Invalid max_length: {max_length}")

        # Check regex pattern
        if pattern:
            try:
                if not re.search(pattern, value):
                    errors.append(f"正则不匹配: {pattern}")
            except re.error as e:
                errors.append(f"Invalid regex pattern: {e}")

        # Check allowed values
        if allowed_values is not None:
            if isinstance(allowed_values, list):
                if value not in allowed_values:
                    errors.append(f"值不在允许列表中: {allowed_values}")
            elif isinstance(allowed_values, str):
                allowed_list = [v.strip() for v in allowed_values.split(',')]
                if value not in allowed_list:
                    errors.append(f"值不在允许列表中: {allowed_list}")

        is_valid = len(errors) == 0
        result_data = {
            'valid': is_valid,
            'value': value,
            'original_value': original_value,
            'errors': errors
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        if is_valid:
            return ActionResult(
                success=True,
                message=f"字符串验证通过: '{value[:20]}...' ({len(value)} chars)",
                data=result_data
            )
        else:
            return ActionResult(
                success=False,
                message=f"字符串验证失败: {'; '.join(errors)}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'min_length': None,
            'max_length': None,
            'pattern': None,
            'allowed_values': None,
            'trim': False,
            'save_to_var': None
        }


class ValidateNumberAction(BaseAction):
    """Validate numeric values against constraints.
    
    Supports integer and float types, range validation,
    and precision control.
    """
    action_type = "validate_number"
    display_name = "验证数字"
    description = "验证数字类型、范围、精度"

    VALID_TYPES = ["int", "float", "integer", "decimal"]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate a numeric value.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, value_type, min_value,
                   max_value, precision, save_to_var.
        
        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', 0)
        value_type = params.get('value_type', 'float')
        min_value = params.get('min_value', None)
        max_value = params.get('max_value', None)
        precision = params.get('precision', None)
        save_to_var = params.get('save_to_var', None)

        errors = []

        # Convert value to number
        try:
            if value_type in ('int', 'integer'):
                numeric_value = int(value)
            else:
                numeric_value = float(value)
        except (ValueError, TypeError) as e:
            result_data = {'valid': False, 'error': f"无法转换为数字: {e}"}
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message=f"数字转换失败: {str(e)}",
                data=result_data
            )

        # Check type constraint
        if value_type in ('int', 'integer'):
            if not isinstance(numeric_value, int) or isinstance(numeric_value, bool):
                errors.append(f"需要整数类型，实际 {type(numeric_value).__name__}")

        # Check range constraints
        if min_value is not None:
            try:
                min_val = float(min_value)
                if numeric_value < min_val:
                    errors.append(f"值 {numeric_value} < 最小值 {min_val}")
            except (ValueError, TypeError):
                errors.append(f"Invalid min_value: {min_value}")

        if max_value is not None:
            try:
                max_val = float(max_value)
                if numeric_value > max_val:
                    errors.append(f"值 {numeric_value} > 最大值 {max_val}")
            except (ValueError, TypeError):
                errors.append(f"Invalid max_value: {max_value}")

        # Check precision
        if precision is not None:
            try:
                prec = int(precision)
                if prec < 0:
                    errors.append(f"Precision must be non-negative, got {prec}")
            except (ValueError, TypeError):
                errors.append(f"Invalid precision: {precision}")

        is_valid = len(errors) == 0
        result_data = {
            'valid': is_valid,
            'value': numeric_value,
            'errors': errors
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        if is_valid:
            return ActionResult(
                success=True,
                message=f"数字验证通过: {numeric_value}",
                data=result_data
            )
        else:
            return ActionResult(
                success=False,
                message=f"数字验证失败: {'; '.join(errors)}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'value_type': 'float',
            'min_value': None,
            'max_value': None,
            'precision': None,
            'save_to_var': None
        }


class ValidateEmailAction(BaseAction):
    """Validate email address format.
    
    Uses regex pattern for standard email format validation.
    """
    action_type = "validate_email"
    display_name = "验证邮箱"
    description = "验证邮箱地址格式是否合法"

    EMAIL_PATTERN = re.compile(
        r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    )

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate an email address.
        
        Args:
            context: Execution context.
            params: Dict with keys: email, save_to_var.
        
        Returns:
            ActionResult with validation result.
        """
        email = params.get('email', '')
        save_to_var = params.get('save_to_var', None)

        if not email:
            result_data = {'valid': False, 'error': '邮箱地址为空'}
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message="邮箱地址为空",
                data=result_data
            )

        is_valid = bool(self.EMAIL_PATTERN.match(str(email).strip()))

        # Extract parts if valid
        parts = {}
        if is_valid:
            at_idx = email.rfind('@')
            if at_idx > 0:
                parts = {
                    'local': email[:at_idx],
                    'domain': email[at_idx+1:]
                }

        result_data = {
            'valid': is_valid,
            'email': email.strip(),
            'parts': parts
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        if is_valid:
            return ActionResult(
                success=True,
                message=f"邮箱验证通过: {email}",
                data=result_data
            )
        else:
            return ActionResult(
                success=False,
                message=f"邮箱格式无效: {email}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['email']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'save_to_var': None}


class ValidateURLAction(BaseAction):
    """Validate URL format and optionally check availability.
    
    Supports HTTP/HTTPS URLs with query parameters.
    """
    action_type = "validate_url"
    display_name = "验证URL"
    description = "验证URL格式是否合法，可选检查URL可访问性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate a URL.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, schemes, check_exists,
                   timeout, save_to_var.
        
        Returns:
            ActionResult with validation result.
        """
        url = params.get('url', '')
        schemes = params.get('schemes', ['http', 'https'])
        check_exists = params.get('check_exists', False)
        timeout = params.get('timeout', 10)
        save_to_var = params.get('save_to_var', None)

        if not url:
            result_data = {'valid': False, 'error': 'URL为空'}
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(
                success=False,
                message="URL为空",
                data=result_data
            )

        errors = []

        # Check scheme
        if '://' in url:
            scheme = url.split('://')[0].lower()
            if scheme not in schemes:
                errors.append(f"URL方案必须是 {schemes} 之一，实际: {scheme}")
        else:
            errors.append("URL缺少scheme (需要 http:// 或 https://)")

        # Check basic format
        if not errors:
            try:
                from urllib.parse import urlparse
                parsed = urlparse(url)
                if not parsed.netloc:
                    errors.append("URL缺少域名部分")
                if not parsed.scheme:
                    errors.append("URL缺少scheme")
            except Exception as e:
                errors.append(f"URL解析失败: {e}")

        is_valid = len(errors) == 0

        result_data = {
            'valid': is_valid,
            'url': url,
            'errors': errors
        }

        # Check if URL exists (optional)
        if is_valid and check_exists:
            try:
                import urllib.request
                import urllib.error
                req = urllib.request.Request(url, method='HEAD')
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    result_data['exists'] = True
                    result_data['status'] = response.status
            except urllib.error.HTTPError as e:
                result_data['exists'] = True
                result_data['status'] = e.code
            except Exception as e:
                result_data['exists'] = False
                result_data['check_error'] = str(e)
        else:
            result_data['exists'] = None

        if save_to_var:
            context.variables[save_to_var] = result_data

        if is_valid:
            return ActionResult(
                success=True,
                message=f"URL验证通过: {url}",
                data=result_data
            )
        else:
            return ActionResult(
                success=False,
                message=f"URL验证失败: {'; '.join(errors)}",
                data=result_data
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'schemes': ['http', 'https'],
            'check_exists': False,
            'timeout': 10,
            'save_to_var': None
        }
