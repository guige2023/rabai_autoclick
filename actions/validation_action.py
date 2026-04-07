"""Data validation action module for RabAI AutoClick.

Provides validation operations:
- ValidateEmailAction: Validate email
- ValidateUrlAction: Validate URL
- ValidatePhoneAction: Validate phone number
- ValidateIpAction: Validate IP address
- ValidateJsonAction: Validate JSON
- ValidateNumberAction: Validate number range
- ValidateLengthAction: Validate string length
- ValidateRegexAction: Validate with regex
"""

from __future__ import annotations

import re
import sys
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidateEmailAction(BaseAction):
    """Validate email address."""
    action_type = "validate_email"
    display_name = "邮箱验证"
    description = "验证邮箱地址"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute email validation."""
        email = params.get('email', '')
        output_var = params.get('output_var', 'email_valid')

        if not email:
            return ActionResult(success=False, message="email is required")

        try:
            resolved = context.resolve_value(email) if context else email
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            valid = bool(re.match(pattern, str(resolved)))

            result = {'valid': valid, 'email': str(resolved)}
            if context:
                context.set(output_var, valid)
            return ActionResult(success=valid, message=f"Email {'valid' if valid else 'invalid'}: {resolved}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Email validation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['email']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'email_valid'}


class ValidateUrlAction(BaseAction):
    """Validate URL."""
    action_type = "validate_url"
    display_name = "URL验证"
    description = "验证URL"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute URL validation."""
        url = params.get('url', '')
        schemes = params.get('schemes', ['http', 'https'])
        output_var = params.get('output_var', 'url_valid')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            from urllib.parse import urlparse

            resolved = context.resolve_value(url) if context else url
            resolved_schemes = context.resolve_value(schemes) if context else schemes

            parsed = urlparse(resolved)
            valid = bool(parsed.scheme and parsed.netloc and parsed.scheme in resolved_schemes)

            result = {'valid': valid, 'scheme': parsed.scheme, 'netloc': parsed.netloc}
            if context:
                context.set(output_var, valid)
            return ActionResult(success=valid, message=f"URL {'valid' if valid else 'invalid'}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"URL validation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'schemes': ['http', 'https'], 'output_var': 'url_valid'}


class ValidatePhoneAction(BaseAction):
    """Validate phone number."""
    action_type = "validate_phone"
    display_name = "手机号验证"
    description = "验证手机号"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute phone validation."""
        phone = params.get('phone', '')
        country = params.get('country', 'CN')  # CN, US, etc.
        output_var = params.get('output_var', 'phone_valid')

        if not phone:
            return ActionResult(success=False, message="phone is required")

        try:
            resolved = context.resolve_value(phone) if context else phone
            resolved_country = context.resolve_value(country) if context else country

            patterns = {
                'CN': r'^1[3-9]\d{9}$',
                'US': r'^\+?1?\d{10}$',
                'UK': r'^\+?44\d{10}$',
                'DEFAULT': r'^\+?[\d\s-]{10,}$',
            }
            pattern = patterns.get(resolved_country, patterns['DEFAULT'])
            cleaned = re.sub(r'[\s\-\(\)]', '', str(resolved))
            valid = bool(re.match(pattern, cleaned))

            result = {'valid': valid, 'phone': resolved, 'country': resolved_country}
            if context:
                context.set(output_var, valid)
            return ActionResult(success=valid, message=f"Phone {'valid' if valid else 'invalid'}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Phone validation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['phone']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'country': 'CN', 'output_var': 'phone_valid'}


class ValidateIpAction(BaseAction):
    """Validate IP address."""
    action_type = "validate_ip"
    display_name = "IP地址验证"
    description = "验证IP地址"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute IP validation."""
        ip = params.get('ip', '')
        ip_type = params.get('ip_type', None)  # None, 4, 6
        output_var = params.get('output_var', 'ip_valid')

        if not ip:
            return ActionResult(success=False, message="ip is required")

        try:
            import socket

            resolved = context.resolve_value(ip) if context else ip
            resolved_type = context.resolve_value(ip_type) if context else ip_type

            valid = False
            ip_version = None

            try:
                socket.inet_pton(socket.AF_INET6, str(resolved))
                valid = True
                ip_version = 6
            except socket.error:
                try:
                    socket.inet_aton(str(resolved))
                    valid = True
                    ip_version = 4
                except socket.error:
                    valid = False

            if resolved_type:
                expected = int(resolved_type)
                valid = valid and ip_version == expected

            result = {'valid': valid, 'ip': str(resolved), 'version': ip_version}
            if context:
                context.set(output_var, valid)
            return ActionResult(success=valid, message=f"IP {'valid' if valid else 'invalid'}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"IP validation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['ip']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'ip_type': None, 'output_var': 'ip_valid'}


class ValidateJsonAction(BaseAction):
    """Validate JSON string."""
    action_type = "validate_json"
    display_name = "JSON验证"
    description = "验证JSON"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute JSON validation."""
        json_str = params.get('json', '')
        output_var = params.get('output_var', 'json_valid')

        if not json_str:
            return ActionResult(success=False, message="json is required")

        try:
            import json

            resolved = context.resolve_value(json_str) if context else json_str
            json.loads(resolved)

            result = {'valid': True}
            if context:
                context.set(output_var, True)
            return ActionResult(success=True, message="JSON is valid")
        except json.JSONDecodeError as e:
            result = {'valid': False, 'error': str(e)}
            if context:
                context.set(output_var, False)
            return ActionResult(success=False, message=f"Invalid JSON: {str(e)}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"JSON validation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['json']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'json_valid'}


class ValidateNumberAction(BaseAction):
    """Validate number in range."""
    action_type = "validate_number"
    display_name = "数字范围验证"
    description = "验证数字范围"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute number validation."""
        value = params.get('value', None)
        min_val = params.get('min', None)
        max_val = params.get('max', None)
        integer_only = params.get('integer_only', False)
        output_var = params.get('output_var', 'number_valid')

        if value is None:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            resolved_min = context.resolve_value(min_val) if context else min_val
            resolved_max = context.resolve_value(max_val) if context else max_val
            resolved_int = context.resolve_value(integer_only) if context else integer_only

            num = float(resolved)
            if resolved_int and not num.is_integer():
                valid = False
            elif resolved_min is not None and num < float(resolved_min):
                valid = False
            elif resolved_max is not None and num > float(resolved_max):
                valid = False
            else:
                valid = True

            result = {'valid': valid, 'value': num, 'min': resolved_min, 'max': resolved_max}
            if context:
                context.set(output_var, valid)
            return ActionResult(success=valid, message=f"Number {'in' if valid else 'out of'} range", data=result)
        except ValueError:
            result = {'valid': False, 'error': 'Not a number'}
            if context:
                context.set(output_var, False)
            return ActionResult(success=False, message=f"Not a number: {resolved}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Number validation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'min': None, 'max': None, 'integer_only': False, 'output_var': 'number_valid'}


class ValidateLengthAction(BaseAction):
    """Validate string length."""
    action_type = "validate_length"
    display_name = "字符串长度验证"
    description = "验证字符串长度"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute length validation."""
        value = params.get('value', '')
        min_len = params.get('min_length', None)
        max_len = params.get('max_length', None)
        output_var = params.get('output_var', 'length_valid')

        try:
            resolved = context.resolve_value(value) if context else value
            resolved_min = context.resolve_value(min_len) if context else min_len
            resolved_max = context.resolve_value(max_len) if context else max_len

            length = len(str(resolved))
            if resolved_min is not None and length < int(resolved_min):
                valid = False
            elif resolved_max is not None and length > int(resolved_max):
                valid = False
            else:
                valid = True

            result = {'valid': valid, 'length': length, 'min': resolved_min, 'max': resolved_max}
            if context:
                context.set(output_var, valid)
            return ActionResult(success=valid, message=f"Length {length}: {'valid' if valid else 'invalid'}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Length validation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'min_length': None, 'max_length': None, 'output_var': 'length_valid'}


class ValidateRegexAction(BaseAction):
    """Validate with regex pattern."""
    action_type = "validate_regex"
    display_name = "正则验证"
    description = "正则验证"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute regex validation."""
        value = params.get('value', '')
        pattern = params.get('pattern', '')
        output_var = params.get('output_var', 'regex_valid')

        if not value or not pattern:
            return ActionResult(success=False, message="value and pattern are required")

        try:
            import re as re_module

            resolved_val = context.resolve_value(value) if context else value
            resolved_pattern = context.resolve_value(pattern) if context else pattern

            match = re_module.search(resolved_pattern, str(resolved_val))
            valid = match is not None

            result = {'valid': valid, 'pattern': resolved_pattern, 'value': str(resolved_val)}
            if context:
                context.set(output_var, valid)
            return ActionResult(success=valid, message=f"Regex {'matched' if valid else 'not matched'}", data=result)
        except re_module.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Regex validation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value', 'pattern']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'regex_valid'}
