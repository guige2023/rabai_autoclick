"""Data formatter action module for RabAI AutoClick.

Provides data formatting operations:
- DataFormatterAction: Format data values
- DateFormatterAction: Format date/time values
- NumberFormatterAction: Format numeric values
- StringFormatterAction: Format string values
- CurrencyFormatterAction: Format currency values
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataFormatterAction(BaseAction):
    """Format data values."""
    action_type = "data_formatter"
    display_name = "数据格式化器"
    description = "格式化数据值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            format_rules = params.get("format_rules", {})
            output_format = params.get("output_format", "dict")

            formatted = {}

            for field, rule in format_rules.items():
                value = data.get(field)
                if value is None:
                    formatted[field] = None
                    continue

                format_type = rule.get("type", "string")

                if format_type == "uppercase":
                    formatted[field] = str(value).upper()
                elif format_type == "lowercase":
                    formatted[field] = str(value).lower()
                elif format_type == "title":
                    formatted[field] = str(value).title()
                elif format_type == "capitalize":
                    formatted[field] = str(value).capitalize()
                elif format_type == "strip":
                    formatted[field] = str(value).strip()
                elif format_type == "pad_left":
                    char = rule.get("char", " ")
                    width = rule.get("width", 10)
                    formatted[field] = str(value).rjust(width, char)
                elif format_type == "pad_right":
                    char = rule.get("char", " ")
                    width = rule.get("width", 10)
                    formatted[field] = str(value).ljust(width, char)
                elif format_type == "truncate":
                    length = rule.get("length", 50)
                    suffix = rule.get("suffix", "...")
                    if len(str(value)) > length:
                        formatted[field] = str(value)[:length] + suffix
                    else:
                        formatted[field] = str(value)
                else:
                    formatted[field] = value

            return ActionResult(
                success=True,
                data={
                    "formatted": formatted,
                    "original": data,
                    "fields_formatted": len(format_rules),
                    "output_format": output_format
                },
                message=f"Formatted {len(format_rules)} fields"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data formatter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"format_rules": {}, "output_format": "dict"}


class DateFormatterAction(BaseAction):
    """Format date/time values."""
    action_type = "data_date_formatter"
    display_name = "日期格式化器"
    description = "格式化日期/时间值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            date_fields = params.get("date_fields", [])
            input_format = params.get("input_format", "%Y-%m-%d")
            output_format = params.get("output_format", "%Y-%m-%d %H:%M:%S")
            timezone = params.get("timezone", "UTC")
            locale = params.get("locale", "en_US")

            formatted = dict(data)

            for field in date_fields:
                value = data.get(field)
                if value is None:
                    continue

                try:
                    if isinstance(value, str):
                        dt = datetime.strptime(value, input_format)
                    elif isinstance(value, (int, float)):
                        dt = datetime.fromtimestamp(value)
                    elif isinstance(value, datetime):
                        dt = value
                    else:
                        dt = datetime.strptime(str(value), input_format)

                    formatted[field] = dt.strftime(output_format)
                    formatted[f"{field}_iso"] = dt.isoformat()
                    formatted[f"{field}_unix"] = int(dt.timestamp())

                except Exception:
                    formatted[field] = str(value)

            return ActionResult(
                success=True,
                data={
                    "formatted": formatted,
                    "date_fields": date_fields,
                    "output_format": output_format,
                    "timezone": timezone
                },
                message=f"Formatted {len(date_fields)} date fields to {output_format}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Date formatter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"date_fields": [], "input_format": "%Y-%m-%d", "output_format": "%Y-%m-%d %H:%M:%S", "timezone": "UTC", "locale": "en_US"}


class NumberFormatterAction(BaseAction):
    """Format numeric values."""
    action_type = "data_number_formatter"
    display_name = "数字格式化器"
    description = "格式化数值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            number_fields = params.get("number_fields", [])
            decimal_places = params.get("decimal_places", 2)
            thousands_sep = params.get("thousands_sep", ",")
            decimal_sep = params.get("decimal_sep", ".")
            prefix = params.get("prefix", "")
            suffix = params.get("suffix", "")
            scientific_notation_threshold = params.get("scientific_notation_threshold", 1e9)

            formatted = dict(data)

            for field in number_fields:
                value = data.get(field)
                if value is None:
                    continue

                try:
                    num = float(value)

                    if abs(num) >= scientific_notation_threshold:
                        formatted[field] = f"{num:.2e}"
                    else:
                        if decimal_places == 0:
                            formatted[field] = f"{prefix}{int(num):,}{suffix}"
                        else:
                            formatted[field] = f"{prefix}{num:,.{decimal_places}f}{suffix}"

                    formatted[f"{field}_int"] = int(num)
                    formatted[f"{field}_float"] = float(num)
                    formatted[f"{field}_abs"] = abs(num)
                    formatted[f"{field}_sign"] = "positive" if num >= 0 else "negative"

                except (ValueError, TypeError):
                    formatted[field] = str(value)

            return ActionResult(
                success=True,
                data={
                    "formatted": formatted,
                    "number_fields": number_fields,
                    "decimal_places": decimal_places
                },
                message=f"Formatted {len(number_fields)} number fields"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Number formatter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"number_fields": [], "decimal_places": 2, "thousands_sep": ",", "decimal_sep": ".", "prefix": "", "suffix": "", "scientific_notation_threshold": 1e9}


class StringFormatterAction(BaseAction):
    """Format string values."""
    action_type = "data_string_formatter"
    display_name = "字符串格式化器"
    description = "格式化字符串值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            string_fields = params.get("string_fields", [])
            transforms = params.get("transforms", ["trim"])
            template = params.get("template")
            placeholders = params.get("placeholders", {})

            formatted = dict(data)

            for field in string_fields:
                value = str(data.get(field, ""))

                for transform in transforms:
                    if transform == "trim":
                        value = value.strip()
                    elif transform == "lower":
                        value = value.lower()
                    elif transform == "upper":
                        value = value.upper()
                    elif transform == "title":
                        value = value.title()
                    elif transform == "capitalize":
                        value = value.capitalize()
                    elif transform == "reverse":
                        value = value[::-1]
                    elif transform == "slugify":
                        import re
                        value = re.sub(r"[^\w\s-]", "", value).strip().lower()
                        value = re.sub(r"[-\s]+", "-", value)
                    elif transform == "md5":
                        import hashlib
                        value = hashlib.md5(value.encode()).hexdigest()
                    elif transform == "sha256":
                        import hashlib
                        value = hashlib.sha256(value.encode()).hexdigest()

                formatted[field] = value

            if template:
                for placeholder, value in placeholders.items():
                    template = template.replace(f"{{{placeholder}}}", str(value))
                formatted["_template_output"] = template

            return ActionResult(
                success=True,
                data={
                    "formatted": formatted,
                    "string_fields": string_fields,
                    "transforms_applied": transforms
                },
                message=f"Formatted {len(string_fields)} string fields with {len(transforms)} transforms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"String formatter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"string_fields": [], "transforms": ["trim"], "template": None, "placeholders": {}}


class CurrencyFormatterAction(BaseAction):
    """Format currency values."""
    action_type = "data_currency_formatter"
    display_name = "货币格式化器"
    description = "格式化货币值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            currency_fields = params.get("currency_fields", [])
            currency_code = params.get("currency_code", "USD")
            decimal_places = params.get("decimal_places", 2)
            show_symbol = params.get("show_symbol", True)
            symbol_position = params.get("symbol_position", "before")

            currency_symbols = {
                "USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥", "CNY": "¥",
                "KRW": "₩", "INR": "₹", "RUB": "₽", "BRL": "R$", "CAD": "C$",
                "AUD": "A$", "CHF": "CHF", "HKD": "HK$", "SGD": "S$"
            }

            formatted = dict(data)
            symbol = currency_symbols.get(currency_code, currency_code)

            for field in currency_fields:
                value = data.get(field)
                if value is None:
                    continue

                try:
                    num = float(value)

                    if show_symbol:
                        if symbol_position == "before":
                            formatted[field] = f"{symbol}{num:,.{decimal_places}f}"
                        else:
                            formatted[field] = f"{num:,.{decimal_places}f}{symbol}"
                    else:
                        formatted[field] = f"{num:,.{decimal_places}f}"

                    formatted[f"{field}_value"] = num
                    formatted[f"{field}_code"] = currency_code
                    formatted[f"{field}_symbol"] = symbol
                    formatted[f"{field}_cents"] = int(num * 100)

                except (ValueError, TypeError):
                    formatted[field] = str(value)

            return ActionResult(
                success=True,
                data={
                    "formatted": formatted,
                    "currency_fields": currency_fields,
                    "currency_code": currency_code,
                    "symbol": symbol
                },
                message=f"Formatted {len(currency_fields)} currency fields as {currency_code}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Currency formatter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"currency_fields": [], "currency_code": "USD", "decimal_places": 2, "show_symbol": True, "symbol_position": "before"}
