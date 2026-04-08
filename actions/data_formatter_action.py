"""Data formatter action module for RabAI AutoClick.

Provides data formatting operations including string formatting,
number formatting, date formatting, and type conversion.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StringFormatAction(BaseAction):
    """Format string fields with templates and patterns.
    
    Applies string formatting including templating,
    padding, trimming, and case conversion.
    """
    action_type = "string_format"
    display_name = "字符串格式化"
    description = "格式化字符串字段"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Format string values.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, template, operations
                   (list of {op, value}), result_field.
        
        Returns:
            ActionResult with formatted data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        template = params.get('template', '')
        operations = params.get('operations', [])
        result_field = params.get('result_field', 'formatted')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        results = []
        for row in data:
            new_row = dict(row)
            value = self._get_field(row, field)

            if template:
                value = self._apply_template(template, row)

            for op_def in operations:
                op = op_def.get('op', '')
                value = self._apply_string_op(value, op, op_def)

            new_row[result_field] = value
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Formatted {len(results)} strings",
            data={
                'data': results,
                'result_field': result_field,
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _apply_template(self, template: str, row: Dict) -> str:
        """Apply template with field placeholders."""
        result = template
        for key, value in row.items():
            placeholder = f'{{{key}}}'
            if placeholder in result:
                result = result.replace(placeholder, str(value or ''))
        return result

    def _apply_string_op(self, value: Any, op: str, op_def: Dict) -> Any:
        """Apply string operation."""
        s = str(value) if value is not None else ''
        if op == 'upper':
            return s.upper()
        elif op == 'lower':
            return s.lower()
        elif op == 'title':
            return s.title()
        elif op == 'capitalize':
            return s.capitalize()
        elif op == 'strip':
            return s.strip()
        elif op == 'lstrip':
            return s.lstrip()
        elif op == 'rstrip':
            return s.rstrip()
        elif op == 'pad_left':
            width = op_def.get('width', 0)
            char = op_def.get('char', ' ')
            return s.rjust(width, char)
        elif op == 'pad_right':
            width = op_def.get('width', 0)
            char = op_def.get('char', ' ')
            return s.ljust(width, char)
        elif op == 'center':
            width = op_def.get('width', 0)
            char = op_def.get('char', ' ')
            return s.center(width, char)
        elif op == 'replace':
            old = op_def.get('old', '')
            new = op_def.get('new', '')
            return s.replace(old, new)
        elif op == 'truncate':
            length = op_def.get('length', 100)
            suffix = op_def.get('suffix', '...')
            if len(s) > length:
                return s[:length] + suffix
            return s
        elif op == 'regex_replace':
            import re
            pattern = op_def.get('pattern', '')
            repl = op_def.get('replacement', '')
            return re.sub(pattern, repl, s)
        elif op == 'prepend':
            return str(op_def.get('text', '')) + s
        elif op == 'append':
            return s + str(op_def.get('text', ''))
        elif op == 'reverse':
            return s[::-1]
        return s

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class NumberFormatAction(BaseAction):
    """Format numeric fields with precision and display options.
    
    Formats numbers with thousands separators, fixed decimal
    places, currency symbols, and scientific notation.
    """
    action_type = "number_format"
    display_name = "数字格式化"
    description = "格式化数字字段"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Format numeric values.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, format_type (fixed|comma|scientific|currency|percent),
                   decimals, currency_symbol, result_field.
        
        Returns:
            ActionResult with formatted data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        format_type = params.get('format_type', 'fixed')
        decimals = params.get('decimals', 2)
        currency_symbol = params.get('currency_symbol', '$')
        result_field = params.get('result_field', 'formatted')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        results = []
        for row in data:
            new_row = dict(row)
            value = self._get_field(row, field)
            try:
                num = float(value) if value is not None else 0
            except (TypeError, ValueError):
                num = 0

            formatted = self._format_number(num, format_type, decimals, currency_symbol)
            new_row[result_field] = formatted
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Formatted {len(results)} numbers as {format_type}",
            data={
                'data': results,
                'result_field': result_field,
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _format_number(
        self,
        num: float,
        format_type: str,
        decimals: int,
        currency_symbol: str
    ) -> str:
        """Format number based on type."""
        if format_type == 'fixed':
            return f"{num:.{decimals}f}"
        elif format_type == 'comma':
            return f"{num:,.{decimals}f}"
        elif format_type == 'scientific':
            return f"{num:.{decimals}e}"
        elif format_type == 'currency':
            return f"{currency_symbol}{num:,.{decimals}f}"
        elif format_type == 'percent':
            return f"{num * 100:.{decimals}f}%"
        elif format_type == 'int':
            return str(int(num))
        elif format_type == 'round':
            return str(round(num))
        return str(num)

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class DateFormatAction(BaseAction):
    """Format date/datetime fields to various string representations.
    
    Converts date values to formatted strings with customizable
    patterns and locale settings.
    """
    action_type = "date_format"
    display_name = "日期格式化"
    description = "格式化日期字段"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Format date values.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, output_format,
                   input_format (auto|strptime pattern), result_field.
        
        Returns:
            ActionResult with formatted data.
        """
        import datetime

        data = params.get('data', [])
        field = params.get('field', '')
        output_format = params.get('output_format', '%Y-%m-%d')
        input_format = params.get('input_format', 'auto')
        result_field = params.get('result_field', 'formatted_date')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        results = []
        for row in data:
            new_row = dict(row)
            value = self._get_field(row, field)
            parsed = self._parse_date(value, input_format)

            if parsed:
                new_row[result_field] = parsed.strftime(output_format)
            else:
                new_row[result_field] = None
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Formatted {len(results)} dates",
            data={
                'data': results,
                'result_field': result_field,
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _parse_date(self, value: Any, input_format: str):
        """Parse date from various formats."""
        import datetime

        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, (int, float)):
            try:
                return datetime.datetime.fromtimestamp(value)
            except:
                return None

        if input_format == 'auto':
            formats = [
                '%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y',
                '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f', '%d %b %Y', '%d %B %Y',
            ]
            for fmt in formats:
                try:
                    return datetime.datetime.strptime(str(value), fmt)
                except ValueError:
                    pass
            return None
        else:
            try:
                return datetime.datetime.strptime(str(value), input_format)
            except ValueError:
                return None

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class TypeConvertAction(BaseAction):
    """Convert field values between data types.
    
    Converts strings to numbers, numbers to strings,
    dates to strings, and other type conversions.
    """
    action_type = "type_convert"
    display_name = "类型转换"
    description = "转换字段数据类型"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Convert field types.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, to_type (int|float|str|bool|date),
                   result_field, date_format, default_value.
        
        Returns:
            ActionResult with converted data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        to_type = params.get('to_type', 'str')
        result_field = params.get('result_field', 'converted')
        date_format = params.get('date_format', '%Y-%m-%d')
        default_value = params.get('default_value', None)
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        results = []
        for row in data:
            new_row = dict(row)
            value = self._get_field(row, field)
            converted = self._convert(value, to_type, date_format, default_value)
            new_row[result_field] = converted
            results.append(new_row)

        return ActionResult(
            success=True,
            message=f"Converted {len(results)} values to {to_type}",
            data={
                'data': results,
                'result_field': result_field,
                'to_type': to_type,
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _convert(self, value: Any, to_type: str, date_format: str, default: Any) -> Any:
        """Convert value to target type."""
        if value is None:
            return default

        if to_type == 'str':
            if hasattr(value, 'strftime'):
                return value.strftime(date_format)
            return str(value)
        elif to_type == 'int':
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return default
        elif to_type == 'float':
            try:
                return float(value)
            except (TypeError, ValueError):
                return default
        elif to_type == 'bool':
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        elif to_type == 'date':
            import datetime
            if isinstance(value, datetime.datetime):
                return value
            formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y-%m-%dT%H:%M:%S']
            for fmt in formats:
                try:
                    return datetime.datetime.strptime(str(value), fmt)
                except:
                    pass
            return default
        return value

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value


class NullReplaceAction(BaseAction):
    """Replace null/empty values with specified replacements.
    
    Fills null values with defaults, previous values,
    computed values, or field-specific replacements.
    """
    action_type = "null_replace"
    display_name = "空值替换"
    description = "替换空值为指定值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Replace null values.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, replacement_type (static|forward|backward|mean),
                   static_value, result_field.
        
        Returns:
            ActionResult with replaced data.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        replacement_type = params.get('replacement_type', 'static')
        static_value = params.get('static_value', '')
        result_field = params.get('result_field', 'filled')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        results = []
        running_values = []

        if replacement_type == 'mean':
            numeric_values = []
            for row in data:
                val = self._get_field(row, field)
                try:
                    numeric_values.append(float(val))
                except (TypeError, ValueError):
                    pass
            fill_value = sum(numeric_values) / len(numeric_values) if numeric_values else static_value
        else:
            fill_value = static_value

        prev_value = None
        for row in data:
            new_row = dict(row)
            value = self._get_field(row, field)
            is_null = value is None or value == '' or value == []

            if is_null:
                if replacement_type == 'static':
                    value = static_value
                elif replacement_type == 'forward':
                    value = prev_value if prev_value is not None else static_value
                elif replacement_type == 'backward':
                    value = None
                elif replacement_type == 'mean':
                    value = fill_value
                elif replacement_type == 'interpolate':
                    running_values.append(value)
                else:
                    value = static_value
            else:
                prev_value = value
                running_values.append(value)

            new_row[result_field] = value
            results.append(new_row)

        if replacement_type == 'backward':
            next_value = None
            for row in reversed(results):
                if row[result_field] is None:
                    row[result_field] = next_value if next_value is not None else static_value
                else:
                    next_value = row[result_field]

        if replacement_type == 'interpolate':
            valid = []
            for row in results:
                v = row[result_field]
                if v is not None and v != '':
                    valid.append(v)
            if valid:
                avg = sum(valid) / len(valid)
                for row in results:
                    if row[result_field] is None or row[result_field] == '':
                        row[result_field] = avg

        return ActionResult(
            success=True,
            message=f"Replaced nulls in {len(results)} rows ({replacement_type})",
            data={
                'data': results,
                'result_field': result_field,
                'replacement_type': replacement_type,
                'count': len(results)
            },
            duration=time.time() - start_time
        )

    def _get_field(self, row: Any, field: str) -> Any:
        if not field:
            return row
        keys = field.split('.')
        value = row
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif hasattr(value, k):
                value = getattr(value, k)
            else:
                return None
        return value
