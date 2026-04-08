"""Data formatter action module for RabAI AutoClick.

Provides data formatting capabilities including string formatting,
date/time formatting, and number formatting.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataFormatterAction(BaseAction):
    """Data formatter action for formatting data values.
    
    Supports string formatting, date/time formatting,
    number formatting, and currency formatting.
    """
    action_type = "data_formatter"
    display_name = "数据格式化"
    description = "字符串与日期格式化"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute formatting operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: format|format_date|format_number|format_string
                data: Data to format
                field: Field to format
                format_string: Format pattern
                locale: Locale for formatting.
        
        Returns:
            ActionResult with formatted data.
        """
        operation = params.get('operation', 'format')
        data = params.get('data', [])
        field = params.get('field')
        format_string = params.get('format_string', '')
        locale = params.get('locale', 'en_US')
        
        if isinstance(data, list):
            return self._format_list(data, field, operation, format_string, locale)
        else:
            return self._format_single(data, operation, format_string, locale)
    
    def _format_list(
        self,
        data: List[Any],
        field: Optional[str],
        operation: str,
        format_string: str,
        locale: str
    ) -> ActionResult:
        """Format list of items."""
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        formatted = []
        
        for item in data:
            if field and isinstance(item, dict):
                value = item.get(field)
                new_item = dict(item)
                new_item[f'{field}_formatted'] = self._format_value(value, operation, format_string, locale)
                formatted.append(new_item)
            else:
                formatted.append(self._format_value(item, operation, format_string, locale))
        
        return ActionResult(
            success=True,
            message=f"Formatted {len(formatted)} items",
            data={
                'items': formatted,
                'count': len(formatted)
            }
        )
    
    def _format_single(
        self,
        data: Any,
        operation: str,
        format_string: str,
        locale: str
    ) -> ActionResult:
        """Format single value."""
        result = self._format_value(data, operation, format_string, locale)
        
        return ActionResult(
            success=True,
            message=f"Formatted: {result}",
            data={
                'value': result,
                'original': data
            }
        )
    
    def _format_value(
        self,
        value: Any,
        operation: str,
        format_string: str,
        locale: str
    ) -> Any:
        """Format a single value."""
        if value is None:
            return None
        
        if operation == 'format' or operation == 'format_string':
            return self._format_string(value, format_string)
        elif operation == 'format_date':
            return self._format_date(value, format_string)
        elif operation == 'format_number':
            return self._format_number(value, format_string, locale)
        elif operation == 'uppercase':
            return str(value).upper()
        elif operation == 'lowercase':
            return str(value).lower()
        elif operation == 'titlecase':
            return str(value).title()
        elif operation == 'capitalize':
            return str(value).capitalize()
        elif operation == 'strip':
            return str(value).strip()
        elif operation == 'pad_left':
            width = int(format_string) if format_string else 10
            return str(value).zfill(width)
        elif operation == 'pad_right':
            width = int(format_string) if format_string else 10
            return str(value).ljust(width)
        
        return value
    
    def _format_string(self, value: Any, format_string: str) -> str:
        """Format string with pattern."""
        if not format_string:
            return str(value)
        
        if format_string == 'upper':
            return str(value).upper()
        elif format_string == 'lower':
            return str(value).lower()
        elif format_string == 'title':
            return str(value).title()
        elif format_string == 'capitalize':
            return str(value).capitalize()
        elif format_string == 'reverse':
            return str(value)[::-1]
        elif format_string == 'slugify':
            return re.sub(r'[^a-z0-9]+', '-', str(value).lower()).strip('-')
        elif format_string == 'md5':
            import hashlib
            return hashlib.md5(str(value).encode()).hexdigest()
        elif format_string == 'sha256':
            import hashlib
            return hashlib.sha256(str(value).encode()).hexdigest()
        
        return str(value)
    
    def _format_date(self, value: Any, format_string: str) -> str:
        """Format date/datetime value."""
        if format_string is None:
            format_string = '%Y-%m-%d %H:%M:%S'
        
        if isinstance(value, datetime):
            return value.strftime(format_string)
        
        if isinstance(value, str):
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                try:
                    dt = datetime.strptime(value, fmt)
                    return dt.strftime(format_string)
                except ValueError:
                    continue
        
        return str(value)
    
    def _format_number(self, value: Any, format_string: str, locale: str) -> str:
        """Format number with pattern."""
        try:
            num = float(value)
        except (TypeError, ValueError):
            return str(value)
        
        if not format_string:
            if num == int(num):
                return f'{int(num):,}'
            return f'{num:,.2f}'
        
        if format_string == 'currency':
            if locale.startswith('en'):
                return f'${num:,.2f}'
            elif locale.startswith('de'):
                return f'{num:,.2f} €'.replace(',', 'X').replace('.', ',').replace('X', '.')
        
        if format_string == 'percent':
            return f'{num * 100:.1f}%'
        
        if format_string == 'compact':
            if abs(num) >= 1_000_000:
                return f'{num / 1_000_000:.1f}M'
            elif abs(num) >= 1_000:
                return f'{num / 1_000:.1f}K'
            return str(num)
        
        return str(num)
