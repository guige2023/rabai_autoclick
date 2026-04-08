"""Format utilities action module for RabAI AutoClick.

Provides formatting operations for numbers, strings,
dates, and files with locale support.
"""

import sys
import os
from typing import Any, Dict, List, Optional
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FormatNumberAction(BaseAction):
    """Format numbers with thousands separator.
    
    Supports decimal places, sign display, and locale.
    """
    action_type = "format_number"
    display_name = "格式化数字"
    description = "格式化数字显示"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Format number.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, decimals, thousands_sep,
                   prefix, suffix, save_to_var.
        
        Returns:
            ActionResult with formatted string.
        """
        value = params.get('value', 0)
        decimals = params.get('decimals', 2)
        thousands_sep = params.get('thousands_sep', ',')
        prefix = params.get('prefix', '')
        suffix = params.get('suffix', '')
        save_to_var = params.get('save_to_var', None)

        try:
            num = float(value)
            formatted = f'{num:,.{decimals}f}'
            if thousands_sep != ',':
                formatted = formatted.replace(',', thousands_sep)
            formatted = prefix + formatted + suffix

            result_data = {
                'formatted': formatted,
                'original': num,
                'prefix': prefix,
                'suffix': suffix
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"数字格式化: {formatted}",
                data=result_data
            )

        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"数字格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'decimals': 2,
            'thousands_sep': ',',
            'prefix': '',
            'suffix': '',
            'save_to_var': None
        }


class FormatBytesAction(BaseAction):
    """Format bytes to human-readable size.
    
    Converts bytes to KB, MB, GB, TB, etc.
    """
    action_type = "format_bytes"
    display_name = "格式化字节"
    description = "格式化字节为人类可读大小"

    UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Format bytes.
        
        Args:
            context: Execution context.
            params: Dict with keys: bytes, precision,
                   unit, save_to_var.
        
        Returns:
            ActionResult with formatted size.
        """
        bytes_val = params.get('bytes', 0)
        precision = params.get('precision', 2)
        unit = params.get('unit', None)
        save_to_var = params.get('save_to_var', None)

        try:
            size = float(bytes_val)

            if unit:
                # Convert to specified unit
                if unit.upper() in self.UNITS:
                    unit_idx = self.UNITS.index(unit.upper())
                    size = size / (1024 ** unit_idx)
                else:
                    return ActionResult(
                        success=False,
                        message=f"Invalid unit: {unit}"
                    )
            else:
                # Auto-scale
                unit_idx = 0
                while size >= 1024 and unit_idx < len(self.UNITS) - 1:
                    size /= 1024
                    unit_idx += 1

            formatted = f'{size:.{precision}f} {self.UNITS[unit_idx]}'

            result_data = {
                'formatted': formatted,
                'bytes': int(bytes_val),
                'size': round(size, precision),
                'unit': self.UNITS[unit_idx]
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"字节格式化: {formatted}",
                data=result_data
            )

        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"字节格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bytes']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'precision': 2,
            'unit': None,
            'save_to_var': None
        }


class FormatDurationAction(BaseAction):
    """Format duration in seconds to human-readable string.
    
    Converts seconds to days, hours, minutes, seconds.
    """
    action_type = "format_duration"
    display_name = "格式化时长"
    description = "格式化时长为人类可读字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Format duration.
        
        Args:
            context: Execution context.
            params: Dict with keys: seconds, format, precision,
                   save_to_var.
        
        Returns:
            ActionResult with formatted duration.
        """
        seconds = params.get('seconds', 0)
        format_type = params.get('format', 'full')  # full, short, compact
        precision = params.get('precision', 0)
        save_to_var = params.get('save_to_var', None)

        try:
            secs = float(seconds)
            if secs < 0:
                return ActionResult(success=False, message="Duration cannot be negative")

            # Calculate components
            days = int(secs // 86400)
            secs %= 86400
            hours = int(secs // 3600)
            secs %= 3600
            minutes = int(secs // 60)
            secs %= 60

            if format_type == 'full':
                parts = []
                if days:
                    parts.append(f"{days}d")
                if hours:
                    parts.append(f"{hours}h")
                if minutes:
                    parts.append(f"{minutes}m")
                if secs >= 1 or not parts:
                    parts.append(f"{int(secs)}s")
                formatted = ' '.join(parts)
            elif format_type == 'short':
                total_mins = int(secs // 60)
                secs = int(secs % 60)
                total_hours = int(total_mins // 60)
                total_mins = total_mins % 60
                total_days = int(total_hours // 24)
                total_hours = total_hours % 24
                formatted = f"{total_days:02d}:{total_hours:02d}:{total_mins:02d}:{secs:02d}"
            else:  # compact
                formatted = f"{secs:.{precision}f}s"

            result_data = {
                'formatted': formatted,
                'seconds': secs,
                'minutes': minutes,
                'hours': hours,
                'days': days
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"时长格式化: {formatted}",
                data=result_data
            )

        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"时长格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['seconds']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'format': 'full',
            'precision': 0,
            'save_to_var': None
        }
