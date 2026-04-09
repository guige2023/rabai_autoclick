"""Data Normalizer Action Module.

Provides data normalization and standardization operations.
"""

import re
import math
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataNormalizerAction(BaseAction):
    """Normalize data to standard formats.
    
    Handles string normalization, date/time normalization, and numeric scaling.
    """
    action_type = "data_normalizer"
    display_name = "数据标准化"
    description = "将数据标准化为统一格式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute normalization.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, normalization_type, options.
        
        Returns:
            ActionResult with normalized data.
        """
        data = params.get('data', [])
        norm_type = params.get('normalization_type', 'minmax')
        options = params.get('options', {})
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data to normalize"
            )
        
        try:
            if isinstance(data, list) and len(data) > 0:
                if norm_type == 'minmax':
                    normalized = self._minmax_normalize(data, options)
                elif norm_type == 'zscore':
                    normalized = self._zscore_normalize(data, options)
                elif norm_type == 'robust':
                    normalized = self._robust_normalize(data, options)
                elif norm_type == 'decimal':
                    normalized = self._decimal_normalize(data, options)
                elif norm_type == 'log':
                    normalized = self._log_normalize(data, options)
                else:
                    return ActionResult(
                        success=False,
                        data=None,
                        error=f"Unknown normalization type: {norm_type}"
                    )
            else:
                normalized = self._normalize_single(data, norm_type, options)
            
            return ActionResult(
                success=True,
                data={
                    'normalized': normalized,
                    'type': norm_type
                },
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Normalization failed: {str(e)}"
            )
    
    def _minmax_normalize(self, data: List, options: Dict) -> List:
        """Min-max normalization to [0, 1] range."""
        min_val = options.get('min', min(data))
        max_val = options.get('max', max(data))
        range_val = max_val - min_val
        
        if range_val == 0:
            return [0.5] * len(data)
        
        return [(x - min_val) / range_val for x in data]
    
    def _zscore_normalize(self, data: List, options: Dict) -> List:
        """Z-score normalization (standardization)."""
        mean = sum(data) / len(data)
        variance = sum((x - mean) ** 2 for x in data) / len(data)
        stddev = math.sqrt(variance)
        
        if stddev == 0:
            return [0.0] * len(data)
        
        return [(x - mean) / stddev for x in data]
    
    def _robust_normalize(self, data: List, options: Dict) -> List:
        """Robust normalization using median and IQR."""
        sorted_data = sorted(data)
        n = len(sorted_data)
        
        median = sorted_data[n // 2]
        q1 = sorted_data[n // 4]
        q3 = sorted_data[3 * n // 4]
        iqr = q3 - q1
        
        if iqr == 0:
            return [0.0] * len(data)
        
        return [(x - median) / iqr for x in data]
    
    def _decimal_normalize(self, data: List, options: Dict) -> List:
        """Decimal scaling normalization."""
        max_abs = max(abs(x) for x in data)
        if max_abs == 0:
            return data
        
        # Find appropriate scale
        scale = 1
        while max_abs >= 10:
            max_abs /= 10
            scale *= 10
        
        return [x / scale for x in data]
    
    def _log_normalize(self, data: List, options: Dict) -> List:
        """Log transformation normalization."""
        min_val = min(data)
        shift = options.get('shift', 1 - min_val if min_val <= 0 else 0)
        
        return [math.log(x + shift) for x in data]
    
    def _normalize_single(self, data: Any, norm_type: str, options: Dict) -> Any:
        """Normalize a single non-list value."""
        return data


class StringNormalizerAction(BaseAction):
    """Normalize string data.
    
    Handles case normalization, whitespace, special characters, and encoding.
    """
    action_type = "string_normalizer"
    display_name = "字符串标准化"
    description = "标准化字符串数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute string normalization.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, operations.
        
        Returns:
            ActionResult with normalized string.
        """
        text = params.get('text', '')
        operations = params.get('operations', ['trim', 'lowercase'])
        
        if not isinstance(text, str):
            return ActionResult(
                success=False,
                data=None,
                error="Input is not a string"
            )
        
        result = text
        
        for op in operations:
            if op == 'trim':
                result = result.strip()
            elif op == 'lowercase':
                result = result.lower()
            elif op == 'uppercase':
                result = result.upper()
            elif op == 'titlecase':
                result = result.title()
            elif op == 'remove_whitespace':
                result = re.sub(r'\s+', '', result)
            elif op == 'normalize_whitespace':
                result = re.sub(r'\s+', ' ', result)
            elif op == 'remove_special':
                result = re.sub(r'[^a-zA-Z0-9\s]', '', result)
            elif op == 'remove_accents':
                result = self._remove_accents(result)
            elif op == 'remove_punctuation':
                result = re.sub(r'[^\w\s]', '', result)
        
        return ActionResult(
            success=True,
            data={
                'original': text,
                'normalized': result,
                'operations_applied': operations
            },
            error=None
        )
    
    def _remove_accents(self, text: str) -> str:
        """Remove accents from text."""
        # Simple implementation
        replacements = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U'
        }
        for char, replacement in replacements.items():
            text = text.replace(char, replacement)
        return text


class DateTimeNormalizerAction(BaseAction):
    """Normalize date and time data.
    
    Handles various date formats and converts to standard representations.
    """
    action_type = "datetime_normalizer"
    display_name = "日期时间标准化"
    description = "标准化日期时间数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute datetime normalization.
        
        Args:
            context: Execution context.
            params: Dict with keys: datetime_value, input_format, output_format.
        
        Returns:
            ActionResult with normalized datetime.
        """
        datetime_value = params.get('datetime_value', '')
        input_format = params.get('input_format', 'auto')
        output_format = params.get('output_format', 'iso8601')
        timezone = params.get('timezone', 'UTC')
        
        if not datetime_value:
            return ActionResult(
                success=False,
                data=None,
                error="No datetime value provided"
            )
        
        try:
            # Parse datetime
            parsed = self._parse_datetime(datetime_value, input_format)
            
            # Convert to output format
            normalized = self._format_datetime(parsed, output_format)
            
            return ActionResult(
                success=True,
                data={
                    'original': datetime_value,
                    'normalized': normalized,
                    'timestamp': parsed.timestamp(),
                    'output_format': output_format
                },
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Datetime normalization failed: {str(e)}"
            )
    
    def _parse_datetime(self, value: str, fmt: str):
        """Parse datetime string."""
        import datetime as dt
        
        if fmt == 'auto':
            # Try common formats
            formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%m/%d/%Y',
                '%Y/%m/%d'
            ]
            for f in formats:
                try:
                    return dt.datetime.strptime(value, f)
                except ValueError:
                    continue
            raise ValueError(f"Could not parse datetime: {value}")
        else:
            return dt.datetime.strptime(value, fmt)
    
    def _format_datetime(self, dt_obj, fmt: str) -> str:
        """Format datetime object."""
        if fmt == 'iso8601':
            return dt_obj.isoformat()
        elif fmt == 'unix':
            return str(int(dt_obj.timestamp()))
        elif fmt == 'date_only':
            return dt_obj.strftime('%Y-%m-%d')
        elif fmt == 'time_only':
            return dt_obj.strftime('%H:%M:%S')
        elif fmt == 'readable':
            return dt_obj.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return dt_obj.strftime(fmt)


class DataEncoderAction(BaseAction):
    """Encode data to various formats.
    
    Supports encoding categorical data, text to numbers, and custom encodings.
    """
    action_type = "data_encoder"
    display_name = "数据编码"
    description = "将数据编码为各种格式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data encoding.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, encoding_type, options.
        
        Returns:
            ActionResult with encoded data.
        """
        data = params.get('data', [])
        encoding_type = params.get('encoding_type', 'label')
        options = params.get('options', {})
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data to encode"
            )
        
        try:
            if encoding_type == 'label':
                encoded, mapping = self._label_encode(data, options)
            elif encoding_type == 'onehot':
                encoded, mapping = self._onehot_encode(data, options)
            elif encoding_type == 'ordinal':
                encoded, mapping = self._ordinal_encode(data, options)
            elif encoding_type == 'binary':
                encoded, mapping = self._binary_encode(data, options)
            elif encoding_type == 'frequency':
                encoded, mapping = self._frequency_encode(data, options)
            elif encoding_type == 'target':
                encoded, mapping = self._target_encode(data, options)
            else:
                return ActionResult(
                    success=False,
                    data=None,
                    error=f"Unknown encoding type: {encoding_type}"
                )
            
            return ActionResult(
                success=True,
                data={
                    'encoded': encoded,
                    'mapping': mapping,
                    'encoding_type': encoding_type
                },
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Encoding failed: {str(e)}"
            )
    
    def _label_encode(self, data: List, options: Dict) -> Tuple[List, Dict]:
        """Label encoding."""
        unique_values = list(set(data))
        mapping = {v: i for i, v in enumerate(unique_values)}
        encoded = [mapping[v] for v in data]
        return encoded, mapping
    
    def _onehot_encode(self, data: List, options: Dict) -> Tuple[List[List], Dict]:
        """One-hot encoding."""
        unique_values = list(set(data))
        mapping = {v: i for i, v in enumerate(unique_values)}
        
        encoded = []
        for v in data:
            row = [0] * len(unique_values)
            row[mapping[v]] = 1
            encoded.append(row)
        
        return encoded, mapping
    
    def _ordinal_encode(self, data: List, options: Dict) -> Tuple[List, Dict]:
        """Ordinal encoding with specified order."""
        order = options.get('order', sorted(set(data)))
        mapping = {v: i for i, v in enumerate(order)}
        encoded = [mapping.get(v, -1) for v in data]
        return encoded, mapping
    
    def _binary_encode(self, data: List, options: Dict) -> Tuple[List[str], Dict]:
        """Binary encoding."""
        unique_values = list(set(data))
        mapping = {v: i for i, v in enumerate(unique_values)}
        max_val = len(unique_values) - 1
        bits = max(1, (max_val).bit_length())
        
        encoded = [format(mapping[v], f'0{bits}b') for v in data]
        return encoded, mapping
    
    def _frequency_encode(self, data: List, options: Dict) -> Tuple[List, Dict]:
        """Frequency encoding."""
        from collections import Counter
        freq = Counter(data)
        total = len(data)
        mapping = {v: freq[v] / total for v in freq}
        encoded = [mapping[v] for v in data]
        return encoded, mapping
    
    def _target_encode(self, data: List, options: Dict) -> Tuple[List, Dict]:
        """Target encoding (requires target values)."""
        target = options.get('target', [])
        if len(data) != len(target):
            raise ValueError("Data and target must have same length")
        
        # Group by category and compute mean target
        groups = {}
        for d, t in zip(data, target):
            if d not in groups:
                groups[d] = []
            groups[d].append(t)
        
        mapping = {k: sum(v) / len(v) for k, v in groups.items()}
        global_mean = sum(target) / len(target)
        
        encoded = [mapping.get(v, global_mean) for v in data]
        return encoded, mapping


def register_actions():
    """Register all Data Normalizer actions."""
    return [
        DataNormalizerAction,
        StringNormalizerAction,
        DateTimeNormalizerAction,
        DataEncoderAction,
    ]
