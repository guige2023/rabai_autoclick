"""Data Filter Action Module.

Provides advanced data filtering capabilities including
query-based filtering, fuzzy matching, and complex condition evaluation.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class QueryFilterAction(BaseAction):
    """Filter data using query expressions.
    
    Supports comparison operators, logical operators, and nested field queries.
    """
    action_type = "query_filter"
    display_name = "查询过滤"
    description = "使用查询表达式过滤数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Filter data using query expressions.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - query: Query expression dict.
                - conditions: List of condition dicts.
                - logic: 'and' or 'or' for combining conditions.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with filtered data or error.
        """
        data = params.get('data', [])
        query = params.get('query', {})
        conditions = params.get('conditions', [])
        logic = params.get('logic', 'and')
        output_var = params.get('output_var', 'filtered')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Build conditions list
            all_conditions = []
            if query:
                all_conditions.append(query)
            all_conditions.extend(conditions)

            # Filter data
            filtered = []
            for item in data:
                if self._evaluate_conditions(item, all_conditions, logic):
                    filtered.append(item)

            context.variables[output_var] = filtered
            return ActionResult(
                success=True,
                data={'filtered': filtered, 'count': len(filtered), 'original_count': len(data)},
                message=f"Filtered {len(data)} items to {len(filtered)} results"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Query filter failed: {str(e)}"
            )

    def _evaluate_conditions(
        self, item: Dict, conditions: List[Dict], logic: str
    ) -> bool:
        """Evaluate conditions against an item."""
        if not conditions:
            return True

        results = []
        for condition in conditions:
            result = self._evaluate_condition(item, condition)
            results.append(result)

        if logic == 'and':
            return all(results)
        else:
            return any(results)

    def _evaluate_condition(self, item: Dict, condition: Dict) -> bool:
        """Evaluate a single condition."""
        field = condition.get('field', '')
        operator = condition.get('op', 'eq')
        value = condition.get('value')
        nested = condition.get('nested', False)

        # Get field value
        if nested:
            field_value = self._get_nested_value(item, field)
        else:
            field_value = item.get(field)

        # Evaluate operator
        if operator == 'eq':
            return field_value == value
        elif operator == 'ne':
            return field_value != value
        elif operator == 'gt':
            return field_value is not None and field_value > value
        elif operator == 'gte':
            return field_value is not None and field_value >= value
        elif operator == 'lt':
            return field_value is not None and field_value < value
        elif operator == 'lte':
            return field_value is not None and field_value <= value
        elif operator == 'in':
            return field_value in value if value else False
        elif operator == 'nin':
            return field_value not in value if value else True
        elif operator == 'contains':
            return value in field_value if field_value else False
        elif operator == 'startswith':
            return str(field_value).startswith(str(value)) if field_value else False
        elif operator == 'endswith':
            return str(field_value).endswith(str(value)) if field_value else False
        elif operator == 'regex':
            try:
                return bool(re.search(str(value), str(field_value)))
            except re.error:
                return False
        elif operator == 'exists':
            return field_value is not None if value else field_value is None
        elif operator == 'type':
            return type(field_value).__name__ == value
        else:
            return False

    def _get_nested_value(self, data: Dict, path: str) -> Any:
        """Get nested value using dot notation."""
        parts = path.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    current = current[int(part)]
                except (ValueError, IndexError):
                    return None
            else:
                return None
            if current is None:
                return None
        return current


class FuzzyFilterAction(BaseAction):
    """Filter data using fuzzy matching.
    
    Supports string similarity, phonetic matching, and typo tolerance.
    """
    action_type = "fuzzy_filter"
    display_name = "模糊过滤"
    description = "使用模糊匹配过滤数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Filter data using fuzzy matching.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - field: Field to match against.
                - pattern: Search pattern.
                - threshold: Match threshold (0-1).
                - match_type: 'contains', 'similar', 'fuzzy'.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with fuzzy filtered data or error.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        pattern = params.get('pattern', '')
        threshold = params.get('threshold', 0.6)
        match_type = params.get('match_type', 'contains')
        output_var = params.get('output_var', 'fuzzy_filtered')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            filtered = []
            for item in data:
                if isinstance(item, dict):
                    field_value = str(item.get(field, ''))
                else:
                    field_value = str(item)

                score = self._calculate_match_score(field_value, pattern, match_type)

                if score >= threshold:
                    filtered.append({
                        'item': item,
                        'match_score': score
                    })

            # Sort by score
            filtered.sort(key=lambda x: x['match_score'], reverse=True)

            result = {
                'matches': [f['item'] for f in filtered],
                'scores': {str(i): f['match_score'] for i, f in enumerate(filtered)},
                'count': len(filtered)
            }

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Found {len(filtered)} fuzzy matches for '{pattern}'"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Fuzzy filter failed: {str(e)}"
            )

    def _calculate_match_score(
        self, text: str, pattern: str, match_type: str
    ) -> float:
        """Calculate match score between text and pattern."""
        if not pattern:
            return 1.0

        text_lower = text.lower()
        pattern_lower = pattern.lower()

        if match_type == 'contains':
            # Simple substring match
            if pattern_lower in text_lower:
                return 1.0
            return 0.0

        elif match_type == 'similar':
            # Levenshtein distance-based similarity
            distance = self._levenshtein_distance(text_lower, pattern_lower)
            max_len = max(len(text_lower), len(pattern_lower))
            return 1 - (distance / max_len) if max_len > 0 else 1.0

        elif match_type == 'fuzzy':
            # Combined fuzzy matching
            if pattern_lower in text_lower:
                return 1.0

            # Check word-level matching
            pattern_words = pattern_lower.split()
            text_words = text_lower.split()
            matched_words = sum(1 for pw in pattern_words if any(
                self._levenshtein_distance(pw, tw) <= 2 for tw in text_words
            ))

            if pattern_words:
                return matched_words / len(pattern_words)
            return 0.0

        return 0.0

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance between two strings."""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]


class DateRangeFilterAction(BaseAction):
    """Filter data by date ranges.
    
    Supports absolute dates, relative ranges, and date comparisons.
    """
    action_type = "date_range_filter"
    display_name = "日期范围过滤"
    description = "按日期范围过滤数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Filter data by date range.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - date_field: Field containing date values.
                - start_date: Start of range (ISO format or relative).
                - end_date: End of range (ISO format or relative).
                - inclusive: Include boundary dates.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with filtered data or error.
        """
        data = params.get('data', [])
        date_field = params.get('date_field', 'date')
        start_date = params.get('start_date', None)
        end_date = params.get('end_date', None)
        inclusive = params.get('inclusive', True)
        output_var = params.get('output_var', 'date_filtered')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Parse date boundaries
            start = self._parse_date(start_date) if start_date else None
            end = self._parse_date(end_date) if end_date else None

            # Filter data
            filtered = []
            for item in data:
                if not isinstance(item, dict):
                    continue

                date_value = item.get(date_field)
                if not date_value:
                    continue

                item_date = self._parse_date(date_value)
                if not item_date:
                    continue

                if self._is_in_range(item_date, start, end, inclusive):
                    filtered.append(item)

            result = {
                'filtered': filtered,
                'count': len(filtered),
                'start_date': start_date,
                'end_date': end_date
            }

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Filtered {len(data)} items to {len(filtered)} in date range"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Date range filter failed: {str(e)}"
            )

    def _parse_date(self, date_value: Any) -> Optional[datetime]:
        """Parse date from various formats."""
        if isinstance(date_value, datetime):
            return date_value

        if isinstance(date_value, (int, float)):
            # Unix timestamp
            return datetime.fromtimestamp(date_value)

        if isinstance(date_value, str):
            # Try ISO format first
            try:
                return datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            except ValueError:
                pass

            # Try common formats
            formats = [
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S',
                '%Y/%m/%d',
                '%d/%m/%Y',
                '%m/%d/%Y',
            ]
            for fmt in formats:
                try:
                    return datetime.strptime(date_value, fmt)
                except ValueError:
                    continue

        return None

    def _is_in_range(
        self, date: datetime, start: Optional[datetime], end: Optional[datetime], inclusive: bool
    ) -> bool:
        """Check if date is within range."""
        if start and end:
            if inclusive:
                return start <= date <= end
            else:
                return start < date < end
        elif start:
            return date >= start if inclusive else date > start
        elif end:
            return date <= end if inclusive else date < end
        return True


class CompositeFilterAction(BaseAction):
    """Apply multiple filters in sequence.
    
    Supports filter chaining and priority-based filtering.
    """
    action_type = "composite_filter"
    display_name = "组合过滤"
    description = "按顺序应用多个过滤器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Apply composite filters.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data list.
                - filters: List of filter definitions.
                - filter_type: Type of each filter.
                - stop_on_first: Stop at first match.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with filtered data or error.
        """
        data = params.get('data', [])
        filters = params.get('filters', [])
        filter_types = params.get('filter_types', {})
        stop_on_first = params.get('stop_on_first', False)
        output_var = params.get('output_var', 'composite_filtered')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            current_data = list(data)
            filter_log = []

            for i, filter_def in enumerate(filters):
                filter_type = filter_types.get(i, filter_def.get('type', 'query'))
                filter_name = filter_def.get('name', f'filter_{i}')

                filter_start = time.time()

                if filter_type == 'query':
                    filtered = self._apply_query_filter(current_data, filter_def)
                elif filter_type == 'fuzzy':
                    filtered = self._apply_fuzzy_filter(current_data, filter_def)
                elif filter_type == 'date_range':
                    filtered = self._apply_date_filter(current_data, filter_def)
                elif filter_type == 'field':
                    filtered = self._apply_field_filter(current_data, filter_def)
                else:
                    filtered = current_data

                filter_duration = time.time() - filter_start
                filter_log.append({
                    'name': filter_name,
                    'type': filter_type,
                    'input_count': len(current_data),
                    'output_count': len(filtered),
                    'duration': filter_duration
                })

                current_data = filtered

                if stop_on_first and len(current_data) > 0:
                    break

            result = {
                'filtered': current_data,
                'count': len(current_data),
                'original_count': len(data),
                'filter_log': filter_log
            }

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Applied {len(filters)} filters: {len(data)} -> {len(current_data)} items"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Composite filter failed: {str(e)}"
            )

    def _apply_query_filter(self, data: List, filter_def: Dict) -> List:
        """Apply query filter."""
        query_action = QueryFilterAction()
        result = query_action.execute(None, {
            'data': data,
            'query': filter_def.get('query'),
            'conditions': filter_def.get('conditions', []),
            'logic': filter_def.get('logic', 'and')
        })
        return result.data.get('filtered', data) if result.success else data

    def _apply_fuzzy_filter(self, data: List, filter_def: Dict) -> List:
        """Apply fuzzy filter."""
        fuzzy_action = FuzzyFilterAction()
        result = fuzzy_action.execute(None, {
            'data': data,
            'field': filter_def.get('field', ''),
            'pattern': filter_def.get('pattern', ''),
            'threshold': filter_def.get('threshold', 0.6)
        })
        return result.data.get('matches', data)[:filter_def.get('max_results', len(data))] if result.success else data

    def _apply_date_filter(self, data: List, filter_def: Dict) -> List:
        """Apply date range filter."""
        date_action = DateRangeFilterAction()
        result = date_action.execute(None, {
            'data': data,
            'date_field': filter_def.get('date_field', 'date'),
            'start_date': filter_def.get('start_date'),
            'end_date': filter_def.get('end_date')
        })
        return result.data.get('filtered', data) if result.success else data

    def _apply_field_filter(self, data: List, filter_def: Dict) -> List:
        """Apply field-based filter."""
        field = filter_def.get('field', '')
        values = filter_def.get('values', [])
        exclude = filter_def.get('exclude', False)

        filtered = [item for item in data if (item.get(field) in values) != exclude]
        return filtered


import time
