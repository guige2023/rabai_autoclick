"""Data profiler action module for RabAI AutoClick.

Provides data profiling with statistics computation,
schema inference, and quality assessment.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from collections import Counter
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataProfilerAction(BaseAction):
    """Profile data and compute statistics.
    
    Supports column profiling, type inference,
    pattern detection, and quality scoring.
    """
    action_type = "data_profiler"
    display_name = "数据画像"
    description = "数据统计分析，质量评分和类型推断"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data profiling operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: records, profile_type, compute_stats.
        
        Returns:
            ActionResult with profiling results.
        """
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        if isinstance(records, dict):
            records = [records]
        
        profile_type = params.get('profile_type', 'full')
        compute_stats = params.get('compute_stats', True)
        
        if profile_type == 'full':
            return self._full_profile(records, compute_stats)
        elif profile_type == 'column':
            return self._column_profile(records, params.get('column'))
        elif profile_type == 'schema':
            return self._schema_inference(records)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown profile_type: {profile_type}"
            )
    
    def _full_profile(
        self,
        records: List[Dict[str, Any]],
        compute_stats: bool
    ) -> ActionResult:
        """Generate full data profile."""
        if not records:
            return ActionResult(success=False, message="No records to profile")
        
        columns = {}
        
        for record in records:
            for key, value in record.items():
                if key not in columns:
                    columns[key] = []
                columns[key].append(value)
        
        profile = {
            'record_count': len(records),
            'column_count': len(columns),
            'columns': {}
        }
        
        for col_name, values in columns.items():
            col_profile = self._profile_column(col_name, values, compute_stats)
            profile['columns'][col_name] = col_profile
        
        quality_score = self._compute_quality_score(profile)
        profile['quality_score'] = quality_score
        
        return ActionResult(
            success=True,
            message=f"Profiled {len(records)} records across {len(columns)} columns",
            data=profile
        )
    
    def _profile_column(
        self,
        col_name: str,
        values: List[Any],
        compute_stats: bool
    ) -> Dict[str, Any]:
        """Profile a single column."""
        non_null = [v for v in values if v is not None and v != '']
        null_count = len(values) - len(non_null)
        
        profile = {
            'name': col_name,
            'total_count': len(values),
            'null_count': null_count,
            'null_percentage': (null_count / len(values) * 100) if values else 0,
            'unique_count': len(set(non_null)) if non_null else 0,
            'inferred_type': self._infer_type(non_null)
        }
        
        if non_null:
            types = [type(v).__name__ for v in non_null]
            type_counts = Counter(types)
            profile['type_distribution'] = dict(type_counts.most_common())
        
        if compute_stats and non_null:
            if all(isinstance(v, (int, float)) for v in non_null):
                numeric_values = [float(v) for v in non_null if v is not None]
                if numeric_values:
                    profile['min'] = min(numeric_values)
                    profile['max'] = max(numeric_values)
                    profile['mean'] = sum(numeric_values) / len(numeric_values)
                    profile['sum'] = sum(numeric_values)
            
            if all(isinstance(v, str) for v in non_null):
                lengths = [len(str(v)) for v in non_null]
                profile['min_length'] = min(lengths)
                profile['max_length'] = max(lengths)
                profile['avg_length'] = sum(lengths) / len(lengths)
                
                patterns = self._detect_patterns(non_null)
                if patterns:
                    profile['patterns'] = patterns
        
        return profile
    
    def _infer_type(self, values: List[Any]) -> str:
        """Infer the type of a column."""
        if not values:
            return 'unknown'
        
        type_counts = Counter()
        
        for v in values:
            if v is None or v == '':
                type_counts['null'] += 1
            elif isinstance(v, bool):
                type_counts['boolean'] += 1
            elif isinstance(v, int):
                type_counts['integer'] += 1
            elif isinstance(v, float):
                type_counts['float'] += 1
            elif isinstance(v, str):
                if self._looks_like_email(v):
                    type_counts['email'] += 1
                elif self._looks_like_url(v):
                    type_counts['url'] += 1
                elif self._looks_like_date(v):
                    type_counts['date'] += 1
                elif self._looks_like_phone(v):
                    type_counts['phone'] += 1
                else:
                    type_counts['string'] += 1
            else:
                type_counts[type(v).__name__] += 1
        
        if not type_counts:
            return 'unknown'
        
        most_common = type_counts.most_common(1)[0][0]
        
        if type_counts['null'] > len(values) * 0.5:
            return 'mostly_null'
        
        return most_common
    
    def _looks_like_email(self, value: str) -> bool:
        import re
        return bool(re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', str(value)))
    
    def _looks_like_url(self, value: str) -> bool:
        import re
        return bool(re.match(r'^https?://[\w\.-]+', str(value)))
    
    def _looks_like_date(self, value: str) -> bool:
        import re
        return bool(re.match(r'\d{4}[-/]\d{2}[-/]\d{2}', str(value)))
    
    def _looks_like_phone(self, value: str) -> bool:
        import re
        return bool(re.match(r'^[\d\s\-\+\(\)]{7,}$', str(value)))
    
    def _detect_patterns(self, values: List[str]) -> Dict[str, int]:
        """Detect common patterns in string values."""
        import re
        
        patterns = {
            'has_uppercase': 0,
            'has_lowercase': 0,
            'has_digits': 0,
            'has_special': 0,
            'is_alphanumeric': 0,
            'is_hex': 0
        }
        
        for v in values:
            s = str(v)
            if any(c.isupper() for c in s):
                patterns['has_uppercase'] += 1
            if any(c.islower() for c in s):
                patterns['has_lowercase'] += 1
            if any(c.isdigit() for c in s):
                patterns['has_digits'] += 1
            if any(not c.isalnum() for c in s):
                patterns['has_special'] += 1
            if s.isalnum():
                patterns['is_alphanumeric'] += 1
            if re.match(r'^[0-9a-fA-F]+$', s):
                patterns['is_hex'] += 1
        
        return {k: v for k, v in patterns.items() if v > 0}
    
    def _schema_inference(
        self,
        records: List[Dict[str, Any]]
    ) -> ActionResult:
        """Infer schema from records."""
        schema = {
            'fields': {},
            'required_fields': [],
            'optional_fields': []
        }
        
        all_keys = set()
        for record in records:
            all_keys.update(record.keys())
        
        for key in all_keys:
            values = [r.get(key) for r in records if key in r]
            non_null = [v for v in values if v is not None and v != '']
            
            field_schema = {
                'name': key,
                'type': self._infer_type(non_null),
                'nullable': len(non_null) < len(values),
                'unique': len(set(non_null)) == len(non_null) if non_null else False
            }
            
            schema['fields'][key] = field_schema
            
            if not field_schema['nullable']:
                schema['required_fields'].append(key)
            else:
                schema['optional_fields'].append(key)
        
        return ActionResult(
            success=True,
            message=f"Inferred schema with {len(schema['fields'])} fields",
            data=schema
        )
    
    def _compute_quality_score(self, profile: Dict[str, Any]) -> float:
        """Compute overall data quality score."""
        score = 100.0
        
        for col_name, col_profile in profile.get('columns', {}).items():
            null_pct = col_profile.get('null_percentage', 0)
            score -= null_pct * 0.5
        
        return max(0.0, min(100.0, score))
    
    def _column_profile(
        self,
        records: List[Dict[str, Any]],
        column: Optional[str]
    ) -> ActionResult:
        """Profile a specific column."""
        if not column:
            return ActionResult(success=False, message="column is required")
        
        values = [r.get(column) for r in records if column in r]
        
        if not values:
            return ActionResult(
                success=False,
                message=f"Column '{column}' not found"
            )
        
        profile = self._profile_column(column, values, True)
        
        return ActionResult(
            success=True,
            message=f"Profiled column '{column}'",
            data=profile
        )
