"""Data searcher action module for RabAI AutoClick.

Provides data search with full-text search,
filtering, sorting, and relevance scoring.
"""

import re
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from collections import Counter
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSearcherAction(BaseAction):
    """Search data with filtering and relevance scoring.
    
    Supports full-text search, field-based filtering,
    fuzzy matching, and result ranking.
    """
    action_type = "data_searcher"
    display_name = "数据搜索"
    description = "数据搜索和过滤"
    
    def __init__(self):
        super().__init__()
        self._index: Dict[str, List[Dict[str, Any]]] = {}
        self._lock = threading.RLock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute search operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (search, index, clear), config.
        
        Returns:
            ActionResult with search results.
        """
        action = params.get('action', 'search')
        
        if action == 'search':
            return self._search(params)
        elif action == 'index':
            return self._build_index(params)
        elif action == 'clear':
            return self._clear_index(params)
        elif action == 'search_one':
            return self._search_one(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _search(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Search for matching records."""
        query = params.get('query', '')
        fields = params.get('fields', [])
        filters = params.get('filters', {})
        sort_by = params.get('sort_by')
        sort_order = params.get('sort_order', 'desc')
        limit = params.get('limit', 10)
        fuzzy = params.get('fuzzy', False)
        fuzzy_threshold = params.get('fuzzy_threshold', 0.8)
        
        with self._lock:
            if not self._index:
                return ActionResult(
                    success=False,
                    message="No index available. Call 'index' action first."
                )
            
            all_records = []
            for records in self._index.values():
                all_records.extend(records)
            
            if not all_records:
                return ActionResult(
                    success=False,
                    message="No records in index"
                )
            
            results = []
            
            for record in all_records:
                score, matched_fields = self._calculate_score(
                    record, query, fields, fuzzy, fuzzy_threshold
                )
                
                if score > 0:
                    if self._apply_filters(record, filters):
                        results.append({
                            'record': record,
                            'score': score,
                            'matched_fields': matched_fields
                        })
            
            if sort_by:
                reverse = sort_order == 'desc'
                results.sort(key=lambda x: x['record'].get(sort_by, 0), reverse=reverse)
            
            results = results[:limit]
            
            return ActionResult(
                success=True,
                message=f"Found {len(results)} results for '{query}'",
                data={
                    'results': results,
                    'count': len(results),
                    'query': query
                }
            )
    
    def _calculate_score(
        self,
        record: Dict[str, Any],
        query: str,
        fields: List[str],
        fuzzy: bool,
        threshold: float
    ) -> tuple:
        """Calculate relevance score for a record."""
        if not query:
            return 1.0, []
        
        query_lower = query.lower()
        query_words = query_lower.split()
        score = 0.0
        matched_fields = []
        
        if not fields:
            fields = list(record.keys())
        
        for field in fields:
            value = record.get(field)
            if value is None:
                continue
            
            value_str = str(value).lower()
            
            if query_lower in value_str:
                score += 10.0
                matched_fields.append(field)
            else:
                for word in query_words:
                    if word in value_str:
                        score += 5.0
                        if field not in matched_fields:
                            matched_fields.append(field)
            
            if fuzzy:
                similarity = self._fuzzy_similarity(query_lower, value_str)
                if similarity >= threshold:
                    score += similarity * 3.0
        
        return score, matched_fields
    
    def _fuzzy_similarity(self, s1: str, s2: str) -> float:
        """Calculate fuzzy similarity between two strings."""
        if s1 == s2:
            return 1.0
        
        longer = s1 if len(s1) > len(s2) else s2
        shorter = s2 if len(s1) > len(s2) else s1
        
        if len(longer) == 0:
            return 1.0
        
        matches = sum(1 for c in shorter if c in longer)
        return matches / len(longer)
    
    def _apply_filters(
        self,
        record: Dict[str, Any],
        filters: Dict[str, Any]
    ) -> bool:
        """Apply filters to a record."""
        for field, filter_config in filters.items():
            if isinstance(filter_config, dict):
                operator = filter_config.get('operator', 'eq')
                value = filter_config.get('value')
                
                record_value = record.get(field)
                
                if operator == 'eq':
                    if record_value != value:
                        return False
                elif operator == 'ne':
                    if record_value == value:
                        return False
                elif operator == 'gt':
                    if not (isinstance(record_value, (int, float)) and record_value > value):
                        return False
                elif operator == 'gte':
                    if not (isinstance(record_value, (int, float)) and record_value >= value):
                        return False
                elif operator == 'lt':
                    if not (isinstance(record_value, (int, float)) and record_value < value):
                        return False
                elif operator == 'lte':
                    if not (isinstance(record_value, (int, float)) and record_value <= value):
                        return False
                elif operator == 'in':
                    if record_value not in value:
                        return False
                elif operator == 'contains':
                    if value not in str(record_value):
                        return False
                elif operator == 'starts_with':
                    if not str(record_value).startswith(str(value)):
                        return False
                elif operator == 'ends_with':
                    if not str(record_value).endswith(str(value)):
                        return False
                elif operator == 'regex':
                    if not re.match(value, str(record_value)):
                        return False
                elif operator == 'exists':
                    if value and field not in record:
                        return False
            else:
                if record.get(field) != filter_config:
                    return False
        
        return True
    
    def _build_index(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Build search index from records."""
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records to index")
        
        index_key = params.get('index_key', 'default')
        
        with self._lock:
            self._index[index_key] = records.copy()
        
        return ActionResult(
            success=True,
            message=f"Indexed {len(records)} records",
            data={
                'index_key': index_key,
                'count': len(records)
            }
        )
    
    def _clear_index(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear the search index."""
        index_key = params.get('index_key')
        
        with self._lock:
            if index_key:
                if index_key in self._index:
                    count = len(self._index[index_key])
                    del self._index[index_key]
                    return ActionResult(
                        success=True,
                        message=f"Cleared index '{index_key}'",
                        data={'count': count}
                    )
                else:
                    return ActionResult(
                        success=False,
                        message=f"Index '{index_key}' not found"
                    )
            else:
                self._index.clear()
        
        return ActionResult(
            success=True,
            message="Cleared all indexes"
        )
    
    def _search_one(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Search for a single matching record."""
        query = params.get('query', '')
        fields = params.get('fields', [])
        
        params['limit'] = 1
        result = self._search(params)
        
        if result.data and result.data.get('results'):
            return ActionResult(
                success=True,
                message=f"Found match for '{query}'",
                data={'record': result.data['results'][0]['record']}
            )
        else:
            return ActionResult(
                success=False,
                message=f"No match found for '{query}'"
            )
