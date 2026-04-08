"""Data classifier action module for RabAI AutoClick.

Provides data classification with rule-based and pattern-based
classification, category management, and label assignment.
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


class DataClassifierAction(BaseAction):
    """Classify data records based on rules and patterns.
    
    Supports rule-based classification, pattern matching,
    category management, and multi-label classification.
    """
    action_type = "data_classifier"
    display_name = "数据分类"
    description = "数据分类，支持规则和模式匹配"
    
    def __init__(self):
        super().__init__()
        self._categories: Dict[str, Dict[str, Any]] = {}
        self._classifications: Dict[str, List[str]] = {}
        self._lock = threading.RLock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute classification operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (classify, add_category,
                   list_categories, classify_batch), config.
        
        Returns:
            ActionResult with classification results.
        """
        action = params.get('action', 'classify')
        
        if action == 'classify':
            return self._classify(params)
        elif action == 'classify_batch':
            return self._classify_batch(params)
        elif action == 'add_category':
            return self._add_category(params)
        elif action == 'list_categories':
            return self._list_categories(params)
        elif action == 'get_classification':
            return self._get_classification(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _classify(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Classify a single record."""
        record = params.get('record', {})
        key_field = params.get('key_field')
        
        if not record:
            return ActionResult(success=False, message="record is required")
        
        key = record.get(key_field, str(hash(str(record)))) if key_field else str(hash(str(record)))
        
        with self._lock:
            categories = self._classify_record(record)
            self._classifications[key] = categories
        
        return ActionResult(
            success=True,
            message=f"Classified into {len(categories)} categories",
            data={
                'categories': categories,
                'key': key
            }
        )
    
    def _classify_batch(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Classify multiple records."""
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        key_field = params.get('key_field')
        multi_label = params.get('multi_label', True)
        
        results = []
        category_counts = Counter()
        
        with self._lock:
            for idx, record in enumerate(records):
                key = record.get(key_field, idx) if key_field else idx
                categories = self._classify_record(record)
                
                if multi_label:
                    self._classifications[key] = categories
                else:
                    category = categories[0] if categories else 'uncategorized'
                    self._classifications[key] = [category]
                
                for cat in categories:
                    category_counts[cat] += 1
                
                results.append({
                    'key': key,
                    'categories': categories
                })
        
        return ActionResult(
            success=True,
            message=f"Classified {len(records)} records",
            data={
                'results': results,
                'count': len(results),
                'category_distribution': dict(category_counts)
            }
        )
    
    def _classify_record(
        self,
        record: Dict[str, Any]
    ) -> List[str]:
        """Classify a record based on rules."""
        categories = []
        
        for cat_name, cat_config in self._categories.items():
            rules = cat_config.get('rules', [])
            
            if self._check_rules(record, rules):
                categories.append(cat_name)
        
        return categories if categories else ['uncategorized']
    
    def _check_rules(
        self,
        record: Dict[str, Any],
        rules: List[Dict[str, Any]]
    ) -> bool:
        """Check if record matches rules."""
        if not rules:
            return False
        
        match_all = all(self._check_rule(record, rule) for rule in rules)
        return match_all
    
    def _check_rule(
        self,
        record: Dict[str, Any],
        rule: Dict[str, Any]
    ) -> bool:
        """Check if record matches a single rule."""
        field = rule.get('field')
        operator = rule.get('operator', 'eq')
        value = rule.get('value')
        values = rule.get('values', [])
        
        if not field:
            return True
        
        record_value = record.get(field)
        
        if operator == 'eq':
            return record_value == value
        elif operator == 'ne':
            return record_value != value
        elif operator == 'gt':
            return isinstance(record_value, (int, float)) and record_value > value
        elif operator == 'gte':
            return isinstance(record_value, (int, float)) and record_value >= value
        elif operator == 'lt':
            return isinstance(record_value, (int, float)) and record_value < value
        elif operator == 'lte':
            return isinstance(record_value, (int, float)) and record_value <= value
        elif operator == 'in':
            return record_value in values
        elif operator == 'not_in':
            return record_value not in values
        elif operator == 'contains':
            return isinstance(record_value, str) and value in record_value
        elif operator == 'starts_with':
            return isinstance(record_value, str) and record_value.startswith(str(value))
        elif operator == 'ends_with':
            return isinstance(record_value, str) and record_value.endswith(str(value))
        elif operator == 'regex':
            return isinstance(record_value, str) and bool(re.match(value, record_value))
        elif operator == 'exists':
            return (value and field in record) or (not value and field not in record)
        elif operator == 'is_null':
            return (value and record_value is None) or (not value and record_value is not None)
        elif operator == 'is_empty':
            is_empty = (record_value is None or record_value == '' or 
                       (isinstance(record_value, (list, dict)) and len(record_value) == 0))
            return is_empty
        
        return False
    
    def _add_category(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Add a classification category with rules."""
        name = params.get('name')
        if not name:
            return ActionResult(success=False, message="name is required")
        
        description = params.get('description', '')
        rules = params.get('rules', [])
        priority = params.get('priority', 0)
        
        with self._lock:
            self._categories[name] = {
                'name': name,
                'description': description,
                'rules': rules,
                'priority': priority,
                'created_at': __import__('time').time()
            }
        
        return ActionResult(
            success=True,
            message=f"Added category '{name}' with {len(rules)} rules",
            data={
                'category': name,
                'rule_count': len(rules)
            }
        )
    
    def _list_categories(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List all classification categories."""
        with self._lock:
            categories = []
            
            for name, config in self._categories.items():
                categories.append({
                    'name': name,
                    'description': config.get('description', ''),
                    'rule_count': len(config.get('rules', [])),
                    'priority': config.get('priority', 0)
                })
            
            categories.sort(key=lambda x: x['priority'], reverse=True)
        
        return ActionResult(
            success=True,
            message=f"Found {len(categories)} categories",
            data={'categories': categories, 'count': len(categories)}
        )
    
    def _get_classification(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get classification for a specific key."""
        key = params.get('key')
        
        if key is None:
            return ActionResult(success=False, message="key is required")
        
        with self._lock:
            categories = self._classifications.get(key, [])
        
        return ActionResult(
            success=True,
            message=f"Found {len(categories)} categories for key",
            data={
                'key': key,
                'categories': categories
            }
        )
