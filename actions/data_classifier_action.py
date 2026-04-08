"""Data classifier action module for RabAI AutoClick.

Provides classification actions for categorizing data
based on rules, patterns, and ML models.
"""

import re
import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RuleBasedClassifierAction(BaseAction):
    """Classify data using rule-based logic.
    
    Applies classification rules to categorize data.
    """
    action_type = "rule_classifier"
    display_name = "规则分类"
    description = "基于规则的数据分类"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Classify using rules.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, rules, default_category.
                   rules: list of {condition, category, priority}.
        
        Returns:
            ActionResult with classification results.
        """
        data = params.get('data', {})
        rules = params.get('rules', [])
        default_category = params.get('default_category', 'uncategorized')

        if not rules:
            return ActionResult(success=False, message="rules are required")

        rules_sorted = sorted(rules, key=lambda r: r.get('priority', 0), reverse=True)

        if isinstance(data, dict):
            category = self._classify_record(data, rules_sorted, default_category)
            return ActionResult(
                success=True,
                message=f"Classified as: {category}",
                data={'category': category, 'data': data}
            )

        elif isinstance(data, list):
            results = []
            for record in data:
                if isinstance(record, dict):
                    cat = self._classify_record(record, rules_sorted, default_category)
                    results.append({'record': record, 'category': cat})
            
            categories = [r['category'] for r in results]
            return ActionResult(
                success=True,
                message=f"Classified {len(results)} records",
                data={'results': results, 'categories': categories}
            )

        return ActionResult(success=False, message="data must be dict or list")

    def _classify_record(self, record: Dict, rules: List, default: str) -> str:
        """Classify a single record."""
        for rule in rules:
            condition = rule.get('condition', '')
            category = rule.get('category', default)
            
            if self._evaluate_condition(condition, record):
                return category
        
        return default

    def _evaluate_condition(self, condition: str, record: Dict) -> bool:
        """Evaluate condition against record."""
        if not condition:
            return True
        
        try:
            if condition.startswith('$'):
                return bool(record.get(condition[1:]))
            
            match = re.match(r'(\w+)\s*(==|!=|>| |<|>=|<=|contains|startswith)\s*(.+)', condition)
            if match:
                field, op, value = match.groups()
                field_value = record.get(field, '')
                value = value.strip('"\'')
                
                if op == '==':
                    return str(field_value) == value
                elif op == '!=':
                    return str(field_value) != value
                elif op == '>':
                    return float(field_value) > float(value)
                elif op == '<':
                    return float(field_value) < float(value)
                elif op == '>=':
                    return float(field_value) >= float(value)
                elif op == '<=':
                    return float(field_value) <= float(value)
                elif op == 'contains':
                    return value in str(field_value)
                elif op == 'startswith':
                    return str(field_value).startswith(value)
            
            return False
        except:
            return False


class KeywordClassifierAction(BaseAction):
    """Classify text using keyword matching.
    
    Categorizes data based on keyword presence.
    """
    action_type = "keyword_classifier"
    display_name = "关键词分类"
    description = "基于关键词的数据分类"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Classify using keywords.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, categories, match_mode,
                   case_sensitive.
        
        Returns:
            ActionResult with classification result.
        """
        text = params.get('text', '')
        categories = params.get('categories', {})
        match_mode = params.get('match_mode', 'first')
        case_sensitive = params.get('case_sensitive', False)

        if not text:
            return ActionResult(success=False, message="text is required")

        if not categories:
            return ActionResult(success=False, message="categories are required")

        search_text = text if case_sensitive else text.lower()
        
        matches = {}
        for category, keywords in categories.items():
            if not isinstance(keywords, list):
                keywords = [keywords]
            
            count = 0
            for keyword in keywords:
                search_keyword = keyword if case_sensitive else keyword.lower()
                if search_keyword in search_text:
                    count += 1
            
            if count > 0:
                matches[category] = count

        if not matches:
            return ActionResult(
                success=True,
                message="No category matched",
                data={'category': None, 'text': text}
            )

        if match_mode == 'first':
            matched_category = list(matches.keys())[0]
        elif match_mode == 'most_keywords':
            matched_category = max(matches, key=matches.get)
        else:
            matched_category = list(matches.keys())[0]

        return ActionResult(
            success=True,
            message=f"Classified as: {matched_category}",
            data={
                'category': matched_category,
                'matches': matches,
                'text_preview': text[:100]
            }
        )

    def execute_batch(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Classify multiple texts.
        
        Args:
            context: Execution context.
            params: Dict with keys: texts, categories, match_mode,
                   case_sensitive.
        
        Returns:
            ActionResult with batch classification.
        """
        texts = params.get('texts', [])
        categories = params.get('categories', {})
        match_mode = params.get('match_mode', 'first')
        case_sensitive = params.get('case_sensitive', False)

        results = []
        for text in texts:
            result = self.execute(context, {
                'text': text,
                'categories': categories,
                'match_mode': match_mode,
                'case_sensitive': case_sensitive
            })
            results.append(result.data)

        return ActionResult(
            success=True,
            message=f"Classified {len(results)} texts",
            data={'results': results, 'count': len(results)}
        )


class PatternClassifierAction(BaseAction):
    """Classify data using regex patterns.
    
    Categorizes based on pattern matching.
    """
    action_type = "pattern_classifier"
    display_name = "模式分类"
    description = "基于正则模式的数据分类"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Classify using patterns.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, patterns, default_category.
                   patterns: dict of {category: [pattern_list]}.
        
        Returns:
            ActionResult with classification.
        """
        text = params.get('text', '')
        patterns = params.get('patterns', {})
        default_category = params.get('default_category', 'uncategorized')

        if not text:
            return ActionResult(success=False, message="text is required")

        if not patterns:
            return ActionResult(success=False, message="patterns are required")

        for category, pattern_list in patterns.items():
            if not isinstance(pattern_list, list):
                pattern_list = [pattern_list]
            
            for pattern in pattern_list:
                if re.search(pattern, text):
                    return ActionResult(
                        success=True,
                        message=f"Matched pattern for: {category}",
                        data={
                            'category': category,
                            'matched_pattern': pattern,
                            'text': text[:100]
                        }
                    )

        return ActionResult(
            success=True,
            message=f"No pattern matched, default: {default_category}",
            data={'category': default_category, 'text': text[:100]}
        )


class MultiLabelClassifierAction(BaseAction):
    """Multi-label classification for data.
    
    Assigns multiple labels to data points.
    """
    action_type = "multilabel_classifier"
    display_name = "多标签分类"
    description = "多标签数据分类"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Multi-label classify.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, classifiers, threshold.
                   classifiers: list of {name, condition}.
        
        Returns:
            ActionResult with all matched labels.
        """
        data = params.get('data', {})
        classifiers = params.get('classifiers', [])
        threshold = params.get('threshold', 1)

        if not data:
            return ActionResult(success=False, message="data is required")
        if not classifiers:
            return ActionResult(success=False, message="classifiers are required")

        matched_labels = []
        scores = {}

        for clf in classifiers:
            name = clf.get('name', 'unnamed')
            condition = clf.get('condition', '')
            
            if self._evaluate_condition(condition, data):
                matched_labels.append(name)
                scores[name] = 1.0
            else:
                scores[name] = 0.0

        if len(matched_labels) < threshold:
            return ActionResult(
                success=True,
                message=f"Only {len(matched_labels)} labels matched, threshold: {threshold}",
                data={'labels': [], 'scores': scores, 'data': data}
            )

        return ActionResult(
            success=True,
            message=f"Assigned {len(matched_labels)} labels",
            data={
                'labels': matched_labels,
                'scores': scores,
                'data': data
            }
        )

    def _evaluate_condition(self, condition: str, record: Dict) -> bool:
        """Evaluate condition."""
        if not condition:
            return True
        
        try:
            match = re.match(r'(\w+)\s*(==|!=|>| |<|>=|<=|contains)\s*(.+)', condition)
            if match:
                field, op, value = match.groups()
                field_value = record.get(field, '')
                value = value.strip('"\'')
                
                if op == '==':
                    return str(field_value) == value
                elif op == '!=':
                    return str(field_value) != value
                elif op == 'contains':
                    return value in str(field_value)
            return False
        except:
            return False
