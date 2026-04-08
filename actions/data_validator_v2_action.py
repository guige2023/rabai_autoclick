"""Data validator v2 action module for RabAI AutoClick.

Provides enhanced data validation with cross-field validation,
conditional rules, and validation result tracking.
"""

import time
import re
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CrossFieldValidatorAction(BaseAction):
    """Validate relationships between fields.
    
    Checks consistency and dependencies between
    multiple fields in the same record.
    """
    action_type = "cross_field_validator"
    display_name = "跨字段验证"
    description = "验证字段之间的关系"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate cross-field rules.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, rules (list of
                   {name, fields, predicate, error_message}).
        
        Returns:
            ActionResult with validation results.
        """
        data = params.get('data', [])
        rules = params.get('rules', [])
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        if not rules:
            return ActionResult(
                success=True,
                message="No cross-field rules defined",
                data={'validated': data, 'count': len(data)}
            )

        results = []
        total_errors = 0

        for row in data:
            errors = []
            for rule in rules:
                rule_name = rule.get('name', 'unnamed')
                fields = rule.get('fields', [])
                predicate = rule.get('predicate')
                error_msg = rule.get('error_message', f"Rule '{rule_name}' failed")

                field_values = {f: self._get_field(row, f) for f in fields}
                if predicate and not predicate(field_values):
                    errors.append({
                        'rule': rule_name,
                        'fields': fields,
                        'values': field_values,
                        'message': error_msg
                    })

            if errors:
                total_errors += len(errors)
                results.append({
                    'valid': False,
                    'errors': errors,
                    'data': row
                })
            else:
                results.append({'valid': True, 'data': row})

        valid_count = sum(1 for r in results if r['valid'])

        return ActionResult(
            success=total_errors == 0,
            message=f"Cross-field validation: {valid_count}/{len(results)} valid",
            data={
                'results': results,
                'valid_count': valid_count,
                'invalid_count': len(results) - valid_count,
                'total_errors': total_errors
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


class ConditionalValidatorAction(BaseAction):
    """Apply conditional validation rules.
    
    Only validates certain fields when conditions are met.
    """
    action_type = "conditional_validator"
    display_name = "条件验证"
    description = "应用条件验证规则"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Apply conditional validation.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, validation_rules (list of
                   {when, then (list of {field, rules})}).
        
        Returns:
            ActionResult with validation results.
        """
        data = params.get('data', [])
        validation_rules = params.get('validation_rules', [])
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        results = []
        total_errors = 0

        for row in data:
            all_errors = []

            for rule_group in validation_rules:
                condition = rule_group.get('when', {})
                then_rules = rule_group.get('then', [])

                if self._evaluate_when(condition, row):
                    for rule in then_rules:
                        field = rule.get('field', '')
                        rules = rule.get('rules', [])
                        for r in rules:
                            error = self._apply_rule(row, field, r)
                            if error:
                                all_errors.append(error)

            if all_errors:
                total_errors += len(all_errors)
                results.append({'valid': False, 'errors': all_errors, 'data': row})
            else:
                results.append({'valid': True, 'data': row})

        valid_count = sum(1 for r in results if r['valid'])

        return ActionResult(
            success=total_errors == 0,
            message=f"Conditional validation: {valid_count}/{len(results)} valid",
            data={
                'results': results,
                'valid_count': valid_count,
                'invalid_count': len(results) - valid_count
            },
            duration=time.time() - start_time
        )

    def _evaluate_when(self, condition: Dict, row: Dict) -> bool:
        """Evaluate when condition."""
        if not condition:
            return True
        operator = condition.get('operator', 'eq')
        field = condition.get('field', '')
        value = condition.get('value')
        actual = self._get_field(row, field)
        ops = {
            'eq': actual == value,
            'ne': actual != value,
            'gt': actual > value if actual is not None else False,
            'gte': actual >= value if actual is not None else False,
            'lt': actual < value if actual is not None else False,
            'lte': actual <= value if actual is not None else False,
            'is_null': actual is None,
            'is_not_null': actual is not None,
        }
        return ops.get(operator, True)

    def _apply_rule(self, row: Dict, field: str, rule: Dict) -> Optional[Dict]:
        """Apply a single validation rule."""
        rule_type = rule.get('type', '')
        value = self._get_field(row, field)
        error_msg = rule.get('message', f"Validation failed for {field}")

        if rule_type == 'required':
            if value is None or value == '':
                return {'field': field, 'rule': 'required', 'message': error_msg}
        elif rule_type == 'min_length':
            if value is not None and len(str(value)) < rule.get('min', 0):
                return {'field': field, 'rule': 'min_length', 'message': error_msg}
        elif rule_type == 'max_length':
            if value is not None and len(str(value)) > rule.get('max', float('inf')):
                return {'field': field, 'rule': 'max_length', 'message': error_msg}
        elif rule_type == 'pattern':
            if value and not re.match(rule.get('pattern', ''), str(value)):
                return {'field': field, 'rule': 'pattern', 'message': error_msg}
        elif rule_type == 'in':
            if value not in rule.get('values', []):
                return {'field': field, 'rule': 'in', 'message': error_msg}

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


class ValidationReporterAction(BaseAction):
    """Generate validation reports and summaries.
    
    Creates detailed reports of validation results
    with error categorization and suggestions.
    """
    action_type = "validation_reporter"
    display_name = "验证报告"
    description = "生成验证报告和汇总"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate validation report.
        
        Args:
            context: Execution context.
            params: Dict with keys: validation_results, format (summary|detailed),
                   group_by_field.
        
        Returns:
            ActionResult with formatted report.
        """
        validation_results = params.get('validation_results', [])
        report_format = params.get('format', 'summary')
        group_by = params.get('group_by_field', '')
        start_time = time.time()

        if not validation_results:
            return ActionResult(
                success=True,
                message="No validation results to report",
                data={'report': {}}
            )

        total = len(validation_results)
        valid = sum(1 for r in validation_results if r.get('valid', False))
        invalid = total - valid

        error_counts = {}
        error_by_field = {}

        for result in validation_results:
            if not result.get('valid', False):
                for error in result.get('errors', []):
                    rule = error.get('rule', 'unknown')
                    field = error.get('field', 'unknown')
                    error_counts[rule] = error_counts.get(rule, 0) + 1
                    if field not in error_by_field:
                        error_by_field[field] = 0
                    error_by_field[field] += 1

        report = {
            'total_records': total,
            'valid_records': valid,
            'invalid_records': invalid,
            'pass_rate': round(valid / total * 100, 2) if total > 0 else 0,
            'error_counts_by_rule': error_counts,
            'error_counts_by_field': error_by_field,
        }

        if report_format == 'detailed':
            report['invalid_records_detail'] = [
                r for r in validation_results if not r.get('valid', False)
            ]

        return ActionResult(
            success=True,
            message=f"Validation report: {valid}/{total} valid ({report['pass_rate']}%)",
            data={
                'report': report,
                'pass_rate': report['pass_rate'],
                'total_errors': sum(error_counts.values())
            },
            duration=time.time() - start_time
        )
