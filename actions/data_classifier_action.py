"""
Data Classifier Action Module.

Classifies data records using rule-based classification,
keyword matching, and pattern recognition.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class ClassificationResult:
    """Result of classification."""
    records: list[dict[str, Any]]
    classification_field: str
    class_counts: dict[str, int]
    unclassified_count: int


class DataClassifierAction(BaseAction):
    """Classify data records based on rules."""

    def __init__(self) -> None:
        super().__init__("data_classifier")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Classify records.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - rules: List of classification rules
                    - class_name: Target class
                    - conditions: List of {field, operator, value} conditions
                    - priority: Rule priority (higher = first)
                - output_field: Field name for class output
                - default_class: Default class for unmatched records

        Returns:
            ClassificationResult with classified records
        """
        records = params.get("records", [])
        rules = params.get("rules", [])
        output_field = params.get("output_field", "class")
        default_class = params.get("default_class", "unknown")

        rules = sorted(rules, key=lambda r: r.get("priority", 0), reverse=True)
        class_counts: dict[str, int] = {}
        unclassified = 0

        for r in records:
            if not isinstance(r, dict):
                continue

            classified = False
            for rule in rules:
                class_name = rule.get("class_name", "")
                conditions = rule.get("conditions", [])

                if self._evaluate_conditions(r, conditions):
                    r[output_field] = class_name
                    class_counts[class_name] = class_counts.get(class_name, 0) + 1
                    classified = True
                    break

            if not classified:
                r[output_field] = default_class
                unclassified += 1

        return ClassificationResult(
            records=records,
            classification_field=output_field,
            class_counts=class_counts,
            unclassified_count=unclassified
        ).__dict__

    def _evaluate_conditions(self, record: dict, conditions: list[dict]) -> bool:
        """Evaluate all conditions for a record."""
        import re

        for cond in conditions:
            field = cond.get("field", "")
            operator = cond.get("operator", "eq")
            value = cond.get("value")
            record_value = record.get(field)

            if operator == "eq" and record_value != value:
                return False
            if operator == "ne" and record_value == value:
                return False
            if operator == "gt" and not (isinstance(record_value, (int, float)) and record_value > value):
                return False
            if operator == "gte" and not (isinstance(record_value, (int, float)) and record_value >= value):
                return False
            if operator == "lt" and not (isinstance(record_value, (int, float)) and record_value < value):
                return False
            if operator == "lte" and not (isinstance(record_value, (int, float)) and record_value <= value):
                return False
            if operator == "contains" and not (isinstance(record_value, str) and value in record_value):
                return False
            if operator == "startswith" and not (isinstance(record_value, str) and record_value.startswith(str(value))):
                return False
            if operator == "endswith" and not (isinstance(record_value, str) and record_value.endswith(str(value))):
                return False
            if operator == "regex":
                try:
                    if not re.search(str(value), str(record_value)):
                        return False
                except Exception:
                    return False
            if operator == "in" and record_value not in (value if isinstance(value, list) else [value]):
                return False

        return True
