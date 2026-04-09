"""API Contract Testing Action Module.

Provides contract testing capabilities for validating API
compatibility between producers and consumers.
"""

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class ContractStatus(Enum):
    """Contract validation status."""
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    PENDING = "pending"


@dataclass
class ContractRule:
    """A single contract rule for validation."""
    field_path: str
    rule_type: str  # type, required, range, pattern, enum
    expected: Any = None
    description: str = ""


@dataclass
class ContractSpec:
    """API contract specification."""
    name: str
    version: str = "1.0"
    endpoint: str = ""
    method: str = "GET"
    rules: List[ContractRule] = field(default_factory=list)
    request_schema: Dict[str, Any] = field(default_factory=dict)
    response_schema: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of a single field validation."""
    field_path: str
    passed: bool
    actual_value: Any = None
    expected_value: Any = None
    error_message: str = ""


class APIContractTestAction(BaseAction):
    """API contract testing action.

    Validates API requests and responses against defined
    contract specifications with detailed error reporting.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation type (validate, create_contract, test, report)
            - contract: Contract specification dict
            - actual_data: Actual request/response to validate
            - data_type: 'request' or 'response'
            - contract_name: Name of contract to test
    """
    action_type = "api_contract_test"
    display_name = "API契约测试"
    description = "API契约验证与兼容性检测"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "contracts": [],
            "contract": None,
            "actual_data": None,
            "data_type": "response",
            "contract_name": None,
            "strict_mode": False,
        }

    def __init__(self) -> None:
        super().__init__()
        self._contracts: Dict[str, ContractSpec] = {}
        self._validation_history: List[Dict] = []

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contract testing operation."""
        start_time = time.time()

        operation = params.get("operation", "validate")
        contract = params.get("contract")
        actual_data = params.get("actual_data")
        data_type = params.get("data_type", "response")
        contract_name = params.get("contract_name")
        strict_mode = params.get("strict_mode", False)

        if operation == "create_contract":
            return self._create_contract(contract, start_time)
        elif operation == "validate":
            return self._validate_against_contract(
                contract, actual_data, data_type, strict_mode, start_time
            )
        elif operation == "test":
            return self._test_contract(
                contract_name, params.get("test_data"), start_time
            )
        elif operation == "report":
            return self._get_test_report(start_time)
        elif operation == "add_rule":
            return self._add_contract_rule(
                contract_name, params.get("rule"), start_time
            )
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _create_contract(self, contract: Optional[Dict], start_time: float) -> ActionResult:
        """Create a new contract specification."""
        if not contract:
            return ActionResult(success=False, message="Contract specification required", duration=time.time() - start_time)

        name = contract.get("name", "unnamed")
        version = contract.get("version", "1.0")
        rules = []
        for rule_data in contract.get("rules", []):
            rules.append(ContractRule(**rule_data))

        spec = ContractSpec(
            name=name,
            version=version,
            endpoint=contract.get("endpoint", ""),
            method=contract.get("method", "GET"),
            rules=rules,
            request_schema=contract.get("request_schema", {}),
            response_schema=contract.get("response_schema", {}),
        )

        key = f"{name}:{version}"
        self._contracts[key] = spec

        return ActionResult(
            success=True,
            message=f"Contract '{name}' v{version} created with {len(rules)} rules",
            data={"contract_name": name, "version": version, "rules_count": len(rules)},
            duration=time.time() - start_time
        )

    def _validate_against_contract(
        self,
        contract: Optional[Dict],
        actual_data: Any,
        data_type: str,
        strict_mode: bool,
        start_time: float
    ) -> ActionResult:
        """Validate data against a contract."""
        if not contract:
            return ActionResult(success=False, message="Contract required", duration=time.time() - start_time)

        rules = []
        for rule_data in contract.get("rules", []):
            rules.append(ContractRule(**rule_data))

        # Build field path to value map
        flat_data = self._flatten_dict(actual_data) if isinstance(actual_data, dict) else {}

        validation_results: List[ValidationResult] = []
        failed_count = 0

        for rule in rules:
            result = self._validate_rule(rule, flat_data, actual_data)
            validation_results.append(result)
            if not result.passed:
                failed_count += 1

        passed = failed_count == 0

        return ActionResult(
            success=passed,
            message=f"Contract validation {'PASSED' if passed else 'FAILED'}: {len(validation_results) - failed_count}/{len(validation_results)} rules passed",
            data={
                "passed": passed,
                "total_rules": len(validation_results),
                "passed_rules": len(validation_results) - failed_count,
                "failed_rules": failed_count,
                "strict_mode": strict_mode,
                "validations": [
                    {
                        "field": vr.field_path,
                        "rule_type": r.rule_type,
                        "passed": vr.passed,
                        "error": vr.error_message,
                    }
                    for vr, r in zip(validation_results, rules)
                ]
            },
            duration=time.time() - start_time
        )

    def _validate_rule(
        self,
        rule: ContractRule,
        flat_data: Dict[str, Any],
        original_data: Any
    ) -> ValidationResult:
        """Validate a single rule."""
        actual_value = flat_data.get(rule.field_path)
        passed = True
        error_msg = ""

        if rule.rule_type == "required":
            if actual_value is None:
                passed = False
                error_msg = f"Required field '{rule.field_path}' is missing"
        elif rule.rule_type == "type":
            expected_type = rule.expected
            if expected_type == "string" and not isinstance(actual_value, str):
                passed = False
                error_msg = f"Field '{rule.field_path}' must be string, got {type(actual_value).__name__}"
            elif expected_type == "number" and not isinstance(actual_value, (int, float)):
                passed = False
                error_msg = f"Field '{rule.field_path}' must be number, got {type(actual_value).__name__}"
            elif expected_type == "boolean" and not isinstance(actual_value, bool):
                passed = False
                error_msg = f"Field '{rule.field_path}' must be boolean, got {type(actual_value).__name__}"
            elif expected_type == "array" and not isinstance(actual_value, list):
                passed = False
                error_msg = f"Field '{rule.field_path}' must be array, got {type(actual_value).__name__}"
            elif expected_type == "object" and not isinstance(actual_value, dict):
                passed = False
                error_msg = f"Field '{rule.field_path}' must be object, got {type(actual_value).__name__}"
        elif rule.rule_type == "range":
            if actual_value is not None and isinstance(actual_value, (int, float)):
                min_val = rule.expected.get("min") if isinstance(rule.expected, dict) else None
                max_val = rule.expected.get("max") if isinstance(rule.expected, dict) else None
                if min_val is not None and actual_value < min_val:
                    passed = False
                    error_msg = f"Field '{rule.field_path}' value {actual_value} is below minimum {min_val}"
                if max_val is not None and actual_value > max_val:
                    passed = False
                    error_msg = f"Field '{rule.field_path}' value {actual_value} exceeds maximum {max_val}"
        elif rule.rule_type == "enum":
            if actual_value not in rule.expected:
                passed = False
                error_msg = f"Field '{rule.field_path}' value '{actual_value}' not in allowed values {rule.expected}"
        elif rule.rule_type == "pattern":
            import re
            if actual_value is not None and isinstance(actual_value, str):
                try:
                    if not re.match(rule.expected, actual_value):
                        passed = False
                        error_msg = f"Field '{rule.field_path}' value does not match pattern {rule.expected}"
                except re.error:
                    pass

        return ValidationResult(
            field_path=rule.field_path,
            passed=passed,
            actual_value=actual_value,
            expected_value=rule.expected,
            error_message=error_msg
        )

    def _flatten_dict(
        self,
        d: Dict,
        parent_key: str = "",
        sep: str = "."
    ) -> Dict[str, Any]:
        """Flatten a nested dictionary."""
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self._flatten_dict(v, new_key, sep))
            else:
                items[new_key] = v
        return items

    def _test_contract(
        self,
        contract_name: Optional[str],
        test_data: Any,
        start_time: float
    ) -> ActionResult:
        """Test a contract by name."""
        if not contract_name:
            return ActionResult(success=False, message="contract_name required", duration=time.time() - start_time)

        # Find contract
        key = None
        for k in self._contracts:
            if contract_name in k:
                key = k
                break

        if not key:
            return ActionResult(success=False, message=f"Contract '{contract_name}' not found", duration=time.time() - start_time)

        spec = self._contracts[key]
        flat_data = self._flatten_dict(test_data) if isinstance(test_data, dict) else {}

        results = []
        for rule in spec.rules:
            result = self._validate_rule(rule, flat_data, test_data)
            results.append((rule, result))

        passed = all(r.passed for _, r in results)
        failed = sum(1 for _, r in results if not r.passed)

        return ActionResult(
            success=passed,
            message=f"Contract test {'PASSED' if passed else 'FAILED'}",
            data={
                "contract": spec.name,
                "version": spec.version,
                "passed": passed,
                "passed_count": len(results) - failed,
                "failed_count": failed,
                "details": [{"field": r.field_path, "passed": r.passed, "error": r.error_message} for _, r in results]
            },
            duration=time.time() - start_time
        )

    def _add_contract_rule(
        self,
        contract_name: Optional[str],
        rule_data: Optional[Dict],
        start_time: float
    ) -> ActionResult:
        """Add a rule to an existing contract."""
        if not contract_name or not rule_data:
            return ActionResult(success=False, message="contract_name and rule required", duration=time.time() - start_time)

        key = None
        for k in self._contracts:
            if contract_name in k:
                key = k
                break

        if not key:
            return ActionResult(success=False, message=f"Contract '{contract_name}' not found", duration=time.time() - start_time)

        rule = ContractRule(**rule_data)
        self._contracts[key].rules.append(rule)

        return ActionResult(
            success=True,
            message=f"Added rule to contract '{contract_name}'",
            data={"contract": contract_name, "total_rules": len(self._contracts[key].rules)},
            duration=time.time() - start_time
        )

    def _get_test_report(self, start_time: float) -> ActionResult:
        """Get validation test report."""
        return ActionResult(
            success=True,
            message="Test report retrieved",
            data={
                "total_contracts": len(self._contracts),
                "contracts": [
                    {"name": c.name, "version": c.version, "rules_count": len(c.rules)}
                    for c in self._contracts.values()
                ]
            },
            duration=time.time() - start_time
        )


from enum import Enum
