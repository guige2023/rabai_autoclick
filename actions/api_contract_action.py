"""
API Contract Action - Contract testing for APIs.

This module provides contract testing capabilities for
validating API contracts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ContractRule:
    """A contract rule for validation."""
    field: str
    expected_type: str
    required: bool = False


@dataclass
class ContractSpec:
    """API contract specification."""
    name: str
    status_code: int
    rules: list[ContractRule] = field(default_factory=list)


@dataclass
class ContractResult:
    """Result of contract validation."""
    passed: bool
    contract_name: str
    errors: list[str] = field(default_factory=list)


class ContractValidator:
    """Validates API contracts."""
    
    def __init__(self) -> None:
        self._contracts: dict[str, ContractSpec] = {}
    
    def register_contract(self, contract: ContractSpec) -> None:
        """Register a contract."""
        self._contracts[contract.name] = contract
    
    def validate(self, name: str, status_code: int, data: Any) -> ContractResult:
        """Validate response against contract."""
        if name not in self._contracts:
            return ContractResult(passed=False, contract_name=name, errors=[f"Contract {name} not found"])
        
        contract = self._contracts[name]
        errors = []
        
        if status_code != contract.status_code:
            errors.append(f"Expected status {contract.status_code}, got {status_code}")
        
        for rule in contract.rules:
            value = self._get_nested(data, rule.field)
            if rule.required and value is None:
                errors.append(f"Required field missing: {rule.field}")
            elif value is not None and not self._check_type(value, rule.expected_type):
                errors.append(f"Field {rule.field} expected type {rule.expected_type}")
        
        return ContractResult(passed=len(errors) == 0, contract_name=name, errors=errors)
    
    def _get_nested(self, data: Any, path: str) -> Any:
        """Get nested value."""
        if not path:
            return data
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current
    
    def _check_type(self, value: Any, expected: str) -> bool:
        """Check if value matches expected type."""
        type_map = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict,
        }
        expected_type = type_map.get(expected)
        if expected_type:
            return isinstance(value, expected_type)
        return True


class APIContractAction:
    """API contract action for automation workflows."""
    
    def __init__(self) -> None:
        self.validator = ContractValidator()
    
    def add_contract(self, name: str, status_code: int, rules: list[dict]) -> None:
        """Add a contract."""
        contract = ContractSpec(
            name=name,
            status_code=status_code,
            rules=[ContractRule(**r) for r in rules],
        )
        self.validator.register_contract(contract)
    
    async def validate(self, name: str, status_code: int, data: Any) -> ContractResult:
        """Validate response against contract."""
        return self.validator.validate(name, status_code, data)


__all__ = ["ContractRule", "ContractSpec", "ContractResult", "ContractValidator", "APIContractAction"]
