"""Data contract action module for RabAI AutoClick.

Provides data contract operations:
- DataContractBuilderAction: Build data contracts with schemas
- DataContractValidatorAction: Validate data against contracts
- DataContractNegotiatorAction: Negotiate contracts between producer/consumer
- DataContractEnforcerAction: Enforce data contracts
- DataContractVersionManagerAction: Manage data contract versions
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ContractStatus(Enum):
    """Data contract status."""
    DRAFT = auto()
    NEGOTIATING = auto()
    AGREED = auto()
    ACTIVE = auto()
    VIOLATED = auto()
    TERMINATED = auto()


class DataContractBuilderAction(BaseAction):
    """Build data contracts with schemas."""
    action_type = "data_contract_builder"
    display_name = "数据契约构建"
    description = "构建带Schema的数据契约"

    def __init__(self) -> None:
        super().__init__()
        self._contracts: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "build":
                return self._build_contract(params)
            elif action == "add_field":
                return self._add_field(params)
            elif action == "add_constraint":
                return self._add_constraint(params)
            elif action == "finalize":
                return self._finalize_contract(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Data contract building failed: {e}")

    def _build_contract(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        producer = params.get("producer", "")
        consumer = params.get("consumer", "")
        schema = params.get("schema", {})
        sla = params.get("sla", {})
        if not name:
            return ActionResult(success=False, message="name is required")

        contract_id = str(uuid.uuid4())
        fingerprint = hashlib.sha256(json.dumps({"name": name, "schema": schema}, sort_keys=True).encode()).hexdigest()[:16]
        self._contracts[contract_id] = {
            "id": contract_id,
            "name": name,
            "producer": producer,
            "consumer": consumer,
            "schema": schema,
            "fields": schema.get("properties", {}),
            "sla": sla,
            "status": ContractStatus.DRAFT.name,
            "fingerprint": fingerprint,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0.0",
            "constraints": [],
            "violations": [],
        }
        return ActionResult(success=True, message=f"Contract '{name}' built", data={"contract_id": contract_id, "fingerprint": fingerprint})

    def _add_field(self, params: Dict[str, Any]) -> ActionResult:
        contract_id = params.get("contract_id", "")
        field_name = params.get("field_name", "")
        field_schema = params.get("field_schema", {"type": "string"})
        if contract_id not in self._contracts:
            return ActionResult(success=False, message="Contract not found")
        if not field_name:
            return ActionResult(success=False, message="field_name is required")
        self._contracts[contract_id]["fields"][field_name] = field_schema
        return ActionResult(success=True, message=f"Field '{field_name}' added to contract")

    def _add_constraint(self, params: Dict[str, Any]) -> ActionResult:
        contract_id = params.get("contract_id", "")
        constraint_type = params.get("constraint_type", "")
        field = params.get("field", "")
        value = params.get("value", None)
        if contract_id not in self._contracts:
            return ActionResult(success=False, message="Contract not found")
        constraint = {
            "id": str(uuid.uuid4()),
            "type": constraint_type,
            "field": field,
            "value": value,
        }
        self._contracts[contract_id]["constraints"].append(constraint)
        return ActionResult(success=True, message=f"Constraint '{constraint_type}' added")

    def _finalize_contract(self, params: Dict[str, Any]) -> ActionResult:
        contract_id = params.get("contract_id", "")
        if contract_id not in self._contracts:
            return ActionResult(success=False, message="Contract not found")
        self._contracts[contract_id]["status"] = ContractStatus.NEGOTIATING.name
        self._contracts[contract_id]["finalized_at"] = datetime.now(timezone.utc).isoformat()
        return ActionResult(success=True, message="Contract finalized and sent for negotiation")


class DataContractValidatorAction(BaseAction):
    """Validate data against contracts."""
    action_type = "data_contract_validator"
    display_name = "数据契约验证"
    description = "根据契约验证数据"

    def __init__(self) -> None:
        super().__init__()
        self._violations: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            contract_id = params.get("contract_id", "")
            data = params.get("data", {})
            contracts = params.get("contracts", {})
            if contract_id:
                contract = contracts.get(contract_id) if contracts else None
                if not contract:
                    return ActionResult(success=False, message=f"Contract not found: {contract_id}")
            else:
                return ActionResult(success=False, message="contract_id is required")

            violations = self._validate(data, contract)
            if violations:
                self._violations.extend(violations)
            return ActionResult(
                success=len(violations) == 0,
                message=f"Validation: {'PASSED' if not violations else f'{len(violations)} violations'}",
                data={"violations": violations, "valid": len(violations) == 0},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Contract validation failed: {e}")

    def _validate(self, data: Dict[str, Any], contract: Dict[str, Any]) -> List[Dict[str, Any]]:
        violations: List[Dict[str, Any]] = []
        fields = contract.get("fields", {})
        for field_name, field_schema in fields.items():
            if field_name not in data:
                violations.append({
                    "field": field_name,
                    "type": "MISSING_FIELD",
                    "message": f"Required field '{field_name}' is missing",
                })
            else:
                value = data[field_name]
                expected_type = field_schema.get("type", "string")
                if not self._check_type(value, expected_type):
                    violations.append({
                        "field": field_name,
                        "type": "TYPE_MISMATCH",
                        "message": f"Field '{field_name}' expected {expected_type}",
                    })
        for constraint in contract.get("constraints", []):
            field = constraint.get("field", "")
            ctype = constraint.get("type", "")
            if field in data:
                if ctype == "not_null" and data[field] is None:
                    violations.append({"field": field, "type": "CONSTRAINT_VIOLATION", "message": f"not_null violation on {field}"})
                elif ctype == "min_length" and isinstance(data[field], str) and len(data[field]) < constraint.get("value", 0):
                    violations.append({"field": field, "type": "CONSTRAINT_VIOLATION", "message": f"min_length violation on {field}"})
        return violations

    def _check_type(self, value: Any, expected: str) -> bool:
        type_map = {"string": str, "number": (int, float), "integer": int, "boolean": bool, "array": list, "object": dict}
        return isinstance(value, type_map.get(expected, str))


class DataContractNegotiatorAction(BaseAction):
    """Negotiate contracts between producer and consumer."""
    action_type = "data_contract_negotiator"
    display_name = "数据契约协商"
    description = "生产者和消费者之间协商契约"

    def __init__(self) -> None:
        super().__init__()
        self._negotiations: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "propose":
                return self._propose_contract(params)
            elif action == "counter":
                return self._counter_proposal(params)
            elif action == "accept":
                return self._accept_proposal(params)
            elif action == "reject":
                return self._reject_proposal(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Contract negotiation failed: {e}")

    def _propose_contract(self, params: Dict[str, Any]) -> ActionResult:
        contract_id = params.get("contract_id", "")
        producer_terms = params.get("producer_terms", {})
        consumer_terms = params.get("consumer_terms", {})
        negotiation_id = str(uuid.uuid4())
        self._negotiations[negotiation_id] = {
            "id": negotiation_id,
            "contract_id": contract_id,
            "producer_terms": producer_terms,
            "consumer_terms": consumer_terms,
            "status": "PROPOSED",
            "proposed_at": datetime.now(timezone.utc).isoformat(),
        }
        return ActionResult(success=True, message=f"Contract proposed: {negotiation_id[:8]}", data={"negotiation_id": negotiation_id})

    def _counter_proposal(self, params: Dict[str, Any]) -> ActionResult:
        negotiation_id = params.get("negotiation_id", "")
        counter_terms = params.get("counter_terms", {})
        if negotiation_id not in self._negotiations:
            return ActionResult(success=False, message="Negotiation not found")
        self._negotiations[negotiation_id]["consumer_terms"] = counter_terms
        self._negotiations[negotiation_id]["status"] = "COUNTERED"
        return ActionResult(success=True, message="Counter proposal sent")

    def _accept_proposal(self, params: Dict[str, Any]) -> ActionResult:
        negotiation_id = params.get("negotiation_id", "")
        if negotiation_id not in self._negotiations:
            return ActionResult(success=False, message="Negotiation not found")
        self._negotiations[negotiation_id]["status"] = ContractStatus.AGREED.name
        self._negotiations[negotiation_id]["accepted_at"] = datetime.now(timezone.utc).isoformat()
        return ActionResult(success=True, message="Contract agreed!")

    def _reject_proposal(self, params: Dict[str, Any]) -> ActionResult:
        negotiation_id = params.get("negotiation_id", "")
        reason = params.get("reason", "")
        if negotiation_id not in self._negotiations:
            return ActionResult(success=False, message="Negotiation not found")
        self._negotiations[negotiation_id]["status"] = ContractStatus.TERMINATED.name
        self._negotiations[negotiation_id]["rejected_reason"] = reason
        return ActionResult(success=True, message="Contract rejected")


class DataContractEnforcerAction(BaseAction):
    """Enforce data contracts."""
    action_type = "data_contract_enforcer"
    display_name = "数据契约执行"
    description = "强制执行数据契约"

    def __init__(self) -> None:
        super().__init__()
        self._enforcement_log: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            contract_id = params.get("contract_id", "")
            data = params.get("data", {})
            if not contract_id:
                return ActionResult(success=False, message="contract_id is required")

            violations = params.get("violations", [])
            allowed = len(violations) == 0
            log_entry = {
                "contract_id": contract_id,
                "data_hash": hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()[:16],
                "allowed": allowed,
                "violations": violations,
                "enforced_at": datetime.now(timezone.utc).isoformat(),
            }
            self._enforcement_log.append(log_entry)
            return ActionResult(
                success=True,
                message=f"Contract enforcement: {'ALLOWED' if allowed else 'BLOCKED'}",
                data=log_entry,
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Contract enforcement failed: {e}")


class DataContractVersionManagerAction(BaseAction):
    """Manage data contract versions."""
    action_type = "data_contract_version_manager"
    display_name = "数据契约版本管理"
    description = "管理数据契约版本"

    def __init__(self) -> None:
        super().__init__()
        self._version_history: Dict[str, List[Dict[str, Any]]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "version_up":
                return self._version_up(params)
            elif action == "list_versions":
                return self._list_versions(params)
            elif action == "rollback":
                return self._rollback(params)
            elif action == "diff":
                return self._diff_versions(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Contract version management failed: {e}")

    def _version_up(self, params: Dict[str, Any]) -> ActionResult:
        contract_name = params.get("contract_name", "")
        change_type = params.get("change_type", "minor")
        new_version = params.get("new_version", "")
        if not new_version:
            return ActionResult(success=False, message="new_version is required")
        self._version_history.setdefault(contract_name, []).append({
            "version": new_version,
            "change_type": change_type,
            "changed_at": datetime.now(timezone.utc).isoformat(),
        })
        return ActionResult(success=True, message=f"Contract versioned to {new_version}")

    def _list_versions(self, params: Dict[str, Any]) -> ActionResult:
        contract_name = params.get("contract_name", "")
        versions = self._version_history.get(contract_name, [])
        return ActionResult(success=True, message=f"{len(versions)} versions", data={"versions": versions})

    def _rollback(self, params: Dict[str, Any]) -> ActionResult:
        contract_name = params.get("contract_name", "")
        target_version = params.get("target_version", "")
        if contract_name in self._version_history and self._version_history[contract_name]:
            self._version_history[contract_name].pop()
            return ActionResult(success=True, message=f"Rolled back to previous version")
        return ActionResult(success=False, message="No version to rollback to")

    def _diff_versions(self, params: Dict[str, Any]) -> ActionResult:
        v1 = params.get("version1", "")
        v2 = params.get("version2", "")
        diff = {"added_fields": [], "removed_fields": [], "changed_fields": []}
        return ActionResult(success=True, message=f"Diff v{v1} vs v{v2}", data=diff)
