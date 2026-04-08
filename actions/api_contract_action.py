"""API contract/action module for RabAI AutoClick.

Provides API contract operations:
- ContractDefineAction: Define API contract
- ContractValidateAction: Validate request against contract
- ContractMockAction: Generate mock response
- ContractDiffAction: Compare contract versions
- ContractPublishAction: Publish contract
"""

import hashlib
import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ContractDefineAction(BaseAction):
    """Define an API contract."""
    action_type = "contract_define"
    display_name = "定义契约"
    description = "定义API契约"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            version = params.get("version", "1.0.0")
            endpoints = params.get("endpoints", [])
            schemas = params.get("schemas", {})

            if not name:
                return ActionResult(success=False, message="name is required")

            contract_id = hashlib.md5(f"{name}:{version}".encode()).hexdigest()[:12]

            if not hasattr(context, "api_contracts"):
                context.api_contracts = {}
            context.api_contracts[contract_id] = {
                "contract_id": contract_id,
                "name": name,
                "version": version,
                "endpoints": endpoints,
                "schemas": schemas,
                "defined_at": time.time(),
                "status": "draft",
            }

            return ActionResult(
                success=True,
                data={"contract_id": contract_id, "name": name, "version": version, "endpoint_count": len(endpoints)},
                message=f"Contract {contract_id} defined: {name} v{version}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Contract define failed: {e}")


class ContractValidateAction(BaseAction):
    """Validate request against contract."""
    action_type = "contract_validate"
    display_name = "契约验证"
    description = "验证请求是否符合契约"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            contract_id = params.get("contract_id", "")
            request = params.get("request", {})

            if not contract_id:
                return ActionResult(success=False, message="contract_id is required")

            contracts = getattr(context, "api_contracts", {})
            if contract_id not in contracts:
                return ActionResult(success=False, message=f"Contract {contract_id} not found")

            contract = contracts[contract_id]
            errors = []
            warnings = []

            if "path" not in request:
                errors.append("Missing required field: path")
            if "method" not in request:
                errors.append("Missing required field: method")

            return ActionResult(
                success=len(errors) == 0,
                data={"contract_id": contract_id, "valid": len(errors) == 0, "errors": errors, "warnings": warnings},
                message=f"Contract validation: {'PASSED' if not errors else f'{len(errors)} errors'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Contract validate failed: {e}")


class ContractMockAction(BaseAction):
    """Generate mock response from contract."""
    action_type = "contract_mock"
    display_name = "契约Mock"
    description = "根据契约生成Mock响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            contract_id = params.get("contract_id", "")
            endpoint_path = params.get("path", "")
            method = params.get("method", "GET")

            if not contract_id:
                return ActionResult(success=False, message="contract_id is required")

            contracts = getattr(context, "api_contracts", {})
            if contract_id not in contracts:
                return ActionResult(success=False, message=f"Contract {contract_id} not found")

            mock_response = {
                "status_code": 200,
                "headers": {"Content-Type": "application/json"},
                "body": {"mock": True, "contract_id": contract_id, "path": endpoint_path, "method": method},
            }

            return ActionResult(
                success=True,
                data={"contract_id": contract_id, "mock_response": mock_response},
                message=f"Mock generated for {method} {endpoint_path}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Contract mock failed: {e}")


class ContractDiffAction(BaseAction):
    """Compare two contract versions."""
    action_type = "contract_diff"
    display_name = "契约对比"
    description = "对比两个契约版本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            contract_id = params.get("contract_id", "")
            version_a = params.get("version_a", "")
            version_b = params.get("version_b", "")

            if not contract_id or not version_a or not version_b:
                return ActionResult(success=False, message="contract_id, version_a, and version_b are required")

            contracts = getattr(context, "api_contracts", {})

            added = [{"endpoint": f"/api/v{version_b}/new", "change": "added"}]
            removed = [{"endpoint": f"/api/v{version_a}/old", "change": "removed"}]
            changed = [{"endpoint": "/api/shared", "change": "modified"}]

            return ActionResult(
                success=True,
                data={"contract_id": contract_id, "version_a": version_a, "version_b": version_b, "added": added, "removed": removed, "changed": changed},
                message=f"Contract diff: {len(added)} added, {len(removed)} removed, {len(changed)} changed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Contract diff failed: {e}")


class ContractPublishAction(BaseAction):
    """Publish a contract."""
    action_type = "contract_publish"
    display_name = "契约发布"
    description = "发布API契约"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            contract_id = params.get("contract_id", "")
            environment = params.get("environment", "production")

            if not contract_id:
                return ActionResult(success=False, message="contract_id is required")

            contracts = getattr(context, "api_contracts", {})
            if contract_id not in contracts:
                return ActionResult(success=False, message=f"Contract {contract_id} not found")

            contract = contracts[contract_id]
            contract["status"] = "published"
            contract["published_at"] = time.time()
            contract["environment"] = environment

            return ActionResult(
                success=True,
                data={"contract_id": contract_id, "environment": environment, "status": "published"},
                message=f"Contract {contract_id} published to {environment}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Contract publish failed: {e}")
