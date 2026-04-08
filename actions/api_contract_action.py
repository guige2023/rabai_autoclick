"""API Contract Action Module.

Manages API contracts and consumer-driven contracts
for integration testing.
"""

from __future__ import annotations

import sys
import os
import time
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Contract:
    """An API contract."""
    name: str
    provider: str
    consumer: str
    interactions: List[Dict[str, Any]]
    created_at: float = 0.0


class APIContractAction(BaseAction):
    """
    API contract management.

    Manages consumer-driven contracts for API
    integration testing and verification.

    Example:
        contract_mgr = APIContractAction()
        result = contract_mgr.execute(ctx, {"action": "create_contract", "name": "user-service"})
    """
    action_type = "api_contract"
    display_name = "API契约管理"
    description = "API契约管理和消费者驱动测试"

    def __init__(self) -> None:
        super().__init__()
        self._contracts: Dict[str, Contract] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "create_contract":
                return self._create_contract(params)
            elif action == "verify_contract":
                return self._verify_contract(params)
            elif action == "list_contracts":
                return self._list_contracts(params)
            elif action == "get_contract":
                return self._get_contract(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Contract error: {str(e)}")

    def _create_contract(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        provider = params.get("provider", "")
        consumer = params.get("consumer", "")
        interactions = params.get("interactions", [])

        if not name:
            return ActionResult(success=False, message="name is required")

        contract = Contract(name=name, provider=provider, consumer=consumer, interactions=interactions, created_at=time.time())
        self._contracts[name] = contract

        return ActionResult(success=True, message=f"Contract created: {name}", data={"name": name, "interactions": len(interactions)})

    def _verify_contract(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        responses = params.get("responses", [])

        if name not in self._contracts:
            return ActionResult(success=False, message=f"Contract not found: {name}")

        contract = self._contracts[name]
        verified = len(responses) >= len(contract.interactions)

        return ActionResult(success=True, message="Contract verified" if verified else "Contract verification failed", data={"contract": name, "verified": verified})

    def _list_contracts(self, params: Dict[str, Any]) -> ActionResult:
        provider = params.get("provider")
        consumer = params.get("consumer")

        contracts = list(self._contracts.values())

        if provider:
            contracts = [c for c in contracts if c.provider == provider]
        if consumer:
            contracts = [c for c in contracts if c.consumer == consumer]

        return ActionResult(success=True, data={"contracts": [{"name": c.name, "provider": c.provider, "consumer": c.consumer} for c in contracts], "count": len(contracts)})

    def _get_contract(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")

        if name not in self._contracts:
            return ActionResult(success=False, message=f"Contract not found: {name}")

        contract = self._contracts[name]

        return ActionResult(success=True, data={"name": contract.name, "provider": contract.provider, "consumer": contract.consumer, "interactions": contract.interactions})
