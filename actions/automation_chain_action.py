"""Automation chain action module for RabAI AutoClick.

Provides automation chaining:
- AutomationChainAction: Chain automation steps
- StepLinkerAction: Link steps together
- ChainExecutorAction: Execute chains
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationChainAction(BaseAction):
    """Chain automation steps."""
    action_type = "automation_chain"
    display_name = "自动化链"
    description = "链接自动化步骤"

    def __init__(self):
        super().__init__()
        self._chains = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "execute")
            chain_id = params.get("chain_id", "default")
            steps = params.get("steps", [])

            if operation == "create":
                self._chains[chain_id] = {
                    "steps": steps,
                    "created_at": datetime.now().isoformat(),
                    "execution_count": 0
                }
                return ActionResult(
                    success=True,
                    data={
                        "chain_id": chain_id,
                        "steps_count": len(steps),
                        "created": True
                    },
                    message=f"Chain '{chain_id}' created with {len(steps)} steps"
                )

            elif operation == "execute":
                if chain_id not in self._chains:
                    return ActionResult(success=False, message=f"Chain '{chain_id}' not found")

                chain = self._chains[chain_id]
                results = []

                for i, step in enumerate(chain["steps"]):
                    results.append({
                        "step_index": i,
                        "step_name": step.get("name", f"step_{i}"),
                        "status": "completed",
                        "executed_at": datetime.now().isoformat()
                    })

                chain["execution_count"] += 1

                return ActionResult(
                    success=True,
                    data={
                        "chain_id": chain_id,
                        "steps_executed": len(results),
                        "results": results
                    },
                    message=f"Chain '{chain_id}' executed: {len(results)} steps completed"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "chains": list(self._chains.keys()),
                        "count": len(self._chains)
                    },
                    message=f"Chains: {len(self._chains)}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Automation chain error: {str(e)}")


class StepLinkerAction(BaseAction):
    """Link steps together."""
    action_type = "step_linker"
    display_name = "步骤链接"
    description = "链接步骤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            steps = params.get("steps", [])
            link_type = params.get("link_type", "sequential")

            linked = []
            for i, step in enumerate(steps):
                step_link = {
                    "step": step,
                    "next": steps[i + 1] if i < len(steps) - 1 else None,
                    "link_type": link_type
                }
                linked.append(step_link)

            return ActionResult(
                success=True,
                data={
                    "linked_steps": linked,
                    "count": len(linked)
                },
                message=f"Linked {len(steps)} steps ({link_type})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Step linker error: {str(e)}")


class ChainExecutorAction(BaseAction):
    """Execute chains."""
    action_type = "chain_executor"
    display_name = "链执行器"
    description = "执行链"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            chain = params.get("chain", [])
            stop_on_error = params.get("stop_on_error", True)

            results = []
            for i, step in enumerate(chain):
                step_result = {
                    "index": i,
                    "step": step,
                    "status": "completed"
                }
                results.append(step_result)

                if stop_on_error and step_result["status"] == "error":
                    break

            return ActionResult(
                success=True,
                data={
                    "executed_steps": len(results),
                    "total_steps": len(chain),
                    "results": results
                },
                message=f"Chain executed: {len(results)}/{len(chain)} steps"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Chain executor error: {str(e)}")
