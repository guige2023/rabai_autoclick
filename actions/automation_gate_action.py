"""Automation Gate action module for RabAI AutoClick.

Gate actions for conditional execution, approvals,
and manual intervention points.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationGateAction(BaseAction):
    """Conditional gate for automation execution.

    Blocks execution until conditions are met,
    approvals received, or timeout.
    """
    action_type = "automation_gate"
    display_name = "自动化门控"
    description = "条件执行和审批门控"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage gate.

        Args:
            context: Execution context.
            params: Dict with keys: action (check/pass/approve/reset),
                   gate_id, condition_fn, approval_required,
                   timeout_seconds.

        Returns:
            ActionResult with gate status.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'check')
            gate_id = params.get('gate_id', 'default')
            condition_fn = params.get('condition_fn')
            approval_required = params.get('approval_required', False)
            timeout_seconds = params.get('timeout_seconds', 300)

            if not hasattr(context, '_automation_gates'):
                context._automation_gates = {}
            gates = context._automation_gates
            if gate_id not in gates:
                gates[gate_id] = {
                    'approved': False,
                    'condition_met': False,
                    'opened_at': None,
                    'created_at': time.time(),
                    'approvers': set(),
                }

            g = gates[gate_id]
            now = time.time()

            if action == 'check':
                if approval_required and not g['approved']:
                    return ActionResult(
                        success=False,
                        message=f"Gate {gate_id}: waiting for approval",
                        data={'gate_id': gate_id, 'approved': False, 'condition_met': g['condition_met'], 'waiting_for': 'approval'},
                        duration=time.time() - start_time,
                    )
                if callable(condition_fn):
                    try:
                        condition_met = condition_fn(context, g)
                        g['condition_met'] = condition_met
                    except Exception as e:
                        return ActionResult(success=False, message=f"Condition error: {str(e)}", duration=time.time() - start_time)
                    if not condition_met:
                        return ActionResult(
                            success=False,
                            message=f"Gate {gate_id}: condition not met",
                            data={'gate_id': gate_id, 'approved': g['approved'], 'condition_met': False, 'waiting_for': 'condition'},
                            duration=time.time() - start_time,
                        )
                g['opened_at'] = now
                return ActionResult(
                    success=True,
                    message=f"Gate {gate_id}: OPEN",
                    data={'gate_id': gate_id, 'opened': True, 'wait_time': (now - g['created_at']) if not g['opened_at'] else 0},
                    duration=time.time() - start_time,
                )

            elif action == 'approve':
                approver = params.get('approver', 'unknown')
                g['approved'] = True
                g['approvers'].add(approver)
                return ActionResult(
                    success=True,
                    message=f"Gate {gate_id}: approved by {approver}",
                    data={'gate_id': gate_id, 'approved': True, 'approvers': list(g['approvers'])},
                    duration=time.time() - start_time,
                )

            elif action == 'pass':
                g['opened_at'] = now
                g['condition_met'] = True
                g['approved'] = True
                return ActionResult(
                    success=True,
                    message=f"Gate {gate_id}: passed",
                    data={'gate_id': gate_id, 'passed': True},
                    duration=time.time() - start_time,
                )

            elif action == 'reset':
                gates[gate_id] = {'approved': False, 'condition_met': False, 'opened_at': None, 'created_at': time.time(), 'approvers': set()}
                return ActionResult(success=True, message=f"Gate {gate_id} reset", duration=time.time() - start_time)

            elif action == 'status':
                return ActionResult(
                    success=g['approved'] and g['condition_met'],
                    message=f"Gate {gate_id}: {'OPEN' if g['opened_at'] else 'CLOSED'}",
                    data={
                        'gate_id': gate_id,
                        'approved': g['approved'],
                        'condition_met': g['condition_met'],
                        'opened': g['opened_at'] is not None,
                        'approvers': list(g['approvers']),
                    },
                    duration=time.time() - start_time,
                )

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}", duration=time.time() - start_time)

        except Exception as e:
            return ActionResult(success=False, message=f"Gate error: {str(e)}", duration=time.time() - start_time)
