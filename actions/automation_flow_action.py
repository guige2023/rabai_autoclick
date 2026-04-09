"""Automation Flow Action Module.

Provides flow control and state management for automation.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FlowState(Enum):
    """Flow execution states."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AutomationFlowAction(BaseAction):
    """Control flow execution with state management.
    
    Manages workflow execution states and transitions.
    """
    action_type = "automation_flow"
    display_name = "自动化流程"
    description = "管理工作流执行状态和转换"
    
    def __init__(self):
        super().__init__()
        self._flows: Dict[str, Dict] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flow control operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: flow_id, action, state.
        
        Returns:
            ActionResult with flow control result.
        """
        flow_id = params.get('flow_id', '')
        action = params.get('action', 'start')
        
        if not flow_id and action != 'list':
            return ActionResult(
                success=False,
                data=None,
                error="Flow ID required"
            )
        
        if action == 'start':
            return self._start_flow(flow_id, params)
        elif action == 'pause':
            return self._pause_flow(flow_id)
        elif action == 'resume':
            return self._resume_flow(flow_id)
        elif action == 'complete':
            return self._complete_flow(flow_id, params)
        elif action == 'fail':
            return self._fail_flow(flow_id, params)
        elif action == 'cancel':
            return self._cancel_flow(flow_id)
        elif action == 'status':
            return self._get_flow_status(flow_id)
        elif action == 'list':
            return self._list_flows()
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _start_flow(self, flow_id: str, params: Dict) -> ActionResult:
        """Start a new flow."""
        self._flows[flow_id] = {
            'flow_id': flow_id,
            'state': FlowState.RUNNING.value,
            'started_at': time.time(),
            'steps': params.get('steps', []),
            'current_step': 0,
            'metadata': params.get('metadata', {})
        }
        
        return ActionResult(
            success=True,
            data={
                'flow_id': flow_id,
                'state': FlowState.RUNNING.value
            },
            error=None
        )
    
    def _pause_flow(self, flow_id: str) -> ActionResult:
        """Pause a running flow."""
        if flow_id not in self._flows:
            return ActionResult(
                success=False,
                data=None,
                error="Flow not found"
            )
        
        self._flows[flow_id]['state'] = FlowState.PAUSED.value
        self._flows[flow_id]['paused_at'] = time.time()
        
        return ActionResult(
            success=True,
            data={
                'flow_id': flow_id,
                'state': FlowState.PAUSED.value
            },
            error=None
        )
    
    def _resume_flow(self, flow_id: str) -> ActionResult:
        """Resume a paused flow."""
        if flow_id not in self._flows:
            return ActionResult(
                success=False,
                data=None,
                error="Flow not found"
            )
        
        self._flows[flow_id]['state'] = FlowState.RUNNING.value
        self._flows[flow_id]['resumed_at'] = time.time()
        
        return ActionResult(
            success=True,
            data={
                'flow_id': flow_id,
                'state': FlowState.RUNNING.value
            },
            error=None
        )
    
    def _complete_flow(self, flow_id: str, params: Dict) -> ActionResult:
        """Mark flow as completed."""
        if flow_id not in self._flows:
            return ActionResult(
                success=False,
                data=None,
                error="Flow not found"
            )
        
        result = params.get('result', {})
        self._flows[flow_id]['state'] = FlowState.COMPLETED.value
        self._flows[flow_id]['completed_at'] = time.time()
        self._flows[flow_id]['result'] = result
        
        return ActionResult(
            success=True,
            data={
                'flow_id': flow_id,
                'state': FlowState.COMPLETED.value
            },
            error=None
        )
    
    def _fail_flow(self, flow_id: str, params: Dict) -> ActionResult:
        """Mark flow as failed."""
        if flow_id not in self._flows:
            return ActionResult(
                success=False,
                data=None,
                error="Flow not found"
            )
        
        error = params.get('error', 'Unknown error')
        self._flows[flow_id]['state'] = FlowState.FAILED.value
        self._flows[flow_id]['failed_at'] = time.time()
        self._flows[flow_id]['error'] = error
        
        return ActionResult(
            success=False,
            data={
                'flow_id': flow_id,
                'state': FlowState.FAILED.value,
                'error': error
            },
            error=error
        )
    
    def _cancel_flow(self, flow_id: str) -> ActionResult:
        """Cancel a flow."""
        if flow_id not in self._flows:
            return ActionResult(
                success=False,
                data=None,
                error="Flow not found"
            )
        
        self._flows[flow_id]['state'] = FlowState.CANCELLED.value
        self._flows[flow_id]['cancelled_at'] = time.time()
        
        return ActionResult(
            success=True,
            data={
                'flow_id': flow_id,
                'state': FlowState.CANCELLED.value
            },
            error=None
        )
    
    def _get_flow_status(self, flow_id: str) -> ActionResult:
        """Get flow status."""
        if flow_id not in self._flows:
            return ActionResult(
                success=False,
                data=None,
                error="Flow not found"
            )
        
        return ActionResult(
            success=True,
            data=self._flows[flow_id],
            error=None
        )
    
    def _list_flows(self) -> ActionResult:
        """List all flows."""
        flows = list(self._flows.values())
        
        return ActionResult(
            success=True,
            data={
                'flows': flows,
                'count': len(flows)
            },
            error=None
        )


class AutomationSequenceAction(BaseAction):
    """Execute actions in sequence with dependencies.
    
    Manages sequential execution with step dependencies.
    """
    action_type = "automation_sequence"
    display_name: "自动化序列"
    description: "带依赖的顺序执行"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sequence.
        
        Args:
            context: Execution context.
            params: Dict with keys: steps, start_from.
        
        Returns:
            ActionResult with sequence results.
        """
        steps = params.get('steps', [])
        start_from = params.get('start_from', 0)
        
        if not steps:
            return ActionResult(
                success=False,
                data=None,
                error="No steps provided"
            )
        
        results = []
        for i in range(start_from, len(steps)):
            step = steps[i]
            step_result = self._execute_step(step, i)
            results.append(step_result)
            
            if not step_result['success'] and not step.get('continue_on_error', False):
                break
        
        return ActionResult(
            success=all(r['success'] for r in results),
            data={
                'results': results,
                'completed_steps': len(results)
            },
            error=None if all(r['success'] for r in results) else "Sequence failed"
        )
    
    def _execute_step(self, step: Dict, index: int) -> Dict:
        """Execute a single step."""
        step_type = step.get('type', 'task')
        step_name = step.get('name', f'step_{index}')
        
        # Simulate step execution
        return {
            'index': index,
            'name': step_name,
            'type': step_type,
            'success': True,
            'executed_at': time.time()
        }


def register_actions():
    """Register all Automation Flow actions."""
    return [
        AutomationFlowAction,
        AutomationSequenceAction,
    ]
