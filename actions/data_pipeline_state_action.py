"""Data Pipeline State Machine action module for RabAI AutoClick.

State machine for managing pipeline lifecycle with
transitions, guards, and side effects.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataPipelineStateMachineAction(BaseAction):
    """State machine for pipeline lifecycle management.

    States: IDLE, RUNNING, PAUSED, COMPLETED, FAILED, CANCELLED.
    Transitions with guards and side effects.
    """
    action_type = "data_pipeline_state_machine"
    display_name = "数据管道状态机"
    description = "管理管道生命周期的状态机"

    class PipelineState(Enum):
        IDLE = "idle"
        RUNNING = "running"
        PAUSED = "paused"
        COMPLETED = "completed"
        FAILED = "failed"
        CANCELLED = "cancelled"

    VALID_TRANSITIONS = {
        PipelineState.IDLE: [PipelineState.RUNNING],
        PipelineState.RUNNING: [PipelineState.PAUSED, PipelineState.COMPLETED, PipelineState.FAILED, PipelineState.CANCELLED],
        PipelineState.PAUSED: [PipelineState.RUNNING, PipelineState.CANCELLED],
        PipelineState.COMPLETED: [PipelineState.IDLE],
        PipelineState.FAILED: [PipelineState.IDLE, PipelineState.RUNNING],
        PipelineState.CANCELLED: [PipelineState.IDLE],
    }

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage pipeline state.

        Args:
            context: Execution context.
            params: Dict with keys: action (transition/status/reset),
                   pipeline_id, target_state, guard_fn,
                   side_effect_fn.

        Returns:
            ActionResult with state transition result.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'status')
            pipeline_id = params.get('pipeline_id', 'default')
            target_state_str = params.get('target_state', '')
            guard_fn = params.get('guard_fn')
            side_effect_fn = params.get('side_effect_fn')

            if not hasattr(context, '_pipeline_states'):
                context._pipeline_states = {}

            if pipeline_id not in context._pipeline_states:
                context._pipeline_states[pipeline_id] = {
                    'state': self.PipelineState.IDLE.value,
                    'history': [],
                    'entry_time': time.time(),
                    'data': {},
                }

            ps = context._pipeline_states[pipeline_id]
            current_state = self.PipelineState(ps['state'])

            if action == 'status':
                return ActionResult(
                    success=True,
                    message=f"Pipeline {pipeline_id}: {ps['state']}",
                    data={
                        'pipeline_id': pipeline_id,
                        'state': ps['state'],
                        'history': ps['history'],
                        'data': ps.get('data', {}),
                    },
                    duration=time.time() - start_time,
                )

            elif action == 'transition':
                if not target_state_str:
                    return ActionResult(
                        success=False,
                        message="target_state is required",
                        duration=time.time() - start_time,
                    )

                try:
                    target_state = self.PipelineState(target_state_str)
                except ValueError:
                    return ActionResult(
                        success=False,
                        message=f"Invalid state: {target_state_str}",
                        duration=time.time() - start_time,
                    )

                # Check if transition is valid
                valid_targets = self.VALID_TRANSITIONS.get(current_state, [])
                if target_state not in valid_targets:
                    return ActionResult(
                        success=False,
                        message=f"Invalid transition: {ps['state']} -> {target_state_str}",
                        data={
                            'pipeline_id': pipeline_id,
                            'current_state': ps['state'],
                            'target_state': target_state_str,
                            'valid_transitions': [s.value for s in valid_targets],
                        },
                        duration=time.time() - start_time,
                    )

                # Check guard
                if guard_fn and callable(guard_fn):
                    try:
                        guard_result = guard_fn(current_state, target_state, ps, context)
                        if not guard_result:
                            return ActionResult(
                                success=False,
                                message=f"Guard prevented transition: {ps['state']} -> {target_state_str}",
                                data={'blocked': True},
                                duration=time.time() - start_time,
                            )
                    except Exception as e:
                        return ActionResult(
                            success=False,
                            message=f"Guard error: {str(e)}",
                            duration=time.time() - start_time,
                        )

                # Execute side effects
                if side_effect_fn and callable(side_effect_fn):
                    try:
                        side_effect_fn(current_state, target_state, ps, context)
                    except Exception as e:
                        return ActionResult(
                            success=False,
                            message=f"Side effect error: {str(e)}",
                            duration=time.time() - start_time,
                        )

                # Perform transition
                old_state = ps['state']
                ps['state'] = target_state.value
                ps['history'].append({
                    'from': old_state,
                    'to': target_state.value,
                    'timestamp': time.time(),
                })
                ps['entry_time'] = time.time()

                return ActionResult(
                    success=True,
                    message=f"Pipeline {pipeline_id}: {old_state} -> {target_state.value}",
                    data={
                        'pipeline_id': pipeline_id,
                        'from_state': old_state,
                        'to_state': target_state.value,
                        'history': ps['history'],
                    },
                    duration=time.time() - start_time,
                )

            elif action == 'reset':
                context._pipeline_states[pipeline_id] = {
                    'state': self.PipelineState.IDLE.value,
                    'history': [],
                    'entry_time': time.time(),
                    'data': {},
                }
                return ActionResult(
                    success=True,
                    message=f"Pipeline {pipeline_id} reset to IDLE",
                    duration=time.time() - start_time,
                )

            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown action: {action}",
                    duration=time.time() - start_time,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"State machine error: {str(e)}",
                duration=duration,
            )


class WorkflowOrchestratorAction(BaseAction):
    """Orchestrate multi-step workflows with state management.

    Coordinates complex workflows with parallel branches,
    joins, and compensation.
    """
    action_type = "workflow_orchestrator"
    display_name = "工作流编排器"
    description = "编排多步骤工作流"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Orchestrate workflow.

        Args:
            context: Execution context.
            params: Dict with keys: workflow_id, action (start/status/complete/fail/cancel),
                   steps, current_step, step_results.

        Returns:
            ActionResult with workflow result.
        """
        start_time = time.time()
        try:
            workflow_id = params.get('workflow_id', 'default')
            action = params.get('action', 'status')
            steps = params.get('steps', [])
            current_step = params.get('current_step', 0)
            step_results = params.get('step_results', {})

            if not hasattr(context, '_workflows'):
                context._workflows = {}

            if workflow_id not in context._workflows:
                context._workflows[workflow_id] = {
                    'status': 'pending',
                    'current_step': 0,
                    'step_results': {},
                    'started_at': None,
                    'completed_at': None,
                    'error': None,
                }

            wf = context._workflows[workflow_id]

            if action == 'start':
                wf['status'] = 'running'
                wf['started_at'] = time.time()
                wf['current_step'] = 0
                return ActionResult(
                    success=True,
                    message=f"Workflow {workflow_id} started",
                    data={
                        'workflow_id': workflow_id,
                        'status': wf['status'],
                        'total_steps': len(steps),
                        'current_step': 0,
                    },
                    duration=time.time() - start_time,
                )

            elif action == 'advance':
                step_name = steps[current_step].get('name', f'step_{current_step}') if current_step < len(steps) else 'unknown'
                wf['current_step'] = current_step
                wf['step_results'][current_step] = step_results.get(current_step)

                if current_step >= len(steps) - 1:
                    wf['status'] = 'completed'
                    wf['completed_at'] = time.time()
                    return ActionResult(
                        success=True,
                        message=f"Workflow {workflow_id} completed",
                        data={
                            'workflow_id': workflow_id,
                            'status': 'completed',
                            'total_steps': len(steps),
                            'completed_at': wf['completed_at'],
                        },
                        duration=time.time() - start_time,
                    )

                return ActionResult(
                    success=True,
                    message=f"Workflow {workflow_id}: step {current_step} ({step_name}) done",
                    data={
                        'workflow_id': workflow_id,
                        'status': 'running',
                        'completed_step': current_step,
                        'next_step': current_step + 1,
                        'next_step_name': steps[current_step + 1].get('name') if current_step + 1 < len(steps) else None,
                    },
                    duration=time.time() - start_time,
                )

            elif action == 'fail':
                error = params.get('error', 'Unknown error')
                wf['status'] = 'failed'
                wf['error'] = error
                wf['completed_at'] = time.time()
                return ActionResult(
                    success=False,
                    message=f"Workflow {workflow_id} failed: {error}",
                    data={
                        'workflow_id': workflow_id,
                        'status': 'failed',
                        'error': error,
                        'failed_at_step': wf['current_step'],
                    },
                    duration=time.time() - start_time,
                )

            elif action == 'cancel':
                wf['status'] = 'cancelled'
                wf['completed_at'] = time.time()
                return ActionResult(
                    success=True,
                    message=f"Workflow {workflow_id} cancelled",
                    data={'workflow_id': workflow_id, 'status': 'cancelled'},
                    duration=time.time() - start_time,
                )

            elif action == 'status':
                return ActionResult(
                    success=True,
                    message=f"Workflow {workflow_id}: {wf['status']}",
                    data={
                        'workflow_id': workflow_id,
                        'status': wf['status'],
                        'current_step': wf['current_step'],
                        'step_results': wf['step_results'],
                        'error': wf.get('error'),
                    },
                    duration=time.time() - start_time,
                )

            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown action: {action}",
                    duration=time.time() - start_time,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Workflow orchestrator error: {str(e)}",
                duration=duration,
            )
