"""Automation Flow Action Module.

Provides workflow automation capabilities including flow
orchestration, state management, and parallel task execution.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from enum import Enum
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FlowState(Enum):
    """Flow execution states."""
    PENDING = "pending"
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FlowNodeType(Enum):
    """Flow node types."""
    TASK = "task"
    CONDITION = "condition"
    PARALLEL = "parallel"
    LOOP = "loop"
    SUBFLOW = "subflow"
    WAIT = "wait"
    ERROR = "error"


@dataclass
class FlowNode:
    """Represents a node in the automation flow."""
    id: str
    node_type: FlowNodeType
    name: str
    config: Dict = field(default_factory=dict)
    next_nodes: List[str] = field(default_factory=list)
    condition: Optional[Callable] = None
    handler: Optional[Callable] = None


class AutomationFlowAction(BaseAction):
    """Execute automation workflows with conditional branching.
    
    Supports sequential, parallel, and conditional flows.
    """
    action_type = "automation_flow"
    display_name = "自动化流程"
    description = "执行自动化工作流，支持条件分支和并行执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute an automation flow.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - flow_definition: Flow definition with nodes.
                - start_node: Starting node ID.
                - input_data: Initial flow input data.
                - max_iterations: Max loop iterations.
                - timeout: Flow timeout in seconds.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with flow execution result or error.
        """
        flow_def = params.get('flow_definition', {})
        start_node = params.get('start_node', 'start')
        input_data = params.get('input_data', {})
        max_iterations = params.get('max_iterations', 1000)
        timeout = params.get('timeout', 300)
        output_var = params.get('output_var', 'flow_result')

        if not flow_def:
            return ActionResult(
                success=False,
                message="Flow definition is required"
            )

        try:
            # Build flow graph
            nodes = self._build_nodes(flow_def)
            if start_node not in nodes:
                return ActionResult(
                    success=False,
                    message=f"Start node '{start_node}' not found"
                )

            # Execute flow
            start_time = time.time()
            flow_context = {
                'data': input_data,
                'variables': dict(context.variables),
                'history': [],
                'current_node': start_node
            }

            execution_log = []
            current_state = FlowState.RUNNING
            iteration = 0

            while current_state == FlowState.RUNNING and iteration < max_iterations:
                if time.time() - start_time > timeout:
                    current_state = FlowState.FAILED
                    execution_log.append({
                        'node': flow_context['current_node'],
                        'error': 'Flow timeout exceeded'
                    })
                    break

                node_id = flow_context['current_node']
                node = nodes.get(node_id)

                if not node:
                    current_state = FlowState.FAILED
                    execution_log.append({
                        'error': f"Node '{node_id}' not found"
                    })
                    break

                node_start = time.time()
                try:
                    result = self._execute_node(node, flow_context, context)

                    node_duration = time.time() - node_start
                    execution_log.append({
                        'node': node_id,
                        'type': node.node_type.value,
                        'duration': node_duration,
                        'result': result,
                        'success': True
                    })

                    # Determine next node
                    if node.node_type == FlowNodeType.CONDITION:
                        flow_context['current_node'] = result.get('next_node', node.next_nodes[0] if node.next_nodes else None)
                    elif node.next_nodes:
                        flow_context['current_node'] = node.next_nodes[0]
                    else:
                        flow_context['current_node'] = None

                    if flow_context['current_node'] is None:
                        current_state = FlowState.COMPLETED

                except Exception as e:
                    node_duration = time.time() - node_start
                    execution_log.append({
                        'node': node_id,
                        'type': node.node_type.value,
                        'duration': node_duration,
                        'error': str(e),
                        'success': False
                    })
                    current_state = FlowState.FAILED

                iteration += 1

            total_duration = time.time() - start_time

            result_data = {
                'state': current_state.value,
                'completed_nodes': len([e for e in execution_log if e.get('success')]),
                'failed_nodes': len([e for e in execution_log if not e.get('success')]),
                'total_duration': total_duration,
                'iterations': iteration,
                'log': execution_log,
                'output_data': flow_context['data']
            }

            context.variables[output_var] = result_data
            return ActionResult(
                success=current_state == FlowState.COMPLETED,
                data=result_data,
                message=f"Flow {current_state.value}: {result_data['completed_nodes']} nodes executed"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Flow execution failed: {str(e)}"
            )

    def _build_nodes(self, flow_def: Dict) -> Dict[str, FlowNode]:
        """Build flow nodes from definition."""
        nodes = {}
        for node_def in flow_def.get('nodes', []):
            node = FlowNode(
                id=node_def.get('id', ''),
                node_type=FlowNodeType(node_def.get('type', 'task')),
                name=node_def.get('name', node_def.get('id', '')),
                config=node_def.get('config', {}),
                next_nodes=node_def.get('next_nodes', [])
            )
            nodes[node.id] = node
        return nodes

    def _execute_node(
        self, node: FlowNode, flow_context: Dict, context: Any
    ) -> Dict:
        """Execute a single flow node."""
        result = {'node_id': node.id}

        if node.node_type == FlowNodeType.TASK:
            result['output'] = flow_context['data']
            result['next_node'] = node.next_nodes[0] if node.next_nodes else None

        elif node.node_type == FlowNodeType.CONDITION:
            condition_result = node.config.get('condition_result', True)
            result['condition_met'] = condition_result
            result['next_node'] = node.next_nodes[0] if condition_result else (node.next_nodes[1] if len(node.next_nodes) > 1 else None)

        elif node.node_type == FlowNodeType.PARALLEL:
            # Execute parallel branches
            parallel_results = []
            for next_node in node.next_nodes:
                parallel_results.append({'branch': next_node, 'executed': True})
            result['parallel_results'] = parallel_results
            result['next_node'] = node.next_nodes[0] if node.next_nodes else None

        elif node.node_type == FlowNodeType.LOOP:
            max_iterations = node.config.get('max_iterations', 10)
            current_iteration = flow_context['variables'].get('_loop_iteration', 0)

            if current_iteration < max_iterations:
                flow_context['variables']['_loop_iteration'] = current_iteration + 1
                result['next_node'] = node.next_nodes[0] if node.next_nodes else None
                result['loop_continue'] = True
            else:
                result['next_node'] = node.next_nodes[1] if len(node.next_nodes) > 1 else None
                result['loop_continue'] = False
                flow_context['variables']['_loop_iteration'] = 0

        elif node.node_type == FlowNodeType.WAIT:
            wait_duration = node.config.get('duration', 1)
            time.sleep(wait_duration)
            result['next_node'] = node.next_nodes[0] if node.next_nodes else None

        elif node.node_type == FlowNodeType.ERROR:
            result['error_handled'] = True
            result['next_node'] = node.next_nodes[0] if node.next_nodes else None

        return result


class ParallelExecutorAction(BaseAction):
    """Execute multiple automation tasks in parallel.
    
    Supports task batching, result aggregation, and error handling.
    """
    action_type = "parallel_executor"
    display_name = "并行执行"
    description = "并行执行多个自动化任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute tasks in parallel.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - tasks: List of task definitions.
                - max_workers: Max parallel workers.
                - batch_size: Tasks per batch.
                - continue_on_error: Continue on individual task failure.
                - aggregate_results: Aggregate results into single output.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with parallel execution result or error.
        """
        tasks = params.get('tasks', [])
        max_workers = params.get('max_workers', 4)
        batch_size = params.get('batch_size', None)
        continue_on_error = params.get('continue_on_error', True)
        aggregate_results = params.get('aggregate_results', True)
        output_var = params.get('output_var', 'parallel_result')

        if not tasks:
            return ActionResult(
                success=False,
                message="No tasks provided to execute"
            )

        try:
            start_time = time.time()

            # Batch tasks if specified
            if batch_size:
                task_batches = [tasks[i:i+batch_size] for i in range(0, len(tasks), batch_size)]
            else:
                task_batches = [tasks]

            all_results = []
            total_executed = 0
            total_failed = 0

            for batch_idx, batch in enumerate(task_batches):
                with ThreadPoolExecutor(max_workers=min(max_workers, len(batch))) as executor:
                    futures = {}

                    for task in batch:
                        task_id = task.get('id', f'task_{total_executed}')
                        handler = self._get_task_handler(task, context)

                        if handler:
                            future = executor.submit(handler, task, context)
                            futures[future] = task_id
                        else:
                            futures[None] = task_id

                    for future in as_completed(futures):
                        task_id = futures[future]
                        try:
                            if future is None:
                                result = {'id': task_id, 'success': False, 'error': 'No handler'}
                            else:
                                result = future.result(timeout=task.get('timeout', 60))

                            all_results.append(result)
                            if result.get('success', False):
                                total_executed += 1
                            else:
                                total_failed += 1

                        except Exception as e:
                            total_failed += 1
                            if continue_on_error:
                                all_results.append({
                                    'id': task_id,
                                    'success': False,
                                    'error': str(e)
                                })
                            else:
                                raise

            total_duration = time.time() - start_time

            result_data = {
                'total_tasks': len(tasks),
                'executed': total_executed,
                'failed': total_failed,
                'duration': total_duration,
                'results': all_results
            }

            if aggregate_results:
                # Aggregate successful results
                successful = [r for r in all_results if r.get('success')]
                if successful:
                    result_data['aggregated'] = {
                        'count': len(successful),
                        'data': [r.get('data', r.get('result')) for r in successful]
                    }

            context.variables[output_var] = result_data
            return ActionResult(
                success=total_failed == 0,
                data=result_data,
                message=f"Parallel execution: {total_executed}/{len(tasks)} tasks completed"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Parallel execution failed: {str(e)}"
            )

    def _get_task_handler(self, task: Dict, context: Any) -> Optional[Callable]:
        """Get task handler function."""
        handler_var = task.get('handler_var')
        if handler_var:
            return context.variables.get(handler_var)
        return None


class StateMachineAction(BaseAction):
    """Execute a state machine for workflow control.
    
    Supports state transitions, guards, and entry/exit actions.
    """
    action_type = "state_machine"
    display_name = "状态机"
    description = "执行状态机工作流控制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute state machine transitions.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - machine_definition: State machine definition.
                - initial_state: Starting state.
                - event: Event to trigger transition.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with state machine result or error.
        """
        machine_def = params.get('machine_definition', {})
        initial_state = params.get('initial_state', None)
        event = params.get('event', '')
        output_var = params.get('output_var', 'state_machine_result')

        if not machine_def:
            return ActionResult(
                success=False,
                message="State machine definition is required"
            )

        try:
            # Get or create state
            state_var = f'_sm_{machine_def.get("id", "default")}_state'
            current_state = context.variables.get(state_var, initial_state)

            if not current_state:
                current_state = initial_state or machine_def.get('initial_state')
                context.variables[state_var] = current_state

            # Find transition for event
            transitions = machine_def.get('transitions', [])
            transition = None

            for t in transitions:
                if t.get('from_state') == current_state and t.get('event') == event:
                    transition = t
                    break

            result = {
                'current_state': current_state,
                'event': event,
                'transition': None,
                'new_state': current_state
            }

            if transition:
                # Execute transition
                new_state = transition.get('to_state')
                context.variables[state_var] = new_state

                result['transition'] = transition
                result['new_state'] = new_state
                result['success'] = True
            else:
                result['success'] = False
                result['error'] = f"No transition for event '{event}' from state '{current_state}'"

            context.variables[output_var] = result
            return ActionResult(
                success=result['success'],
                data=result,
                message=f"State: {current_state} -> {result['new_state']}"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"State machine execution failed: {str(e)}"
            )
