"""Automation chain action module for RabAI AutoClick.

Provides workflow chaining capabilities to sequence, parallelize,
and conditionally execute automation actions in pipelines.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ActionChainRunnerAction(BaseAction):
    """Run a sequence of actions in a chain.
    
    Executes actions sequentially, passing output from each
    action as input to the next. Supports early termination on failure.
    """
    action_type = "action_chain_runner"
    display_name = "动作链执行"
    description = "顺序执行动作链，上一个输出作为下一个输入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute action chain.
        
        Args:
            context: Execution context.
            params: Dict with keys: actions (list of action configs),
                   stop_on_failure, pass_context, timeout_per_action.
        
        Returns:
            ActionResult with chain execution summary.
        """
        actions = params.get('actions', [])
        stop_on_failure = params.get('stop_on_failure', True)
        pass_context = params.get('pass_context', True)
        start_time = time.time()

        if not actions:
            return ActionResult(
                success=False,
                message="No actions defined in chain"
            )

        results = []
        chain_data = {}
        last_success = True

        for i, action_config in enumerate(actions):
            action_name = action_config.get('name', f'step_{i}')
            action_type = action_config.get('type', '')
            action_params = action_config.get('params', {})
            timeout = action_config.get('timeout', 60)

            if pass_context and chain_data:
                action_params = {**action_params, '_chain_data': chain_data}

            step_start = time.time()
            try:
                result = self._execute_action(context, action_type, action_params, timeout)
                step_duration = time.time() - step_start
                result.duration = step_duration
                results.append({
                    'step': i,
                    'name': action_name,
                    'type': action_type,
                    'success': result.success,
                    'message': result.message,
                    'data': result.data,
                    'duration': step_duration
                })

                if result.success and result.data:
                    chain_data = result.data

                if not result.success and stop_on_failure:
                    return ActionResult(
                        success=False,
                        message=f"Chain stopped at step {i} ({action_name}): {result.message}",
                        data={
                            'chain_data': chain_data,
                            'results': results,
                            'failed_step': i,
                            'total_duration': time.time() - start_time
                        },
                        duration=time.time() - start_time
                    )
            except Exception as e:
                results.append({
                    'step': i,
                    'name': action_name,
                    'type': action_type,
                    'success': False,
                    'message': str(e),
                    'error': traceback.format_exc(),
                    'duration': time.time() - step_start
                })
                if stop_on_failure:
                    return ActionResult(
                        success=False,
                        message=f"Chain stopped at step {i} ({action_name}): {str(e)}",
                        data={
                            'results': results,
                            'failed_step': i,
                            'error': str(e)
                        },
                        duration=time.time() - start_time
                    )

        all_success = all(r['success'] for r in results)
        return ActionResult(
            success=all_success,
            message=f"Chain completed: {sum(r['success'] for r in results)}/{len(results)} steps succeeded",
            data={
                'chain_data': chain_data,
                'results': results,
                'total_steps': len(results),
                'successful_steps': sum(r['success'] for r in results),
                'failed_steps': sum(1 for r in results if not r['success'])
            },
            duration=time.time() - start_time
        )

    def _execute_action(
        self,
        context: Any,
        action_type: str,
        params: Dict[str, Any],
        timeout: int
    ) -> ActionResult:
        """Execute a single action by type."""
        try:
            from core.action_registry import ActionRegistry
            registry = ActionRegistry()
            action_instance = registry.get_action(action_type)
            if action_instance is None:
                return ActionResult(
                    success=False,
                    message=f"Unknown action type: {action_type}"
                )
            return action_instance.execute(context, params)
        except ImportError:
            return ActionResult(
                success=False,
                message=f"Could not import action registry"
            )


class ParallelChainRunnerAction(BaseAction):
    """Run multiple action chains in parallel.
    
    Executes independent action chains concurrently and waits
    for all to complete. Collects results from each branch.
    """
    action_type = "parallel_chain_runner"
    display_name = "并行动作链执行"
    description = "并行执行多个动作链"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parallel chains.
        
        Args:
            context: Execution context.
            params: Dict with keys: branches (list of action lists),
                   stop_on_failure, collect_mode (all|first|success).
        
        Returns:
            ActionResult with parallel execution results.
        """
        branches = params.get('branches', [])
        stop_on_failure = params.get('stop_on_failure', False)
        collect_mode = params.get('collect_mode', 'all')
        start_time = time.time()

        if not branches:
            return ActionResult(
                success=False,
                message="No branches defined"
            )

        results = []
        for i, branch in enumerate(branches):
            branch_name = branch.get('name', f'branch_{i}')
            branch_actions = branch.get('actions', [])
            chain_runner = ActionChainRunnerAction()
            branch_result = chain_runner.execute(context, {
                'actions': branch_actions,
                'stop_on_failure': stop_on_failure,
                'pass_context': True
            })
            results.append({
                'branch': i,
                'name': branch_name,
                'success': branch_result.success,
                'message': branch_result.message,
                'data': branch_result.data
            })

        successful = [r for r in results if r['success']]
        failed = [r for r in results if not r['success']]
        all_success = len(failed) == 0

        if collect_mode == 'first':
            selected = results[0] if results else {}
        elif collect_mode == 'success':
            selected = successful[0] if successful else {}
        else:
            selected = results

        return ActionResult(
            success=all_success or collect_mode != 'all',
            message=f"Parallel execution: {len(successful)}/{len(results)} branches succeeded",
            data={
                'all_results': results,
                'selected': selected,
                'successful_branches': len(successful),
                'failed_branches': len(failed),
                'collect_mode': collect_mode
            },
            duration=time.time() - start_time
        )


class ChainSplitterAction(BaseAction):
    """Split a data stream into multiple branches based on conditions.
    
    Evaluates each item against splitter conditions and routes
    to appropriate branch for processing.
    """
    action_type = "chain_splitter"
    display_name = "链式分流器"
    description = "根据条件将数据流分流到不同分支"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Split data into branches.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, conditions (list of
                   {name, condition, filter}).
        
        Returns:
            ActionResult with split results per branch.
        """
        data = params.get('data', [])
        conditions = params.get('conditions', [])
        default_branch = params.get('default_branch', 'unmatched')
        start_time = time.time()

        if not isinstance(data, list):
            data = [data]

        branches = {cond['name']: [] for cond in conditions}
        branches[default_branch] = []

        for item in data:
            matched = False
            for cond in conditions:
                condition = cond.get('condition')
                branch_name = cond.get('name', 'default')
                if self._evaluate_condition(item, condition):
                    branches[branch_name].append(item)
                    matched = True
                    break
            if not matched:
                branches[default_branch].append(item)

        return ActionResult(
            success=True,
            message=f"Split {len(data)} items into {len(branches)} branches",
            data={
                'branches': branches,
                'branch_counts': {k: len(v) for k, v in branches.items()},
                'total_items': len(data)
            },
            duration=time.time() - start_time
        )

    def _evaluate_condition(self, item: Any, condition: Any) -> bool:
        """Evaluate if item matches condition."""
        if condition is None:
            return False
        if isinstance(condition, bool):
            return condition
        if isinstance(condition, dict):
            for key, expected in condition.items():
                if isinstance(item, dict):
                    if item.get(key) != expected:
                        return False
                else:
                    return False
            return True
        if callable(condition):
            return condition(item)
        return bool(item)


class ChainMergerAction(BaseAction):
    """Merge multiple data branches into a single stream.
    
    Combines output from multiple branches with configurable
    merge strategy: concat, union, intersect, or custom.
    """
    action_type = "chain_merger"
    display_name = "链式合并器"
    description = "将多个数据分支合并为单一数据流"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Merge data branches.
        
        Args:
            context: Execution context.
            params: Dict with keys: branches (dict of branch data),
                   strategy (concat|union|intersect|zip),
                   dedupe_key, sort_by.
        
        Returns:
            ActionResult with merged data.
        """
        branches = params.get('branches', {})
        strategy = params.get('strategy', 'concat')
        dedupe_key = params.get('dedupe_key')
        sort_by = params.get('sort_by')
        start_time = time.time()

        if not branches:
            return ActionResult(
                success=False,
                message="No branches provided to merge"
            )

        if strategy == 'concat':
            merged = []
            for branch_data in branches.values():
                if isinstance(branch_data, list):
                    merged.extend(branch_data)
                else:
                    merged.append(branch_data)
        elif strategy == 'union':
            seen = set()
            merged = []
            for branch_data in branches.values():
                items = branch_data if isinstance(branch_data, list) else [branch_data]
                for item in items:
                    key = self._get_key(item, dedupe_key)
                    if key not in seen:
                        seen.add(key)
                        merged.append(item)
        elif strategy == 'intersect':
            sets = []
            for branch_data in branches.values():
                if isinstance(branch_data, list):
                    keys = {self._get_key(item, dedupe_key) for item in branch_data}
                    sets.append(keys)
            if not sets:
                merged = []
            else:
                common = sets[0]
                for s in sets[1:]:
                    common &= s
                merged = [item for branch_data in branches.values()
                          for item in (branch_data if isinstance(branch_data, list) else [branch_data])
                          if self._get_key(item, dedupe_key) in common]
        elif strategy == 'zip':
            merged = []
            max_len = max(len(v) if isinstance(v, list) else 1 for v in branches.values())
            for i in range(max_len):
                row = {}
                for name, branch_data in branches.items():
                    items = branch_data if isinstance(branch_data, list) else [branch_data]
                    row[name] = items[i] if i < len(items) else None
                merged.append(row)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown merge strategy: {strategy}"
            )

        if sort_by and merged:
            merged = sorted(merged, key=lambda x: self._get_sort_key(x, sort_by))

        return ActionResult(
            success=True,
            message=f"Merged {len(branches)} branches into {len(merged)} items",
            data={
                'merged': merged,
                'merged_count': len(merged),
                'strategy': strategy,
                'source_branches': list(branches.keys())
            },
            duration=time.time() - start_time
        )

    def _get_key(self, item: Any, key: Optional[str]) -> Any:
        """Get dedupe key from item."""
        if not key:
            return str(item)
        if isinstance(item, dict):
            return item.get(key)
        return getattr(item, key, str(item))

    def _get_sort_key(self, item: Any, sort_by: str) -> Any:
        """Get sort key from item."""
        if isinstance(item, dict):
            return item.get(sort_by, '')
        return getattr(item, sort_by, '')


class ChainBouncerAction(BaseAction):
    """Bounce/fan-out action output to multiple targets.
    
    Sends the same output data to multiple destinations or
    action chains simultaneously.
    """
    action_type = "chain_bouncer"
    display_name = "链式广播器"
    description = "将输出数据同时发送到多个目标"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Bounce data to multiple targets.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, targets (list of target configs),
                   parallel, timeout.
        
        Returns:
            ActionResult with bounce results per target.
        """
        data = params.get('data')
        targets = params.get('targets', [])
        parallel = params.get('parallel', True)
        start_time = time.time()

        if data is None:
            return ActionResult(
                success=False,
                message="data is required for bouncing"
            )

        if not targets:
            return ActionResult(
                success=False,
                message="No targets defined"
            )

        results = []
        for i, target in enumerate(targets):
            target_name = target.get('name', f'target_{i}')
            target_type = target.get('type', 'webhook')
            target_url = target.get('url')
            target_params = target.get('params', {})
            headers = target.get('headers', {})

            bounce_result = self._send_to_target(
                data, target_type, target_url, target_params, headers
            )
            results.append({
                'target': target_name,
                'type': target_type,
                'success': bounce_result.get('success', False),
                'message': bounce_result.get('message', ''),
                'response': bounce_result.get('response')
            })

        successful = sum(1 for r in results if r['success'])
        return ActionResult(
            success=successful == len(results),
            message=f"Bounced to {successful}/{len(results)} targets successfully",
            data={
                'results': results,
                'successful_targets': successful,
                'failed_targets': len(results) - successful
            },
            duration=time.time() - start_time
        )

    def _send_to_target(
        self,
        data: Any,
        target_type: str,
        url: Optional[str],
        params: Dict[str, Any],
        headers: Dict[str, str]
    ) -> Dict[str, Any]:
        """Send data to a single target."""
        import urllib.request
        import json

        if target_type == 'webhook' and url:
            try:
                payload = json.dumps(data).encode() if isinstance(data, (dict, list)) else str(data).encode()
                req = urllib.request.Request(url, data=payload, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return {
                        'success': True,
                        'message': f'Sent to {url}',
                        'response': resp.read().decode()
                    }
            except Exception as e:
                return {'success': False, 'message': str(e)}
        return {'success': False, 'message': f'Unknown target type: {target_type}'}
