"""State manager action module for RabAI AutoClick.

Provides state management for workflows including
state persistence, snapshots, and rollback capabilities.
"""

import time
import json
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StateManagerAction(BaseAction):
    """Manage workflow execution state.
    
    Stores, retrieves, updates, and snapshots state
    with optional persistence to disk or database.
    """
    action_type = "state_manager"
    display_name = "状态管理器"
    description = "管理工作流执行状态"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage state.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (get|set|update|delete|clear),
                   key, value, namespace, persist.
        
        Returns:
            ActionResult with state operation result.
        """
        operation = params.get('operation', 'get')
        key = params.get('key', 'default')
        value = params.get('value')
        namespace = params.get('namespace', 'workflow')
        persist = params.get('persist', False)
        start_time = time.time()

        if not hasattr(context, '_state_manager'):
            context._state_manager = {}

        ns_key = f"{namespace}:{key}"
        state = context._state_manager

        if operation == 'get':
            val = state.get(ns_key)
            return ActionResult(
                success=True,
                message=f"State get: {ns_key}",
                data={'key': ns_key, 'value': val, 'exists': ns_key in state}
            )

        elif operation == 'set':
            state[ns_key] = value
            if persist:
                self._persist_state(namespace, key, value)
            return ActionResult(
                success=True,
                message=f"State set: {ns_key}",
                data={'key': ns_key, 'value': value}
            )

        elif operation == 'update':
            current = state.get(ns_key, {})
            if isinstance(current, dict) and isinstance(value, dict):
                current.update(value)
                state[ns_key] = current
            else:
                state[ns_key] = value
            return ActionResult(
                success=True,
                message=f"State updated: {ns_key}",
                data={'key': ns_key, 'value': state[ns_key]}
            )

        elif operation == 'delete':
            if ns_key in state:
                del state[ns_key]
            return ActionResult(
                success=True,
                message=f"State deleted: {ns_key}",
                data={'deleted': True}
            )

        elif operation == 'clear':
            if namespace:
                keys_to_remove = [k for k in state if k.startswith(f"{namespace}:")]
                for k in keys_to_remove:
                    del state[k]
            else:
                state.clear()
            return ActionResult(
                success=True,
                message="State cleared",
                data={'cleared': True}
            )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _persist_state(self, namespace: str, key: str, value: Any) -> None:
        """Persist state to disk."""
        import tempfile
        state_dir = os.path.join(tempfile.gettempdir(), 'rabai_state')
        os.makedirs(state_dir, exist_ok=True)
        file_path = os.path.join(state_dir, f"{namespace}_{key}.json")
        try:
            with open(file_path, 'w') as f:
                json.dump(value, f)
        except:
            pass


class StateSnapshotAction(BaseAction):
    """Create and manage state snapshots.
    
    Captures point-in-time snapshots of state for
    rollback and audit purposes.
    """
    action_type = "state_snapshot"
    display_name = "状态快照"
    description = "创建和管理状态快照"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage state snapshots.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (create|restore|list|delete),
                   snapshot_id, max_snapshots, namespace.
        
        Returns:
            ActionResult with snapshot operation result.
        """
        operation = params.get('operation', 'create')
        snapshot_id = params.get('snapshot_id', '')
        max_snapshots = params.get('max_snapshots', 10)
        namespace = params.get('namespace', 'workflow')
        start_time = time.time()

        if not hasattr(context, '_state_snapshots'):
            context._state_snapshots = {}

        snapshots = context._state_snapshots.get(namespace, [])

        if operation == 'create':
            if not hasattr(context, '_state_manager'):
                return ActionResult(success=False, message="No state manager found")

            import copy
            state = copy.deepcopy(context._state_manager)

            if not snapshot_id:
                snapshot_id = f"snapshot_{int(time.time() * 1000)}"

            snapshot = {
                'id': snapshot_id,
                'timestamp': time.time(),
                'state': state,
                'namespace': namespace
            }

            snapshots.append(snapshot)
            if len(snapshots) > max_snapshots:
                snapshots = snapshots[-max_snapshots:]
            context._state_snapshots[namespace] = snapshots

            return ActionResult(
                success=True,
                message=f"Snapshot created: {snapshot_id}",
                data={
                    'snapshot_id': snapshot_id,
                    'snapshot_count': len(snapshots)
                }
            )

        elif operation == 'restore':
            if not snapshot_id:
                return ActionResult(success=False, message="snapshot_id required")

            found = None
            for s in snapshots:
                if s['id'] == snapshot_id:
                    found = s
                    break

            if not found:
                return ActionResult(success=False, message=f"Snapshot not found: {snapshot_id}")

            if hasattr(context, '_state_manager'):
                import copy
                context._state_manager = copy.deepcopy(found['state'])

            return ActionResult(
                success=True,
                message=f"Restored snapshot: {snapshot_id}",
                data={'snapshot_id': snapshot_id}
            )

        elif operation == 'list':
            return ActionResult(
                success=True,
                message=f"{len(snapshots)} snapshots",
                data={
                    'snapshots': [
                        {'id': s['id'], 'timestamp': s['timestamp']}
                        for s in snapshots
                    ],
                    'count': len(snapshots)
                }
            )

        elif operation == 'delete':
            snapshots = [s for s in snapshots if s['id'] != snapshot_id]
            context._state_snapshots[namespace] = snapshots
            return ActionResult(
                success=True,
                message=f"Deleted snapshot: {snapshot_id}",
                data={'deleted': True}
            )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")


class WorkflowCheckpointAction(BaseAction):
    """Create checkpoints for resumable workflow execution.
    
    Saves execution progress at checkpoints for
    recovery after interruption.
    """
    action_type = "workflow_checkpoint"
    display_name = "工作流检查点"
    description = "创建可恢复工作流执行检查点"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create or restore checkpoint.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (save|load), workflow_id,
                   step_id, step_data, checkpoint_dir.
        
        Returns:
            ActionResult with checkpoint operation result.
        """
        operation = params.get('operation', 'save')
        workflow_id = params.get('workflow_id', 'default')
        step_id = params.get('step_id', '')
        step_data = params.get('step_data', {})
        checkpoint_dir = params.get('checkpoint_dir', '/tmp/rabai_checkpoints')
        start_time = time.time()

        checkpoint_file = os.path.join(checkpoint_dir, f"{workflow_id}.json")

        if operation == 'save':
            os.makedirs(checkpoint_dir, exist_ok=True)
            checkpoint = {
                'workflow_id': workflow_id,
                'last_step': step_id,
                'step_data': step_data,
                'timestamp': time.time()
            }
            try:
                with open(checkpoint_file, 'w') as f:
                    json.dump(checkpoint, f)
                return ActionResult(
                    success=True,
                    message=f"Checkpoint saved for workflow '{workflow_id}' at step '{step_id}'",
                    data={
                        'workflow_id': workflow_id,
                        'step_id': step_id,
                        'checkpoint_file': checkpoint_file
                    }
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Failed to save checkpoint: {str(e)}"
                )

        elif operation == 'load':
            if not os.path.exists(checkpoint_file):
                return ActionResult(
                    success=True,
                    message=f"No checkpoint found for workflow '{workflow_id}'",
                    data={'found': False, 'workflow_id': workflow_id}
                )
            try:
                with open(checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                return ActionResult(
                    success=True,
                    message=f"Checkpoint loaded for workflow '{workflow_id}'",
                    data={
                        'found': True,
                        'workflow_id': workflow_id,
                        'checkpoint': checkpoint
                    }
                )
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Failed to load checkpoint: {str(e)}"
                )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")
