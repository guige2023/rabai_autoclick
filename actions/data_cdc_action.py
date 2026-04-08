"""Data CDC (Change Data Capture) action module for RabAI AutoClick.

Captures and processes data changes for incremental
updates and event-driven pipelines.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataCDCAction(BaseAction):
    """Capture change data from sources.

    Tracks changes using timestamps, triggers, or
    comparison with previous snapshots.
    """
    action_type = "data_cdc"
    display_name = "变更数据捕获"
    description = "捕获数据源中的变更"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Capture changes.

        Args:
            context: Execution context.
            params: Dict with keys: current_data, previous_data,
                   key_field, capture_mode (full/incremental/triggered).

        Returns:
            ActionResult with captured changes.
        """
        start_time = time.time()
        try:
            current_data = params.get('current_data', [])
            previous_data = params.get('previous_data', [])
            key_field = params.get('key_field', 'id')
            capture_mode = params.get('capture_mode', 'incremental')

            if not isinstance(current_data, list):
                current_data = [current_data]
            if not isinstance(previous_data, list):
                previous_data = previous_data if previous_data else []

            prev_index = {item.get(key_field): item for item in previous_data if isinstance(item, dict)}

            created = []
            updated = []
            deleted = []
            unchanged = []

            for item in current_data:
                if not isinstance(item, dict):
                    continue
                key = item.get(key_field)
                prev_item = prev_index.get(key)

                if prev_item is None:
                    created.append({'type': 'insert', 'data': item})
                elif self._is_changed(prev_item, item):
                    updated.append({'type': 'update', 'data': item, 'previous': prev_item})
                else:
                    unchanged.append(item)

            # Detect deletions (in prev but not in current)
            if capture_mode != 'triggered':
                current_keys = {item.get(key_field) for item in current_data if isinstance(item, dict)}
                for key, prev_item in prev_index.items():
                    if key not in current_keys:
                        deleted.append({'type': 'delete', 'data': prev_item})

            changes = {
                'created': created,
                'updated': updated,
                'deleted': deleted,
                'unchanged_count': len(unchanged),
                'total_changes': len(created) + len(updated) + len(deleted),
            }

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"CDC: {len(created)} inserts, {len(updated)} updates, {len(deleted)} deletes",
                data={
                    'changes': changes,
                    'summary': {
                        'inserts': len(created),
                        'updates': len(updated),
                        'deletes': len(deleted),
                        'unchanged': len(unchanged),
                    },
                    'capture_mode': capture_mode,
                    'captured_at': datetime.now(timezone.utc).isoformat(),
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"CDC error: {str(e)}",
                duration=duration,
            )

    def _is_changed(self, prev: Dict, current: Dict) -> bool:
        """Check if item has changed."""
        ignore_fields = {'updated_at', 'modified_at', 'sync_time', '_last_sync'}
        for key, value in current.items():
            if key in ignore_fields:
                continue
            if prev.get(key) != value:
                return True
        return False


class DataCDRFAction(BaseAction):
    """Process data change events in real-time.

    Handles CDC events (INSERT, UPDATE, DELETE) with
    routing and transformation.
    """
    action_type = "data_cdrf"
    display_name = "变更数据路由"
    description = "实时处理CDC事件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Process CDC events.

        Args:
            context: Execution context.
            params: Dict with keys: events, handlers,
                   default_handler.

        Returns:
            ActionResult with processing results.
        """
        start_time = time.time()
        try:
            events = params.get('events', [])
            handlers = params.get('handlers', {})
            default_handler = params.get('default_handler')

            if not events:
                return ActionResult(
                    success=False,
                    message="No events to process",
                    duration=time.time() - start_time,
                )

            results = []
            for event in events:
                event_type = event.get('type', 'unknown')
                data = event.get('data', {})
                key = event.get('key')

                handler = handlers.get(event_type, default_handler)
                if handler and callable(handler):
                    try:
                        result = handler(event, context)
                        results.append({
                            'event_type': event_type,
                            'key': key,
                            'success': True,
                            'result': result,
                        })
                    except Exception as e:
                        results.append({
                            'event_type': event_type,
                            'key': key,
                            'success': False,
                            'error': str(e),
                        })
                else:
                    results.append({
                        'event_type': event_type,
                        'key': key,
                        'success': False,
                        'error': 'No handler',
                    })

            success_count = sum(1 for r in results if r.get('success', False))
            duration = time.time() - start_time
            return ActionResult(
                success=success_count == len(events),
                message=f"CDR: Processed {len(events)} events ({success_count} successful)",
                data={
                    'results': results,
                    'total': len(events),
                    'successful': success_count,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"CDR error: {str(e)}",
                duration=duration,
            )


class DataSnapshotAction(BaseAction):
    """Create and manage data snapshots for CDC.

    Captures point-in-time snapshots for comparison
    and rollback.
    """
    action_type = "data_snapshot"
    display_name = "数据快照"
    description = "创建和管理CDC数据快照"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage snapshots.

        Args:
            context: Execution context.
            params: Dict with keys: action (create/load/list/diff),
                   data, snapshot_id, snapshot_dir.

        Returns:
            ActionResult with snapshot data.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'create')
            data = params.get('data')
            snapshot_id = params.get('snapshot_id', datetime.now().strftime('%Y%m%d_%H%M%S'))
            snapshot_dir = params.get('snapshot_dir', '/tmp/snapshots')

            os.makedirs(snapshot_dir, exist_ok=True)
            snapshot_path = os.path.join(snapshot_dir, f"snapshot_{snapshot_id}.json")

            if action == 'create':
                if data is None:
                    return ActionResult(
                        success=False,
                        message="Data is required for create",
                        duration=time.time() - start_time,
                    )
                snapshot = {
                    'snapshot_id': snapshot_id,
                    'created_at': datetime.now(timezone.utc).isoformat(),
                    'data': data,
                    'item_count': len(data) if isinstance(data, list) else 1,
                }
                with open(snapshot_path, 'w') as f:
                    json.dump(snapshot, f, indent=2, default=str)
                return ActionResult(
                    success=True,
                    message=f"Created snapshot {snapshot_id}",
                    data={'snapshot_id': snapshot_id, 'path': snapshot_path},
                    duration=time.time() - start_time,
                )

            elif action == 'load':
                if not os.path.exists(snapshot_path):
                    return ActionResult(
                        success=False,
                        message=f"Snapshot {snapshot_id} not found",
                        duration=time.time() - start_time,
                    )
                with open(snapshot_path, 'r') as f:
                    snapshot = json.load(f)
                return ActionResult(
                    success=True,
                    message=f"Loaded snapshot {snapshot_id}",
                    data=snapshot,
                    duration=time.time() - start_time,
                )

            elif action == 'list':
                snapshots = []
                if os.path.exists(snapshot_dir):
                    for fname in sorted(os.listdir(snapshot_dir)):
                        if fname.startswith('snapshot_') and fname.endswith('.json'):
                            fpath = os.path.join(snapshot_dir, fname)
                            try:
                                with open(fpath, 'r') as f:
                                    meta = json.load(f)
                                    snapshots.append({
                                        'id': meta.get('snapshot_id'),
                                        'created_at': meta.get('created_at'),
                                        'item_count': meta.get('item_count'),
                                        'path': fpath,
                                    })
                            except Exception:
                                pass
                return ActionResult(
                    success=True,
                    message=f"Found {len(snapshots)} snapshots",
                    data={'snapshots': snapshots},
                    duration=time.time() - start_time,
                )

            elif action == 'diff':
                other_id = params.get('other_snapshot_id')
                if not other_id:
                    return ActionResult(
                        success=False,
                        message="other_snapshot_id is required for diff",
                        duration=time.time() - start_time,
                    )
                other_path = os.path.join(snapshot_dir, f"snapshot_{other_id}.json")
                if not os.path.exists(snapshot_path) or not os.path.exists(other_path):
                    return ActionResult(
                        success=False,
                        message="One or both snapshots not found",
                        duration=time.time() - start_time,
                    )
                with open(snapshot_path, 'r') as f:
                    snap1 = json.load(f)
                with open(other_path, 'r') as f:
                    snap2 = json.load(f)
                # Compute diff
                data1 = snap1.get('data', [])
                data2 = snap2.get('data', [])
                if isinstance(data1, list) and isinstance(data2, list):
                    ids1 = {item.get('id'): item for item in data1 if isinstance(item, dict)}
                    ids2 = {item.get('id'): item for item in data2 if isinstance(item, dict)}
                    added = [ids2[k] for k in ids2 if k not in ids1]
                    removed = [ids1[k] for k in ids1 if k not in ids2]
                    common = [k for k in ids1 if k in ids2 and ids1[k] != ids2[k]]
                    return ActionResult(
                        success=True,
                        message=f"Diff: {len(added)} added, {len(removed)} removed, {len(common)} changed",
                        data={'added': added, 'removed': removed, 'changed': common},
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
                message=f"Snapshot error: {str(e)}",
                duration=duration,
            )
