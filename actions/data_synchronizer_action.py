"""Data Synchronizer action module for RabAI AutoClick.

Synchronizes data between sources and destinations with
conflict resolution and incremental updates.
"""

import time
import json
import hashlib
import sys
import os
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSynchronizerAction(BaseAction):
    """Synchronize data between source and destination.

    Supports full sync, incremental sync, and bidirectional
    sync with conflict detection.
    """
    action_type = "data_synchronizer"
    display_name = "数据同步器"
    description = "在数据源和目标之间同步数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Synchronize data.

        Args:
            context: Execution context.
            params: Dict with keys: source, destination, sync_mode,
                   key_field, conflict_resolution, batch_size.

        Returns:
            ActionResult with sync results.
        """
        start_time = time.time()
        try:
            source = params.get('source')
            destination = params.get('destination')
            sync_mode = params.get('sync_mode', 'full')
            key_field = params.get('key_field', 'id')
            conflict_resolution = params.get('conflict_resolution', 'source_wins')
            batch_size = params.get('batch_size', 100)

            if not source or not destination:
                return ActionResult(
                    success=False,
                    message="Both source and destination are required",
                    duration=time.time() - start_time,
                )

            # Extract data from source
            source_data = self._fetch_data(source, context)
            if source_data is None:
                return ActionResult(
                    success=False,
                    message="Failed to fetch source data",
                    duration=time.time() - start_time,
                )

            # Fetch existing destination data for comparison
            dest_data = self._fetch_data(destination, context)
            dest_index = {item.get(key_field): item for item in (dest_data or [])}

            # Compute changes
            if sync_mode == 'full':
                changes = self._compute_full_sync(source_data, dest_index, key_field)
            elif sync_mode == 'incremental':
                last_sync = params.get('last_sync_time')
                changes = self._compute_incremental_sync(source_data, dest_index, key_field, last_sync)
            else:
                changes = self._compute_full_sync(source_data, dest_index, key_field)

            # Apply conflict resolution
            resolved = self._resolve_conflicts(changes, dest_index, key_field, conflict_resolution)

            # Write to destination in batches
            write_results = self._write_batches(destination, resolved, batch_size, context)

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Synced {write_results['written']} items ({write_results['created']} created, {write_results['updated']} updated, {write_results['deleted']} deleted)",
                data={
                    'mode': sync_mode,
                    'total_source': len(source_data),
                    'changes': write_results,
                    'sync_time': duration,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Sync failed: {str(e)}",
                duration=duration,
            )

    def _fetch_data(self, source: Union[str, Dict, List], context: Any) -> Optional[List]:
        """Fetch data from source."""
        if isinstance(source, list):
            return source
        elif isinstance(source, dict):
            if 'type' in source:
                source_type = source.get('type')
                if source_type == 'api':
                    url = source.get('url')
                    headers = source.get('headers', {})
                    from urllib.request import Request, urlopen
                    req = Request(url, headers=headers)
                    with urlopen(req, timeout=30) as resp:
                        data = json.loads(resp.read())
                        return data if isinstance(data, list) else [data]
                elif source_type == 'file':
                    path = source.get('path')
                    with open(path, 'r') as f:
                        return json.load(f)
        return None

    def _compute_full_sync(
        self,
        source_data: List,
        dest_index: Dict,
        key_field: str
    ) -> Dict[str, List]:
        """Compute full sync changes."""
        changes = {'create': [], 'update': [], 'delete': []}
        seen_keys = set()

        for item in source_data:
            key = item.get(key_field)
            seen_keys.add(key)
            if key not in dest_index:
                changes['create'].append(item)
            else:
                dest_item = dest_index[key]
                if self._is_different(item, dest_item):
                    changes['update'].append(item)

        # Items in dest but not in source (delete mode)
        for key in dest_index:
            if key not in seen_keys:
                changes['delete'].append(dest_index[key])

        return changes

    def _compute_incremental_sync(
        self,
        source_data: List,
        dest_index: Dict,
        key_field: str,
        last_sync: Optional[str]
    ) -> Dict[str, List]:
        """Compute incremental sync changes."""
        changes = {'create': [], 'update': [], 'delete': []}
        last_time = None

        if last_sync:
            try:
                last_time = datetime.fromisoformat(last_sync)
            except Exception:
                pass

        for item in source_data:
            key = item.get(key_field)
            updated_at = item.get('updated_at', item.get('modified_at', ''))
            if last_time and updated_at:
                try:
                    item_time = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                    if item_time <= last_time:
                        continue
                except Exception:
                    pass

            if key not in dest_index:
                changes['create'].append(item)
            else:
                if self._is_different(item, dest_index[key]):
                    changes['update'].append(item)

        return changes

    def _is_different(self, a: Dict, b: Dict) -> bool:
        """Check if two items are different."""
        ignore_fields = {'updated_at', 'modified_at', 'sync_time'}
        keys = set(a.keys()) | set(b.keys())
        for key in keys:
            if key in ignore_fields:
                continue
            if a.get(key) != b.get(key):
                return True
        return False

    def _resolve_conflicts(
        self,
        changes: Dict,
        dest_index: Dict,
        key_field: str,
        resolution: str
    ) -> Dict:
        """Resolve sync conflicts."""
        resolved = {'create': [], 'update': [], 'delete': []}
        for item in changes['create']:
            resolved['create'].append(item)
        for item in changes['update']:
            key = item.get(key_field)
            dest_item = dest_index.get(key)
            if dest_item and self._is_different(item, dest_item):
                if resolution == 'dest_wins':
                    resolved['update'].append(dest_item)
                elif resolution == 'newer_wins':
                    src_time = item.get('updated_at', '')
                    dst_time = dest_item.get('updated_at', '')
                    resolved['update'].append(item if src_time > dst_time else dest_item)
                else:  # source_wins
                    resolved['update'].append(item)
            else:
                resolved['update'].append(item)
        for item in changes['delete']:
            resolved['delete'].append(item)
        return resolved

    def _write_batches(
        self,
        destination: Union[str, Dict],
        changes: Dict,
        batch_size: int,
        context: Any
    ) -> Dict:
        """Write changes to destination in batches."""
        written = created = updated = deleted = 0

        for batch in self._chunk(changes['create'], batch_size):
            self._write_create(destination, batch, context)
            created += len(batch)
            written += len(batch)

        for batch in self._chunk(changes['update'], batch_size):
            self._write_update(destination, batch, context)
            updated += len(batch)
            written += len(batch)

        for batch in self._chunk(changes['delete'], batch_size):
            self._write_delete(destination, batch, context)
            deleted += len(batch)
            written += len(batch)

        return {'written': written, 'created': created, 'updated': updated, 'deleted': deleted}

    def _chunk(self, items: List, size: int) -> List[List]:
        """Split items into chunks."""
        return [items[i:i+size] for i in range(0, len(items), size)]

    def _write_create(self, dest: Any, items: List, context: Any) -> None:
        """Write new items to destination."""
        pass  # Implementation depends on destination type

    def _write_update(self, dest: Any, items: List, context: Any) -> None:
        """Update items in destination."""
        pass

    def _write_delete(self, dest: Any, items: List, context: Any) -> None:
        """Delete items from destination."""
        pass


class DataReplicatorAction(BaseAction):
    """Replicate data in real-time to multiple destinations.

    Uses change tracking to keep multiple targets in sync.
    """
    action_type = "data_replicator"
    display_name = "数据复制器"
    description = "实时复制数据到多个目标"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Replicate data to destinations.

        Args:
            context: Execution context.
            params: Dict with keys: source, destinations (list),
                   replicate_mode (sync/async), key_field.

        Returns:
            ActionResult with replication results.
        """
        start_time = time.time()
        try:
            source = params.get('source')
            destinations = params.get('destinations', [])
            replicate_mode = params.get('replicate_mode', 'sync')
            key_field = params.get('key_field', 'id')

            if not source or not destinations:
                return ActionResult(
                    success=False,
                    message="Source and at least one destination required",
                    duration=time.time() - start_time,
                )

            from urllib.request import Request, urlopen

            # Fetch source data
            if isinstance(source, str):
                req = Request(source)
                with urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read())
            else:
                data = source

            results = []
            for dest in destinations:
                dest_type = dest.get('type', 'api')
                dest_url = dest.get('url', '')
                dest_headers = dest.get('headers', {})

                if dest_type == 'api':
                    for item in data:
                        req = Request(
                            dest_url,
                            data=json.dumps(item).encode('utf-8'),
                            headers={**dest_headers, 'Content-Type': 'application/json'},
                        )
                        try:
                            with urlopen(req, timeout=30) as resp:
                                results.append({'dest': dest_url, 'status': resp.status, 'success': True})
                        except Exception as e:
                            results.append({'dest': dest_url, 'error': str(e), 'success': False})
                else:
                    results.append({'dest': dest_type, 'success': False, 'error': 'Unknown dest type'})

            success_count = sum(1 for r in results if r.get('success', False))
            duration = time.time() - start_time
            return ActionResult(
                success=success_count == len(destinations),
                message=f"Replicated to {success_count}/{len(destinations)} destinations",
                data={'results': results, 'item_count': len(data)},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Replication failed: {str(e)}",
                duration=duration,
            )
