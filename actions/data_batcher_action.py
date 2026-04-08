"""Data Batcher action module for RabAI AutoClick.

Batches data items with size limits, time windows,
and flush triggers.
"""

import time
import sys
import os
from typing import Any, Dict, List
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataBatcherAction(BaseAction):
    """Batch data items with configurable triggers.

    Accumulates items and flushes when batch_size,
    time_window, or manual flush is triggered.
    """
    action_type = "data_batcher"
    display_name = "数据批处理器"
    description = "基于大小和时间触发器批处理数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage data batcher.

        Args:
            context: Execution context.
            params: Dict with keys: action (add/flush/status),
                   batch_id, item, batch_size, time_window_seconds,
                   flush_fn.

        Returns:
            ActionResult with batch result.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'add')
            batch_id = params.get('batch_id', 'default')
            item = params.get('item')
            batch_size = params.get('batch_size', 100)
            time_window = params.get('time_window_seconds', 60)
            flush_fn = params.get('flush_fn')

            if not hasattr(context, '_data_batchers'):
                context._data_batchers = {}

            if batch_id not in context._data_batchers:
                context._data_batchers[batch_id] = {
                    'items': deque(),
                    'first_item_time': None,
                    'batch_size': batch_size,
                    'time_window': time_window,
                    'flush_count': 0,
                }

            b = context._data_batchers[batch_id]
            now = time.time()

            if action == 'add':
                if item is None:
                    return ActionResult(success=False, message="item required", duration=time.time() - start_time)
                if not b['items']:
                    b['first_item_time'] = now
                b['items'].append(item)

                should_flush = len(b['items']) >= b['batch_size']
                if b['first_item_time'] and (now - b['first_item_time']) >= b['time_window']:
                    should_flush = True

                if should_flush:
                    return self._flush_batch(b, batch_id, flush_fn, context, start_time)

                return ActionResult(success=True, message=f"Added to batch ({len(b['items'])}/{batch_size})", data={'batch_count': len(b['items']), 'flush_triggered': False}, duration=time.time() - start_time)

            elif action == 'flush':
                return self._flush_batch(b, batch_id, flush_fn, context, start_time)

            elif action == 'status':
                return ActionResult(success=True, message=f"Batch {batch_id}: {len(b['items'])} items", data={'batch_id': batch_id, 'items': len(b['items']), 'flush_count': b['flush_count']}, duration=time.time() - start_time)

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}", duration=time.time() - start_time)

        except Exception as e:
            return ActionResult(success=False, message=f"Batcher error: {str(e)}", duration=time.time() - start_time)

    def _flush_batch(self, b: Dict, batch_id: str, flush_fn: Any, context: Any, start_time: float) -> ActionResult:
        items = list(b['items'])
        b['items'].clear()
        b['first_item_time'] = None
        b['flush_count'] += 1

        flushed_data = items
        if callable(flush_fn):
            flushed_data = flush_fn(items, context)

        return ActionResult(
            success=True,
            message=f"Flushed batch {batch_id} ({len(items)} items)",
            data={'batch_id': batch_id, 'items': flushed_data, 'count': len(items), 'flush_count': b['flush_count']},
            duration=time.time() - start_time,
        )


class DataDedupeV2Action(BaseAction):
    """Advanced deduplication with multiple strategies.

    Deduplicates based on hash, bloom filter, or
    exact match with configurable windows.
    """
    action_type = "data_dedupe_v2"
    display_name = "数据去重V2"
    description = "基于哈希和布隆过滤的高级去重"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Deduplicate data.

        Args:
            context: Execution context.
            params: Dict with keys: items, key_field, strategy (hash/bloom/exact),
                   bloom_size, ttl_seconds.

        Returns:
            ActionResult with deduplicated items.
        """
        start_time = time.time()
        try:
            items = params.get('items', [])
            key_field = params.get('key_field', 'id')
            strategy = params.get('strategy', 'hash')
            bloom_size = params.get('bloom_size', 10000)
            ttl_seconds = params.get('ttl_seconds', 3600)

            dedupe_id = params.get('dedupe_id', 'default')
            if not hasattr(context, '_dedupe_filters'):
                context._dedupe_filters = {}
            if dedupe_id not in context._dedupe_filters:
                context._dedupe_filters[dedupe_id] = {
                    'seen': set(),
                    'bloom': [0] * bloom_size,
                    'strategy': strategy,
                    'created_at': time.time(),
                }

            df = context._dedupe_filters[dedupe_id]
            now = time.time()
            if now - df['created_at'] > ttl_seconds:
                df['seen'].clear()
                df['bloom'] = [0] * bloom_size
                df['created_at'] = now

            unique = []
            dupes = 0

            for item in items:
                if not isinstance(item, dict):
                    key = str(item)
                else:
                    key = str(item.get(key_field, ''))

                if strategy == 'exact':
                    if key in df['seen']:
                        dupes += 1
                        continue
                    df['seen'].add(key)
                elif strategy == 'hash':
                    import hashlib
                    h = hashlib.sha256(key.encode()).hexdigest()
                    if h in df['seen']:
                        dupes += 1
                        continue
                    df['seen'].add(h)
                elif strategy == 'bloom':
                    import hashlib
                    h1 = int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % bloom_size
                    h2 = int(hashlib.md5(key.encode()).hexdigest()[:8], 16) % bloom_size
                    if df['bloom'][h1] and df['bloom'][h2]:
                        dupes += 1
                        continue
                    df['bloom'][h1] = 1
                    df['bloom'][h2] = 1

                unique.append(item)

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Dedupe: {len(items)} -> {len(unique)} ({dupes} dupes removed)",
                data={'unique': unique, 'total': len(items), 'unique_count': len(unique), 'dupes': dupes},
                duration=duration,
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Dedupe error: {str(e)}", duration=time.time() - start_time)
