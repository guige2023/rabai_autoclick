"""Data Federation action module for RabAI AutoClick.

Federates queries across multiple data sources and
merges results.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataFederationAction(BaseAction):
    """Query multiple data sources and merge results.

    Executes queries in parallel across heterogeneous
    sources and combines outputs.
    """
    action_type = "data_federation"
    display_name = "数据联邦"
    description = "跨多个数据源联合查询并合并结果"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute federated query.

        Args:
            context: Execution context.
            params: Dict with keys: sources, query, merge_strategy,
                   timeout, parallel.

        Returns:
            ActionResult with federated results.
        """
        start_time = time.time()
        try:
            sources = params.get('sources', [])
            query = params.get('query', {})
            merge_strategy = params.get('merge_strategy', 'union')
            timeout = params.get('timeout', 60)
            parallel = params.get('parallel', True)

            if not sources:
                return ActionResult(
                    success=False,
                    message="At least one source is required",
                    duration=time.time() - start_time,
                )

            def query_source(source: Dict) -> Dict:
                source_name = source.get('name', source.get('url', 'unknown'))
                source_type = source.get('type', 'api')
                try:
                    if source_type == 'api':
                        from urllib.request import urlopen, Request
                        url = source.get('url', '')
                        headers = source.get('headers', {})
                        req = Request(url, headers=headers)
                        with urlopen(req, timeout=timeout) as resp:
                            data = json.loads(resp.read())
                            return {'source': source_name, 'success': True, 'data': data}
                    elif source_type == 'file':
                        import os
                        path = source.get('path', '')
                        with open(path, 'r') as f:
                            if path.endswith('.json'):
                                data = json.load(f)
                            else:
                                data = f.read()
                            return {'source': source_name, 'success': True, 'data': data}
                    elif source_type == 'memory':
                        data = source.get('data', [])
                        return {'source': source_name, 'success': True, 'data': data}
                    return {'source': source_name, 'success': False, 'error': f'Unknown type: {source_type}'}
                except Exception as e:
                    return {'source': source_name, 'success': False, 'error': str(e)}

            results = []
            if parallel:
                with ThreadPoolExecutor(max_workers=min(len(sources), 10)) as executor:
                    futures = [executor.submit(query_source, source) for source in sources]
                    for future in as_completed(futures):
                        results.append(future.result())
            else:
                for source in sources:
                    results.append(query_source(source))

            # Merge results
            merged = self._merge_results([r.get('data') for r in results if r.get('success')], merge_strategy)

            success_count = sum(1 for r in results if r.get('success', False))
            duration = time.time() - start_time

            return ActionResult(
                success=success_count == len(sources),
                message=f"Federated {success_count}/{len(sources)} sources",
                data={
                    'results': results,
                    'merged': merged,
                    'sources_queried': len(sources),
                    'successful_queries': success_count,
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Federation error: {str(e)}",
                duration=duration,
            )

    def _merge_results(self, data_list: List, strategy: str) -> Any:
        """Merge results from multiple sources."""
        if not data_list:
            return []

        # Filter out None
        data_list = [d for d in data_list if d is not None]

        if strategy == 'union':
            seen = set()
            result = []
            for data in data_list:
                if isinstance(data, list):
                    for item in data:
                        key = json.dumps(item, sort_keys=True) if isinstance(item, dict) else str(item)
                        if key not in seen:
                            seen.add(key)
                            result.append(item)
                elif isinstance(data, dict):
                    key = json.dumps(data, sort_keys=True)
                    if key not in seen:
                        seen.add(key)
                        result.append(data)
            return result

        elif strategy == 'intersection':
            if all(isinstance(d, list) for d in data_list):
                sets = [set(json.dumps(item, sort_keys=True) for item in d) for d in data_list]
                intersection = sets[0]
                for s in sets[1:]:
                    intersection &= s
                return [json.loads(k) for k in intersection]
            return data_list[0]

        elif strategy == 'first':
            return data_list[0]

        elif strategy == 'last':
            return data_list[-1]

        elif strategy == 'concat':
            result = []
            for data in data_list:
                if isinstance(data, list):
                    result.extend(data)
                else:
                    result.append(data)
            return result

        return data_list


class DataFederatedJoinAction(BaseAction):
    """Join data from multiple federated sources.

    Performs join operations across heterogeneous
    data sources.
    """
    action_type = "data_federated_join"
    display_name = "数据联邦连接"
    description = "跨多个数据源执行连接操作"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Join federated data.

        Args:
            context: Execution context.
            params: Dict with keys: left_source, right_source,
                   join_key, join_type (inner/left/right/full).

        Returns:
            ActionResult with joined data.
        """
        start_time = time.time()
        try:
            left_data = params.get('left_data', [])
            right_data = params.get('right_data', [])
            join_key = params.get('join_key', 'id')
            join_type = params.get('join_type', 'inner')

            if not left_data or not right_data:
                return ActionResult(
                    success=False,
                    message="Both left_data and right_data are required",
                    duration=time.time() - start_time,
                )

            left_index = {}
            for item in left_data:
                if isinstance(item, dict):
                    key = item.get(join_key)
                    left_index.setdefault(key, []).append(item)

            right_index = {}
            for item in right_data:
                if isinstance(item, dict):
                    key = item.get(join_key)
                    right_index.setdefault(key, []).append(item)

            joined = []
            all_keys = set(left_index.keys()) | set(right_index.keys())

            for key in all_keys:
                left_items = left_index.get(key, [])
                right_items = right_index.get(key, [])

                if join_type == 'inner':
                    if left_items and right_items:
                        for l in left_items:
                            for r in right_items:
                                merged = {**l, **r}
                                merged['_join_key'] = key
                                joined.append(merged)
                elif join_type == 'left':
                    for l in left_items:
                        if right_items:
                            for r in right_items:
                                merged = {**l, **r}
                                merged['_join_key'] = key
                                joined.append(merged)
                        else:
                            merged = {**l, '_right_null': True}
                            merged['_join_key'] = key
                            joined.append(merged)
                elif join_type == 'right':
                    for r in right_items:
                        if left_items:
                            for l in left_items:
                                merged = {**l, **r}
                                merged['_join_key'] = key
                                joined.append(merged)
                        else:
                            merged = {'_left_null': True, **r}
                            merged['_join_key'] = key
                            joined.append(merged)
                elif join_type == 'full':
                    if left_items and right_items:
                        for l in left_items:
                            for r in right_items:
                                merged = {**l, **r}
                                merged['_join_key'] = key
                                joined.append(merged)
                    elif left_items:
                        for l in left_items:
                            merged = {**l, '_right_null': True}
                            merged['_join_key'] = key
                            joined.append(merged)
                    elif right_items:
                        for r in right_items:
                            merged = {'_left_null': True, **r}
                            merged['_join_key'] = key
                            joined.append(merged)

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Joined {len(left_data)} x {len(right_data)} -> {len(joined)} rows",
                data={
                    'joined': joined,
                    'join_type': join_type,
                    'join_key': join_key,
                    'row_count': len(joined),
                },
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Join error: {str(e)}",
                duration=duration,
            )
