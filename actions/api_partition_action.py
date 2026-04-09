"""API partition action module for RabAI AutoClick.

Provides API request partitioning:
- ApiPartitionAction: Partition large API requests
- ApiPartitionByKeyAction: Partition by key/range
- ApiPartitionBySizeAction: Partition by size limits
- ApiPartitionMergeAction: Merge partitioned results
"""

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ApiPartitionAction(BaseAction):
    """Partition large API requests into smaller chunks."""
    action_type = "api_partition"
    display_name = "API分区"
    description = "将大型API请求分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "partition")
            data = params.get("data", [])
            partition_by = params.get("partition_by", "count")
            partition_size = params.get("partition_size", 100)
            partition_count = params.get("partition_count", 4)
            key = params.get("key")

            if operation == "partition":
                if not data:
                    return ActionResult(success=False, message="data is required")

                if partition_by == "count":
                    partitions = self._partition_by_count(data, partition_size)
                elif partition_by == "number":
                    partitions = self._partition_by_count(data, (len(data) + partition_count - 1) // partition_count)
                elif partition_by == "key" and key:
                    partitions = self._partition_by_key(data, key, partition_count)
                else:
                    partitions = self._partition_by_count(data, partition_size)

                return ActionResult(
                    success=True,
                    message=f"Partitioned into {len(partitions)} parts",
                    data={"partitions": partitions, "partition_count": len(partitions), "total_items": len(data)}
                )

            elif operation == "execute":
                url = params.get("url")
                method = params.get("method", "POST")
                partitions = params.get("partitions", [])
                parallel = params.get("parallel", False)

                if not url or not partitions:
                    return ActionResult(success=False, message="url and partitions required")

                results = []
                if parallel:
                    with ThreadPoolExecutor(max_workers=min(len(partitions), 5)) as executor:
                        futures = {executor.submit(self._execute_partition, url, method, p): i for i, p in enumerate(partitions)}
                        for future in as_completed(futures):
                            results.append(future.result())
                else:
                    for p in partitions:
                        results.append(self._execute_partition(url, method, p))

                return ActionResult(
                    success=True,
                    message=f"Executed {len(results)} partitions",
                    data={"results": results, "partition_count": len(partitions)}
                )

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"API partition error: {e}")

    def _partition_by_count(self, data: List, size: int) -> List[List]:
        """Partition by item count."""
        return [data[i:i + size] for i in range(0, len(data), size)]

    def _partition_by_key(self, data: List[Dict], key: str, num_partitions: int) -> List[List]:
        """Partition by key hash."""
        buckets = [[] for _ in range(num_partitions)]
        for item in data:
            if isinstance(item, dict) and key in item:
                hash_val = hash(str(item[key])) % num_partitions
                buckets[hash_val].append(item)
            else:
                buckets[0].append(item)
        return [b for b in buckets if b]

    def _execute_partition(self, url: str, method: str, partition: Any) -> Dict[str, Any]:
        """Execute a single partition."""
        try:
            import urllib.request
            import json as json_module

            data = json_module.dumps(partition).encode() if isinstance(partition, (dict, list)) else str(partition).encode()
            req = urllib.request.Request(url, method=method, data=data)
            req.add_header("Content-Type", "application/json")

            with urllib.request.urlopen(req, timeout=60) as response:
                content = response.read().decode()
                return {"success": True, "content": content, "status": response.status, "size": len(partition)}
        except Exception as e:
            return {"success": False, "error": str(e), "size": len(partition) if hasattr(partition, "__len__") else 0}


class ApiPartitionByKeyAction(BaseAction):
    """Partition API requests by key/range."""
    action_type = "api_partition_by_key"
    display_name = "API按键分区"
    description = "按键或范围分区API请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            partition_key = params.get("partition_key")
            ranges = params.get("ranges", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            if partition_key and not ranges:
                keys = set()
                for item in data:
                    if isinstance(item, dict) and partition_key in item:
                        keys.add(item[partition_key])

                key_partitions = {}
                for key in keys:
                    key_partitions[str(key)] = [item for item in data if isinstance(item, dict) and item.get(partition_key) == key]

                return ActionResult(
                    success=True,
                    message=f"Partitioned by key '{partition_key}': {len(key_partitions)} partitions",
                    data={"partitions": key_partitions, "partition_count": len(key_partitions), "keys": list(keys)}
                )

            elif ranges:
                range_partitions = []
                for r in ranges:
                    min_val = r.get("min")
                    max_val = r.get("max")
                    partition = [item for item in data if isinstance(item, dict) and partition_key in item and min_val <= item.get(partition_key) <= max_val]
                    range_partitions.append({"range": r, "items": partition, "count": len(partition)})

                return ActionResult(
                    success=True,
                    message=f"Partitioned into {len(range_partitions)} range partitions",
                    data={"partitions": range_partitions, "partition_count": len(range_partitions)}
                )

            return ActionResult(success=False, message="partition_key or ranges required")
        except Exception as e:
            return ActionResult(success=False, message=f"Partition by key error: {e}")


class ApiPartitionBySizeAction(BaseAction):
    """Partition by size limits."""
    action_type = "api_partition_by_size"
    display_name = "API按大小分区"
    description = "按大小限制分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            max_partition_size = params.get("max_partition_size", 1000)
            max_partition_bytes = params.get("max_partition_bytes", 1024 * 1024)
            size_by = params.get("size_by", "items")

            if not data:
                return ActionResult(success=False, message="data is required")

            partitions = []
            current_partition = []
            current_size = 0

            for item in data:
                item_size = self._get_item_size(item, size_by)

                if len(current_partition) >= max_partition_size or current_size + item_size > max_partition_bytes:
                    if current_partition:
                        partitions.append(current_partition)
                    current_partition = []
                    current_size = 0

                current_partition.append(item)
                current_size += item_size

            if current_partition:
                partitions.append(current_partition)

            partition_info = [
                {"index": i, "count": len(p), "bytes": sum(self._get_item_size(item, size_by) for item in p)}
                for i, p in enumerate(partitions)
            ]

            return ActionResult(
                success=True,
                message=f"Partitioned into {len(partitions)} parts by size ({size_by})",
                data={"partitions": partitions, "partition_info": partition_info, "total_items": len(data)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Partition by size error: {e}")

    def _get_item_size(self, item: Any, size_by: str) -> int:
        """Get size of an item."""
        if size_by == "items":
            return 1
        elif size_by == "json":
            import json
            return len(json.dumps(item))
        elif size_by == "str":
            return len(str(item))
        return 1


class ApiPartitionMergeAction(BaseAction):
    """Merge partitioned API results."""
    action_type = "api_partition_merge"
    display_name = "API分区合并"
    description = "合并分区API结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            partition_results = params.get("partition_results", [])
            merge_strategy = params.get("merge_strategy", "concat")
            deduplicate = params.get("deduplicate", False)
            dedupe_key = params.get("dedupe_key")

            if not partition_results:
                return ActionResult(success=False, message="partition_results required")

            merged = []

            for result in partition_results:
                if isinstance(result, dict):
                    if "data" in result:
                        merged.append(result["data"])
                    elif "content" in result:
                        try:
                            import json
                            merged.append(json.loads(result["content"]))
                        except Exception:
                            merged.append(result["content"])
                elif isinstance(result, (list, tuple)):
                    merged.extend(result)
                else:
                    merged.append(result)

            if merge_strategy == "concat":
                final = self._flatten(merged)
            elif merge_strategy == "union":
                final = self._union(merged)
            else:
                final = self._flatten(merged)

            if deduplicate and dedupe_key:
                final = self._deduplicate_by_key(final, dedupe_key)
            elif deduplicate:
                final = list(dict.fromkeys(final))

            return ActionResult(
                success=True,
                message=f"Merged {len(partition_results)} results into {len(final)} items",
                data={"merged": final, "merged_count": len(final), "source_count": len(partition_results)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Merge error: {e}")

    def _flatten(self, data: List) -> List:
        """Flatten nested lists."""
        result = []
        for item in data:
            if isinstance(item, list):
                result.extend(self._flatten(item))
            else:
                result.append(item)
        return result

    def _union(self, data: List) -> List:
        """Union of lists/dicts."""
        if not data:
            return []
        if isinstance(data[0], dict):
            seen = set()
            result = []
            for item in data:
                key = self._dict_key(item)
                if key not in seen:
                    seen.add(key)
                    result.append(item)
            return result
        return list(dict.fromkeys(self._flatten(data)))

    def _dict_key(self, d: Dict) -> str:
        """Generate hashable key from dict."""
        import json
        return json.dumps(d, sort_keys=True, default=str)

    def _deduplicate_by_key(self, data: List[Dict], key: str) -> List[Dict]:
        """Deduplicate list of dicts by key."""
        seen = set()
        result = []
        for item in data:
            if isinstance(item, dict) and key in item:
                item_key = item[key]
                if item_key not in seen:
                    seen.add(item_key)
                    result.append(item)
            elif isinstance(item, dict):
                result.append(item)
        return result
