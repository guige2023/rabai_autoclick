"""Data partitioner action module for RabAI AutoClick.

Provides data partitioning:
- DataPartitionerAction: Partition data into segments
- HashPartitionerAction: Hash-based partitioning
- RangePartitionerAction: Range-based partitioning
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataPartitionerAction(BaseAction):
    """Partition data into segments."""
    action_type = "data_partitioner"
    display_name = "数据分区"
    description = "将数据分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            partition_count = params.get("partition_count", 3)
            partition_key = params.get("partition_key", None)

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            partitions = [[] for _ in range(partition_count)]

            for i, item in enumerate(data):
                if partition_key and isinstance(item, dict):
                    key_value = item.get(partition_key, str(item))
                    partition_idx = hash(str(key_value)) % partition_count
                else:
                    partition_idx = i % partition_count
                partitions[partition_idx].append(item)

            return ActionResult(
                success=True,
                data={
                    "partition_count": partition_count,
                    "partitions": [{"id": i, "size": len(p)} for i, p in enumerate(partitions)],
                    "total_items": len(data)
                },
                message=f"Data partitioned: {len(data)} items into {partition_count} partitions"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data partitioner error: {str(e)}")


class HashPartitionerAction(BaseAction):
    """Hash-based partitioning."""
    action_type = "hash_partitioner"
    display_name = "哈希分区"
    description = "基于哈希的分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            num_partitions = params.get("num_partitions", 4)
            hash_field = params.get("hash_field", "id")

            partitions = {i: [] for i in range(num_partitions)}

            for item in data:
                if isinstance(item, dict) and hash_field in item:
                    key = str(item[hash_field])
                else:
                    key = str(item)
                partition_id = hash(key) % num_partitions
                partitions[partition_id].append(item)

            return ActionResult(
                success=True,
                data={
                    "num_partitions": num_partitions,
                    "hash_field": hash_field,
                    "partition_sizes": {k: len(v) for k, v in partitions.items()}
                },
                message=f"Hash partitioned: {num_partitions} partitions by {hash_field}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Hash partitioner error: {str(e)}")


class RangePartitionerAction(BaseAction):
    """Range-based partitioning."""
    action_type = "range_partitioner"
    display_name = "范围分区"
    description = "基于范围的分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            ranges = params.get("ranges", [(0, 33), (33, 66), (66, 100)])
            value_field = params.get("value_field", "value")

            partitions = [[] for _ in ranges]

            for item in data:
                if isinstance(item, dict) and value_field in item:
                    value = item[value_field]
                else:
                    value = float(item) if str(item).isdigit() else 0

                assigned = False
                for i, (low, high) in enumerate(ranges):
                    if low <= value < high:
                        partitions[i].append(item)
                        assigned = True
                        break
                if not assigned:
                    partitions[-1].append(item)

            return ActionResult(
                success=True,
                data={
                    "ranges": ranges,
                    "value_field": value_field,
                    "partition_sizes": [len(p) for p in partitions]
                },
                message=f"Range partitioned: {len(data)} items into {len(ranges)} ranges"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Range partitioner error: {str(e)}")
