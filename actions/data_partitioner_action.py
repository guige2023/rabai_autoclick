"""Data partitioner action module for RabAI AutoClick.

Provides data partitioning operations:
- PartitionByFieldAction: Partition by field values
- PartitionBySizeAction: Partition by size
- PartitionByRangeAction: Partition by numeric range
- PartitionByHashAction: Hash-based partitioning
- PartitionBalancedAction: Balanced partitioning
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PartitionByFieldAction(BaseAction):
    """Partition data by field values."""
    action_type = "partition_by_field"
    display_name = "按字段分区"
    description = "按字段值分区数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "category")

            if not data:
                return ActionResult(success=False, message="data is required")

            partitions: Dict[str, List] = {}
            for item in data:
                key = str(item.get(field, "unknown"))
                if key not in partitions:
                    partitions[key] = []
                partitions[key].append(item)

            return ActionResult(
                success=True,
                data={"partitions": partitions, "partition_count": len(partitions)},
                message=f"Partitioned {len(data)} items into {len(partitions)} partitions by {field}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Partition by field failed: {e}")


class PartitionBySizeAction(BaseAction):
    """Partition data by size."""
    action_type = "partition_by_size"
    display_name = "按大小分区"
    description = "按大小分区数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            partition_size = params.get("partition_size", 100)

            if not data:
                return ActionResult(success=False, message="data is required")

            partitions = []
            for i in range(0, len(data), partition_size):
                partitions.append(data[i : i + partition_size])

            return ActionResult(
                success=True,
                data={"partitions": partitions, "partition_count": len(partitions), "partition_size": partition_size},
                message=f"Partitioned {len(data)} items into {len(partitions)} partitions of ~{partition_size}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Partition by size failed: {e}")


class PartitionByRangeAction(BaseAction):
    """Partition by numeric range."""
    action_type = "partition_by_range"
    display_name = "按范围分区"
    description = "按数值范围分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            ranges = params.get("ranges", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            partitions = [{"range": r, "items": []} for r in ranges]
            for item in data:
                val = item.get(field, 0)
                for i, r in enumerate(ranges):
                    if r.get("min", float("-inf")) <= val < r.get("max", float("inf")):
                        partitions[i]["items"].append(item)
                        break

            return ActionResult(
                success=True,
                data={"partitions": partitions, "partition_count": len(partitions)},
                message=f"Partitioned {len(data)} items into {len(ranges)} ranges",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Partition by range failed: {e}")


class PartitionByHashAction(BaseAction):
    """Hash-based partitioning."""
    action_type = "partition_by_hash"
    display_name = "哈希分区"
    description = "基于哈希的分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key_field = params.get("key_field", "id")
            num_partitions = params.get("num_partitions", 4)

            if not data:
                return ActionResult(success=False, message="data is required")

            partitions: List[List] = [[] for _ in range(num_partitions)]
            for item in data:
                key = str(item.get(key_field, ""))
                hash_val = hash(key) % num_partitions
                partitions[hash_val].append(item)

            return ActionResult(
                success=True,
                data={"partitions": partitions, "partition_count": num_partitions},
                message=f"Hash partitioned {len(data)} items into {num_partitions} partitions",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Partition by hash failed: {e}")


class PartitionBalancedAction(BaseAction):
    """Balanced partitioning."""
    action_type = "partition_balanced"
    display_name = "均衡分区"
    description = "均衡分区数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            num_partitions = params.get("num_partitions", 4)
            balance_field = params.get("balance_field", "weight")

            if not data:
                return ActionResult(success=False, message="data is required")

            sorted_data = sorted(data, key=lambda x: x.get(balance_field, 0), reverse=True)
            partitions: List[List] = [[] for _ in range(num_partitions)]
            partition_weights = [0] * num_partitions

            for item in sorted_data:
                min_partition = min(range(num_partitions), key=lambda i: partition_weights[i])
                partitions[min_partition].append(item)
                partition_weights[min_partition] += item.get(balance_field, 0)

            return ActionResult(
                success=True,
                data={"partitions": partitions, "partition_count": num_partitions, "partition_weights": partition_weights},
                message=f"Balanced partitioned {len(data)} items into {num_partitions} partitions",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Partition balanced failed: {e}")
