"""Data partitioning action module for RabAI AutoClick.

Provides data partitioning operations:
- HashPartitionAction: Hash-based data partitioning
- RangePartitionAction: Range-based data partitioning
- RoundRobinPartitionAction: Round-robin partitioning
- ListPartitionAction: List-based partitioning
"""

from typing import Any, Dict, List, Optional, Callable
import hashlib

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HashPartitionAction(BaseAction):
    """Hash-based data partitioning."""
    action_type = "hash_partition"
    display_name = "哈希分区"
    description = "基于哈希函数的数据分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            partition_key = params.get("partition_key", "id")
            num_partitions = params.get("num_partitions", 4)
            hash_algo = params.get("hash_algo", "md5")

            if not isinstance(data, list):
                data = [data]

            partitions: List[List] = [[] for _ in range(num_partitions)]

            for item in data:
                if isinstance(item, dict):
                    key_value = item.get(partition_key, "")
                else:
                    key_value = str(item)

                hash_input = str(key_value).encode("utf-8")
                if hash_algo == "md5":
                    hash_val = hashlib.md5(hash_input).hexdigest()
                elif hash_algo == "sha1":
                    hash_val = hashlib.sha1(hash_input).hexdigest()
                elif hash_algo == "sha256":
                    hash_val = hashlib.sha256(hash_input).hexdigest()
                else:
                    hash_val = str(hash(key_value))

                partition_idx = int(hash_val, 16) % num_partitions if hash_val.startswith(("0x", "0X")) else int(hash_val[:8], 16) % num_partitions
                partitions[partition_idx].append(item)

            partition_info = {f"partition_{i}": {"size": len(partitions[i]), "index": i} for i in range(num_partitions)}

            return ActionResult(
                success=True,
                message=f"Hash partitioned {len(data)} items into {num_partitions} partitions",
                data={"partitions": partitions, "partition_info": partition_info, "num_partitions": num_partitions},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HashPartition error: {e}")


class RangePartitionAction(BaseAction):
    """Range-based data partitioning."""
    action_type = "range_partition"
    display_name = "范围分区"
    description = "基于数值范围的数据分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            partition_key = params.get("partition_key", "value")
            boundaries = params.get("boundaries", [0, 25, 50, 75, 100])
            labels = params.get("labels", None)

            if not isinstance(data, list):
                data = [data]

            if not boundaries:
                return ActionResult(success=False, message="boundaries is required")

            num_partitions = len(boundaries) + 1
            partitions: List[List] = [[] for _ in range(num_partitions)]

            for item in data:
                if isinstance(item, dict):
                    val = item.get(partition_key, 0)
                else:
                    val = item

                if not isinstance(val, (int, float)):
                    partitions[-1].append(item)
                    continue

                partition_idx = 0
                for i, boundary in enumerate(boundaries):
                    if val >= boundary:
                        partition_idx = i + 1
                partition_idx = min(partition_idx, num_partitions - 1)
                partitions[partition_idx].append(item)

            if labels:
                partition_labels = labels[:num_partitions] + [f"partition_{i}" for i in range(len(labels), num_partitions)]
            else:
                partition_labels = [f"partition_{i}" for i in range(num_partitions)]

            partition_info = {}
            for i, label in enumerate(partition_labels):
                partition_info[label] = {"size": len(partitions[i]), "index": i}

            return ActionResult(
                success=True,
                message=f"Range partitioned {len(data)} items into {num_partitions} partitions",
                data={
                    "partitions": partitions,
                    "partition_info": partition_info,
                    "num_partitions": num_partitions,
                    "boundaries": boundaries,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"RangePartition error: {e}")


class RoundRobinPartitionAction(BaseAction):
    """Round-robin partitioning."""
    action_type = "round_robin_partition"
    display_name = "轮询分区"
    description = "轮询分配数据到分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            num_partitions = params.get("num_partitions", 4)

            if not isinstance(data, list):
                data = [data]

            partitions: List[List] = [[] for _ in range(num_partitions)]

            for i, item in enumerate(data):
                partition_idx = i % num_partitions
                partitions[partition_idx].append(item)

            partition_info = {f"partition_{i}": {"size": len(partitions[i]), "index": i} for i in range(num_partitions)}

            sizes = [len(p) for p in partitions]
            max_diff = max(sizes) - min(sizes) if sizes else 0

            return ActionResult(
                success=True,
                message=f"Round-robin partitioned {len(data)} items into {num_partitions} partitions (max imbalance: {max_diff})",
                data={
                    "partitions": partitions,
                    "partition_info": partition_info,
                    "num_partitions": num_partitions,
                    "max_imbalance": max_diff,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"RoundRobinPartition error: {e}")


class ListPartitionAction(BaseAction):
    """List-based partitioning."""
    action_type = "list_partition"
    display_name = "列表分区"
    description = "基于列表值的数据分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            partition_key = params.get("partition_key", "category")
            partition_map = params.get("partition_map", {})
            default_partition = params.get("default_partition", "other")

            if not isinstance(data, list):
                data = [data]

            if not partition_map:
                unique_values = set()
                for item in data:
                    if isinstance(item, dict):
                        val = item.get(partition_key)
                        if val is not None:
                            unique_values.add(str(val))
                partition_names = sorted(unique_values)
                partition_map = {v: v for v in partition_names}
                partitions: Dict[str, List] = {v: [] for v in partition_names}
                partitions[default_partition] = []
            else:
                partition_names = list(partition_map.values())
                partitions = {name: [] for name in partition_names}
                if default_partition not in partitions:
                    partitions[default_partition] = []

            for item in data:
                if isinstance(item, dict):
                    val = str(item.get(partition_key, ""))
                else:
                    val = str(item)

                matched_partition = None
                for src_val, dst_name in partition_map.items():
                    if str(src_val) == val:
                        matched_partition = dst_name
                        break

                if matched_partition:
                    partitions[matched_partition].append(item)
                else:
                    partitions[default_partition].append(item)

            partition_info = {name: {"size": len(partitions[name]), "index": i} for i, name in enumerate(partitions)}

            return ActionResult(
                success=True,
                message=f"List partitioned {len(data)} items into {len(partitions)} partitions",
                data={
                    "partitions": partitions,
                    "partition_info": partition_info,
                    "num_partitions": len(partitions),
                    "partition_map": partition_map,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ListPartition error: {e}")
