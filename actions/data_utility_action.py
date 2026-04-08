"""Data utility action module for RabAI AutoClick.

Provides various data utility operations:
- DataRandomAction: Random data generation
- DataShuffleAction: Shuffle and sample data
- DataChunkAction: Split data into chunks
- DataFlattenAction: Flatten nested structures
"""

import random
import json
from typing import Any, Dict, List, Optional
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataRandomAction(BaseAction):
    """Random data generation."""
    action_type = "data_random"
    display_name: "随机数据生成"
    description: "生成随机数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "generate")
            data_type = params.get("data_type", "int")
            count = params.get("count", 10)
            min_val = params.get("min", 0)
            max_val = params.get("max", 100)
            seed = params.get("seed", None)

            if seed is not None:
                random.seed(seed)

            if action == "generate":
                results = []
                for _ in range(count):
                    if data_type == "int":
                        results.append(random.randint(min_val, max_val))
                    elif data_type == "float":
                        results.append(round(random.uniform(min_val, max_val), 6))
                    elif data_type == "choice":
                        choices = params.get("choices", ["a", "b", "c"])
                        results.append(random.choice(choices))
                    elif data_type == "uuid":
                        import uuid
                        results.append(str(uuid.uuid4()))
                    elif data_type == "string":
                        import string
                        length = params.get("length", 10)
                        results.append("".join(random.choices(string.ascii_letters + string.digits, k=length)))
                    elif data_type == "bool":
                        results.append(random.choice([True, False]))
                    elif data_type == "date":
                        import datetime
                        start = datetime.datetime.now() - datetime.timedelta(days=365)
                        random_date = start + datetime.timedelta(seconds=random.randint(0, 365 * 24 * 3600))
                        results.append(random_date.isoformat())

                return ActionResult(
                    success=True,
                    message=f"Generated {count} random {data_type} values",
                    data={"data": results, "count": len(results), "type": data_type},
                )

            elif action == "sample":
                population = params.get("population", [])
                k = min(count, len(population))
                sampled = random.sample(population, k)
                return ActionResult(success=True, message=f"Sampled {k} items", data={"sample": sampled, "count": k})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"DataRandom error: {e}")


class DataShuffleAction(BaseAction):
    """Shuffle and sample data."""
    action_type: "data_shuffle"
    display_name: "数据洗牌"
    description: "打乱和采样数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            shuffle = params.get("shuffle", True)
            sample_size = params.get("sample_size", None)
            seed = params.get("seed", None)

            if not isinstance(data, list):
                data = [data]

            if seed is not None:
                random.seed(seed)

            if shuffle:
                result = data[:]
                random.shuffle(result)
            else:
                result = data

            if sample_size is not None and sample_size > 0:
                if shuffle:
                    result = random.sample(result, min(sample_size, len(result)))
                else:
                    result = result[:sample_size]

            return ActionResult(
                success=True,
                message=f"Shuffle {'with' if shuffle else 'without'} shuffle, sample_size={sample_size}",
                data={"data": result, "count": len(result)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DataShuffle error: {e}")


class DataChunkAction(BaseAction):
    """Split data into chunks."""
    action_type: "data_chunk"
    display_name: "数据分块"
    description: "将数据分割成块"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            chunk_size = params.get("chunk_size", 10)
            mode = params.get("mode", "equal")

            if not isinstance(data, list):
                data = [data]

            if chunk_size <= 0:
                return ActionResult(success=False, message="chunk_size must be positive")

            if mode == "equal":
                chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
            elif mode == "head":
                chunk_size = min(chunk_size, len(data))
                chunks = [data[:chunk_size]]
            elif mode == "tail":
                chunk_size = min(chunk_size, len(data))
                chunks = [data[-chunk_size:]]
            elif mode == "batch":
                batch_num = params.get("batch_number", 0)
                start = batch_num * chunk_size
                chunks = [data[start : start + chunk_size]]
            else:
                chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

            return ActionResult(
                success=True,
                message=f"Split {len(data)} items into {len(chunks)} chunks (size={chunk_size}, mode={mode})",
                data={
                    "chunks": chunks,
                    "chunk_count": len(chunks),
                    "chunk_size": chunk_size,
                    "mode": mode,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DataChunk error: {e}")


class DataFlattenAction(BaseAction):
    """Flatten nested structures."""
    action_type: "data_flatten"
    display_name: "数据扁平化"
    description: "扁平化嵌套结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            separator = params.get("separator", ".")
            max_depth = params.get("max_depth", 10)

            if isinstance(data, list):
                flat_list = []
                for item in data:
                    flat_item = self._flatten_dict(item, separator, 0, max_depth)
                    flat_list.append(flat_item)
                return ActionResult(
                    success=True,
                    message=f"Flattened {len(data)} nested items",
                    data={"flattened": flat_list, "count": len(flat_list)},
                )
            else:
                flat = self._flatten_dict(data, separator, 0, max_depth)
                return ActionResult(success=True, message=f"Flattened dict with {len(flat)} keys", data={"flattened": flat, "count": len(flat)})
        except Exception as e:
            return ActionResult(success=False, message=f"DataFlatten error: {e}")

    def _flatten_dict(self, d: Any, sep: str, depth: int, max_depth: int) -> Dict:
        if depth >= max_depth:
            return {"value": str(d)}
        if not isinstance(d, dict):
            return {"value": d}
        result = {}
        for k, v in d.items():
            if isinstance(v, dict):
                nested = self._flatten_dict(v, sep, depth + 1, max_depth)
                for nk, nv in nested.items():
                    result[f"{k}{sep}{nk}"] = nv
            elif isinstance(v, list):
                for i, item in enumerate(v[:10]):
                    if isinstance(item, dict):
                        nested = self._flatten_dict(item, sep, depth + 1, max_depth)
                        for nk, nv in nested.items():
                            result[f"{k}[{i}]{sep}{nk}"] = nv
                    else:
                        result[f"{k}[{i}]"] = item
            else:
                result[k] = v
        return result
