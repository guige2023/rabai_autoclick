"""Data hashing action module for RabAI AutoClick.

Provides data hashing operations:
- HashComputeAction: Compute various hash functions
- HashVerifyAction: Verify data against hash
- HashDictionaryAction: Hash dictionary with salting
- BloomFilterAction: Bloom filter for membership testing
"""

import hashlib
import hmac
from typing import Any, Dict, List, Optional, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HashComputeAction(BaseAction):
    """Compute various hash functions."""
    action_type = "hash_compute"
    display_name = "哈希计算"
    description = "计算各种哈希函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            algorithm = params.get("algorithm", "sha256")
            digest_type = params.get("digest_type", "hex")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, dict):
                import json
                data = json.dumps(data, sort_keys=True)

            data_bytes = str(data).encode("utf-8")

            if algorithm == "md5":
                h = hashlib.md5(data_bytes)
            elif algorithm == "sha1":
                h = hashlib.sha1(data_bytes)
            elif algorithm == "sha256":
                h = hashlib.sha256(data_bytes)
            elif algorithm == "sha512":
                h = hashlib.sha512(data_bytes)
            elif algorithm == "blake2b":
                h = hashlib.blake2b(data_bytes)
            elif algorithm == "blake2s":
                h = hashlib.blake2s(data_bytes)
            elif algorithm == "sha3_256":
                h = hashlib.sha3_256(data_bytes)
            elif algorithm == "sha3_512":
                h = hashlib.sha3_512(data_bytes)
            else:
                h = hashlib.sha256(data_bytes)

            if digest_type == "hex":
                result = h.hexdigest()
            elif digest_type == "base64":
                import base64
                result = base64.b64encode(h.digest()).decode("ascii")
            else:
                result = h.hexdigest()

            return ActionResult(
                success=True,
                message=f"Hash computed using {algorithm}",
                data={"hash": result, "algorithm": algorithm, "digest_type": digest_type},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HashCompute error: {e}")


class HashVerifyAction(BaseAction):
    """Verify data against hash."""
    action_type = "hash_verify"
    display_name = "哈希验证"
    description = "验证数据哈希"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            expected_hash = params.get("expected_hash", "")
            algorithm = params.get("algorithm", "sha256")

            if not data or not expected_hash:
                return ActionResult(success=False, message="data and expected_hash are required")

            result = HashComputeAction().execute(context, {"data": data, "algorithm": algorithm, "digest_type": "hex"})
            computed_hash = result.data.get("hash", "")

            is_valid = hmac.compare_digest(computed_hash.lower(), expected_hash.lower())

            return ActionResult(
                success=is_valid,
                message=f"Hash verification: {'PASSED' if is_valid else 'FAILED'}",
                data={
                    "is_valid": is_valid,
                    "computed_hash": computed_hash,
                    "expected_hash": expected_hash,
                    "algorithm": algorithm,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HashVerify error: {e}")


class HashDictionaryAction(BaseAction):
    """Hash dictionary with salting."""
    action_type = "hash_dictionary"
    display_name: "字典哈希"
    description = "带盐值的字典哈希"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            salt = params.get("salt", "")
            algorithm = params.get("algorithm", "sha256")

            if not isinstance(data, dict):
                return ActionResult(success=False, message="data must be a dict")

            if not salt:
                import secrets
                salt = secrets.token_hex(16)

            hashed_data = {}
            for key, value in data.items():
                combined = f"{salt}{key}{str(value)}"
                h = hashlib.sha256(combined.encode("utf-8")).hexdigest()
                hashed_data[key] = h

            return ActionResult(
                success=True,
                message=f"Dictionary hashed with salt",
                data={"hashed_data": hashed_data, "salt": salt, "algorithm": algorithm, "key_count": len(hashed_data)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HashDictionary error: {e}")


class BloomFilterAction(BaseAction):
    """Bloom filter for membership testing."""
    action_type = "bloom_filter"
    display_name = "布隆过滤器"
    description = "布隆过滤器用于成员测试"

    def __init__(self):
        super().__init__()
        self._size = params.get("size", 1000) if hasattr(self, "_size") else 1000
        self._hash_count = params.get("hash_count", 3) if hasattr(self, "_hash_count") else 3
        self._bit_array = [False] * self._size

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "add")
            items = params.get("items", [])
            query = params.get("query", "")
            false_positive_rate = params.get("false_positive_rate", 0.01)

            if not isinstance(items, list):
                items = [items]

            if action == "create":
                size = params.get("size", 1000)
                hash_count = params.get("hash_count", 3)
                self._size = size
                self._hash_count = hash_count
                self._bit_array = [False] * size
                return ActionResult(
                    success=True,
                    message=f"Bloom filter created: size={size}, hash_count={hash_count}",
                    data={"size": size, "hash_count": hash_count},
                )

            elif action == "add":
                for item in items:
                    item_str = str(item)
                    for i in range(self._hash_count):
                        idx = self._hash_idx(item_str, i)
                        self._bit_array[idx] = True
                return ActionResult(
                    success=True,
                    message=f"Added {len(items)} items to bloom filter",
                    data={"added": len(items), "set_bits": sum(self._bit_array)},
                )

            elif action == "check":
                if not query:
                    return ActionResult(success=False, message="query is required")
                item_str = str(query)
                for i in range(self._hash_count):
                    idx = self._hash_idx(item_str, i)
                    if not self._bit_array[idx]:
                        return ActionResult(
                            success=True,
                            message=f"'{query}' definitely NOT in bloom filter",
                            data={"in_filter": False, "query": query, "definitely_not": True},
                        )
                return ActionResult(
                    success=True,
                    message=f"'{query}' possibly IN bloom filter (may be false positive)",
                    data={"in_filter": True, "query": query, "possibly": True},
                )

            elif action == "stats":
                set_bits = sum(self._bit_array)
                load_factor = set_bits / self._size
                return ActionResult(
                    success=True,
                    message=f"Bloom filter stats: {set_bits}/{self._size} bits set ({load_factor:.2%})",
                    data={"size": self._size, "set_bits": set_bits, "load_factor": round(load_factor, 4), "hash_count": self._hash_count},
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"BloomFilter error: {e}")

    def _hash_idx(self, item: str, seed: int) -> int:
        combined = f"{item}{seed}"
        h = hashlib.md5(combined.encode("utf-8")).hexdigest()
        return int(h, 16) % self._size
