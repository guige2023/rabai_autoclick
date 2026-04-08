"""Data hash action module for RabAI AutoClick.

Provides data hashing operations:
- HashCreateAction: Create hash
- HashVerifyAction: Verify hash
- HashCompareAction: Compare hashes
- HashSaltAction: Hash with salt
- Hash批量Action: Batch hash
"""

import hashlib
from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class HashCreateAction(BaseAction):
    """Create hash from data."""
    action_type = "hash_create"
    display_name = "创建哈希"
    description = "从数据创建哈希"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            algorithm = params.get("algorithm", "md5")
            encoding = params.get("encoding", "utf-8")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, str):
                data = data.encode(encoding)

            if algorithm == "md5":
                hash_val = hashlib.md5(data).hexdigest()
            elif algorithm == "sha1":
                hash_val = hashlib.sha1(data).hexdigest()
            elif algorithm == "sha256":
                hash_val = hashlib.sha256(data).hexdigest()
            elif algorithm == "sha512":
                hash_val = hashlib.sha512(data).hexdigest()
            else:
                hash_val = hashlib.md5(data).hexdigest()

            return ActionResult(
                success=True,
                data={"hash": hash_val, "algorithm": algorithm, "length": len(hash_val)},
                message=f"Created {algorithm} hash: {hash_val[:16]}...",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Hash create failed: {e}")


class HashVerifyAction(BaseAction):
    """Verify hash."""
    action_type = "hash_verify"
    display_name = "验证哈希"
    description = "验证哈希值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            expected_hash = params.get("expected_hash", "")
            algorithm = params.get("algorithm", "md5")

            if not data or not expected_hash:
                return ActionResult(success=False, message="data and expected_hash are required")

            if isinstance(data, str):
                data = data.encode("utf-8")

            if algorithm == "md5":
                computed = hashlib.md5(data).hexdigest()
            elif algorithm == "sha256":
                computed = hashlib.sha256(data).hexdigest()
            else:
                computed = hashlib.md5(data).hexdigest()

            matches = computed.lower() == expected_hash.lower()

            return ActionResult(
                success=True,
                data={"matches": matches, "expected": expected_hash, "computed": computed},
                message=f"Hash verify: {'MATCHED' if matches else 'MISMATCH'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Hash verify failed: {e}")


class HashCompareAction(BaseAction):
    """Compare two hashes."""
    action_type = "hash_compare"
    display_name = "比较哈希"
    description = "比较两个哈希值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            hash1 = params.get("hash1", "")
            hash2 = params.get("hash2", "")

            if not hash1 or not hash2:
                return ActionResult(success=False, message="hash1 and hash2 are required")

            matches = hash1.lower() == hash2.lower()
            similarity = sum(c1 == c2 for c1, c2 in zip(hash1.lower(), hash2.lower())) / max(len(hash1), len(hash2))

            return ActionResult(
                success=True,
                data={"matches": matches, "similarity": similarity, "hash1": hash1[:16] + "...", "hash2": hash2[:16] + "..."},
                message=f"Hash compare: {'MATCHED' if matches else f'similarity={similarity:.2%}'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Hash compare failed: {e}")


class HashSaltAction(BaseAction):
    """Hash with salt."""
    action_type = "hash_salt"
    display_name = "盐值哈希"
    description = "带盐值哈希"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            salt = params.get("salt", "")
            algorithm = params.get("algorithm", "sha256")

            if not data or not salt:
                return ActionResult(success=False, message="data and salt are required")

            salted_data = f"{salt}{data}{salt}".encode("utf-8")
            if algorithm == "sha256":
                hash_val = hashlib.sha256(salted_data).hexdigest()
            elif algorithm == "sha512":
                hash_val = hashlib.sha512(salted_data).hexdigest()
            else:
                hash_val = hashlib.sha256(salted_data).hexdigest()

            return ActionResult(
                success=True,
                data={"hash": hash_val, "algorithm": algorithm, "salt_used": salt[:4] + "..."},
                message=f"Salted hash created with {algorithm}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Hash salt failed: {e}")


class HashBatchAction(BaseAction):
    """Batch hash creation."""
    action_type = "hash_batch"
    display_name = "批量哈希"
    description = "批量创建哈希"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            algorithm = params.get("algorithm", "md5")
            key_field = params.get("key_field", "data")

            if not items:
                return ActionResult(success=False, message="items list is required")

            results = []
            for item in items:
                data = item.get(key_field, "")
                if isinstance(data, str):
                    data = data.encode("utf-8")
                if algorithm == "md5":
                    hash_val = hashlib.md5(data).hexdigest()
                elif algorithm == "sha256":
                    hash_val = hashlib.sha256(data).hexdigest()
                else:
                    hash_val = hashlib.md5(data).hexdigest()
                results.append({"original": item, "hash": hash_val})

            return ActionResult(
                success=True,
                data={"results": results, "count": len(results), "algorithm": algorithm},
                message=f"Batch hashed {len(results)} items with {algorithm}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Hash batch failed: {e}")
