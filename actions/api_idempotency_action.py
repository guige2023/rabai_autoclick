"""API idempotency action module for RabAI AutoClick.

Provides idempotency operations:
- IdempotencyKeyAction: Generate/check idempotency key
- IdempotencyStoreAction: Store idempotency record
- IdempotencyLookupAction: Lookup existing record
- IdempotencyExpireAction: Expire old idempotency keys
"""

import hashlib
import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class IdempotencyKeyAction(BaseAction):
    """Generate or check idempotency key."""
    action_type = "idempotency_key"
    display_name = "幂等键"
    description = "生成或检查幂等键"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            if not key:
                key = str(uuid.uuid4())

            key_hash = hashlib.sha256(key.encode()).hexdigest()[:16]

            return ActionResult(
                success=True,
                data={"key": key, "key_hash": key_hash},
                message=f"Idempotency key: {key_hash}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Idempotency key failed: {e}")


class IdempotencyStoreAction(BaseAction):
    """Store idempotency record."""
    action_type = "idempotency_store"
    display_name = "存储幂等记录"
    description = "存储幂等性记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            response = params.get("response", {})
            ttl = params.get("ttl", 86400)

            if not key:
                return ActionResult(success=False, message="key is required")

            key_hash = hashlib.sha256(key.encode()).hexdigest()[:16]

            if not hasattr(context, "idempotency_store"):
                context.idempotency_store = {}
            context.idempotency_store[key_hash] = {
                "original_key": key,
                "response": response,
                "created_at": time.time(),
                "expires_at": time.time() + ttl,
            }

            return ActionResult(
                success=True,
                data={"key_hash": key_hash, "ttl": ttl},
                message=f"Idempotency record stored: {key_hash}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Idempotency store failed: {e}")


class IdempotencyLookupAction(BaseAction):
    """Lookup idempotency record."""
    action_type = "idempotency_lookup"
    display_name = "查询幂等记录"
    description = "查询幂等性记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            if not key:
                return ActionResult(success=False, message="key is required")

            key_hash = hashlib.sha256(key.encode()).hexdigest()[:16]
            store = getattr(context, "idempotency_store", {})
            record = store.get(key_hash)

            if not record:
                return ActionResult(success=True, data={"found": False, "key_hash": key_hash}, message="No record found")

            if record.get("expires_at", 0) < time.time():
                return ActionResult(success=True, data={"found": False, "key_hash": key_hash, "expired": True}, message="Record expired")

            return ActionResult(
                success=True,
                data={"found": True, "key_hash": key_hash, "response": record.get("response")},
                message="Record found",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Idempotency lookup failed: {e}")


class IdempotencyExpireAction(BaseAction):
    """Expire old idempotency keys."""
    action_type = "idempotency_expire"
    display_name = "清理幂等过期"
    description = "清理过期的幂等键"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            max_age = params.get("max_age", 86400)
            store = getattr(context, "idempotency_store", {})
            now = time.time()
            expired_keys = [k for k, v in store.items() if v.get("expires_at", 0) < now]

            for k in expired_keys:
                del store[k]

            return ActionResult(
                success=True,
                data={"expired_count": len(expired_keys), "remaining": len(store)},
                message=f"Expired {len(expired_keys)} idempotency keys",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Idempotency expire failed: {e}")
