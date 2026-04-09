"""Idempotency action module for RabAI AutoClick.

Provides idempotency handling:
- IdempotencyStore: Store and check idempotency keys
- IdempotencyChecker: Verify if operation was already executed
- IdempotencyGuard: Guard operations with idempotency keys
- IdempotencyTTLManager: Manage TTL for idempotency keys
"""

from __future__ import annotations

import json
import sys
import os
import time
import hashlib
import threading
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


@dataclass
class IdempotencyRecord:
    """Idempotency record."""
    key: str
    status: str
    result: Optional[Any] = None
    created_at: float = 0.0
    expires_at: float = 0.0
    execution_count: int = 0


class IdempotencyStoreAction(BaseAction):
    """Store and manage idempotency keys."""
    action_type = "idempotency_store"
    display_name = "幂等性存储"
    description = "存储和管理幂等性Key"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            store_path = params.get("store_path", "/tmp/idempotency")
            key = params.get("key", "")
            ttl_seconds = params.get("ttl_seconds", 86400)
            result_data = params.get("result_data", None)
            redis_url = params.get("redis_url", "redis://localhost:6379/0")
            use_redis = params.get("use_redis", False) and REDIS_AVAILABLE

            if not key:
                return ActionResult(success=False, message="key is required")

            key_hash = hashlib.sha256(key.encode()).hexdigest()[:32]
            os.makedirs(store_path, exist_ok=True)
            key_file = os.path.join(store_path, f"{key_hash}.json")

            if use_redis:
                client = redis.from_url(redis_url, decode_responses=False)

            if operation == "check":
                if use_redis:
                    exists = client.exists(key)
                    if exists:
                        val = client.get(key)
                        record = json.loads(val.decode()) if val else {}
                        return ActionResult(
                            success=True,
                            message="Key exists",
                            data={"key": key, "status": record.get("status"), "result": record.get("result")}
                        )
                    return ActionResult(success=True, message="Key not found", data={"key": key, "exists": False})

                if os.path.exists(key_file):
                    with open(key_file) as f:
                        record = json.load(f)
                    if record.get("expires_at", 0) > time.time():
                        return ActionResult(
                            success=True,
                            message="Key exists",
                            data={"key": key, "status": record.get("status"), "result": record.get("result")}
                        )
                    else:
                        os.remove(key_file)
                return ActionResult(success=True, message="Key not found", data={"key": key, "exists": False})

            elif operation == "store":
                status = params.get("status", "completed")
                created_at = time.time()
                expires_at = created_at + ttl_seconds

                record = {
                    "key": key,
                    "key_hash": key_hash,
                    "status": status,
                    "result": result_data,
                    "created_at": created_at,
                    "expires_at": expires_at,
                    "execution_count": 1,
                }

                if use_redis:
                    client.setex(key, ttl_seconds, json.dumps(record))
                else:
                    with open(key_file, "w") as f:
                        json.dump(record, f)

                return ActionResult(success=True, message=f"Stored: {key}", data={"key": key, "expires_at": expires_at})

            elif operation == "delete":
                if use_redis:
                    client.delete(key)
                elif os.path.exists(key_file):
                    os.remove(key_file)

                return ActionResult(success=True, message=f"Deleted: {key}")

            elif operation == "list":
                records = []
                if use_redis:
                    keys = client.keys("*")
                    for k in keys:
                        val = client.get(k)
                        if val:
                            rec = json.loads(val.decode())
                            records.append({"key": rec.get("key"), "status": rec.get("status"), "expires_at": rec.get("expires_at")})
                else:
                    for filename in os.listdir(store_path):
                        if filename.endswith(".json"):
                            with open(os.path.join(store_path, filename)) as f:
                                record = json.load(f)
                                if record.get("expires_at", 0) > time.time():
                                    records.append({"key": record.get("key"), "status": record.get("status"), "expires_at": record.get("expires_at")})

                return ActionResult(success=True, message=f"{len(records)} active keys", data={"records": records, "count": len(records)})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class IdempotencyGuardAction(BaseAction):
    """Guard operations with idempotency key checking."""
    action_type = "idempotency_guard"
    display_name = "幂等性保护"
    description = "基于幂等性Key保护操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "")
            operation_fn = params.get("operation_fn", None)
            ttl_seconds = params.get("ttl_seconds", 3600)
            store_path = params.get("store_path", "/tmp/idempotency")
            redis_url = params.get("redis_url", "redis://localhost:6379/0")
            use_redis = params.get("use_redis", False) and REDIS_AVAILABLE

            if not key:
                return ActionResult(success=False, message="key is required")

            key_hash = hashlib.sha256(key.encode()).hexdigest()[:32]
            os.makedirs(store_path, exist_ok=True)

            if use_redis:
                client = redis.from_url(redis_url, decode_responses=True)

                existing = client.get(key)
                if existing:
                    record = json.loads(existing)
                    return ActionResult(
                        success=True,
                        message="Operation already executed (idempotent)",
                        data={"key": key, "status": record.get("status"), "cached_result": record.get("result")}
                    )

                client.setex(key, ttl_seconds, json.dumps({"status": "in_progress", "key": key}))
            else:
                key_file = os.path.join(store_path, f"{key_hash}.json")
                if os.path.exists(key_file):
                    with open(key_file) as f:
                        record = json.load(f)
                    if record.get("expires_at", 0) > time.time():
                        return ActionResult(
                            success=True,
                            message="Operation already executed (idempotent)",
                            data={"key": key, "status": record.get("status"), "cached_result": record.get("result")}
                        )

                record = {
                    "key": key,
                    "status": "in_progress",
                    "created_at": time.time(),
                    "expires_at": time.time() + ttl_seconds,
                }
                with open(key_file, "w") as f:
                    json.dump(record, f)

            return ActionResult(
                success=True,
                message=f"Idempotency key acquired: {key}",
                data={"key": key, "acquired": True}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class IdempotencyTTLManagerAction(BaseAction):
    """Manage TTL for idempotency keys."""
    action_type = "idempotency_ttl_manager"
    display_name = "幂等性TTL管理"
    description = "管理幂等性Key的TTL"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "cleanup")
            store_path = params.get("store_path", "/tmp/idempotency")
            redis_url = params.get("redis_url", "redis://localhost:6379/0")
            use_redis = params.get("use_redis", False) and REDIS_AVAILABLE
            key = params.get("key", "")
            new_ttl = params.get("new_ttl", 86400)

            if operation == "cleanup":
                cleaned = 0
                if use_redis:
                    client = redis.from_url(redis_url, decode_responses=True)
                    keys = client.keys("*")
                    for k in keys:
                        val = client.get(k)
                        if val:
                            record = json.loads(val)
                            if record.get("expires_at", 0) < time.time():
                                client.delete(k)
                                cleaned += 1
                else:
                    for filename in os.listdir(store_path):
                        if filename.endswith(".json"):
                            filepath = os.path.join(store_path, filename)
                            with open(filepath) as f:
                                record = json.load(f)
                            if record.get("expires_at", 0) < time.time():
                                os.remove(filepath)
                                cleaned += 1

                return ActionResult(success=True, message=f"Cleaned {cleaned} expired keys")

            elif operation == "extend":
                if not key:
                    return ActionResult(success=False, message="key required")

                key_hash = hashlib.sha256(key.encode()).hexdigest()[:32]
                key_file = os.path.join(store_path, f"{key_hash}.json")

                if not os.path.exists(key_file):
                    return ActionResult(success=False, message=f"Key not found: {key}")

                with open(key_file) as f:
                    record = json.load(f)

                record["expires_at"] = time.time() + new_ttl
                with open(key_file, "w") as f:
                    json.dump(record, f)

                return ActionResult(success=True, message=f"Extended TTL for: {key}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
