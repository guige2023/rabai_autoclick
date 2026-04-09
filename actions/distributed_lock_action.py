"""Distributed lock action module for RabAI AutoClick.

Provides distributed locking mechanisms:
- RedisDistributedLock: Redis-based distributed lock
- LockAcquirer: Acquire locks with retry logic
- LockContext: Context manager for lock lifecycle
- LockRegistry: Track and manage multiple locks
- Semaphore: Counting semaphore for resource limiting
"""

from __future__ import annotations

import sys
import os
import time
import uuid
import threading
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from contextlib import contextmanager

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


@dataclass
class LockInfo:
    """Lock metadata container."""
    lock_key: str
    lock_id: str
    acquired_at: float
    expires_at: float
    ttl: int
    is_held: bool = True


class RedisDistributedLockAction(BaseAction):
    """Redis-based distributed lock implementation."""
    action_type = "redis_distributed_lock"
    display_name = "Redis分布式锁"
    description = "基于Redis的分布式锁"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not REDIS_AVAILABLE:
            return ActionResult(success=False, message="redis not installed: pip install redis")

        try:
            operation = params.get("operation", "acquire")
            lock_key = params.get("lock_key", "")
            lock_value = params.get("lock_value", str(uuid.uuid4()))
            ttl_ms = params.get("ttl_ms", 30000)
            retry_count = params.get("retry_count", 3)
            retry_delay_ms = params.get("retry_delay_ms", 200)
            redis_url = params.get("redis_url", "redis://localhost:6379/0")

            if not lock_key:
                return ActionResult(success=False, message="lock_key is required")

            client = redis.from_url(redis_url, decode_responses=False)

            if operation == "acquire":
                acquired = False
                lock_id = ""
                for attempt in range(retry_count + 1):
                    lock_id = f"{lock_value}:{attempt}"
                    acquired = client.set(
                        lock_key,
                        lock_id,
                        nx=True,
                        px=ttl_ms,
                    )
                    if acquired:
                        break
                    if attempt < retry_count:
                        time.sleep(retry_delay_ms / 1000.0)

                if acquired:
                    return ActionResult(
                        success=True,
                        message=f"Lock acquired: {lock_key}",
                        data={"lock_key": lock_key, "lock_id": lock_id, "ttl_ms": ttl_ms}
                    )
                else:
                    return ActionResult(success=False, message=f"Lock not acquired: {lock_key}")

            elif operation == "release":
                lua_script = """
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
                """
                result = client.eval(lua_script, 1, lock_key, lock_value)
                released = result == 1

                if released:
                    return ActionResult(success=True, message=f"Lock released: {lock_key}")
                else:
                    return ActionResult(success=False, message=f"Lock not owned or expired: {lock_key}")

            elif operation == "extend":
                new_ttl_ms = params.get("new_ttl_ms", ttl_ms)
                lua_script = """
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("pexpire", KEYS[1], ARGV[2])
                else
                    return 0
                end
                """
                result = client.eval(lua_script, 1, lock_key, lock_value, new_ttl_ms)
                extended = result == 1

                if extended:
                    return ActionResult(success=True, message=f"Lock extended: {lock_key}")
                else:
                    return ActionResult(success=False, message=f"Lock not owned: {lock_key}")

            elif operation == "status":
                exists = client.exists(lock_key)
                if exists:
                    current_value = client.get(lock_key)
                    ttl = client.pttl(lock_key)
                    return ActionResult(
                        success=True,
                        message=f"Lock held: {lock_key}",
                        data={"lock_key": lock_key, "is_held": True, "ttl_ms": ttl, "owner": current_value.decode() if current_value else None}
                    )
                else:
                    return ActionResult(success=True, message=f"Lock not held: {lock_key}", data={"lock_key": lock_key, "is_held": False})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Lock error: {str(e)}")


class LockContextManagerAction(BaseAction):
    """Context manager for distributed locks with automatic release."""
    action_type = "lock_context_manager"
    display_name = "锁上下文管理"
    description = "自动释放的锁上下文管理器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not REDIS_AVAILABLE:
            return ActionResult(success=False, message="redis not installed: pip install redis")

        try:
            lock_key = params.get("lock_key", "")
            ttl_ms = params.get("ttl_ms", 30000)
            redis_url = params.get("redis_url", "redis://localhost:6379/0")
            auto_release = params.get("auto_release", True)
            lock_timeout_s = params.get("lock_timeout_s", 60)

            if not lock_key:
                return ActionResult(success=False, message="lock_key is required")

            client = redis.from_url(redis_url, decode_responses=False)
            lock_id = str(uuid.uuid4())

            acquired = client.set(lock_key, lock_id, nx=True, px=ttl_ms)

            if not acquired:
                return ActionResult(success=False, message=f"Could not acquire lock: {lock_key}")

            acquired_at = time.time()

            if auto_release:
                def release():
                    lua_script = """
                    if redis.call("get", KEYS[1]) == ARGV[1] then
                        return redis.call("del", KEYS[1])
                    else
                        return 0
                    end
                    """
                    client.eval(lua_script, 1, lock_key, lock_id)

                import atexit
                atexit.register(release)

            return ActionResult(
                success=True,
                message=f"Lock acquired with context: {lock_key}",
                data={
                    "lock_key": lock_key,
                    "lock_id": lock_id,
                    "acquired_at": acquired_at,
                    "ttl_ms": ttl_ms,
                    "auto_release": auto_release,
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class LockRegistryAction(BaseAction):
    """Registry for tracking multiple distributed locks."""
    action_type = "lock_registry"
    display_name = "锁注册表"
    description = "管理多个分布式锁的注册表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not REDIS_AVAILABLE:
            return ActionResult(success=False, message="redis not installed: pip install redis")

        try:
            operation = params.get("operation", "register")
            lock_key = params.get("lock_key", "")
            metadata = params.get("metadata", {})
            redis_url = params.get("redis_url", "redis://localhost:6379/0")
            registry_prefix = params.get("registry_prefix", "lock:registry:")

            client = redis.from_url(redis_url, decode_responses=False)

            if operation == "register":
                if not lock_key:
                    return ActionResult(success=False, message="lock_key is required")

                lock_info = {
                    "key": lock_key,
                    "registered_at": time.time(),
                    "metadata": metadata,
                }
                client.hset(registry_prefix + "keys", lock_key, json.dumps(lock_info))
                return ActionResult(success=True, message=f"Lock registered: {lock_key}")

            elif operation == "unregister":
                if not lock_key:
                    return ActionResult(success=False, message="lock_key is required")
                client.hdel(registry_prefix + "keys", lock_key)
                return ActionResult(success=True, message=f"Lock unregistered: {lock_key}")

            elif operation == "list":
                all_locks = client.hgetall(registry_prefix + "keys")
                locks = []
                for key, info in all_locks.items():
                    locks.append({
                        "key": key.decode() if isinstance(key, bytes) else key,
                        "info": json.loads(info.decode() if isinstance(info, bytes) else info),
                    })
                return ActionResult(success=True, message=f"Registry has {len(locks)} locks", data={"locks": locks})

            elif operation == "check_status":
                if not lock_key:
                    return ActionResult(success=False, message="lock_key is required")
                exists = client.exists(lock_key)
                ttl = client.pttl(lock_key) if exists else None
                return ActionResult(
                    success=True,
                    message=f"Status: {lock_key}",
                    data={"lock_key": lock_key, "is_held": bool(exists), "ttl_ms": ttl}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


import json


class SemaphoreAction(BaseAction):
    """Counting semaphore for distributed resource limiting."""
    action_type = "semaphore"
    display_name = "分布式信号量"
    description = "用于限制资源的分布式信号量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not REDIS_AVAILABLE:
            return ActionResult(success=False, message="redis not installed: pip install redis")

        try:
            operation = params.get("operation", "acquire")
            sem_key = params.get("sem_key", "")
            permits = params.get("permits", 1)
            ttl_ms = params.get("ttl_ms", 60000)
            timeout_ms = params.get("timeout_ms", 10000)
            redis_url = params.get("redis_url", "redis://localhost:6379/0")

            if not sem_key:
                return ActionResult(success=False, message="sem_key is required")

            client = redis.from_url(redis_url, decode_responses=False)

            if operation == "acquire":
                start = time.time()
                acquired = False
                while time.time() * 1000 - start < timeout_ms:
                    current = client.get(sem_key)
                    current_val = int(current) if current else 0
                    if current_val + permits <= permits * 100:
                        pipe = client.pipeline()
                        pipe.incrby(sem_key, permits)
                        pipe.pexpire(sem_key, ttl_ms)
                        pipe.execute()
                        acquired = True
                        break
                    time.sleep(0.01)

                if acquired:
                    return ActionResult(success=True, message=f"Semaphore acquired: {sem_key}", data={"permits": permits})
                else:
                    return ActionResult(success=False, message=f"Semaphore timeout: {sem_key}")

            elif operation == "release":
                current = client.get(sem_key)
                current_val = int(current) if current else 0
                new_val = max(0, current_val - permits)
                if new_val == 0:
                    client.delete(sem_key)
                else:
                    client.decrby(sem_key, permits)
                return ActionResult(success=True, message=f"Semaphore released: {sem_key}", data={"permits": permits})

            elif operation == "status":
                current = client.get(sem_key)
                current_val = int(current) if current else 0
                ttl = client.pttl(sem_key)
                return ActionResult(
                    success=True,
                    message=f"Semaphore status: {sem_key}",
                    data={"current_permits": current_val, "ttl_ms": ttl}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
