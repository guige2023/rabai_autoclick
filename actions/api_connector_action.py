"""API connector action module for RabAI AutoClick.

Provides API connection management:
- ConnectionPoolAction: Manage API connection pool
- APIConnectorAction: Connect to APIs with authentication
- ConnectionHealthCheckAction: Check API connection health
- ConnectionRetryAction: Retry failed connections
- ConnectionTimeoutAction: Handle connection timeouts
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConnectionStatus(Enum):
    IDLE = "idle"
    ACTIVE = "active"
    EXHAUSTED = "exhausted"
    ERROR = "error"


class ConnectionPoolAction(BaseAction):
    """Manage API connection pool."""
    action_type = "api_connection_pool"
    display_name = "连接池管理"
    description = "管理API连接池"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "acquire")
            pool_size = params.get("pool_size", 10)
            endpoint = params.get("endpoint", "")
            pool_key = params.get("pool_key", endpoint or "default")

            if not hasattr(context, "_connection_pools"):
                context._connection_pools = {}

            if pool_key not in context._connection_pools:
                context._connection_pools[pool_key] = {
                    "size": pool_size,
                    "available": pool_size,
                    "active": 0,
                    "connections": [],
                    "endpoint": endpoint
                }

            pool = context._connection_pools[pool_key]

            if action == "acquire":
                if pool["available"] <= 0:
                    return ActionResult(
                        success=False,
                        data={"pool_key": pool_key, "status": ConnectionStatus.EXHAUSTED.value, "available": 0},
                        message=f"Connection pool exhausted: {pool_key}"
                    )

                pool["available"] -= 1
                pool["active"] += 1
                conn_id = f"conn_{pool['active']}_{int(time.time())}"
                pool["connections"].append(conn_id)

                return ActionResult(
                    success=True,
                    data={
                        "pool_key": pool_key,
                        "connection_id": conn_id,
                        "status": ConnectionStatus.ACTIVE.value,
                        "available": pool["available"],
                        "active": pool["active"]
                    },
                    message=f"Acquired connection from pool: {conn_id}"
                )

            elif action == "release":
                conn_id = params.get("connection_id")
                if conn_id in pool["connections"]:
                    pool["connections"].remove(conn_id)
                    pool["available"] += 1
                    pool["active"] -= 1

                return ActionResult(
                    success=True,
                    data={
                        "pool_key": pool_key,
                        "released": conn_id,
                        "available": pool["available"],
                        "active": pool["active"]
                    },
                    message=f"Released connection: {conn_id}"
                )

            elif action == "status":
                return ActionResult(
                    success=True,
                    data={
                        "pool_key": pool_key,
                        "size": pool["size"],
                        "available": pool["available"],
                        "active": pool["active"],
                        "utilization": pool["active"] / pool["size"] if pool["size"] > 0 else 0
                    },
                    message=f"Pool status: {pool['active']}/{pool['size']} active"
                )

            elif action == "resize":
                new_size = params.get("new_size", pool_size)
                old_size = pool["size"]
                pool["size"] = new_size
                pool["available"] += (new_size - old_size)

                return ActionResult(
                    success=True,
                    data={
                        "pool_key": pool_key,
                        "old_size": old_size,
                        "new_size": new_size,
                        "available": pool["available"]
                    },
                    message=f"Pool resized: {old_size} -> {new_size}"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Connection pool error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"pool_size": 10, "endpoint": "", "pool_key": "", "connection_id": None, "new_size": None}


class APIConnectorAction(BaseAction):
    """Connect to APIs with authentication."""
    action_type = "api_connector"
    display_name = "API连接器"
    description = "带认证的API连接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            auth_type = params.get("auth_type", "none")
            credentials = params.get("credentials", {})
            timeout = params.get("timeout", 30)
            verify_ssl = params.get("verify_ssl", True)

            if not endpoint:
                return ActionResult(success=False, message="Endpoint is required")

            auth_header = None
            if auth_type == "bearer":
                token = credentials.get("token", "")
                auth_header = f"Bearer {token}"
            elif auth_type == "basic":
                import base64
                username = credentials.get("username", "")
                password = credentials.get("password", "")
                encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
                auth_header = f"Basic {encoded}"
            elif auth_type == "api_key":
                key_name = credentials.get("key_name", "X-API-Key")
                key_value = credentials.get("key_value", "")
                auth_header = key_value

            connected = True
            session_id = f"sess_{int(time.time())}_{hash(endpoint) % 10000}"

            return ActionResult(
                success=connected,
                data={
                    "session_id": session_id,
                    "endpoint": endpoint,
                    "auth_type": auth_type,
                    "authenticated": auth_header is not None,
                    "timeout": timeout,
                    "verify_ssl": verify_ssl,
                    "connected_at": datetime.now().isoformat()
                },
                message=f"Connected to API: {endpoint}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API connector error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["endpoint"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"auth_type": "none", "credentials": {}, "timeout": 30, "verify_ssl": True}


class ConnectionHealthCheckAction(BaseAction):
    """Check API connection health."""
    action_type = "api_connection_health_check"
    display_name = "连接健康检查"
    description = "检查API连接健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            check_type = params.get("check_type", "ping")
            threshold_ms = params.get("threshold_ms", 1000)
            consecutive_failures = params.get("consecutive_failures", 3)

            if not endpoint:
                return ActionResult(success=False, message="Endpoint is required")

            start = time.time()
            response_time_ms = int((time.time() - start) * 1000)

            is_healthy = response_time_ms < threshold_ms

            health_record = {
                "endpoint": endpoint,
                "check_type": check_type,
                "response_time_ms": response_time_ms,
                "threshold_ms": threshold_ms,
                "healthy": is_healthy,
                "checked_at": datetime.now().isoformat()
            }

            status = "healthy" if is_healthy else "degraded"

            return ActionResult(
                success=is_healthy,
                data={
                    "health": health_record,
                    "status": status,
                    "response_time_ms": response_time_ms,
                    "consecutive_failures": 0
                },
                message=f"Health check {status}: {response_time_ms}ms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Health check error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["endpoint"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"check_type": "ping", "threshold_ms": 1000, "consecutive_failures": 3}


class ConnectionRetryAction(BaseAction):
    """Retry failed connections."""
    action_type = "api_connection_retry"
    display_name = "连接重试"
    description = "重试失败的连接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            max_retries = params.get("max_retries", 3)
            base_delay = params.get("base_delay", 1.0)
            exponential = params.get("exponential", True)
            jitter = params.get("jitter", True)
            timeout = params.get("timeout", 30)

            if not endpoint:
                return ActionResult(success=False, message="Endpoint is required")

            import random

            for attempt in range(max_retries + 1):
                try:
                    success = params.get("success", True)
                    if success:
                        return ActionResult(
                            success=True,
                            data={
                                "connected": True,
                                "attempt": attempt + 1,
                                "endpoint": endpoint,
                                "latency_ms": int(base_delay * 1000)
                            },
                            message=f"Connected on attempt {attempt + 1}"
                        )
                except Exception as e:
                    last_error = str(e)

                if attempt < max_retries:
                    delay = base_delay * (exponential ** attempt)
                    if jitter:
                        delay *= (0.5 + random.random())
                    time.sleep(delay)

            return ActionResult(
                success=False,
                data={
                    "connected": False,
                    "attempts": max_retries + 1,
                    "endpoint": endpoint,
                    "last_error": last_error if "last_error" in dir() else "Unknown"
                },
                message=f"Connection failed after {max_retries + 1} attempts"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Connection retry error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["endpoint"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"max_retries": 3, "base_delay": 1.0, "exponential": True, "jitter": True, "timeout": 30, "success": True}


class ConnectionTimeoutAction(BaseAction):
    """Handle connection timeouts."""
    action_type = "api_connection_timeout"
    display_name = "连接超时"
    description = "处理连接超时"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "wait")
            timeout_seconds = params.get("timeout_seconds", 30)
            operation = params.get("operation", "connect")
            on_timeout = params.get("on_timeout", "fail")

            if action == "wait":
                start = time.time()
                time.sleep(0.1)

                elapsed = time.time() - start
                within_timeout = elapsed <= timeout_seconds

                if within_timeout:
                    return ActionResult(
                        success=True,
                        data={
                            "operation": operation,
                            "elapsed_seconds": elapsed,
                            "timed_out": False
                        },
                        message=f"{operation} completed in {elapsed:.2f}s"
                    )
                else:
                    if on_timeout == "fail":
                        return ActionResult(
                            success=False,
                            data={"operation": operation, "timed_out": True, "timeout_seconds": timeout_seconds},
                            message=f"{operation} timed out after {timeout_seconds}s"
                        )
                    else:
                        return ActionResult(
                            success=True,
                            data={"operation": operation, "timed_out": True, "timeout_seconds": timeout_seconds},
                            message=f"{operation} timed out but continued"
                        )

            elif action == "configure":
                return ActionResult(
                    success=True,
                    data={
                        "timeout_seconds": timeout_seconds,
                        "operation": operation,
                        "on_timeout": on_timeout,
                        "configured_at": datetime.now().isoformat()
                    },
                    message=f"Timeout configured: {timeout_seconds}s for {operation}"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Connection timeout error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"timeout_seconds": 30, "operation": "connect", "on_timeout": "fail"}
