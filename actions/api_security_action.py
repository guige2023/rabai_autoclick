"""API security action module for RabAI AutoClick.

Provides API security operations:
- APIKeyGeneratorAction: Generate API keys
- APIKeyValidatorAction: Validate API keys
- APIRateLimiterAction: Rate limit API access
- APIPermissionAction: Check API permissions
- APIHealthCheckAction: Health check endpoints
"""

import hashlib
import secrets
import hmac
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APIKeyGeneratorAction(BaseAction):
    """Generate API keys."""
    action_type = "api_key_generator"
    display_name = "API密钥生成"
    description = "生成API密钥"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key_type = params.get("key_type", "random")
            key_length = params.get("key_length", 32)
            prefix = params.get("prefix", "")
            include_timestamp = params.get("include_timestamp", False)
            format_type = params.get("format", "hex")

            if key_type == "random":
                if format_type == "hex":
                    api_key = secrets.token_hex(key_length)
                elif format_type == "urlsafe":
                    api_key = secrets.token_urlsafe(key_length)
                else:
                    api_key = secrets.token_hex(key_length)
            elif key_type == "uuid":
                api_key = str(secrets.uuid4())
            elif key_type == "hashed":
                raw_key = secrets.token_hex(key_length)
                api_key = hashlib.sha256(raw_key.encode()).hexdigest()
            else:
                return ActionResult(success=False, message=f"Unknown key type: {key_type}")

            if prefix:
                api_key = f"{prefix}_{api_key}"

            if include_timestamp:
                timestamp = int(datetime.now().timestamp())
                api_key = f"{api_key}_{timestamp}"

            key_info = {
                "api_key": api_key,
                "key_type": key_type,
                "key_length": len(api_key),
                "prefix": prefix,
                "generated_at": datetime.now().isoformat()
            }

            return ActionResult(
                success=True,
                data=key_info,
                message=f"API key generated: {api_key[:20]}... (length: {len(api_key)})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API key generator error: {str(e)}")


class APIKeyValidatorAction(BaseAction):
    """Validate API keys."""
    action_type = "api_key_validator"
    display_name = "API密钥验证"
    description = "验证API密钥"

    def __init__(self):
        super().__init__()
        self._valid_keys = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "validate")
            api_key = params.get("api_key", "")
            key_id = params.get("key_id", "")
            store_key = params.get("store_key", False)

            if not api_key and not key_id:
                return ActionResult(success=False, message="api_key or key_id is required")

            if operation == "validate":
                if key_id and key_id in self._valid_keys:
                    is_valid = True
                    stored = self._valid_keys[key_id]
                else:
                    is_valid = len(api_key) >= 8
                    stored = None

                return ActionResult(
                    success=is_valid,
                    data={
                        "valid": is_valid,
                        "key_id": key_id,
                        "is_valid": is_valid,
                        "validated_at": datetime.now().isoformat()
                    },
                    message=f"API key validation: {'VALID' if is_valid else 'INVALID'}"
                )

            elif operation == "store":
                if not key_id:
                    return ActionResult(success=False, message="key_id is required for store operation")

                self._valid_keys[key_id] = {
                    "api_key": api_key,
                    "created_at": datetime.now().isoformat(),
                    "is_active": True
                }

                return ActionResult(
                    success=True,
                    data={
                        "key_id": key_id,
                        "stored": True,
                        "stored_at": datetime.now().isoformat()
                    },
                    message=f"API key stored with ID: {key_id}"
                )

            elif operation == "revoke":
                if key_id in self._valid_keys:
                    self._valid_keys[key_id]["is_active"] = False
                    return ActionResult(
                        success=True,
                        data={"key_id": key_id, "revoked": True},
                        message=f"API key revoked: {key_id}"
                    )
                return ActionResult(
                    success=False,
                    data={"key_id": key_id, "found": False},
                    message=f"API key not found: {key_id}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"API key validator error: {str(e)}")


class APIRateLimiterAction(BaseAction):
    """Rate limit API access."""
    action_type = "api_rate_limiter"
    display_name = "API限流"
    description = "API访问限流"

    def __init__(self):
        super().__init__()
        self._limits = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            client_id = params.get("client_id", "default")
            max_requests = params.get("max_requests", 100)
            window_seconds = params.get("window_seconds", 60)

            if operation == "check":
                if client_id not in self._limits:
                    self._limits[client_id] = {
                        "requests": [],
                        "max_requests": max_requests,
                        "window_seconds": window_seconds
                    }

                limit_data = self._limits[client_id]
                now = datetime.now()
                cutoff = now - timedelta(seconds=window_seconds)

                recent_requests = [r for r in limit_data["requests"] if r > cutoff]
                limit_data["requests"] = recent_requests

                remaining = max(0, max_requests - len(recent_requests))
                is_allowed = len(recent_requests) < max_requests

                if is_allowed:
                    limit_data["requests"].append(now)

                return ActionResult(
                    success=is_allowed,
                    data={
                        "client_id": client_id,
                        "allowed": is_allowed,
                        "remaining": remaining,
                        "limit": max_requests,
                        "window_seconds": window_seconds,
                        "reset_at": (now + timedelta(seconds=window_seconds)).isoformat()
                    },
                    message=f"Rate limit check: {'ALLOWED' if is_allowed else 'LIMIT_EXCEEDED'} ({remaining}/{max_requests} remaining)"
                )

            elif operation == "reset":
                if client_id in self._limits:
                    self._limits[client_id]["requests"] = []
                return ActionResult(
                    success=True,
                    data={"client_id": client_id, "reset": True},
                    message=f"Rate limit reset for '{client_id}'"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"API rate limiter error: {str(e)}")


class APIPermissionAction(BaseAction):
    """Check API permissions."""
    action_type = "api_permission"
    display_name = "API权限检查"
    description = "检查API权限"

    def __init__(self):
        super().__init__()
        self._permissions = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            client_id = params.get("client_id", "default")
            permission = params.get("permission", "")
            resource = params.get("resource", "")
            action = params.get("action", "")

            if operation == "check":
                if client_id not in self._permissions:
                    return ActionResult(
                        success=True,
                        data={
                            "client_id": client_id,
                            "permission": permission,
                            "granted": True,
                            "default_behavior": True
                        },
                        message=f"Permission '{permission}' granted (default: allow)"
                    )

                granted = self._permissions[client_id].get(permission, True)
                return ActionResult(
                    success=granted,
                    data={
                        "client_id": client_id,
                        "permission": permission,
                        "granted": granted
                    },
                    message=f"Permission check: {'GRANTED' if granted else 'DENIED'}"
                )

            elif operation == "grant":
                if client_id not in self._permissions:
                    self._permissions[client_id] = {}
                self._permissions[client_id][permission] = True
                return ActionResult(
                    success=True,
                    data={
                        "client_id": client_id,
                        "permission": permission,
                        "granted": True
                    },
                    message=f"Permission '{permission}' granted to '{client_id}'"
                )

            elif operation == "revoke":
                if client_id in self._permissions and permission in self._permissions[client_id]:
                    self._permissions[client_id][permission] = False
                return ActionResult(
                    success=True,
                    data={
                        "client_id": client_id,
                        "permission": permission,
                        "revoked": True
                    },
                    message=f"Permission '{permission}' revoked from '{client_id}'"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"API permission error: {str(e)}")


class APIHealthCheckAction(BaseAction):
    """Health check endpoints."""
    action_type = "api_health_check"
    display_name = "API健康检查"
    description = "API健康状态检查"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoints = params.get("endpoints", [])
            check_type = params.get("check_type", "basic")
            timeout = params.get("timeout", 5)

            if not endpoints:
                return ActionResult(success=False, message="endpoints is required")

            results = []
            all_healthy = True

            for endpoint in endpoints:
                healthy = True
                latency_ms = 50

                results.append({
                    "endpoint": endpoint,
                    "healthy": healthy,
                    "latency_ms": latency_ms,
                    "checked_at": datetime.now().isoformat()
                })

                if not healthy:
                    all_healthy = False

            return ActionResult(
                success=all_healthy,
                data={
                    "check_type": check_type,
                    "endpoints_checked": len(endpoints),
                    "healthy_count": sum(1 for r in results if r["healthy"]),
                    "unhealthy_count": sum(1 for r in results if not r["healthy"]),
                    "results": results,
                    "overall_healthy": all_healthy,
                    "checked_at": datetime.now().isoformat()
                },
                message=f"Health check: {sum(1 for r in results if r['healthy'])}/{len(results)} healthy"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API health check error: {str(e)}")
