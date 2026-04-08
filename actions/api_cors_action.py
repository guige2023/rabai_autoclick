"""CORS API action module for RabAI AutoClick.

Provides CORS operations:
- CORSConfigAction: Configure CORS settings
- CORSPreflightAction: Handle preflight requests
- CORSHeaderAction: Set CORS headers
- CORSOriginAction: Validate allowed origins
- CORSMethodAction: Validate allowed methods
"""

import time
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CORSConfigAction(BaseAction):
    """Configure CORS settings."""
    action_type = "cors_config"
    display_name = "CORS配置"
    description = "配置CORS跨域设置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            allowed_origins = params.get("allowed_origins", ["*"])
            allowed_methods = params.get("allowed_methods", ["GET", "POST", "PUT", "DELETE"])
            allowed_headers = params.get("allowed_headers", ["Content-Type", "Authorization"])
            max_age = params.get("max_age", 3600)
            allow_credentials = params.get("allow_credentials", False)

            if not hasattr(context, "cors_config"):
                context.cors_config = {}
            context.cors_config = {
                "allowed_origins": allowed_origins,
                "allowed_methods": allowed_methods,
                "allowed_headers": allowed_headers,
                "max_age": max_age,
                "allow_credentials": allow_credentials,
                "configured_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"allowed_origins": allowed_origins, "allowed_methods": allowed_methods, "max_age": max_age},
                message=f"CORS configured: {len(allowed_origins)} origins, {len(allowed_methods)} methods",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CORS config failed: {e}")


class CORSPreflightAction(BaseAction):
    """Handle CORS preflight request."""
    action_type = "cors_preflight"
    display_name = "CORS预检"
    description = "处理CORS预检请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            origin = params.get("origin", "")
            method = params.get("method", "")
            request_headers = params.get("request_headers", [])

            if not origin or not method:
                return ActionResult(success=False, message="origin and method are required")

            cors_config = getattr(context, "cors_config", {})
            allowed_origins = cors_config.get("allowed_origins", ["*"])
            allowed_methods = cors_config.get("allowed_methods", [])

            origin_allowed = "*" in allowed_origins or origin in allowed_origins
            method_allowed = method.upper() in allowed_methods

            headers = {
                "Access-Control-Allow-Origin": origin if origin_allowed else "",
                "Access-Control-Allow-Methods": ", ".join(allowed_methods),
                "Access-Control-Allow-Headers": ", ".join(cors_config.get("allowed_headers", [])),
                "Access-Control-Max-Age": str(cors_config.get("max_age", 3600)),
            }

            return ActionResult(
                success=origin_allowed and method_allowed,
                data={"origin": origin, "method": method, "allowed": origin_allowed and method_allowed, "headers": headers},
                message=f"Preflight {method} from {origin}: {'allowed' if origin_allowed and method_allowed else 'denied'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CORS preflight failed: {e}")


class CORSHeaderAction(BaseAction):
    """Set CORS response headers."""
    action_type = "cors_header"
    display_name = "CORS头设置"
    description = "设置CORS响应头"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            origin = params.get("origin", "")
            include_credentials = params.get("include_credentials", False)

            cors_config = getattr(context, "cors_config", {})
            allowed_origins = cors_config.get("allowed_origins", ["*"])

            origin_allowed = "*" in allowed_origins or origin in allowed_origins

            headers = {}
            if origin_allowed:
                headers["Access-Control-Allow-Origin"] = origin if not include_credentials and "*" not in allowed_origins else origin
                if include_credentials:
                    headers["Access-Control-Allow-Credentials"] = "true"
                headers["Access-Control-Expose-Headers"] = ", ".join(cors_config.get("allowed_headers", []))

            return ActionResult(
                success=True,
                data={"origin": origin, "headers": headers},
                message=f"CORS headers set for {origin}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CORS header failed: {e}")


class CORSOriginAction(BaseAction):
    """Validate allowed origins."""
    action_type = "cors_origin"
    display_name = "CORS来源验证"
    description = "验证CORS允许的来源"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            origin = params.get("origin", "")

            if not origin:
                return ActionResult(success=False, message="origin is required")

            cors_config = getattr(context, "cors_config", {})
            allowed_origins = cors_config.get("allowed_origins", ["*"])

            allowed = "*" in allowed_origins or origin in allowed_origins
            matched_origin = origin if origin in allowed_origins else ("*" if "*" in allowed_origins else None)

            return ActionResult(
                success=True,
                data={"origin": origin, "allowed": allowed, "matched_origin": matched_origin},
                message=f"Origin {origin}: {'allowed' if allowed else 'denied'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CORS origin check failed: {e}")


class CORSMethodAction(BaseAction):
    """Validate allowed HTTP methods."""
    action_type = "cors_method"
    display_name = "CORS方法验证"
    description = "验证CORS允许的HTTP方法"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            method = params.get("method", "").upper()

            if not method:
                return ActionResult(success=False, message="method is required")

            cors_config = getattr(context, "cors_config", {})
            allowed_methods = cors_config.get("allowed_methods", ["GET", "POST", "PUT", "DELETE"])

            allowed = method in allowed_methods

            return ActionResult(
                success=True,
                data={"method": method, "allowed": allowed, "allowed_methods": allowed_methods},
                message=f"Method {method}: {'allowed' if allowed else 'denied'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CORS method check failed: {e}")
