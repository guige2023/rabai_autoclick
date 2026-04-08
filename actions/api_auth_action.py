"""API auth token action module for RabAI AutoClick.

Provides API token/auth operations:
- TokenGenerateAction: Generate auth token
- TokenValidateAction: Validate token
- TokenRefreshAction: Refresh token
- TokenRevokeAction: Revoke token
- TokenInfoAction: Get token info
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


class TokenGenerateAction(BaseAction):
    """Generate an authentication token."""
    action_type = "token_generate"
    display_name = "生成Token"
    description = "生成认证令牌"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            user_id = params.get("user_id", "")
            token_type = params.get("type", "bearer")
            expires_in = params.get("expires_in", 3600)
            scopes = params.get("scopes", [])

            if not user_id:
                return ActionResult(success=False, message="user_id is required")

            token_value = hashlib.sha256(f"{user_id}:{time.time()}:{uuid.uuid4()}".encode()).hexdigest()
            token_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "auth_tokens"):
                context.auth_tokens = {}
            context.auth_tokens[token_id] = {
                "token_id": token_id,
                "user_id": user_id,
                "token": token_value,
                "type": token_type,
                "scopes": scopes,
                "created_at": time.time(),
                "expires_at": time.time() + expires_in,
                "revoked": False,
            }

            return ActionResult(
                success=True,
                data={"token_id": token_id, "user_id": user_id, "expires_in": expires_in, "scopes": scopes},
                message=f"Token {token_id} generated for user {user_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Token generate failed: {e}")


class TokenValidateAction(BaseAction):
    """Validate an auth token."""
    action_type = "token_validate"
    display_name = "验证Token"
    description = "验证认证令牌"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            token_id = params.get("token_id", "")
            if not token_id:
                return ActionResult(success=False, message="token_id is required")

            tokens = getattr(context, "auth_tokens", {})
            if token_id not in tokens:
                return ActionResult(success=True, data={"valid": False, "reason": "not_found"}, message="Token not found")

            token = tokens[token_id]
            if token.get("revoked"):
                return ActionResult(success=True, data={"valid": False, "reason": "revoked"}, message="Token revoked")

            if token.get("expires_at", 0) < time.time():
                return ActionResult(success=True, data={"valid": False, "reason": "expired"}, message="Token expired")

            return ActionResult(
                success=True,
                data={"valid": True, "user_id": token["user_id"], "scopes": token.get("scopes", [])},
                message="Token is valid",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Token validate failed: {e}")


class TokenRefreshAction(BaseAction):
    """Refresh an auth token."""
    action_type = "token_refresh"
    display_name = "刷新Token"
    description = "刷新认证令牌"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            token_id = params.get("token_id", "")
            expires_in = params.get("expires_in", 3600)

            if not token_id:
                return ActionResult(success=False, message="token_id is required")

            tokens = getattr(context, "auth_tokens", {})
            if token_id not in tokens:
                return ActionResult(success=False, message=f"Token {token_id} not found")

            token = tokens[token_id]
            new_token = hashlib.sha256(f"{token['user_id']}:{time.time()}:{uuid.uuid4()}".encode()).hexdigest()
            token["token"] = new_token
            token["expires_at"] = time.time() + expires_in

            return ActionResult(
                success=True,
                data={"token_id": token_id, "new_token": new_token[:16] + "...", "expires_in": expires_in},
                message=f"Token {token_id} refreshed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Token refresh failed: {e}")


class TokenRevokeAction(BaseAction):
    """Revoke an auth token."""
    action_type = "token_revoke"
    display_name = "撤销Token"
    description = "撤销认证令牌"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            token_id = params.get("token_id", "")
            if not token_id:
                return ActionResult(success=False, message="token_id is required")

            tokens = getattr(context, "auth_tokens", {})
            if token_id not in tokens:
                return ActionResult(success=False, message=f"Token {token_id} not found")

            tokens[token_id]["revoked"] = True
            tokens[token_id]["revoked_at"] = time.time()

            return ActionResult(success=True, data={"token_id": token_id}, message=f"Token {token_id} revoked")
        except Exception as e:
            return ActionResult(success=False, message=f"Token revoke failed: {e}")


class TokenInfoAction(BaseAction):
    """Get token information."""
    action_type = "token_info"
    display_name = "Token信息"
    description = "获取令牌信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            token_id = params.get("token_id", "")
            if not token_id:
                return ActionResult(success=False, message="token_id is required")

            tokens = getattr(context, "auth_tokens", {})
            if token_id not in tokens:
                return ActionResult(success=False, message=f"Token {token_id} not found")

            token = tokens[token_id]
            return ActionResult(
                success=True,
                data={
                    "token_id": token_id,
                    "user_id": token["user_id"],
                    "type": token["type"],
                    "scopes": token.get("scopes", []),
                    "created_at": token["created_at"],
                    "expires_at": token["expires_at"],
                    "revoked": token["revoked"],
                },
                message=f"Token info: user={token['user_id']}, revoked={token['revoked']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Token info failed: {e}")
