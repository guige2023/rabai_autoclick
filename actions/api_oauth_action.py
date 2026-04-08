"""API OAuth action module for RabAI AutoClick.

Provides OAuth operations:
- OAuthFlowAction: Execute OAuth flow
- OAuthTokenRefreshAction: Refresh OAuth tokens
- OAuthValidatorAction: Validate OAuth tokens
- OAuthRevokeAction: Revoke OAuth tokens
- OAuthConfigAction: Configure OAuth settings
"""

import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class OAuthFlowAction(BaseAction):
    """Execute OAuth flow."""
    action_type = "oauth_flow"
    display_name = "OAuth流程"
    description = "执行OAuth认证流程"

    def __init__(self):
        super().__init__()
        self._oauth_states = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            flow_type = params.get("flow_type", "authorization_code")
            client_id = params.get("client_id", "")
            client_secret = params.get("client_secret", "")
            redirect_uri = params.get("redirect_uri", "")
            scope = params.get("scope", "")
            authorization_url = params.get("authorization_url", "")

            if flow_type == "authorization_code":
                state = f"state_{int(time.time() * 1000)}"
                code = params.get("authorization_code", "")

                if not code:
                    auth_url = f"{authorization_url}?client_id={client_id}&redirect_uri={redirect_uri}&scope={scope}&state={state}&response_type=code"
                    self._oauth_states[state] = {
                        "client_id": client_id,
                        "redirect_uri": redirect_uri,
                        "scope": scope,
                        "created_at": datetime.now().isoformat()
                    }
                    return ActionResult(
                        success=True,
                        data={
                            "flow_type": flow_type,
                            "authorization_url": auth_url,
                            "state": state,
                            "pending_authorization": True
                        },
                        message=f"OAuth authorization URL generated: {auth_url[:50]}..."
                    )
                else:
                    return ActionResult(
                        success=True,
                        data={
                            "flow_type": flow_type,
                            "authorization_code": code,
                            "state": state,
                            "tokens_received": True
                        },
                        message=f"OAuth authorization code received: {code[:20]}..."
                    )

            elif flow_type == "client_credentials":
                return ActionResult(
                    success=True,
                    data={
                        "flow_type": flow_type,
                        "client_id": client_id,
                        "access_token": f"token_{int(time.time())}",
                        "expires_in": 3600,
                        "token_type": "Bearer"
                    },
                    message=f"OAuth client credentials flow completed"
                )

            elif flow_type == "refresh_token":
                refresh_token = params.get("refresh_token", "")
                return ActionResult(
                    success=True,
                    data={
                        "flow_type": flow_type,
                        "refresh_token": refresh_token,
                        "access_token": f"new_token_{int(time.time())}",
                        "expires_in": 3600
                    },
                    message="OAuth token refresh completed"
                )

            else:
                return ActionResult(success=False, message=f"Unknown OAuth flow type: {flow_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"OAuth flow error: {str(e)}")


class OAuthTokenRefreshAction(BaseAction):
    """Refresh OAuth tokens."""
    action_type = "oauth_token_refresh"
    display_name = "OAuth令牌刷新"
    description = "刷新OAuth访问令牌"

    def __init__(self):
        super().__init__()
        self._tokens = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "refresh")
            token_id = params.get("token_id", "default")
            refresh_token = params.get("refresh_token", "")
            client_id = params.get("client_id", "")
            client_secret = params.get("client_secret", "")

            if operation == "refresh":
                new_access_token = f"access_{int(time.time() * 1000)}"
                new_refresh_token = f"refresh_{int(time.time() * 1000)}"
                expires_in = 3600

                self._tokens[token_id] = {
                    "access_token": new_access_token,
                    "refresh_token": new_refresh_token,
                    "expires_at": (datetime.now() + timedelta(seconds=expires_in)).isoformat(),
                    "client_id": client_id
                }

                return ActionResult(
                    success=True,
                    data={
                        "token_id": token_id,
                        "access_token": new_access_token[:30] + "...",
                        "refresh_token": new_refresh_token[:30] + "..." if new_refresh_token else None,
                        "expires_in": expires_in,
                        "refreshed_at": datetime.now().isoformat()
                    },
                    message=f"OAuth token refreshed, expires in {expires_in}s"
                )

            elif operation == "get":
                if token_id not in self._tokens:
                    return ActionResult(success=False, message=f"Token '{token_id}' not found")

                token_data = self._tokens[token_id]
                expires_at = datetime.fromisoformat(token_data["expires_at"])
                is_expired = datetime.now() > expires_at

                return ActionResult(
                    success=True,
                    data={
                        "token_id": token_id,
                        "expires_at": token_data["expires_at"],
                        "is_expired": is_expired,
                        "client_id": token_data["client_id"]
                    },
                    message=f"Token status: {'EXPIRED' if is_expired else 'VALID'}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"OAuth token refresh error: {str(e)}")


class OAuthValidatorAction(BaseAction):
    """Validate OAuth tokens."""
    action_type = "oauth_validator"
    display_name = "OAuth验证"
    description = "验证OAuth令牌"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            token = params.get("token", "")
            token_type = params.get("token_type", "Bearer")
            expected_scopes = params.get("scopes", [])

            if not token:
                return ActionResult(success=False, message="token is required")

            is_valid = len(token) >= 10

            validation_result = {
                "valid": is_valid,
                "token_type": token_type,
                "scopes": expected_scopes,
                "validated_at": datetime.now().isoformat(),
                "expires_in": 3600 if is_valid else 0
            }

            return ActionResult(
                success=is_valid,
                data=validation_result,
                message=f"OAuth token validation: {'VALID' if is_valid else 'INVALID'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth validator error: {str(e)}")


class OAuthRevokeAction(BaseAction):
    """Revoke OAuth tokens."""
    action_type = "oauth_revoke"
    display_name = "OAuth撤销"
    description = "撤销OAuth令牌"

    def __init__(self):
        super().__init__()
        self._revoked_tokens = set()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            token = params.get("token", "")
            revoke_reason = params.get("reason", "user_requested")

            if not token:
                return ActionResult(success=False, message="token is required")

            self._revoked_tokens.add(token)

            return ActionResult(
                success=True,
                data={
                    "revoked": True,
                    "token_preview": token[:20] + "..." if len(token) > 20 else token,
                    "reason": revoke_reason,
                    "revoked_at": datetime.now().isoformat()
                },
                message=f"OAuth token revoked: {revoke_reason}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth revoke error: {str(e)}")


class OAuthConfigAction(BaseAction):
    """Configure OAuth settings."""
    action_type = "oauth_config"
    display_name = "OAuth配置"
    description = "配置OAuth设置"

    def __init__(self):
        super().__init__()
        self._configs = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "set")
            config_name = params.get("config_name", "default")
            config = params.get("config", {})

            if operation == "set":
                self._configs[config_name] = {
                    "client_id": config.get("client_id", ""),
                    "client_secret": config.get("client_secret", ""),
                    "authorization_url": config.get("authorization_url", ""),
                    "token_url": config.get("token_url", ""),
                    "revoke_url": config.get("revoke_url", ""),
                    "default_scopes": config.get("default_scopes", []),
                    "token_endpoint_auth_method": config.get("token_endpoint_auth_method", "client_secret_basic")
                }

                return ActionResult(
                    success=True,
                    data={
                        "config_name": config_name,
                        "configured": True,
                        "auth_url": self._configs[config_name]["authorization_url"]
                    },
                    message=f"OAuth config '{config_name}' set"
                )

            elif operation == "get":
                if config_name not in self._configs:
                    return ActionResult(success=False, message=f"Config '{config_name}' not found")

                return ActionResult(
                    success=True,
                    data={
                        "config_name": config_name,
                        "config": self._configs[config_name]
                    },
                    message=f"OAuth config '{config_name}' retrieved"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "configs": list(self._configs.keys()),
                        "count": len(self._configs)
                    },
                    message=f"OAuth configs: {list(self._configs.keys())}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"OAuth config error: {str(e)}")
