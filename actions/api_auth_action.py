"""API authentication action module for RabAI AutoClick.

Provides API authentication operations:
- ApiKeyAuthAction: API key authentication
- BearerTokenAuthAction: Bearer token authentication
- BasicAuthAction: Basic authentication
- DigestAuthAction: Digest authentication
- OAuth1AuthAction: OAuth 1.0 authentication
- JWTAuthAction: JWT token authentication
- HmacAuthAction: HMAC signature authentication
- CustomHeaderAuthAction: Custom header authentication
"""

import hashlib
import hmac
import base64
import time
import secrets
import urllib.parse
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ApiKeyAuthAction(BaseAction):
    """API key authentication."""
    action_type = "api_key_auth"
    display_name = "API密钥认证"
    description = "使用API密钥进行认证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            api_key = params.get("api_key", "")
            key_name = params.get("key_name", "X-API-Key")
            location = params.get("location", "header").lower()

            if not api_key:
                return ActionResult(success=False, message="api_key is required")

            if location == "header":
                auth_data = {key_name: api_key}
            elif location == "query":
                auth_data = {key_name: api_key}
                auth_type = "query_param"
            else:
                return ActionResult(success=False, message=f"Invalid location: {location}")

            return ActionResult(
                success=True,
                data={
                    "auth_type": "api_key",
                    "location": location,
                    "key_name": key_name,
                    "api_key": api_key[:8] + "..." if len(api_key) > 8 else api_key
                },
                message="API key authentication configured"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API key auth error: {str(e)}")


class BearerTokenAuthAction(BaseAction):
    """Bearer token authentication."""
    action_type = "bearer_token_auth"
    display_name = "Bearer令牌认证"
    description = "使用Bearer令牌进行认证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            token = params.get("token", "")
            scheme = params.get("scheme", "Bearer")

            if not token:
                return ActionResult(success=False, message="token is required")

            header_value = f"{scheme} {token}"
            return ActionResult(
                success=True,
                data={
                    "auth_type": "bearer",
                    "scheme": scheme,
                    "token_prefix": scheme + " ",
                    "token_preview": token[:8] + "..." if len(token) > 8 else token
                },
                message="Bearer token authentication configured"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Bearer token auth error: {str(e)}")


class BasicAuthAction(BaseAction):
    """Basic authentication."""
    action_type = "basic_auth"
    display_name = "Basic认证"
    description = "使用用户名密码进行Basic认证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            username = params.get("username", "")
            password = params.get("password", "")

            if not username or not password:
                return ActionResult(success=False, message="username and password are required")

            credentials = f"{username}:{password}"
            encoded = base64.b64encode(credentials.encode()).decode()

            return ActionResult(
                success=True,
                data={
                    "auth_type": "basic",
                    "username": username,
                    "header_value": f"Basic {encoded}",
                    "encoded_length": len(encoded)
                },
                message="Basic authentication configured"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Basic auth error: {str(e)}")


class DigestAuthAction(BaseAction):
    """Digest authentication."""
    action_type = "digest_auth"
    display_name = "Digest认证"
    description = "使用Digest认证协议"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            username = params.get("username", "")
            password = params.get("password", "")
            realm = params.get("realm", "")
            nonce = params.get("nonce", "")
            qop = params.get("qop", "auth")
            opaque = params.get("opaque", "")

            if not username or not password:
                return ActionResult(success=False, message="username and password are required")

            return ActionResult(
                success=True,
                data={
                    "auth_type": "digest",
                    "username": username,
                    "realm": realm,
                    "nonce": nonce[:16] + "..." if len(nonce) > 16 else nonce,
                    "qop": qop
                },
                message="Digest authentication configured"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Digest auth error: {str(e)}")


class OAuth1AuthAction(BaseAction):
    """OAuth 1.0 authentication."""
    action_type = "oauth1_auth"
    display_name = "OAuth1认证"
    description = "OAuth 1.0认证流程"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            consumer_key = params.get("consumer_key", "")
            consumer_secret = params.get("consumer_secret", "")
            access_token = params.get("access_token", "")
            access_secret = params.get("access_secret", "")
            signature_method = params.get("signature_method", "HMAC-SHA1")

            if not consumer_key or not consumer_secret:
                return ActionResult(success=False, message="consumer_key and consumer_secret are required")

            oauth_params = {
                "oauth_consumer_key": consumer_key,
                "oauth_signature_method": signature_method,
                "oauth_timestamp": str(int(time.time())),
                "oauth_nonce": secrets.token_hex(16),
                "oauth_version": "1.0"
            }

            if access_token:
                oauth_params["oauth_token"] = access_token

            return ActionResult(
                success=True,
                data={
                    "auth_type": "oauth1",
                    "consumer_key": consumer_key[:8] + "...",
                    "signature_method": signature_method,
                    "oauth_params_count": len(oauth_params)
                },
                message="OAuth 1.0 authentication configured"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OAuth1 auth error: {str(e)}")


class JWTAuthAction(BaseAction):
    """JWT token authentication."""
    action_type = "jwt_auth"
    display_name = "JWT认证"
    description = "JWT令牌认证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            secret = params.get("secret", "")
            algorithm = params.get("algorithm", "HS256")
            payload = params.get("payload", {})
            subject = params.get("subject", "")
            issuer = params.get("issuer", "")
            expiry_hours = params.get("expiry_hours", 24)

            if not secret:
                return ActionResult(success=False, message="secret is required")

            import jwt

            now = datetime.utcnow()
            exp = now + timedelta(hours=expiry_hours)

            jwt_payload = {
                "iat": now,
                "exp": exp,
                "nbf": now,
                **payload
            }

            if subject:
                jwt_payload["sub"] = subject
            if issuer:
                jwt_payload["iss"] = issuer

            token = jwt.encode(jwt_payload, secret, algorithm=algorithm)

            return ActionResult(
                success=True,
                data={
                    "auth_type": "jwt",
                    "algorithm": algorithm,
                    "subject": subject,
                    "expiry_hours": expiry_hours,
                    "token_preview": token[:20] + "..." if len(token) > 20 else token
                },
                message="JWT authentication configured"
            )
        except ImportError:
            return ActionResult(success=False, message="PyJWT not installed. Install with: pip install PyJWT")
        except Exception as e:
            return ActionResult(success=False, message=f"JWT auth error: {str(e)}")


class HmacAuthAction(BaseAction):
    """HMAC signature authentication."""
    action_type = "hmac_auth"
    display_name = "HMAC签名认证"
    description = "HMAC签名认证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            secret = params.get("secret", "")
            algorithm = params.get("algorithm", "sha256")
            include_timestamp = params.get("include_timestamp", True)
            include_nonce = params.get("include_nonce", True)

            if not secret:
                return ActionResult(success=False, message="secret is required")

            timestamp = str(int(time.time())) if include_timestamp else ""
            nonce = secrets.token_hex(16) if include_nonce else ""

            return ActionResult(
                success=True,
                data={
                    "auth_type": "hmac",
                    "algorithm": algorithm,
                    "secret_length": len(secret),
                    "include_timestamp": include_timestamp,
                    "include_nonce": include_nonce
                },
                message="HMAC authentication configured"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"HMAC auth error: {str(e)}")


class CustomHeaderAuthAction(BaseAction):
    """Custom header authentication."""
    action_type = "custom_header_auth"
    display_name = "自定义头认证"
    description = "使用自定义请求头进行认证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            headers = params.get("headers", {})

            if not headers:
                return ActionResult(success=False, message="headers are required")

            if not isinstance(headers, dict):
                return ActionResult(success=False, message="headers must be a dictionary")

            return ActionResult(
                success=True,
                data={
                    "auth_type": "custom_header",
                    "header_count": len(headers),
                    "header_names": list(headers.keys())
                },
                message="Custom header authentication configured"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Custom header auth error: {str(e)}")


class AuthProviderAction(BaseAction):
    """Authentication provider managing multiple auth methods."""
    action_type = "auth_provider"
    display_name = "认证提供者"
    description = "管理多种认证方式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            auth_configs = params.get("auth_configs", [])
            default_auth = params.get("default_auth", "")

            if not auth_configs:
                return ActionResult(success=False, message="auth_configs is required")

            providers = {}
            for config in auth_configs:
                auth_type = config.get("type", "unknown")
                providers[auth_type] = config

            return ActionResult(
                success=True,
                data={
                    "auth_type": "provider",
                    "available_auths": list(providers.keys()),
                    "default_auth": default_auth,
                    "auth_count": len(providers)
                },
                message=f"Auth provider configured with {len(providers)} auth methods"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Auth provider error: {str(e)}")
