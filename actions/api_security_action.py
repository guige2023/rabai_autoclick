"""API security action module for RabAI AutoClick.

Provides security operations for API interactions:
- APIKeyValidationAction: Validate API keys
- TokenGeneratorAction: Generate secure tokens
- RateLimitEnforcementAction: Enforce rate limits
- RequestSigningAction: Sign API requests
- IPWhitelistAction: IP whitelist management
"""

from typing import Any, Dict, List, Optional
import hashlib
import hmac
import secrets
import base64
import time

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APIKeyValidationAction(BaseAction):
    """Validate API keys."""
    action_type = "api_key_validation"
    display_name = "API密钥验证"
    description = "验证API密钥"
    
    def __init__(self):
        super().__init__()
        self._valid_keys: Dict[str, Dict] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "validate")
            
            if operation == "validate":
                return self._validate_key(params)
            elif operation == "register":
                return self._register_key(params)
            elif operation == "revoke":
                return self._revoke_key(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _validate_key(self, params: Dict[str, Any]) -> ActionResult:
        api_key = params.get("api_key")
        
        if not api_key:
            return ActionResult(success=False, message="api_key is required")
        
        if api_key not in self._valid_keys:
            return ActionResult(
                success=False,
                message="Invalid API key",
                data={"valid": False}
            )
        
        key_info = self._valid_keys[api_key]
        
        if key_info.get("expires_at"):
            if time.time() > key_info["expires_at"]:
                return ActionResult(
                    success=False,
                    message="API key has expired",
                    data={"valid": False, "expired": True}
                )
        
        return ActionResult(
            success=True,
            message="API key is valid",
            data={"valid": True, "key_info": key_info}
        )
    
    def _register_key(self, params: Dict[str, Any]) -> ActionResult:
        api_key = params.get("api_key", self._generate_key())
        name = params.get("name", "unnamed")
        scopes = params.get("scopes", [])
        expires_in = params.get("expires_in")
        
        key_info = {
            "name": name,
            "scopes": scopes,
            "created_at": time.time()
        }
        
        if expires_in:
            key_info["expires_at"] = time.time() + expires_in
        
        self._valid_keys[api_key] = key_info
        
        return ActionResult(
            success=True,
            message="API key registered",
            data={"api_key": api_key, "key_info": key_info}
        )
    
    def _revoke_key(self, params: Dict[str, Any]) -> ActionResult:
        api_key = params.get("api_key")
        
        if api_key in self._valid_keys:
            del self._valid_keys[api_key]
            return ActionResult(success=True, message="API key revoked")
        
        return ActionResult(success=False, message="API key not found")
    
    def _generate_key(self) -> str:
        return secrets.token_urlsafe(32)


class TokenGeneratorAction(BaseAction):
    """Generate secure tokens."""
    action_type = "token_generator"
    display_name = "令牌生成"
    description = "生成安全令牌"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            token_type = params.get("type", "random")
            length = params.get("length", 32)
            
            if token_type == "random":
                token = secrets.token_urlsafe(length)
            elif token_type == "hex":
                token = secrets.token_hex(length)
            elif token_type == "bytes":
                token = base64.b64encode(secrets.token_bytes(length)).decode()
            else:
                return ActionResult(success=False, message=f"Unknown token type: {token_type}")
            
            return ActionResult(
                success=True,
                message=f"Token generated ({token_type})",
                data={
                    "token": token,
                    "type": token_type,
                    "length": length
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class RateLimitEnforcementAction(BaseAction):
    """Enforce rate limits."""
    action_type = "rate_limit_enforcement"
    display_name = "限流执行"
    description = "执行API限流"
    
    def __init__(self):
        super().__init__()
        self._limits: Dict[str, List[float]] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            
            if operation == "check":
                return self._check_rate_limit(params)
            elif operation == "configure":
                return self._configure_limit(params)
            elif operation == "reset":
                return self._reset_limit(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _check_rate_limit(self, params: Dict[str, Any]) -> ActionResult:
        key = params.get("key", "default")
        limit = params.get("limit", 100)
        window = params.get("window", 60)
        
        if key not in self._limits:
            self._limits[key] = []
        
        now = time.time()
        cutoff = now - window
        
        self._limits[key] = [t for t in self._limits[key] if t > cutoff]
        
        remaining = limit - len(self._limits[key])
        
        if remaining > 0:
            self._limits[key].append(now)
            return ActionResult(
                success=True,
                message="Request allowed",
                data={
                    "allowed": True,
                    "remaining": remaining - 1,
                    "limit": limit,
                    "window": window
                }
            )
        else:
            oldest = min(self._limits[key])
            retry_after = oldest + window - now
            
            return ActionResult(
                success=False,
                message="Rate limit exceeded",
                data={
                    "allowed": False,
                    "remaining": 0,
                    "limit": limit,
                    "window": window,
                    "retry_after": retry_after
                }
            )
    
    def _configure_limit(self, params: Dict[str, Any]) -> ActionResult:
        key = params.get("key", "default")
        limit = params.get("limit", 100)
        window = params.get("window", 60)
        
        return ActionResult(
            success=True,
            message=f"Rate limit configured for {key}",
            data={
                "key": key,
                "limit": limit,
                "window": window
            }
        )
    
    def _reset_limit(self, params: Dict[str, Any]) -> ActionResult:
        key = params.get("key", "default")
        
        if key in self._limits:
            count = len(self._limits[key])
            self._limits[key] = []
            return ActionResult(
                success=True,
                message=f"Rate limit reset for {key}",
                data={"cleared_requests": count}
            )
        
        return ActionResult(success=True, message="No rate limit to reset")


class RequestSigningAction(BaseAction):
    """Sign API requests."""
    action_type = "request_signing"
    display_name = "请求签名"
    description = "签名API请求"
    
    def __init__(self):
        super().__init__()
        self._secret_key: Optional[str] = None
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "sign")
            
            if operation == "sign":
                return self._sign_request(params)
            elif operation == "verify":
                return self._verify_signature(params)
            elif operation == "set_key":
                return self._set_secret_key(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _sign_request(self, params: Dict[str, Any]) -> ActionResult:
        if not self._secret_key:
            return ActionResult(success=False, message="Secret key not set")
        
        method = params.get("method", "GET")
        path = params.get("path", "/")
        body = params.get("body", "")
        timestamp = params.get("timestamp", str(int(time.time())))
        
        message = f"{method}:{path}:{timestamp}:{body}"
        
        signature = hmac.new(
            self._secret_key.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return ActionResult(
            success=True,
            message="Request signed",
            data={
                "signature": signature,
                "timestamp": timestamp,
                "message": message
            }
        )
    
    def _verify_signature(self, params: Dict[str, Any]) -> ActionResult:
        if not self._secret_key:
            return ActionResult(success=False, message="Secret key not set")
        
        signature = params.get("signature")
        method = params.get("method", "GET")
        path = params.get("path", "/")
        body = params.get("body", "")
        timestamp = params.get("timestamp")
        
        message = f"{method}:{path}:{timestamp}:{body}"
        
        expected = hmac.new(
            self._secret_key.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        
        valid = hmac.compare_digest(signature, expected)
        
        return ActionResult(
            success=True,
            message="Signature verified",
            data={"valid": valid}
        )
    
    def _set_secret_key(self, params: Dict[str, Any]) -> ActionResult:
        secret_key = params.get("secret_key")
        
        if not secret_key:
            return ActionResult(success=False, message="secret_key is required")
        
        self._secret_key = secret_key
        
        return ActionResult(
            success=True,
            message="Secret key set"
        )


class IPWhitelistAction(BaseAction):
    """IP whitelist management."""
    action_type = "ip_whitelist"
    display_name = "IP白名单"
    description = "管理IP白名单"
    
    def __init__(self):
        super().__init__()
        self._whitelist: set = set()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            
            if operation == "check":
                return self._check_ip(params)
            elif operation == "add":
                return self._add_ip(params)
            elif operation == "remove":
                return self._remove_ip(params)
            elif operation == "list":
                return self._list_ips()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _check_ip(self, params: Dict[str, Any]) -> ActionResult:
        ip = params.get("ip")
        
        if not ip:
            return ActionResult(success=False, message="ip is required")
        
        if not self._whitelist:
            return ActionResult(
                success=True,
                message="Whitelist is empty, all IPs allowed",
                data={"whitelisted": True, "allow_all": True}
            )
        
        whitelisted = ip in self._whitelist
        
        return ActionResult(
            success=True,
            message=f"IP {'is' if whitelisted else 'is not'} whitelisted",
            data={
                "ip": ip,
                "whitelisted": whitelisted
            }
        )
    
    def _add_ip(self, params: Dict[str, Any]) -> ActionResult:
        ip = params.get("ip")
        
        if not ip:
            return ActionResult(success=False, message="ip is required")
        
        self._whitelist.add(ip)
        
        return ActionResult(
            success=True,
            message=f"IP {ip} added to whitelist",
            data={"whitelist_count": len(self._whitelist)}
        )
    
    def _remove_ip(self, params: Dict[str, Any]) -> ActionResult:
        ip = params.get("ip")
        
        if not ip:
            return ActionResult(success=False, message="ip is required")
        
        if ip in self._whitelist:
            self._whitelist.remove(ip)
            return ActionResult(
                success=True,
                message=f"IP {ip} removed from whitelist",
                data={"whitelist_count": len(self._whitelist)}
            )
        
        return ActionResult(success=False, message=f"IP {ip} not found in whitelist")
    
    def _list_ips(self) -> ActionResult:
        return ActionResult(
            success=True,
            message=f"{len(self._whitelist)} IPs in whitelist",
            data={"whitelist": list(self._whitelist)}
        )
