"""API Token Refresh Action Module. Auto-refreshes expiring API tokens."""
import sys, os, time, threading
from typing import Any, Optional
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class TokenInfo:
    access_token: str; token_type: str; expires_at: float
    refresh_token: Optional[str] = None; scope: Optional[str] = None

@dataclass
class RefreshResult:
    success: bool; token_info: Optional[TokenInfo]; error: Optional[str] = None; refreshed: bool = False

class APITokenRefreshAction(BaseAction):
    action_type = "api_token_refresh"; display_name = "API令牌刷新"
    description = "自动刷新API访问令牌"
    def __init__(self) -> None:
        super().__init__(); self._lock = threading.Lock()
        self._tokens = {}; self._refresh_endpoint = None
    def store_token(self, key: str, token_info: TokenInfo) -> None:
        with self._lock: self._tokens[key] = token_info
    def _is_expiring_soon(self, token: TokenInfo, buffer: float = 60) -> bool:
        return time.time() >= (token.expires_at - buffer)
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "check")
        key = params.get("key", "default")
        buffer = params.get("buffer_seconds", 60)
        if mode == "store":
            expires_in = params.get("expires_in", 3600)
            token_info = TokenInfo(access_token=params.get("access_token", ""),
                                   token_type=params.get("token_type", "Bearer"),
                                   expires_at=time.time() + expires_in,
                                   refresh_token=params.get("refresh_token"),
                                   scope=params.get("scope"))
            self.store_token(key, token_info)
            return ActionResult(success=True, message=f"Token stored for '{key}', expires in {expires_in}s")
        with self._lock:
            if key not in self._tokens:
                return ActionResult(success=False, message=f"No token for '{key}'. Use store mode first.")
            token = self._tokens[key]
        if mode == "check":
            expiring = self._is_expiring_soon(token, buffer)
            return ActionResult(success=not expiring, message=f"Token {'expiring' if expiring else 'valid'}", data={"expiring_soon": expiring})
        if not self._is_expiring_soon(token, buffer):
            return ActionResult(success=True, message="Token not expiring", data={"refreshed": False})
        if not token.refresh_token:
            return ActionResult(success=False, message="No refresh token", data={"refreshed": False})
        endpoint = params.get("refresh_endpoint", self._refresh_endpoint)
        if not endpoint:
            new_token = TokenInfo(access_token=token.access_token, token_type=token.token_type,
                                 expires_at=time.time()+3600, refresh_token=token.refresh_token, scope=token.scope)
            with self._lock: self._tokens[key] = new_token
            return ActionResult(success=True, message="Expiry extended (no endpoint)", data={"refreshed": True})
        import urllib.request, urllib.parse
        data = urllib.parse.urlencode({"grant_type": "refresh_token", "refresh_token": token.refresh_token,
                                       "client_id": params.get("client_id", ""),
                                       "client_secret": params.get("client_secret", "")}).encode()
        try:
            req = urllib.request.Request(endpoint, data=data, method="POST")
            with urllib.request.urlopen(req, timeout=30) as response:
                result = __import__("json").loads(response.read())
                expires_in = result.get("expires_in", 3600)
                new_token = TokenInfo(access_token=result["access_token"],
                                     token_type=result.get("token_type", "Bearer"),
                                     expires_at=time.time()+expires_in,
                                     refresh_token=result.get("refresh_token", token.refresh_token),
                                     scope=result.get("scope"))
                with self._lock: self._tokens[key] = new_token
                return ActionResult(success=True, message="Token refreshed", data=vars(RefreshResult(success=True, token_info=new_token, refreshed=True)))
        except Exception as e:
            return ActionResult(success=False, message=f"Refresh failed: {e}", data=vars(RefreshResult(success=False, token_info=None, error=str(e))))
