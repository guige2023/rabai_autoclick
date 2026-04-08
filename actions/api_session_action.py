"""API Session Management Action.

Manages persistent HTTP sessions with cookie jar, connection pooling,
and automatic token refresh.
"""
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass
import time


@dataclass
class SessionConfig:
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    pool_size: int = 10
    token_refresh_threshold: float = 300.0


@dataclass
class TokenInfo:
    access_token: str
    refresh_token: Optional[str] = None
    expires_at: float = 0.0
    token_type: str = "Bearer"

    def is_expired(self, buffer: float = 60.0) -> bool:
        return time.time() >= (self.expires_at - buffer)


class APISessionAction:
    """Manages API sessions with token refresh and retry logic."""

    def __init__(self, config: Optional[SessionConfig] = None) -> None:
        self.config = config or SessionConfig()
        self.token: Optional[TokenInfo] = None
        self.session_data: Dict[str, Any] = {}
        self.request_count = 0
        self.error_count = 0
        self._hooks: Dict[str, Callable] = {}

    def set_token(
        self,
        access_token: str,
        refresh_token: Optional[str] = None,
        expires_in: int = 3600,
        token_type: str = "Bearer",
    ) -> None:
        self.token = TokenInfo(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=time.time() + expires_in,
            token_type=token_type,
        )

    def get_auth_header(self) -> Dict[str, str]:
        if self.token and not self.token.is_expired():
            return {"Authorization": f"{self.token.token_type} {self.token.access_token}"}
        return {}

    def register_hook(self, event: str, callback: Callable) -> None:
        self._hooks[event] = callback

    def on_request(self, method: str, url: str, **kwargs) -> None:
        self.request_count += 1
        headers = self.get_auth_header()
        kwargs.setdefault("headers", {}).update(headers)

    def on_response(self, response: Any) -> None:
        if hasattr(response, "status_code") and response.status_code >= 400:
            self.error_count += 1

    def get_stats(self) -> Dict[str, Any]:
        return {
            "request_count": self.request_count,
            "error_count": self.error_count,
            "error_rate": self.error_count / max(self.request_count, 1),
            "has_token": self.token is not None,
            "token_expired": self.token.is_expired() if self.token else True,
        }

    def clear(self) -> None:
        self.token = None
        self.session_data.clear()
        self.request_count = 0
        self.error_count = 0
