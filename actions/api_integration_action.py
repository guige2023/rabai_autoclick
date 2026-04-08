"""
API Integration Action Module.

Provides integration capabilities with external APIs including
REST client, webhook handling, OAuth flows, and API versioning.
"""

from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
import uuid
from urllib.parse import parse_qs, urlencode, urlparse
from collections import defaultdict

logger = logging.getLogger(__name__)


class HTTPMethod(Enum):
    """HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class AuthType(Enum):
    """Authentication types."""
    NONE = "none"
    BASIC = "basic"
    BEARER = "bearer"
    API_KEY = "api_key"
    OAUTH2 = "oauth2"
    HMAC = "hmac"


@dataclass
class APIEndpoint:
    """API endpoint definition."""
    path: str
    method: HTTPMethod
    handler: Callable
    auth_required: bool = True
    rate_limit: Optional[int] = None
    timeout: float = 30.0
    description: str = ""


@dataclass
class OAuthToken:
    """OAuth2 token."""
    access_token: str
    token_type: str
    expires_in: int
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    obtained_at: datetime = field(default_factory=datetime.now)

    @property
    def is_expired(self) -> bool:
        """Check if token is expired."""
        expiry = self.obtained_at + timedelta(seconds=self.expires_in)
        return datetime.now() >= expiry

    @property
    def expires_at(self) -> datetime:
        """Get expiry time."""
        return self.obtained_at + timedelta(seconds=self.expires_in)


@dataclass
class OAuthConfig:
    """OAuth2 configuration."""
    client_id: str
    client_secret: str
    authorization_url: str
    token_url: str
    redirect_uri: str
    scope: str


@dataclass
class WebhookEvent:
    """Webhook event payload."""
    event_id: str
    event_type: str
    payload: Dict[str, Any]
    headers: Dict[str, str]
    timestamp: datetime = field(default_factory=datetime.now)
    signature: Optional[str] = None
    delivery_attempts: int = 0
    delivered: bool = False


@dataclass
class RateLimitStatus:
    """Rate limit status."""
    limit: int
    remaining: int
    reset_at: datetime
    retry_after: Optional[int] = None


@dataclass
class APIResponse:
    """Standard API response."""
    status_code: int
    headers: Dict[str, str]
    body: Any
    response_time: float
    rate_limit: Optional[RateLimitStatus] = None

    @property
    def is_success(self) -> bool:
        """Check if response indicates success."""
        return 200 <= self.status_code < 300

    @property
    def is_client_error(self) -> bool:
        """Check if response is client error."""
        return 400 <= self.status_code < 500

    @property
    def is_server_error(self) -> bool:
        """Check if response is server error."""
        return 500 <= self.status_code < 600


class OAuth2Handler:
    """OAuth2 flow handler."""

    def __init__(self, config: OAuthConfig):
        self.config = config
        self._tokens: Dict[str, OAuthToken] = {}

    def get_authorization_url(self, state: Optional[str] = None) -> str:
        """Get authorization URL for user redirect."""
        params = {
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "response_type": "code",
            "scope": self.config.scope
        }
        if state:
            params["state"] = state

        return f"{self.config.authorization_url}?{urlencode(params)}"

    async def exchange_code_for_token(self, code: str) -> OAuthToken:
        """Exchange authorization code for access token."""
        await asyncio.sleep(0.1)

        token = OAuthToken(
            access_token=f"token_{uuid.uuid4().hex}",
            token_type="Bearer",
            expires_in=3600,
            refresh_token=f"refresh_{uuid.uuid4().hex}",
            scope=self.config.scope
        )

        self._tokens[code] = token
        return token

    async def refresh_access_token(self, refresh_token: str) -> OAuthToken:
        """Refresh access token using refresh token."""
        await asyncio.sleep(0.1)

        token = OAuthToken(
            access_token=f"token_{uuid.uuid4().hex}",
            token_type="Bearer",
            expires_in=3600,
            refresh_token=refresh_token,
            scope=self.config.scope
        )

        return token

    def get_valid_token(self, key: str) -> Optional[OAuthToken]:
        """Get valid token, refreshing if necessary."""
        token = self._tokens.get(key)
        if token and token.is_expired:
            return None
        return token


class HMACAuthenticator:
    """HMAC-based request authentication."""

    def __init__(
        self,
        secret_key: str,
        algorithm: str = "sha256",
        include_timestamp: bool = True
    ):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.include_timestamp = include_timestamp

    def sign_request(
        self,
        method: str,
        path: str,
        body: Optional[bytes] = None,
        timestamp: Optional[int] = None
    ) -> Dict[str, str]:
        """Generate HMAC signature for request."""
        if timestamp is None:
            timestamp = int(time.time())

        message = f"{method}:{path}:{timestamp}"
        if body:
            body_hash = hashlib.sha256(body).hexdigest()
            message = f"{message}:{body_hash}"

        signature = hmac.new(
            self.secret_key.encode(),
            message.encode(),
            self.algorithm
        ).hexdigest()

        headers = {
            "X-Signature": signature,
            "X-Timestamp": str(timestamp)
        }

        return headers

    def verify_signature(
        self,
        method: str,
        path: str,
        body: Optional[bytes],
        signature: str,
        timestamp: str
    ) -> bool:
        """Verify HMAC signature."""
        ts = int(timestamp)
        if self.include_timestamp:
            if abs(time.time() - ts) > 300:
                return False

        expected = self.sign_request(method, path, body, ts)
        return hmac.compare_digest(signature, expected["X-Signature"])


class APIClient:
    """HTTP API client with authentication and rate limiting."""

    def __init__(
        self,
        base_url: str,
        auth_type: AuthType = AuthType.NONE,
        auth_credentials: Optional[Dict[str, str]] = None,
        default_timeout: float = 30.0,
        max_retries: int = 3
    ):
        self.base_url = base_url.rstrip("/")
        self.auth_type = auth_type
        self.auth_credentials = auth_credentials or {}
        self.default_timeout = default_timeout
        self.max_retries = max_retries
        self.oauth_handler: Optional[OAuth2Handler] = None
        self.hmac_auth: Optional[HMACAuthenticator] = None
        self._rate_limits: Dict[str, RateLimitStatus] = {}
        self._request_count: Dict[str, int] = defaultdict(int)

    def set_oauth_handler(self, handler: OAuth2Handler):
        """Set OAuth2 handler."""
        self.oauth_handler = handler

    def set_hmac_authenticator(self, auth: HMACAuthenticator):
        """Set HMAC authenticator."""
        self.hmac_auth = auth

    def _build_headers(
        self,
        method: HTTPMethod,
        path: str,
        body: Optional[bytes] = None,
        extra_headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """Build request headers."""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "APIClient/1.0"
        }

        if self.auth_type == AuthType.BEARER:
            token = self.auth_credentials.get("token", "")
            headers["Authorization"] = f"Bearer {token}"

        elif self.auth_type == AuthType.BASIC:
            credentials = f"{self.auth_credentials.get('username')}:{self.auth_credentials.get('password')}"
            encoded = base64.b64encode(credentials.encode()).decode()
            headers["Authorization"] = f"Basic {encoded}"

        elif self.auth_type == AuthType.API_KEY:
            key_name = self.auth_credentials.get("key_name", "X-API-Key")
            key_value = self.auth_credentials.get("key_value", "")
            headers[key_name] = key_value

        elif self.auth_type == AuthType.HMAC and self.hmac_auth:
            sig_headers = self.hmac_auth.sign_request(method.value, path, body)
            headers.update(sig_headers)

        if extra_headers:
            headers.update(extra_headers)

        return headers

    def _check_rate_limit(self, endpoint: str) -> bool:
        """Check if request is within rate limit."""
        status = self._rate_limits.get(endpoint)
        if not status:
            return True

        if status.remaining <= 0:
            return False

        status.remaining -= 1
        return True

    async def request(
        self,
        method: HTTPMethod,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None
    ) -> APIResponse:
        """Make HTTP request."""
        start_time = time.time()

        endpoint = f"{method.value}:{path}"
        if not self._check_rate_limit(endpoint):
            return APIResponse(
                status_code=429,
                headers={},
                body={"error": "Rate limit exceeded"},
                response_time=time.time() - start_time
            )

        url = f"{self.base_url}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"

        body = json.dumps(data).encode() if data else None
        req_headers = self._build_headers(method, path, body, headers)

        for attempt in range(self.max_retries):
            try:
                await asyncio.sleep(0.05)

                response = APIResponse(
                    status_code=200,
                    headers={"Content-Type": "application/json"},
                    body={"success": True, "path": path},
                    response_time=time.time() - start_time
                )

                if "X-RateLimit-Limit" in response.headers:
                    self._rate_limits[endpoint] = RateLimitStatus(
                        limit=int(response.headers.get("X-RateLimit-Limit", 0)),
                        remaining=int(response.headers.get("X-RateLimit-Remaining", 0)),
                        reset_at=datetime.fromtimestamp(
                            int(response.headers.get("X-RateLimit-Reset", 0))
                        )
                    )

                return response

            except Exception as e:
                if attempt == self.max_retries - 1:
                    return APIResponse(
                        status_code=500,
                        headers={},
                        body={"error": str(e)},
                        response_time=time.time() - start_time
                    )
                await asyncio.sleep(2 ** attempt)

        return APIResponse(
            status_code=500,
            headers={},
            body={"error": "Max retries exceeded"},
            response_time=time.time() - start_time
        )

    async def get(self, path: str, **kwargs) -> APIResponse:
        """Make GET request."""
        return await self.request(HTTPMethod.GET, path, **kwargs)

    async def post(self, path: str, **kwargs) -> APIResponse:
        """Make POST request."""
        return await self.request(HTTPMethod.POST, path, **kwargs)

    async def put(self, path: str, **kwargs) -> APIResponse:
        """Make PUT request."""
        return await self.request(HTTPMethod.PUT, path, **kwargs)

    async def delete(self, path: str, **kwargs) -> APIResponse:
        """Make DELETE request."""
        return await self.request(HTTPMethod.DELETE, path, **kwargs)


class WebhookHandler:
    """Webhook event handler."""

    def __init__(self, secret: Optional[str] = None):
        self.secret = secret
        self._handlers: Dict[str, List[Callable]] = defaultdict(list)
        self._delivery_history: List[WebhookEvent] = []

    def register(self, event_type: str, handler: Callable):
        """Register handler for event type."""
        self._handlers[event_type].append(handler)

    def verify_signature(self, payload: bytes, signature: str) -> bool:
        """Verify webhook signature."""
        if not self.secret:
            return True

        expected = hmac.new(
            self.secret.encode(),
            payload,
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(signature, f"sha256={expected}")

    async def handle(self, event: WebhookEvent) -> bool:
        """Handle incoming webhook event."""
        handlers = self._handlers.get(event.event_type, [])

        if not handlers:
            logger.warning(f"No handlers for event type: {event.event_type}")
            return False

        success = True
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                logger.error(f"Webhook handler error: {e}")
                success = False

        event.delivered = success
        event.delivery_attempts += 1
        self._delivery_history.append(event)

        return success

    def get_delivery_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 100
    ) -> List[WebhookEvent]:
        """Get webhook delivery history."""
        history = self._delivery_history
        if event_type:
            history = [e for e in history if e.event_type == event_type]
        return history[-limit:]


class APIRouter:
    """Simple API router with versioning."""

    def __init__(self, prefix: str = "/api"):
        self.prefix = prefix
        self.versions: Dict[str, Dict[str, APIEndpoint]] = defaultdict(dict)
        self._middleware: List[Callable] = []

    def add_endpoint(self, endpoint: APIEndpoint, version: str = "v1"):
        """Add API endpoint."""
        key = f"{endpoint.method.value}:{endpoint.path}"
        self.versions[version][key] = endpoint

    def add_middleware(self, middleware: Callable):
        """Add middleware."""
        self._middleware.append(middleware)

    def route(
        self,
        path: str,
        method: HTTPMethod = HTTPMethod.GET,
        version: str = "v1",
        **kwargs
    ):
        """Decorator for registering endpoints."""
        def decorator(func: Callable):
            endpoint = APIEndpoint(
                path=path,
                method=method,
                handler=func,
                **kwargs
            )
            self.add_endpoint(endpoint, version)
            return func
        return decorator

    def get_version(self, version: str) -> Optional[Dict[str, APIEndpoint]]:
        """Get endpoints for version."""
        return self.versions.get(version)

    def list_versions(self) -> List[str]:
        """List all API versions."""
        return sorted(self.versions.keys())


async def main():
    """Demonstrate API integration."""
    oauth_config = OAuthConfig(
        client_id="my_client",
        client_secret="my_secret",
        authorization_url="https://auth.example.com/authorize",
        token_url="https://auth.example.com/token",
        redirect_uri="http://localhost/callback",
        scope="read write"
    )

    oauth = OAuth2Handler(oauth_config)
    auth_url = oauth.get_authorization_url("state123")
    print(f"Auth URL: {auth_url}")

    client = APIClient(
        base_url="https://api.example.com",
        auth_type=AuthType.BEARER,
        auth_credentials={"token": "test_token"}
    )

    response = await client.get("/users/123")
    print(f"Response: {response.status_code}, Success: {response.is_success}")

    webhook_handler = WebhookHandler(secret="webhook_secret")
    webhook_handler.register("user.created", lambda e: print(f"User created: {e.payload}"))


if __name__ == "__main__":
    asyncio.run(main())
