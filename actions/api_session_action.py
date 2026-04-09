"""API Session Action Module.

Provides session management for API interactions including connection
pooling, session persistence, and cookie management.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class SessionState(Enum):
    """Session states."""
    NEW = "new"
    ACTIVE = "active"
    IDLE = "idle"
    EXPIRED = "expired"
    CLOSED = "closed"


@dataclass
class Cookie:
    """Represents an HTTP cookie."""
    name: str
    value: str
    domain: Optional[str] = None
    path: Optional[str] = None
    expires: Optional[float] = None
    http_only: bool = False
    secure: bool = False
    same_site: Optional[str] = None


@dataclass
class APISession:
    """Represents an API session."""
    session_id: str
    name: str
    state: SessionState = SessionState.NEW
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    cookies: Dict[str, Cookie] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    request_count: int = 0
    total_bytes_sent: int = 0
    total_bytes_received: int = 0

    def elapsed_ms(self) -> float:
        """Return elapsed time since creation in milliseconds."""
        return (time.time() - self.created_at) * 1000

    def idle_seconds(self) -> float:
        """Return seconds since last use."""
        return time.time() - self.last_used

    def touch(self) -> None:
        """Update last used timestamp."""
        self.last_used = time.time()

    def is_expired(self, max_idle_seconds: float) -> bool:
        """Check if session has expired due to inactivity."""
        return self.idle_seconds() > max_idle_seconds

    def add_cookie(self, cookie: Cookie) -> None:
        """Add a cookie to the session."""
        self.cookies[cookie.name] = cookie

    def get_cookie(self, name: str) -> Optional[Cookie]:
        """Get a cookie by name."""
        return self.cookies.get(name)

    def remove_cookie(self, name: str) -> bool:
        """Remove a cookie."""
        return self.cookies.pop(name, None) is not None

    def get_cookies_dict(self) -> Dict[str, str]:
        """Get cookies as a dictionary for requests."""
        return {c.name: c.value for c in self.cookies.values()}


class SessionPool:
    """Pool of API sessions."""

    def __init__(
        self,
        max_size: int = 100,
        max_idle_seconds: float = 300,
        max_lifetime_seconds: float = 3600
    ):
        self._sessions: Dict[str, APISession] = {}
        self._max_size = max_size
        self._max_idle = max_idle_seconds
        self._max_lifetime = max_lifetime_seconds
        self._lock = asyncio.Lock()

    async def create_session(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> APISession:
        """Create a new session."""
        async with self._lock:
            # Evict expired sessions if at capacity
            if len(self._sessions) >= self._max_size:
                await self._evict_expired()

            session_id = str(uuid.uuid4())[:8]
            session = APISession(
                session_id=session_id,
                name=name,
                metadata=metadata or {}
            )
            self._sessions[session_id] = session
            return session

    async def get_session(self, session_id: str) -> Optional[APISession]:
        """Get a session by ID."""
        async with self._lock:
            session = self._sessions.get(session_id)
            if session:
                session.touch()
            return session

    async def close_session(self, session_id: str) -> bool:
        """Close a session."""
        async with self._lock:
            session = self._sessions.pop(session_id, None)
            return session is not None

    async def list_sessions(
        self,
        state: Optional[SessionState] = None
    ) -> List[Dict[str, Any]]:
        """List all sessions."""
        async with self._lock:
            sessions = list(self._sessions.values())
            if state:
                sessions = [s for s in sessions if s.state == state]

            return [
                {
                    "session_id": s.session_id,
                    "name": s.name,
                    "state": s.state.value,
                    "created_at": s.created_at,
                    "last_used": s.last_used,
                    "request_count": s.request_count
                }
                for s in sessions
            ]

    async def _evict_expired(self) -> int:
        """Evict expired sessions. Returns count of evicted sessions."""
        now = time.time()
        expired_ids = [
            sid for sid, s in self._sessions.items()
            if s.is_expired(self._max_idle) or
               (now - s.created_at) > self._max_lifetime
        ]

        for sid in expired_ids:
            self._sessions.pop(sid, None)

        return len(expired_ids)

    async def cleanup(self) -> int:
        """Clean up expired sessions. Returns count of cleaned sessions."""
        async with self._lock:
            return await self._evict_expired()

    async def count(self) -> int:
        """Return number of sessions."""
        async with self._lock:
            return len(self._sessions)


class ConnectionPool:
    """Manages connection pooling for HTTP clients."""

    def __init__(
        self,
        max_connections: int = 100,
        max_connections_per_host: int = 10,
        keepalive_seconds: float = 30
    ):
        self._max_connections = max_connections
        self._max_per_host = max_connections_per_host
        self._keepalive = keepalive_seconds
        self._connections: Dict[str, List[Any]] = {}
        self._in_use: Dict[str, Set[str]] = {}
        self._lock = asyncio.Lock()
        self._connection_count = 0

    async def acquire(self, host: str) -> Optional[str]:
        """Acquire a connection for a host."""
        async with self._lock:
            if host not in self._connections:
                self._connections[host] = []
                self._in_use[host] = set()

            # Try to get an idle connection
            while self._connections[host]:
                conn_id = self._connections[host].pop()
                if conn_id not in self._in_use[host]:
                    self._in_use[host].add(conn_id)
                    return conn_id

            # Create new connection if under limits
            total = self._connection_count
            host_count = len(self._in_use[host])

            if total >= self._max_connections or host_count >= self._max_per_host:
                return None

            conn_id = str(uuid.uuid4())[:8]
            self._in_use[host].add(conn_id)
            self._connection_count += 1
            return conn_id

    async def release(self, host: str, conn_id: str) -> None:
        """Release a connection back to the pool."""
        async with self._lock:
            if host in self._in_use:
                self._in_use[host].discard(conn_id)
                if host in self._connections:
                    self._connections[host].append(conn_id)

    async def close_all(self) -> None:
        """Close all connections."""
        async with self._lock:
            self._connections.clear()
            self._in_use.clear()
            self._connection_count = 0


class CookieJar:
    """Manages cookies for sessions."""

    def __init__(self):
        self._cookies: Dict[str, Dict[str, Cookie]] = {}  # domain -> cookies
        self._lock = asyncio.Lock()

    async def set_cookie(self, domain: str, cookie: Cookie) -> None:
        """Set a cookie for a domain."""
        async with self._lock:
            if domain not in self._cookies:
                self._cookies[domain] = {}
            self._cookies[domain][cookie.name] = cookie

    async def get_cookies(self, domain: str) -> Dict[str, str]:
        """Get all cookies for a domain."""
        async with self._lock:
            result = {}
            for d, cookies in self._cookies.items():
                if self._domain_matches(domain, d):
                    for cookie in cookies.values():
                        if self._cookie_valid(cookie):
                            result[cookie.name] = cookie.value
            return result

    async def clear(self, domain: Optional[str] = None) -> None:
        """Clear cookies for a domain or all domains."""
        async with self._lock:
            if domain:
                self._cookies.pop(domain, None)
            else:
                self._cookies.clear()

    @staticmethod
    def _domain_matches(request_domain: str, cookie_domain: str) -> bool:
        """Check if request domain matches cookie domain."""
        if cookie_domain.startswith("."):
            return request_domain.endswith(cookie_domain) or request_domain == cookie_domain[1:]
        return request_domain == cookie_domain

    @staticmethod
    def _cookie_valid(cookie: Cookie) -> bool:
        """Check if cookie is still valid."""
        if cookie.expires is None:
            return True
        return time.time() < cookie.expires


class APISessionAction:
    """Main action class for API session management."""

    def __init__(self):
        self._session_pool = SessionPool()
        self._connection_pool = ConnectionPool()
        self._cookie_jar = CookieJar()

    async def create_session(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new session."""
        session = await self._session_pool.create_session(name, metadata)
        return {
            "session_id": session.session_id,
            "name": session.name,
            "state": session.state.value
        }

    async def get_session(self, session_id: str) -> Optional[APISession]:
        """Get a session by ID."""
        return await self._session_pool.get_session(session_id)

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the API session action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - session_id: Session ID
                - Other operation-specific fields

        Returns:
            Dictionary with operation results.
        """
        operation = context.get("operation", "create")

        if operation == "create":
            result = await self.create_session(
                name=context.get("name", "default"),
                metadata=context.get("metadata")
            )
            return {"success": True, **result}

        elif operation == "get":
            session = await self.get_session(context.get("session_id", ""))
            if session:
                return {
                    "success": True,
                    "session": {
                        "session_id": session.session_id,
                        "name": session.name,
                        "state": session.state.value,
                        "request_count": session.request_count
                    }
                }
            return {"success": False, "error": "Session not found"}

        elif operation == "close":
            success = await self._session_pool.close_session(context.get("session_id", ""))
            return {"success": success}

        elif operation == "list":
            sessions = await self._session_pool.list_sessions()
            return {"success": True, "sessions": sessions}

        elif operation == "set_header":
            session_id = context.get("session_id", "")
            session = await self.get_session(session_id)
            if session:
                key = context.get("key", "")
                value = context.get("value", "")
                session.headers[key] = value
                return {"success": True}
            return {"success": False, "error": "Session not found"}

        elif operation == "get_headers":
            session_id = context.get("session_id", "")
            session = await self.get_session(session_id)
            if session:
                return {"success": True, "headers": session.headers}
            return {"success": False, "error": "Session not found"}

        elif operation == "set_cookie":
            session_id = context.get("session_id", "")
            session = await self.get_session(session_id)
            if session:
                cookie = Cookie(
                    name=context.get("name", ""),
                    value=context.get("value", ""),
                    domain=context.get("domain"),
                    path=context.get("path"),
                    expires=context.get("expires")
                )
                session.add_cookie(cookie)
                return {"success": True}
            return {"success": False, "error": "Session not found"}

        elif operation == "cleanup":
            count = await self._session_pool.cleanup()
            return {"success": True, "cleaned": count}

        elif operation == "stats":
            count = await self._session_pool.count()
            return {
                "success": True,
                "stats": {
                    "active_sessions": count,
                    "max_sessions": self._session_pool._max_size
                }
            }

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
