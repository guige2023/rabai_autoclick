"""In-Memory Session Store for API Users.

This module provides session management for API authentication:
- Session creation and validation
- Sliding window expiration
- Session data storage
- Force logout support

Example:
    >>> from actions.api_session_store_action import SessionStore
    >>> store = SessionStore(default_ttl=3600)
    >>> sid = store.create_session({"user_id": "123"}, metadata={"ip": "1.2.3.4"})
"""

from __future__ import annotations

import secrets
import time
import logging
import threading
import hashlib
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class Session:
    """A user session."""
    session_id: str
    user_id: str
    data: dict[str, Any]
    created_at: float
    last_accessed_at: float
    expires_at: float
    is_active: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


class SessionStore:
    """Thread-safe in-memory session store."""

    def __init__(self, default_ttl: int = 3600, max_sessions: int = 100000) -> None:
        """Initialize the session store.

        Args:
            default_ttl: Default session TTL in seconds.
            max_sessions: Maximum number of sessions to store.
        """
        self._sessions: dict[str, Session] = {}
        self._user_sessions: dict[str, set[str]] = {}
        self._default_ttl = default_ttl
        self._max_sessions = max_sessions
        self._lock = threading.RLock()
        self._stats = {"created": 0, "validated": 0, "expired": 0, "deleted": 0}

    def create_session(
        self,
        user_id: str,
        data: Optional[dict[str, Any]] = None,
        ttl: Optional[int] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """Create a new session.

        Args:
            user_id: The user identifier.
            data: Session data to store.
            ttl: Session TTL in seconds. None = use default.
            metadata: Additional metadata.

        Returns:
            The session ID.
        """
        with self._lock:
            if len(self._sessions) >= self._max_sessions:
                self._evict_oldest()

            session_id = self._generate_session_id()
            now = time.time()
            expires_at = now + (ttl or self._default_ttl)

            session = Session(
                session_id=session_id,
                user_id=user_id,
                data=data or {},
                created_at=now,
                last_accessed_at=now,
                expires_at=expires_at,
                metadata=metadata or {},
            )

            self._sessions[session_id] = session
            if user_id not in self._user_sessions:
                self._user_sessions[user_id] = set()
            self._user_sessions[user_id].add(session_id)
            self._stats["created"] += 1

            logger.info("Created session %s for user %s", session_id[:8], user_id)
            return session_id

    def get_session(self, session_id: str) -> Optional[Session]:
        """Get a session by ID (extends TTL on access).

        Args:
            session_id: The session ID.

        Returns:
            Session if valid and not expired, None otherwise.
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return None

            if not session.is_active:
                return None

            if time.time() > session.expires_at:
                self._expire_session(session_id)
                return None

            session.last_accessed_at = time.time()
            session.expires_at = time.time() + self._default_ttl
            self._stats["validated"] += 1
            return session

    def validate_session(self, session_id: str) -> bool:
        """Check if a session is valid without updating it.

        Args:
            session_id: The session ID.

        Returns:
            True if valid, False otherwise.
        """
        session = self.get_session(session_id)
        return session is not None

    def delete_session(self, session_id: str) -> bool:
        """Delete a session (logout).

        Args:
            session_id: The session ID.

        Returns:
            True if deleted, False if not found.
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return False

            session.is_active = False
            if session.user_id in self._user_sessions:
                self._user_sessions[session.user_id].discard(session_id)
            del self._sessions[session_id]
            self._stats["deleted"] += 1
            logger.info("Deleted session %s", session_id[:8])
            return True

    def delete_user_sessions(self, user_id: str) -> int:
        """Delete all sessions for a user.

        Args:
            user_id: The user ID.

        Returns:
            Number of sessions deleted.
        """
        with self._lock:
            session_ids = self._user_sessions.get(user_id, set()).copy()
            count = 0
            for sid in session_ids:
                if sid in self._sessions:
                    self._sessions[sid].is_active = False
                    del self._sessions[sid]
                    count += 1
            self._user_sessions.pop(user_id, None)
            self._stats["deleted"] += count
            logger.info("Deleted %d sessions for user %s", count, user_id)
            return count

    def extend_session(self, session_id: str, ttl: Optional[int] = None) -> bool:
        """Extend a session's expiration time.

        Args:
            session_id: The session ID.
            ttl: New TTL in seconds. None = use default.

        Returns:
            True if extended, False if session not found.
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None or not session.is_active:
                return False
            session.expires_at = time.time() + (ttl or self._default_ttl)
            return True

    def get_stats(self) -> dict[str, int]:
        """Get session store statistics."""
        with self._lock:
            return {
                **self._stats,
                "active_sessions": len(self._sessions),
                "active_users": len(self._user_sessions),
            }

    def cleanup_expired(self) -> int:
        """Remove expired sessions.

        Returns:
            Number of sessions removed.
        """
        with self._lock:
            now = time.time()
            expired = [
                sid for sid, s in self._sessions.items()
                if now > s.expires_at
            ]
            for sid in expired:
                self._expire_session(sid)
            return len(expired)

    def _expire_session(self, session_id: str) -> None:
        """Internal: expire a session."""
        session = self._sessions.get(session_id)
        if session:
            session.is_active = False
            self._user_sessions.get(session.user_id, set()).discard(session_id)
            del self._sessions[session_id]
            self._stats["expired"] += 1

    def _evict_oldest(self) -> None:
        """Evict the oldest session when at capacity."""
        if not self._sessions:
            return
        oldest = min(self._sessions.values(), key=lambda s: s.last_accessed_at)
        self._expire_session(oldest.session_id)
        logger.warning("Evicted oldest session due to capacity limit")

    def _generate_session_id(self) -> str:
        """Generate a secure session ID."""
        raw = f"{secrets.token_urlsafe(32)}.{time.time()}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def list_user_sessions(self, user_id: str) -> list[Session]:
        """List all active sessions for a user."""
        with self._lock:
            sids = self._user_sessions.get(user_id, set())
            now = time.time()
            result = []
            for sid in sids:
                s = self._sessions.get(sid)
                if s and s.is_active and now <= s.expires_at:
                    result.append(s)
            return result
