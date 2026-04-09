"""Session manager action for managing user sessions.

Provides session creation, tracking, timeout handling,
and secure session storage.
"""

import hashlib
import logging
import secrets
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class SessionState(Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    REVOKED = "revoked"


@dataclass
class Session:
    session_id: str
    user_id: str
    created_at: float
    last_activity: float
    expires_at: float
    state: SessionState = SessionState.ACTIVE
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class SessionManagerAction:
    """Manage user sessions with secure token generation.

    Args:
        session_timeout: Session timeout in seconds.
        max_sessions_per_user: Maximum concurrent sessions per user.
        enable_refresh_tokens: Enable refresh token functionality.
    """

    def __init__(
        self,
        session_timeout: int = 3600,
        max_sessions_per_user: int = 5,
        enable_refresh_tokens: bool = True,
    ) -> None:
        self._sessions: dict[str, Session] = {}
        self._user_sessions: dict[str, set[str]] = {}
        self._session_timeout = session_timeout
        self._max_sessions_per_user = max_sessions_per_user
        self._enable_refresh_tokens = enable_refresh_tokens
        self._refresh_tokens: dict[str, str] = {}
        self._activity_hooks: list[Callable[[Session], None]] = []

    def create_session(
        self,
        user_id: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        custom_timeout: Optional[int] = None,
    ) -> Optional[str]:
        """Create a new session for a user.

        Args:
            user_id: User ID.
            ip_address: Client IP address.
            user_agent: Client user agent.
            metadata: Optional session metadata.
            custom_timeout: Custom timeout in seconds.

        Returns:
            Session ID or None if creation failed.
        """
        if user_id in self._user_sessions:
            current_count = len(self._user_sessions[user_id])
            if current_count >= self._max_sessions_per_user:
                oldest_session = self._get_oldest_session(user_id)
                if oldest_session:
                    self.revoke_session(oldest_session.session_id)

        session_id = self._generate_session_id()
        now = time.time()
        timeout = custom_timeout or self._session_timeout

        session = Session(
            session_id=session_id,
            user_id=user_id,
            created_at=now,
            last_activity=now,
            expires_at=now + timeout,
            ip_address=ip_address,
            user_agent=user_agent,
            metadata=metadata or {},
        )

        self._sessions[session_id] = session
        if user_id not in self._user_sessions:
            self._user_sessions[user_id] = set()
        self._user_sessions[user_id].add(session_id)

        logger.debug(f"Created session for user {user_id}: {session_id}")
        return session_id

    def _generate_session_id(self) -> str:
        """Generate a secure session ID.

        Returns:
            Session ID string.
        """
        random_bytes = secrets.token_bytes(32)
        timestamp = str(time.time()).encode()
        return hashlib.sha256(random_bytes + timestamp).hexdigest()[:48]

    def _get_oldest_session(self, user_id: str) -> Optional[Session]:
        """Get the oldest active session for a user.

        Args:
            user_id: User ID.

        Returns:
            Oldest session or None.
        """
        session_ids = self._user_sessions.get(user_id, set())
        oldest = None
        for sid in session_ids:
            session = self._sessions.get(sid)
            if session and session.state == SessionState.ACTIVE:
                if oldest is None or session.created_at < oldest.created_at:
                    oldest = session
        return oldest

    def get_session(self, session_id: str) -> Optional[Session]:
        """Get a session by ID.

        Args:
            session_id: Session ID.

        Returns:
            Session object or None.
        """
        return self._sessions.get(session_id)

    def validate_session(self, session_id: str) -> bool:
        """Validate a session and update activity.

        Args:
            session_id: Session ID to validate.

        Returns:
            True if session is valid and active.
        """
        session = self._sessions.get(session_id)
        if not session:
            return False

        if session.state != SessionState.ACTIVE:
            return False

        if time.time() > session.expires_at:
            session.state = SessionState.EXPIRED
            return False

        session.last_activity = time.time()
        session.expires_at = time.time() + self._session_timeout

        for hook in self._activity_hooks:
            try:
                hook(session)
            except Exception as e:
                logger.error(f"Activity hook error: {e}")

        return True

    def refresh_session(self, session_id: str) -> bool:
        """Extend a session's expiration time.

        Args:
            session_id: Session ID.

        Returns:
            True if session was refreshed.
        """
        session = self._sessions.get(session_id)
        if not session or session.state != SessionState.ACTIVE:
            return False

        session.last_activity = time.time()
        session.expires_at = time.time() + self._session_timeout
        return True

    def revoke_session(self, session_id: str) -> bool:
        """Revoke a session.

        Args:
            session_id: Session ID to revoke.

        Returns:
            True if session was found and revoked.
        """
        session = self._sessions.get(session_id)
        if not session:
            return False

        session.state = SessionState.REVOKED
        if session.user_id in self._user_sessions:
            self._user_sessions[session.user_id].discard(session_id)

        logger.debug(f"Revoked session: {session_id}")
        return True

    def revoke_all_user_sessions(self, user_id: str) -> int:
        """Revoke all sessions for a user.

        Args:
            user_id: User ID.

        Returns:
            Number of sessions revoked.
        """
        session_ids = self._user_sessions.get(user_id, set()).copy()
        count = 0
        for sid in session_ids:
            if self.revoke_session(sid):
                count += 1
        return count

    def register_activity_hook(self, hook: Callable[[Session], None]) -> None:
        """Register a callback for session activity.

        Args:
            hook: Callback function.
        """
        self._activity_hooks.append(hook)

    def cleanup_expired(self) -> int:
        """Clean up expired sessions.

        Returns:
            Number of sessions cleaned up.
        """
        now = time.time()
        expired_ids = [
            sid for sid, s in self._sessions.items()
            if s.expires_at < now and s.state == SessionState.ACTIVE
        ]

        for sid in expired_ids:
            self._sessions[sid].state = SessionState.EXPIRED

        return len(expired_ids)

    def get_user_sessions(self, user_id: str) -> list[Session]:
        """Get all sessions for a user.

        Args:
            user_id: User ID.

        Returns:
            List of user sessions.
        """
        session_ids = self._user_sessions.get(user_id, set())
        return [
            self._sessions[sid] for sid in session_ids
            if sid in self._sessions
        ]

    def get_stats(self) -> dict[str, Any]:
        """Get session manager statistics.

        Returns:
            Dictionary with session stats.
        """
        total = len(self._sessions)
        active = sum(1 for s in self._sessions.values() if s.state == SessionState.ACTIVE)
        expired = sum(1 for s in self._sessions.values() if s.state == SessionState.EXPIRED)
        revoked = sum(1 for s in self._sessions.values() if s.state == SessionState.REVOKED)

        return {
            "total_sessions": total,
            "active_sessions": active,
            "expired_sessions": expired,
            "revoked_sessions": revoked,
            "total_users": len(self._user_sessions),
            "session_timeout": self._session_timeout,
            "max_sessions_per_user": self._max_sessions_per_user,
        }
