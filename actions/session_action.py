"""Session Action Module.

Provides session management with state persistence,
timeout handling, and session cleanup.
"""

import time
import threading
import hashlib
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SessionStatus(Enum):
    """Session status."""
    ACTIVE = "active"
    IDLE = "idle"
    EXPIRED = "expired"


@dataclass
class Session:
    """User session."""
    session_id: str
    user_id: Optional[str]
    data: Dict[str, Any]
    created_at: float
    last_accessed: float
    timeout_seconds: float
    status: SessionStatus = SessionStatus.ACTIVE


class SessionManager:
    """Manages user sessions."""

    def __init__(self):
        self._sessions: Dict[str, Session] = {}
        self._lock = threading.RLock()
        self._cleanup_interval = 60
        self._last_cleanup = time.time()

    def create_session(
        self,
        user_id: Optional[str] = None,
        timeout_seconds: float = 3600,
        initial_data: Optional[Dict] = None
    ) -> str:
        """Create a new session."""
        session_id = hashlib.md5(
            f"{user_id or 'anon'}{time.time()}".encode()
        ).hexdigest()[:16]

        session = Session(
            session_id=session_id,
            user_id=user_id,
            data=initial_data or {},
            created_at=time.time(),
            last_accessed=time.time(),
            timeout_seconds=timeout_seconds
        )

        with self._lock:
            self._sessions[session_id] = session

        return session_id

    def get_session(self, session_id: str) -> Optional[Session]:
        """Get session by ID."""
        self._cleanup_expired()

        session = self._sessions.get(session_id)
        if session and self._is_expired(session):
            self._expire_session(session_id)
            return None

        if session:
            session.last_accessed = time.time()

        return session

    def update_session(
        self,
        session_id: str,
        data: Optional[Dict] = None
    ) -> bool:
        """Update session data."""
        session = self.get_session(session_id)
        if not session:
            return False

        if data:
            session.data.update(data)
        session.last_accessed = time.time()
        return True

    def delete_session(self, session_id: str) -> bool:
        """Delete a session."""
        with self._lock:
            if session_id in self._sessions:
                del self._sessions[session_id]
                return True
        return False

    def _is_expired(self, session: Session) -> bool:
        """Check if session is expired."""
        return time.time() - session.last_accessed > session.timeout_seconds

    def _expire_session(self, session_id: str) -> None:
        """Mark session as expired."""
        session = self._sessions.get(session_id)
        if session:
            session.status = SessionStatus.EXPIRED

    def _cleanup_expired(self) -> None:
        """Clean up expired sessions."""
        now = time.time()
        if now - self._last_cleanup < self._cleanup_interval:
            return

        with self._lock:
            expired = [
                sid for sid, s in self._sessions.items()
                if self._is_expired(s)
            ]
            for sid in expired:
                self._sessions[sid].status = SessionStatus.EXPIRED

        self._last_cleanup = now

    def get_active_sessions(self, limit: int = 100) -> List[Dict]:
        """Get active sessions."""
        self._cleanup_expired()
        sessions = list(self._sessions.values())[-limit:]
        return [
            {
                "session_id": s.session_id,
                "user_id": s.user_id,
                "status": s.status.value,
                "created_at": s.created_at,
                "last_accessed": s.last_accessed
            }
            for s in sessions
        ]


class SessionAction(BaseAction):
    """Action for session operations."""

    def __init__(self):
        super().__init__("session")
        self._manager = SessionManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute session action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "update":
                return self._update(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "list":
                return self._list(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create a session."""
        session_id = self._manager.create_session(
            user_id=params.get("user_id"),
            timeout_seconds=params.get("timeout_seconds", 3600),
            initial_data=params.get("data")
        )
        return ActionResult(success=True, data={"session_id": session_id})

    def _get(self, params: Dict) -> ActionResult:
        """Get a session."""
        session = self._manager.get_session(params.get("session_id", ""))
        if not session:
            return ActionResult(success=False, message="Session not found")
        return ActionResult(success=True, data={
            "session_id": session.session_id,
            "data": session.data,
            "status": session.status.value
        })

    def _update(self, params: Dict) -> ActionResult:
        """Update session."""
        success = self._manager.update_session(
            params.get("session_id", ""),
            params.get("data")
        )
        return ActionResult(success=success)

    def _delete(self, params: Dict) -> ActionResult:
        """Delete session."""
        success = self._manager.delete_session(params.get("session_id", ""))
        return ActionResult(success=success)

    def _list(self, params: Dict) -> ActionResult:
        """List sessions."""
        sessions = self._manager.get_active_sessions(params.get("limit", 100))
        return ActionResult(success=True, data={"sessions": sessions})
