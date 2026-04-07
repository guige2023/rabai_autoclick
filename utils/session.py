"""Session management utilities for RabAI AutoClick.

Provides:
- Session creation
- Session state
- Session persistence
"""

import json
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class SessionStatus(Enum):
    """Session status."""
    ACTIVE = "active"
    PAUSED = "paused"
    ENDED = "ended"


@dataclass
class SessionInfo:
    """Session information."""
    session_id: str
    name: str
    status: SessionStatus
    created_at: float
    updated_at: float
    data: Dict[str, Any] = field(default_factory=dict)


class Session:
    """A single session."""

    def __init__(self, session_id: str, name: str) -> None:
        """Initialize session.

        Args:
            session_id: Unique session ID.
            name: Session name.
        """
        self._session_id = session_id
        self._name = name
        self._status = SessionStatus.ACTIVE
        self._created_at = time.time()
        self._updated_at = self._created_at
        self._data: Dict[str, Any] = {}
        self._state_handlers: Dict[str, List[Callable]] = {}

    @property
    def session_id(self) -> str:
        """Get session ID."""
        return self._session_id

    @property
    def name(self) -> str:
        """Get session name."""
        return self._name

    @property
    def status(self) -> SessionStatus:
        """Get session status."""
        return self._status

    @property
    def created_at(self) -> float:
        """Get creation timestamp."""
        return self._created_at

    @property
    def data(self) -> Dict[str, Any]:
        """Get session data."""
        return self._data.copy()

    def set_status(self, status: SessionStatus) -> None:
        """Set session status.

        Args:
            status: New status.
        """
        self._status = status
        self._updated_at = time.time()
        self._notify_handlers("status", status)

    def get(self, key: str, default: Any = None) -> Any:
        """Get data value.

        Args:
            key: Data key.
            default: Default if not found.

        Returns:
            Value or default.
        """
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set data value.

        Args:
            key: Data key.
            value: Value to set.
        """
        self._data[key] = value
        self._updated_at = time.time()
        self._notify_handlers("data", key, value)

    def update(self, data: Dict[str, Any]) -> None:
        """Update multiple data values.

        Args:
            data: Dict of values to update.
        """
        self._data.update(data)
        self._updated_at = time.time()

    def remove(self, key: str) -> bool:
        """Remove data value.

        Args:
            key: Data key to remove.

        Returns:
            True if removed.
        """
        if key in self._data:
            del self._data[key]
            self._updated_at = time.time()
            return True
        return False

    def clear(self) -> None:
        """Clear all data."""
        self._data.clear()
        self._updated_at = time.time()

    def on_change(
        self,
        handler: Callable[[str, Any], None],
    ) -> None:
        """Register change handler.

        Args:
            handler: Function to call on change.
        """
        if "change" not in self._state_handlers:
            self._state_handlers["change"] = []
        self._state_handlers["change"].append(handler)

    def _notify_handlers(self, *args: Any) -> None:
        """Notify state handlers."""
        if "change" in self._state_handlers:
            for handler in self._state_handlers["change"]:
                try:
                    handler(*args)
                except Exception:
                    pass

    def to_dict(self) -> dict:
        """Convert to dictionary.

        Returns:
            Session as dict.
        """
        return {
            "session_id": self._session_id,
            "name": self._name,
            "status": self._status.value,
            "created_at": self._created_at,
            "updated_at": self._updated_at,
            "data": self._data,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Session":
        """Create from dictionary.

        Args:
            data: Session data.

        Returns:
            Session instance.
        """
        session = cls(
            session_id=data["session_id"],
            name=data["name"],
        )
        session._status = SessionStatus(data["status"])
        session._created_at = data["created_at"]
        session._updated_at = data["updated_at"]
        session._data = data.get("data", {})
        return session


class SessionManager:
    """Manage multiple sessions."""

    def __init__(self) -> None:
        """Initialize manager."""
        self._sessions: Dict[str, Session] = {}
        self._active_session: Optional[str] = None

    def create(
        self,
        session_id: str,
        name: str,
        make_active: bool = True,
    ) -> Session:
        """Create a new session.

        Args:
            session_id: Unique session ID.
            name: Session name.
            make_active: Set as active session.

        Returns:
            Created session.
        """
        session = Session(session_id, name)
        self._sessions[session_id] = session
        if make_active:
            self._active_session = session_id
        return session

    def get(self, session_id: str) -> Optional[Session]:
        """Get session by ID.

        Args:
            session_id: Session ID.

        Returns:
            Session or None.
        """
        return self._sessions.get(session_id)

    def get_active(self) -> Optional[Session]:
        """Get active session.

        Returns:
            Active session or None.
        """
        if self._active_session:
            return self._sessions.get(self._active_session)
        return None

    def set_active(self, session_id: str) -> bool:
        """Set active session.

        Args:
            session_id: Session ID to make active.

        Returns:
            True if set successfully.
        """
        if session_id in self._sessions:
            self._active_session = session_id
            return True
        return False

    def end(self, session_id: str) -> bool:
        """End a session.

        Args:
            session_id: Session ID to end.

        Returns:
            True if ended.
        """
        session = self._sessions.get(session_id)
        if session:
            session.set_status(SessionStatus.ENDED)
            if self._active_session == session_id:
                self._active_session = None
            return True
        return False

    def delete(self, session_id: str) -> bool:
        """Delete a session.

        Args:
            session_id: Session ID to delete.

        Returns:
            True if deleted.
        """
        if session_id in self._sessions:
            del self._sessions[session_id]
            if self._active_session == session_id:
                self._active_session = None
            return True
        return False

    def list_sessions(self) -> List[Session]:
        """List all sessions.

        Returns:
            List of sessions.
        """
        return list(self._sessions.values())

    def get_by_status(self, status: SessionStatus) -> List[Session]:
        """Get sessions by status.

        Args:
            status: Status to filter by.

        Returns:
            List of matching sessions.
        """
        return [s for s in self._sessions.values() if s.status == status]


class SessionStorage:
    """Persist sessions to disk."""

    def __init__(self, directory: str) -> None:
        """Initialize storage.

        Args:
            directory: Directory to store sessions.
        """
        self._directory = directory
        os.makedirs(directory, exist_ok=True)

    def save(self, session: Session) -> bool:
        """Save session to disk.

        Args:
            session: Session to save.

        Returns:
            True if saved.
        """
        path = self._get_path(session.session_id)
        try:
            with open(path, "w") as f:
                json.dump(session.to_dict(), f, indent=2)
            return True
        except Exception:
            return False

    def load(self, session_id: str) -> Optional[Session]:
        """Load session from disk.

        Args:
            session_id: Session ID to load.

        Returns:
            Session or None.
        """
        path = self._get_path(session_id)
        if not os.path.exists(path):
            return None

        try:
            with open(path, "r") as f:
                data = json.load(f)
            return Session.from_dict(data)
        except Exception:
            return None

    def delete(self, session_id: str) -> bool:
        """Delete session from disk.

        Args:
            session_id: Session ID to delete.

        Returns:
            True if deleted.
        """
        path = self._get_path(session_id)
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    def list_sessions(self) -> List[str]:
        """List stored session IDs.

        Returns:
            List of session IDs.
        """
        if not os.path.exists(self._directory):
            return []

        sessions = []
        for filename in os.listdir(self._directory):
            if filename.endswith(".json"):
                sessions.append(filename[:-5])
        return sessions

    def _get_path(self, session_id: str) -> str:
        """Get file path for session.

        Args:
            session_id: Session ID.

        Returns:
            File path.
        """
        return os.path.join(self._directory, f"{session_id}.json")
