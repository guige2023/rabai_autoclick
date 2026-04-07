"""Tests for session management utilities."""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.session import (
    SessionStatus,
    SessionInfo,
    Session,
    SessionManager,
    SessionStorage,
)


class TestSessionStatus:
    """Tests for SessionStatus."""

    def test_values(self) -> None:
        """Test status values."""
        assert SessionStatus.ACTIVE.value == "active"
        assert SessionStatus.PAUSED.value == "paused"


class TestSession:
    """Tests for Session."""

    def test_create(self) -> None:
        """Test creating session."""
        session = Session("test-id", "Test Session")
        assert session.session_id == "test-id"
        assert session.name == "Test Session"
        assert session.status == SessionStatus.ACTIVE

    def test_set_status(self) -> None:
        """Test setting status."""
        session = Session("test-id", "Test")
        session.set_status(SessionStatus.PAUSED)
        assert session.status == SessionStatus.PAUSED

    def test_get_set_data(self) -> None:
        """Test getting and setting data."""
        session = Session("test-id", "Test")
        session.set("key", "value")
        assert session.get("key") == "value"

    def test_get_default(self) -> None:
        """Test getting default value."""
        session = Session("test-id", "Test")
        assert session.get("nonexistent", "default") == "default"

    def test_update(self) -> None:
        """Test updating data."""
        session = Session("test-id", "Test")
        session.update({"a": 1, "b": 2})
        assert session.get("a") == 1
        assert session.get("b") == 2

    def test_remove(self) -> None:
        """Test removing data."""
        session = Session("test-id", "Test")
        session.set("key", "value")
        session.remove("key")
        assert session.get("key") is None

    def test_clear(self) -> None:
        """Test clearing data."""
        session = Session("test-id", "Test")
        session.set("key", "value")
        session.clear()
        assert len(session.data) == 0

    def test_to_dict(self) -> None:
        """Test converting to dict."""
        session = Session("test-id", "Test")
        session.set("key", "value")
        d = session.to_dict()
        assert d["session_id"] == "test-id"
        assert d["data"]["key"] == "value"

    def test_from_dict(self) -> None:
        """Test creating from dict."""
        d = {
            "session_id": "test-id",
            "name": "Test",
            "status": "active",
            "created_at": 0,
            "updated_at": 0,
            "data": {"key": "value"},
        }
        session = Session.from_dict(d)
        assert session.session_id == "test-id"
        assert session.get("key") == "value"


class TestSessionManager:
    """Tests for SessionManager."""

    def test_create(self) -> None:
        """Test creating manager."""
        manager = SessionManager()
        assert len(manager._sessions) == 0

    def test_create_session(self) -> None:
        """Test creating session."""
        manager = SessionManager()
        session = manager.create("test-id", "Test")
        assert session is not None
        assert manager.get("test-id") is session

    def test_get_active(self) -> None:
        """Test getting active session."""
        manager = SessionManager()
        manager.create("test-id", "Test")
        assert manager.get_active() is not None

    def test_set_active(self) -> None:
        """Test setting active session."""
        manager = SessionManager()
        manager.create("a", "A")
        manager.create("b", "B")
        manager.set_active("b")
        assert manager.get_active().session_id == "b"

    def test_end_session(self) -> None:
        """Test ending session."""
        manager = SessionManager()
        manager.create("test-id", "Test")
        manager.end("test-id")
        assert manager.get("test-id").status == SessionStatus.ENDED

    def test_delete_session(self) -> None:
        """Test deleting session."""
        manager = SessionManager()
        manager.create("test-id", "Test")
        manager.delete("test-id")
        assert manager.get("test-id") is None

    def test_list_sessions(self) -> None:
        """Test listing sessions."""
        manager = SessionManager()
        manager.create("a", "A")
        manager.create("b", "B")
        sessions = manager.list_sessions()
        assert len(sessions) == 2

    def test_get_by_status(self) -> None:
        """Test filtering by status."""
        manager = SessionManager()
        manager.create("a", "A")
        manager.create("b", "B")
        manager.end("a")
        active = manager.get_by_status(SessionStatus.ACTIVE)
        assert len(active) == 1


class TestSessionStorage:
    """Tests for SessionStorage."""

    def test_save_load(self) -> None:
        """Test saving and loading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = SessionStorage(tmpdir)
            session = Session("test-id", "Test")
            session.set("key", "value")

            assert storage.save(session) is True
            loaded = storage.load("test-id")
            assert loaded is not None
            assert loaded.get("key") == "value"

    def test_delete(self) -> None:
        """Test deleting."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = SessionStorage(tmpdir)
            session = Session("test-id", "Test")
            storage.save(session)
            assert storage.delete("test-id") is True
            assert storage.load("test-id") is None

    def test_list_sessions(self) -> None:
        """Test listing sessions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = SessionStorage(tmpdir)
            storage.save(Session("a", "A"))
            storage.save(Session("b", "B"))
            sessions = storage.list_sessions()
            assert len(sessions) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])