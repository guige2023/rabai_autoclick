"""Tests for context management utilities."""

import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.context import (
    lock,
    temp_override,
    working_directory,
    suppress,
    ignored,
    ContextStack,
    nested,
    redirect_stdout,
    redirect_stderr,
    capture_stdout,
    capture_stderr,
    ExitStack,
)


class TestLock:
    """Tests for lock context manager."""

    def test_lock_acquire_release(self) -> None:
        """Test acquiring and releasing lock."""
        l = threading.Lock()
        with lock(l):
            assert l.locked()
        assert not l.locked()


class TestTempOverride:
    """Tests for temp_override context manager."""

    def test_temp_override(self) -> None:
        """Test temporarily overriding attribute."""
        class Obj:
            x = 1

        obj = Obj()
        with temp_override(obj, "x", 2):
            assert obj.x == 2
        assert obj.x == 1


class TestWorkingDirectory:
    """Tests for working_directory context manager."""

    def test_working_directory(self) -> None:
        """Test temporarily changing directory."""
        import os
        original = os.getcwd()
        with working_directory("/tmp"):
            assert os.getcwd() == "/tmp"
        assert os.getcwd() == original


class TestSuppress:
    """Tests for suppress context manager."""

    def test_suppress(self) -> None:
        """Test suppressing exceptions."""
        with suppress(ValueError):
            raise ValueError("test")
        # No exception raised


class TestIgnored:
    """Tests for ignored context manager."""

    def test_ignored(self) -> None:
        """Test ignoring exceptions."""
        with ignored(ValueError):
            raise ValueError("test")
        # No exception raised


class TestContextStack:
    """Tests for ContextStack."""

    def test_push_pop(self) -> None:
        """Test pushing and popping."""
        stack = ContextStack()
        ctx = threading.Lock()
        stack.push(ctx)
        assert len(stack) == 1
        stack.pop()

    def test_pop_all(self) -> None:
        """Test popping all."""
        stack = ContextStack()
        l1 = threading.Lock()
        l2 = threading.Lock()
        stack.push(l1)
        stack.push(l2)
        stack.pop_all()
        assert len(stack) == 0


class TestNested:
    """Tests for nested context managers."""

    def test_nested(self) -> None:
        """Test entering multiple managers."""
        l1 = threading.Lock()
        l2 = threading.Lock()
        with nested(l1, l2):
            assert l1.locked()
            assert l2.locked()
        assert not l1.locked()
        assert not l2.locked()


class TestRedirectStdout:
    """Tests for redirect_stdout."""

    def test_redirect(self) -> None:
        """Test redirecting stdout."""
        import io
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            print("test")
        assert "test" in buffer.getvalue()


class TestRedirectStderr:
    """Tests for redirect_stderr."""

    def test_redirect(self) -> None:
        """Test redirecting stderr."""
        import io
        buffer = io.StringIO()
        with redirect_stderr(buffer):
            import sys
            sys.stderr.write("test")
        assert "test" in buffer.getvalue()


class TestCaptureStdout:
    """Tests for capture_stdout."""

    def test_capture(self) -> None:
        """Test capturing stdout."""
        with capture_stdout() as buffer:
            print("hello")
        assert "hello" in buffer.getvalue()


class TestCaptureStderr:
    """Tests for capture_stderr."""

    def test_capture(self) -> None:
        """Test capturing stderr."""
        with capture_stderr() as buffer:
            import sys
            sys.stderr.write("error")
        assert "error" in buffer.getvalue()


class TestExitStack:
    """Tests for ExitStack."""

    def test_enter_exit(self) -> None:
        """Test entering and exiting."""
        with ExitStack() as stack:
            l = threading.Lock()
            stack.enter(l)
            assert l.locked()
        assert not l.locked()

    def test_callback(self) -> None:
        """Test callback registration."""
        result = []
        with ExitStack() as stack:
            stack.callback(lambda: result.append(1))
        assert result == [1]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])