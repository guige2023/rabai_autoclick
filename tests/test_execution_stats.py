"""Tests for execution statistics utilities."""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.execution_stats import (
    MAX_HISTORY_SIZE,
    ExecutionStats,
)


class TestExecutionStats:
    """Tests for ExecutionStats."""

    def test_create(self) -> None:
        """Test creating ExecutionStats."""
        stats = ExecutionStats()
        assert stats is not None
        assert stats.history == []

    def test_start_session(self) -> None:
        """Test starting a session."""
        stats = ExecutionStats()
        session = stats.start_session(workflow_name="Test Workflow", loop_count=5)
        assert session is not None
        assert session['workflow_name'] == "Test Workflow"
        assert session['loop_count'] == 5

    def test_record_loop(self) -> None:
        """Test recording loop."""
        stats = ExecutionStats()
        stats.start_session()
        stats.record_loop(loop_index=0, duration=1.5, success=True, step_count=10)
        assert len(stats.current_session['loops']) == 1

    def test_record_step(self) -> None:
        """Test recording step."""
        stats = ExecutionStats()
        stats.start_session()
        stats.record_step(step_type="click", duration=0.5, success=True)
        assert len(stats.current_session['steps']) == 1
        assert stats.current_session['steps'][0]['type'] == "click"

    def test_record_error(self) -> None:
        """Test recording error."""
        stats = ExecutionStats()
        stats.start_session()
        stats.record_error(step_type="click", error_msg="Element not found")
        assert len(stats.current_session['errors']) == 1

    def test_end_session(self) -> None:
        """Test ending session."""
        stats = ExecutionStats()
        stats.start_session()
        session = stats.end_session(success=True)
        assert session is not None
        assert 'total_duration' in session
        assert 'success' in session
        assert session['success'] is True

    def test_end_session_no_active(self) -> None:
        """Test ending session when none active."""
        stats = ExecutionStats()
        result = stats.end_session()
        assert result is None

    def test_get_summary_empty(self) -> None:
        """Test getting summary with no history."""
        stats = ExecutionStats()
        stats.history = []
        summary = stats.get_summary()
        assert summary['total_sessions'] == 0

    def test_get_summary_with_sessions(self) -> None:
        """Test getting summary with sessions."""
        stats = ExecutionStats()
        stats.start_session()
        stats.record_loop(0, 1.0, True, 5)
        stats.end_session(success=True)
        summary = stats.get_summary()
        assert summary['total_sessions'] == 1

    def test_get_recent_sessions(self) -> None:
        """Test getting recent sessions."""
        stats = ExecutionStats()
        stats.start_session()
        stats.end_session()
        recent = stats.get_recent_sessions(limit=10)
        assert isinstance(recent, list)

    def test_get_step_performance(self) -> None:
        """Test getting step performance."""
        stats = ExecutionStats()
        stats.start_session()
        stats.record_step("click", 0.5, True)
        stats.end_session()
        perf = stats.get_step_performance()
        assert isinstance(perf, dict)

    def test_clear_history(self) -> None:
        """Test clearing history."""
        stats = ExecutionStats()
        stats.start_session()
        stats.end_session()
        stats.clear_history()
        assert stats.history == []

    def test_max_history_size(self) -> None:
        """Test max history size constant."""
        assert MAX_HISTORY_SIZE == 1000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])