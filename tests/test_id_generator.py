"""Tests for ID generator utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.id_generator import (
    IDGenerator,
    generate_id,
    generate_uuid,
    generate_short_id,
    hash_id,
    generate_workflow_id,
    generate_step_id,
    generate_action_id,
    IDPool,
    ulid,
)


class TestIDGenerator:
    """Tests for IDGenerator."""

    def test_generate_basic(self) -> None:
        """Test basic ID generation."""
        id1 = IDGenerator.generate()
        id2 = IDGenerator.generate()
        assert id1 != id2

    def test_generate_with_prefix(self) -> None:
        """Test ID generation with prefix."""
        id_str = IDGenerator.generate(prefix="test")
        assert "test" in id_str

    def test_generate_with_timestamp(self) -> None:
        """Test ID generation with timestamp."""
        id_str = IDGenerator.generate(include_timestamp=True)
        assert "-" in id_str

    def test_uuid4(self) -> None:
        """Test UUID4 generation."""
        uuid1 = IDGenerator.uuid4()
        uuid2 = IDGenerator.uuid4()
        assert uuid1 != uuid2
        assert "-" in uuid1

    def test_short_id(self) -> None:
        """Test short ID generation."""
        id1 = IDGenerator.short_id()
        assert len(id1) == 8

        id2 = IDGenerator.short_id(length=12)
        assert len(id2) == 12


class TestGenerateFunctions:
    """Tests for convenience functions."""

    def test_generate_id(self) -> None:
        """Test generate_id function."""
        id1 = generate_id()
        id2 = generate_id()
        assert id1 != id2

    def test_generate_uuid(self) -> None:
        """Test generate_uuid function."""
        uuid = generate_uuid()
        assert len(uuid) == 36

    def test_generate_short_id(self) -> None:
        """Test generate_short_id function."""
        id_str = generate_short_id()
        assert len(id_str) == 8

    def test_hash_id(self) -> None:
        """Test hash_id function."""
        id1 = hash_id("content1")
        id2 = hash_id("content1")
        id3 = hash_id("content2")

        assert id1 == id2  # Same content = same hash
        assert id1 != id3

    def test_generate_workflow_id(self) -> None:
        """Test workflow ID generation."""
        wf_id = generate_workflow_id("My Workflow")
        assert "my-workflow" in wf_id
        assert "-" in wf_id

    def test_generate_step_id(self) -> None:
        """Test step ID generation."""
        step_id = generate_step_id("Click Button", 0)
        assert "click_button" in step_id
        assert "0" in step_id

    def test_generate_action_id(self) -> None:
        """Test action ID generation."""
        action_id = generate_action_id("click")
        assert "action_click" in action_id


class TestIDPool:
    """Tests for IDPool."""

    def test_acquire_release(self) -> None:
        """Test acquiring and releasing IDs."""
        pool = IDPool()
        id1 = pool.acquire()
        id2 = pool.acquire()

        assert id1 != id2

        pool.release(id1)
        id3 = pool.acquire()

        assert id3 == id1  # Should get released ID back

    def test_len(self) -> None:
        """Test pool length."""
        pool = IDPool()
        assert len(pool) == 0

        pool.acquire()
        pool.acquire()
        assert len(pool) == 2


class TestULID:
    """Tests for ULID generation."""

    def test_ulid_generation(self) -> None:
        """Test ULID generation."""
        ulid1 = ulid()
        ulid2 = ulid()

        assert ulid1 != ulid2
        assert len(ulid1) == 26


if __name__ == "__main__":
    pytest.main([__file__, "-v"])