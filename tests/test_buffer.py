"""Tests for buffer utilities."""

import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.buffer import (
    CircularBuffer,
    RingBuffer,
    StreamBuffer,
    BufferedWriter,
)


class TestCircularBuffer:
    """Tests for CircularBuffer."""

    def test_append_and_get(self) -> None:
        """Test appending and getting items."""
        buffer = CircularBuffer[int](3)
        buffer.append(1)
        buffer.append(2)
        assert buffer.get(0) == 1
        assert buffer.get(1) == 2

    def test_overwrite_oldest(self) -> None:
        """Test overwriting oldest element when full."""
        buffer = CircularBuffer[int](3)
        buffer.append(1)
        buffer.append(2)
        buffer.append(3)
        buffer.append(4)
        assert buffer.get(0) == 2
        assert buffer.get(1) == 3
        assert buffer.get(2) == 4

    def test_to_list(self) -> None:
        """Test converting buffer to list."""
        buffer = CircularBuffer[int](5)
        buffer.append(1)
        buffer.append(2)
        buffer.append(3)
        assert buffer.to_list() == [1, 2, 3]

    def test_clear(self) -> None:
        """Test clearing buffer."""
        buffer = CircularBuffer[int](3)
        buffer.append(1)
        buffer.clear()
        assert buffer.size == 0

    def test_size_property(self) -> None:
        """Test size property."""
        buffer = CircularBuffer[int](3)
        assert buffer.size == 0
        buffer.append(1)
        assert buffer.size == 1


class TestRingBuffer:
    """Tests for RingBuffer."""

    def test_push_and_pop(self) -> None:
        """Test push and pop."""
        buffer = RingBuffer[int](3)
        buffer.push(1)
        buffer.push(2)
        assert buffer.pop() == 1
        assert buffer.pop() == 2

    def test_pop_empty_returns_none(self) -> None:
        """Test popping empty buffer returns None."""
        buffer = RingBuffer[int](3)
        assert buffer.pop() is None

    def test_peek(self) -> None:
        """Test peeking at oldest item."""
        buffer = RingBuffer[int](3)
        buffer.push(1)
        buffer.push(2)
        assert buffer.peek() == 1

    def test_is_full(self) -> None:
        """Test is_full property."""
        buffer = RingBuffer[int](2)
        buffer.push(1)
        buffer.push(2)
        assert buffer.is_full is True

    def test_is_empty(self) -> None:
        """Test is_empty property."""
        buffer = RingBuffer[int](3)
        assert buffer.is_empty is True
        buffer.push(1)
        assert buffer.is_empty is False


class TestStreamBuffer:
    """Tests for StreamBuffer."""

    def test_write_and_read(self) -> None:
        """Test writing and reading."""
        buffer = StreamBuffer[int]()
        buffer.write(1)
        buffer.write(2)
        assert buffer.read() == 1
        assert buffer.read() == 2

    def test_read_empty_returns_none(self) -> None:
        """Test reading empty buffer returns None."""
        buffer = StreamBuffer[int]()
        assert buffer.read(timeout=0.1) is None

    def test_peek(self) -> None:
        """Test peeking without removing."""
        buffer = StreamBuffer[int]()
        buffer.write(1)
        buffer.write(2)
        assert buffer.peek() == 1
        assert buffer.peek() == 1

    def test_size(self) -> None:
        """Test size method."""
        buffer = StreamBuffer[int]()
        buffer.write(1)
        buffer.write(2)
        assert buffer.size() == 2

    def test_is_empty(self) -> None:
        """Test is_empty method."""
        buffer = StreamBuffer[int]()
        assert buffer.is_empty() is True
        buffer.write(1)
        assert buffer.is_empty() is False


class TestBufferedWriter:
    """Tests for BufferedWriter."""

    def test_write_buffers(self) -> None:
        """Test that write buffers items."""
        written = []

        def writer(data):
            written.extend(data)

        bw = BufferedWriter(writer, buffer_size=3)
        bw.write(1)
        bw.write(2)
        assert len(written) == 0

        bw.write(3)
        assert len(written) == 3

    def test_manual_flush(self) -> None:
        """Test manual flush."""
        written = []

        def writer(data):
            written.extend(data)

        bw = BufferedWriter(writer, buffer_size=10)
        bw.write(1)
        bw.write(2)
        bw.flush()
        assert written == [1, 2]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])