"""Tests for pipeline utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.pipeline import Pipeline, Stream, pipe


class TestPipeline:
    """Tests for Pipeline."""

    def test_map(self) -> None:
        """Test map transformation."""
        result = (
            Pipeline([1, 2, 3])
            .map(lambda x: x * 2)
            .collect()
        )
        assert result == [2, 4, 6]

    def test_filter(self) -> None:
        """Test filter transformation."""
        result = (
            Pipeline([1, 2, 3, 4, 5])
            .filter(lambda x: x > 2)
            .collect()
        )
        assert result == [3, 4, 5]

    def test_flat_map(self) -> None:
        """Test flatmap transformation."""
        result = (
            Pipeline([1, 2])
            .flat_map(lambda x: [x, x * 2])
            .collect()
        )
        assert result == [1, 2, 2, 4]

    def test_reduce(self) -> None:
        """Test reduce operation."""
        result = (
            Pipeline([1, 2, 3, 4])
            .map(lambda x: x * 2)
            .reduce(lambda acc, x: acc + x, 0)
        )
        assert result == 20

    def test_distinct(self) -> None:
        """Test distinct."""
        result = (
            Pipeline([1, 2, 2, 3, 3, 3])
            .distinct()
            .collect()
        )
        assert result == [1, 2, 3]

    def test_sorted(self) -> None:
        """Test sorted."""
        result = (
            Pipeline([3, 1, 2])
            .sorted()
            .collect()
        )
        assert result == [1, 2, 3]

    def test_take(self) -> None:
        """Test take."""
        result = (
            Pipeline([1, 2, 3, 4, 5])
            .take(3)
            .collect()
        )
        assert result == [1, 2, 3]

    def test_skip(self) -> None:
        """Test skip."""
        result = (
            Pipeline([1, 2, 3, 4, 5])
            .skip(2)
            .collect()
        )
        assert result == [3, 4, 5]

    def test_first(self) -> None:
        """Test first."""
        assert Pipeline([1, 2, 3]).first() == 1
        assert Pipeline([]).first(default=0) == 0


class TestStream:
    """Tests for Stream."""

    def test_of(self) -> None:
        """Test creating stream from items."""
        stream = Stream.of(1, 2, 3)
        assert stream.collect() == [1, 2, 3]

    def test_map(self) -> None:
        """Test map transformation."""
        result = Stream.of(1, 2, 3).map(lambda x: x * 2).collect()
        assert result == [2, 4, 6]

    def test_filter(self) -> None:
        """Test filter transformation."""
        result = Stream.of(1, 2, 3, 4).filter(lambda x: x > 2).collect()
        assert result == [3, 4]

    def test_limit(self) -> None:
        """Test limit."""
        result = Stream.of(1, 2, 3, 4, 5).limit(3).collect()
        assert result == [1, 2, 3]


class TestPipe:
    """Tests for pipe function."""

    def test_pipe(self) -> None:
        """Test piping functions."""
        f = pipe(lambda x: x + 1, lambda x: x * 2)
        assert f(5) == 12  # (5 + 1) * 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])