"""Tests for functional utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.functional import (
    compose,
    pipe,
    curry,
    memoize,
    chunk,
    flatten,
    group_by,
    partition,
    pluck,
    sort_by,
    unique,
    batch,
)


class TestCompose:
    """Tests for compose."""

    def test_compose_single(self) -> None:
        """Test compose with single function."""
        f = compose(lambda x: x + 1)
        assert f(5) == 6

    def test_compose_multiple(self) -> None:
        """Test compose with multiple functions."""
        f = compose(lambda x: x + 1, lambda x: x * 2)
        assert f(5) == 11  # (5 + 1) * 2

    def test_compose_order(self) -> None:
        """Test compose processes right to left."""
        f = compose(lambda x: x + 1, lambda x: x + 2, lambda x: x + 3)
        # ((5 + 3) + 2) + 1 = 11
        assert f(5) == 11


class TestPipe:
    """Tests for pipe."""

    def test_pipe_single(self) -> None:
        """Test pipe with single function."""
        f = pipe(lambda x: x + 1)
        assert f(5) == 6

    def test_pipe_multiple(self) -> None:
        """Test pipe with multiple functions."""
        f = pipe(lambda x: x + 1, lambda x: x * 2)
        assert f(5) == 12  # (5 + 1) * 2

    def test_pipe_order(self) -> None:
        """Test pipe processes left to right."""
        f = pipe(lambda x: x + 1, lambda x: x + 2, lambda x: x + 3)
        # (5 + 1) + 2 + 3 = 11
        assert f(5) == 11


class TestCurry:
    """Tests for curry."""

    def test_curry_basic(self) -> None:
        """Test basic currying."""
        @curry
        def add(a, b):
            return a + b

        assert add(1)(2) == 3
        assert add(1, 2) == 3

    def test_curry_with_existing_args(self) -> None:
        """Test currying with some args provided."""
        @curry
        def add_three(a, b, c):
            return a + b + c

        assert add_three(1)(2)(3) == 6
        assert add_three(1, 2)(3) == 6


class TestMemoize:
    """Tests for memoize."""

    def test_memoize_basic(self) -> None:
        """Test basic memoization."""
        call_count = [0]

        @memoize
        def fib(n):
            call_count[0] += 1
            if n <= 1:
                return n
            return fib(n - 1) + fib(n - 2)

        result = fib(10)
        assert result == 55
        # Without memoization this would be called many more times
        assert call_count[0] < 20

    def test_memoize_cache_clear(self) -> None:
        """Test cache clearing."""
        @memoize
        def f(x):
            return x * 2

        assert f(5) == 10
        assert f.cache_info()["size"] == 1

        f.cache_clear()
        assert f.cache_info()["size"] == 0


class TestChunk:
    """Tests for chunk."""

    def test_chunk_even(self) -> None:
        """Test chunking evenly divisible list."""
        result = list(chunk([1, 2, 3, 4], 2))
        assert result == [[1, 2], [3, 4]]

    def test_chunk_uneven(self) -> None:
        """Test chunking uneven list."""
        result = list(chunk([1, 2, 3, 4, 5], 2))
        assert result == [[1, 2], [3, 4], [5]]

    def test_chunk_larger_than_list(self) -> None:
        """Test chunk size larger than list."""
        result = list(chunk([1, 2], 5))
        assert result == [[1, 2]]


class TestFlatten:
    """Tests for flatten."""

    def test_flatten_nested(self) -> None:
        """Test flattening nested structure."""
        result = list(flatten([[1, 2], [3, [4, 5]]]))
        assert result == [1, 2, 3, 4, 5]

    def test_flatten_flat(self) -> None:
        """Test flattening already flat list."""
        result = list(flatten([1, 2, 3]))
        assert result == [1, 2, 3]


class TestGroupBy:
    """Tests for group_by."""

    def test_group_by(self) -> None:
        """Test grouping by key."""
        items = [{"type": "a", "v": 1}, {"type": "b", "v": 2}, {"type": "a", "v": 3}]
        result = group_by(items, lambda x: x["type"])
        assert result == {"a": [{"type": "a", "v": 1}, {"type": "a", "v": 3}], "b": [{"type": "b", "v": 2}]}


class TestPartition:
    """Tests for partition."""

    def test_partition(self) -> None:
        """Test partitioning."""
        items = [1, 2, 3, 4, 5]
        even, odd = partition(items, lambda x: x % 2 == 0)
        assert even == [2, 4]
        assert odd == [1, 3, 5]


class TestPluck:
    """Tests for pluck."""

    def test_pluck(self) -> None:
        """Test plucking values."""
        items = [{"a": 1}, {"a": 2}, {"a": 3}]
        assert pluck(items, "a") == [1, 2, 3]

    def test_pluck_missing_key(self) -> None:
        """Test pluck with missing key."""
        items = [{"a": 1}, {"b": 2}]
        assert pluck(items, "a", default=0) == [1, 0]


class TestSortBy:
    """Tests for sort_by."""

    def test_sort_by(self) -> None:
        """Test sorting by key."""
        items = [{"v": 3}, {"v": 1}, {"v": 2}]
        result = sort_by(items, lambda x: x["v"])
        assert result == [{"v": 1}, {"v": 2}, {"v": 3}]


class TestUnique:
    """Tests for unique."""

    def test_unique_with_dups(self) -> None:
        """Test unique with duplicates."""
        assert unique([1, 2, 2, 3, 1]) == [1, 2, 3]

    def test_unique_preserve_order(self) -> None:
        """Test unique preserves order."""
        assert unique([3, 1, 2, 1, 3]) == [3, 1, 2]


class TestBatch:
    """Tests for batch."""

    def test_batch(self) -> None:
        """Test batching."""
        result = list(batch([1, 2, 3, 4, 5], 2))
        assert result == [[1, 2], [3, 4], [5]]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])