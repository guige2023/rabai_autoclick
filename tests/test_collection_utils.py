"""Tests for collection utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.collection_utils import (
    unique,
    unique_by,
    partition,
    first,
    last,
    sample,
    transpose,
    zip_with,
    batch,
    sliding_window,
    count_by,
    group_by_to_dict,
    intersection,
    union,
    difference,
    symmetric_difference,
    find,
    find_index,
    contains,
    all_match,
    none_match,
    sort_by,
    chunk_list,
    deduplicate,
)


class TestUnique:
    """Tests for unique function."""

    def test_unique_removes_duplicates(self) -> None:
        """Test unique removes duplicates."""
        assert unique([1, 2, 2, 3, 3, 3]) == [1, 2, 3]

    def test_unique_preserves_order(self) -> None:
        """Test unique preserves order."""
        assert unique([3, 1, 2, 1, 3]) == [3, 1, 2]


class TestUniqueBy:
    """Tests for unique_by function."""

    def test_unique_by_key(self) -> None:
        """Test unique by key function."""
        items = [{"a": 1, "b": 2}, {"a": 1, "b": 3}, {"a": 2, "b": 4}]
        result = unique_by(items, lambda x: x["a"])
        assert len(result) == 2


class TestPartition:
    """Tests for partition function."""

    def test_partition_by_even(self) -> None:
        """Test partition by even."""
        evens, odds = partition([1, 2, 3, 4, 5], lambda x: x % 2 == 0)
        assert evens == [2, 4]
        assert odds == [1, 3, 5]


class TestFirst:
    """Tests for first function."""

    def test_first_returns_first(self) -> None:
        """Test first returns first element."""
        assert first([1, 2, 3]) == 1

    def test_first_empty_returns_default(self) -> None:
        """Test first returns default for empty list."""
        assert first([], default=0) == 0


class TestLast:
    """Tests for last function."""

    def test_last_returns_last(self) -> None:
        """Test last returns last element."""
        assert last([1, 2, 3]) == 3

    def test_last_empty_returns_default(self) -> None:
        """Test last returns default for empty list."""
        assert last([], default=0) == 0


class TestSample:
    """Tests for sample function."""

    def test_sample_count(self) -> None:
        """Test sample returns correct count."""
        items = [1, 2, 3, 4, 5]
        result = sample(items, 3)
        assert len(result) == 3

    def test_sample_all_if_count_larger(self) -> None:
        """Test sample returns all items if count is larger."""
        items = [1, 2, 3]
        result = sample(items, 5)
        assert len(result) == 3


class TestTranspose:
    """Tests for transpose function."""

    def test_transpose_matrix(self) -> None:
        """Test transposing a matrix."""
        matrix = [[1, 2], [3, 4]]
        result = transpose(matrix)
        assert result == [[1, 3], [2, 4]]


class TestZipWith:
    """Tests for zip_with function."""

    def test_zip_with_add(self) -> None:
        """Test zipping with addition."""
        result = zip_with(lambda x, y: x + y, [1, 2], [3, 4])
        assert result == [4, 6]


class TestBatch:
    """Tests for batch function."""

    def test_batch_items(self) -> None:
        """Test batching items."""
        items = [1, 2, 3, 4, 5]
        result = list(batch(items, 2))
        assert result == [[1, 2], [3, 4], [5]]


class TestSlidingWindow:
    """Tests for sliding_window function."""

    def test_sliding_window(self) -> None:
        """Test sliding window."""
        items = [1, 2, 3, 4]
        result = list(sliding_window(items, 2))
        assert result == [[1, 2], [2, 3], [3, 4]]


class TestCountBy:
    """Tests for count_by function."""

    def test_count_by_length(self) -> None:
        """Test counting by length."""
        items = ["a", "ab", "abc", "b", "bc"]
        result = count_by(items, len)
        assert result == {1: 2, 2: 2, 3: 1}


class TestGroupByToDict:
    """Tests for group_by_to_dict function."""

    def test_group_by_first_letter(self) -> None:
        """Test grouping by first letter."""
        items = ["apple", "banana", "apricot"]
        result = group_by_to_dict(items, lambda x: x[0])
        assert result == {"a": ["apple", "apricot"], "b": ["banana"]}


class TestIntersection:
    """Tests for intersection function."""

    def test_intersection_basic(self) -> None:
        """Test intersection of lists."""
        result = intersection([1, 2, 3], [2, 3, 4])
        assert set(result) == {2, 3}


class TestUnion:
    """Tests for union function."""

    def test_union_basic(self) -> None:
        """Test union of lists."""
        result = union([1, 2], [3, 4])
        assert set(result) == {1, 2, 3, 4}


class TestDifference:
    """Tests for difference function."""

    def test_difference_basic(self) -> None:
        """Test difference of lists."""
        result = difference([1, 2, 3], [2, 3])
        assert result == [1]


class TestSymmetricDifference:
    """Tests for symmetric_difference function."""

    def test_symmetric_difference(self) -> None:
        """Test symmetric difference."""
        result = symmetric_difference([1, 2], [2, 3])
        assert set(result) == {1, 3}


class TestFind:
    """Tests for find function."""

    def test_find_returns_match(self) -> None:
        """Test find returns matching element."""
        result = find([1, 2, 3, 4], lambda x: x > 2)
        assert result == 3

    def test_find_returns_none(self) -> None:
        """Test find returns None when no match."""
        result = find([1, 2, 3], lambda x: x > 10)
        assert result is None


class TestFindIndex:
    """Tests for find_index function."""

    def test_find_index_returns_index(self) -> None:
        """Test find_index returns correct index."""
        result = find_index([1, 2, 3, 4], lambda x: x > 2)
        assert result == 2

    def test_find_index_returns_negative_one(self) -> None:
        """Test find_index returns -1 when no match."""
        result = find_index([1, 2, 3], lambda x: x > 10)
        assert result == -1


class TestContains:
    """Tests for contains function."""

    def test_contains_true(self) -> None:
        """Test contains returns True when found."""
        result = contains([1, 2, 3], lambda x: x == 2)
        assert result is True

    def test_contains_false(self) -> None:
        """Test contains returns False when not found."""
        result = contains([1, 2, 3], lambda x: x == 10)
        assert result is False


class TestAllMatch:
    """Tests for all_match function."""

    def test_all_match_true(self) -> None:
        """Test all_match returns True when all match."""
        result = all_match([2, 4, 6], lambda x: x % 2 == 0)
        assert result is True

    def test_all_match_false(self) -> None:
        """Test all_match returns False when not all match."""
        result = all_match([1, 2, 3], lambda x: x % 2 == 0)
        assert result is False


class TestNoneMatch:
    """Tests for none_match function."""

    def test_none_match_true(self) -> None:
        """Test none_match returns True when none match."""
        result = none_match([1, 3, 5], lambda x: x % 2 == 0)
        assert result is True

    def test_none_match_false(self) -> None:
        """Test none_match returns False when some match."""
        result = none_match([1, 2, 3], lambda x: x % 2 == 0)
        assert result is False


class TestSortBy:
    """Tests for sort_by function."""

    def test_sort_by_length(self) -> None:
        """Test sorting by length."""
        result = sort_by(["ccc", "a", "bb"], len)
        assert result == ["a", "bb", "ccc"]


class TestChunkList:
    """Tests for chunk_list function."""

    def test_chunk_list_basic(self) -> None:
        """Test chunking list."""
        result = chunk_list([1, 2, 3, 4, 5], 2)
        assert result == [[1, 2], [3, 4], [5]]


class TestDeduplicate:
    """Tests for deduplicate function."""

    def test_deduplicate_basic(self) -> None:
        """Test deduplicating list."""
        result = deduplicate([1, 2, 2, 1, 3])
        assert result == [1, 2, 3]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
