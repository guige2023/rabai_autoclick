"""Tests for list utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.list_utils import (
    first,
    last,
    get_at,
    set_at,
    insert_at,
    delete_at,
    append,
    extend,
    prepend,
    unique,
    unique_by,
    filter_list,
    reject_list,
    map_list,
    flat_map,
    reduce_list,
    find_item,
    find_index,
    contains_item,
    all_match,
    none_match,
    partition_list,
    chunk,
    window,
    flatten,
    group_by,
    sort_by,
    reverse_list,
    shuffle,
    sample,
    take,
    drop,
    take_while,
    drop_while,
    zip_with_index,
    unzip_pairs,
    interleave,
    intersperse,
    count_items,
    sum_list,
    product_list,
    average_list,
    min_item,
    max_item,
)


class TestFirst:
    """Tests for first function."""

    def test_first(self) -> None:
        """Test getting first item."""
        assert first([1, 2, 3]) == 1
        assert first([]) is None


class TestLast:
    """Tests for last function."""

    def test_last(self) -> None:
        """Test getting last item."""
        assert last([1, 2, 3]) == 3
        assert last([]) is None


class TestGetAt:
    """Tests for get_at function."""

    def test_get_at(self) -> None:
        """Test getting item at index."""
        assert get_at([1, 2, 3], 1) == 2
        assert get_at([1, 2, 3], -1) == 3
        assert get_at([1, 2, 3], 10) is None


class TestSetAt:
    """Tests for set_at function."""

    def test_set_at(self) -> None:
        """Test setting item at index."""
        items = [1, 2, 3]
        assert set_at(items, 1, 5) is True
        assert items == [1, 5, 3]

    def test_set_at_out_of_bounds(self) -> None:
        """Test setting out of bounds."""
        items = [1, 2, 3]
        assert set_at(items, 10, 5) is False


class TestInsertAt:
    """Tests for insert_at function."""

    def test_insert_at(self) -> None:
        """Test inserting at index."""
        items = [1, 2, 3]
        insert_at(items, 1, 5)
        assert items == [1, 5, 2, 3]


class TestDeleteAt:
    """Tests for delete_at function."""

    def test_delete_at(self) -> None:
        """Test deleting at index."""
        items = [1, 2, 3]
        result = delete_at(items, 1)
        assert result == 2
        assert items == [1, 3]


class TestAppend:
    """Tests for append function."""

    def test_append(self) -> None:
        """Test appending item."""
        items = [1, 2]
        append(items, 3)
        assert items == [1, 2, 3]


class TestExtend:
    """Tests for extend function."""

    def test_extend(self) -> None:
        """Test extending list."""
        items = [1, 2]
        extend(items, [3, 4])
        assert items == [1, 2, 3, 4]


class TestPrepend:
    """Tests for prepend function."""

    def test_prepend(self) -> None:
        """Test prepending item."""
        items = [1, 2]
        prepend(items, 0)
        assert items == [0, 1, 2]


class TestUnique:
    """Tests for unique function."""

    def test_unique(self) -> None:
        """Test getting unique items."""
        assert unique([1, 2, 2, 3, 1]) == [1, 2, 3]


class TestUniqueBy:
    """Tests for unique_by function."""

    def test_unique_by(self) -> None:
        """Test getting unique items by key."""
        items = [{"a": 1}, {"a": 2}, {"a": 1}]
        result = unique_by(items, lambda x: x["a"])
        assert len(result) == 2


class TestFilterList:
    """Tests for filter_list function."""

    def test_filter_list(self) -> None:
        """Test filtering list."""
        assert filter_list([1, 2, 3, 4], lambda x: x > 2) == [3, 4]


class TestRejectList:
    """Tests for reject_list function."""

    def test_reject_list(self) -> None:
        """Test rejecting items."""
        assert reject_list([1, 2, 3, 4], lambda x: x > 2) == [1, 2]


class TestMapList:
    """Tests for map_list function."""

    def test_map_list(self) -> None:
        """Test mapping list."""
        assert map_list([1, 2, 3], lambda x: x * 2) == [2, 4, 6]


class TestFlatMap:
    """Tests for flat_map function."""

    def test_flat_map(self) -> None:
        """Test flat mapping."""
        result = flat_map([1, 2, 3], lambda x: [x, x])
        assert result == [1, 1, 2, 2, 3, 3]


class TestReduceList:
    """Tests for reduce_list function."""

    def test_reduce_list(self) -> None:
        """Test reducing list."""
        result = reduce_list([1, 2, 3], lambda acc, x: acc + x, 0)
        assert result == 6


class TestFindItem:
    """Tests for find_item function."""

    def test_find_item(self) -> None:
        """Test finding item."""
        assert find_item([1, 2, 3], lambda x: x == 2) == 2
        assert find_item([1, 2, 3], lambda x: x == 5) is None


class TestFindIndex:
    """Tests for find_index function."""

    def test_find_index(self) -> None:
        """Test finding index."""
        assert find_index([1, 2, 3], lambda x: x == 2) == 1
        assert find_index([1, 2, 3], lambda x: x == 5) == -1


class TestContainsItem:
    """Tests for contains_item function."""

    def test_contains_item(self) -> None:
        """Test checking contains."""
        assert contains_item([1, 2, 3], lambda x: x == 2) is True
        assert contains_item([1, 2, 3], lambda x: x == 5) is False


class TestAllMatch:
    """Tests for all_match function."""

    def test_all_match(self) -> None:
        """Test all match."""
        assert all_match([2, 4, 6], lambda x: x % 2 == 0) is True
        assert all_match([1, 2, 3], lambda x: x > 0) is True
        assert all_match([1, 2, 3], lambda x: x > 2) is False


class TestNoneMatch:
    """Tests for none_match function."""

    def test_none_match(self) -> None:
        """Test none match."""
        assert none_match([1, 3, 5], lambda x: x % 2 == 0) is True
        assert none_match([1, 2, 3], lambda x: x > 5) is True
        assert none_match([1, 2, 3], lambda x: x > 2) is False


class TestPartitionList:
    """Tests for partition_list function."""

    def test_partition_list(self) -> None:
        """Test partitioning list."""
        matching, non_matching = partition_list([1, 2, 3, 4], lambda x: x > 2)
        assert matching == [3, 4]
        assert non_matching == [1, 2]


class TestChunk:
    """Tests for chunk function."""

    def test_chunk(self) -> None:
        """Test chunking list."""
        assert chunk([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]


class TestWindow:
    """Tests for window function."""

    def test_window(self) -> None:
        """Test creating windows."""
        assert window([1, 2, 3, 4], 2) == [[1, 2], [2, 3], [3, 4]]


class TestFlatten:
    """Tests for flatten function."""

    def test_flatten(self) -> None:
        """Test flattening nested lists."""
        assert flatten([1, [2, [3, 4]], 5]) == [1, 2, 3, 4, 5]


class TestGroupBy:
    """Tests for group_by function."""

    def test_group_by(self) -> None:
        """Test grouping by key."""
        items = [{"type": "a", "v": 1}, {"type": "b", "v": 2}, {"type": "a", "v": 3}]
        result = group_by(items, "type")
        assert len(result["a"]) == 2
        assert len(result["b"]) == 1


class TestSortBy:
    """Tests for sort_by function."""

    def test_sort_by(self) -> None:
        """Test sorting by key."""
        items = [{"v": 3}, {"v": 1}, {"v": 2}]
        result = sort_by(items, lambda x: x["v"])
        assert [x["v"] for x in result] == [1, 2, 3]


class TestReverseList:
    """Tests for reverse_list function."""

    def test_reverse_list(self) -> None:
        """Test reversing list."""
        assert reverse_list([1, 2, 3]) == [3, 2, 1]


class TestShuffle:
    """Tests for shuffle function."""

    def test_shuffle(self) -> None:
        """Test shuffling list."""
        items = [1, 2, 3, 4, 5]
        result = shuffle(items)
        assert len(result) == 5
        assert set(result) == set(items)


class TestSample:
    """Tests for sample function."""

    def test_sample(self) -> None:
        """Test sampling items."""
        items = [1, 2, 3, 4, 5]
        result = sample(items, 3)
        assert len(result) == 3
        assert all(x in items for x in result)


class TestTake:
    """Tests for take function."""

    def test_take(self) -> None:
        """Test taking items."""
        assert take([1, 2, 3, 4], 2) == [1, 2]


class TestDrop:
    """Tests for drop function."""

    def test_drop(self) -> None:
        """Test dropping items."""
        assert drop([1, 2, 3, 4], 2) == [3, 4]


class TestTakeWhile:
    """Tests for take_while function."""

    def test_take_while(self) -> None:
        """Test taking while."""
        assert take_while([1, 2, 3, 4], lambda x: x < 3) == [1, 2]


class TestDropWhile:
    """Tests for drop_while function."""

    def test_drop_while(self) -> None:
        """Test dropping while."""
        assert drop_while([1, 2, 3, 4], lambda x: x < 3) == [3, 4]


class TestZipWithIndex:
    """Tests for zip_with_index function."""

    def test_zip_with_index(self) -> None:
        """Test zipping with index."""
        assert zip_with_index(["a", "b", "c"]) == [(0, "a"), (1, "b"), (2, "c")]


class TestUnzipPairs:
    """Tests for unzip_pairs function."""

    def test_unzip_pairs(self) -> None:
        """Test unzipping pairs."""
        pairs = [(1, "a"), (2, "b"), (3, "c")]
        result = unzip_pairs(pairs)
        assert result == ([1, 2, 3], ["a", "b", "c"])


class TestInterleave:
    """Tests for interleave function."""

    def test_interleave(self) -> None:
        """Test interleaving lists."""
        result = interleave([1, 2], ["a", "b"])
        assert result == [1, "a", 2, "b"]


class TestIntersperse:
    """Tests for intersperse function."""

    def test_intersperse(self) -> None:
        """Test interspersing values."""
        assert intersperse([1, 2, 3], 0) == [1, 0, 2, 0, 3]


class TestCountItems:
    """Tests for count_items function."""

    def test_count_items(self) -> None:
        """Test counting items."""
        assert count_items([1, 2, 3, 4], lambda x: x > 2) == 2


class TestSumList:
    """Tests for sum_list function."""

    def test_sum_list(self) -> None:
        """Test summing list."""
        assert sum_list([1, 2, 3, 4]) == 10


class TestProductList:
    """Tests for product_list function."""

    def test_product_list(self) -> None:
        """Test product of list."""
        assert product_list([1, 2, 3, 4]) == 24


class TestAverageList:
    """Tests for average_list function."""

    def test_average_list(self) -> None:
        """Test averaging list."""
        assert average_list([1, 2, 3, 4]) == 2.5


class TestMinItem:
    """Tests for min_item function."""

    def test_min_item(self) -> None:
        """Test getting minimum item."""
        assert min_item([3, 1, 4, 2]) == 1
        assert min_item([]) is None


class TestMaxItem:
    """Tests for max_item function."""

    def test_max_item(self) -> None:
        """Test getting maximum item."""
        assert max_item([3, 1, 4, 2]) == 4
        assert max_item([]) is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
