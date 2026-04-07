"""Tests for filter utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.filter import (
    Filter,
    PredicateFilter,
    EqFilter,
    NeFilter,
    LtFilter,
    LteFilter,
    GtFilter,
    GteFilter,
    InFilter,
    ContainsFilter,
    RegexFilter,
    WildcardFilter,
    KeyFilter,
    FilterChain,
    OrFilter,
    NotFilter,
    filter_list,
    filter_dict,
    partition,
)


class TestPredicateFilter:
    """Tests for PredicateFilter."""

    def test_matches(self) -> None:
        """Test predicate matching."""
        f = PredicateFilter(lambda x: x > 5)
        assert f.matches(10) is True
        assert f.matches(3) is False


class TestEqFilter:
    """Tests for EqFilter."""

    def test_matches_equal(self) -> None:
        """Test equal matching."""
        f = EqFilter(42)
        assert f.matches(42) is True
        assert f.matches(43) is False

    def test_matches_string(self) -> None:
        """Test string matching."""
        f = EqFilter("hello")
        assert f.matches("hello") is True
        assert f.matches("world") is False


class TestNeFilter:
    """Tests for NeFilter."""

    def test_matches_not_equal(self) -> None:
        """Test not equal matching."""
        f = NeFilter(42)
        assert f.matches(43) is True
        assert f.matches(42) is False


class TestLtFilter:
    """Tests for LtFilter."""

    def test_less_than(self) -> None:
        """Test less than matching."""
        f = LtFilter(10)
        assert f.matches(5) is True
        assert f.matches(10) is False
        assert f.matches(15) is False


class TestLteFilter:
    """Tests for LteFilter."""

    def test_less_than_or_equal(self) -> None:
        """Test less than or equal matching."""
        f = LteFilter(10)
        assert f.matches(5) is True
        assert f.matches(10) is True
        assert f.matches(15) is False


class TestGtFilter:
    """Tests for GtFilter."""

    def test_greater_than(self) -> None:
        """Test greater than matching."""
        f = GtFilter(10)
        assert f.matches(15) is True
        assert f.matches(10) is False
        assert f.matches(5) is False


class TestGteFilter:
    """Tests for GteFilter."""

    def test_greater_than_or_equal(self) -> None:
        """Test greater than or equal matching."""
        f = GteFilter(10)
        assert f.matches(15) is True
        assert f.matches(10) is True
        assert f.matches(5) is False


class TestInFilter:
    """Tests for InFilter."""

    def test_in_list(self) -> None:
        """Test membership matching."""
        f = InFilter([1, 2, 3])
        assert f.matches(1) is True
        assert f.matches(2) is True
        assert f.matches(5) is False

    def test_in_string(self) -> None:
        """Test substring membership."""
        f = InFilter("abc")
        assert f.matches("a") is True
        assert f.matches("x") is False


class TestContainsFilter:
    """Tests for ContainsFilter."""

    def test_contains_substring(self) -> None:
        """Test substring containing."""
        f = ContainsFilter("ello")
        assert f.matches("hello") is True
        assert f.matches("world") is False

    def test_contains_number(self) -> None:
        """Test number contains."""
        f = ContainsFilter("42")
        assert f.matches("The answer is 42") is True


class TestRegexFilter:
    """Tests for RegexFilter."""

    def test_matches_pattern(self) -> None:
        """Test regex matching."""
        f = RegexFilter(r"^\d{3}-\d{4}$")
        assert f.matches("123-4567") is True
        assert f.matches("12-34567") is False

    def test_matches_anywhere(self) -> None:
        """Test regex search anywhere."""
        f = RegexFilter(r"hello")
        assert f.matches("say hello world") is True


class TestWildcardFilter:
    """Tests for WildcardFilter."""

    def test_glob_star(self) -> None:
        """Test glob * matching."""
        f = WildcardFilter("*.txt")
        assert f.matches("file.txt") is True
        assert f.matches("file.md") is False

    def test_glob_question(self) -> None:
        """Test glob ? matching."""
        f = WildcardFilter("file?.txt")
        assert f.matches("file1.txt") is True
        assert f.matches("file10.txt") is False


class TestKeyFilter:
    """Tests for KeyFilter."""

    def test_key_match(self) -> None:
        """Test dictionary key filtering."""
        f = KeyFilter("name", EqFilter("Alice"))
        assert f.matches({"name": "Alice", "age": 30}) is True
        assert f.matches({"name": "Bob", "age": 25}) is False

    def test_key_missing(self) -> None:
        """Test missing key returns False."""
        f = KeyFilter("name", EqFilter("Alice"))
        assert f.matches({"age": 30}) is False


class TestFilterChain:
    """Tests for FilterChain."""

    def test_empty_chain(self) -> None:
        """Test empty chain matches everything."""
        chain = FilterChain()
        assert chain.matches(42) is True

    def test_single_filter(self) -> None:
        """Test chain with single filter."""
        chain = FilterChain([EqFilter(42)])
        assert chain.matches(42) is True
        assert chain.matches(43) is False

    def test_multiple_filters_and(self) -> None:
        """Test chain with multiple filters (AND)."""
        chain = FilterChain([GtFilter(5), LtFilter(10)])
        assert chain.matches(7) is True
        assert chain.matches(5) is False
        assert chain.matches(10) is False

    def test_add_filter(self) -> None:
        """Test adding filters."""
        chain = FilterChain().add(GtFilter(5)).add(LtFilter(10))
        assert chain.matches(7) is True


class TestOrFilter:
    """Tests for OrFilter."""

    def test_empty_or(self) -> None:
        """Test empty OR returns False."""
        f = OrFilter()
        assert f.matches(42) is False

    def test_single_filter(self) -> None:
        """Test OR with single filter."""
        f = OrFilter([EqFilter(42)])
        assert f.matches(42) is True
        assert f.matches(43) is False

    def test_multiple_filters_or(self) -> None:
        """Test OR with multiple filters."""
        f = OrFilter([EqFilter(1), EqFilter(2), EqFilter(3)])
        assert f.matches(2) is True
        assert f.matches(5) is False

    def test_add_filter(self) -> None:
        """Test adding filters to OR."""
        f = OrFilter().add(EqFilter(1)).add(EqFilter(2))
        assert f.matches(2) is True


class TestNotFilter:
    """Tests for NotFilter."""

    def test_invert_match(self) -> None:
        """Test NOT inverts filter."""
        f = NotFilter(EqFilter(42))
        assert f.matches(42) is False
        assert f.matches(43) is True


class TestFilterList:
    """Tests for filter_list."""

    def test_filter_list(self) -> None:
        """Test filtering list."""
        items = [1, 2, 3, 4, 5, 6]
        result = filter_list(items, GtFilter(3))
        assert result == [4, 5, 6]

    def test_filter_empty(self) -> None:
        """Test filtering empty list."""
        result = filter_list([], EqFilter(1))
        assert result == []


class TestFilterDict:
    """Tests for filter_dict."""

    def test_filter_dict(self) -> None:
        """Test filtering dictionary."""
        data = {"a": 1, "b": 2, "c": 3}
        result = filter_dict(data, GtFilter(1))
        assert result == {"b": 2, "c": 3}


class TestPartition:
    """Tests for partition."""

    def test_partition_even(self) -> None:
        """Test partitioning list."""
        items = [1, 2, 3, 4, 5, 6]
        matching, non_matching = partition(items, GtFilter(3))
        assert matching == [4, 5, 6]
        assert non_matching == [1, 2, 3]

    def test_partition_all_match(self) -> None:
        """Test partition when all match."""
        items = [1, 2, 3]
        matching, non_matching = partition(items, LtFilter(10))
        assert matching == [1, 2, 3]
        assert non_matching == []

    def test_partition_none_match(self) -> None:
        """Test partition when none match."""
        items = [1, 2, 3]
        matching, non_matching = partition(items, GtFilter(10))
        assert matching == []
        assert non_matching == [1, 2, 3]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])