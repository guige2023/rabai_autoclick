"""Tests for transform utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.transform import (
    transform,
    transform_dict,
    filter_map,
    flatten,
    group_by,
    chunk,
    pluck,
    merge,
    pick,
    omit,
    map_values,
    invert,
    deep_get,
    deep_set,
    sanitize_string,
    truncate,
    normalize_whitespace,
    camel_to_snake,
    snake_to_camel,
    parse_int,
    parse_float,
    parse_bool,
    coerce_type,
)


class TestTransform:
    """Tests for transform function."""

    def test_transform_list(self) -> None:
        """Test transforming list items."""
        items = [1, 2, 3, 4]
        result = transform(items, lambda x: x * 2)
        assert result == [2, 4, 6, 8]

    def test_transform_empty(self) -> None:
        """Test transforming empty list."""
        result = transform([], lambda x: x * 2)
        assert result == []


class TestTransformDict:
    """Tests for transform_dict function."""

    def test_transform_dict_values(self) -> None:
        """Test transforming dictionary values."""
        data = {"a": 1, "b": 2, "c": 3}
        result = transform_dict(data, lambda x: x * 2)
        assert result == {"a": 2, "b": 4, "c": 6}


class TestFilterMap:
    """Tests for filter_map function."""

    def test_filter_and_map(self) -> None:
        """Test filtering and mapping."""
        items = [1, 2, 3, 4, 5, 6]
        result = filter_map(items, lambda x: x % 2 == 0, lambda x: x * 2)
        assert result == [4, 8, 12]

    def test_filter_map_empty(self) -> None:
        """Test filter_map with no matches."""
        items = [1, 3, 5]
        result = filter_map(items, lambda x: x % 2 == 0, lambda x: x * 2)
        assert result == []


class TestFlatten:
    """Tests for flatten function."""

    def test_flatten_nested_lists(self) -> None:
        """Test flattening nested lists."""
        nested = [[1, 2], [3, 4], [5]]
        result = flatten(nested)
        assert result == [1, 2, 3, 4, 5]

    def test_flatten_empty(self) -> None:
        """Test flattening empty list."""
        result = flatten([])
        assert result == []

    def test_flatten_single(self) -> None:
        """Test flattening single nested list."""
        result = flatten([[1, 2, 3]])
        assert result == [1, 2, 3]


class TestGroupBy:
    """Tests for group_by function."""

    def test_group_by_key(self) -> None:
        """Test grouping items by key."""
        items = ["apple", "banana", "apricot", "blueberry", "cherry"]
        result = group_by(items, lambda x: x[0])
        assert result == {
            "a": ["apple", "apricot"],
            "b": ["banana", "blueberry"],
            "c": ["cherry"],
        }


class TestChunk:
    """Tests for chunk function."""

    def test_chunk_list(self) -> None:
        """Test chunking list."""
        items = [1, 2, 3, 4, 5, 6, 7]
        result = chunk(items, 3)
        assert result == [[1, 2, 3], [4, 5, 6], [7]]

    def test_chunk_exact_fit(self) -> None:
        """Test chunking with exact fit."""
        items = [1, 2, 3, 4, 5, 6]
        result = chunk(items, 3)
        assert result == [[1, 2, 3], [4, 5, 6]]

    def test_chunk_larger_size(self) -> None:
        """Test chunking with size larger than list."""
        items = [1, 2, 3]
        result = chunk(items, 10)
        assert result == [[1, 2, 3]]


class TestPluck:
    """Tests for pluck function."""

    def test_pluck_values(self) -> None:
        """Test extracting values from list of dicts."""
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        result = pluck(data, "name")
        assert result == ["Alice", "Bob"]

    def test_pluck_with_default(self) -> None:
        """Test pluck with default value."""
        data = [{"name": "Alice"}, {"name": "Bob", "age": 25}]
        result = pluck(data, "age", 0)
        assert result == [0, 25]


class TestMerge:
    """Tests for merge function."""

    def test_merge_dicts(self) -> None:
        """Test merging dictionaries."""
        d1 = {"a": 1, "b": 2}
        d2 = {"b": 3, "c": 4}
        result = merge(d1, d2)
        assert result == {"a": 1, "b": 3, "c": 4}


class TestPick:
    """Tests for pick function."""

    def test_pick_keys(self) -> None:
        """Test picking specific keys."""
        data = {"a": 1, "b": 2, "c": 3}
        result = pick(data, ["a", "c"])
        assert result == {"a": 1, "c": 3}

    def test_pick_missing_key(self) -> None:
        """Test picking with missing key."""
        data = {"a": 1}
        result = pick(data, ["a", "b"])
        assert result == {"a": 1}


class TestOmit:
    """Tests for omit function."""

    def test_omit_keys(self) -> None:
        """Test omitting specific keys."""
        data = {"a": 1, "b": 2, "c": 3}
        result = omit(data, ["b"])
        assert result == {"a": 1, "c": 3}


class TestMapValues:
    """Tests for map_values function."""

    def test_map_dict_values(self) -> None:
        """Test mapping dictionary values."""
        data = {"a": 1, "b": 2}
        result = map_values(data, lambda k, v: f"{k}={v}")
        assert result == {"a": "a=1", "b": "b=2"}


class TestInvert:
    """Tests for invert function."""

    def test_invert_dict(self) -> None:
        """Test inverting dictionary."""
        data = {"a": 1, "b": 2}
        result = invert(data)
        assert result == {1: "a", 2: "b"}


class TestDeepGet:
    """Tests for deep_get function."""

    def test_deep_get_nested(self) -> None:
        """Test getting nested value."""
        data = {"a": {"b": {"c": 42}}}
        result = deep_get(data, "a.b.c")
        assert result == 42

    def test_deep_get_default(self) -> None:
        """Test deep_get with default."""
        data = {"a": 1}
        result = deep_get(data, "b.c", "default")
        assert result == "default"


class TestDeepSet:
    """Tests for deep_set function."""

    def test_deep_set_nested(self) -> None:
        """Test setting nested value."""
        data: dict = {}
        deep_set(data, "a.b.c", 42)
        assert data == {"a": {"b": {"c": 42}}}

    def test_deep_set_existing(self) -> None:
        """Test deep_set on existing path."""
        data = {"a": {"b": {"c": 0}}}
        deep_set(data, "a.b.c", 42)
        assert data == {"a": {"b": {"c": 42}}}


class TestSanitizeString:
    """Tests for sanitize_string function."""

    def test_sanitize_crlf(self) -> None:
        """Test sanitizing CRLF."""
        result = sanitize_string("a\r\nb\r\nc")
        assert result == "a\nb\nc"

    def test_sanitize_tabs(self) -> None:
        """Test sanitizing tabs."""
        result = sanitize_string("a\tb")
        assert result == "a    b"


class TestTruncate:
    """Tests for truncate function."""

    def test_truncate_shortens(self) -> None:
        """Test truncating long text."""
        result = truncate("Hello World", 8)
        assert result == "Hello..."

    def test_truncate_no_change(self) -> None:
        """Test not truncating short text."""
        result = truncate("Hi", 10)
        assert result == "Hi"

    def test_truncate_custom_suffix(self) -> None:
        """Test truncate with custom suffix."""
        result = truncate("Hello World", 8, suffix="~~")
        assert result == "Hello ~~"


class TestNormalizeWhitespace:
    """Tests for normalize_whitespace function."""

    def test_normalize_whitespace(self) -> None:
        """Test normalizing whitespace."""
        result = normalize_whitespace("  Hello   World  ")
        assert result == "Hello World"


class TestCamelToSnake:
    """Tests for camel_to_snake function."""

    def test_camel_to_snake(self) -> None:
        """Test converting camelCase to snake_case."""
        result = camel_to_snake("camelCase")
        assert result == "camel_case"

    def test_camel_to_snake_multiple(self) -> None:
        """Test converting multiple capital letters."""
        result = camel_to_snake("someXMLString")
        assert result == "some_xml_string"


class TestSnakeToCamel:
    """Tests for snake_to_camel function."""

    def test_snake_to_camel(self) -> None:
        """Test converting snake_case to camelCase."""
        result = snake_to_camel("snake_case")
        assert result == "snakeCase"


class TestParseInt:
    """Tests for parse_int function."""

    def test_parse_int_string(self) -> None:
        """Test parsing integer from string."""
        result = parse_int("42")
        assert result == 42

    def test_parse_int_default(self) -> None:
        """Test parsing invalid with default."""
        result = parse_int("invalid", default=0)
        assert result == 0

    def test_parse_int_already_int(self) -> None:
        """Test parsing already integer."""
        result = parse_int(42)
        assert result == 42


class TestParseFloat:
    """Tests for parse_float function."""

    def test_parse_float_string(self) -> None:
        """Test parsing float from string."""
        result = parse_float("3.14")
        assert result == 3.14

    def test_parse_float_default(self) -> None:
        """Test parsing invalid with default."""
        result = parse_float("invalid", default=0.0)
        assert result == 0.0


class TestParseBool:
    """Tests for parse_bool function."""

    def test_parse_bool_true_strings(self) -> None:
        """Test parsing true strings."""
        assert parse_bool("true") is True
        assert parse_bool("yes") is True
        assert parse_bool("1") is True
        assert parse_bool("on") is True

    def test_parse_bool_false_strings(self) -> None:
        """Test parsing false strings."""
        assert parse_bool("false") is False
        assert parse_bool("no") is False
        assert parse_bool("0") is False
        assert parse_bool("off") is False

    def test_parse_bool_already_bool(self) -> None:
        """Test parsing already boolean."""
        assert parse_bool(True) is True
        assert parse_bool(False) is False


class TestCoerceType:
    """Tests for coerce_type function."""

    def test_coerce_to_int(self) -> None:
        """Test coercing to int."""
        result = coerce_type("42", int)
        assert result == 42

    def test_coerce_to_float(self) -> None:
        """Test coercing to float."""
        result = coerce_type("3.14", float)
        assert result == 3.14

    def test_coerce_to_str(self) -> None:
        """Test coercing to str."""
        result = coerce_type(42, str)
        assert result == "42"

    def test_coerce_to_bool(self) -> None:
        """Test coercing to bool."""
        result = coerce_type("true", bool)
        assert result is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
