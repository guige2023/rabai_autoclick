"""Tests for dictionary utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.dict_utils import (
    get_value,
    set_value,
    delete_key,
    has_key,
    get_nested,
    set_nested,
    delete_nested,
    merge_dicts,
    diff_dicts,
    filter_dict,
    map_dict,
    invert_dict,
    flatten_dict,
    unflatten_dict,
    pick_keys,
    omit_keys,
    deep_get,
    deep_set,
    deep_delete,
    dict_from_tuples,
    dict_to_tuples,
    update_if_exists,
    update_if_not_exists,
    get_or_create,
    count_values,
    count_keys,
    is_empty,
    clear_dict,
    swap_keys_values,
    rename_key,
    extract_subdict,
    group_by,
)


class TestGetValue:
    """Tests for get_value function."""

    def test_get_value(self) -> None:
        """Test getting value with default."""
        data = {"a": 1, "b": 2}
        assert get_value(data, "a") == 1
        assert get_value(data, "c", 3) == 3

    def test_get_value_missing(self) -> None:
        """Test getting missing key."""
        data = {"a": 1}
        assert get_value(data, "missing", "default") == "default"


class TestSetValue:
    """Tests for set_value function."""

    def test_set_value(self) -> None:
        """Test setting value."""
        data = {}
        set_value(data, "key", "value")
        assert data["key"] == "value"


class TestDeleteKey:
    """Tests for delete_key function."""

    def test_delete_key(self) -> None:
        """Test deleting key."""
        data = {"a": 1, "b": 2}
        delete_key(data, "a")
        assert "a" not in data


class TestHasKey:
    """Tests for has_key function."""

    def test_has_key(self) -> None:
        """Test checking key existence."""
        data = {"a": 1}
        assert has_key(data, "a")
        assert not has_key(data, "b")


class TestGetNested:
    """Tests for get_nested function."""

    def test_get_nested(self) -> None:
        """Test getting nested value."""
        data = {"a": {"b": {"c": 1}}}
        assert get_nested(data, "a.b.c") == 1

    def test_get_nested_default(self) -> None:
        """Test getting missing nested value."""
        data = {"a": {"b": 1}}
        assert get_nested(data, "a.c", "default") == "default"


class TestSetNested:
    """Tests for set_nested function."""

    def test_set_nested(self) -> None:
        """Test setting nested value."""
        data = {}
        set_nested(data, "a.b.c", 1)
        assert data["a"]["b"]["c"] == 1


class TestDeleteNested:
    """Tests for delete_nested function."""

    def test_delete_nested(self) -> None:
        """Test deleting nested value."""
        data = {"a": {"b": {"c": 1}}}
        result = delete_nested(data, "a.b.c")
        assert result is True
        assert "c" not in data["a"]["b"]

    def test_delete_nested_missing(self) -> None:
        """Test deleting missing path."""
        data = {"a": 1}
        result = delete_nested(data, "b.c")
        assert result is False


class TestMergeDicts:
    """Tests for merge_dicts function."""

    def test_merge_dicts(self) -> None:
        """Test merging dictionaries."""
        base = {"a": 1, "b": 2}
        update = {"b": 3, "c": 4}
        result = merge_dicts(base, update)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_merge_dicts_deep(self) -> None:
        """Test deep merging."""
        base = {"a": {"b": 1}}
        update = {"a": {"c": 2}}
        result = merge_dicts(base, update)
        assert result == {"a": {"b": 1, "c": 2}}


class TestDiffDicts:
    """Tests for diff_dicts function."""

    def test_diff_dicts(self) -> None:
        """Test finding differences."""
        dict1 = {"a": 1, "b": 2}
        dict2 = {"a": 1, "b": 3}
        result = diff_dicts(dict1, dict2)
        assert result == {"b": (2, 3)}


class TestFilterDict:
    """Tests for filter_dict function."""

    def test_filter_dict(self) -> None:
        """Test filtering dictionary."""
        data = {"a": 1, "b": 2, "c": 3}
        result = filter_dict(data, lambda k, v: v > 1)
        assert result == {"b": 2, "c": 3}


class TestMapDict:
    """Tests for map_dict function."""

    def test_map_dict(self) -> None:
        """Test mapping dictionary."""
        data = {"a": 1, "b": 2}
        result = map_dict(data, lambda k, v: (k.upper(), v * 2))
        assert result == {"A": 2, "B": 4}


class TestInvertDict:
    """Tests for invert_dict function."""

    def test_invert_dict(self) -> None:
        """Test inverting dictionary."""
        data = {"a": 1, "b": 2}
        result = invert_dict(data)
        assert result == {1: "a", 2: "b"}


class TestFlattenDict:
    """Tests for flatten_dict function."""

    def test_flatten_dict(self) -> None:
        """Test flattening dictionary."""
        data = {"a": {"b": {"c": 1}}}
        result = flatten_dict(data)
        assert result == {"a.b.c": 1}


class TestUnflattenDict:
    """Tests for unflatten_dict function."""

    def test_unflatten_dict(self) -> None:
        """Test unflattening dictionary."""
        data = {"a.b.c": 1}
        result = unflatten_dict(data)
        assert result == {"a": {"b": {"c": 1}}}


class TestPickKeys:
    """Tests for pick_keys function."""

    def test_pick_keys(self) -> None:
        """Test picking keys."""
        data = {"a": 1, "b": 2, "c": 3}
        result = pick_keys(data, ["a", "c"])
        assert result == {"a": 1, "c": 3}


class TestOmitKeys:
    """Tests for omit_keys function."""

    def test_omit_keys(self) -> None:
        """Test omitting keys."""
        data = {"a": 1, "b": 2, "c": 3}
        result = omit_keys(data, ["b"])
        assert result == {"a": 1, "c": 3}


class TestDeepGet:
    """Tests for deep_get function."""

    def test_deep_get(self) -> None:
        """Test deep get."""
        data = {"a": {"b": 1}}
        assert deep_get(data, "a.b") == 1


class TestDeepSet:
    """Tests for deep_set function."""

    def test_deep_set(self) -> None:
        """Test deep set."""
        data = {}
        deep_set(data, "a.b", 1)
        assert data["a"]["b"] == 1


class TestDeepDelete:
    """Tests for deep_delete function."""

    def test_deep_delete(self) -> None:
        """Test deep delete."""
        data = {"a": {"b": 1}}
        result = deep_delete(data, "a.b")
        assert result is True
        assert "b" not in data["a"]


class TestDictFromTuples:
    """Tests for dict_from_tuples function."""

    def test_dict_from_tuples(self) -> None:
        """Test creating dict from tuples."""
        pairs = [("a", 1), ("b", 2)]
        result = dict_from_tuples(pairs)
        assert result == {"a": 1, "b": 2}


class TestDictToTuples:
    """Tests for dict_to_tuples function."""

    def test_dict_to_tuples(self) -> None:
        """Test converting dict to tuples."""
        data = {"a": 1, "b": 2}
        result = dict_to_tuples(data)
        assert set(result) == {("a", 1), ("b", 2)}


class TestUpdateIfExists:
    """Tests for update_if_exists function."""

    def test_update_if_exists(self) -> None:
        """Test conditional update."""
        data = {"a": 1}
        result = update_if_exists(data, "a", 2)
        assert result is True
        assert data["a"] == 2

    def test_update_if_exists_missing(self) -> None:
        """Test conditional update missing."""
        data = {"a": 1}
        result = update_if_exists(data, "b", 2)
        assert result is False


class TestUpdateIfNotExists:
    """Tests for update_if_not_exists function."""

    def test_update_if_not_exists(self) -> None:
        """Test conditional update when not exists."""
        data = {"a": 1}
        result = update_if_not_exists(data, "b", 2)
        assert result is True
        assert data["b"] == 2

    def test_update_if_not_exists_exists(self) -> None:
        """Test conditional update when exists."""
        data = {"a": 1}
        result = update_if_not_exists(data, "a", 2)
        assert result is False
        assert data["a"] == 1


class TestGetOrCreate:
    """Tests for get_or_create function."""

    def test_get_or_create_existing(self) -> None:
        """Test get existing value."""
        data = {"a": 1}
        result = get_or_create(data, "a", lambda: 2)
        assert result == 1

    def test_get_or_create_new(self) -> None:
        """Test create new value."""
        data = {}
        result = get_or_create(data, "a", lambda: 1)
        assert result == 1
        assert data["a"] == 1


class TestCountValues:
    """Tests for count_values function."""

    def test_count_values(self) -> None:
        """Test counting values."""
        data = {"a": 1, "b": 2}
        assert count_values(data) == 2


class TestCountKeys:
    """Tests for count_keys function."""

    def test_count_keys(self) -> None:
        """Test counting keys."""
        data = {"a": 1, "b": 2}
        assert count_keys(data) == 2


class TestIsEmpty:
    """Tests for is_empty function."""

    def test_is_empty(self) -> None:
        """Test checking empty."""
        assert is_empty({})
        assert not is_empty({"a": 1})


class TestClearDict:
    """Tests for clear_dict function."""

    def test_clear_dict(self) -> None:
        """Test clearing dictionary."""
        data = {"a": 1, "b": 2}
        clear_dict(data)
        assert len(data) == 0


class TestSwapKeysValues:
    """Tests for swap_keys_values function."""

    def test_swap_keys_values(self) -> None:
        """Test swapping keys and values."""
        data = {"a": 1, "b": 2}
        result = swap_keys_values(data)
        assert result == {1: "a", 2: "b"}


class TestRenameKey:
    """Tests for rename_key function."""

    def test_rename_key(self) -> None:
        """Test renaming key."""
        data = {"a": 1}
        result = rename_key(data, "a", "b")
        assert result is True
        assert data == {"b": 1}

    def test_rename_key_missing(self) -> None:
        """Test renaming missing key."""
        data = {"a": 1}
        result = rename_key(data, "b", "c")
        assert result is False


class TestExtractSubdict:
    """Tests for extract_subdict function."""

    def test_extract_subdict(self) -> None:
        """Test extracting subdict."""
        data = {"a": 1, "b": 2, "c": 3}
        result = extract_subdict(data, ["a", "c"])
        assert result == {"a": 1, "c": 3}


class TestGroupBy:
    """Tests for group_by function."""

    def test_group_by(self) -> None:
        """Test grouping by key."""
        data = [{"type": "a", "v": 1}, {"type": "b", "v": 2}, {"type": "a", "v": 3}]
        result = group_by(data, "type")
        assert result == {"a": [{"type": "a", "v": 1}, {"type": "a", "v": 3}], "b": [{"type": "b", "v": 2}]}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
