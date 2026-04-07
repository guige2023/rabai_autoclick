"""Tests for JSON utilities."""

import datetime
import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.json_utils import (
    ExtendedJSONEncoder,
    safe_json_loads,
    safe_json_dumps,
    deep_merge,
    load_json_file,
    save_json_file,
    patch_json_file,
    JSONFile,
)


class TestSafeJsonLoads:
    """Tests for safe_json_loads."""

    def test_valid_json(self) -> None:
        """Test loading valid JSON."""
        result = safe_json_loads('{"key": "value"}')
        assert result == {"key": "value"}

    def test_invalid_json_default(self) -> None:
        """Test invalid JSON returns default."""
        result = safe_json_loads("not json", default={})
        assert result == {}

    def test_strict_mode(self) -> None:
        """Test strict mode parsing."""
        result = safe_json_loads('{"key": "value"}', strict=True)
        assert result == {"key": "value"}

    def test_non_strict_with_comments(self) -> None:
        """Test non-strict allows comments."""
        json_str = '{"key": "value"} // comment'
        result = safe_json_loads(json_str, strict=False)
        assert result == {"key": "value"}


class TestSafeJsonDumps:
    """Tests for safe_json_dumps."""

    def test_basic_dumps(self) -> None:
        """Test basic serialization."""
        result = safe_json_dumps({"key": "value"})
        assert '"key": "value"' in result

    def test_datetime_serialization(self) -> None:
        """Test datetime serialization."""
        dt = datetime.datetime(2024, 1, 1, 12, 0, 0)
        result = safe_json_dumps({"date": dt})
        assert "2024-01-01T12:00:00" in result

    def test_none_returns_none(self) -> None:
        """Test None returns None string."""
        result = safe_json_dumps(object())
        assert result is None

    def test_indent(self) -> None:
        """Test indentation."""
        result = safe_json_dumps({"key": "value"}, indent=2)
        assert "\n" in result


class TestDeepMerge:
    """Tests for deep_merge."""

    def test_simple_merge(self) -> None:
        """Test simple dictionary merge."""
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        result = deep_merge(base, override)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_nested_merge(self) -> None:
        """Test nested dictionary merge."""
        base = {"outer": {"a": 1, "b": 2}}
        override = {"outer": {"b": 3, "c": 4}}
        result = deep_merge(base, override)
        assert result == {"outer": {"a": 1, "b": 3, "c": 4}}

    def test_inplace(self) -> None:
        """Test inplace modification."""
        base = {"a": 1}
        deep_merge(base, {"b": 2}, inplace=True)
        assert base == {"a": 1, "b": 2}

    def test_not_inplace_by_default(self) -> None:
        """Test default is not inplace."""
        base = {"a": 1}
        result = deep_merge(base, {"b": 2})
        assert base == {"a": 1}
        assert result == {"a": 1, "b": 2}


class TestLoadSaveJsonFile:
    """Tests for load_json_file and save_json_file."""

    def test_save_and_load(self) -> None:
        """Test saving and loading JSON file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            path = f.name

        try:
            data = {"key": "value", "number": 42}
            assert save_json_file(path, data)

            loaded = load_json_file(path)
            assert loaded == data
        finally:
            Path(path).unlink(missing_ok=True)

    def test_load_missing_file(self) -> None:
        """Test loading non-existent file."""
        result = load_json_file("/nonexistent/path.json", default={"default": True})
        assert result == {"default": True}

    def test_save_invalid_path(self) -> None:
        """Test save to invalid path."""
        result = save_json_file("/invalid/readonly/path.json", {"key": "value"})
        assert result is False


class TestPatchJsonFile:
    """Tests for patch_json_file."""

    def test_patch_existing_file(self) -> None:
        """Test patching existing file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            path = f.name
            f.write('{"a": 1, "b": 2}')

        try:
            result = patch_json_file(path, {"b": 3, "c": 4})
            assert result is True

            loaded = load_json_file(path)
            assert loaded == {"a": 1, "b": 3, "c": 4}
        finally:
            Path(path).unlink(missing_ok=True)

    def test_patch_create(self) -> None:
        """Test patch creates file if not exists."""
        path = tempfile.mktemp(suffix='.json')

        try:
            result = patch_json_file(path, {"key": "value"}, create=True)
            assert result is True
            assert load_json_file(path) == {"key": "value"}
        finally:
            Path(path).unlink(missing_ok=True)


class TestJSONFile:
    """Tests for JSONFile context manager."""

    def test_context_manager(self) -> None:
        """Test JSONFile as context manager."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            path = f.name

        try:
            with JSONFile(path) as data:
                data["key"] = "value"

            loaded = load_json_file(path)
            assert loaded == {"key": "value"}
        finally:
            Path(path).unlink(missing_ok=True)

    def test_load_method(self) -> None:
        """Test JSONFile load method."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            path = f.name
            f.write('{"loaded": true}')

        try:
            jf = JSONFile(path)
            data = jf.load()
            assert data == {"loaded": True}
        finally:
            Path(path).unlink(missing_ok=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])