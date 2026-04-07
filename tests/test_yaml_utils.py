"""Tests for YAML utilities."""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.yaml_utils import (
    load_yaml,
    dump_yaml,
    safe_load_yaml,
    load_yaml_or_json,
    merge_yaml,
    validate_yaml_schema,
)


class TestLoadYaml:
    """Tests for load_yaml."""

    def test_load_valid_yaml(self) -> None:
        """Test loading valid YAML."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("name: test\nversion: 1.0\n")
            path = f.name
        try:
            result = load_yaml(path)
            assert result is not None
            assert result['name'] == 'test'
            assert result['version'] == 1.0
        finally:
            os.unlink(path)

    def test_load_invalid_yaml(self) -> None:
        """Test loading invalid YAML."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content:")
            path = f.name
        try:
            result = load_yaml(path)
            assert result is None
        finally:
            os.unlink(path)

    def test_load_nonexistent_file(self) -> None:
        """Test loading nonexistent file."""
        result = load_yaml("/nonexistent/path.yaml")
        assert result is None


class TestDumpYaml:
    """Tests for dump_yaml."""

    def test_dump_to_string(self) -> None:
        """Test dumping to YAML string."""
        data = {"name": "test", "version": "1.0"}
        result = dump_yaml(data)
        assert result is not None
        assert "name: test" in result
        assert "version: '1.0'" in result

    def test_dump_to_file(self) -> None:
        """Test dumping to YAML file."""
        data = {"name": "test", "items": [1, 2, 3]}
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            path = f.name
        try:
            result = dump_yaml(data, path)
            assert result is None
            with open(path, 'r') as f:
                content = f.read()
            assert "name: test" in content
        finally:
            os.unlink(path)

    def test_dump_with_indent(self) -> None:
        """Test dumping with custom indent."""
        data = {"nested": {"key": "value"}}
        result = dump_yaml(data, indent=4)
        assert result is not None
        assert "    nested:" in result


class TestSafeLoadYaml:
    """Tests for safe_load_yaml."""

    def test_safe_load_yaml(self) -> None:
        """Test safe loading YAML."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("key: value\n")
            path = f.name
        try:
            result = safe_load_yaml(path)
            assert result is not None
            assert result['key'] == 'value'
        finally:
            os.unlink(path)


class TestLoadYamlOrJson:
    """Tests for load_yaml_or_json."""

    def test_load_yaml_file(self) -> None:
        """Test loading YAML file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("type: yaml\n")
            path = f.name
        try:
            result = load_yaml_or_json(path)
            assert result is not None
            assert result['type'] == 'yaml'
        finally:
            os.unlink(path)

    def test_load_json_file(self) -> None:
        """Test loading JSON file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"type": "json"}')
            path = f.name
        try:
            result = load_yaml_or_json(path)
            assert result is not None
            assert result['type'] == 'json'
        finally:
            os.unlink(path)

    def test_load_unsupported_extension(self) -> None:
        """Test loading unsupported file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("content")
            path = f.name
        try:
            result = load_yaml_or_json(path)
            assert result is None
        finally:
            os.unlink(path)


class TestMergeYaml:
    """Tests for merge_yaml."""

    def test_merge_simple(self) -> None:
        """Test simple merge."""
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        result = merge_yaml(base, override)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_merge_nested(self) -> None:
        """Test nested merge."""
        base = {"config": {"host": "localhost", "port": 8080}}
        override = {"config": {"port": 9090}}
        result = merge_yaml(base, override)
        assert result == {"config": {"host": "localhost", "port": 9090}}

    def test_merge_overrides_non_dict_with_dict(self) -> None:
        """Test non-dict replaced by dict."""
        base = {"value": 42}
        override = {"value": {"nested": True}}
        result = merge_yaml(base, override)
        assert result == {"value": {"nested": True}}

    def test_merge_empty_override(self) -> None:
        """Test merge with empty override."""
        base = {"a": 1, "b": 2}
        result = merge_yaml(base, {})
        assert result == base


class TestValidateYamlSchema:
    """Tests for validate_yaml_schema."""

    def test_valid_schema(self) -> None:
        """Test valid schema."""
        data = {"name": "test", "count": 42}
        schema = {"name": str, "count": int}
        assert validate_yaml_schema(data, schema) is True

    def test_missing_key(self) -> None:
        """Test missing key."""
        data = {"name": "test"}
        schema = {"name": str, "count": int}
        assert validate_yaml_schema(data, schema) is False

    def test_wrong_type(self) -> None:
        """Test wrong type."""
        data = {"name": "test", "count": "not an int"}
        schema = {"name": str, "count": int}
        assert validate_yaml_schema(data, schema) is False

    def test_empty_schema(self) -> None:
        """Test empty schema."""
        data = {"any": "thing"}
        assert validate_yaml_schema(data, {}) is True

    def test_empty_data(self) -> None:
        """Test empty data."""
        schema = {"name": str}
        assert validate_yaml_schema({}, schema) is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])