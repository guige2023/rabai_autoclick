"""Tests for validation utilities."""

import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.validation import (
    validate_workflow_config,
    validate_step,
    validate_coordinates,
    validate_file_path,
    validate_action_params,
    validate_screen_region,
    sanitize_string,
    validate_json_serializable,
    ValidationResult,
)


class TestValidateWorkflowConfig:
    """Tests for validate_workflow_config."""

    def test_valid_config(self) -> None:
        """Test validation of valid workflow config."""
        config = {
            "workflow_id": "test_wf",
            "name": "Test Workflow",
            "steps": [
                {"action": "click", "target": "button"},
                {"action": "wait", "delay": 1.0},
            ],
        }
        result = validate_workflow_config(config)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_missing_workflow_id(self) -> None:
        """Test validation with missing workflow_id."""
        config = {"name": "Test"}
        result = validate_workflow_config(config)
        assert not result.is_valid
        assert any("workflow_id" in e for e in result.errors)

    def test_missing_name(self) -> None:
        """Test validation with missing name."""
        config = {"workflow_id": "test"}
        result = validate_workflow_config(config)
        assert not result.is_valid
        assert any("name" in e for e in result.errors)

    def test_invalid_steps_type(self) -> None:
        """Test validation with invalid steps type."""
        config = {
            "workflow_id": "test",
            "name": "Test",
            "steps": "not a list",
        }
        result = validate_workflow_config(config)
        assert not result.is_valid

    def test_empty_steps(self) -> None:
        """Test validation with empty steps list."""
        config = {
            "workflow_id": "test",
            "name": "Test",
            "steps": [],
        }
        result = validate_workflow_config(config)
        assert result.is_valid

    def test_invalid_timeout(self) -> None:
        """Test validation with invalid timeout."""
        config = {
            "workflow_id": "test",
            "name": "Test",
            "timeout": -5,
        }
        result = validate_workflow_config(config)
        assert not result.is_valid


class TestValidateStep:
    """Tests for validate_step."""

    def test_valid_step(self) -> None:
        """Test validation of valid step."""
        step = {"action": "click", "target": "button"}
        result = validate_step(step, 0)
        assert result.is_valid

    def test_missing_action(self) -> None:
        """Test validation with missing action."""
        step = {"target": "button"}
        result = validate_step(step, 0)
        assert not result.is_valid

    def test_invalid_action_type(self) -> None:
        """Test validation with non-string action."""
        step = {"action": 123}
        result = validate_step(step, 0)
        assert not result.is_valid

    def test_invalid_delay(self) -> None:
        """Test validation with negative delay."""
        step = {"action": "wait", "delay": -1}
        result = validate_step(step, 0)
        assert not result.is_valid


class TestValidateCoordinates:
    """Tests for validate_coordinates."""

    def test_valid_integers(self) -> None:
        """Test validation of valid integer coordinates."""
        valid, msg = validate_coordinates(100, 200)
        assert valid
        assert msg is None

    def test_valid_strings(self) -> None:
        """Test validation of string coordinates that convert."""
        valid, msg = validate_coordinates("100", "200")
        assert valid
        assert msg is None

    def test_negative_coordinates(self) -> None:
        """Test validation rejects negative coordinates."""
        valid, msg = validate_coordinates(-1, 100)
        assert not valid
        assert msg is not None

    def test_non_numeric(self) -> None:
        """Test validation rejects non-numeric coordinates."""
        valid, msg = validate_coordinates("abc", 100)
        assert not valid

    def test_extremely_large(self) -> None:
        """Test validation rejects extremely large coordinates."""
        valid, msg = validate_coordinates(50000, 50000)
        assert not valid


class TestValidateFilePath:
    """Tests for validate_file_path."""

    def test_valid_path(self) -> None:
        """Test validation of valid file path."""
        with tempfile.NamedTemporaryFile(suffix=".json") as f:
            result = validate_file_path(f.name, must_exist=True)
            assert result.is_valid

    def test_nonexistent_file(self) -> None:
        """Test validation with non-existent file when required."""
        result = validate_file_path("/nonexistent/file.json", must_exist=True)
        assert not result.is_valid

    def test_extension_check(self) -> None:
        """Test extension validation."""
        with tempfile.NamedTemporaryFile(suffix=".txt") as f:
            result = validate_file_path(f.name, must_exist=True, extensions=[".json"])
            assert not result.is_valid

    def test_empty_path(self) -> None:
        """Test validation rejects empty path."""
        result = validate_file_path("")
        assert not result.is_valid

    def test_path_traversal(self) -> None:
        """Test validation rejects path traversal."""
        result = validate_file_path("/path/../../../etc/passwd")
        assert not result.is_valid


class TestValidateActionParams:
    """Tests for validate_action_params."""

    def test_valid_click_action(self) -> None:
        """Test validation of valid click action."""
        params = {"x": 100, "y": 200, "button": "left", "clicks": 2}
        result = validate_action_params("click", params)
        assert result.is_valid

    def test_invalid_button(self) -> None:
        """Test validation rejects invalid button."""
        params = {"button": "invalid"}
        result = validate_action_params("click", params)
        assert not result.is_valid

    def test_invalid_clicks(self) -> None:
        """Test validation rejects invalid clicks count."""
        params = {"clicks": 15}
        result = validate_action_params("click", params)
        assert not result.is_valid

    def test_type_action_requires_text(self) -> None:
        """Test type action requires text parameter."""
        result = validate_action_params("type", {})
        assert not result.is_valid

    def test_press_action_requires_key(self) -> None:
        """Test press action requires key parameter."""
        result = validate_action_params("press", {})
        assert not result.is_valid


class TestValidateScreenRegion:
    """Tests for validate_screen_region."""

    def test_valid_region(self) -> None:
        """Test validation of valid region."""
        result = validate_screen_region([100, 200, 300, 400])
        assert result.is_valid

    def test_none_region(self) -> None:
        """Test validation accepts None region."""
        result = validate_screen_region(None)
        assert result.is_valid

    def test_invalid_length(self) -> None:
        """Test validation rejects wrong length."""
        result = validate_screen_region([100, 200, 300])
        assert not result.is_valid

    def test_negative_dimensions(self) -> None:
        """Test validation rejects negative dimensions."""
        result = validate_screen_region([100, 200, -50, 400])
        assert not result.is_valid

    def test_zero_dimensions(self) -> None:
        """Test validation rejects zero dimensions."""
        result = validate_screen_region([100, 200, 0, 400])
        assert not result.is_valid


class TestSanitizeString:
    """Tests for sanitize_string."""

    def test_basic_sanitization(self) -> None:
        """Test basic string sanitization."""
        result = sanitize_string("  hello world  ")
        assert result == "hello world"

    def test_null_removal(self) -> None:
        """Test null character removal."""
        result = sanitize_string("hello\x00world")
        assert "\x00" not in result
        assert result == "helloworld"

    def test_max_length(self) -> None:
        """Test max length truncation."""
        result = sanitize_string("a" * 2000, max_length=100)
        assert len(result) == 100

    def test_non_string(self) -> None:
        """Test non-string conversion."""
        result = sanitize_string(123)
        assert result == "123"


class TestValidateJsonSerializable:
    """Tests for validate_json_serializable."""

    def test_serializable_types(self) -> None:
        """Test that basic types are serializable."""
        for value in [None, True, False, 1, 1.5, "string", [1, 2], {"key": "value"}]:
            valid, errors = validate_json_serializable(value)
            assert valid, f"{type(value)} should be serializable: {errors}"

    def test_nested_dict(self) -> None:
        """Test nested dictionary validation."""
        data = {"outer": {"inner": {"value": 1}}}
        valid, errors = validate_json_serializable(data)
        assert valid

    def test_unserializable_type(self) -> None:
        """Test unserializable type detection."""
        valid, errors = validate_json_serializable(set([1, 2, 3]))
        assert not valid
        assert len(errors) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])