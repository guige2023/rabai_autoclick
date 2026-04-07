"""Tests for configuration utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import (
    ConfigOption,
    Config,
    EnvironmentConfig,
    ConfigLoader,
    ConfigValidator,
    ConfigSection,
    ConfigManager,
)


class TestConfigOption:
    """Tests for ConfigOption."""

    def test_create(self) -> None:
        """Test creating option."""
        opt = ConfigOption(name="test", value=1, default=0)
        assert opt.name == "test"
        assert opt.value == 1


class TestConfig:
    """Tests for Config."""

    def test_create(self) -> None:
        """Test creating config."""
        config = Config()
        assert len(config._options) == 0

    def test_add_option(self) -> None:
        """Test adding option."""
        config = Config()
        config.add_option("test", default=1, required=True)
        assert "test" in config._options

    def test_get_set(self) -> None:
        """Test getting and setting."""
        config = Config()
        config.add_option("test", default=1)
        assert config.get("test") == 1
        config.set("test", 2)
        assert config.get("test") == 2

    def test_get_default(self) -> None:
        """Test getting default value."""
        config = Config()
        assert config.get("nonexistent", 42) == 42

    def test_validate_required(self) -> None:
        """Test required validation."""
        config = Config()
        config.add_option("test", required=True)
        errors = config.validate()
        assert len(errors) == 1

    def test_to_dict(self) -> None:
        """Test converting to dict."""
        config = Config()
        config.add_option("a", default=1)
        config.add_option("b", default=2)
        d = config.to_dict()
        assert d["a"] == 1
        assert d["b"] == 2

    def test_from_dict(self) -> None:
        """Test loading from dict."""
        config = Config()
        config.add_option("a", default=0)
        config.from_dict({"a": 10, "b": 20})
        assert config.get("a") == 10
        assert config.get("b") == 20


class TestEnvironmentConfig:
    """Tests for EnvironmentConfig."""

    def test_create(self) -> None:
        """Test creating env config."""
        config = EnvironmentConfig(prefix="TEST_")
        assert config._prefix == "TEST_"


class TestConfigLoader:
    """Tests for ConfigLoader."""

    def test_load_json_nonexistent(self) -> None:
        """Test loading nonexistent JSON."""
        result = ConfigLoader.load_json("/nonexistent.json")
        assert result is None

    def test_load_yaml_nonexistent(self) -> None:
        """Test loading nonexistent YAML."""
        result = ConfigLoader.load_yaml("/nonexistent.yaml")
        assert result is None


class TestConfigValidator:
    """Tests for ConfigValidator."""

    def test_is_positive_int(self) -> None:
        """Test positive int validator."""
        assert ConfigValidator.is_positive_int(1) is True
        assert ConfigValidator.is_positive_int(0) is False
        assert ConfigValidator.is_positive_int(-1) is False

    def test_is_non_negative_int(self) -> None:
        """Test non-negative int validator."""
        assert ConfigValidator.is_non_negative_int(0) is True
        assert ConfigValidator.is_non_negative_int(1) is True
        assert ConfigValidator.is_non_negative_int(-1) is False

    def test_is_in_range(self) -> None:
        """Test range validator."""
        validator = ConfigValidator.is_in_range(0, 10)
        assert validator(5) is True
        assert validator(0) is True
        assert validator(10) is True
        assert validator(-1) is False
        assert validator(11) is False

    def test_is_one_of(self) -> None:
        """Test choices validator."""
        validator = ConfigValidator.is_one_of([1, 2, 3])
        assert validator(1) is True
        assert validator(4) is False


class TestConfigSection:
    """Tests for ConfigSection."""

    def test_create(self) -> None:
        """Test creating section."""
        section = ConfigSection("test")
        assert section.name == "test"

    def test_add_option(self) -> None:
        """Test adding option."""
        section = ConfigSection("test")
        section.add_option("a", default=1)
        assert section.get("a") == 1

    def test_validate(self) -> None:
        """Test validation."""
        section = ConfigSection("test")
        section.add_option("a", required=True)
        errors = section.validate()
        assert len(errors) == 1


class TestConfigManager:
    """Tests for ConfigManager."""

    def test_create(self) -> None:
        """Test creating manager."""
        manager = ConfigManager()
        assert len(manager._sections) == 0

    def test_add_section(self) -> None:
        """Test adding section."""
        manager = ConfigManager()
        section = manager.add_section("test")
        assert section.name == "test"

    def test_get_section(self) -> None:
        """Test getting section."""
        manager = ConfigManager()
        manager.add_section("test")
        section = manager.get_section("test")
        assert section is not None

    def test_validate_all(self) -> None:
        """Test validating all sections."""
        manager = ConfigManager()
        section = manager.add_section("test")
        section.add_option("a", required=True)
        errors = manager.validate_all()
        assert len(errors) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])