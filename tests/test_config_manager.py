"""Tests for config_manager utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_manager import (
    ConfigManager,
    config_manager,
    AppConfig,
    DEFAULT_CONFIG,
)


class TestConfigManager:
    """Tests for ConfigManager."""

    def test_singleton(self) -> None:
        """Test ConfigManager is singleton."""
        cm1 = ConfigManager()
        cm2 = ConfigManager()
        assert cm1 is cm2

    def test_get_default_value(self) -> None:
        """Test getting default config value."""
        cm = ConfigManager()
        assert cm.get("app.name") is not None
        assert cm.get("nonexistent.key", "default") == "default"

    def test_get_section(self) -> None:
        """Test getting config section."""
        cm = ConfigManager()
        app_section = cm.get_section("app")
        assert isinstance(app_section, dict)

    def test_set_value(self) -> None:
        """Test setting config value."""
        cm = ConfigManager()
        cm.set("test.key", "test_value")
        assert cm.get("test.key") == "test_value"

    def test_get_all_keys(self) -> None:
        """Test getting all config keys."""
        cm = ConfigManager()
        keys = cm.get_all_keys()
        assert isinstance(keys, list)
        assert len(keys) > 0

    def test_reset(self) -> None:
        """Test resetting config to defaults."""
        cm = ConfigManager()
        cm.set("test.key", "value")
        cm.reset()
        # Reset clears custom values
        assert cm.get("test.key") is None


class TestAppConfig:
    """Tests for AppConfig dataclass."""

    def test_from_manager(self) -> None:
        """Test creating AppConfig from manager."""
        config = AppConfig.from_manager()
        assert isinstance(config, AppConfig)
        assert config.debug in [True, False]
        assert isinstance(config.log_level, str)


class TestDefaultConfig:
    """Tests for DEFAULT_CONFIG."""

    def test_default_config_structure(self) -> None:
        """Test DEFAULT_CONFIG has expected keys."""
        assert "app.name" in DEFAULT_CONFIG
        assert "app.version" in DEFAULT_CONFIG
        assert "execution.default_timeout" in DEFAULT_CONFIG

    def test_default_config_values(self) -> None:
        """Test DEFAULT_CONFIG has valid values."""
        assert isinstance(DEFAULT_CONFIG["app.name"], str)
        assert isinstance(DEFAULT_CONFIG["execution.default_timeout"], (int, float))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])