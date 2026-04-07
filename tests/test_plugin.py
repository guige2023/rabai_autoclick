"""Tests for plugin utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.plugin import (
    PluginMetadata,
    Plugin,
    PluginManager,
    PluginRegistry,
    discover_plugins_in_path,
)


class DummyPlugin(Plugin):
    """Dummy plugin for testing."""

    metadata = PluginMetadata(
        name="dummy",
        version="1.0.0",
        description="Test plugin",
    )

    def __init__(self):
        self.initialized = False
        self.shutdown_called = False

    def initialize(self) -> None:
        self.initialized = True

    def shutdown(self) -> None:
        self.shutdown_called = True


class TestPluginMetadata:
    """Tests for PluginMetadata."""

    def test_create(self) -> None:
        """Test creating metadata."""
        meta = PluginMetadata(name="test", version="1.0.0")
        assert meta.name == "test"
        assert meta.version == "1.0.0"
        assert meta.loaded is False

    def test_from_dict(self) -> None:
        """Test creating from dict."""
        data = {"name": "test", "version": "1.0.0", "author": "Test"}
        meta = PluginMetadata.from_dict(data)
        assert meta.name == "test"
        assert meta.author == "Test"


class TestPlugin:
    """Tests for Plugin."""

    def test_dummy_plugin_initialize(self) -> None:
        """Test initializing plugin."""
        plugin = DummyPlugin()
        plugin.initialize()
        assert plugin.initialized is True

    def test_dummy_plugin_shutdown(self) -> None:
        """Test shutting down plugin."""
        plugin = DummyPlugin()
        plugin.shutdown()
        assert plugin.shutdown_called is True


class TestPluginManager:
    """Tests for PluginManager."""

    def test_create(self) -> None:
        """Test creating manager."""
        manager = PluginManager()
        assert len(manager._plugins) == 0

    def test_add_search_path(self) -> None:
        """Test adding search path."""
        from pathlib import Path
        manager = PluginManager()
        manager.add_search_path(Path("/tmp/plugins"))
        assert len(manager._search_paths) == 1

    def test_discover_plugins_nonexistent(self) -> None:
        """Test discovering in nonexistent path."""
        from pathlib import Path
        manager = PluginManager()
        result = manager.discover_plugins(Path("/nonexistent/path"))
        assert result == []

    def test_load_plugin(self) -> None:
        """Test loading plugin."""
        meta = PluginMetadata(name="dummy", version="1.0.0")
        manager = PluginManager()
        manager._plugins["dummy"] = DummyPlugin()
        meta.loaded = True
        result = manager.load_plugin(meta)
        assert result is True

    def test_unload_plugin(self) -> None:
        """Test unloading plugin."""
        manager = PluginManager()
        plugin = DummyPlugin()
        manager._plugins["dummy"] = plugin
        result = manager.unload_plugin("dummy")
        assert result is True
        assert "dummy" not in manager._plugins

    def test_get_plugin(self) -> None:
        """Test getting plugin."""
        manager = PluginManager()
        manager._plugins["dummy"] = DummyPlugin()
        plugin = manager.get_plugin("dummy")
        assert plugin is not None

    def test_get_plugin_not_found(self) -> None:
        """Test getting nonexistent plugin."""
        manager = PluginManager()
        plugin = manager.get_plugin("nonexistent")
        assert plugin is None

    def test_load_all(self) -> None:
        """Test loading all plugins."""
        manager = PluginManager()
        meta = PluginMetadata(name="dummy", version="1.0.0", loaded=False)
        manager._metadata["dummy"] = meta
        manager._plugins["dummy"] = DummyPlugin()
        meta.loaded = True
        loaded = manager.load_all()
        assert loaded == 0

    def test_unload_all(self) -> None:
        """Test unloading all plugins."""
        manager = PluginManager()
        manager._plugins["dummy"] = DummyPlugin()
        manager._metadata["dummy"] = PluginMetadata(name="dummy", version="1.0.0", loaded=True)
        unloaded = manager.unload_all()
        assert unloaded == 1

    def test_loaded_plugins(self) -> None:
        """Test loaded plugins property."""
        manager = PluginManager()
        manager._plugins["dummy"] = DummyPlugin()
        assert "dummy" in manager.loaded_plugins

    def test_discovered_plugins(self) -> None:
        """Test discovered plugins property."""
        manager = PluginManager()
        manager._metadata["dummy"] = PluginMetadata(name="dummy", version="1.0.0")
        assert "dummy" in manager.discovered_plugins


class TestPluginRegistry:
    """Tests for PluginRegistry."""

    def test_register(self) -> None:
        """Test registering plugin."""

        @PluginRegistry.register("test_plugin")
        class TestPlugin(Plugin):
            metadata = PluginMetadata(name="test_plugin", version="1.0.0")

            def initialize(self) -> None:
                pass

            def shutdown(self) -> None:
                pass

        assert PluginRegistry.get("test_plugin") is not None

    def test_get(self) -> None:
        """Test getting registered plugin."""
        PluginRegistry._plugins.clear()
        PluginRegistry.register("my_plugin")(DummyPlugin)
        assert PluginRegistry.get("my_plugin") is not None

    def test_create_instance(self) -> None:
        """Test creating plugin instance."""
        PluginRegistry._plugins.clear()
        PluginRegistry.register("my_plugin")(DummyPlugin)
        instance = PluginRegistry.create_instance("my_plugin")
        assert isinstance(instance, DummyPlugin)

    def test_list_plugins(self) -> None:
        """Test listing plugins."""
        PluginRegistry._plugins.clear()
        PluginRegistry.register("plugin1")(DummyPlugin)
        PluginRegistry.register("plugin2")(DummyPlugin)
        plugins = PluginRegistry.list_plugins()
        assert len(plugins) >= 2


class TestDiscoverPluginsInPath:
    """Tests for discover_plugins_in_path."""

    def test_nonexistent_path(self) -> None:
        """Test discovering in nonexistent path."""
        result = discover_plugins_in_path("/nonexistent/path")
        assert result == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])