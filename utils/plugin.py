"""Plugin system utilities for RabAI AutoClick.

Provides:
- Plugin discovery
- Plugin loading
- Plugin manager
"""

import importlib
import importlib.util
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Type


@dataclass
class PluginMetadata:
    """Plugin metadata."""
    name: str
    version: str
    description: str = ""
    author: str = ""
    entry_point: str = ""
    dependencies: List[str] = field(default_factory=list)
    loaded: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> 'PluginMetadata':
        """Create from dictionary."""
        return cls(
            name=data.get("name", "unknown"),
            version=data.get("version", "0.0.0"),
            description=data.get("description", ""),
            author=data.get("author", ""),
            entry_point=data.get("entry_point", ""),
            dependencies=data.get("dependencies", []),
        )


class Plugin(ABC):
    """Base plugin class.

    All plugins must inherit from this class.
    """

    metadata: PluginMetadata

    @abstractmethod
    def initialize(self) -> None:
        """Initialize plugin.

        Called when plugin is loaded.
        """
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Shutdown plugin.

        Called when plugin is unloaded.
        """
        pass


class PluginManager:
    """Manages plugin loading and lifecycle.

    Usage:
        manager = PluginManager()
        manager.discover_plugins("./plugins")
        manager.load_all()

        plugin = manager.get_plugin("my_plugin")
        if plugin:
            plugin.initialize()

        manager.unload_all()
    """

    def __init__(self) -> None:
        self._plugins: Dict[str, Plugin] = {}
        self._metadata: Dict[str, PluginMetadata] = {}
        self._search_paths: List[Path] = []

    def add_search_path(self, path: Path) -> None:
        """Add directory to search for plugins.

        Args:
            path: Directory to search.
        """
        self._search_paths.append(path)

    def discover_plugins(self, path: Path) -> List[PluginMetadata]:
        """Discover plugins in directory.

        Args:
            path: Directory to search.

        Returns:
            List of discovered plugin metadata.
        """
        discovered = []

        if not path.exists():
            return discovered

        for item in path.iterdir():
            if not item.is_dir():
                continue

            manifest = item / "manifest.json"
            if manifest.exists():
                import json
                try:
                    with open(manifest) as f:
                        data = json.load(f)
                    metadata = PluginMetadata.from_dict(data)
                    metadata.entry_point = f"{item.name}.plugin"
                    discovered.append(metadata)
                    self._metadata[metadata.name] = metadata
                except Exception:
                    pass

        return discovered

    def load_plugin(self, metadata: PluginMetadata) -> bool:
        """Load a plugin.

        Args:
            metadata: Plugin metadata.

        Returns:
            True if loaded successfully.
        """
        if metadata.name in self._plugins:
            return True

        try:
            spec = importlib.util.find_spec(metadata.entry_point)
            if spec is None:
                return False

            module = importlib.import_module(metadata.entry_point)

            if hasattr(module, 'get_plugin'):
                plugin = module.get_plugin()
            else:
                plugin = module.Plugin()

            if not isinstance(plugin, Plugin):
                return False

            self._plugins[metadata.name] = plugin
            metadata.loaded = True
            return True

        except Exception:
            return False

    def unload_plugin(self, name: str) -> bool:
        """Unload a plugin.

        Args:
            name: Plugin name.

        Returns:
            True if unloaded successfully.
        """
        if name not in self._plugins:
            return False

        try:
            plugin = self._plugins[name]
            plugin.shutdown()
            del self._plugins[name]

            if name in self._metadata:
                self._metadata[name].loaded = False

            return True
        except Exception:
            return False

    def get_plugin(self, name: str) -> Optional[Plugin]:
        """Get loaded plugin by name.

        Args:
            name: Plugin name.

        Returns:
            Plugin instance or None.
        """
        return self._plugins.get(name)

    def load_all(self) -> int:
        """Load all discovered plugins.

        Returns:
            Number of plugins loaded.
        """
        loaded = 0
        for metadata in self._metadata.values():
            if not metadata.loaded:
                if self.load_plugin(metadata):
                    loaded += 1
        return loaded

    def unload_all(self) -> int:
        """Unload all plugins.

        Returns:
            Number of plugins unloaded.
        """
        unloaded = 0
        for name in list(self._plugins.keys()):
            if self.unload_plugin(name):
                unloaded += 1
        return unloaded

    @property
    def loaded_plugins(self) -> List[str]:
        """Get list of loaded plugin names."""
        return list(self._plugins.keys())

    @property
    def discovered_plugins(self) -> List[str]:
        """Get list of discovered plugin names."""
        return list(self._metadata.keys())


def discover_plugins_in_path(path: str) -> List[PluginMetadata]:
    """Convenience function to discover plugins.

    Args:
        path: Directory to search.

    Returns:
        List of plugin metadata.
    """
    manager = PluginManager()
    return manager.discover_plugins(Path(path))


class PluginRegistry:
    """Registry for plugin registration.

    Alternative to PluginManager using decorator pattern.

    Usage:
        @PluginRegistry.register("my_plugin")
        class MyPlugin(Plugin):
            metadata = PluginMetadata(name="my_plugin", version="1.0.0")
            ...
    """

    _plugins: Dict[str, Type[Plugin]] = {}

    @classmethod
    def register(cls, name: str) -> Callable[[Type[Plugin]], Type[Plugin]]:
        """Decorator to register plugin.

        Args:
            name: Plugin name.

        Returns:
            Decorator function.
        """
        def decorator(plugin_cls: Type[Plugin]) -> Type[Plugin]:
            cls._plugins[name] = plugin_cls
            return plugin_cls
        return decorator

    @classmethod
    def get(cls, name: str) -> Optional[Type[Plugin]]:
        """Get registered plugin class.

        Args:
            name: Plugin name.

        Returns:
            Plugin class or None.
        """
        return cls._plugins.get(name)

    @classmethod
    def create_instance(cls, name: str) -> Optional[Plugin]:
        """Create plugin instance.

        Args:
            name: Plugin name.

        Returns:
            Plugin instance or None.
        """
        plugin_cls = cls.get(name)
        if plugin_cls is None:
            return None
        return plugin_cls()

    @classmethod
    def list_plugins(cls) -> List[str]:
        """List registered plugin names."""
        return list(cls._plugins.keys())