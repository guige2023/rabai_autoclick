"""Plugin system utilities.

Dynamic plugin loading, lifecycle management, and dependency resolution
for extensible application architectures.

Example:
    manager = PluginManager()
    manager.register("my_plugin", MyPlugin())
    manager.enable("my_plugin")
    manager.invoke("process", data={"key": "value"})
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


class PluginState(Enum):
    """Lifecycle state of a plugin."""
    DISCOVERED = "discovered"
    LOADED = "loaded"
    ENABLED = "enabled"
    DISABLED = "disabled"
    ERROR = "error"


@dataclass
class PluginMetadata:
    """Metadata about a plugin."""
    name: str
    version: str
    author: str | None = None
    description: str | None = None
    dependencies: list[str] = field(default_factory=list)
    entry_point: str = "main"
    config_schema: dict[str, Any] | None = None


@dataclass
class Plugin:
    """Represents a loaded plugin with lifecycle methods."""
    metadata: PluginMetadata
    instance: Any = None
    state: PluginState = PluginState.DISCOVERED
    error_message: str | None = None
    loaded_at: Any = None

    def initialize(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the plugin with optional configuration."""
        if hasattr(self.instance, "initialize"):
            self.instance.initialize(config or {})
        self.state = PluginState.LOADED

    def enable(self) -> None:
        """Enable the plugin."""
        if hasattr(self.instance, "enable"):
            self.instance.enable()
        self.state = PluginState.ENABLED

    def disable(self) -> None:
        """Disable the plugin."""
        if hasattr(self.instance, "disable"):
            self.instance.disable()
        self.state = PluginState.DISABLED

    def shutdown(self) -> None:
        """Shutdown the plugin and release resources."""
        if hasattr(self.instance, "shutdown"):
            self.instance.shutdown()


class PluginManager:
    """Manages plugin discovery, loading, and lifecycle.

    Supports dynamic loading from Python files/directories, dependency
    ordering, and event-driven enable/disable hooks.
    """

    def __init__(self) -> None:
        self._plugins: dict[str, Plugin] = {}
        self._hooks: dict[str, list[Callable]] = {
            "on_load": [],
            "on_enable": [],
            "on_disable": [],
            "before_invoke": [],
            "after_invoke": [],
        }

    def register(
        self,
        name: str,
        instance: Any,
        metadata: PluginMetadata | None = None,
    ) -> Plugin:
        """Register a plugin instance.

        Args:
            name: Unique plugin name.
            instance: Plugin instance object.
            metadata: Optional plugin metadata.

        Returns:
            The registered Plugin object.
        """
        if name in self._plugins:
            logger.warning("Plugin %s already registered, replacing", name)

        meta = metadata or PluginMetadata(name=name, version="0.0.0")
        plugin = Plugin(metadata=meta, instance=instance)
        self._plugins[name] = plugin

        logger.info("Registered plugin: %s v%s", name, meta.version)
        self._trigger("on_load", plugin)

        return plugin

    def load_from_file(self, path: str | Path) -> Plugin | None:
        """Load a plugin from a Python file.

        Args:
            path: Path to Python file containing plugin class.

        Returns:
            Loaded Plugin or None on failure.
        """
        path = Path(path)
        if not path.exists():
            logger.error("Plugin file not found: %s", path)
            return None

        module_name = path.stem

        try:
            spec = importlib.util.spec_from_file_location(module_name, path)
            if not spec or not spec.loader:
                return None

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            plugin_class = getattr(module, "Plugin", None)
            if not plugin_class:
                logger.error("No Plugin class found in %s", path)
                return None

            instance = plugin_class()
            meta = getattr(instance, "metadata", None)
            if isinstance(meta, dict):
                meta = PluginMetadata(**meta)

            plugin = self.register(
                name=meta.name if meta else module_name,
                instance=instance,
                metadata=meta,
            )
            plugin.state = PluginState.LOADED
            return plugin

        except Exception as e:
            logger.error("Failed to load plugin from %s: %s", path, e)
            return None

    def load_from_directory(self, directory: str | Path) -> list[Plugin]:
        """Discover and load all plugins from a directory.

        Args:
            directory: Directory containing plugin Python files.

        Returns:
            List of loaded Plugin objects.
        """
        directory = Path(directory)
        if not directory.is_dir():
            logger.error("Plugin directory not found: %s", directory)
            return []

        loaded: list[Plugin] = []

        for fpath in directory.glob("*.py"):
            if fpath.name.startswith("_"):
                continue
            plugin = self.load_from_file(fpath)
            if plugin:
                loaded.append(plugin)

        return loaded

    def enable(self, name: str) -> bool:
        """Enable a registered plugin.

        Args:
            name: Plugin name to enable.

        Returns:
            True if successful, False otherwise.
        """
        plugin = self._plugins.get(name)
        if not plugin:
            logger.error("Plugin not found: %s", name)
            return False

        if plugin.state == PluginState.ENABLED:
            return True

        if not self._check_dependencies(plugin):
            logger.error("Dependency check failed for plugin: %s", name)
            return False

        try:
            plugin.enable()
            self._trigger("on_enable", plugin)
            logger.info("Enabled plugin: %s", name)
            return True
        except Exception as e:
            plugin.state = PluginState.ERROR
            plugin.error_message = str(e)
            logger.error("Failed to enable plugin %s: %s", name, e)
            return False

    def disable(self, name: str) -> bool:
        """Disable an enabled plugin.

        Args:
            name: Plugin name to disable.

        Returns:
            True if successful, False otherwise.
        """
        plugin = self._plugins.get(name)
        if not plugin:
            return False

        dependents = self._get_dependent_plugins(name)
        if dependents:
            logger.warning(
                "Cannot disable %s: %d plugins depend on it: %s",
                name, len(dependents), [p.metadata.name for p in dependents],
            )
            return False

        try:
            plugin.disable()
            self._trigger("on_disable", plugin)
            logger.info("Disabled plugin: %s", name)
            return True
        except Exception as e:
            logger.error("Failed to disable plugin %s: %s", name, e)
            return False

    def invoke(self, method: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Invoke a method on all enabled plugins.

        Args:
            method: Method name to call.
            *args: Positional arguments passed to each plugin.
            **kwargs: Keyword arguments passed to each plugin.

        Returns:
            List of return values from each plugin.
        """
        results: list[Any] = []
        for plugin in self.enabled_plugins():
            if not hasattr(plugin.instance, method):
                continue
            try:
                self._trigger("before_invoke", plugin, method, args, kwargs)
                result = getattr(plugin.instance, method)(*args, **kwargs)
                self._trigger("after_invoke", plugin, method, result)
                results.append(result)
            except Exception as e:
                logger.error("Plugin %s.%s failed: %s", plugin.metadata.name, method, e)
        return results

    def enabled_plugins(self) -> list[Plugin]:
        """Get all currently enabled plugins."""
        return [p for p in self._plugins.values() if p.state == PluginState.ENABLED]

    def all_plugins(self) -> list[Plugin]:
        """Get all registered plugins."""
        return list(self._plugins.values())

    def get(self, name: str) -> Plugin | None:
        """Get plugin by name."""
        return self._plugins.get(name)

    def unregister(self, name: str) -> bool:
        """Unregister and shutdown a plugin.

        Args:
            name: Plugin name to unregister.

        Returns:
            True if plugin was removed.
        """
        plugin = self._plugins.pop(name, None)
        if plugin:
            plugin.shutdown()
            return True
        return False

    def register_hook(self, event: str, handler: Callable) -> None:
        """Register a hook handler for lifecycle events.

        Args:
            event: Event name ("on_load", "on_enable", etc).
            handler: Callable to invoke when event fires.
        """
        if event in self._hooks:
            self._hooks[event].append(handler)

    def _trigger(self, event: str, *args: Any, **kwargs: Any) -> None:
        """Trigger all handlers for an event."""
        for handler in self._hooks.get(event, []):
            try:
                handler(*args, **kwargs)
            except Exception as e:
                logger.error("Hook handler for %s failed: %s", event, e)

    def _check_dependencies(self, plugin: Plugin) -> bool:
        """Check if all plugin dependencies are enabled."""
        for dep in plugin.metadata.dependencies:
            dep_plugin = self._plugins.get(dep)
            if not dep_plugin or dep_plugin.state != PluginState.ENABLED:
                return False
        return True

    def _get_dependent_plugins(self, name: str) -> list[Plugin]:
        """Get plugins that depend on the given plugin."""
        return [
            p for p in self._plugins.values()
            if name in p.metadata.dependencies and p.state == PluginState.ENABLED
        ]
