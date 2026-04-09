"""Plugin loader action for dynamic plugin management.

Discovers, loads, and manages plugins with lifecycle hooks
and dependency resolution.
"""

import importlib
import logging
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class PluginState(Enum):
    DISCOVERED = "discovered"
    LOADING = "loading"
    LOADED = "loaded"
    INITIALIZED = "initialized"
    FAILED = "failed"
    DISABLED = "disabled"


@dataclass
class PluginMetadata:
    name: str
    version: str
    author: str = ""
    description: str = ""
    dependencies: list[str] = field(default_factory=list)
    entry_point: str = "main"


@dataclass
class Plugin:
    metadata: PluginMetadata
    state: PluginState = PluginState.DISCOVERED
    module: Optional[Any] = None
    instance: Optional[Any] = None
    error: Optional[str] = None


class PluginLoaderAction:
    """Load and manage plugins dynamically.

    Args:
        plugin_dir: Directory to search for plugins.
        auto_init: Automatically initialize plugins after loading.
    """

    def __init__(
        self,
        plugin_dir: Optional[str] = None,
        auto_init: bool = True,
    ) -> None:
        self._plugin_dir = Path(plugin_dir) if plugin_dir else Path.cwd() / "plugins"
        self._auto_init = auto_init
        self._plugins: dict[str, Plugin] = {}
        self._load_order: list[str] = []
        self._lifecycle_hooks: dict[str, list[Callable]] = {
            "on_load": [],
            "on_init": [],
            "on_enable": [],
            "on_disable": [],
            "on_unload": [],
        }

    def discover(self) -> list[str]:
        """Discover plugins in the plugin directory.

        Returns:
            List of discovered plugin names.
        """
        discovered = []
        if not self._plugin_dir.exists():
            logger.warning(f"Plugin directory not found: {self._plugin_dir}")
            return discovered

        for path in self._plugin_dir.iterdir():
            if path.is_dir() and not path.name.startswith("_"):
                plugin_name = path.name
                if self._is_valid_plugin(path):
                    if plugin_name not in self._plugins:
                        plugin = Plugin(
                            metadata=PluginMetadata(
                                name=plugin_name,
                                version="0.0.0",
                            )
                        )
                        self._plugins[plugin_name] = plugin
                    discovered.append(plugin_name)
                    logger.debug(f"Discovered plugin: {plugin_name}")

        return discovered

    def _is_valid_plugin(self, path: Path) -> bool:
        """Check if a path contains a valid plugin.

        Args:
            path: Plugin directory path.

        Returns:
            True if valid plugin structure.
        """
        return (path / "__init__.py").exists() or (path / "plugin.py").exists()

    def load(self, plugin_name: str) -> bool:
        """Load a plugin by name.

        Args:
            plugin_name: Name of plugin to load.

        Returns:
            True if loading was successful.
        """
        if plugin_name not in self._plugins:
            logger.error(f"Plugin not found: {plugin_name}")
            return False

        plugin = self._plugins[plugin_name]
        if plugin.state in (PluginState.LOADED, PluginState.INITIALIZED):
            logger.warning(f"Plugin already loaded: {plugin_name}")
            return True

        plugin.state = PluginState.LOADING

        try:
            module_name = f"plugins.{plugin_name}.plugin"
            plugin.module = importlib.import_module(module_name)
            plugin.state = PluginState.LOADED
            self._run_hooks("on_load", plugin_name)
            logger.info(f"Loaded plugin: {plugin_name}")

            if self._auto_init:
                return self.initialize(plugin_name)
            return True

        except Exception as e:
            plugin.state = PluginState.FAILED
            plugin.error = str(e)
            logger.error(f"Failed to load plugin {plugin_name}: {e}")
            return False

    def initialize(self, plugin_name: str) -> bool:
        """Initialize a loaded plugin.

        Args:
            plugin_name: Name of plugin to initialize.

        Returns:
            True if initialization was successful.
        """
        if plugin_name not in self._plugins:
            return False

        plugin = self._plugins[plugin_name]
        if plugin.state != PluginState.LOADED:
            logger.warning(f"Plugin not loaded: {plugin_name}")
            return False

        try:
            if hasattr(plugin.module, "Plugin"):
                plugin.instance = plugin.module.Plugin()
                if hasattr(plugin.instance, "initialize"):
                    plugin.instance.initialize()
            plugin.state = PluginState.INITIALIZED
            self._run_hooks("on_init", plugin_name)
            logger.info(f"Initialized plugin: {plugin_name}")
            return True

        except Exception as e:
            plugin.state = PluginState.FAILED
            plugin.error = str(e)
            logger.error(f"Failed to initialize plugin {plugin_name}: {e}")
            return False

    def enable(self, plugin_name: str) -> bool:
        """Enable a plugin.

        Args:
            plugin_name: Name of plugin to enable.

        Returns:
            True if enabling was successful.
        """
        if plugin_name not in self._plugins:
            return False

        plugin = self._plugins[plugin_name]
        if plugin.state == PluginState.DISABLED:
            plugin.state = PluginState.LOADED
        self._run_hooks("on_enable", plugin_name)
        return True

    def disable(self, plugin_name: str) -> bool:
        """Disable a plugin.

        Args:
            plugin_name: Name of plugin to disable.

        Returns:
            True if disabling was successful.
        """
        if plugin_name not in self._plugins:
            return False

        plugin = self._plugins[plugin_name]
        plugin.state = PluginState.DISABLED
        self._run_hooks("on_disable", plugin_name)
        return True

    def unload(self, plugin_name: str) -> bool:
        """Unload a plugin.

        Args:
            plugin_name: Name of plugin to unload.

        Returns:
            True if unloading was successful.
        """
        if plugin_name not in self._plugins:
            return False

        plugin = self._plugins[plugin_name]
        self._run_hooks("on_unload", plugin_name)

        if plugin.instance and hasattr(plugin.instance, "shutdown"):
            try:
                plugin.instance.shutdown()
            except Exception as e:
                logger.error(f"Plugin shutdown error: {e}")

        if plugin.module:
            module_name = f"plugins.{plugin_name}.plugin"
            if module_name in sys.modules:
                del sys.modules[module_name]

        plugin.state = PluginState.DISCOVERED
        plugin.module = None
        plugin.instance = None
        logger.info(f"Unloaded plugin: {plugin_name}")
        return True

    def get_plugin(self, plugin_name: str) -> Optional[Plugin]:
        """Get plugin by name.

        Args:
            plugin_name: Name of plugin.

        Returns:
            Plugin object or None.
        """
        return self._plugins.get(plugin_name)

    def get_all_plugins(self, state_filter: Optional[PluginState] = None) -> list[Plugin]:
        """Get all plugins, optionally filtered by state.

        Args:
            state_filter: Filter by plugin state.

        Returns:
            List of plugins.
        """
        plugins = list(self._plugins.values())
        if state_filter:
            plugins = [p for p in plugins if p.state == state_filter]
        return plugins

    def register_hook(self, hook_name: str, callback: Callable) -> None:
        """Register a lifecycle hook callback.

        Args:
            hook_name: Name of the hook.
            callback: Callback function.
        """
        if hook_name in self._lifecycle_hooks:
            self._lifecycle_hooks[hook_name].append(callback)

    def _run_hooks(self, hook_name: str, plugin_name: str) -> None:
        """Run all registered hooks for an event.

        Args:
            hook_name: Name of the hook.
            plugin_name: Plugin being operated on.
        """
        for callback in self._lifecycle_hooks.get(hook_name, []):
            try:
                callback(plugin_name)
            except Exception as e:
                logger.error(f"Hook error in {hook_name}: {e}")

    def resolve_dependencies(self) -> list[str]:
        """Resolve plugin load order based on dependencies.

        Returns:
            List of plugin names in load order.
        """
        resolved: list[str] = []
        seen: set[str] = set()

        def visit(name: str) -> None:
            if name in seen:
                return
            seen.add(name)
            plugin = self._plugins.get(name)
            if plugin:
                for dep in plugin.metadata.dependencies:
                    visit(dep)
                resolved.append(name)

        for name in self._plugins:
            visit(name)

        self._load_order = resolved
        return resolved

    def load_all(self) -> dict[str, bool]:
        """Load all discovered plugins in dependency order.

        Returns:
            Dictionary mapping plugin names to load success.
        """
        results = {}
        order = self.resolve_dependencies()
        for name in order:
            results[name] = self.load(name)
        return results
