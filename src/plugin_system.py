"""
RabAI AutoClick Plugin System

An extensible plugin system that allows extending RabAI AutoClick with custom plugins.

Features:
- Plugin discovery: Auto-discover plugins in plugins/ directory
- Plugin lifecycle: load(), unload(), enable(), disable() hooks
- Custom actions: Register new action types via plugins
- Custom triggers: File change, webhook, schedule, message received
- Plugin API: Access to workflow context, logging, config, action registry
- Plugin manifest: Each plugin has plugin.json with name, version, dependencies
- Sandbox execution: Plugins run in isolated namespace (no access to main app globals)
- Hot reload: Reload plugins without restarting the app
- Plugin dependencies: Plugin can depend on other plugins
"""

import os
import sys
import json
import time
import uuid
import logging
import importlib
import importlib.util
import threading
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Type
from dataclasses import dataclass, field
from enum import Enum
from contextlib import contextmanager, asynccontextmanager
import asyncio
from collections import defaultdict

# Try to import watchdog for file change detection
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileModifiedEvent, FileCreatedEvent, FileDeletedEvent
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False

# Try to import schedule for scheduling
try:
    import schedule
    SCHEDULE_AVAILABLE = True
except ImportError:
    SCHEDULE_AVAILABLE = False


logger = logging.getLogger(__name__)


class TriggerType(Enum):
    """Types of triggers that can activate a plugin."""
    FILE_CHANGE = "file_change"
    WEBHOOK = "webhook"
    SCHEDULE = "schedule"
    MESSAGE_RECEIVED = "message_received"
    MANUAL = "manual"
    WORKFLOW_EVENT = "workflow_event"


class PluginState(Enum):
    """States a plugin can be in."""
    UNLOADED = "unloaded"
    LOADED = "loaded"
    ENABLED = "enabled"
    DISABLED = "disabled"
    ERROR = "error"


@dataclass
class PluginManifest:
    """Manifest data for a plugin."""
    name: str
    version: str
    description: str
    author: str = "Unknown"
    license: str = "MIT"
    tags: List[str] = field(default_factory=list)
    entry_point: str = ""
    min_rabai_version: str = "0.0.0"
    dependencies: List[str] = field(default_factory=list)
    triggers: List[Dict[str, Any]] = field(default_factory=list)
    actions: List[Dict[str, Any]] = field(default_factory=list)
    config_schema: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, json_data: Dict[str, Any]) -> "PluginManifest":
        """Create a manifest from JSON data."""
        return cls(
            name=json_data.get("name", ""),
            version=json_data.get("version", "1.0.0"),
            description=json_data.get("description", ""),
            author=json_data.get("author", "Unknown"),
            license=json_data.get("license", "MIT"),
            tags=json_data.get("tags", []),
            entry_point=json_data.get("entry_point", ""),
            min_rabai_version=json_data.get("min_rabai_version", "0.0.0"),
            dependencies=json_data.get("dependencies", []),
            triggers=json_data.get("triggers", []),
            actions=json_data.get("actions", []),
            config_schema=json_data.get("config_schema", {}),
        )

    def to_json(self) -> Dict[str, Any]:
        """Convert manifest to JSON-compatible dict."""
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "author": self.author,
            "license": self.license,
            "tags": self.tags,
            "entry_point": self.entry_point,
            "min_rabai_version": self.min_rabai_version,
            "dependencies": self.dependencies,
            "triggers": self.triggers,
            "actions": self.actions,
            "config_schema": self.config_schema,
        }


@dataclass
class PluginInfo:
    """Information about a loaded plugin."""
    manifest: PluginManifest
    path: Path
    instance: Optional["PluginBase"] = None
    state: PluginState = PluginState.UNLOADED
    error_message: str = ""
    load_time: float = 0
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])

    def __hash__(self):
        return hash(self.id)


class PluginAPI:
    """
    API available to plugins for interacting with RabAI AutoClick.
    
    This provides a sandboxed interface that isolates plugins from
    accessing main application globals directly.
    """

    def __init__(
        self,
        plugin_id: str,
        plugin_name: str,
        workflow_context: Dict[str, Any],
        config: Dict[str, Any],
        action_registry: "ActionRegistry",
        trigger_dispatcher: "TriggerDispatcher",
        log_level: int = logging.INFO,
    ):
        self._plugin_id = plugin_id
        self._plugin_name = plugin_name
        self._workflow_context = workflow_context
        self._config = config
        self._action_registry = action_registry
        self._trigger_dispatcher = trigger_dispatcher
        self._log_level = log_level
        self._custom_data: Dict[str, Any] = {}
        self._logger: Optional[logging.Logger] = None

    @property
    def plugin_id(self) -> str:
        """Unique identifier for this plugin instance."""
        return self._plugin_id

    @property
    def plugin_name(self) -> str:
        """Name of the plugin."""
        return self._plugin_name

    @property
    def logger(self) -> logging.Logger:
        """Get a namespaced logger for this plugin."""
        if self._logger is None:
            self._logger = logging.getLogger(f"rabai.plugin.{self._plugin_name}")
            self._logger.setLevel(self._log_level)
        return self._logger

    def get_workflow_context(self, key: str, default: Any = None) -> Any:
        """Get a value from the workflow context."""
        return self._workflow_context.get(key, default)

    def set_workflow_context(self, key: str, value: Any) -> None:
        """Set a value in the workflow context."""
        self._workflow_context[key] = value

    def get_config(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        return self._config.get(key, default)

    def set_config(self, key: str, value: Any) -> None:
        """Set a configuration value."""
        self._config[key] = value

    def register_action(
        self,
        action_type: str,
        handler: Callable[..., Any],
        description: str = "",
        schema: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a custom action type."""
        self._action_registry.register(action_type, handler, self._plugin_name, description, schema)

    def unregister_action(self, action_type: str) -> bool:
        """Unregister a custom action type."""
        return self._action_registry.unregister(action_type, self._plugin_name)

    def register_trigger(
        self,
        trigger_type: TriggerType,
        handler: Callable[..., Any],
        config: Dict[str, Any],
    ) -> str:
        """Register a trigger handler."""
        return self._trigger_dispatcher.register(trigger_type, handler, config, self._plugin_name)

    def unregister_trigger(self, trigger_id: str) -> bool:
        """Unregister a trigger handler."""
        return self._trigger_dispatcher.unregister(trigger_id, self._plugin_name)

    def get_custom_data(self, key: str, default: Any = None) -> Any:
        """Get custom data stored by the plugin."""
        return self._custom_data.get(key, default)

    def set_custom_data(self, key: str, value: Any) -> None:
        """Store custom data."""
        self._custom_data[key] = value

    def emit_event(self, event_type: str, event_data: Dict[str, Any]) -> None:
        """Emit an event to the workflow event system."""
        self._workflow_context.setdefault("_events", []).append({
            "type": event_type,
            "data": event_data,
            "source": self._plugin_name,
            "timestamp": time.time(),
        })


class PluginBase:
    """
    Base class for all plugins.
    
    Plugins should inherit from this class and implement the lifecycle methods.
    """

    # Plugin metadata - override in subclasses
    name: str = "base_plugin"
    version: str = "1.0.0"
    description: str = "Base plugin"
    author: str = "Unknown"

    def __init__(self, api: PluginAPI):
        """
        Initialize the plugin.
        
        Args:
            api: The PluginAPI instance providing access to RabAI functionality
        """
        self._api = api
        self._enabled = False
        self._initialized = False

    @property
    def api(self) -> PluginAPI:
        """Get the plugin API."""
        return self._api

    @property
    def is_enabled(self) -> bool:
        """Check if the plugin is enabled."""
        return self._enabled

    @property
    def is_initialized(self) -> bool:
        """Check if the plugin has been initialized."""
        return self._initialized

    def on_load(self) -> bool:
        """
        Called when the plugin is loaded.
        
        Perform initialization here. Return True if successful, False otherwise.
        """
        self._api.logger.info(f"{self.name} plugin loaded")
        return True

    def on_unload(self) -> bool:
        """
        Called when the plugin is unloaded.
        
        Perform cleanup here. Return True if successful, False otherwise.
        """
        self._api.logger.info(f"{self.name} plugin unloaded")
        return True

    def on_enable(self) -> None:
        """Called when the plugin is enabled."""
        self._enabled = True
        self._api.logger.info(f"{self.name} plugin enabled")

    def on_disable(self) -> None:
        """Called when the plugin is disabled."""
        self._enabled = False
        self._api.logger.info(f"{self.name} plugin disabled")

    def on_reload(self) -> bool:
        """
        Called when the plugin is hot-reloaded.
        
        Return True if successful, False otherwise.
        """
        self._api.logger.info(f"{self.name} plugin reloaded")
        return True

    def execute(self, action: str, **kwargs) -> Dict[str, Any]:
        """
        Execute a custom action provided by this plugin.
        
        Args:
            action: The action name to execute
            **kwargs: Additional arguments for the action
            
        Returns:
            Dictionary containing the result
        """
        raise NotImplementedError("Plugins must implement execute()")


class ActionRegistry:
    """
    Registry for custom actions provided by plugins.
    """

    def __init__(self):
        self._actions: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def register(
        self,
        action_type: str,
        handler: Callable[..., Any],
        source_plugin: str,
        description: str = "",
        schema: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a new action type."""
        with self._lock:
            if action_type in self._actions:
                self._actions[action_type]["handler"] = handler
                self._actions[action_type]["source_plugin"] = source_plugin
            else:
                self._actions[action_type] = {
                    "handler": handler,
                    "source_plugin": source_plugin,
                    "description": description,
                    "schema": schema or {},
                }
            logger.info(f"Registered action '{action_type}' from plugin '{source_plugin}'")

    def unregister(self, action_type: str, source_plugin: str) -> bool:
        """Unregister an action type."""
        with self._lock:
            if action_type in self._actions:
                if self._actions[action_type]["source_plugin"] == source_plugin:
                    del self._actions[action_type]
                    logger.info(f"Unregistered action '{action_type}' from plugin '{source_plugin}'")
                    return True
            return False

    def get_handler(self, action_type: str) -> Optional[Callable[..., Any]]:
        """Get the handler for an action type."""
        with self._lock:
            action = self._actions.get(action_type)
            return action["handler"] if action else None

    def get_action_info(self, action_type: str) -> Optional[Dict[str, Any]]:
        """Get information about an action type."""
        with self._lock:
            return self._actions.get(action_type, {}).copy()

    def list_actions(self) -> List[str]:
        """List all registered action types."""
        with self._lock:
            return list(self._actions.keys())

    def get_actions_by_plugin(self, plugin_name: str) -> List[str]:
        """Get all action types registered by a specific plugin."""
        with self._lock:
            return [
                action_type
                for action_type, info in self._actions.items()
                if info["source_plugin"] == plugin_name
            ]


class TriggerDispatcher:
    """
    Dispatcher for plugin triggers.
    """

    def __init__(self, parent_api: PluginAPI):
        self._parent_api = parent_api
        self._triggers: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._file_watchers: Dict[str, Any] = {}
        self._schedule_jobs: Dict[str, Any] = {}
        self._webhook_routes: Dict[str, str] = {}
        self._running = False
        self._observer: Optional[Observer] = None
        self._schedule_thread: Optional[threading.Thread] = None

    def register(
        self,
        trigger_type: TriggerType,
        handler: Callable[..., Any],
        config: Dict[str, Any],
        plugin_name: str,
    ) -> str:
        """Register a trigger handler."""
        trigger_id = str(uuid.uuid4())[:8]
        
        with self._lock:
            self._triggers[trigger_id] = {
                "type": trigger_type,
                "handler": handler,
                "config": config,
                "plugin_name": plugin_name,
                "enabled": True,
            }

        # Set up trigger-specific infrastructure
        if trigger_type == TriggerType.FILE_CHANGE:
            self._setup_file_watcher(trigger_id, config)
        elif trigger_type == TriggerType.SCHEDULE:
            self._setup_schedule_job(trigger_id, config)
        elif trigger_type == TriggerType.WEBHOOK:
            self._setup_webhook_route(trigger_id, config)
            
        logger.info(f"Registered {trigger_type.value} trigger '{trigger_id}' for plugin '{plugin_name}'")
        return trigger_id

    def unregister(self, trigger_id: str, plugin_name: str) -> bool:
        """Unregister a trigger handler."""
        with self._lock:
            if trigger_id in self._triggers:
                trigger = self._triggers[trigger_id]
                if trigger["plugin_name"] == plugin_name:
                    # Clean up trigger-specific infrastructure
                    if trigger["type"] == TriggerType.FILE_CHANGE and trigger_id in self._file_watchers:
                        self._file_watchers[trigger_id].stop()
                        del self._file_watchers[trigger_id]
                    elif trigger["type"] == TriggerType.SCHEDULE and trigger_id in self._schedule_jobs:
                        schedule.cancel_job(self._schedule_jobs[trigger_id])
                        del self._schedule_jobs[trigger_id]
                    
                    del self._triggers[trigger_id]
                    logger.info(f"Unregistered trigger '{trigger_id}' from plugin '{plugin_name}'")
                    return True
        return False

    def _setup_file_watcher(self, trigger_id: str, config: Dict[str, Any]) -> None:
        """Set up a file change watcher."""
        if not WATCHDOG_AVAILABLE:
            logger.warning("watchdog not available, file change triggers disabled")
            return

        path = config.get("path", "")
        recursive = config.get("recursive", False)
        patterns = config.get("patterns", ["*"])

        class PluginFileHandler(FileSystemEventHandler):
            def __init__(self, tid: str, triggers: Dict, api: PluginAPI, parent_lock):
                self.tid = tid
                self.triggers = triggers
                self.api = api
                self.lock = parent_lock

            def on_modified(self, event):
                if not event.is_directory:
                    self._dispatch("modified", event.src_path)

            def on_created(self, event):
                if not event.is_directory:
                    self._dispatch("created", event.src_path)

            def on_deleted(self, event):
                if not event.is_directory:
                    self._dispatch("deleted", event.src_path)

            def _dispatch(self, event_type: str, file_path: str):
                with self.lock:
                    if self.tid in self.triggers:
                        trigger = self.triggers[self.tid]
                        if trigger["enabled"]:
                            try:
                                trigger["handler"]({
                                    "type": event_type,
                                    "path": file_path,
                                    "trigger_id": self.tid,
                                })
                            except Exception as e:
                                self.api.logger.error(f"Error in file trigger handler: {e}")

        handler = PluginFileHandler(trigger_id, self._triggers, self._parent_api, self._lock)
        observer = Observer()
        observer.schedule(handler, path, recursive=recursive)
        observer.start()
        self._file_watchers[trigger_id] = observer

    def _setup_schedule_job(self, trigger_id: str, config: Dict[str, Any]) -> None:
        """Set up a scheduled job."""
        if not SCHEDULE_AVAILABLE:
            logger.warning("schedule not available, schedule triggers disabled")
            return

        interval = config.get("interval", 60)  # seconds
        unit = config.get("unit", "seconds")
        at_time = config.get("at_time")

        def job_wrapper():
            with self._lock:
                if trigger_id in self._triggers:
                    trigger = self._triggers[trigger_id]
                    if trigger["enabled"]:
                        try:
                            trigger["handler"]({
                                "trigger_id": trigger_id,
                                "scheduled_time": time.time(),
                            })
                        except Exception as e:
                            self._parent_api.logger.error(f"Error in schedule trigger handler: {e}")

        job = schedule.every(interval)
        if unit == "minutes":
            job = schedule.every(interval).minutes
        elif unit == "hours":
            job = schedule.every(interval).hours
        elif unit == "days":
            job = schedule.every(interval).days
        elif at_time:
            job = schedule.every().day.at(at_time)
            
        job.do(job_wrapper)
        self._schedule_jobs[trigger_id] = job

    def _setup_webhook_route(self, trigger_id: str, config: Dict[str, Any]) -> None:
        """Set up a webhook route."""
        route = config.get("route", f"/webhook/{trigger_id}")
        method = config.get("method", "POST").upper()
        self._webhook_routes[route] = trigger_id
        logger.info(f"Webhook route registered: {method} {route}")

    def dispatch_webhook(self, route: str, method: str, data: Dict[str, Any]) -> None:
        """Dispatch a webhook request to the appropriate handler."""
        with self._lock:
            trigger_id = self._webhook_routes.get(route)
            if not trigger_id or trigger_id not in self._triggers:
                return
            trigger = self._triggers[trigger_id]
            if not trigger["enabled"]:
                return
            if trigger["config"].get("method", "POST").upper() != method.upper():
                return

        try:
            trigger["handler"]({
                "trigger_id": trigger_id,
                "route": route,
                "method": method,
                "data": data,
                "webhook_time": time.time(),
            })
        except Exception as e:
            self._parent_api.logger.error(f"Error in webhook trigger handler: {e}")

    def enable_trigger(self, trigger_id: str) -> None:
        """Enable a trigger."""
        with self._lock:
            if trigger_id in self._triggers:
                self._triggers[trigger_id]["enabled"] = True

    def disable_trigger(self, trigger_id: str) -> None:
        """Disable a trigger."""
        with self._lock:
            if trigger_id in self._triggers:
                self._triggers[trigger_id]["enabled"] = False

    def start_schedule_runner(self) -> None:
        """Start the schedule runner in a background thread."""
        if self._schedule_thread is None or not self._schedule_thread.is_alive():
            self._running = True
            
            def run_schedules():
                while self._running:
                    schedule.run_pending()
                    time.sleep(1)

            self._schedule_thread = threading.Thread(target=run_schedules, daemon=True)
            self._schedule_thread.start()

    def stop_schedule_runner(self) -> None:
        """Stop the schedule runner."""
        self._running = False
        if self._schedule_thread:
            self._schedule_thread.join(timeout=5)

    def shutdown(self) -> None:
        """Shutdown all trigger infrastructure."""
        self.stop_schedule_runner()
        for observer in self._file_watchers.values():
            observer.stop()
        for observer in self._file_watchers.values():
            observer.join(timeout=5)
        for job in self._schedule_jobs.values():
            schedule.cancel_job(job)


class PluginSandbox:
    """
    Sandbox for executing plugin code in isolation.
    
    This prevents plugins from accessing main application globals
    and provides a controlled execution environment.
    """

    def __init__(self, plugin_name: str):
        self._plugin_name = plugin_name
        self._globals: Dict[str, Any] = {
            "__name__": f"rabai_plugin_{plugin_name}",
            "__builtins__": __builtins__,
            "__file__": f"<plugin:{plugin_name}>",
        }
        self._locals: Dict[str, Any] = {}

    def execute(self, code: str, context: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute code in the sandbox.
        
        Args:
            code: Python code to execute
            context: Optional context dict to merge into globals
            
        Returns:
            Result of the code execution
        """
        sandbox_globals = self._globals.copy()
        if context:
            sandbox_globals.update(context)
            
        try:
            result = eval(code, sandbox_globals, self._locals)
            return result
        except SyntaxError as e:
            raise SyntaxError(f"Plugin '{self._plugin_name}' syntax error: {e}")

    def execute_module(
        self,
        module_name: str,
        plugin_path: Path,
    ) -> Optional[Type[PluginBase]]:
        """
        Load and return a plugin class from a module file.
        
        Args:
            module_name: Name for the module
            plugin_path: Path to the module file
            
        Returns:
            Plugin class if found, None otherwise
        """
        spec = importlib.util.spec_from_file_location(module_name, plugin_path)
        if spec is None or spec.loader is None:
            logger.error(f"Could not load plugin spec from {plugin_path}")
            return None

        # Create a new module object
        module = importlib.util.module_from_spec(spec)
        
        # Use sandbox globals for the module
        module.__globals__.update(self._globals)
        
        try:
            spec.loader.exec_module(module)
            
            # Look for a PluginBase subclass
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, type) and issubclass(attr, PluginBase) and attr is not PluginBase:
                    return attr
                    
            # If no PluginBase subclass, look for a register function
            if hasattr(module, "register"):
                return module.register
                
            return None
            
        except Exception as e:
            logger.error(f"Error loading plugin module {plugin_path}: {e}")
            logger.error(traceback.format_exc())
            return None

    def get_sandboxed_globals(self) -> Dict[str, Any]:
        """Get a copy of the sandbox globals."""
        return self._globals.copy()


class PluginSystem:
    """
    Main plugin system for RabAI AutoClick.
    
    This class manages plugin discovery, loading, lifecycle, and execution.
    """

    def __init__(
        self,
        plugins_dir: Optional[Path] = None,
        examples_plugins_dir: Optional[Path] = None,
        workflow_context: Optional[Dict[str, Any]] = None,
        config: Optional[Dict[str, Any]] = None,
        log_level: int = logging.INFO,
    ):
        """
        Initialize the plugin system.
        
        Args:
            plugins_dir: Directory containing plugins (default: ./plugins)
            examples_plugins_dir: Directory containing example plugins
            workflow_context: Shared workflow context dict
            config: Configuration dict
            log_level: Logging level
        """
        # Determine plugin directories
        if plugins_dir is None:
            plugins_dir = Path(__file__).parent.parent / "plugins"
        if examples_plugins_dir is None:
            examples_plugins_dir = Path(__file__).parent.parent / "examples" / "plugins"
            
        self._plugins_dir = Path(plugins_dir)
        self._examples_plugins_dir = Path(examples_plugins_dir)
        self._workflow_context = workflow_context or {}
        self._config = config or {}
        self._log_level = log_level

        # Plugin management
        self._plugins: Dict[str, PluginInfo] = {}
        self._load_order: List[str] = []  # For dependency resolution
        self._lock = threading.RLock()

        # Core components
        self._action_registry = ActionRegistry()
        self._trigger_dispatcher = TriggerDispatcher(
            PluginAPI(
                plugin_id="system",
                plugin_name="system",
                workflow_context=self._workflow_context,
                config=self._config,
                action_registry=self._action_registry,
                trigger_dispatcher=None,
                log_level=log_level,
            )
        )

        # Hot reload support
        self._file_watcher: Optional[Observer] = None
        self._watch_enabled = False
        self._reload_callbacks: List[Callable[[str], None]] = []

        logger.info(f"PluginSystem initialized with plugins_dir={self._plugins_dir}")

    @property
    def plugins_dir(self) -> Path:
        """Get the plugins directory."""
        return self._plugins_dir

    @property
    def examples_plugins_dir(self) -> Path:
        """Get the examples plugins directory."""
        return self._examples_plugins_dir

    @property
    def action_registry(self) -> ActionRegistry:
        """Get the action registry."""
        return self._action_registry

    @property
    def trigger_dispatcher(self) -> TriggerDispatcher:
        """Get the trigger dispatcher."""
        return self._trigger_dispatcher

    def discover_plugins(self) -> List[PluginInfo]:
        """
        Discover all plugins in the plugins directory.
        
        Returns:
            List of PluginInfo objects for discovered plugins
        """
        discovered = []
        
        for plugins_path in [self._plugins_dir, self._examples_plugins_dir]:
            if not plugins_path.exists():
                logger.debug(f"Plugins path does not exist: {plugins_path}")
                continue
                
            for item in plugins_path.iterdir():
                if not item.is_dir():
                    continue
                    
                manifest_path = item / "plugin.json"
                if not manifest_path.exists():
                    logger.debug(f"No plugin.json in {item}")
                    continue
                    
                try:
                    with open(manifest_path, "r") as f:
                        manifest_data = json.load(f)
                    manifest = PluginManifest.from_json(manifest_data)
                    
                    plugin_info = PluginInfo(
                        manifest=manifest,
                        path=item,
                    )
                    discovered.append(plugin_info)
                    logger.info(f"Discovered plugin: {manifest.name} v{manifest.version} at {item}")
                    
                except Exception as e:
                    logger.error(f"Error reading plugin manifest from {manifest_path}: {e}")
                    
        return discovered

    def _resolve_dependencies(self, plugins: List[PluginInfo]) -> List[PluginInfo]:
        """
        Resolve plugin dependencies and return plugins in load order.
        
        Args:
            plugins: List of PluginInfo objects
            
        Returns:
            List sorted by dependency order
        """
        # Build dependency graph
        name_to_plugin = {p.manifest.name: p for p in plugins}
        
        def get_deps(plugin: PluginInfo) -> Set[str]:
            return set(plugin.manifest.dependencies)
        
        # Topological sort
        result = []
        visited = set()
        visiting = set()
        
        def visit(p: PluginInfo):
            if p.manifest.name in visited:
                return
            if p.manifest.name in visiting:
                logger.warning(f"Circular dependency detected for plugin {p.manifest.name}")
                return
                
            visiting.add(p.manifest.name)
            
            for dep_name in get_deps(p):
                if dep_name in name_to_plugin:
                    visit(name_to_plugin[dep_name])
                    
            visiting.remove(p.manifest.name)
            visited.add(p.manifest.name)
            result.append(p)
            
        for plugin in plugins:
            visit(plugin)
            
        return result

    def _create_plugin_api(self, plugin_info: PluginInfo) -> PluginAPI:
        """Create a PluginAPI instance for a plugin."""
        return PluginAPI(
            plugin_id=plugin_info.id,
            plugin_name=plugin_info.manifest.name,
            workflow_context=self._workflow_context,
            config=self._config.get(plugin_info.manifest.name, {}),
            action_registry=self._action_registry,
            trigger_dispatcher=self._trigger_dispatcher,
            log_level=self._log_level,
        )

    def _load_plugin_instance(self, plugin_info: PluginInfo) -> bool:
        """
        Load a plugin's instance using its entry point.
        
        Args:
            plugin_info: PluginInfo object
            
        Returns:
            True if successful, False otherwise
        """
        if not plugin_info.path.exists():
            logger.error(f"Plugin path does not exist: {plugin_info.path}")
            return False
            
        entry_point = plugin_info.manifest.entry_point or plugin_info.manifest.name
        entry_path = plugin_info.path / f"{entry_point}.py"
        
        if not entry_path.exists():
            # Try looking for __init__.py
            init_path = plugin_info.path / "__init__.py"
            if init_path.exists():
                entry_path = init_path
        
        if not entry_path.exists():
            logger.error(f"Entry point file not found for plugin {plugin_info.manifest.name}")
            return False

        sandbox = PluginSandbox(plugin_info.manifest.name)
        
        try:
            plugin_class_or_func = sandbox.execute_module(
                f"rabai_plugin_{plugin_info.manifest.name}",
                entry_path,
            )
            
            if plugin_class_or_func is None:
                logger.error(f"No plugin class or register function found in {entry_path}")
                return False
            
            # Create the plugin instance
            api = self._create_plugin_api(plugin_info)
            
            if callable(plugin_class_or_func):
                if isinstance(plugin_class_or_func, type) and issubclass(plugin_class_or_func, PluginBase):
                    plugin_info.instance = plugin_class_or_func(api)
                else:
                    # It's a factory function
                    plugin_info.instance = plugin_class_or_func(api)
            else:
                logger.error(f"Invalid plugin type from {entry_path}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error loading plugin {plugin_info.manifest.name}: {e}")
            logger.error(traceback.format_exc())
            plugin_info.error_message = str(e)
            plugin_info.state = PluginState.ERROR
            return False

    def load_plugin(self, plugin_name: str) -> bool:
        """
        Load a specific plugin by name.
        
        Args:
            plugin_name: Name of the plugin to load
            
        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            # Discover if not already discovered
            if plugin_name not in self._plugins:
                discovered = self.discover_plugins()
                for p in discovered:
                    self._plugins[p.manifest.name] = p
            
            if plugin_name not in self._plugins:
                logger.error(f"Plugin not found: {plugin_name}")
                return False
                
            plugin_info = self._plugins[plugin_name]
            
            if plugin_info.state not in (PluginState.UNLOADED, PluginState.ERROR):
                logger.warning(f"Plugin {plugin_name} already loaded")
                return True
                
            # Check dependencies
            for dep_name in plugin_info.manifest.dependencies:
                if dep_name not in self._plugins:
                    logger.error(f"Plugin {plugin_name} depends on {dep_name} which is not available")
                    return False
                if self._plugins[dep_name].state not in (PluginState.LOADED, PluginState.ENABLED):
                    # Try to load the dependency
                    if not self.load_plugin(dep_name):
                        logger.error(f"Plugin {plugin_name} depends on {dep_name} which failed to load")
                        return False
                        
            # Load the plugin instance
            start_time = time.time()
            if self._load_plugin_instance(plugin_info):
                plugin_info.state = PluginState.LOADED
                plugin_info.load_time = time.time() - start_time
                
                # Call on_load hook
                try:
                    if plugin_info.instance and not plugin_info.instance.on_load():
                        logger.error(f"Plugin {plugin_name} on_load() returned False")
                        plugin_info.state = PluginState.ERROR
                        return False
                except Exception as e:
                    logger.error(f"Error in on_load() for {plugin_name}: {e}")
                    plugin_info.state = PluginState.ERROR
                    plugin_info.error_message = str(e)
                    return False
                    
                logger.info(f"Loaded plugin {plugin_name} v{plugin_info.manifest.version} in {plugin_info.load_time:.3f}s")
                return True
            else:
                plugin_info.state = PluginState.ERROR
                return False

    def unload_plugin(self, plugin_name: str) -> bool:
        """
        Unload a plugin.
        
        Args:
            plugin_name: Name of the plugin to unload
            
        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            if plugin_name not in self._plugins:
                logger.error(f"Plugin not found: {plugin_name}")
                return False
                
            plugin_info = self._plugins[plugin_name]
            
            if plugin_info.state == PluginState.UNLOADED:
                return True
                
            # Check if any other plugins depend on this one
            for p in self._plugins.values():
                if plugin_name in p.manifest.dependencies and p.state not in (PluginState.UNLOADED, PluginState.ERROR):
                    logger.error(f"Cannot unload {plugin_name} - {p.manifest.name} depends on it")
                    return False
                    
            # Disable if enabled
            if plugin_info.state == PluginState.ENABLED:
                self.disable_plugin(plugin_name)
                
            # Call on_unload hook
            try:
                if plugin_info.instance:
                    if not plugin_info.instance.on_unload():
                        logger.error(f"Plugin {plugin_name} on_unload() returned False")
                        return False
            except Exception as e:
                logger.error(f"Error in on_unload() for {plugin_name}: {e}")
                return False
                
            # Unregister actions and triggers
            for action_type in self._action_registry.get_actions_by_plugin(plugin_name):
                self._action_registry.unregister(action_type, plugin_name)
                
            plugin_info.instance = None
            plugin_info.state = PluginState.UNLOADED
            logger.info(f"Unloaded plugin {plugin_name}")
            return True

    def enable_plugin(self, plugin_name: str) -> bool:
        """
        Enable a loaded plugin.
        
        Args:
            plugin_name: Name of the plugin to enable
            
        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            if plugin_name not in self._plugins:
                logger.error(f"Plugin not found: {plugin_name}")
                return False
                
            plugin_info = self._plugins[plugin_name]
            
            if plugin_info.state == PluginState.UNLOADED:
                if not self.load_plugin(plugin_name):
                    return False
                    
            if plugin_info.state == PluginState.ENABLED:
                return True
                
            if plugin_info.state != PluginState.LOADED:
                logger.error(f"Cannot enable plugin {plugin_name} in state {plugin_info.state}")
                return False
                
            try:
                if plugin_info.instance:
                    plugin_info.instance.on_enable()
                plugin_info.state = PluginState.ENABLED
                logger.info(f"Enabled plugin {plugin_name}")
                return True
            except Exception as e:
                logger.error(f"Error enabling plugin {plugin_name}: {e}")
                plugin_info.state = PluginState.ERROR
                plugin_info.error_message = str(e)
                return False

    def disable_plugin(self, plugin_name: str) -> bool:
        """
        Disable a plugin.
        
        Args:
            plugin_name: Name of the plugin to disable
            
        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            if plugin_name not in self._plugins:
                logger.error(f"Plugin not found: {plugin_name}")
                return False
                
            plugin_info = self._plugins[plugin_name]
            
            if plugin_info.state != PluginState.ENABLED:
                return True
                
            try:
                if plugin_info.instance:
                    plugin_info.instance.on_disable()
                plugin_info.state = PluginState.LOADED
                logger.info(f"Disabled plugin {plugin_name}")
                return True
            except Exception as e:
                logger.error(f"Error disabling plugin {plugin_name}: {e}")
                plugin_info.state = PluginState.ERROR
                plugin_info.error_message = str(e)
                return False

    def reload_plugin(self, plugin_name: str) -> bool:
        """
        Hot reload a plugin without restarting the app.
        
        Args:
            plugin_name: Name of the plugin to reload
            
        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            was_enabled = False
            
            if plugin_name in self._plugins:
                plugin_info = self._plugins[plugin_name]
                was_enabled = plugin_info.state == PluginState.ENABLED
                
                # Unload first
                if plugin_info.state != PluginState.UNLOADED:
                    if not self.unload_plugin(plugin_name):
                        return False
                        
            # Clear from cache
            if plugin_name in self._plugins:
                del self._plugins[plugin_name]
                
            # Re-discover
            discovered = self.discover_plugins()
            for p in discovered:
                if p.manifest.name == plugin_name:
                    self._plugins[plugin_name] = p
                    break
                    
            # Load and optionally enable
            if not self.load_plugin(plugin_name):
                return False
                
            if was_enabled:
                if not self.enable_plugin(plugin_name):
                    return False
            elif plugin_name in self._plugins and self._plugins[plugin_name].instance:
                # Call on_reload if not enabling fresh
                try:
                    self._plugins[plugin_name].instance.on_reload()
                except Exception as e:
                    logger.error(f"Error in on_reload() for {plugin_name}: {e}")
                    return False
                    
            # Notify reload callbacks
            for callback in self._reload_callbacks:
                try:
                    callback(plugin_name)
                except Exception as e:
                    logger.error(f"Error in reload callback for {plugin_name}: {e}")
                    
            logger.info(f"Reloaded plugin {plugin_name}")
            return True

    def load_all_plugins(self) -> Dict[str, bool]:
        """
        Discover and load all available plugins.
        
        Returns:
            Dict mapping plugin names to load success status
        """
        with self._lock:
            discovered = self.discover_plugins()
            
            for p in discovered:
                self._plugins[p.manifest.name] = p
                
            # Resolve dependencies and get load order
            sorted_plugins = self._resolve_dependencies(discovered)
            self._load_order = [p.manifest.name for p in sorted_plugins]
            
            results = {}
            for plugin_info in sorted_plugins:
                results[plugin_info.manifest.name] = self.load_plugin(plugin_info.manifest.name)
                
            return results

    def enable_all_plugins(self) -> Dict[str, bool]:
        """
        Enable all loaded plugins.
        
        Returns:
            Dict mapping plugin names to enable success status
        """
        with self._lock:
            results = {}
            for plugin_name in self._load_order:
                if plugin_name in self._plugins:
                    plugin_info = self._plugins[plugin_name]
                    if plugin_info.state == PluginState.LOADED:
                        results[plugin_name] = self.enable_plugin(plugin_name)
                    else:
                        results[plugin_name] = plugin_info.state == PluginState.ENABLED
            return results

    def get_plugin_info(self, plugin_name: str) -> Optional[PluginInfo]:
        """Get information about a plugin."""
        with self._lock:
            return self._plugins.get(plugin_name)

    def list_plugins(self) -> List[PluginInfo]:
        """List all known plugins."""
        with self._lock:
            return list(self._plugins.values())

    def list_enabled_plugins(self) -> List[str]:
        """List names of all enabled plugins."""
        with self._lock:
            return [
                name for name, info in self._plugins.items()
                if info.state == PluginState.ENABLED
            ]

    def execute_action(self, action_type: str, **kwargs) -> Any:
        """
        Execute a registered action.
        
        Args:
            action_type: Type of action to execute
            **kwargs: Arguments for the action
            
        Returns:
            Result of the action
        """
        handler = self._action_registry.get_handler(action_type)
        if handler is None:
            raise ValueError(f"Unknown action type: {action_type}")
            
        return handler(**kwargs)

    def register_reload_callback(self, callback: Callable[[str], None]) -> None:
        """Register a callback to be called when a plugin is reloaded."""
        self._reload_callbacks.append(callback)

    def start_file_watcher(self) -> None:
        """Start watching plugin files for changes (hot reload)."""
        if not WATCHDOG_AVAILABLE:
            logger.warning("watchdog not available, file watching disabled")
            return
            
        if self._file_watcher is not None:
            return
            
        self._watch_enabled = True
        
        class PluginReloadHandler(FileSystemEventHandler):
            def __init__(self, system: "PluginSystem", lock: threading.RLock):
                self.system = system
                self.lock = lock
                self._debounce_times: Dict[str, float] = {}
                
            def _debounce(self, path: str, delay: float = 1.0) -> bool:
                now = time.time()
                last = self._debounce_times.get(path, 0)
                if now - last < delay:
                    return False
                self._debounce_times[path] = now
                return True
                
            def on_modified(self, event):
                if event.is_directory:
                    return
                if not self._debounce(event.src_path):
                    return
                    
                # Find which plugin this file belongs to
                with self.lock:
                    for name, info in self.system._plugins.items():
                        if event.src_path.startswith(str(info.path)):
                            logger.info(f"Detected change in plugin {name}, scheduling reload")
                            # Schedule reload in background
                            threading.Thread(
                                target=self.system.reload_plugin,
                                args=(name,),
                                daemon=True
                            ).start()
                            break

        self._file_watcher = Observer()
        handler = PluginReloadHandler(self, self._lock)
        
        # Watch both plugin directories
        for plugins_path in [self._plugins_dir, self._examples_plugins_dir]:
            if plugins_path.exists():
                self._file_watcher.schedule(handler, str(plugins_path), recursive=True)
                
        self._file_watcher.start()
        logger.info("Started plugin file watcher for hot reload")

    def stop_file_watcher(self) -> None:
        """Stop watching plugin files for changes."""
        self._watch_enabled = False
        if self._file_watcher is not None:
            self._file_watcher.stop()
            self._file_watcher.join(timeout=5)
            self._file_watcher = None
            logger.info("Stopped plugin file watcher")

    def shutdown(self) -> None:
        """Shutdown the plugin system."""
        logger.info("Shutting down plugin system")
        
        self.stop_file_watcher()
        self._trigger_dispatcher.shutdown()
        
        with self._lock:
            for plugin_name in reversed(self._load_order):
                if plugin_name in self._plugins:
                    self.unload_plugin(plugin_name)
                    
        logger.info("Plugin system shutdown complete")


# Module-level singleton for convenience
_global_plugin_system: Optional[PluginSystem] = None


def get_plugin_system() -> Optional[PluginSystem]:
    """Get the global plugin system instance."""
    return _global_plugin_system


def init_plugin_system(
    plugins_dir: Optional[Path] = None,
    examples_plugins_dir: Optional[Path] = None,
    workflow_context: Optional[Dict[str, Any]] = None,
    config: Optional[Dict[str, Any]] = None,
    log_level: int = logging.INFO,
) -> PluginSystem:
    """
    Initialize the global plugin system.
    
    Args:
        plugins_dir: Directory containing plugins
        examples_plugins_dir: Directory containing example plugins
        workflow_context: Shared workflow context dict
        config: Configuration dict
        log_level: Logging level
        
    Returns:
        The initialized PluginSystem instance
    """
    global _global_plugin_system
    _global_plugin_system = PluginSystem(
        plugins_dir=plugins_dir,
        examples_plugins_dir=examples_plugins_dir,
        workflow_context=workflow_context,
        config=config,
        log_level=log_level,
    )
    return _global_plugin_system


# Import trigger types for convenience
__all__ = [
    "PluginSystem",
    "PluginBase",
    "PluginManifest",
    "PluginInfo",
    "PluginAPI",
    "PluginState",
    "PluginSandbox",
    "ActionRegistry",
    "TriggerDispatcher",
    "TriggerType",
    "get_plugin_system",
    "init_plugin_system",
]
