"""
Plugin Manager for RabAI AutoClick.

Provides plugin discovery, loading, hot reload, and sandboxed execution.
"""

import os
import sys
import json
import time
import importlib
import importlib.util
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Any, Type, Callable
from dataclasses import dataclass, field
from datetime import datetime
import threading
import hashlib
import tempfile
import shutil

# Try to import restricted modules for sandboxing
try:
    import resource
    HAS_RESOURCE = True
except ImportError:
    HAS_RESOURCE = False


@dataclass
class PluginMetadata:
    """Plugin metadata from plugin.json manifest."""
    name: str
    version: str
    description: str = ""
    author: str = ""
    license: str = ""
    dependencies: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    entry_point: str = ""
    min_rabai_version: str = ""
    file_path: str = ""


@dataclass
class PluginInfo:
    """Information about a loaded plugin."""
    metadata: PluginMetadata
    instance: Optional['BasePlugin'] = None
    module: Any = None
    is_loaded: bool = False
    is_enabled: bool = True
    load_time: Optional[datetime] = None
    last_reload: Optional[datetime] = None
    load_error: Optional[str] = None
    file_hash: str = ""


class BasePlugin:
    """Base class for all RabAI AutoClick plugins."""
    
    name: str = "base_plugin"
    version: str = "1.0.0"
    
    def __init__(self):
        self._enabled = True
        self._context: Dict[str, Any] = {}
    
    def on_load(self) -> bool:
        """Called when plugin is loaded. Return True on success."""
        return True
    
    def on_unload(self) -> bool:
        """Called when plugin is unloaded. Return True on success."""
        return True
    
    def execute(self, *args, **kwargs) -> Any:
        """Execute the plugin's main functionality."""
        raise NotImplementedError("Plugins must implement execute()")
    
    def enable(self) -> None:
        """Enable the plugin."""
        self._enabled = True
    
    def disable(self) -> None:
        """Disable the plugin."""
        self._enabled = False
    
    @property
    def is_enabled(self) -> bool:
        return self._enabled


class SandboxedEnvironment:
    """Sandboxed execution environment for plugins."""
    
    def __init__(self, plugin_path: str, restrictions: Optional[Dict] = None):
        self.plugin_path = Path(plugin_path)
        self.restrictions = restrictions or {}
        self._allowed_modules = self._init_allowed_modules()
        self._restricted_globals: Dict[str, Any] = {}
        self._setup_restricted_globals()
    
    def _init_allowed_modules(self) -> List[str]:
        """Initialize list of allowed modules for sandbox."""
        default_allowed = [
            'json', 'os', 'sys', 'time', 'datetime', 'pathlib',
            'typing', 'collections', 'itertools', 'functools',
            'random', 'math', 're', 'traceback'
        ]
        return default_allowed
    
    def _setup_restricted_globals(self) -> None:
        """Setup restricted global imports for sandbox."""
        safe_modules = {}
        for mod_name in self._allowed_modules:
            try:
                safe_modules[mod_name] = importlib.import_module(mod_name)
            except ImportError:
                pass
        
        # Create restricted __builtins__
        self._restricted_globals = {
            '__builtins__': safe_modules,
            '_sand boxed': True,
            'plugin_path': str(self.plugin_path),
        }
    
    def create_execution_context(self) -> Dict[str, Any]:
        """Create a sandboxed execution context."""
        context = self._restricted_globals.copy()
        context['__name__'] = f'sandbox_{self.plugin_path.name}'
        context['__doc__'] = None
        return context
    
    def set_memory_limit(self, limit_bytes: int) -> bool:
        """Set memory limit for plugin execution (Unix only)."""
        if not HAS_RESOURCE:
            return False
        try:
            resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
            return True
        except (ValueError, OSError):
            return False
    
    def set_cpu_limit(self, seconds: int) -> bool:
        """Set CPU time limit for plugin execution (Unix only)."""
        if not HAS_RESOURCE:
            return False
        try:
            resource.setrlimit(resource.RLIMIT_CPU, (seconds, seconds + 1))
            return True
        except (ValueError, OSError):
            return False


class PluginManager:
    """
    Manages plugin discovery, loading, unloading, and hot reload.
    
    Usage:
        pm = PluginManager('/path/to/project')
        pm.discover_plugins()
        pm.load_all()
        plugin = pm.get_plugin('my_plugin')
        plugin.execute()
    """
    
    def __init__(self, project_root: Optional[str] = None, plugins_dir: str = "plugins"):
        if project_root is None:
            project_root = Path(__file__).parent.parent.absolute()
        self.project_root = Path(project_root)
        self.plugins_dir = self.project_root / plugins_dir
        self._plugins: Dict[str, PluginInfo] = {}
        self._watchers: Dict[str, float] = {}
        self._hot_reload_enabled = False
        self._hot_reload_thread: Optional[threading.Thread] = None
        self._sandbox_enabled = True
        self._sandbox_restrictions = {
            'max_memory_mb': 256,
            'max_cpu_seconds': 30,
        }
        self._on_load_callbacks: List[Callable] = []
        self._on_unload_callbacks: List[Callable] = []
        self._on_reload_callbacks: List[Callable] = []
    
    def discover_plugins(self) -> List[PluginMetadata]:
        """Discover all plugins in the plugins directory."""
        discovered = []
        
        if not self.plugins_dir.exists():
            self.plugins_dir.mkdir(parents=True, exist_ok=True)
            return discovered
        
        for item in self.plugins_dir.iterdir():
            if not item.is_dir():
                continue
            if item.name.startswith('_') or item.name.startswith('.'):
                continue
            
            manifest_path = item / 'plugin.json'
            if not manifest_path.exists():
                continue
            
            try:
                metadata = self._load_manifest(manifest_path)
                metadata.file_path = str(item.absolute())
                discovered.append(metadata)
            except Exception as e:
                print(f"Warning: Failed to load manifest for {item.name}: {e}")
        
        return discovered
    
    def _load_manifest(self, manifest_path: Path) -> PluginMetadata:
        """Load plugin metadata from plugin.json."""
        with open(manifest_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return PluginMetadata(
            name=data.get('name', manifest_path.parent.name),
            version=data.get('version', '1.0.0'),
            description=data.get('description', ''),
            author=data.get('author', ''),
            license=data.get('license', ''),
            dependencies=data.get('dependencies', []),
            tags=data.get('tags', []),
            entry_point=data.get('entry_point', ''),
            min_rabai_version=data.get('min_rabai_version', ''),
        )
    
    def _compute_file_hash(self, plugin_path: Path) -> str:
        """Compute hash of plugin files for change detection."""
        hash_md5 = hashlib.md5()
        for py_file in plugin_path.rglob('*.py'):
            with open(py_file, 'rb') as f:
                hash_md5.update(f.read())
        return hash_md5.hexdigest()
    
    def load_plugin(self, plugin_name: str, use_sandbox: bool = True) -> bool:
        """Load a plugin by name."""
        plugin_path = self.plugins_dir / plugin_name
        if not plugin_path.exists():
            raise FileNotFoundError(f"Plugin not found: {plugin_name}")
        
        manifest_path = plugin_path / 'plugin.json'
        if not manifest_path.exists():
            raise ValueError(f"Plugin {plugin_name} has no plugin.json")
        
        metadata = self._load_manifest(manifest_path)
        metadata.file_path = str(plugin_path.absolute())
        
        plugin_info = PluginInfo(
            metadata=metadata,
            file_hash=self._compute_file_hash(plugin_path),
            load_time=datetime.now(),
        )
        
        try:
            module = self._import_plugin_module(plugin_path, metadata.entry_point)
            
            if module is None:
                raise ImportError(f"Could not import plugin module from {plugin_path}")
            
            plugin_class = self._find_plugin_class(module)
            if plugin_class is None:
                raise ValueError(f"No BasePlugin subclass found in {plugin_name}")
            
            if use_sandbox and self._sandbox_enabled:
                sandbox = SandboxedEnvironment(str(plugin_path), self._sandbox_restrictions)
                # Apply resource limits
                sandbox.set_memory_limit(self._sandbox_restrictions['max_memory_mb'] * 1024 * 1024)
                sandbox.set_cpu_limit(self._sandbox_restrictions['max_cpu_seconds'])
            
            instance = plugin_class()
            
            # Initialize plugin context
            instance._context = {
                'plugin_dir': str(plugin_path),
                'project_root': str(self.project_root),
                'metadata': metadata,
            }
            
            # Call on_load hook
            if not instance.on_load():
                raise RuntimeError(f"Plugin {plugin_name} on_load() returned False")
            
            plugin_info.instance = instance
            plugin_info.module = module
            plugin_info.is_loaded = True
            plugin_info.load_error = None
            
        except Exception as e:
            plugin_info.load_error = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
            plugin_info.is_loaded = False
        
        self._plugins[plugin_name] = plugin_info
        return plugin_info.is_loaded
    
    def _import_plugin_module(self, plugin_path: Path, entry_point: str) -> Any:
        """Import plugin module from path."""
        if entry_point:
            module_name = entry_point
        else:
            module_name = f"rabai_plugins.{plugin_path.name}"
        
        # Create a unique module name to avoid conflicts
        unique_name = f"rabai_plugin_{plugin_path.name}_{id(plugin_path)}"
        
        spec = importlib.util.spec_from_file_location(
            unique_name,
            plugin_path / '__init__.py' if (plugin_path / '__init__.py').exists() else plugin_path / f"{plugin_path.name}.py"
        )
        
        if spec is None or spec.loader is None:
            return None
        
        module = importlib.util.module_from_spec(spec)
        sys.modules[unique_name] = module
        
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(unique_name, None)
            raise
        
        return module
    
    def _find_plugin_class(self, module: Any) -> Optional[Type[BasePlugin]]:
        """Find a BasePlugin subclass in the module."""
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (isinstance(attr, type) and 
                issubclass(attr, BasePlugin) and 
                attr is not BasePlugin):
                return attr
        return None
    
    def unload_plugin(self, plugin_name: str) -> bool:
        """Unload a plugin by name."""
        if plugin_name not in self._plugins:
            return False
        
        plugin_info = self._plugins[plugin_name]
        
        if plugin_info.instance:
            try:
                if not plugin_info.instance.on_unload():
                    return False
            except Exception as e:
                print(f"Warning: Plugin {plugin_name} on_unload() failed: {e}")
        
        # Remove module from sys.modules
        if plugin_info.module:
            module_name = plugin_info.module.__name__
            if module_name in sys.modules:
                del sys.modules[module_name]
        
        plugin_info.is_loaded = False
        plugin_info.instance = None
        
        for callback in self._on_unload_callbacks:
            try:
                callback(plugin_name)
            except Exception as e:
                print(f"Warning: unload callback failed: {e}")
        
        return True
    
    def reload_plugin(self, plugin_name: str) -> bool:
        """Hot reload a plugin."""
        if plugin_name not in self._plugins:
            return False
        
        plugin_info = self._plugins[plugin_name]
        
        # Check if files have changed
        new_hash = self._compute_file_hash(Path(plugin_info.metadata.file_path))
        if new_hash == plugin_info.file_hash:
            return True  # No changes
        
        # Temporarily store old info
        old_info = plugin_info
        
        # Unload
        self.unload_plugin(plugin_name)
        
        # Reload
        success = self.load_plugin(plugin_name)
        
        if success:
            self._plugins[plugin_name].last_reload = datetime.now()
            self._plugins[plugin_name].file_hash = new_hash
            
            for callback in self._on_reload_callbacks:
                try:
                    callback(plugin_name)
                except Exception as e:
                    print(f"Warning: reload callback failed: {e}")
        
        return success
    
    def load_all(self, use_sandbox: bool = True) -> Dict[str, bool]:
        """Load all discovered plugins."""
        results = {}
        for metadata in self.discover_plugins():
            results[metadata.name] = self.load_plugin(metadata.name, use_sandbox)
        return results
    
    def unload_all(self) -> None:
        """Unload all plugins."""
        for plugin_name in list(self._plugins.keys()):
            self.unload_plugin(plugin_name)
    
    def get_plugin(self, plugin_name: str) -> Optional[BasePlugin]:
        """Get a loaded plugin instance by name."""
        plugin_info = self._plugins.get(plugin_name)
        if plugin_info and plugin_info.is_loaded:
            return plugin_info.instance
        return None
    
    def get_plugin_info(self, plugin_name: str) -> Optional[PluginInfo]:
        """Get plugin info including metadata."""
        return self._plugins.get(plugin_name)
    
    def list_plugins(self) -> List[PluginMetadata]:
        """List all discovered plugins."""
        return [info.metadata for info in self._plugins.values()]
    
    def list_loaded_plugins(self) -> List[str]:
        """List names of loaded plugins."""
        return [name for name, info in self._plugins.items() if info.is_loaded]
    
    def enable_hot_reload(self, interval: float = 2.0) -> None:
        """Enable hot reload with specified check interval (seconds)."""
        if self._hot_reload_enabled:
            return
        
        self._hot_reload_enabled = True
        
        def _hot_reload_loop():
            while self._hot_reload_enabled:
                time.sleep(interval)
                for plugin_name, plugin_info in list(self._plugins.items()):
                    if not plugin_info.is_enabled:
                        continue
                    plugin_path = Path(plugin_info.metadata.file_path)
                    if not plugin_path.exists():
                        continue
                    
                    new_hash = self._compute_file_hash(plugin_path)
                    if new_hash != plugin_info.file_hash:
                        print(f"Hot reload triggered for {plugin_name}")
                        self.reload_plugin(plugin_name)
        
        self._hot_reload_thread = threading.Thread(target=_hot_reload_loop, daemon=True)
        self._hot_reload_thread.start()
    
    def disable_hot_reload(self) -> None:
        """Disable hot reload."""
        self._hot_reload_enabled = False
        if self._hot_reload_thread:
            self._hot_reload_thread.join(timeout=5)
            self._hot_reload_thread = None
    
    def on_load(self, callback: Callable) -> None:
        """Register a callback for plugin load events."""
        self._on_load_callbacks.append(callback)
    
    def on_unload(self, callback: Callable) -> None:
        """Register a callback for plugin unload events."""
        self._on_unload_callbacks.append(callback)
    
    def on_reload(self, callback: Callable) -> None:
        """Register a callback for plugin reload events."""
        self._on_reload_callbacks.append(callback)
    
    @property
    def plugins(self) -> Dict[str, PluginInfo]:
        """Get all plugin info."""
        return self._plugins
    
    def validate_plugin(self, plugin_path: Path) -> tuple[bool, Optional[str]]:
        """Validate a plugin directory structure."""
        if not plugin_path.exists():
            return False, f"Plugin path does not exist: {plugin_path}"
        
        if not plugin_path.is_dir():
            return False, f"Plugin path is not a directory: {plugin_path}"
        
        manifest_path = plugin_path / 'plugin.json'
        if not manifest_path.exists():
            return False, f"Missing plugin.json in {plugin_path}"
        
        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)
            
            required_fields = ['name', 'version']
            for field in required_fields:
                if field not in manifest:
                    return False, f"Missing required field '{field}' in plugin.json"
            
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON in plugin.json: {e}"
        except Exception as e:
            return False, f"Error reading plugin.json: {e}"
        
        return True, None
