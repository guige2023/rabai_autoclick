"""
Loader Action Module.

Provides dynamic module and class loading capabilities
with caching and error handling.
"""

import importlib
import importlib.util
import sys
import os
import threading
from typing import Any, Callable, Dict, Optional, Type, Union
from dataclasses import dataclass, field
from enum import Enum
import json


class LoaderType(Enum):
    """Types of dynamic loaders."""
    MODULE = "module"
    CLASS = "class"
    FUNCTION = "function"
    PLUGIN = "plugin"


@dataclass
class LoaderConfig:
    """Configuration for dynamic loading."""
    cache_loaded: bool = True
    reload_on_change: bool = False
    search_paths: list = field(default_factory=list)
    module_attrs: bool = True


@dataclass
class LoadedItem:
    """Represents a loaded item."""
    name: str
    item_type: LoaderType
    item: Any
    loaded_at: float
    source: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class LoaderCache:
    """Cache for loaded modules and objects."""

    def __init__(self):
        self._cache: Dict[str, LoadedItem] = {}
        self._lock = threading.RLock()
        self._file_mtimes: Dict[str, float] = {}

    def get(self, key: str) -> Optional[LoadedItem]:
        """Get item from cache."""
        with self._lock:
            return self._cache.get(key)

    def set(self, key: str, item: LoadedItem) -> None:
        """Set item in cache."""
        with self._lock:
            self._cache[key] = item

    def invalidate(self, key: str) -> bool:
        """Remove item from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        """Clear all cache."""
        with self._lock:
            self._cache.clear()
            self._file_mtimes.clear()


class LoaderAction:
    """
    Action for dynamic loading of modules and classes.

    Example:
        loader = LoaderAction()
        MyClass = loader.load_class("my_module", "MyClass")
        instance = MyClass()
    """

    def __init__(
        self,
        name: str = "loader",
        config: Optional[LoaderConfig] = None,
    ):
        self.name = name
        self.config = config or LoaderConfig()
        self._cache = LoaderCache()
        self._lock = threading.RLock()

    def _make_cache_key(
        self,
        module_path: str,
        item_name: str,
        item_type: LoaderType,
    ) -> str:
        """Generate cache key."""
        return f"{item_type.value}:{module_path}:{item_name}"

    def load_module(self, module_path: str) -> Any:
        """Dynamically load a module."""
        cache_key = self._make_cache_key(
            module_path, "", LoaderType.MODULE
        )

        if self.config.cache_loaded:
            cached = self._cache.get(cache_key)
            if cached:
                return cached.item

        try:
            module = importlib.import_module(module_path)

            if self.config.cache_loaded:
                self._cache.set(
                    cache_key,
                    LoadedItem(
                        name=module_path,
                        item_type=LoaderType.MODULE,
                        item=module,
                        loaded_at=0,
                        source=module_path,
                    ),
                )

            return module

        except ImportError as e:
            raise ImportError(f"Failed to load module {module_path}: {e}")

    def load_class(
        self,
        module_path: str,
        class_name: str,
        *args,
        **kwargs,
    ) -> Type:
        """Dynamically load a class from a module."""
        cache_key = self._make_cache_key(
            module_path, class_name, LoaderType.CLASS
        )

        if self.config.cache_loaded:
            cached = self._cache.get(cache_key)
            if cached:
                return cached.item

        module = self.load_module(module_path)

        if not hasattr(module, class_name):
            raise AttributeError(
                f"Module {module_path} has no class {class_name}"
            )

        cls = getattr(module, class_name)

        if not isinstance(cls, type):
            raise TypeError(f"{class_name} is not a class")

        if self.config.cache_loaded:
            self._cache.set(
                cache_key,
                LoadedItem(
                    name=class_name,
                    item_type=LoaderType.CLASS,
                    item=cls,
                    loaded_at=0,
                    source=module_path,
                ),
            )

        return cls

    def load_function(
        self,
        module_path: str,
        function_name: str,
    ) -> Callable:
        """Dynamically load a function from a module."""
        cache_key = self._make_cache_key(
            module_path, function_name, LoaderType.FUNCTION
        )

        if self.config.cache_loaded:
            cached = self._cache.get(cache_key)
            if cached:
                return cached.item

        module = self.load_module(module_path)

        if not hasattr(module, function_name):
            raise AttributeError(
                f"Module {module_path} has no function {function_name}"
            )

        func = getattr(module, function_name)

        if not callable(func):
            raise TypeError(f"{function_name} is not callable")

        if self.config.cache_loaded:
            self._cache.set(
                cache_key,
                LoadedItem(
                    name=function_name,
                    item_type=LoaderType.FUNCTION,
                    item=func,
                    loaded_at=0,
                    source=module_path,
                ),
            )

        return func

    def load_from_file(
        self,
        file_path: str,
        item_name: str,
        item_type: LoaderType = LoaderType.CLASS,
    ) -> Any:
        """Load from a specific Python file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        module_name = os.path.splitext(os.path.basename(file_path))[0]

        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load spec from {file_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        if item_type == LoaderType.MODULE:
            return module

        if not hasattr(module, item_name):
            raise AttributeError(
                f"Module {module_name} has no attribute {item_name}"
            )

        item = getattr(module, item_name)

        if self.config.cache_loaded:
            cache_key = self._make_cache_key(file_path, item_name, item_type)
            self._cache.set(
                cache_key,
                LoadedItem(
                    name=item_name,
                    item_type=item_type,
                    item=item,
                    loaded_at=0,
                    source=file_path,
                ),
            )

        return item

    def load_plugin(
        self,
        plugin_path: str,
        plugin_config: Optional[Dict] = None,
    ) -> Any:
        """Load a plugin with configuration."""
        config = plugin_config or {}

        if os.path.isfile(plugin_path):
            return self.load_from_file(
                plugin_path,
                config.get("class_name", "Plugin"),
                LoaderType.PLUGIN,
            )
        else:
            return self.load_class(
                plugin_path,
                config.get("class_name", "Plugin"),
            )

    def create_instance(
        self,
        module_path: str,
        class_name: str,
        *args,
        **kwargs,
    ) -> Any:
        """Load a class and create an instance."""
        cls = self.load_class(module_path, class_name)
        return cls(*args, **kwargs)

    def has_item(
        self,
        module_path: str,
        item_name: str,
        item_type: LoaderType,
    ) -> bool:
        """Check if an item exists without loading it."""
        cache_key = self._make_cache_key(module_path, item_name, item_type)
        return self._cache.get(cache_key) is not None

    def reload(self, module_path: str, item_name: str = "") -> Any:
        """Reload a module or item."""
        cache_key = self._make_cache_key(module_path, item_name, LoaderType.CLASS)

        with self._lock:
            self._cache.invalidate(cache_key)

            if module_path in sys.modules:
                importlib.reload(sys.modules[module_path])

            if item_name:
                return self.load_class(module_path, item_name)
            return self.load_module(module_path)

    def unload(self, module_path: str) -> bool:
        """Unload a module from sys.modules."""
        with self._lock:
            if module_path in sys.modules:
                del sys.modules[module_path]
                return True
            return False

    def get_loaded_items(self) -> Dict[str, LoadedItem]:
        """Get all cached items."""
        return dict(self._cache._cache)

    def clear_cache(self) -> None:
        """Clear the loader cache."""
        self._cache.clear()
