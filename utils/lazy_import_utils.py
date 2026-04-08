"""Lazy import utilities.

Provides deferred module importing to speed up startup time
and reduce memory usage for optional dependencies.
"""

import importlib
from typing import Any, Callable, Optional, TypeVar


T = TypeVar("T")


class LazyModule:
    """Lazy module wrapper that defers import until first use.

    Example:
        pd = LazyModule("pandas")
        df = pd.read_csv("data.csv")  # pandas imported here
    """

    def __init__(self, module_name: str) -> None:
        self._module_name = module_name
        self._module: Optional[Any] = None

    def _ensure(self) -> Any:
        if self._module is None:
            self._module = importlib.import_module(self._module_name)
        return self._module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._ensure(), name)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._ensure()(*args, **kwargs)

    def __repr__(self) -> str:
        return f"LazyModule({self._module_name!r})"

    @property
    def loaded(self) -> bool:
        """Check if module has been loaded."""
        return self._module is not None


def lazy_import(module_name: str) -> LazyModule:
    """Create a lazy module wrapper.

    Args:
        module_name: Full module name (e.g., "numpy").

    Returns:
        LazyModule instance.
    """
    return LazyModule(module_name)


class LazyProxy:
    """Generic lazy proxy for any object.

    Example:
        config = LazyProxy(lambda: load_heavy_config())
        value = config.key  # loaded only when accessed
    """

    def __init__(self, loader: Callable[[], T]) -> None:
        self._loader = loader
        self._instance: Optional[T] = None
        self._loaded = False

    def _ensure(self) -> T:
        if not self._loaded:
            self._instance = self._loader()
            self._loaded = True
        return self._instance

    def __getattr__(self, name: str) -> Any:
        return getattr(self._ensure(), name)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._ensure()(*args, **kwargs)

    def __repr__(self) -> str:
        return f"LazyProxy(loaded={self._loaded})"

    @property
    def loaded(self) -> bool:
        """Check if proxy has been resolved."""
        return self._loaded


def lazy_property(func: Callable[..., T]) -> property:
    """Decorator for lazy property evaluation.

    Example:
        class MyClass:
            @lazy_property
            def heavy_data(self):
                return load_heavy_data()
    """
    attr_name = f"_lazy_{func.__name__}"

    @property
    def wrapper(self: Any) -> T:
        if not hasattr(self, attr_name):
            setattr(self, attr_name, func(self))
        return getattr(self, attr_name)

    return wrapper


def try_import(module_name: str, fallback: Any = None) -> Any:
    """Try to import a module, return fallback on failure.

    Args:
        module_name: Module to import.
        fallback: Value to return if import fails.

    Returns:
        Module or fallback.
    """
    try:
        return importlib.import_module(module_name)
    except ImportError:
        return fallback
