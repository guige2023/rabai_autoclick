"""inspect action extensions for rabai_autoclick.

Provides utilities for introspecting Python objects, functions,
classes, and callables at runtime.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable, get_type_hints

__all__ = [
    "get_signature",
    "get_parameters",
    "get_return_type",
    "get_arg_count",
    "get_arg_names",
    "is_coroutine_function",
    "is_async_function",
    "is_generator",
    "is_partial",
    "unwrap_function",
    "get_closure",
    "get_globals",
    "get_locals",
    "get_source",
    "get_source_file",
    "get_source_line",
    "get_docstring",
    "get_name",
    "get_qualname",
    "get_module",
    "get_class_that_defined",
    "get_mro",
    "get_attributes",
    "get_method_resolution_order",
    "call_with_optional_args",
    "inspect_callable",
    "CallableInfo",
    "FunctionInfo",
    "ClassInfo",
    "MethodInfo",
]


def get_signature(func: Callable) -> inspect.Signature:
    """Get the signature of a callable.

    Args:
        func: Function or method to inspect.

    Returns:
        Signature object.
    """
    try:
        return inspect.signature(func)
    except (ValueError, TypeError):
        return inspect.Signature()


def get_parameters(func: Callable) -> list[inspect.Parameter]:
    """Get the parameters of a callable.

    Args:
        func: Function or method to inspect.

    Returns:
        List of parameter objects.
    """
    try:
        return list(inspect.signature(func).parameters.values())
    except (ValueError, TypeError):
        return []


def get_return_type(func: Callable) -> Any:
    """Get the return annotation of a callable.

    Args:
        func: Function or method to inspect.

    Returns:
        Return annotation or None.
    """
    try:
        sig = inspect.signature(func)
        return sig.return_annotation
    except (ValueError, TypeError):
        return None


def get_arg_count(func: Callable) -> int:
    """Get the number of parameters a callable accepts.

    Args:
        func: Function or method to inspect.

    Returns:
        Number of parameters.
    """
    try:
        sig = inspect.signature(func)
        params = sig.parameters
        required = sum(
            1 for p in params.values()
            if p.default is inspect.Parameter.empty
            and p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
        )
        return required
    except (ValueError, TypeError):
        return 0


def get_arg_names(func: Callable) -> list[str]:
    """Get the names of parameters.

    Args:
        func: Function or method to inspect.

    Returns:
        List of parameter names.
    """
    try:
        return list(inspect.signature(func).parameters.keys())
    except (ValueError, TypeError):
        return []


def is_coroutine_function(func: Callable) -> bool:
    """Check if function is a coroutine (async def).

    Args:
        func: Function to check.

    Returns:
        True if coroutine function.
    """
    return inspect.iscoroutinefunction(func)


def is_async_function(func: Callable) -> bool:
    """Check if function is async (alias for coroutine).

    Args:
        func: Function to check.

    Returns:
        True if async function.
    """
    return is_coroutine_function(func)


def is_generator(func: Callable) -> bool:
    """Check if function is a generator.

    Args:
        func: Function to check.

    Returns:
        True if generator function.
    """
    return inspect.isgeneratorfunction(func)


def is_partial(func: Any) -> bool:
    """Check if object is a functools.partial.

    Args:
        func: Object to check.

    Returns:
        True if partial.
    """
    import functools
    return isinstance(func, functools.partial)


def unwrap_function(func: Callable) -> Callable:
    """Unwrap a function, following partial/wrapped chains.

    Args:
        func: Function to unwrap.

    Returns:
        Underlying function.
    """
    try:
        return inspect.unwrap(func)
    except ValueError:
        return func


def get_closure(func: Callable) -> tuple | None:
    """Get the closure of a function.

    Args:
        func: Function to inspect.

    Returns:
        Tuple of cell objects or None.
    """
    try:
        code = func.__code__
        return code.co_freevars
    except AttributeError:
        return None


def get_globals(func: Callable) -> dict:
    """Get the globals of a function.

    Args:
        func: Function to inspect.

    Returns:
        Global variables dict.
    """
    try:
        return func.__globals__
    except AttributeError:
        return {}


def get_locals(func: Callable) -> dict:
    """Get the locals of a frame (not directly accessible).

    Args:
        func: Function to inspect.

    Returns:
        Empty dict (locals not accessible from function).
    """
    return {}


def get_source(func: Callable) -> str:
    """Get the source code of a function.

    Args:
        func: Function to inspect.

    Returns:
        Source code string.

    Raises:
        OSError: If source not available.
    """
    try:
        return inspect.getsource(func)
    except (OSError, TypeError):
        return ""


def get_source_file(func: Callable) -> str:
    """Get the file where a function is defined.

    Args:
        func: Function to inspect.

    Returns:
        File path string.
    """
    try:
        return inspect.getfile(func)
    except TypeError:
        return ""


def get_source_line(func: Callable) -> int:
    """Get the line number where a function starts.

    Args:
        func: Function to inspect.

    Returns:
        Line number.
    """
    try:
        return inspect.getsourcelines(func)[1]
    except (OSError, TypeError):
        return 0


def get_docstring(func: Callable) -> str | None:
    """Get the docstring of a function.

    Args:
        func: Function to inspect.

    Returns:
        Docstring or None.
    """
    return inspect.getdoc(func)


def get_name(func: Callable) -> str:
    """Get the __name__ of a function.

    Args:
        func: Function to inspect.

    Returns:
        Function name.
    """
    return getattr(func, "__name__", str(func))


def get_qualname(func: Callable) -> str:
    """Get the qualified name of a function.

    Args:
        func: Function to inspect.

    Returns:
        Qualified name (module.class.method).
    """
    return getattr(func, "__qualname__", get_name(func))


def get_module(func: Callable) -> str:
    """Get the module name where a function is defined.

    Args:
        func: Function to inspect.

    Returns:
        Module name.
    """
    return getattr(func, "__module__", "")


def get_class_that_defined(method: Callable) -> type | None:
    """Get the class that defined a method.

    Args:
        method: Method to inspect.

    Returns:
        Class or None.
    """
    if hasattr(method, "__self__"):
        return type(method.__self__)
    return None


def get_mro(cls: type) -> list[type]:
    """Get the Method Resolution Order of a class.

    Args:
        cls: Class to inspect.

    Returns:
        List of classes in MRO order.
    """
    return list(cls.__mro__)


def get_attributes(obj: Any, include_properties: bool = True) -> list[str]:
    """Get attribute names of an object.

    Args:
        obj: Object to inspect.
        include_properties: Include @property names.

    Returns:
        List of attribute names.
    """
    attrs = list(dir(obj))
    if not include_properties:
        attrs = [a for a in attrs if not hasattr(getattr(type(obj), a, None), "fget")]
    return attrs


def get_method_resolution_order(cls: type) -> list[type]:
    """Get the method resolution order of a class.

    Args:
        cls: Class to inspect.

    Returns:
        List of classes in MRO.
    """
    return list(inspect.getmro(cls))


def call_with_optional_args(func: Callable, args: tuple, kwargs: dict) -> Any:
    """Call a function, handling extra/missing arguments gracefully.

    Args:
        func: Function to call.
        args: Arguments to pass.
        kwargs: Keyword arguments to pass.

    Returns:
        Return value of function.
    """
    sig = get_signature(func)
    params = sig.parameters

    valid_args = {}
    valid_kwargs = {}

    for name, param in params.items():
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            continue
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            valid_kwargs.update(kwargs)
        elif name in kwargs:
            valid_kwargs[name] = kwargs[name]
        elif param.default is not inspect.Parameter.empty:
            valid_args[name] = param.default

    for arg, name in zip(args, [n for n in params if params[n].kind not in (
        inspect.Parameter.VAR_POSITIONAL,
        inspect.Parameter.VAR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    )]):
        if name in valid_args:
            valid_args[name] = arg
        elif name not in kwargs:
            pass

    return func(*args, **kwargs)


def inspect_callable(func: Callable) -> dict[str, Any]:
    """Get comprehensive information about a callable.

    Args:
        func: Function to inspect.

    Returns:
        Dict with type, name, signature, etc.
    """
    info = {
        "type": type(func).__name__,
        "name": get_name(func),
        "qualname": get_qualname(func),
        "module": get_module(func),
        "is_coroutine": is_coroutine_function(func),
        "is_generator": is_generator(func),
        "is_partial": is_partial(func),
        "is_async": is_async_function(func),
    }

    try:
        info["signature"] = str(get_signature(func))
    except (ValueError, TypeError):
        info["signature"] = "<unknown>"

    try:
        info["arg_count"] = get_arg_count(func)
        info["arg_names"] = get_arg_names(func)
    except Exception:
        info["arg_count"] = 0
        info["arg_names"] = []

    try:
        info["return_type"] = get_return_type(func)
    except Exception:
        info["return_type"] = None

    try:
        info["source_file"] = get_source_file(func)
        info["source_line"] = get_source_line(func)
    except Exception:
        info["source_file"] = ""
        info["source_line"] = 0

    info["docstring"] = get_docstring(func)

    return info


class CallableInfo:
    """Container for callable inspection results."""

    def __init__(self, func: Callable) -> None:
        self._info = inspect_callable(func)

    @property
    def name(self) -> str:
        return self._info["name"]

    @property
    def qualname(self) -> str:
        return self._info["qualname"]

    @property
    def module(self) -> str:
        return self._info["module"]

    @property
    def signature(self) -> str:
        return self._info["signature"]

    @property
    def arg_count(self) -> int:
        return self._info["arg_count"]

    @property
    def arg_names(self) -> list[str]:
        return self._info["arg_names"]

    @property
    def return_type(self) -> Any:
        return self._info["return_type"]

    @property
    def is_coroutine(self) -> bool:
        return self._info["is_coroutine"]

    @property
    def is_generator(self) -> bool:
        return self._info["is_generator"]

    @property
    def is_partial(self) -> bool:
        return self._info["is_partial"]

    def __repr__(self) -> str:
        return f"CallableInfo({self.name}: {self.signature})"


class FunctionInfo(CallableInfo):
    """Info for function callables."""
    pass


class ClassInfo:
    """Container for class inspection results."""

    def __init__(self, cls: type) -> None:
        self._cls = cls

    @property
    def name(self) -> str:
        return self._cls.__name__

    @property
    def qualname(self) -> str:
        return getattr(self._cls, "__qualname__", self._cls.__name__)

    @property
    def module(self) -> str:
        return getattr(self._cls, "__module__", "")

    @property
    def mro(self) -> list[type]:
        return get_mro(self._cls)

    @property
    def attributes(self) -> list[str]:
        return get_attributes(self._cls)

    @property
    def docstring(self) -> str | None:
        return inspect.getdoc(self._cls)

    def __repr__(self) -> str:
        return f"ClassInfo({self.name})"


class MethodInfo(CallableInfo):
    """Info for method callables."""

    @property
    def defining_class(self) -> type | None:
        return get_class_that_defined(self._func)
