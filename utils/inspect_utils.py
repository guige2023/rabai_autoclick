"""Inspection utilities for RabAI AutoClick.

Provides:
- Object introspection helpers
- Function signature analysis
- Source code extraction
- Type information utilities
"""

import inspect
import typing
from typing import Any, Callable, Dict, List, Optional, get_type_hints


def get_signature(func: Callable) -> Optional[inspect.Signature]:
    """Get function signature.

    Args:
        func: Function to inspect.

    Returns:
        Signature object or None.
    """
    try:
        return inspect.signature(func)
    except (ValueError, TypeError):
        return None


def get_parameters(func: Callable) -> List[inspect.Parameter]:
    """Get function parameters.

    Args:
        func: Function to inspect.

    Returns:
        List of parameters.
    """
    sig = get_signature(func)
    if sig is None:
        return []
    return list(sig.parameters.values())


def get_return_type(func: Callable) -> Optional[Any]:
    """Get function return type.

    Args:
        func: Function to inspect.

    Returns:
        Return type annotation or None.
    """
    sig = get_signature(func)
    if sig is None:
        return None
    return sig.return_annotation


def get_type_hints_func(func: Callable) -> Dict[str, Any]:
    """Get function type hints.

    Args:
        func: Function to inspect.

    Returns:
        Dictionary of parameter names to types.
    """
    try:
        return get_type_hints(func)
    except Exception:
        return {}


def has_parameter(func: Callable, name: str) -> bool:
    """Check if function has parameter.

    Args:
        func: Function to check.
        name: Parameter name.

    Returns:
        True if parameter exists.
    """
    params = get_parameters(func)
    return any(p.name == name for p in params)


def get_parameter_defaults(func: Callable) -> Dict[str, Any]:
    """Get parameter defaults.

    Args:
        func: Function to inspect.

    Returns:
        Dict of parameter names to default values.
    """
    params = get_parameters(func)
    return {p.name: p.default for p in params if p.default is not inspect.Parameter.empty}


def is_async(func: Callable) -> bool:
    """Check if function is async.

    Args:
        func: Function to check.

    Returns:
        True if async.
    """
    return inspect.iscoroutinefunction(func)


def is_generator(func: Callable) -> bool:
    """Check if function is generator.

    Args:
        func: Function to check.

    Returns:
        True if generator.
    """
    return inspect.isgeneratorfunction(func)


def is_class_method(func: Callable) -> bool:
    """Check if function is classmethod.

    Args:
        func: Function to check.

    Returns:
        True if classmethod.
    """
    return isinstance(inspect.getattr_static(func, 0, None), classmethod)


def is_static_method(func: Callable) -> bool:
    """Check if function is staticmethod.

    Args:
        func: Function to check.

    Returns:
        True if staticmethod.
    """
    return isinstance(inspect.getattr_static(func, 0, None), staticmethod)


def get_qualified_name(obj: Any) -> str:
    """Get qualified name of object.

    Args:
        obj: Object to inspect.

    Returns:
        Qualified name string.
    """
    module = getattr(obj, "__module__", None)
    qualname = getattr(obj, "__qualname__", None)
    if module and qualname:
        return f"{module}.{qualname}"
    if qualname:
        return qualname
    return type(obj).__name__


def get_source(obj: Any) -> Optional[str]:
    """Get source code of object.

    Args:
        obj: Object to get source for.

    Returns:
        Source code string or None.
    """
    try:
        return inspect.getsource(obj)
    except (OSError, TypeError):
        return None


def get_file(obj: Any) -> Optional[str]:
    """Get file where object is defined.

    Args:
        obj: Object to check.

    Returns:
        File path or None.
    """
    try:
        return inspect.getfile(obj)
    except TypeError:
        return None


def get_line_number(obj: Any) -> Optional[int]:
    """Get line number where object is defined.

    Args:
        obj: Object to check.

    Returns:
        Line number or None.
    """
    try:
        return inspect.getsourcelines(obj)[1]
    except (OSError, TypeError):
        return None


def get_doc(obj: Any) -> Optional[str]:
    """Get docstring of object.

    Args:
        obj: Object to check.

    Returns:
        Docstring or None.
    """
    return inspect.getdoc(obj)


def get_attributes(obj: Any, include_methods: bool = True) -> List[str]:
    """Get object attributes.

    Args:
        obj: Object to inspect.
        include_methods: Include methods.

    Returns:
        List of attribute names.
    """
    attrs = dir(obj)
    if not include_methods:
        attrs = [a for a in attrs if not callable(getattr(obj, a, None))]
    return sorted(attrs)


def get_method_names(obj: Any) -> List[str]:
    """Get method names from object.

    Args:
        obj: Object to inspect.

    Returns:
        List of method names.
    """
    return [name for name in dir(obj) if callable(getattr(obj, name, None)) and not name.startswith("_")]


def get_callable_members(obj: Any) -> List[str]:
    """Get callable public members.

    Args:
        obj: Object to inspect.

    Returns:
        List of member names.
    """
    members = []
    for name, value in inspect.getmembers(obj, predicate=inspect.isfunction):
        if not name.startswith("_"):
            members.append(name)
    return members


def get_class_hierarchy(cls: type) -> List[type]:
    """Get class hierarchy.

    Args:
        cls: Class to inspect.

    Returns:
        List of classes from most base to most derived.
    """
    result = []
    current = cls
    while current is not object and current is not None:
        result.append(current)
        bases = current.__bases__
        current = bases[0] if bases else None
    return result


def get_function_info(func: Callable) -> Dict[str, Any]:
    """Get comprehensive function info.

    Args:
        func: Function to inspect.

    Returns:
        Dictionary with function details.
    """
    return {
        "name": func.__name__,
        "qualified_name": get_qualified_name(func),
        "signature": str(get_signature(func)) if get_signature(func) else None,
        "is_async": is_async(func),
        "is_generator": is_generator(func),
        "is_class_method": is_class_method(func),
        "is_static_method": is_static_method(func),
        "parameters": [p.name for p in get_parameters(func)],
        "defaults": get_parameter_defaults(func),
        "return_type": get_return_type(func),
        "type_hints": get_type_hints_func(func),
        "doc": get_doc(func),
        "source_file": get_file(func),
        "line_number": get_line_number(func),
    }


def get_object_info(obj: Any) -> Dict[str, Any]:
    """Get comprehensive object info.

    Args:
        obj: Object to inspect.

    Returns:
        Dictionary with object details.
    """
    return {
        "type": type(obj).__name__,
        "qualified_type_name": get_qualified_name(type(obj)),
        "module": getattr(obj, "__module__", None),
        "qualified_name": get_qualified_name(obj),
        "attributes": get_attributes(obj),
        "methods": get_method_names(obj),
        "doc": get_doc(obj),
        "source_file": get_file(obj),
        "line_number": get_line_number(obj),
    }


def format_signature(func: Callable) -> str:
    """Format function signature as string.

    Args:
        func: Function to format.

    Returns:
        Formatted signature string.
    """
    sig = get_signature(func)
    if sig is None:
        return f"{func.__name__}(...)"
    return f"{func.__name__}{sig}"


def format_parameters(func: Callable) -> str:
    """Format function parameters as string.

    Args:
        func: Function to format.

    Returns:
        Parameters string.
    """
    params = get_parameters(func)
    if not params:
        return ""
    formatted = []
    for p in params:
        if p.default is not inspect.Parameter.empty:
            formatted.append(f"{p.name}={p.default!r}")
        else:
            formatted.append(p.name)
    return ", ".join(formatted)