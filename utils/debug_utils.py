"""Debug utilities for RabAI AutoClick.

Provides:
- Debug helpers
- Inspection utilities
- Logging helpers
"""

import sys
import traceback
from typing import Any, Callable, Dict, List, Optional
from functools import wraps


def get_type_name(obj: Any) -> str:
    """Get the type name of an object.

    Args:
        obj: Object to inspect.

    Returns:
        Type name string.
    """
    return type(obj).__name__


def get_type_module(obj: Any) -> str:
    """Get the module name of an object's type.

    Args:
        obj: Object to inspect.

    Returns:
        Module name string.
    """
    return type(obj).__module__


def get_full_type_name(obj: Any) -> str:
    """Get the full type name including module.

    Args:
        obj: Object to inspect.

    Returns:
        Full type name string.
    """
    return f"{type(obj).__module__}.{type(obj).__name__}"


def get_object_size(obj: Any) -> int:
    """Get approximate size of object in bytes.

    Args:
        obj: Object to measure.

    Returns:
        Size in bytes.
    """
    return sys.getsizeof(obj)


def get_memory_address(obj: Any) -> str:
    """Get memory address of object.

    Args:
        obj: Object to inspect.

    Returns:
        Memory address string.
    """
    return hex(id(obj))


def get_call_stack(max_depth: int = 10) -> List[str]:
    """Get current call stack as list of strings.

    Args:
        max_depth: Maximum stack depth to return.

    Returns:
        List of stack frame strings.
    """
    stack = traceback.extract_stack()[:-1]
    frames = []
    for frame in stack[-max_depth:]:
        frames.append(f"{frame.filename}:{frame.lineno} in {frame.name}")
    return frames


def get_frame_info(frame_index: int = 0) -> Dict[str, Any]:
    """Get info about a stack frame.

    Args:
        frame_index: Index from current frame (0 = caller).

    Returns:
        Dictionary with frame information.
    """
    frame = sys._getframe(frame_index + 1)
    return {
        "filename": frame.f_code.co_filename,
        "lineno": frame.f_lineno,
        "function": frame.f_code.co_name,
        "locals": list(frame.f_locals.keys()),
    }


def format_dict(data: Dict[str, Any], indent: int = 2) -> str:
    """Format dictionary for pretty printing.

    Args:
        data: Dictionary to format.
        indent: Indentation spaces.

    Returns:
        Formatted string.
    """
    lines = []
    for key, value in data.items():
        if isinstance(value, dict):
            lines.append(f"{' ' * indent}{key}:")
            lines.append(format_dict(value, indent + 2))
        else:
            lines.append(f"{' ' * indent}{key}: {repr(value)}")
    return "\n".join(lines)


def format_list(items: List[Any], indent: int = 2) -> str:
    """Format list for pretty printing.

    Args:
        items: List to format.
        indent: Indentation spaces.

    Returns:
        Formatted string.
    """
    lines = []
    for i, item in enumerate(items):
        if isinstance(item, dict):
            lines.append(f"{' ' * indent}[{i}]:")
            lines.append(format_dict(item, indent + 2))
        elif isinstance(item, list):
            lines.append(f"{' ' * indent}[{i}]:")
            lines.append(format_list(item, indent + 2))
        else:
            lines.append(f"{' ' * indent}[{i}]: {repr(item)}")
    return "\n".join(lines)


def inspect_object(obj: Any, max_depth: int = 3) -> str:
    """Get detailed object inspection.

    Args:
        obj: Object to inspect.
        max_depth: Maximum recursion depth.

    Returns:
        Inspection string.
    """
    lines = [
        f"Type: {get_full_type_name(obj)}",
        f"Size: {get_object_size(obj)} bytes",
        f"Address: {get_memory_address(obj)}",
    ]

    if hasattr(obj, "__dict__"):
        lines.append("Attributes:")
        for key, value in vars(obj).items():
            lines.append(f"  {key}: {repr(value)}")

    if isinstance(obj, dict):
        lines.append("Contents:")
        for i, (key, value) in enumerate(obj.items()):
            if i >= max_depth:
                lines.append(f"  ... ({len(obj) - max_depth} more)")
                break
            lines.append(f"  {repr(key)}: {repr(value)}")

    elif isinstance(obj, (list, tuple)):
        lines.append(f"Length: {len(obj)}")
        if len(obj) > 0:
            lines.append("Items:")
            for i, item in enumerate(obj):
                if i >= max_depth:
                    lines.append(f"  ... ({len(obj) - max_depth} more)")
                    break
                lines.append(f"  [{i}]: {repr(item)}")

    return "\n".join(lines)


def log_call(func: Callable) -> Callable:
    """Decorator to log function calls.

    Args:
        func: Function to wrap.

    Returns:
        Wrapped function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Calling {func.__name__}(args={args}, kwargs={kwargs})")
        try:
            result = func(*args, **kwargs)
            print(f"{func.__name__} returned: {repr(result)}")
            return result
        except Exception as e:
            print(f"{func.__name__} raised: {type(e).__name__}: {e}")
            raise
    return wrapper


def log_args(func: Callable) -> Callable:
    """Decorator to log function arguments.

    Args:
        func: Function to wrap.

    Returns:
        Wrapped function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        arg_names = func.__code__.co_varnames[:func.__code__.co_argcount]
        args_repr = [f"{name}={repr(arg)}" for name, arg in zip(arg_names, args)]
        kwargs_repr = [f"{name}={repr(arg)}" for name, arg in kwargs.items()]
        all_args = ", ".join(args_repr + kwargs_repr)
        print(f"{func.__name__}({all_args})")
        return func(*args, **kwargs)
    return wrapper


def trace_calls(func: Callable) -> Callable:
    """Decorator to trace function calls with stack.

    Args:
        func: Function to wrap.

    Returns:
        Wrapped function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        stack = get_call_stack(3)
        print(f"Trace: {func.__name__}")
        for frame in stack:
            print(f"  {frame}")
        return func(*args, **kwargs)
    return wrapper


def dump_locals(max_frames: int = 5) -> str:
    """Dump local variables from call stack.

    Args:
        max_frames: Maximum frames to inspect.

    Returns:
        String representation of local variables.
    """
    lines = []
    for i in range(max_frames):
        try:
            frame = sys._getframe(i + 1)
            lines.append(f"Frame {i + 1} ({frame.f_code.co_filename}:{frame.f_lineno}):")
            for name, value in frame.f_locals.items():
                if not name.startswith("_"):
                    lines.append(f"  {name}: {repr(value)[:100]}")
        except ValueError:
            break
    return "\n".join(lines)


def print_memory_stats() -> None:
    """Print current memory statistics."""
    import gc
    gc.collect()
    stats = {
        "objects": len(gc.get_objects()),
        "garbage": len(gc.garbage),
    }
    print(f"Memory Stats: {stats}")


def get_reference_count(obj: Any) -> int:
    """Get reference count for object.

    Args:
        obj: Object to check.

    Returns:
        Reference count.
    """
    return sys.getrefcount(obj) - 1


def get_all_attributes(obj: Any) -> List[str]:
    """Get all attributes of object including from parent classes.

    Args:
        obj: Object to inspect.

    Returns:
        List of attribute names.
    """
    attrs = set()
    for klass in type(obj).__mro__:
        attrs.update(klass.__dict__.keys())
    return sorted(attrs)


def has_attribute_chain(obj: Any, attr_path: str) -> bool:
    """Check if object has a chain of attributes.

    Args:
        obj: Object to check.
        attr_path: Dot-separated attribute path.

    Returns:
        True if full path exists.
    """
    parts = attr_path.split(".")
    current = obj
    for part in parts:
        if not hasattr(current, part):
            return False
        current = getattr(current, part)
    return True


def get_attribute_chain(obj: Any, attr_path: str, default: Any = None) -> Any:
    """Get a chain of attributes from object.

    Args:
        obj: Object to traverse.
        attr_path: Dot-separated attribute path.
        default: Default value if path not found.

    Returns:
        Value at path or default.
    """
    parts = attr_path.split(".")
    current = obj
    for part in parts:
        if not hasattr(current, part):
            return default
        current = getattr(current, part)
    return current


def is_debug_mode() -> bool:
    """Check if running in debug mode.

    Returns:
        True if debug mode is enabled.
    """
    return sys.flags.debug or hasattr(sys, 'gettotalrefcount')


def breakpoint_debug() -> None:
    """Breakpoint for debugging using pdb.

    This is a simple breakpoint that invokes pdb.
    """
    import pdb
    pdb.set_trace()
