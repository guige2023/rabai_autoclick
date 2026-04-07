"""sinspect action extensions for rabai_autoclick.

Provides advanced introspection utilities for functions, classes,
modules, and callables. Complements the inspect module.
"""

from __future__ import annotations

import inspect
import sys
import types
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_type_hints,
)

__all__ = [
    "get_name",
    "get_qualname",
    "get_module",
    "get_file",
    "get_source",
    "get_source_lines",
    "get_signature",
    "get_params",
    "get_return_annotation",
    "get_annotations",
    "get_defaults",
    "get_locals",
    "get_globals",
    "get_closure",
    "get_code",
    "get_written_vars",
    "read_closure",
    "is_function",
    "is_method",
    "is_classmethod",
    "is_staticmethod",
    "is_property",
    "is_generator",
    "is_async",
    "is_coroutine",
    "is_builtin",
    "is_class",
    "is_module",
    "is_lambda",
    "is_partial",
    "is_wrapped",
    "unwrap_function",
    "get_wrapper_chain",
    "callable_name",
    "callable_signature",
    "safe_signature",
    "format_signature",
    "format_params",
    "short_signature",
    "get_arg_names",
    "get_arg_count",
    "get_positional_only",
    "get_variadic",
    "has_varargs",
    "has_varkw",
    "get_type_check",
    "get_doc",
    "get_short_doc",
    "has_doc",
    "is_dataclass",
    "is_enum_class",
    "is_exception_class",
    "is_abstract",
    "get_subclasses",
    "get_mro",
    "get_bases",
    "class_properties",
    "class_methods",
    "instance_methods",
    "walk_mro",
    "get_inner",
    "get_outer",
    "call_with_bind",
    "try_bind",
    "signature_bind",
    "count_locals",
    "estimate_size",
    "get_frame_info",
    "get_call_stack",
    "current_frame",
    "get_source_object",
    "get_method_self",
    "get_method_class",
    "is_final_method",
    "is_override",
    "is_slot",
    "get_dict",
    "has_dict",
    "get_slots",
    "is_pickleable",
    "FuncInfo",
    "ClassInfo",
    "FrameInfo",
]


T = TypeVar("T")


def get_name(obj: Any) -> str:
    """Get the name of an object.

    Args:
        obj: Object to get name from.

    Returns:
        Object's __name__ or str representation.
    """
    return getattr(obj, "__name__", str(obj))


def get_qualname(obj: Any) -> str:
    """Get the qualified name of an object.

    Args:
        obj: Object to get qualified name from.

    Returns:
        Fully qualified name.
    """
    return getattr(obj, "__qualname__", get_name(obj))


def get_module(obj: Any) -> Optional[types.ModuleType]:
    """Get the module an object belongs to.

    Args:
        obj: Object to get module for.

    Returns:
        Module object or None.
    """
    return getattr(obj, "__module__", None)


def get_file(obj: Any) -> Optional[str]:
    """Get the file where an object is defined.

    Args:
        obj: Object to get file for.

    Returns:
        File path or None.
    """
    try:
        return inspect.getfile(obj)
    except TypeError:
        return None


def get_source(obj: Any) -> Optional[str]:
    """Get the source code of an object.

    Args:
        obj: Object to get source for.

    Returns:
        Source code string or None.
    """
    try:
        return inspect.getsource(obj)
    except (OSError, TypeError):
        return None


def get_source_lines(obj: Any) -> Optional[Tuple[List[str], int]]:
    """Get source code as lines.

    Args:
        obj: Object to get source for.

    Returns:
        Tuple of (lines, start_line) or None.
    """
    try:
        lines, start = inspect.getsourcelines(obj)
        return lines, start
    except (OSError, TypeError):
        return None


def get_signature(func: Callable[..., Any]) -> Optional[inspect.Signature]:
    """Get the signature of a callable.

    Args:
        func: Callable to get signature for.

    Returns:
        Signature object or None.
    """
    try:
        return inspect.signature(func)
    except (ValueError, TypeError):
        return None


def get_params(func: Callable[..., Any]) -> List[inspect.Parameter]:
    """Get parameters of a callable.

    Args:
        func: Callable to get params for.

    Returns:
        List of parameters.
    """
    sig = get_signature(func)
    if sig is None:
        return []
    return list(sig.parameters.values())


def get_return_annotation(func: Callable[..., Any]) -> Any:
    """Get return type annotation.

    Args:
        func: Function to get return annotation for.

    Returns:
        Return annotation or inspect.Parameter.empty.
    """
    hints = get_annotations(func)
    return hints.get("return", inspect.Parameter.empty)


def get_annotations(obj: Any) -> Dict[str, Any]:
    """Get type annotations.

    Args:
        obj: Object to get annotations for.

    Returns:
        Dict of annotations.
    """
    try:
        return get_type_hints(obj)
    except Exception:
        try:
            return getattr(obj, "__annotations__", {})
        except Exception:
            return {}


def get_defaults(func: Callable[..., Any]) -> Tuple[Tuple, Dict]:
    """Get default argument values.

    Args:
        func: Function to get defaults for.

    Returns:
        Tuple of (positional_defaults, keyword_defaults).
    """
    try:
        code = func.__code__
        defaults = func.__defaults__ or ()
        # Split positional defaults
        total_args = code.co_argcount + code.co_kwonlyargcount
        positional_count = total_args - len(defaults)
        return defaults, {}
    except AttributeError:
        return (), {}


def get_locals(func: Callable[..., Any]) -> Dict[str, Any]:
    """Get local variable names from code object.

    Args:
        func: Function to get locals from.

    Returns:
        Dict of local variable info.
    """
    try:
        code = func.__code__
        return {
            "vars": code.co_varnames,
            "nlocals": code.co_nlocals,
            "freevars": code.co_freevars,
            "cellvars": code.co_cellvars,
        }
    except AttributeError:
        return {}


def get_globals(func: Callable[..., Any]) -> Dict[str, Any]:
    """Get global names used by function.

    Args:
        func: Function to get globals from.

    Returns:
        Dict of global names.
    """
    try:
        code = func.__code__
        return {
            "names": code.co_names,
            "consts": code.co_consts,
        }
    except AttributeError:
        return {}


def get_closure(func: Callable[..., Any]) -> Optional[Tuple[Any, ...]]:
    """Get closure of a function.

    Args:
        func: Function to get closure from.

    Returns:
        Closure tuple or None.
    """
    return getattr(func, "__closure__", None)


def get_code(func: Callable[..., Any]) -> Optional[types.CodeType]:
    """Get code object of a function.

    Args:
        func: Function to get code from.

    Returns:
        Code object or None.
    """
    return getattr(func, "__code__", None)


def get_written_vars(func: Callable[..., Any]) -> List[str]:
    """Get variables written by a function.

    Args:
        func: Function to analyze.

    Returns:
        List of variable names written.
    """
    try:
        code = func.__code__
        return list(code.co_varnames[:code.co_nlocals])
    except AttributeError:
        return []


def read_closure(func: Callable[..., Any]) -> Dict[str, Any]:
    """Read values from closure.

    Args:
        func: Function with closure.

    Returns:
        Dict mapping freevar names to cell contents.
    """
    closure = get_closure(func)
    if closure is None:
        return {}
    try:
        code = func.__code__
        freevars = code.co_freevars
        return dict(zip(freevars, (cell.cell_contents for cell in closure)))
    except (AttributeError, TypeError):
        return {}


def is_function(obj: Any) -> bool:
    """Check if object is a function.

    Args:
        obj: Object to check.

    Returns:
        True if obj is a function.
    """
    return callable(obj) and hasattr(obj, "__code__")


def is_method(obj: Any) -> bool:
    """Check if object is a bound method.

    Args:
        obj: Object to check.

    Returns:
        True if obj is a bound method.
    """
    return inspect.ismethod(obj)


def is_classmethod(func: Callable[..., Any]) -> bool:
    """Check if function is a classmethod.

    Args:
        func: Function to check.

    Returns:
        True if func is classmethod.
    """
    return isinstance(inspect.getattr_static(func, "__func__", None), classmethod)


def is_staticmethod(func: Callable[..., Any]) -> bool:
    """Check if function is a staticmethod.

    Args:
        func: Function to check.

    Returns:
        True if func is staticmethod.
    """
    return isinstance(inspect.getattr_static(func, "__func__", None), staticmethod)


def is_property(func: Callable[..., Any]) -> bool:
    """Check if function is a property.

    Args:
        func: Function to check.

    Returns:
        True if func is a property.
    """
    return isinstance(inspect.getattr_static(func, "fget", None), property)


def is_generator(func: Callable[..., Any]) -> bool:
    """Check if function is a generator.

    Args:
        func: Function to check.

    Returns:
        True if func yields values.
    """
    try:
        code = func.__code__
        return code.co_flags & inspect.CO_GENERATOR != 0
    except AttributeError:
        return False


def is_async(func: Callable[..., Any]) -> bool:
    """Check if function is async.

    Args:
        func: Function to check.

    Returns:
        True if func is coroutine function.
    """
    return inspect.iscoroutinefunction(func)


def is_coroutine(obj: Any) -> bool:
    """Check if object is a coroutine.

    Args:
        obj: Object to check.

    Returns:
        True if obj is coroutine.
    """
    return inspect.iscoroutine(obj)


def is_builtin(obj: Any) -> bool:
    """Check if object is a builtin.

    Args:
        obj: Object to check.

    Returns:
        True if obj is builtin.
    """
    return inspect.isbuiltin(obj)


def is_class(obj: Any) -> bool:
    """Check if object is a class.

    Args:
        obj: Object to check.

    Returns:
        True if obj is a class.
    """
    return inspect.isclass(obj)


def is_module(obj: Any) -> bool:
    """Check if object is a module.

    Args:
        obj: Object to check.

    Returns:
        True if obj is a module.
    """
    return inspect.ismodule(obj)


def is_lambda(func: Callable[..., Any]) -> bool:
    """Check if function is a lambda.

    Args:
        func: Function to check.

    Returns:
        True if func is a lambda.
    """
    name = get_name(func)
    return name == "<lambda>"


def is_partial(func: Callable[..., Any]) -> bool:
    """Check if object is a partial.

    Args:
        func: Object to check.

    Returns:
        True if func is functools.partial.
    """
    return isinstance(func, type(().__class__.__class__.__init__)) and hasattr(func, "func")


def is_wrapped(func: Callable[..., Any]) -> bool:
    """Check if function wraps another.

    Args:
        func: Function to check.

    Returns:
        True if func has __wrapped__.
    """
    return hasattr(func, "__wrapped__")


def unwrap_function(func: Callable[..., Any]) -> Callable[..., Any]:
    """Unwrap a wrapped function.

    Args:
        func: Function to unwrap.

    Returns:
        Innermost function.
    """
    try:
        return inspect.unwrap(func)
    except ValueError:
        return func


def get_wrapper_chain(func: Callable[..., Any]) -> List[Callable[..., Any]]:
    """Get the chain of wrapped functions.

    Args:
        func: Function to unwrap.

    Returns:
        List from outermost to innermost.
    """
    chain = []
    current = func
    while is_wrapped(current):
        chain.append(current)
        current = current.__wrapped__
    chain.append(current)
    return chain


def callable_name(func: Callable[..., Any]) -> str:
    """Get name of callable.

    Args:
        func: Callable to get name of.

    Returns:
        Callable name.
    """
    if is_method(func):
        func = func.__func__
    return get_qualname(func)


def callable_signature(func: Callable[..., Any]) -> str:
    """Get signature as string.

    Args:
        func: Callable to get signature for.

    Returns:
        Signature string.
    """
    sig = get_signature(func)
    if sig is None:
        return "(*args, **kwargs)"
    return str(sig)


def safe_signature(func: Callable[..., Any]) -> inspect.Signature:
    """Get signature with fallback.

    Args:
        func: Callable to get signature for.

    Returns:
        Signature or empty signature.
    """
    sig = get_signature(func)
    if sig is None:
        return inspect.Signature()
    return sig


def format_signature(func: Callable[..., Any], max_params: int = 10) -> str:
    """Format signature for display.

    Args:
        func: Function to format.
        max_params: Max params to show.

    Returns:
        Formatted signature string.
    """
    sig = safe_signature(func)
    params = list(sig.parameters.keys())[:max_params]
    return f"{callable_name(func)}({', '.join(params)})"


def format_params(func: Callable[..., Any]) -> List[str]:
    """Format parameters as strings.

    Args:
        func: Function to format params for.

    Returns:
        List of formatted param strings.
    """
    params = get_params(func)
    result = []
    for p in params:
        s = p.name
        if p.default is not inspect.Parameter.empty:
            s += f"={p.default!r}"
        if p.kind == inspect.Parameter.VAR_POSITIONAL:
            s = f"*{s}"
        elif p.kind == inspect.Parameter.VAR_KEYWORD:
            s = f"**{s}"
        result.append(s)
    return result


def short_signature(func: Callable[..., Any], max_len: int = 60) -> str:
    """Get short signature string.

    Args:
        func: Function to get signature for.
        max_len: Maximum length.

    Returns:
        Truncated signature string.
    """
    sig = format_signature(func)
    if len(sig) > max_len:
        return sig[:max_len - 3] + "..."
    return sig


def get_arg_names(func: Callable[..., Any]) -> List[str]:
    """Get argument names.

    Args:
        func: Function to get args from.

    Returns:
        List of argument names.
    """
    return [p.name for p in get_params(func)]


def get_arg_count(func: Callable[..., Any]) -> int:
    """Get number of arguments.

    Args:
        func: Function to count args for.

    Returns:
        Number of arguments.
    """
    return len(get_params(func))


def get_positional_only(func: Callable[..., Any]) -> List[str]:
    """Get positional-only argument names.

    Args:
        func: Function to get args from.

    Returns:
        List of positional-only arg names.
    """
    return [p.name for p in get_params(func)
            if p.kind == inspect.Parameter.POSITIONAL_ONLY]


def get_variadic(func: Callable[..., Any]) -> Tuple[Optional[str], Optional[str]]:
    """Get *args and **kwargs names.

    Args:
        func: Function to get variadic from.

    Returns:
        Tuple of (*args_name, **kwargs_name).
    """
    var_positional = None
    var_keyword = None
    for p in get_params(func):
        if p.kind == inspect.Parameter.VAR_POSITIONAL:
            var_positional = p.name
        elif p.kind == inspect.Parameter.VAR_KEYWORD:
            var_keyword = p.name
    return var_positional, var_keyword


def has_varargs(func: Callable[..., Any]) -> bool:
    """Check if function has *args.

    Args:
        func: Function to check.

    Returns:
        True if *args present.
    """
    return get_variadic(func)[0] is not None


def has_varkw(func: Callable[..., Any]) -> bool:
    """Check if function has **kwargs.

    Args:
        func: Function to check.

    Returns:
        True if **kwargs present.
    """
    return get_variadic(func)[1] is not None


def get_type_check(func: Callable[..., Any]) -> bool:
    """Check if function has type checking enabled.

    Args:
        func: Function to check.

    Returns:
        True if type checking is on.
    """
    return getattr(func, "__annotations__", {}) and hasattr(func, "__code__")


def get_doc(func: Callable[..., Any]) -> Optional[str]:
    """Get docstring.

    Args:
        func: Function to get doc for.

    Returns:
        Docstring or None.
    """
    return inspect.getdoc(func)


def get_short_doc(func: Callable[..., Any], max_len: int = 100) -> str:
    """Get short docstring.

    Args:
        func: Function to get doc for.
        max_len: Maximum length.

    Returns:
        Truncated docstring.
    """
    doc = get_doc(func) or ""
    first_line = doc.split("\n")[0].strip()
    if len(first_line) > max_len:
        return first_line[:max_len - 3] + "..."
    return first_line


def has_doc(func: Callable[..., Any]) -> bool:
    """Check if function has a docstring.

    Args:
        func: Function to check.

    Returns:
        True if docstring exists.
    """
    return get_doc(func) is not None


def is_dataclass(cls: Type) -> bool:
    """Check if class is a dataclass.

    Args:
        cls: Class to check.

    Returns:
        True if cls is a dataclass.
    """
    return hasattr(cls, "__dataclass_fields__")


def is_enum_class(cls: Type) -> bool:
    """Check if class is an enum.

    Args:
        cls: Class to check.

    Returns:
        True if cls is an enum.
    """
    try:
        import enum
        return issubclass(cls, enum.Enum)
    except TypeError:
        return False


def is_exception_class(cls: Type) -> bool:
    """Check if class is an exception.

    Args:
        cls: Class to check.

    Returns:
        True if cls inherits from Exception.
    """
    try:
        return issubclass(cls, BaseException)
    except TypeError:
        return False


def is_abstract(cls: Type) -> bool:
    """Check if class is abstract.

    Args:
        cls: Class to check.

    Returns:
        True if cls is abstract.
    """
    return inspect.isabstract(cls)


def get_subclasses(cls: Type) -> List[Type]:
    """Get all subclasses of a class.

    Args:
        cls: Class to get subclasses for.

    Returns:
        List of subclasses.
    """
    return list(cls.__subclasses__())


def get_mro(cls: Type) -> List[Type]:
    """Get method resolution order.

    Args:
        cls: Class to get MRO for.

    Returns:
        List of classes in MRO.
    """
    return list(cls.__mro__)


def get_bases(cls: Type) -> Tuple[Type, ...]:
    """Get base classes.

    Args:
        cls: Class to get bases for.

    Returns:
        Tuple of base classes.
    """
    return cls.__bases__


def class_properties(cls: Type) -> List[str]:
    """Get property names of a class.

    Args:
        cls: Class to get properties from.

    Returns:
        List of property names.
    """
    return [name for name in dir(cls)
            if isinstance(getattr(type(cls), name, None), property)]


def class_methods(cls: Type) -> List[str]:
    """Get method names of a class.

    Args:
        cls: Class to get methods from.

    Returns:
        List of method names.
    """
    return [name for name, obj in inspect.getmembers(cls, inspect.isfunction)
            if not name.startswith("_")]


def instance_methods(cls: Type) -> List[str]:
    """Get instance method names of a class.

    Args:
        cls: Class to get methods from.

    Returns:
        List of instance method names.
    """
    return [name for name in dir(cls)
            if callable(getattr(cls, name, None)) and not name.startswith("_")]


def walk_mro(cls: Type) -> List[Type]:
    """Walk MRO and yield classes.

    Args:
        cls: Class to start from.

    Yields:
        Classes in MRO order.
    """
    for c in cls.__mro__:
        yield c


def get_inner(obj: Any) -> Optional[Callable[..., Any]]:
    """Get inner callable of wrapped object.

    Args:
        obj: Object to get inner from.

    Returns:
        Inner callable or None.
    """
    if hasattr(obj, "__wrapped__"):
        return obj.__wrapped__
    if hasattr(obj, "__func__"):
        return obj.__func__
    if hasattr(obj, "func"):
        return obj.func
    return None


def get_outer(func: Callable[..., Any]) -> Optional[Callable[..., Any]]:
    """Get outer wrapper of wrapped function.

    Args:
        func: Function to get outer from.

    Returns:
        Outer wrapper or None.
    """
    for wrapper in getattr(func, "__wrapped__", []):
        return wrapper
    return None


def call_with_bind(
    func: Callable[..., T],
    args: Tuple,
    kwargs: Dict,
) -> T:
    """Call function with pre-bound arguments.

    Args:
        func: Function to call.
        args: Positional arguments.
        kwargs: Keyword arguments.

    Returns:
        Result of function call.
    """
    sig = safe_signature(func)
    bound = sig.bind_partial(*args, **kwargs)
    return func(*bound.args, **bound.kwargs)


def try_bind(
    func: Callable[..., T],
    args: Tuple,
    kwargs: Dict,
) -> Tuple[bool, Optional[T]]:
    """Try to bind arguments to function.

    Args:
        func: Function to bind to.
        args: Positional arguments.
        kwargs: Keyword arguments.

    Returns:
        Tuple of (success, result or None).
    """
    try:
        sig = safe_signature(func)
        sig.bind(*args, **kwargs)
        return True, None
    except TypeError:
        return False, None


def signature_bind(
    func: Callable[..., T],
    *args: Any,
    **kwargs: Any,
) -> inspect.BoundArguments:
    """Bind arguments to signature.

    Args:
        func: Function to bind to.
        *args: Positional arguments.
        **kwargs: Keyword arguments.

    Returns:
        Bound arguments.
    """
    sig = safe_signature(func)
    return sig.bind(*args, **kwargs)


def count_locals(func: Callable[..., Any]) -> int:
    """Count local variables in function.

    Args:
        func: Function to count.

    Returns:
        Number of local variables.
    """
    try:
        return func.__code__.co_nlocals
    except AttributeError:
        return 0


def estimate_size(func: Callable[..., Any]) -> int:
    """Estimate size of function object.

    Args:
        func: Function to estimate.

    Returns:
        Estimated size in bytes.
    """
    try:
        code = func.__code__
        size = code.co_stacksize * 8
        size += len(code.co_varnames) * 64
        size += len(code.co_freevars) * 64
        return size
    except AttributeError:
        return 0


def get_frame_info(frame: types.FrameType) -> Dict[str, Any]:
    """Get information about a frame.

    Args:
        frame: Frame to inspect.

    Returns:
        Dict with frame information.
    """
    return {
        "filename": frame.f_code.co_filename,
        "lineno": frame.f_lineno,
        "function": frame.f_code.co_name,
        "locals": list(frame.f_locals.keys()),
        "globals": list(frame.f_globals.keys()),
    }


def get_call_stack(depth: int = 10) -> List[Dict[str, Any]]:
    """Get current call stack.

    Args:
        depth: Maximum depth to trace.

    Returns:
        List of frame info dicts.
    """
    frames = []
    frame = sys._getframe()
    for _ in range(depth):
        if frame is None:
            break
        frames.append(get_frame_info(frame))
        frame = frame.f_back
    return frames


def current_frame() -> Optional[types.FrameType]:
    """Get current frame.

    Returns:
        Current frame or None.
    """
    return sys._getframe()


def get_source_object(obj: Any) -> Optional[Any]:
    """Get source object (follows wrappers).

    Args:
        obj: Object to get source of.

    Returns:
        Source object or None.
    """
    while hasattr(obj, "__wrapped__"):
        obj = obj.__wrapped__
    return obj


def get_method_self(method: Callable[..., Any]) -> Optional[Any]:
    """Get the self argument of a method.

    Args:
        method: Method to get self from.

    Returns:
        Self object or None.
    """
    return getattr(method, "__self__", None)


def get_method_class(func: Callable[..., Any]) -> Optional[Type]:
    """Get class that a method belongs to.

    Args:
        func: Function to get class for.

    Returns:
        Class or None.
    """
    self_obj = get_method_self(func)
    if self_obj is not None:
        return type(self_obj)
    return None


def is_final_method(cls: Type, name: str) -> bool:
    """Check if method is final.

    Args:
        cls: Class to check.
        name: Method name.

    Returns:
        True if method is marked final.
    """
    try:
        import typing
        method = getattr(cls, name, None)
        return getattr(method, "__final__", False)
    except Exception:
        return False


def is_override(method: Callable[..., Any]) -> bool:
    """Check if method is an override.

    Args:
        method: Method to check.

    Returns:
        True if method overrides a parent.
    """
    return getattr(method, "__override__", False)


def is_slot(cls: Type, name: str) -> bool:
    """Check if attribute is a slot.

    Args:
        cls: Class to check.
        name: Attribute name.

    Returns:
        True if attribute is a slot.
    """
    return hasattr(cls, "__slots__") and name in cls.__slots__


def get_dict(obj: Any) -> Optional[Dict]:
    """Get __dict__ of object.

    Args:
        obj: Object to get dict from.

    Returns:
        __dict__ or None.
    """
    return getattr(obj, "__dict__", None)


def has_dict(obj: Any) -> bool:
    """Check if object has __dict__.

    Args:
        obj: Object to check.

    Returns:
        True if object has __dict__.
    """
    return hasattr(obj, "__dict__")


def get_slots(obj: Any) -> Optional[List[str]]:
    """Get slots of object.

    Args:
        obj: Object to get slots from.

    Returns:
        List of slot names or None.
    """
    return getattr(type(obj), "__slots__", None)


def is_pickleable(obj: Any) -> bool:
    """Check if object is pickleable.

    Args:
        obj: Object to check.

    Returns:
        True if object can be pickled.
    """
    try:
        import pickle
        pickle.dumps(obj)
        return True
    except Exception:
        return False


class FuncInfo:
    """Information about a function."""

    def __init__(self, func: Callable[..., Any]) -> None:
        """Initialize FuncInfo.

        Args:
            func: Function to analyze.
        """
        self.func = func
        self.name = callable_name(func)
        self.qualname = get_qualname(func)
        self.module = get_module(func)
        self.file = get_file(func)
        self.signature = safe_signature(func)
        self.doc = get_doc(func)
        self.annotations = get_annotations(func)
        self.is_async = is_async(func)
        self.is_generator = is_generator(func)
        self.is_method = is_method(func)
        self.is_lambda = is_lambda(func)
        self.is_wrapped = is_wrapped(func)

    def __repr__(self) -> str:
        return f"FuncInfo({self.name})"


class ClassInfo:
    """Information about a class."""

    def __init__(self, cls: Type) -> None:
        """Initialize ClassInfo.

        Args:
            cls: Class to analyze.
        """
        self.cls = cls
        self.name = cls.__name__
        self.qualname = get_qualname(cls)
        self.module = get_module(cls)
        self.mro = get_mro(cls)
        self.bases = get_bases(cls)
        self.is_dataclass = is_dataclass(cls)
        self.is_enum = is_enum_class(cls)
        self.is_exception = is_exception_class(cls)
        self.is_abstract = is_abstract(cls)
        self.properties = class_properties(cls)
        self.methods = class_methods(cls)

    def __repr__(self) -> str:
        return f"ClassInfo({self.name})"


class FrameInfo:
    """Information about a stack frame."""

    def __init__(self, frame: types.FrameType) -> None:
        """Initialize FrameInfo.

        Args:
            frame: Frame to analyze.
        """
        self.frame = frame
        self.filename = frame.f_code.co_filename
        self.lineno = frame.f_lineno
        self.function = frame.f_code.co_name
        self.locals = frame.f_locals
        self.globals = frame.f_globals

    def __repr__(self) -> str:
        return f"FrameInfo({self.function} at {self.filename}:{self.lineno})"
