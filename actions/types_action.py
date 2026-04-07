"""
Types Action Module

Provides built-in type operations including type checking, conversion,
creation, and introspection for the automation framework.

Author: AI Assistant
Version: 1.0.0
"""

from __future__ import annotations

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
)

# Type variables
T = TypeVar("T")


class TypesAction:
    """
    Main types action handler providing type operations.
    
    This class provides utilities for working with Python's built-in
    types and type checking/conversion operations.
    
    Attributes:
        None (all methods are static or class methods)
    """
    
    # Built-in type mappings
    TYPE_NAMES: Dict[Type, str] = {
        int: "int",
        float: "float",
        str: "str",
        bool: "bool",
        list: "list",
        dict: "dict",
        tuple: "tuple",
        set: "set",
        frozenset: "frozenset",
        bytes: "bytes",
        bytearray: "bytearray",
        type(None): "NoneType",
        list[int]: "list[int]",
        dict[str, Any]: "dict[str, Any]",
    }
    
    @staticmethod
    def get_type(obj: Any) -> Type:
        """
        Get the type of an object.
        
        Args:
            obj: Object to get type of
        
        Returns:
            Type of the object
        
        Example:
            >>> TypesAction.get_type(42)
            <class 'int'>
        """
        return type(obj)
    
    @staticmethod
    def get_type_name(obj: Any) -> str:
        """
        Get the name of an object's type.
        
        Args:
            obj: Object to get type name of
        
        Returns:
            Type name as string
        
        Example:
            >>> TypesAction.get_type_name(42)
            'int'
        """
        return type(obj).__name__
    
    @staticmethod
    def is_int(obj: Any) -> bool:
        """
        Check if object is an integer (int or bool).
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is an integer type
        
        Example:
            >>> TypesAction.is_int(42)
            True
            >>> TypesAction.is_int(True)
            True
            >>> TypesAction.is_int(3.14)
            False
        """
        return isinstance(obj, (int, bool))
    
    @staticmethod
    def is_float(obj: Any) -> bool:
        """
        Check if object is a float.
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is a float
        
        Example:
            >>> TypesAction.is_float(3.14)
            True
            >>> TypesAction.is_float(42)
            False
        """
        return isinstance(obj, float)
    
    @staticmethod
    def is_str(obj: Any) -> bool:
        """
        Check if object is a string.
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is a string
        
        Example:
            >>> TypesAction.is_str("hello")
            True
            >>> TypesAction.is_str(b"hello")
            False
        """
        return isinstance(obj, str)
    
    @staticmethod
    def is_bool(obj: Any) -> bool:
        """
        Check if object is a boolean.
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is a boolean
        
        Example:
            >>> TypesAction.is_bool(True)
            True
            >>> TypesAction.is_bool(1)
            False
        """
        return isinstance(obj, bool)
    
    @staticmethod
    def is_list(obj: Any) -> bool:
        """
        Check if object is a list.
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is a list
        
        Example:
            >>> TypesAction.is_list([1, 2, 3])
            True
        """
        return isinstance(obj, list)
    
    @staticmethod
    def is_dict(obj: Any) -> bool:
        """
        Check if object is a dictionary.
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is a dict
        
        Example:
            >>> TypesAction.is_dict({"a": 1})
            True
        """
        return isinstance(obj, dict)
    
    @staticmethod
    def is_tuple(obj: Any) -> bool:
        """
        Check if object is a tuple.
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is a tuple
        """
        return isinstance(obj, tuple)
    
    @staticmethod
    def is_set(obj: Any) -> bool:
        """
        Check if object is a set.
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is a set
        """
        return isinstance(obj, (set, frozenset))
    
    @staticmethod
    def is_bytes(obj: Any) -> bool:
        """
        Check if object is bytes.
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is bytes or bytearray
        """
        return isinstance(obj, (bytes, bytearray))
    
    @staticmethod
    def is_none(obj: Any) -> bool:
        """
        Check if object is None.
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is None
        """
        return obj is None
    
    @staticmethod
    def is_callable(obj: Any) -> bool:
        """
        Check if object is callable.
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is callable
        """
        return callable(obj)
    
    @staticmethod
    def is_iterable(obj: Any) -> bool:
        """
        Check if object is iterable.
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is iterable
        """
        try:
            iter(obj)
            return True
        except TypeError:
            return False
    
    @staticmethod
    def is_mapping(obj: Any) -> bool:
        """
        Check if object is a mapping (dict-like).
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is a mapping
        """
        return isinstance(obj, dict)
    
    @staticmethod
    def is_sequence(obj: Any) -> bool:
        """
        Check if object is a sequence (list-like).
        
        Args:
            obj: Object to check
        
        Returns:
            True if object is a sequence
        """
        return isinstance(obj, (list, tuple, str, bytes, bytearray))
    
    @staticmethod
    def to_int(obj: Any, default: Optional[int] = None) -> Optional[int]:
        """
        Convert object to integer.
        
        Args:
            obj: Object to convert
            default: Default value if conversion fails
        
        Returns:
            Integer value or default
        
        Example:
            >>> TypesAction.to_int(42)
            42
            >>> TypesAction.to_int("123")
            123
            >>> TypesAction.to_int("abc", default=0)
            0
        """
        try:
            return int(obj)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def to_float(obj: Any, default: Optional[float] = None) -> Optional[float]:
        """
        Convert object to float.
        
        Args:
            obj: Object to convert
            default: Default value if conversion fails
        
        Returns:
            Float value or default
        """
        try:
            return float(obj)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def to_str(obj: Any) -> str:
        """
        Convert object to string.
        
        Args:
            obj: Object to convert
        
        Returns:
            String representation
        """
        return str(obj)
    
    @staticmethod
    def to_bool(obj: Any) -> bool:
        """
        Convert object to boolean.
        
        Args:
            obj: Object to convert
        
        Returns:
            Boolean value (truthy/falsy conversion)
        """
        return bool(obj)
    
    @staticmethod
    def to_list(obj: Any) -> List[Any]:
        """
        Convert object to list.
        
        Args:
            obj: Object to convert
        
        Returns:
            List representation
        
        Example:
            >>> TypesAction.to_list((1, 2, 3))
            [1, 2, 3]
            >>> TypesAction.to_list("abc")
            ['a', 'b', 'c']
        """
        if isinstance(obj, (list, tuple, set)):
            return list(obj)
        if isinstance(obj, str):
            return list(obj)
        if isinstance(obj, dict):
            return list(obj.items())
        return [obj]
    
    @staticmethod
    def to_tuple(obj: Any) -> Tuple[Any, ...]:
        """
        Convert object to tuple.
        
        Args:
            obj: Object to convert
        
        Returns:
            Tuple representation
        """
        if isinstance(obj, (list, tuple, set)):
            return tuple(obj)
        if isinstance(obj, str):
            return tuple(obj)
        if isinstance(obj, dict):
            return tuple(obj.items())
        return (obj,)
    
    @staticmethod
    def to_dict(obj: Any) -> Dict[Any, Any]:
        """
        Convert object to dictionary.
        
        Args:
            obj: Object to convert
        
        Returns:
            Dict representation
        
        Example:
            >>> TypesAction.to_dict([["a", 1], ["b", 2]])
            {'a': 1, 'b': 2}
        """
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, (list, tuple)) and obj and isinstance(obj[0], (list, tuple)):
            return dict(obj)
        if hasattr(obj, "__dict__"):
            return vars(obj)
        raise TypeError(f"Cannot convert {type(obj)} to dict")
    
    @staticmethod
    def to_set(obj: Any) -> set:
        """
        Convert object to set.
        
        Args:
            obj: Object to convert
        
        Returns:
            Set representation
        """
        if isinstance(obj, (set, frozenset)):
            return set(obj)
        return set(obj)
    
    @staticmethod
    def coerce_types(
        obj: Any,
        target_type: Type,
        default: Optional[Any] = None,
    ) -> Any:
        """
        Coerce an object to a target type.
        
        Args:
            obj: Object to coerce
            target_type: Target type (int, float, str, bool, list, dict, tuple)
            default: Default value if coercion fails
        
        Returns:
            Coerced value or default
        
        Example:
            >>> TypesAction.coerce_types("42", int)
            42
        """
        converters: Dict[Type, Callable[[Any], Any]] = {
            int: TypesAction.to_int,
            float: TypesAction.to_float,
            str: TypesAction.to_str,
            bool: TypesAction.to_bool,
            list: TypesAction.to_list,
            tuple: TypesAction.to_tuple,
            dict: TypesAction.to_dict,
            set: TypesAction.to_set,
        }
        
        if target_type not in converters:
            raise ValueError(f"Unsupported target type: {target_type}")
        
        result = converters[target_type](obj)
        if result is None and default is not None:
            return default
        return result
    
    @staticmethod
    def isinstance_check(obj: Any, type_or_tuple: Union[Type, Tuple[Type, ...]]) -> bool:
        """
        Check if object is an instance of type(s).
        
        Args:
            obj: Object to check
            type_or_tuple: Type or tuple of types to check against
        
        Returns:
            True if obj is instance of type(s)
        """
        return isinstance(obj, type_or_tuple)
    
    @staticmethod
    def issubclass_check(cls: Type, class_or_tuple: Union[Type, Tuple[Type, ...]]) -> bool:
        """
        Check if cls is a subclass of class_or_tuple.
        
        Args:
            cls: Class to check
            class_or_tuple: Class or tuple of classes
        
        Returns:
            True if cls is subclass
        """
        return issubclass(cls, class_or_tuple)
    
    @staticmethod
    def create_function(
        name: str,
        code: str,
        *,
        globals_dict: Optional[Dict[str, Any]] = None,
        locals_dict: Optional[Dict[str, Any]] = None,
    ) -> types.FunctionType:
        """
        Create a function from a code string.
        
        Args:
            name: Function name
            code: Code string
            globals_dict: Optional globals dict
            locals_dict: Optional locals dict
        
        Returns:
            Created function
        
        Example:
            >>> f = TypesAction.create_function("add", "return lambda x, y: x + y")
            >>> f()(1, 2)
            3
        """
        globals_dict = globals_dict or {}
        locals_dict = locals_dict or {}
        
        compiled = compile(code, "<string>", "eval")
        return types.FunctionType(
            compiled,
            {**globals_dict, **locals_dict},
            name,
        )
    
    @staticmethod
    def create_lambda(params: str, body: str) -> types.LambdaType:
        """
        Create a lambda function.
        
        Args:
            params: Lambda parameters (e.g., "x, y")
            body: Lambda body expression
        
        Returns:
            Lambda function
        
        Example:
            >>> add = TypesAction.create_lambda("x, y", "x + y")
            >>> add(1, 2)
            3
        """
        return eval(f"lambda {params}: {body}", {})
    
    @staticmethod
    def get_module(obj: Any) -> Optional[str]:
        """
        Get the module name of an object.
        
        Args:
            obj: Object to get module of
        
        Returns:
            Module name or None
        """
        return getattr(type(obj), "__module__", None)
    
    @staticmethod
    def get_qualname(obj: Any) -> Optional[str]:
        """
        Get the qualified name of an object.
        
        Args:
            obj: Object to get qualname of
        
        Returns:
            Qualified name or None
        """
        return getattr(type(obj), "__qualname__", None)
    
    @staticmethod
    def get_bases(cls: Type) -> Tuple[Type, ...]:
        """
        Get the base classes of a class.
        
        Args:
            cls: Class to get bases of
        
        Returns:
            Tuple of base classes
        """
        return cls.__bases__
    
    @staticmethod
    def get_mro(cls: Type) -> List[Type]:
        """
        Get the Method Resolution Order of a class.
        
        Args:
            cls: Class to get MRO of
        
        Returns:
            List of classes in MRO
        """
        return list(cls.__mro__)
    
    @staticmethod
    def has_method(obj: Any, name: str) -> bool:
        """
        Check if object has a method with the given name.
        
        Args:
            obj: Object to check
            name: Method name
        
        Returns:
            True if object has the method
        """
        return hasattr(obj, name) and callable(getattr(obj, name))
    
    @staticmethod
    def get_attrs(obj: Any) -> List[str]:
        """
        Get all attribute names of an object.
        
        Args:
            obj: Object to get attributes of
        
        Returns:
            List of attribute names
        """
        return list(dir(obj))
    
    @staticmethod
    def type_hints(func: Callable[..., Any]) -> Dict[str, Any]:
        """
        Get type hints for a function.
        
        Args:
            func: Function to get hints for
        
        Returns:
            Dictionary of parameter names to types
        """
        try:
            import typing
            return typing.get_type_hints(func)
        except Exception:
            return {}
    
    @staticmethod
    def resolve_type(type_hint: Any) -> str:
        """
        Resolve a type hint to a string representation.
        
        Args:
            type_hint: Type hint to resolve
        
        Returns:
            String representation of the type
        """
        try:
            import typing
            if hasattr(type_hint, "__name__"):
                return type_hint.__name__
            return str(type_hint)
        except Exception:
            return str(type_hint)


# Module-level convenience functions
def get_type(obj: Any) -> Type:
    """Get the type of an object."""
    return TypesAction.get_type(obj)


def is_int(obj: Any) -> bool:
    """Check if object is an integer."""
    return TypesAction.is_int(obj)


def is_str(obj: Any) -> bool:
    """Check if object is a string."""
    return TypesAction.is_str(obj)


def to_int(obj: Any, default: Optional[int] = None) -> Optional[int]:
    """Convert object to integer."""
    return TypesAction.to_int(obj, default)


def to_float(obj: Any, default: Optional[float] = None) -> Optional[float]:
    """Convert object to float."""
    return TypesAction.to_float(obj, default)


def to_str(obj: Any) -> str:
    """Convert object to string."""
    return TypesAction.to_str(obj)


def coerce(obj: Any, target: Type, default: Optional[Any] = None) -> Any:
    """Coerce object to target type."""
    return TypesAction.coerce_types(obj, target, default)


# Module metadata
__author__ = "AI Assistant"
__version__ = "1.0.0"
__all__ = [
    "TypesAction",
    "get_type",
    "is_int",
    "is_str",
    "to_int",
    "to_float",
    "to_str",
    "coerce",
]
