"""
Copy Action Module

Provides deep and shallow copy operations for Python objects,
with support for circular reference detection and custom copier functions.

Author: AI Assistant
Version: 1.0.0
"""

from __future__ import annotations

import copy
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

# Type variables
T = TypeVar("T")
D = TypeVar("D")


class CopyAction:
    """
    Main copy action handler providing deep/shallow copy operations.
    
    This class wraps Python's copy module with additional utilities
    for common copying tasks and custom deep copy scenarios.
    
    Attributes:
        deepcopy: Reference to copy.deepcopy
        shallowcopy: Reference to copy.copy
    """
    
    @staticmethod
    def shallow_copy(obj: T) -> T:
        """
        Create a shallow copy of an object.
        
        A shallow copy constructs a new compound object and then inserts
        references into it to the objects found in the original.
        
        Args:
            obj: Object to copy
        
        Returns:
            Shallow copy of the object
        
        Example:
            >>> original = [1, [2, 3]]
            >>> copied = CopyAction.shallow_copy(original)
            >>> copied[1].append(4)
            >>> original
            [1, [2, 3, 4]]
        """
        return copy.copy(obj)
    
    @staticmethod
    def deep_copy(obj: T, memo: Optional[Dict[int, Any]] = None) -> T:
        """
        Create a deep copy of an object.
        
        A deep copy constructs a new compound object and then recursively
        inserts copies of the objects found in the original.
        
        Args:
            obj: Object to copy
            memo: Optional dictionary for memoization (avoids infinite loops)
        
        Returns:
            Deep copy of the object
        
        Example:
            >>> original = [1, [2, 3]]
            >>> copied = CopyAction.deep_copy(original)
            >>> copied[1].append(4)
            >>> original
            [1, [2, 3]]
        """
        if memo is None:
            return copy.deepcopy(obj)
        return copy.deepcopy(obj, memo)
    
    @staticmethod
    def copy_list(
        obj: List[Any],
        *,
        deep: bool = True,
        items: bool = True,
    ) -> List[Any]:
        """
        Create a copy of a list.
        
        Args:
            obj: List to copy
            deep: If True, create deep copy of items; shallow otherwise
            items: If True, copy the list items; otherwise return empty list
        
        Returns:
            Copy of the list
        
        Example:
            >>> CopyAction.copy_list([1, 2, 3])
            [1, 2, 3]
            >>> CopyAction.copy_list([1, [2, 3]], deep=False)
            [1, [2, 3]]
        """
        if items:
            if deep:
                return copy.deepcopy(obj)
            return list(obj)
        return []
    
    @staticmethod
    def copy_dict(
        obj: Dict[Any, Any],
        *,
        deep: bool = True,
        items: bool = True,
    ) -> Dict[Any, Any]:
        """
        Create a copy of a dictionary.
        
        Args:
            obj: Dictionary to copy
            deep: If True, create deep copy of values; shallow otherwise
            items: If True, copy the dict items; otherwise return empty dict
        
        Returns:
            Copy of the dictionary
        
        Example:
            >>> CopyAction.copy_dict({'a': 1, 'b': 2})
            {'a': 1, 'b': 2}
        """
        if items:
            if deep:
                return copy.deepcopy(obj)
            return dict(obj)
        return {}
    
    @staticmethod
    def copy_set(obj: Set[Any], *, deep: bool = True) -> Set[Any]:
        """
        Create a copy of a set.
        
        Args:
            obj: Set to copy
            deep: If True, create deep copy of items; shallow otherwise
        
        Returns:
            Copy of the set
        
        Example:
            >>> CopyAction.copy_set({1, 2, 3})
            {1, 2, 3}
        """
        if deep:
            return copy.deepcopy(obj)
        return set(obj)
    
    @staticmethod
    def copy_tuple(obj: Tuple[Any, ...], *, deep: bool = True) -> Tuple[Any, ...]:
        """
        Create a copy of a tuple.
        
        Args:
            obj: Tuple to copy
            deep: If True, create deep copy of items; shallow otherwise
        
        Returns:
            Copy of the tuple
        
        Example:
            >>> CopyAction.copy_tuple((1, 2, 3))
            (1, 2, 3)
        """
        if deep:
            return copy.deepcopy(obj)
        return tuple(obj)
    
    @staticmethod
    def copy_object(
        obj: Any,
        *,
        deep: bool = True,
        attributes: Optional[List[str]] = None,
    ) -> Any:
        """
        Create a copy of a custom object.
        
        Args:
            obj: Object to copy
            deep: If True, create deep copy; shallow otherwise
            attributes: List of attribute names to copy (None for all)
        
        Returns:
            Copy of the object
        
        Example:
            >>> class MyClass:
            ...     def __init__(self, x):
            ...         self.x = x
            >>> obj = MyClass(10)
            >>> copied = CopyAction.copy_object(obj)
            >>> copied.x
            10
        """
        if deep:
            result = copy.deepcopy(obj)
        else:
            result = copy.copy(obj)
        
        if attributes is not None:
            for attr in attributes:
                if hasattr(result, attr):
                    value = getattr(obj, attr)
                    setattr(result, attr, value if deep else value)
        
        return result
    
    @staticmethod
    def register_custom_copier(
        cls: Type,
        copier: Callable[[Any, Dict[int, Any]], Any],
        *,
        deep: bool = True,
    ) -> None:
        """
        Register a custom copier function for a class.
        
        Args:
            cls: Class to register copier for
            copier: Function that takes (obj, memo) and returns copy
            deep: If True, register for deepcopy; otherwise shallow
        
        Example:
            >>> def custom_copier(obj, memo):
            ...     return MyClass(obj.x * 2)
            >>> CopyAction.register_custom_copier(MyClass, custom_copier)
        """
        if deep:
            copy.deepcopy._copy_dispatch[cls] = copier  # type: ignore
        else:
            copy.copy._copy_dispatch[cls] = copier  # type: ignore
    
    @staticmethod
    def copy_with_circular_ref(obj: Any) -> Any:
        """
        Create a deep copy that handles circular references.
        
        Uses memoization to track already-copied objects and prevent
        infinite recursion when objects reference each other.
        
        Args:
            obj: Object to copy
        
        Returns:
            Deep copy with circular references preserved
        
        Example:
            >>> a = [1, 2]
            >>> b = [a, a]  # circular reference
            >>> copied = CopyAction.copy_with_circular_ref(b)
            >>> copied[0] is copied[1]
            True
        """
        memo: Dict[int, Any] = {}
        return copy.deepcopy(obj, memo)
    
    @staticmethod
    def is_same_object(obj1: Any, obj2: Any) -> bool:
        """
        Check if two objects are the same object (same memory address).
        
        Args:
            obj1: First object
            obj2: Second object
        
        Returns:
            True if same object, False otherwise
        
        Example:
            >>> a = [1, 2]
            >>> b = a
            >>> CopyAction.is_same_object(a, b)
            True
            >>> CopyAction.is_same_object(a, [1, 2])
            False
        """
        return obj1 is obj2
    
    @staticmethod
    def is_shallow_copy(obj1: Any, obj2: Any) -> bool:
        """
        Check if obj2 is a shallow copy of obj1.
        
        Args:
            obj1: Original object
            obj2: Potential shallow copy
        
        Returns:
            True if obj2 is shallow copy, False otherwise
        
        Note:
            This is a best-effort heuristic and may not be accurate
            for all object types.
        """
        if type(obj1) != type(obj2):
            return False
        if obj1 == obj2 and obj1 is not obj2:
            # Same values but different identity suggests a copy
            return True
        return False
    
    @staticmethod
    def is_deep_copy(obj1: Any, obj2: Any) -> bool:
        """
        Check if obj2 is a deep copy of obj1.
        
        Args:
            obj1: Original object
            obj2: Potential deep copy
        
        Returns:
            True if obj2 is deep copy, False otherwise
        
        Note:
            This is a best-effort heuristic and may not be accurate
            for all object types.
        """
        if type(obj1) != type(obj2):
            return False
        if obj1 == obj2 and obj1 is not obj2:
            # For deep copy, also check nested objects
            if hasattr(obj1, "__dict__"):
                return obj1.__dict__ == obj2.__dict__
            return True
        return False
    
    @staticmethod
    def copy_nested(
        obj: Any,
        depths: Dict[str, int],
        *,
        current_depth: int = 0,
    ) -> Any:
        """
        Copy an object with configurable depth per path.
        
        Args:
            obj: Object to copy
            depths: Dictionary mapping type names or paths to copy depth
            current_depth: Current recursion depth
        
        Returns:
            Partially copied object
        
        Example:
            >>> CopyAction.copy_nested(
            ...     {'a': [1, [2, 3]]},
            ...     {'list': 1}  # Shallow copy lists (depth 1)
            ... )
        """
        if current_depth >= 10:  # Prevent infinite recursion
            return obj
        
        depth_key = type(obj).__name__
        max_depth = depths.get(depth_key, float('inf'))
        
        if current_depth >= max_depth:
            return copy.copy(obj)
        
        if isinstance(obj, dict):
            return {
                k: CopyAction.copy_nested(v, depths, current_depth + 1)
                for k, v in obj.items()
            }
        elif isinstance(obj, (list, tuple)):
            converted = (
                CopyAction.copy_nested(item, depths, current_depth + 1)
                for item in obj
            )
            return type(obj)(converted)
        elif isinstance(obj, set):
            return {
                CopyAction.copy_nested(item, depths, current_depth + 1)
                for item in obj
            }
        else:
            return copy.deepcopy(obj)
    
    @staticmethod
    def clone_instance(
        obj: Any,
        *,
        deep: bool = True,
        override: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Clone an instance of a class, optionally overriding attributes.
        
        Args:
            obj: Instance to clone
            deep: If True, deep copy attribute values
            override: Dictionary of attribute values to override
        
        Returns:
            New instance with copied/overridden attributes
        
        Example:
            >>> class MyClass:
            ...     def __init__(self, x, y):
            ...         self.x = x
            ...         self.y = y
            >>> obj = MyClass(1, 2)
            >>> cloned = CopyAction.clone_instance(obj, override={'x': 10})
            >>> cloned.x
            10
            >>> cloned.y
            2
        """
        override = override or {}
        
        # Create new instance
        try:
            new_obj = object.__new__(type(obj))
        except TypeError:
            # Fallback for classes without __new__
            return copy.deepcopy(obj)
        
        # Copy attributes
        if deep:
            attrs = copy.deepcopy(vars(obj))
        else:
            attrs = dict(vars(obj))
        
        # Apply overrides
        attrs.update(override)
        
        # Set attributes
        for k, v in attrs.items():
            setattr(new_obj, k, v)
        
        return new_obj
    
    @staticmethod
    def freeze(obj: Any) -> Any:
        """
        Create a frozen (immutable) deep copy of an object.
        
        For mutable objects, returns an immutable equivalent.
        Lists become tuples, dicts become MappingProxyType, etc.
        
        Args:
            obj: Object to freeze
        
        Returns:
            Immutable copy of the object
        
        Example:
            >>> CopyAction.freeze([1, [2, 3]])
            (1, (2, 3))
        """
        if isinstance(obj, dict):
            return types.MappingProxyType({
                k: CopyAction.freeze(v) for k, v in obj.items()
            })
        elif isinstance(obj, list):
            return tuple(CopyAction.freeze(item) for item in obj)
        elif isinstance(obj, set):
            return frozenset(CopyAction.freeze(item) for item in obj)
        elif isinstance(obj, bytearray):
            return bytes(obj)
        else:
            return copy.deepcopy(obj)
    
    @staticmethod
    def copy_by_type(
        obj: Any,
        *,
        deep: bool = True,
        custom_handlers: Optional[Dict[Type, Callable[[Any], Any]]] = None,
    ) -> Any:
        """
        Copy an object using type-specific handlers.
        
        Args:
            obj: Object to copy
            deep: Default copy mode
            custom_handlers: Dict mapping types to custom copier functions
        
        Returns:
            Copy of the object
        
        Example:
            >>> handlers = {MyClass: lambda x: MyClass(x.value * 2)}
            >>> CopyAction.copy_by_type(obj, custom_handlers=handlers)
        """
        custom_handlers = custom_handlers or {}
        
        obj_type = type(obj)
        
        # Check for custom handler
        for cls, handler in custom_handlers.items():
            if isinstance(obj, cls):
                return handler(obj)
        
        # Default behavior
        if deep:
            return copy.deepcopy(obj)
        return copy.copy(obj)
    
    @staticmethod
    def batch_copy(
        objects: List[Any],
        *,
        deep: bool = True,
    ) -> List[Any]:
        """
        Copy multiple objects in batch.
        
        Args:
            objects: List of objects to copy
            deep: If True, deep copy; otherwise shallow
        
        Returns:
            List of copied objects
        
        Example:
            >>> a, b = [1, 2], {'x': 3}
            >>> c, d = CopyAction.batch_copy([a, b])
            >>> c is a
            False
            >>> d is b
            False
        """
        if deep:
            return [copy.deepcopy(obj) for obj in objects]
        return [copy.copy(obj) for obj in objects]
    
    @staticmethod
    def compare_copies(
        original: Any,
        shallow: Any,
        deep: Any,
    ) -> Dict[str, bool]:
        """
        Compare shallow and deep copies against the original.
        
        Args:
            original: Original object
            shallow: Shallow copy
            deep: Deep copy
        
        Returns:
            Dictionary with comparison results
        
        Example:
            >>> original = [1, [2, 3]]
            >>> shallow = copy.copy(original)
            >>> deep = copy.deepcopy(original)
            >>> CopyAction.compare_copies(original, shallow, deep)
            {'shallow_is_copy': True, 'deep_is_copy': True, 
             'shallow_matches': True, 'deep_matches': True}
        """
        return {
            "shallow_is_copy": shallow is not original,
            "deep_is_copy": deep is not original,
            "shallow_matches": shallow == original,
            "deep_matches": deep == original,
            "shallow_same_type": type(shallow) == type(original),
            "deep_same_type": type(deep) == type(original),
        }


# Convenience function aliases
def shallow_copy(obj: T) -> T:
    """Create a shallow copy of an object."""
    return CopyAction.shallow_copy(obj)


def deep_copy(obj: T, **kwargs: Any) -> T:
    """Create a deep copy of an object."""
    return CopyAction.deep_copy(obj, **kwargs)


def clone(obj: Any, **kwargs: Any) -> Any:
    """Clone an object (alias for deep_copy)."""
    return CopyAction.deep_copy(obj)


# Module metadata
__author__ = "AI Assistant"
__version__ = "1.0.0"
__all__ = [
    "CopyAction",
    "shallow_copy",
    "deep_copy",
    "clone",
]
