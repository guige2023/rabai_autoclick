"""Copy utilities v2 - advanced copying patterns.

Extended copy utilities including circular reference
 handling, object registries, and memoization.
"""

from __future__ import annotations

import copy
from copy import deepcopy, copy as shallow_copy
from typing import Any, Callable, Generic, TypeVar

__all__ = [
    "deep_cached_copy",
    "shallow_cached_copy",
    "copy_with",
    "replace",
    "update_copy",
    "copy_tree",
    "copy_graph",
    "CircularRefHandler",
    "ObjectRegistry",
    "CloneRegistry",
    "IdentityCache",
    "memoized_copy",
    "copy_frozen",
    "copy_immutable",
    "copy_dataclass",
    "copy_persistent",
    "Copier",
]


T = TypeVar("T")


def deep_cached_copy(obj: T, cache: dict[int, T] | None = None) -> T:
    """Deep copy with circular reference detection.

    Args:
        obj: Object to copy.
        cache: Optional cache for memoization.

    Returns:
        Deep copy of object.
    """
    if cache is None:
        cache = {}
    obj_id = id(obj)
    if obj_id in cache:
        return cache[obj_id]
    result = deepcopy(obj, memo=cache)
    return result


def shallow_cached_copy(obj: T, cache: dict[int, Any] | None = None) -> T:
    """Shallow copy with circular reference detection.

    Args:
        obj: Object to copy.
        cache: Optional cache.

    Returns:
        Shallow copy.
    """
    if cache is None:
        cache = {}
    obj_id = id(obj)
    if obj_id in cache:
        return cache[obj_id]
    result = shallow_copy(obj)
    cache[obj_id] = result
    return result


def copy_with(obj: T, **changes: Any) -> T:
    """Copy object with attribute changes.

    Args:
        obj: Object to copy.
        **changes: Attributes to change.

    Returns:
        New object with changes.
    """
    cls = obj.__class__
    if hasattr(obj, "__dict__"):
        new_obj = cls.__new__(cls)
        new_obj.__dict__.update(obj.__dict__)
        new_obj.__dict__.update(changes)
        return new_obj
    return obj


def replace(obj: T, **changes: Any) -> T:
    """Replace attributes (dataclass style).

    Args:
        obj: Object to copy.
        **changes: Attributes to replace.

    Returns:
        New object.
    """
    return copy_with(obj, **changes)


def update_copy(obj: T, updates: dict[str, Any]) -> T:
    """Copy object with dict of updates.

    Args:
        obj: Object to copy.
        updates: Dict of attribute updates.

    Returns:
        New object.
    """
    return copy_with(obj, **updates)


def copy_tree(root: Any, children_fn: Callable[[Any], list[Any]], copy_fn: Callable[[Any], Any] | None = None) -> Any:
    """Deep copy a tree structure.

    Args:
        root: Root node.
        children_fn: Function to get children.
        copy_fn: Optional custom copy function.

    Returns:
        Deep copied tree.
    """
    if copy_fn is None:
        copy_fn = lambda x: copy_with(x)
    new_node = copy_fn(root)
    for child in children_fn(root):
        copied_child = copy_tree(child, children_fn, copy_fn)
    return new_node


def copy_graph(node: Any, neighbors_fn: Callable[[Any], list[Any]], cache: dict[int, Any] | None = None) -> Any:
    """Deep copy a graph (handles cycles).

    Args:
        node: Starting node.
        neighbors_fn: Function to get neighbors.
        cache: Memoization cache.

    Returns:
        Deep copied graph.
    """
    if cache is None:
        cache = {}
    node_id = id(node)
    if node_id in cache:
        return cache[node_id]
    new_node = copy_with(node)
    cache[node_id] = new_node
    for neighbor in neighbors_fn(node):
        copy_graph(neighbor, neighbors_fn, cache)
    return new_node


class CircularRefHandler(copy.deepcopy.__class__):
    """Handle circular references during deep copy."""

    def __init__(self) -> None:
        self._memo: dict[int, Any] = {}

    def copy(self, obj: Any) -> Any:
        """Copy with circular ref handling.

        Args:
            obj: Object to copy.

        Returns:
            Deep copy.
        """
        return deepcopy(obj, memo=self._memo)

    def reset(self) -> None:
        """Clear memo cache."""
        self._memo.clear()


class ObjectRegistry(Generic[T]):
    """Registry for tracking object copies."""

    def __init__(self) -> None:
        self._original_to_copy: dict[int, T] = {}
        self._copy_to_original: dict[int, T] = {}

    def register(self, original: T, copy: T) -> None:
        """Register original-copy pair.

        Args:
            original: Original object.
            copy: Copy of object.
        """
        self._original_to_copy[id(original)] = copy
        self._copy_to_original[id(copy)] = original

    def get_copy(self, original: T) -> T | None:
        """Get copy for original."""
        return self._original_to_copy.get(id(original))

    def get_original(self, copy: T) -> T | None:
        """Get original for copy."""
        return self._copy_to_original.get(id(copy))

    def is_registered(self, obj: T) -> bool:
        """Check if object is registered."""
        return id(obj) in self._original_to_copy or id(obj) in self._copy_to_original

    def clear(self) -> None:
        """Clear registry."""
        self._original_to_copy.clear()
        self._copy_to_original.clear()


class CloneRegistry(ObjectRegistry[T]):
    """Registry for cloning operations."""

    def __init__(self) -> None:
        super().__init__()
        self._cloners: dict[type, Callable[[Any], Any]] = {}

    def register_cloner(self, cls: type, cloner: Callable[[Any], Any]) -> None:
        """Register a custom cloner for type.

        Args:
            cls: Class to handle.
            cloner: Function to clone instances.
        """
        self._cloners[cls] = cloner

    def clone(self, obj: T) -> T:
        """Clone object with registry.

        Args:
            obj: Object to clone.

        Returns:
            Clone of object.
        """
        existing = self.get_copy(obj)
        if existing is not None:
            return existing
        cls = obj.__class__
        if cls in self._cloners:
            new_obj = self._cloners[cls](obj)
        else:
            new_obj = deepcopy(obj)
        self.register(obj, new_obj)
        return new_obj


class IdentityCache(Generic[T]):
    """Cache that stores objects by identity."""

    def __init__(self, copier: Callable[[T], T] | None = None) -> None:
        self._cache: dict[int, T] = {}
        self._copier = copier or deepcopy

    def get(self, key: int) -> T | None:
        """Get cached value by key."""
        return self._cache.get(key)

    def put(self, key: int, value: T) -> None:
        """Store value by key."""
        self._cache[key] = value

    def cached_copy(self, obj: T) -> T:
        """Get or create cached copy.

        Args:
            obj: Object to copy.

        Returns:
            Cached copy.
        """
        key = id(obj)
        if key not in self._cache:
            self._cache[key] = self._copier(obj)
        return self._cache[key]

    def clear(self) -> None:
        """Clear cache."""
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)


def memoized_copy(obj: T, cache: dict[int, T] | None = None) -> T:
    """Memoized deep copy.

    Args:
        obj: Object to copy.
        cache: Optional memoization dict.

    Returns:
        Memoized copy.
    """
    if cache is None:
        cache = {}
    obj_id = id(obj)
    if obj_id in cache:
        return cache[obj_id]
    result = deepcopy(obj, memo=cache)
    cache[obj_id] = result
    return result


def copy_frozen(obj: Any) -> Any:
    """Copy frozen (immutable) object.

    Args:
        obj: Object to copy.

    Returns:
        Same object (frozen = unchanged).
    """
    return obj


def copy_immutable(obj: Any) -> Any:
    """Copy immutable object.

    Args:
        obj: Immutable object.

    Returns:
        Same object.
    """
    return obj


def copy_dataclass(obj: Any) -> Any:
    """Copy dataclass object.

    Args:
        obj: Dataclass instance.

    Returns:
        New dataclass instance.
    """
    if not hasattr(obj, "__dataclass_fields__"):
        raise TypeError(f"{obj} is not a dataclass")
    return deepcopy(obj)


def copy_persistent(obj: Any) -> Any:
    """Copy persistent object (ID-based).

    Args:
        obj: Persistent object.

    Returns:
        Same object.
    """
    return obj


class Copier(Generic[T]):
    """Configurable copier."""

    def __init__(self, deep: bool = True) -> None:
        self._deep = deep
        self._handlers: dict[type, Callable[[Any], Any]] = {}
        self._memo: dict[int, Any] = {}

    def register_handler(self, cls: type, handler: Callable[[Any], Any]) -> Copier[T]:
        """Register custom handler for type.

        Args:
            cls: Class to handle.
            handler: Copy handler function.

        Returns:
            Self.
        """
        self._handlers[cls] = handler
        return self

    def copy(self, obj: T) -> T:
        """Copy object.

        Args:
            obj: Object to copy.

        Returns:
            Copy of object.
        """
        obj_id = id(obj)
        if obj_id in self._memo:
            return self._memo[obj_id]
        cls = obj.__class__
        if cls in self._handlers:
            result = self._handlers[cls](obj)
        elif self._deep:
            result = deepcopy(obj, memo=self._memo)
        else:
            result = shallow_copy(obj)
        self._memo[obj_id] = result
        return result

    def reset(self) -> Copier[T]:
        """Reset memo cache.

        Returns:
            Self.
        """
        self._memo.clear()
        return self
