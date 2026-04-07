"""
Weakref Action Module

Provides weak reference caching and management utilities with support
for various cache strategies including LRU, TTL, and size-limited caches.

Author: AI Assistant
Version: 1.0.0
"""

from __future__ import annotations

import threading
import time
import weakref
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

# Type variables
K = TypeVar("K")
V = TypeVar("V")
T = TypeVar("T")


class WeakRefCache(Generic[K, V]):
    """
    A cache that holds weak references to values.
    
    Values are eligible for garbage collection when no other
    strong references exist outside the cache.
    
    Attributes:
        max_size: Maximum number of entries (0 for unlimited)
    """
    
    def __init__(
        self,
        max_size: int = 0,
        *,
        on_evict: Optional[Callable[[K, V], None]] = None,
    ) -> None:
        """
        Initialize the weak ref cache.
        
        Args:
            max_size: Maximum entries (0 for unlimited)
            on_evict: Optional callback when entries are evicted
        """
        self._data: weakref.WeakValueDictionary[K, V] = weakref.WeakValueDictionary()
        self._keys: List[K] = []
        self._max_size = max_size
        self._on_evict = on_evict
        self._lock = threading.RLock()
    
    def get(self, key: K) -> Optional[V]:
        """
        Get a value from the cache.
        
        Args:
            key: Key to look up
        
        Returns:
            Value if found, None otherwise
        """
        with self._lock:
            return self._data.get(key)
    
    def set(self, key: K, value: V) -> None:
        """
        Set a value in the cache.
        
        Args:
            key: Key to store
            value: Value to store (must be weakly referenceable)
        """
        with self._lock:
            # Evict oldest if at capacity
            if self._max_size > 0 and len(self._keys) >= self._max_size:
                oldest = self._keys.pop(0)
                old_value = self._data.pop(oldest, None)
                if self._on_evict and old_value is not None:
                    self._on_evict(oldest, old_value)
            
            self._data[key] = value
            if key not in self._keys:
                self._keys.append(key)
    
    def delete(self, key: K) -> bool:
        """
        Delete a key from the cache.
        
        Args:
            key: Key to delete
        
        Returns:
            True if key was found and deleted
        """
        with self._lock:
            if key in self._data:
                old_value = self._data.pop(key, None)
                if key in self._keys:
                    self._keys.remove(key)
                if self._on_evict and old_value is not None:
                    self._on_evict(key, old_value)
                return True
            return False
    
    def clear(self) -> None:
        """Clear all entries from the cache."""
        with self._lock:
            if self._on_evict:
                for key in self._keys:
                    value = self._data.get(key)
                    if value is not None:
                        self._on_evict(key, value)
            self._data.clear()
            self._keys.clear()
    
    def size(self) -> int:
        """Return the number of entries in the cache."""
        with self._lock:
            return len(self._keys)
    
    def contains(self, key: K) -> bool:
        """Check if a key exists in the cache."""
        with self._lock:
            return key in self._data
    
    def keys(self) -> List[K]:
        """Return a list of all keys."""
        with self._lock:
            return list(self._keys)
    
    def values(self) -> List[V]:
        """Return a list of all values."""
        with self._lock:
            return [self._data[k] for k in self._keys if k in self._data]
    
    def items(self) -> List[Tuple[K, V]]:
        """Return a list of all (key, value) pairs."""
        with self._lock:
            return [
                (k, self._data[k]) for k in self._keys if k in self._data
            ]


class WeakRefLRUCache(WeakRefCache[K, V]):
    """
    LRU cache with weak references.
    
    Most recently accessed items are kept, oldest are evicted first.
    """
    
    def get(self, key: K) -> Optional[V]:
        """Get and move key to end (most recent)."""
        with self._lock:
            value = self._data.get(key)
            if value is not None and key in self._keys:
                self._keys.remove(key)
                self._keys.append(key)
            return value


class WeakRefTTLCache(WeakRefCache[K, V]):
    """
    Cache with Time-To-Live (TTL) expiration.
    
    Entries expire after a specified duration.
    """
    
    def __init__(
        self,
        ttl: float,
        max_size: int = 0,
        *,
        on_evict: Optional[Callable[[K, V], None]] = None,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        """
        Initialize the TTL cache.
        
        Args:
            ttl: Time-to-live in seconds
            max_size: Maximum entries (0 for unlimited)
            on_evict: Optional callback when entries are evicted
            clock: Optional time function (default: time.time)
        """
        super().__init__(max_size, on_evict=on_evict)
        self._ttl = ttl
        self._timestamps: Dict[K, float] = {}
        self._clock = clock or time.time
    
    def set(self, key: K, value: V) -> None:
        """Set a value with current timestamp."""
        with self._lock:
            super().set(key, value)
            self._timestamps[key] = self._clock()
    
    def get(self, key: K) -> Optional[V]:
        """Get value if not expired."""
        with self._lock:
            if key not in self._timestamps:
                return None
            
            age = self._clock() - self._timestamps[key]
            if age > self._ttl:
                # Expired - remove
                self.delete(key)
                return None
            
            return self._data.get(key)
    
    def delete(self, key: K) -> bool:
        """Delete and clean up timestamp."""
        with self._lock:
            self._timestamps.pop(key, None)
            return super().delete(key)
    
    def clear(self) -> None:
        """Clear all entries including timestamps."""
        with self._lock:
            self._timestamps.clear()
            super().clear()
    
    def cleanup_expired(self) -> int:
        """
        Remove all expired entries.
        
        Returns:
            Number of entries removed
        """
        with self._lock:
            now = self._clock()
            expired_keys = [
                k for k, ts in list(self._timestamps.items())
                if now - ts > self._ttl
            ]
            for key in expired_keys:
                self.delete(key)
            return len(expired_keys)


class WeakRefLinkedCache(WeakRefCache[K, V]):
    """
    Doubly-linked cache with weak references.
    
    Provides O(1) access and O(1) insertion/deletion.
    """
    
    def __init__(
        self,
        max_size: int = 0,
        *,
        on_evict: Optional[Callable[[K, V], None]] = None,
    ) -> None:
        super().__init__(max_size, on_evict=on_evict)
        self._head: Optional[_Node[K, V]] = None
        self._tail: Optional[_Node[K, V]] = None
        self._node_map: Dict[K, _Node[K, V]] = {}
    
    def set(self, key: K, value: V) -> None:
        """Set value and move to front."""
        with self._lock:
            if key in self._node_map:
                node = self._node_map[key]
                node.value = value
                self._move_to_front(node)
            else:
                node = _Node(key, value)
                self._node_map[key] = node
                self._add_to_front(node)
                
                if self._max_size > 0 and len(self._node_map) > self._max_size:
                    self._evict_lru()
            
            super().set(key, value)
    
    def get(self, key: K) -> Optional[V]:
        """Get value and move to front."""
        with self._lock:
            node = self._node_map.get(key)
            if node is None:
                return None
            self._move_to_front(node)
            return node.value
    
    def delete(self, key: K) -> bool:
        """Remove a key from the cache."""
        with self._lock:
            if key not in self._node_map:
                return False
            node = self._node_map.pop(key)
            self._remove_node(node)
            return super().delete(key)
    
    def _add_to_front(self, node: _Node[K, V]) -> None:
        node.prev = None
        node.next = self._head
        if self._head:
            self._head.prev = node
        self._head = node
        if self._tail is None:
            self._tail = node
    
    def _remove_node(self, node: _Node[K, V]) -> None:
        if node.prev:
            node.prev.next = node.next
        else:
            self._head = node.next
        if node.next:
            node.next.prev = node.prev
        else:
            self._tail = node.prev
    
    def _move_to_front(self, node: _Node[K, V]) -> None:
        if node is self._head:
            return
        self._remove_node(node)
        self._add_to_front(node)
    
    def _evict_lru(self) -> None:
        if self._tail:
            key = self._tail.key
            self._remove_node(self._tail)
            self._node_map.pop(key, None)
            super().delete(key)


class _Node(Generic[K, V]):
    """Node for doubly-linked list."""
    
    __slots__ = ("key", "value", "prev", "next")
    
    def __init__(
        self,
        key: K,
        value: V,
    ) -> None:
        self.key = key
        self.value = value
        self.prev: Optional[_Node[K, V]] = None
        self.next: Optional[_Node[K, V]] = None


class WeakRefAction:
    """
    Main weakref action handler providing weak reference utilities.
    
    This class provides static methods for working with weak references
    and creating various types of weak reference caches.
    
    Attributes:
        None (all methods are static or class methods)
    """
    
    @staticmethod
    def create_weakref(
        obj: Any,
        *,
        callback: Optional[Callable[[weakref.ref], None]] = None,
    ) -> weakref.ref:
        """
        Create a weak reference to an object.
        
        Args:
            obj: Object to reference
            callback: Optional finalizer callback
        
        Returns:
            Weak reference to the object
        
        Example:
            >>> import sys
            >>> data = [1, 2, 3]
            >>> ref = WeakRefAction.create_weakref(data)
            >>> ref() is data
            True
            >>> del data
            >>> ref() is None
            True
        """
        if callback is not None:
            return weakref.ref(obj, callback)
        return weakref.ref(obj)
    
    @staticmethod
    def get_weakref(obj: Any) -> Optional[weakref.ref]:
        """
        Get a weak reference to an object if it supports weakref.
        
        Args:
            obj: Object to reference
        
        Returns:
            Weak reference or None if object doesn't support it
        
        Example:
            >>> WeakRefAction.get_weakref([1,2,3]) is not None
            True
        """
        try:
            return weakref.ref(obj)
        except TypeError:
            return None
    
    @staticmethod
    def is_weakref(value: Any) -> bool:
        """
        Check if a value is a weak reference.
        
        Args:
            value: Value to check
        
        Returns:
            True if value is a weak reference
        
        Example:
            >>> ref = weakref.ref([1,2,3])
            >>> WeakRefAction.is_weakref(ref)
            True
        """
        return isinstance(value, weakref.ref)
    
    @staticmethod
    def get_object(weakref_obj: weakref.ref) -> Any:
        """
        Get the object referenced by a weak reference.
        
        Args:
            weakref_obj: Weak reference
        
        Returns:
            Referenced object or None if it was garbage collected
        
        Example:
            >>> data = [1, 2, 3]
            >>> ref = weakref.ref(data)
            >>> WeakRefAction.get_object(ref)
            [1, 2, 3]
        """
        return weakref_obj()
    
    @staticmethod
    def create_cache(
        max_size: int = 128,
        *,
        on_evict: Optional[Callable[[Any, Any], None]] = None,
    ) -> WeakRefCache:
        """
        Create a new weak reference cache.
        
        Args:
            max_size: Maximum number of entries (0 for unlimited)
            on_evict: Optional callback when entries are evicted
        
        Returns:
            New WeakRefCache instance
        
        Example:
            >>> cache = WeakRefAction.create_cache(max_size=100)
            >>> cache.set("key", [1, 2, 3])
            >>> cache.get("key")
            [1, 2, 3]
        """
        return WeakRefCache(max_size=max_size, on_evict=on_evict)
    
    @staticmethod
    def create_lru_cache(
        max_size: int = 128,
        *,
        on_evict: Optional[Callable[[Any, Any], None]] = None,
    ) -> WeakRefLRUCache:
        """
        Create a new LRU weak reference cache.
        
        Args:
            max_size: Maximum number of entries
            on_evict: Optional callback when entries are evicted
        
        Returns:
            New WeakRefLRUCache instance
        """
        return WeakRefLRUCache(max_size=max_size, on_evict=on_evict)
    
    @staticmethod
    def create_ttl_cache(
        ttl: float,
        max_size: int = 128,
        *,
        on_evict: Optional[Callable[[Any, Any], None]] = None,
    ) -> WeakRefTTLCache:
        """
        Create a new TTL weak reference cache.
        
        Args:
            ttl: Time-to-live in seconds
            max_size: Maximum number of entries
            on_evict: Optional callback when entries are evicted
        
        Returns:
            New WeakRefTTLCache instance
        """
        return WeakRefTTLCache(
            ttl=ttl,
            max_size=max_size,
            on_evict=on_evict,
        )
    
    @staticmethod
    def create_linked_cache(
        max_size: int = 128,
        *,
        on_evict: Optional[Callable[[Any, Any], None]] = None,
    ) -> WeakRefLinkedCache:
        """
        Create a new doubly-linked weak reference cache.
        
        Args:
            max_size: Maximum number of entries
            on_evict: Optional callback when entries are evicted
        
        Returns:
            New WeakRefLinkedCache instance
        """
        return WeakRefLinkedCache(max_size=max_size, on_evict=on_evict)
    
    @staticmethod
    def memoize(
        func: Optional[Callable[..., V]] = None,
        *,
        max_size: int = 128,
        ttl: Optional[float] = None,
    ) -> Callable[..., V]:
        """
        Decorator to memoize a function with weak reference caching.
        
        Args:
            func: Function to memoize (or None when used as decorator)
            max_size: Maximum cache size
            ttl: Optional time-to-live in seconds
        
        Returns:
            Decorated function
        
        Example:
            >>> @WeakRefAction.memoize(max_size=256)
            ... def expensive_computation(x):
            ...     return x * x
        """
        if ttl is not None:
            cache: WeakRefCache = WeakRefTTLCache(ttl=ttl, max_size=max_size)
        else:
            cache = WeakRefLRUCache(max_size=max_size)
        
        def decorator(f: Callable[..., V]) -> Callable[..., V]:
            def wrapper(*args: Any, **kwargs: Any) -> V:
                # Create a cache key from args and kwargs
                key = (args, tuple(sorted(kwargs.items())))
                
                result = cache.get(key)  # type: ignore
                if result is not None:
                    return result
                
                result = f(*args, **kwargs)
                cache.set(key, result)  # type: ignore
                return result
            
            wrapper.cache_clear = cache.clear  # type: ignore
            wrapper.cache_info = lambda: {  # type: ignore
                "size": cache.size(),
                "keys": cache.keys(),
            }
            return wrapper
        
        if func is not None:
            return decorator(func)
        return decorator
    
    @staticmethod
    def finalize(
        obj: Any,
        callback: Callable[..., None],
        *args: Any,
        **kwargs: Any,
    ) -> weakref.finalize:
        """
        Register a finalizer for an object.
        
        Args:
            obj: Object to finalize
            callback: Function to call when object is garbage collected
            *args: Positional arguments for callback
            **kwargs: Keyword arguments for callback
        
        Returns:
            weakref.finalize instance
        
        Example:
            >>> def cleanup():
            ...     print("Object cleaned up!")
            >>> finalizer = WeakRefAction.finalize(my_obj, cleanup)
        """
        return weakref.finalize(obj, callback, *args, **kwargs)


# Module metadata
__author__ = "AI Assistant"
__version__ = "1.0.0"
__all__ = [
    "WeakRefCache",
    "WeakRefLRUCache",
    "WeakRefTTLCache",
    "WeakRefLinkedCache",
    "WeakRefAction",
]
