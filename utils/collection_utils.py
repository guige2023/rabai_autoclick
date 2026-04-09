"""Collection and data structure utilities.

Provides specialized collection types, transformations,
and operations for lists, dicts, and nested structures.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generator, Iterator, List, Optional, Tuple, TypeVar, Union


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class TwoWayDict(dict):
    """Dictionary with bidirectional lookup.

    Example:
        d = TwoWayDict({"red": "rot", "green": "gruen"})
        d["red"]  # "rot"
        d["rot"]  # "red"
    """

    def __setitem__(self, key: Any, value: Any) -> None:
        dict.__setitem__(self, key, value)
        dict.__setitem__(self, value, key)

    def __delitem__(self, key: Any) -> None:
        value = self[key]
        dict.__delitem__(self, key)
        dict.__delitem__(self, value)

    def __setitem__(self, key: Any, value: Any) -> None:
        if key in self:
            del self[self[key]]
        if value in self:
            del self[value]
        dict.__setitem__(self, key, value)
        dict.__setitem__(self, value, key)


class OrderedDefaultDict(defaultdict):
    """Default dict that maintains insertion order.

    Example:
        d = OrderedDefaultDict(list)
        d["items"].append(1)
    """

    def __init__(self, default_factory: Callable = None, **kwargs: Any) -> None:
        defaultdict.__init__(self, default_factory, **kwargs)
        self._order: List = []

    def __setitem__(self, key: Any, value: Any) -> None:
        if key not in self:
            self._order.append(key)
        defaultdict.__setitem__(self, key, value)

    def __delitem__(self, key: Any) -> None:
        if key in self:
            self._order.remove(key)
        defaultdict.__delitem__(self, key)

    def order(self) -> List:
        """Return keys in insertion order."""
        return list(self._order)


@dataclass
class TreeNode(Generic[T]):
    """Tree node with children."""
    value: T
    children: List["TreeNode[T]"] = field(default_factory=list)
    parent: Optional["TreeNode[T]"] = None

    def add_child(self, value: T) -> "TreeNode[T]":
        """Add child node."""
        child = TreeNode(value=value, parent=self)
        self.children.append(child)
        return child

    def is_leaf(self) -> bool:
        """Check if node is leaf."""
        return len(self.children) == 0

    def depth(self) -> int:
        """Get depth of node."""
        d = 0
        node = self
        while node.parent:
            d += 1
            node = node.parent
        return d


class Tree(Generic[T]):
    """Tree data structure.

    Example:
        tree = Tree("root")
        child = tree.root.add_child("child")
        grandchild = child.add_child("grandchild")
        for node in tree:
            print(node.value)
    """

    def __init__(self, root_value: T) -> None:
        self.root = TreeNode(root_value)

    def __iter__(self) -> Generator[TreeNode[T], None, None]:
        """Iterate over all nodes in tree."""
        def traverse(node: TreeNode[T]) -> Generator[TreeNode[T], None, None]:
            yield node
            for child in node.children:
                yield from traverse(child)

        yield from traverse(self.root)

    def find(self, predicate: Callable[[T], bool]) -> Optional[TreeNode[T]]:
        """Find node where predicate returns True."""
        for node in self:
            if predicate(node.value):
                return node
        return None


class Stack(List[T]):
    """Stack implementation using list.

    Example:
        stack = Stack()
        stack.push(1)
        stack.push(2)
        stack.pop()  # 2
    """

    def push(self, item: T) -> None:
        """Push item onto stack."""
        self.append(item)

    def pop(self) -> T:
        """Pop item from stack."""
        return list.pop(self)

    def peek(self) -> T:
        """Get top item without removing."""
        return self[-1]

    def is_empty(self) -> bool:
        """Check if stack is empty."""
        return len(self) == 0


class Queue(List[T]):
    """Queue implementation using list.

    Example:
        queue = Queue()
        queue.enqueue(1)
        queue.enqueue(2)
        queue.dequeue()  # 1
    """

    def enqueue(self, item: T) -> None:
        """Add item to queue."""
        self.append(item)

    def dequeue(self) -> T:
        """Remove and return first item."""
        return self.pop(0)

    def peek(self) -> T:
        """Get first item without removing."""
        return self[0]

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self) == 0


def flatten(nested: List[Any], depth: Optional[int] = None) -> List[Any]:
    """Flatten nested list.

    Example:
        flatten([1, [2, [3, 4]], 5])  # [1, 2, 3, 4, 5]
    """
    result = []

    def _flatten(items: List[Any], current_depth: int = 0) -> None:
        for item in items:
            if isinstance(item, list) and (depth is None or current_depth < depth):
                _flatten(item, current_depth + 1)
            else:
                result.append(item)

    _flatten(nested)
    return result


def group_by_key(
    items: List[Dict[K, V]],
    key: str,
) -> Dict[K, List[Dict[K, V]]]:
    """Group items by key value.

    Example:
        group_by_key([{"type": "a", "v": 1}, {"type": "b", "v": 2}, {"type": "a", "v": 3}])
        # {"a": [{"type": "a", "v": 1}, {"type": "a", "v": 3}], "b": [{"type": "b", "v": 2}]}
    """
    groups: Dict[K, List[Dict[K, V]]] = defaultdict(list)
    for item in items:
        k = item.get(key)
        groups[k].append(item)
    return dict(groups)


def transpose(matrix: List[List[T]]) -> List[List[T]]:
    """Transpose matrix (swap rows and columns).

    Example:
        transpose([[1, 2], [3, 4]])  # [[1, 3], [2, 4]]
    """
    if not matrix:
        return []
    return list(map(list, zip(*matrix)))


def zip_longest(
    *iterables: List[T],
    fillvalue: Any = None,
) -> Iterator[List[T]]:
    """Zip iterables of different lengths.

    Example:
        list(zip_longest([1, 2], [3, 4, 5], fillvalue=0))
        # [[1, 3], [2, 4], [0, 5]]
    """
    from itertools import zip_longest
    return zip_longest(*iterables, fillvalue=fillvalue)


def get_in(
    nested: Dict[str, Any],
    path: List[str],
    default: Any = None,
) -> Any:
    """Get value from nested dict using path.

    Example:
        d = {"a": {"b": {"c": 1}}}
        get_in(d, ["a", "b", "c"])  # 1
        get_in(d, ["a", "x"], "default")  # "default"
    """
    current = nested
    for key in path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def set_in(
    nested: Dict[str, Any],
    path: List[str],
    value: Any,
) -> Dict[str, Any]:
    """Set value in nested dict creating path if needed.

    Example:
        d = {}
        set_in(d, ["a", "b", "c"], 1)
        # {"a": {"b": {"c": 1}}}
    """
    current = nested
    for key in path[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    current[path[-1]] = value
    return nested


def update_recursive(base: Dict, update: Dict) -> Dict:
    """Recursively update nested dict.

    Example:
        base = {"a": {"b": 1}}
        update = {"a": {"c": 2}}
        update_recursive(base, update)  # {"a": {"b": 1, "c": 2}}
    """
    for key, value in update.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            base[key] = update_recursive(base[key], value)
        else:
            base[key] = value
    return base


def deep_merge(*dicts: Dict) -> Dict:
    """Deep merge multiple dicts.

    Example:
        deep_merge({"a": 1}, {"b": 2}, {"a": 3})
        # {"a": 3, "b": 2}
    """
    result = {}
    for d in dicts:
        result = update_recursive(result, d)
    return result


def list_to_dict(
    items: List[T],
    key_func: Callable[[T], K],
) -> Dict[K, T]:
    """Convert list to dict using key function.

    Example:
        list_to_dict(["apple", "banana"], len)
        # {5: "apple", 6: "banana"}
    """
    return {key_func(item): item for item in items}


def dict_from_tuples(
    tuples: List[Tuple[K, V]],
) -> Dict[K, V]:
    """Create dict from list of tuples.

    Example:
        dict_from_tuples([("a", 1), ("b", 2)])
        # {"a": 1, "b": 2}
    """
    return dict(tuples)


def invert_dict(d: Dict[K, V]) -> Dict[V, K]:
    """Invert dict keys and values.

    Example:
        invert_dict({"a": 1, "b": 2})
        # {1: "a", 2: "b"}
    """
    return {v: k for k, v in d.items()}


def filter_dict(
    d: Dict[K, V],
    predicate: Callable[[K, V], bool],
) -> Dict[K, V]:
    """Filter dict by predicate.

    Example:
        filter_dict({"a": 1, "b": 2, "c": 3}, lambda k, v: v > 1)
        # {"b": 2, "c": 3}
    """
    return {k: v for k, v in d.items() if predicate(k, v)}


def map_dict(
    d: Dict[K, V],
    func: Callable[[K, V], Tuple[Any, Any]],
) -> Dict:
    """Map dict entries to new dict.

    Example:
        map_dict({"a": 1, "b": 2}, lambda k, v: (k.upper(), v * 2))
        # {"A": 2, "B": 4}
    """
    return dict(func(k, v) for k, v in d.items())


from typing import Generic, TypeVar
