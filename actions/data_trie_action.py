"""
Data Trie Action Module.

Trie data structure for efficient prefix-based lookups,
supports autocomplete, prefix matching, and longest prefix match.
"""

from __future__ import annotations

from typing import Any, Optional, Iterator
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class TrieNode:
    """Node in a trie."""
    children: dict[str, "TrieNode"]
    is_end: bool = False
    value: Any = None
    depth: int = 0


class DataTrieAction:
    """
    Trie data structure for prefix-based operations.

    Supports insert, search, prefix match,
    longest prefix match, and autocomplete.

    Example:
        trie = DataTrieAction()
        trie.insert("apple", {"fruit": "apple", "color": "red"})
        trie.insert("app", {"short": "app"})
        results = trie.prefix_search("app")  # both entries
        lpm = trie.longest_prefix_match("application")  # "app"
    """

    def __init__(self) -> None:
        self._root = TrieNode(children={}, depth=0)
        self._size = 0

    def insert(
        self,
        key: str,
        value: Any = None,
    ) -> None:
        """Insert a key-value pair into the trie."""
        node = self._root

        for char in key:
            if char not in node.children:
                node.children[char] = TrieNode(
                    children={},
                    depth=node.depth + 1,
                )
            node = node.children[char]

        if not node.is_end:
            self._size += 1

        node.is_end = True
        node.value = value

    def contains(self, key: str) -> bool:
        """Check if exact key exists."""
        node = self._find_node(key)
        return node is not None and node.is_end

    def get(self, key: str) -> Optional[Any]:
        """Get value for exact key."""
        node = self._find_node(key)
        if node is not None and node.is_end:
            return node.value
        return None

    def remove(self, key: str) -> bool:
        """Remove a key from the trie."""
        node = self._root
        stack = []

        for char in key:
            if char not in node.children:
                return False
            stack.append((node, char))
            node = node.children[char]

        if not node.is_end:
            return False

        node.is_end = False
        node.value = None
        self._size -= 1

        self._cleanup_nodes(stack)

        return True

    def prefix_search(
        self,
        prefix: str,
        limit: int = 100,
    ) -> list[tuple[str, Any]]:
        """Find all entries with given prefix."""
        node = self._find_node(prefix)

        if node is None:
            return []

        results = []
        self._collect_all(node, prefix, results, limit)

        return results

    def autocomplete(
        self,
        prefix: str,
        max_results: int = 10,
    ) -> list[str]:
        """Get autocomplete suggestions for prefix."""
        results = self.prefix_search(prefix, limit=max_results)
        return [key for key, _ in results]

    def longest_prefix_match(self, text: str) -> Optional[str]:
        """Find longest key that is a prefix of text."""
        node = self._root
        last_match = None

        for char in text:
            if char not in node.children:
                break
            node = node.children[char]
            if node.is_end:
                last_match = char

        return last_match

    def keys_with_suffix(
        self,
        suffix: str,
    ) -> list[str]:
        """Find all keys ending with given suffix."""
        results = []
        self._find_keys_by_suffix(self._root, suffix, "", results)
        return results

    def common_prefix(
        self,
        key1: str,
        key2: str,
    ) -> str:
        """Find common prefix of two keys."""
        common = []
        chars1 = iter(key1)
        chars2 = iter(key2)

        for c1, c2 in zip(chars1, chars2):
            if c1 == c2:
                common.append(c1)
            else:
                break

        return "".join(common)

    def _find_node(self, key: str) -> Optional[TrieNode]:
        """Find node for given key."""
        node = self._root

        for char in key:
            if char not in node.children:
                return None
            node = node.children[char]

        return node

    def _collect_all(
        self,
        node: TrieNode,
        prefix: str,
        results: list,
        limit: int,
    ) -> None:
        """Recursively collect all key-value pairs from node."""
        if len(results) >= limit:
            return

        if node.is_end:
            results.append((prefix, node.value))

        for char, child in node.children.items():
            self._collect_all(child, prefix + char, results, limit)

    def _cleanup_nodes(self, stack: list) -> None:
        """Remove empty branches after deletion."""
        while stack:
            parent, char = stack.pop()
            child = parent.children[char]

            if (not child.is_end and
                not child.children and
                len(stack) > 0):
                del parent.children[char]
            else:
                break

    def _find_keys_by_suffix(
        self,
        node: TrieNode,
        suffix: str,
        current: str,
        results: list,
    ) -> None:
        """Recursively find keys ending with suffix."""
        if not suffix:
            if node.is_end:
                results.append(current)
            return

        first_char = suffix[0]
        remaining = suffix[1:]

        for char, child in node.children.items():
            if char == first_char:
                self._find_keys_by_suffix(child, remaining, current + char, results)
            elif first_char in child.children:
                self._find_keys_by_suffix(
                    child.children[first_char],
                    remaining,
                    current + char + first_char,
                    results,
                )

    @property
    def size(self) -> int:
        """Number of keys in trie."""
        return self._size

    def __len__(self) -> int:
        return self.size
