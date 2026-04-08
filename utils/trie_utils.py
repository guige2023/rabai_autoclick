"""Trie utilities for RabAI AutoClick.

Provides:
- Trie data structure implementation
- Prefix matching
- Auto-complete helpers
"""

from __future__ import annotations

from typing import (
    Dict,
    List,
    Optional,
)


class TrieNode:
    """A node in the trie."""

    def __init__(self) -> None:
        self.children: Dict[str, TrieNode] = {}
        self.is_end: bool = False
        self.value: Optional[str] = None


class Trie:
    """A trie (prefix tree) for string operations."""

    def __init__(self) -> None:
        self._root = TrieNode()

    def insert(self, word: str) -> None:
        """Insert a word into the trie.

        Args:
            word: Word to insert.
        """
        node = self._root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end = True
        node.value = word

    def search(self, word: str) -> bool:
        """Check if a word exists in the trie.

        Args:
            word: Word to search for.

        Returns:
            True if word is found.
        """
        node = self._find_node(word)
        return node is not None and node.is_end

    def starts_with(self, prefix: str) -> bool:
        """Check if any word starts with prefix.

        Args:
            prefix: Prefix to check.

        Returns:
            True if prefix exists.
        """
        return self._find_node(prefix) is not None

    def _find_node(self, prefix: str) -> Optional[TrieNode]:
        """Find node for prefix."""
        node = self._root
        for char in prefix:
            if char not in node.children:
                return None
            node = node.children[char]
        return node

    def autocomplete(self, prefix: str) -> List[str]:
        """Get all words starting with prefix.

        Args:
            prefix: Prefix to match.

        Returns:
            List of matching words.
        """
        node = self._find_node(prefix)
        if not node:
            return []
        return self._collect_words(node)

    def _collect_words(self, node: TrieNode) -> List[str]:
        """Collect all words from node."""
        results: List[str] = []
        if node.is_end:
            results.append(node.value)  # type: ignore
        for child in node.children.values():
            results.extend(self._collect_words(child))
        return results


__all__ = [
    "TrieNode",
    "Trie",
]
