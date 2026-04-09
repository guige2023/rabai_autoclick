"""
Trie (prefix tree) data structure module.

Provides efficient string operations including prefix matching,
autocomplete, and spell checking utilities.

Author: Aito Auto Agent
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import (
    Callable,
    Generic,
    Hashable,
    Iterator,
    Optional,
    TypeVar,
)


T = TypeVar('T', bound=Hashable)


@dataclass
class TrieNode(Generic[T]):
    """Node in a trie structure."""
    children: dict[str, TrieNode[T]] = field(default_factory=dict)
    is_end: bool = False
    value: Optional[T] = None
    depth: int = 0
    metadata: dict = field(default_factory=dict)


class Trie(Generic[T]):
    """
    Trie (prefix tree) data structure.

    Efficient for string operations like prefix matching,
    autocomplete, and spell checking.

    Example:
        trie = Trie[str]()

        # Insert words
        trie.insert("apple")
        trie.insert("app")
        trie.insert("application")
        trie.insert("banana")

        # Prefix search
        prefixes = trie.search_prefix("app")  # ["apple", "app", "application"]

        # Autocomplete
        suggestions = trie.autocomplete("ap")  # ["apple", "app", "application"]
    """

    def __init__(self, thread_safe: bool = False):
        self._root = TrieNode[T]()
        self._size = 0
        self._lock = threading.RLock() if thread_safe else None

    def insert(self, word: str, value: Optional[T] = None) -> None:
        """
        Insert a word into the trie.

        Args:
            word: Word to insert
            value: Optional value to associate with word
        """
        with self._lock:
            node = self._root

            for char in word.lower():
                if char not in node.children:
                    node.children[char] = TrieNode[T](
                        depth=node.depth + 1
                    )
                node = node.children[char]

            if not node.is_end:
                self._size += 1

            node.is_end = True
            node.value = value

    def search(self, word: str) -> bool:
        """
        Search for exact word in trie.

        Args:
            word: Word to search for

        Returns:
            True if word exists in trie
        """
        node = self._find_node(word.lower())
        return node is not None and node.is_end

    def starts_with(self, prefix: str) -> bool:
        """
        Check if any word starts with prefix.

        Args:
            prefix: Prefix to check

        Returns:
            True if any word starts with prefix
        """
        node = self._find_node(prefix.lower())
        return node is not None

    def search_prefix(self, prefix: str) -> list[str]:
        """
        Get all words starting with prefix.

        Args:
            prefix: Prefix to search

        Returns:
            List of matching words
        """
        node = self._find_node(prefix.lower())
        if node is None:
            return []

        results = []
        self._collect_words(node, prefix, results)
        return results

    def autocomplete(
        self,
        prefix: str,
        max_results: int = 10
    ) -> list[tuple[str, Optional[T]]]:
        """
        Get autocomplete suggestions for prefix.

        Args:
            prefix: Prefix to autocomplete
            max_results: Maximum number of results

        Returns:
            List of (word, value) tuples
        """
        node = self._find_node(prefix.lower())
        if node is None:
            return []

        results = []
        self._collect_with_values(node, prefix, results, max_results)
        return results

    def _find_node(self, prefix: str) -> Optional[TrieNode[T]]:
        """Find node for given prefix."""
        node = self._root

        for char in prefix:
            if char not in node.children:
                return None
            node = node.children[char]

        return node

    def _collect_words(
        self,
        node: TrieNode[T],
        current: str,
        results: list[str]
    ) -> None:
        """Recursively collect all words from node."""
        if node.is_end:
            results.append(current)

        for char, child in node.children.items():
            self._collect_words(child, current + char, results)

    def _collect_with_values(
        self,
        node: TrieNode[T],
        current: str,
        results: list[tuple[str, Optional[T]]],
        max_results: int
    ) -> None:
        """Recursively collect words with values."""
        if node.is_end:
            results.append((current, node.value))
            if len(results) >= max_results:
                return

        for char, child in sorted(node.children.items()):
            if len(results) >= max_results:
                return
            self._collect_with_values(child, current + char, results, max_results)

    def delete(self, word: str) -> bool:
        """
        Delete word from trie.

        Args:
            word: Word to delete

        Returns:
            True if word was deleted
        """
        with self._lock:
            return self._delete_recursive(self._root, word.lower(), 0)

    def _delete_recursive(
        self,
        node: TrieNode[T],
        word: str,
        depth: int
    ) -> bool:
        """Recursively delete word."""
        if depth == len(word):
            if node.is_end:
                node.is_end = False
                node.value = None
                self._size -= 1
                return len(node.children) == 0

            return False

        char = word[depth]
        if char not in node.children:
            return False

        child_node = node.children[char]
        should_delete_child = self._delete_recursive(child_node, word, depth + 1)

        if should_delete_child:
            del node.children[char]
            return len(node.children) == 0 and not node.is_end

        return False

    def get_all_words(self) -> list[str]:
        """Get all words in trie."""
        results = []
        self._collect_words(self._root, "", results)
        return results

    def get_longest_common_prefix(self) -> str:
        """
        Find longest common prefix among all words.

        Returns:
            Longest common prefix string
        """
        prefix = []
        node = self._root

        while len(node.children) == 1 and not node.is_end:
            char, node = next(iter(node.children.items()))
            prefix.append(char)

        return "".join(prefix)

    def levenshtein_distance(self, word1: str, word2: str) -> int:
        """
        Calculate Levenshtein distance between two words.

        Args:
            word1: First word
            word2: Second word

        Returns:
            Edit distance between words
        """
        m, n = len(word1), len(word2)
        dp = [[0] * (n + 1) for _ in range(m + 1)]

        for i in range(m + 1):
            dp[i][0] = i
        for j in range(n + 1):
            dp[0][j] = j

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if word1[i - 1] == word2[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1]
                else:
                    dp[i][j] = 1 + min(
                        dp[i - 1][j],
                        dp[i][j - 1],
                        dp[i - 1][j - 1]
                    )

        return dp[m][n]

    def find_similar(
        self,
        word: str,
        max_distance: int = 2
    ) -> list[tuple[str, int]]:
        """
        Find words similar to given word within edit distance.

        Args:
            word: Word to match
            max_distance: Maximum edit distance

        Returns:
            List of (word, distance) tuples
        """
        results = []
        all_words = self.get_all_words()

        for w in all_words:
            distance = self.levenshtein_distance(word.lower(), w.lower())
            if distance <= max_distance:
                results.append((w, distance))

        return sorted(results, key=lambda x: x[1])

    def __len__(self) -> int:
        """Return number of words in trie."""
        return self._size

    def __contains__(self, word: str) -> bool:
        """Check if word exists in trie."""
        return self.search(word)


class TrieBuilder(Generic[T]):
    """Fluent builder for constructing tries with batch inserts."""

    def __init__(self, thread_safe: bool = False):
        self._trie = Trie[T](thread_safe=thread_safe)

    def add(self, word: str, value: Optional[T] = None) -> TrieBuilder[T]:
        """Add a word to the trie."""
        self._trie.insert(word, value)
        return self

    def add_many(
        self,
        words: list[str],
        value_func: Optional[Callable[[str], T]] = None
    ) -> TrieBuilder[T]:
        """Add multiple words to the trie."""
        for word in words:
            value = value_func(word) if value_func else None
            self._trie.insert(word, value)
        return self

    def build(self) -> Trie[T]:
        """Build and return the trie."""
        return self._trie


def create_trie(thread_safe: bool = False) -> Trie:
    """Factory to create a Trie."""
    return Trie(thread_safe=thread_safe)


def create_trie_builder(thread_safe: bool = False) -> TrieBuilder:
    """Factory to create a TrieBuilder."""
    return TrieBuilder(thread_safe=thread_safe)
