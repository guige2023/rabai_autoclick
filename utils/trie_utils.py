"""
Trie (prefix tree) utilities.

Provides Trie and Radix Trie implementations for
efficient string storage and prefix-based operations.
"""

from __future__ import annotations


class TrieNode:
    """Node in a Trie."""

    __slots__ = ("children", "is_word", "frequency")

    def __init__(self) -> None:
        self.children: dict[str, TrieNode] = {}
        self.is_word: bool = False
        self.frequency: int = 0


class Trie:
    """
    Prefix tree for efficient string operations.

    Supports insert, search, prefix match, and autocomplete.
    """

    def __init__(self) -> None:
        self.root = TrieNode()

    def insert(self, word: str) -> None:
        """Insert word into trie."""
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_word = True
        node.frequency += 1

    def search(self, word: str) -> bool:
        """Check if word exists in trie."""
        node = self._find_node(word)
        return node is not None and node.is_word

    def starts_with(self, prefix: str) -> bool:
        """Check if any word starts with prefix."""
        return self._find_node(prefix) is not None

    def _find_node(self, prefix: str) -> TrieNode | None:
        """Find node for prefix, return None if not found."""
        node = self.root
        for char in prefix:
            if char not in node.children:
                return None
            node = node.children[char]
        return node

    def autocomplete(self, prefix: str, max_results: int = 10) -> list[str]:
        """
        Return words starting with prefix.

        Args:
            prefix: Prefix to match
            max_results: Maximum number of results

        Returns:
            List of matching words
        """
        node = self._find_node(prefix)
        if node is None:
            return []
        results: list[str] = []

        def dfs(n: TrieNode, path: list[str]) -> None:
            if len(results) >= max_results:
                return
            if n.is_word:
                results.append("".join(path))
            for char, child in n.children.items():
                path.append(char)
                dfs(child, path)
                path.pop()

        dfs(node, list(prefix))
        return results

    def all_words(self) -> list[str]:
        """Return all words in trie."""
        results: list[str] = []

        def dfs(node: TrieNode, path: list[str]) -> None:
            if node.is_word:
                results.append("".join(path))
            for char, child in node.children.items():
                path.append(char)
                dfs(child, path)
                path.pop()

        dfs(self.root, [])
        return results

    def remove(self, word: str) -> bool:
        """Remove word from trie. Returns True if removed."""
        node = self.root
        stack: list[tuple[TrieNode, str]] = []
        for char in word:
            if char not in node.children:
                return False
            stack.append((node, char))
            node = node.children[char]
        if not node.is_word:
            return False
        node.is_word = False
        node.frequency = 0
        while stack and not node.children and not node.is_word:
            node, char = stack.pop()
            del node.children[char]
            node = node
        return True

    def word_count(self, word: str) -> int:
        """Get frequency count of word (how many times inserted)."""
        node = self._find_node(word)
        return node.frequency if node and node.is_word else 0


class RadixTrie(Trie):
    """
    Radix Trie (compact prefix tree) that compresses single-child chains.
    """

    def insert(self, word: str) -> None:
        """Insert word with path compression."""
        if not word:
            self.root.is_word = True
            return
        node = self.root
        i = 0
        while i < len(word):
            char = word[i]
            if char not in node.children:
                node.children[char] = TrieNode()
            child = node.children[char]
            j = i
            while j < len(word) and hasattr(child, "label"):
                j += 1
            if not hasattr(child, "label"):
                child.label = ""
                child.children = child.children if hasattr(child, "children") else {}
            common_len = 0
            for k in range(min(len(child.label), len(word) - i)):
                if child.label[k] == word[i + k]:
                    common_len += 1
                else:
                    break
            if common_len < len(child.label):
                split_node = TrieNode()
                split_node.label = child.label[:common_len]
                split_node.children[child.label[common_len]] = child
                split_node.children = {child.label[common_len]: child}
                child.label = child.label[common_len:]
                node.children[char] = split_node
                node = split_node
                i += common_len
            else:
                node = child
                i += common_len
        node.is_word = True
        node.frequency += 1
