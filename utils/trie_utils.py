"""Trie (prefix tree) data structure utilities.

Provides trie implementation for efficient prefix matching
and autocomplete in automation workflows.
"""

from typing import Any, Dict, Iterator, List, Optional, Set


class TrieNode:
    """Node in a trie."""

    __slots__ = ("children", "is_word", "data")

    def __init__(self) -> None:
        self.children: Dict[str, TrieNode] = {}
        self.is_word: bool = False
        self.data: Any = None


class Trie:
    """Prefix tree for efficient string operations.

    Example:
        trie = Trie()
        trie.insert("hello")
        trie.insert("help")
        trie.insert("world")
        print(trie.search("hello"))  # True
        print(trie.prefix("he"))  # ["hello", "help"]
    """

    def __init__(self) -> None:
        self._root = TrieNode()
        self._size = 0

    def insert(self, word: str, data: Any = None) -> None:
        """Insert word into trie.

        Args:
            word: Word to insert.
            data: Optional data to store with word.
        """
        node = self._root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        if not node.is_word:
            self._size += 1
        node.is_word = True
        node.data = data

    def search(self, word: str) -> bool:
        """Check if word exists in trie.

        Args:
            word: Word to search.

        Returns:
            True if word found.
        """
        node = self._find_node(word)
        return node is not None and node.is_word

    def starts_with(self, prefix: str) -> bool:
        """Check if any word starts with prefix.

        Args:
            prefix: Prefix to check.

        Returns:
            True if prefix exists.
        """
        return self._find_node(prefix) is not None

    def get(self, word: str) -> Optional[Any]:
        """Get data for word.

        Args:
            word: Word to lookup.

        Returns:
            Data or None if not found.
        """
        node = self._find_node(word)
        if node and node.is_word:
            return node.data
        return None

    def _find_node(self, prefix: str) -> Optional[TrieNode]:
        """Find node for prefix."""
        node = self._root
        for char in prefix:
            if char not in node.children:
                return None
            node = node.children[char]
        return node

    def delete(self, word: str) -> bool:
        """Delete word from trie.

        Args:
            word: Word to delete.

        Returns:
            True if deleted, False if not found.
        """
        path: List[Tuple[TrieNode, str]] = []
        node = self._root
        for char in word:
            if char not in node.children:
                return False
            path.append((node, char))
            node = node.children[char]

        if not node.is_word:
            return False

        node.is_word = False
        node.data = None
        self._size -= 1

        for parent, char in reversed(path):
            child = parent.children[char]
            if not child.children and not child.is_word:
                del parent.children[char]
            else:
                break

        return True

    def words_with_prefix(self, prefix: str) -> List[str]:
        """Get all words starting with prefix.

        Args:
            prefix: Prefix to match.

        Returns:
            List of matching words.
        """
        results: List[str] = []
        node = self._find_node(prefix)
        if node:
            self._collect_words(node, prefix, results)
        return results

    def _collect_words(self, node: TrieNode, prefix: str, results: List[str]) -> None:
        """Recursively collect all words from node."""
        if node.is_word:
            results.append(prefix)
        for char, child in node.children.items():
            self._collect_words(child, prefix + char, results)

    def all_words(self) -> List[str]:
        """Get all words in trie.

        Returns:
            List of all words.
        """
        return self.words_with_prefix("")

    def __len__(self) -> int:
        """Get number of words."""
        return self._size

    def __contains__(self, word: str) -> bool:
        return self.search(word)


class TrieMap(Generic[K, V]):
    """Trie with key-based storage.

    Example:
        tmap = TrieMap()
        tmap["hello"] = "world"
        print(tmap["hello"])  # "world"
    """

    def __setitem__(self, key: str, value: V) -> None:
        self.insert(key, value)

    def __getitem__(self, key: str) -> V:
        result = self.get(key)
        if result is None:
            raise KeyError(key)
        return result

    def insert(self, key: str, value: V) -> None:
        """Insert key-value pair."""
        node = self._root
        for char in key:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_word = True
        node.data = value

    def get(self, key: str) -> Optional[V]:
        """Get value for key."""
        node = self._find_node(key)
        if node and node.is_word:
            return node.data
        return None

    def __delitem__(self, key: str) -> None:
        if not self.delete(key):
            raise KeyError(key)

    def delete(self, key: str) -> bool:
        """Delete key."""
        # Implementation similar to Trie.delete
        pass

    def keys_with_prefix(self, prefix: str) -> List[str]:
        """Get all keys starting with prefix."""
        return self._trie.words_with_prefix(prefix)

    def _find_node(self, prefix: str) -> Optional[TrieNode]:
        node = self._root
        for char in prefix:
            if char not in node.children:
                return None
            node = node.children[char]
        return node


from typing import Generic, TypeVar
K = TypeVar("K")
V = TypeVar("V")
