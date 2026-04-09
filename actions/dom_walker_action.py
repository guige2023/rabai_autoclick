"""
DOM Walker Action Module.

Walks and traverses DOM trees with support for filtering,
searching, path extraction, and tree transformation.
"""

from typing import Any, Callable, Optional


class DOMWalker:
    """Walks and traverses DOM trees."""

    def __init__(self):
        """Initialize DOM walker."""
        pass

    def walk(
        self,
        root: list[dict],
        visitor: Callable[[dict], Optional[bool]],
    ) -> None:
        """
        Walk DOM tree calling visitor for each node.

        Args:
            root: DOM tree (list of elements).
            visitor: Function(element) -> Optional[bool].
                    Return False to skip children, None/True to continue.
        """
        for element in root:
            result = visitor(element)
            if result is not False:
                children = element.get("children", [])
                if children:
                    self.walk(children, visitor)

    def find_all(
        self,
        root: list[dict],
        predicate: Callable[[dict], bool],
    ) -> list[dict]:
        """
        Find all elements matching a predicate.

        Args:
            root: DOM tree.
            predicate: Function(element) -> bool.

        Returns:
            List of matching elements.
        """
        results = []

        def visitor(element: dict) -> Optional[bool]:
            if predicate(element):
                results.append(element)
            return None

        self.walk(root, visitor)
        return results

    def find_first(
        self,
        root: list[dict],
        predicate: Callable[[dict], bool],
    ) -> Optional[dict]:
        """
        Find first element matching predicate.

        Args:
            root: DOM tree.
            predicate: Function(element) -> bool.

        Returns:
            First matching element or None.
        """
        for element in self._iter_flat(root):
            if predicate(element):
                return element
        return None

    def get_path(
        self,
        root: list[dict],
        target: dict,
    ) -> Optional[list[int]]:
        """
        Get the path of indices to reach an element.

        Args:
            root: DOM tree.
            target: Element to find.

        Returns:
            List of child indices from root to target, or None.
        """
        path = []

        def walker(elements: list[dict]) -> bool:
            for i, elem in enumerate(elements):
                if elem is target:
                    path.append(i)
                    return True
                path.append(i)
                if elem.get("children"):
                    if walker(elem["children"]):
                        return True
                path.pop()
            return False

        walker(root)
        return path if path else None

    def get_element_at_path(
        self,
        root: list[dict],
        path: list[int],
    ) -> Optional[dict]:
        """
        Get element at a given path.

        Args:
            root: DOM tree.
            path: List of child indices.

        Returns:
            Element at path or None.
        """
        current = root
        for idx in path:
            if not isinstance(current, list) or idx >= len(current):
                return None
            current = current[idx]
            children = current.get("children", [])
            current = children
        return current

    def transform(
        self,
        root: list[dict],
        transformer: Callable[[dict], dict],
    ) -> list[dict]:
        """
        Transform all elements in the tree.

        Args:
            root: DOM tree.
            transformer: Function(element) -> element.

        Returns:
            New DOM tree with transformed elements.
        """
        result = []
        for element in root:
            transformed = transformer(element)
            if "children" in transformed:
                transformed["children"] = self.transform(
                    transformed["children"], transformer
                )
            result.append(transformed)
        return result

    def count(self, root: list[dict]) -> int:
        """
        Count total elements in DOM tree.

        Args:
            root: DOM tree.

        Returns:
            Total element count.
        """
        count = 0
        for _ in self._iter_flat(root):
            count += 1
        return count

    def get_all_text(self, root: list[dict], separator: str = " ") -> str:
        """
        Extract all text content from DOM.

        Args:
            root: DOM tree.
            separator: Text separator.

        Returns:
            Concatenated text content.
        """
        texts = []
        for elem in self._iter_flat(root):
            text = elem.get("text", "").strip()
            if text:
                texts.append(text)
        return separator.join(texts)

    def _iter_flat(self, root: list[dict]):
        """Iterate all elements in tree (flattened)."""
        for element in root:
            yield element
            children = element.get("children", [])
            if children:
                yield from self._iter_flat(children)
