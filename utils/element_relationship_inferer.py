"""Element relationship inferer for deducing element relationships from accessibility tree."""
from typing import Dict, List, Optional, Set, Any, Tuple
from collections import deque


class ElementRelationshipInferer:
    """Infers semantic relationships between UI elements.
    
    Analyzes accessibility tree structure to deduce relationships
    like label-element, container-contained, etc.
    
    Example:
        inferer = ElementRelationshipInferer()
        relationships = inferer.infer_from_tree(tree)
        for rel in relationships.get("label", []):
            print(f"Label '{rel[0]}' -> element '{rel[1]}'")
    """

    def __init__(self) -> None:
        self._relationships: Dict[str, List[Tuple[str, str]]] = {
            "parent_child": [], "label": [], "described_by": [], "sibling": [],
        }

    def infer_from_tree(self, tree: Any) -> Dict[str, List[Tuple[str, str]]]:
        """Infer relationships from an accessibility tree."""
        if not tree:
            return self._relationships
        self._relationships = {"parent_child": [], "label": [], "described_by": [], "sibling": []}
        self._walk_tree(tree, None, set())
        return self._relationships

    def _walk_tree(self, node: Any, parent: Optional[Any], visited: Set[int]) -> None:
        """Walk accessibility tree and extract relationships."""
        if not node:
            return
        node_id = id(node)
        if node_id in visited:
            return
        visited.add(node_id)
        if parent:
            self._relationships["parent_child"].append((
                parent.get("name", ""), node.get("name", "")
            ))
        self._infer_label_relationship(node)
        for child in node.get("children", []):
            self._walk_tree(child, node, visited)

    def _infer_label_relationship(self, node: Dict) -> None:
        """Infer label-element relationships."""
        role = node.get("role", "").lower()
        name = node.get("name", "")
        if role in ("label", "text") and name:
            for child in node.get("children", []):
                child_role = child.get("role", "").lower()
                if child_role in ("button", "textfield", "checkbox", "radio", "slider"):
                    self._relationships["label"].append((name, child.get("name", "")))

    def find_element_by_label(self, label: str, elements: List[Dict]) -> Optional[Dict]:
        """Find element that corresponds to a label."""
        for source, target in self._relationships.get("label", []):
            if source == label or label in source:
                for elem in elements:
                    if elem.get("name") == target:
                        return elem
        return None

    def get_container_for_element(self, element_id: str, tree: Any) -> Optional[Dict]:
        """Find the likely container for an element."""
        if not tree:
            return None
        path = self._find_path_to_element(tree, element_id)
        return path[-2] if len(path) > 1 else None

    def _find_path_to_element(self, node: Any, target_id: str) -> List[Dict]:
        """Find path from root to target element."""
        queue = deque([(node, [])])
        while queue:
            current, path = queue.popleft()
            current_path = path + [current]
            if current.get("id") == target_id:
                return current_path
            for child in current.get("children", []):
                queue.append((child, current_path))
        return []
