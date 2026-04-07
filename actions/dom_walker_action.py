"""
DOM Tree Walker Action Module.

Traverses and extracts data from HTML/XML DOM trees using
recursive descent, filtering, mapping, and tree transformation.

Example:
    >>> from dom_walker_action import DOMWalker, WalkConfig
    >>> walker = DOMWalker()
    >>> nodes = walker.walk(doc, filter_fn=lambda n: n.tag == "a")
    >>> links = walker.extract_attributes(nodes, {"href": "url", "text": "textContent"})
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Iterator


@dataclass
class DOMNode:
    """Represents a node in the DOM tree."""
    tag: str
    attrs: dict[str, str]
    text: str = ""
    children: list["DOMNode"] = field(default_factory=list)
    parent: Optional["DOMNode"] = None
    index: int = 0


@dataclass
class WalkConfig:
    """Configuration for tree walk operation."""
    max_depth: int = 100
    max_nodes: int = 10000
    tags: Optional[list[str]] = None
    skip_tags: Optional[list[str]] = None


@dataclass
class XPathStep:
    """Single step in an XPath expression."""
    axis: str = "child"
    node_test: str = "*"
    predicates: list[str] = field(default_factory=list)


class DOMWalker:
    """Walk, filter, transform, and extract from DOM trees."""

    def __init__(self):
        self._node_count = 0

    def parse_from_html(self, html: str) -> DOMNode:
        """Parse HTML string into DOM tree."""
        from html.parser_action import HTMLParserAction, HTMLElement
        parser = HTMLParserAction()
        doc = parser.parse_string(html)
        return self._elem_to_node(doc.root)

    def _elem_to_node(self, elem) -> DOMNode:
        node = DOMNode(tag=elem.tag, attrs=elem.attrs, text=elem.text)
        for child in elem.children:
            child_node = self._elem_to_node(child)
            child_node.parent = node
            node.children.append(child_node)
        return node

    def walk(
        self,
        root: DOMNode,
        filter_fn: Optional[Callable[[DOMNode], bool]] = None,
        config: Optional[WalkConfig] = None,
    ) -> list[DOMNode]:
        """
        Walk tree and return matching nodes.

        Args:
            root: Root DOMNode
            filter_fn: Optional predicate function
            config: WalkConfig with depth/node limits

        Returns:
            List of matching nodes in document order
        """
        config = config or WalkConfig()
        self._node_count = 0
        results: list[DOMNode] = []

        def _walk(node: DOMNode, depth: int) -> None:
            if depth > config.max_depth or self._node_count >= config.max_nodes:
                return

            self._node_count += 1

            if config.skip_tags and node.tag.lower() in config.skip_tags:
                return

            if config.tags and node.tag.lower() not in config.tags:
                pass
            elif filter_fn is None or filter_fn(node):
                results.append(node)

            for child in node.children:
                _walk(child, depth + 1)

        _walk(root, 0)
        return results

    def walk_iter(
        self,
        root: DOMNode,
        config: Optional[WalkConfig] = None,
    ) -> Iterator[DOMNode]:
        """Iterate over all nodes in tree (generator)."""
        config = config or WalkConfig()
        self._node_count = 0

        def _iter(node: DOMNode, depth: int) -> Iterator[DOMNode]:
            if depth > config.max_depth or self._node_count >= config.max_nodes:
                return
            self._node_count += 1
            yield node
            for child in node.children:
                yield from _iter(child, depth + 1)

        yield from _iter(root, 0)

    def find_by_tag(self, root: DOMNode, tag: str) -> list[DOMNode]:
        """Find all nodes with given tag name."""
        return self.walk(root, lambda n: n.tag.lower() == tag.lower())

    def find_by_class(self, root: DOMNode, class_name: str) -> list[DOMNode]:
        """Find all nodes with given CSS class."""
        return self.walk(root, lambda n: class_name in n.attrs.get("class", "").split())

    def find_by_id(self, root: DOMNode, elem_id: str) -> Optional[DOMNode]:
        """Find first node with given ID."""
        results = self.walk(root, lambda n: n.attrs.get("id") == elem_id)
        return results[0] if results else None

    def find_by_attribute(self, root: DOMNode, attr: str, value: Optional[str] = None) -> list[DOMNode]:
        """Find nodes with attribute, optionally matching value."""
        def matcher(n: DOMNode) -> bool:
            if attr not in n.attrs:
                return False
            if value is None:
                return True
            return n.attrs[attr] == value
        return self.walk(root, matcher)

    def xpath_query(self, root: DOMNode, xpath: str) -> list[DOMNode]:
        """Execute XPath-like query on DOM tree."""
        steps = self._parse_xpath(xpath)
        nodes: list[DOMNode] = [root]
        for step in steps:
            nodes = self._xpath_step(nodes, step)
        return nodes

    def _parse_xpath(self, xpath: str) -> list[XPathStep]:
        steps: list[XPathStep] = []
        parts = xpath.strip("/").split("/")
        for part in parts:
            if not part:
                continue
            step = XPathStep()
            if part == "*":
                step.node_test = "*"
            elif "[" in part:
                tag, _, pred = part.partition("[")
                step.node_test = tag or "*"
                step.predicates = [pred.rstrip("]")]
            else:
                step.node_test = part
            steps.append(step)
        return steps

    def _xpath_step(self, nodes: list[DOMNode], step: XPathStep) -> list[DOMNode]:
        results: list[DOMNode] = []
        for node in nodes:
            if step.axis == "child":
                for child in node.children:
                    if self._xpath_node_test(child, step.node_test):
                        if self._xpath_predicates_match(child, step.predicates):
                            results.append(child)
            elif step.axis == "descendant":
                for desc in self.walk_iter(node):
                    if desc is not node and self._xpath_node_test(desc, step.node_test):
                        if self._xpath_predicates_match(desc, step.predicates):
                            results.append(desc)
            elif step.axis == "parent":
                if node.parent and self._xpath_node_test(node.parent, step.node_test):
                    results.append(node.parent)
        return results

    def _xpath_node_test(self, node: DOMNode, test: str) -> bool:
        if test == "*":
            return True
        return node.tag.lower() == test.lower()

    def _xpath_predicates_match(self, node: DOMNode, predicates: list[str]) -> bool:
        for pred in predicates:
            if pred.isdigit():
                idx = int(pred) - 1
                siblings = [c for c in node.parent.children if c.tag == node.tag] if node.parent else [node]
                if siblings and siblings[idx] is node:
                    continue
                return False
            elif pred.startswith("@"):
                attr_part = pred[1:]
                if "=" in attr_part:
                    attr, _, val = attr_part.partition("=")
                    if node.attrs.get(attr) != val.strip('"\''):
                        return False
                elif attr_part not in node.attrs:
                    return False
        return True

    def extract_text(self, node: DOMNode, deep: bool = True) -> str:
        """Extract text content from node."""
        parts = [node.text] if node.text else []
        if deep:
            for child in node.children:
                parts.append(self.extract_text(child, True))
        return "".join(parts).strip()

    def extract_attributes(
        self,
        nodes: list[DOMNode],
        attr_map: dict[str, str],
    ) -> list[dict[str, Any]]:
        """
        Extract attributes from nodes into records.

        Args:
            nodes: List of DOMNode objects
            attr_map: Map of output field name to attribute name or "textContent"/"tag"

        Returns:
            List of dictionaries with extracted data
        """
        results: list[dict[str, Any]] = []
        for node in nodes:
            record: dict[str, Any] = {}
            for field_name, attr_path in attr_map.items():
                if attr_path == "textContent":
                    record[field_name] = self.extract_text(node)
                elif attr_path == "tag":
                    record[field_name] = node.tag
                elif attr_path == "innerHTML":
                    record[field_name] = self._serialize_children(node)
                else:
                    record[field_name] = node.attrs.get(attr_path)
            results.append(record)
        return results

    def _serialize_children(self, node: DOMNode) -> str:
        parts = []
        for child in node.children:
            attrs = "".join(f' {k}="{v}"' for k, v in child.attrs.items())
            inner = child.text + "".join(self._serialize_children(c) for c in child.children)
            parts.append(f"<{child.tag}{attrs}>{inner}</{child.tag}>")
        return "".join(parts)

    def transform(
        self,
        root: DOMNode,
        transform_fn: Callable[[DOMNode], Optional[DOMNode]],
    ) -> DOMNode:
        """
        Transform tree by applying function to each node.
        If function returns None, node is removed.

        Returns:
            Transformed tree (may be modified in place)
        """
        def _transform(node: DOMNode) -> bool:
            new_node = transform_fn(node)
            if new_node is None:
                return False
            if new_node is not node:
                for i, child in enumerate(node.children):
                    node.children[i] = child
            children_to_keep: list[DOMNode] = []
            for child in node.children:
                if _transform(child):
                    children_to_keep.append(child)
            node.children = children_to_keep
            return True

        _transform(root)
        return root

    def siblings(self, node: DOMNode) -> list[DOMNode]:
        """Return sibling nodes."""
        if node.parent:
            return [c for c in node.parent.children if c is not node]
        return []

    def ancestors(self, node: DOMNode) -> list[DOMNode]:
        """Return all ancestor nodes up to root."""
        ancestors: list[DOMNode] = []
        current = node.parent
        while current:
            ancestors.append(current)
            current = current.parent
        return ancestors

    def descendants(self, node: DOMNode, max_depth: int = 100) -> list[DOMNode]:
        """Return all descendant nodes."""
        return self.walk(node, config=WalkConfig(max_depth=max_depth))

    def count(self, root: DOMNode, tag: Optional[str] = None) -> int:
        """Count nodes, optionally filtered by tag."""
        if tag:
            return len(self.find_by_tag(root, tag))
        return len(list(self.walk_iter(root)))

    def tree_string(self, root: DOMNode, indent: str = "  ") -> str:
        """Return ASCII representation of tree."""
        lines: list[str] = []

        def _print(node: DOMNode, depth: int) -> None:
            attrs_str = " ".join(f'{k}="{v}"' for k, v in list(node.attrs.items())[:3])
            text_preview = node.text[:20].strip().replace("\n", " ")
            if text_preview:
                text_preview = f" # {text_preview}"
            lines.append(f"{indent * depth}<{node.tag}> {attrs_str}{text_preview}")
            for child in node.children:
                _print(child, depth + 1)

        _print(root, 0)
        return "\n".join(lines)


if __name__ == "__main__":
    walker = DOMWalker()
    html = "<html><body><div class='items'><a href='/1'>Link 1</a><a href='/2'>Link 2</a></div></body></html>"
    root = walker.parse_from_html(html)
    links = walker.find_by_tag(root, "a")
    records = walker.extract_attributes(links, {"href": "href", "text": "textContent"})
    print(f"Found {len(records)} links")
    print(records)
