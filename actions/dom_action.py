"""DOM parsing and manipulation action module for RabAI AutoClick.

Provides DOM operations:
- DomParseAction: Parse HTML/XML to DOM tree
- DomQueryAction: Query DOM elements by selector
- DomTraverseAction: Traverse DOM tree
- DomExtractAction: Extract structured data from DOM
- DomAttributeAction: Get/set DOM element attributes
"""

import re
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DomNode:
    """Represents a DOM node."""
    def __init__(self, tag: str = "", attrs: Dict[str, str] = None, text: str = "", parent: "DomNode" = None):
        self.tag = tag
        self.attrs = attrs or {}
        self.text = text or ""
        self.children: List["DomNode"] = []
        self.parent = parent

    def __repr__(self) -> str:
        return f"DomNode({self.tag}, attrs={self.attrs}, text={self.text[:30]!r})"


class HtmlDomParser(HTMLParser):
    """HTML parser that builds a DOM tree."""
    def __init__(self):
        super().__init__()
        self.root = DomNode("root")
        self.current = self.root
        self.stack: List[DomNode] = [self.root]

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        node = DomNode(tag, dict(attrs))
        node.parent = self.current
        self.current.children.append(node)
        if tag not in ("br", "hr", "img", "input", "meta", "link", "area", "base", "col", "embed", "param", "source", "track", "wbr"):
            self.current = node
            self.stack.append(node)

    def handle_endtag(self, tag: str):
        if tag in ("br", "hr", "img", "input", "meta", "link", "area", "base", "col", "embed", "param", "source", "track", "wbr"):
            return
        if self.stack and self.stack[-1].tag == tag:
            self.stack.pop()
            self.current = self.stack[-1] if self.stack else self.root

    def handle_data(self, data: str):
        if self.current and self.current != self.root:
            self.current.text += data


class DomParseAction(BaseAction):
    """Parse HTML/XML to DOM tree."""
    action_type = "dom_parse"
    display_name = "DOM解析"
    description = "解析HTML/XML为DOM树"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            html = params.get("html", "")
            is_xml = params.get("is_xml", False)

            if not html:
                return ActionResult(success=False, message="html content is required")

            if is_xml:
                try:
                    root = ET.fromstring(html)
                    return ActionResult(
                        success=True,
                        message="XML parsed successfully",
                        data={"type": "xml", "root_tag": root.tag}
                    )
                except ET.ParseError as e:
                    return ActionResult(success=False, message=f"XML parse error: {str(e)}")
            else:
                parser = HtmlDomParser()
                parser.feed(html)
                return ActionResult(
                    success=True,
                    message="HTML parsed successfully",
                    data={"type": "html", "root": self._serialize_node(parser.root)}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Parse error: {str(e)}")

    def _serialize_node(self, node: DomNode) -> Dict:
        """Serialize a DOM node to dict."""
        return {
            "tag": node.tag,
            "attrs": node.attrs,
            "text": node.text.strip(),
            "children": [self._serialize_node(child) for child in node.children]
        }


class DomQueryAction(BaseAction):
    """Query DOM elements by selector."""
    action_type = "dom_query"
    display_name = "DOM查询"
    description = "按选择器查询DOM元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            html = params.get("html", "")
            selector = params.get("selector", "")
            all_matches = params.get("all_matches", True)

            if not html:
                return ActionResult(success=False, message="html content is required")

            parser = HtmlDomParser()
            parser.feed(html)

            matches = self._query(parser.root, selector)

            if not all_matches and matches:
                return ActionResult(success=True, message="Found match", data={"match": self._serialize_node(matches[0])})

            return ActionResult(
                success=True,
                message=f"Found {len(matches)} matches",
                data={"matches": [self._serialize_node(m) for m in matches], "count": len(matches)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Query error: {str(e)}")

    def _query(self, node: DomNode, selector: str) -> List[DomNode]:
        """Query nodes matching selector."""
        results = []

        if self._matches(node, selector):
            results.append(node)

        for child in node.children:
            results.extend(self._query(child, selector))

        return results

    def _matches(self, node: DomNode, selector: str) -> bool:
        """Check if node matches selector."""
        if selector.startswith("."):
            class_name = selector[1:]
            node_classes = node.attrs.get("class", "").split()
            return class_name in node_classes
        elif selector.startswith("#"):
            return node.attrs.get("id", "") == selector[1:]
        elif selector.startswith("["):
            match = re.match(r"\[(\w+)(?:='(.*?)')?\]", selector)
            if match:
                key, val = match.groups()
                return node.attrs.get(key, "") == (val or "")
            return False
        elif selector.startswith("<") and selector.endswith(">"):
            return node.tag == selector[1:-1]
        else:
            return node.tag == selector

    def _serialize_node(self, node: DomNode) -> Dict:
        return {"tag": node.tag, "attrs": node.attrs, "text": node.text.strip()}


class DomTraverseAction(BaseAction):
    """Traverse DOM tree with various strategies."""
    action_type = "dom_traverse"
    display_name = "DOM遍历"
    description = "遍历DOM树"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            html = params.get("html", "")
            strategy = params.get("strategy", "bfs")
            filter_tag = params.get("filter_tag", "")
            max_depth = params.get("max_depth", 100)
            max_nodes = params.get("max_nodes", 1000)

            if not html:
                return ActionResult(success=False, message="html content is required")

            parser = HtmlDomParser()
            parser.feed(html)

            nodes = []
            if strategy == "bfs":
                nodes = self._bfs_traverse(parser.root, filter_tag, max_depth, max_nodes)
            elif strategy == "dfs":
                nodes = self._dfs_traverse(parser.root, filter_tag, max_depth, max_nodes)
            elif strategy == "leaves":
                nodes = self._get_leaves(parser.root, max_nodes)
            elif strategy == "level":
                nodes = self._get_level(parser.root, params.get("target_level", 0))

            return ActionResult(
                success=True,
                message=f"Traversed {len(nodes)} nodes",
                data={"nodes": [self._serialize_node(n) for n in nodes], "count": len(nodes)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Traverse error: {str(e)}")

    def _bfs_traverse(self, root: DomNode, filter_tag: str, max_depth: int, max_nodes: int) -> List[DomNode]:
        """Breadth-first traversal."""
        result = []
        queue = [(root, 0)]
        while queue and len(result) < max_nodes:
            node, depth = queue.pop(0)
            if depth > max_depth:
                break
            if not filter_tag or node.tag == filter_tag:
                result.append(node)
            queue.extend((child, depth + 1) for child in node.children)
        return result

    def _dfs_traverse(self, root: DomNode, filter_tag: str, max_depth: int, max_nodes: int) -> List[DomNode]:
        """Depth-first traversal."""
        result = []
        stack = [(root, 0)]
        while stack and len(result) < max_nodes:
            node, depth = stack.pop()
            if depth > max_depth:
                break
            if not filter_tag or node.tag == filter_tag:
                result.append(node)
            for child in reversed(node.children):
                stack.append((child, depth + 1))
        return result

    def _get_leaves(self, root: DomNode, max_nodes: int) -> List[DomNode]:
        """Get all leaf nodes."""
        result = []
        stack = [root]
        while stack and len(result) < max_nodes:
            node = stack.pop()
            if not node.children:
                result.append(node)
            stack.extend(node.children)
        return result

    def _get_level(self, root: DomNode, target_level: int) -> List[DomNode]:
        """Get all nodes at a specific level."""
        result = []
        queue = [(root, 0)]
        while queue:
            node, level = queue.pop(0)
            if level == target_level:
                result.append(node)
            elif level < target_level:
                queue.extend((child, level + 1) for child in node.children)
        return result

    def _serialize_node(self, node: DomNode) -> Dict:
        return {"tag": node.tag, "attrs": node.attrs, "text": node.text.strip()[:50]}


class DomExtractAction(BaseAction):
    """Extract structured data from DOM."""
    action_type = "dom_extract"
    display_name = "DOM提取"
    description = "从DOM提取结构化数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            html = params.get("html", "")
            extraction_config = params.get("config", {})

            if not html:
                return ActionResult(success=False, message="html content is required")

            parser = HtmlDomParser()
            parser.feed(html)

            results = {}
            for field_name, selector in extraction_config.items():
                nodes = self._query(parser.root, selector)
                if nodes:
                    if len(nodes) == 1:
                        results[field_name] = nodes[0].text.strip()
                    else:
                        results[field_name] = [n.text.strip() for n in nodes]

            return ActionResult(
                success=True,
                message=f"Extracted {len(results)} fields",
                data={"extracted": results}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Extract error: {str(e)}")

    def _query(self, node: DomNode, selector: str) -> List[DomNode]:
        results = []
        if self._matches(node, selector):
            results.append(node)
        for child in node.children:
            results.extend(self._query(child, selector))
        return results

    def _matches(self, node: DomNode, selector: str) -> bool:
        if selector.startswith("."):
            return selector[1:] in node.attrs.get("class", "").split()
        elif selector.startswith("#"):
            return node.attrs.get("id", "") == selector[1:]
        elif selector.startswith("["):
            match = re.match(r"\[(\w+)(?:='(.*?)')?\]", selector)
            if match:
                key, val = match.groups()
                return node.attrs.get(key, "") == (val or "")
            return False
        else:
            return node.tag == selector


class DomAttributeAction(BaseAction):
    """Get/set DOM element attributes."""
    action_type = "dom_attribute"
    display_name = "DOM属性"
    description = "获取/设置DOM元素属性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            html = params.get("html", "")
            selector = params.get("selector", "")
            attribute = params.get("attribute", "")
            operation = params.get("operation", "get")

            if not html:
                return ActionResult(success=False, message="html content is required")

            parser = HtmlDomParser()
            parser.feed(html)

            matches = self._query(parser.root, selector)
            if not matches:
                return ActionResult(success=False, message="No elements match selector")

            results = []
            for node in matches:
                if operation == "get":
                    if attribute:
                        results.append(node.attrs.get(attribute, ""))
                    else:
                        results.append(node.attrs)
                elif operation == "set":
                    node.attrs[attribute] = params.get("value", "")

            if operation == "get":
                return ActionResult(
                    success=True,
                    message=f"Retrieved {len(results)} attribute values",
                    data={"attributes": results, "count": len(results)}
                )
            else:
                return ActionResult(success=True, message="Attributes updated")

        except Exception as e:
            return ActionResult(success=False, message=f"Attribute error: {str(e)}")

    def _query(self, node: DomNode, selector: str) -> List[DomNode]:
        results = []
        if self._matches(node, selector):
            results.append(node)
        for child in node.children:
            results.extend(self._query(child, selector))
        return results

    def _matches(self, node: DomNode, selector: str) -> bool:
        if selector.startswith("."):
            return selector[1:] in node.attrs.get("class", "").split()
        elif selector.startswith("#"):
            return node.attrs.get("id", "") == selector[1:]
        else:
            return node.tag == selector
