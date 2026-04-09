"""
Automation Template Engine Module.

Provides template-based workflow generation with variable substitution,
conditional logic, loops, and reusable template libraries.

Author: AutoGen
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class TemplateToken(Enum):
    VARIABLE = auto()
    CONDITION = auto()
    LOOP = auto()
    INCLUDE = auto()
    FILTER = auto()
    MACRO = auto()
    LITERAL = auto()


@dataclass
class TemplateNode:
    token_type: TemplateToken
    content: str
    children: List[TemplateNode] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Template:
    template_id: str
    name: str
    body: str
    version: str = "1.0"
    variables: List[str] = field(default_factory=list)
    filters: FrozenSet[str] = field(default_factory=frozenset)
    tags: FrozenSet[str] = field(default_factory=frozenset)


@dataclass
class RenderContext:
    variables: Dict[str, Any] = field(default_factory=dict)
    loop_depth: int = 0
    max_loop_depth: int = 10


class TemplateParser:
    """Parses template strings into token trees."""

    VAR_PATTERN = re.compile(r"\{\{\s*(\w+)(?:\.(\w+))?(?:\s*\|\s*(\w+))?\s*\}\}")
    IF_PATTERN = re.compile(r"\{%\s*if\s+(\w+)\s*%\}|\{%\s*else\s*%\}|\{%\s*endif\s*%\}")
    FOR_PATTERN = re.compile(r"\{%\s*for\s+(\w+)\s+in\s+(\w+)\s*%\}|\{%\s*endfor\s*%\}")
    INCLUDE_PATTERN = re.compile(r"\{%\s*include\s+['\"](\w+)['\"]")

    def __init__(self):
        self._templates: Dict[str, Template] = {}

    def register_template(self, template: Template) -> None:
        self._templates[template.template_id] = template

    def parse(self, template_str: str) -> List[TemplateNode]:
        nodes: List[TemplateNode] = []
        lines = template_str.split("\n")
        i = 0
        while i < len(lines):
            line = lines[i]
            var_match = self.VAR_PATTERN.search(line)
            if_match = self.IF_PATTERN.search(line)
            for_match = self.FOR_PATTERN.search(line)
            incl_match = self.INCLUDE_PATTERN.search(line)
            if var_match:
                nodes.append(self._parse_variable(var_match))
            elif if_match:
                nodes.append(self._parse_condition(lines, i))
            elif for_match:
                nodes.append(self._parse_loop(lines, i))
            elif incl_match:
                nodes.append(self._parse_include(incl_match))
            else:
                nodes.append(TemplateNode(TemplateToken.LITERAL, line))
            i += 1
        return nodes

    def _parse_variable(self, match: re.Match) -> TemplateNode:
        name = match.group(1)
        attr = match.group(2)
        filter_name = match.group(3)
        content = name
        if attr:
            content = f"{name}.{attr}"
        params = {}
        if filter_name:
            params["filter"] = filter_name
        return TemplateNode(TemplateToken.VARIABLE, content, params=params)

    def _parse_condition(self, lines: List[str], i: int) -> TemplateNode:
        match = self.IF_PATTERN.search(lines[i])
        condition_str = match.group(1) if match.group(1) else "else"
        children = [TemplateNode(TemplateToken.LITERAL, lines[i])]
        i += 1
        while i < len(lines) and "{% endif %}" not in lines[i]:
            children.append(TemplateNode(TemplateToken.LITERAL, lines[i]))
            i += 1
        return TemplateNode(TemplateToken.CONDITION, condition_str, children=children)

    def _parse_loop(self, lines: List[str], i: int) -> TemplateNode:
        match = self.FOR_PATTERN.search(lines[i])
        var_name = match.group(1)
        iterable = match.group(2)
        children = [TemplateNode(TemplateToken.LITERAL, lines[i])]
        i += 1
        while i < len(lines) and "{% endfor %}" not in lines[i]:
            children.append(TemplateNode(TemplateToken.LITERAL, lines[i]))
            i += 1
        return TemplateNode(TemplateToken.LOOP, f"{var_name} in {iterable}", children=children)

    def _parse_include(self, match: re.Match) -> TemplateNode:
        tpl_id = match.group(1)
        return TemplateNode(TemplateToken.INCLUDE, tpl_id)


class TemplateRenderer:
    """Renders parsed template nodes with variable context."""

    BUILT_IN_FILTERS = {
        "upper": lambda v: str(v).upper(),
        "lower": lambda v: str(v).lower(),
        "trim": lambda v: str(v).strip(),
        "default": lambda v, d="": v if v else d,
        "join": lambda v, sep=",": sep.join(str(x) for x in v) if isinstance(v, (list, tuple)) else str(v),
        "length": lambda v: len(v) if hasattr(v, "__len__") else 0,
        "first": lambda v: v[0] if isinstance(v, (list, tuple)) and len(v) > 0 else "",
        "last": lambda v: v[-1] if isinstance(v, (list, tuple)) and len(v) > 0 else "",
    }

    def __init__(self, parser: TemplateParser):
        self.parser = parser

    def render(
        self, template: Template, context: Optional[Dict[str, Any]] = None
    ) -> str:
        ctx = RenderContext(variables=context or {})
        nodes = self.parser.parse(template.body)
        return self._render_nodes(nodes, ctx)

    def _render_nodes(self, nodes: List[TemplateNode], ctx: RenderContext) -> str:
        output = []
        for node in nodes:
            output.append(self._render_node(node, ctx))
        return "\n".join(output)

    def _render_node(self, node: TemplateNode, ctx: RenderContext) -> str:
        if node.token_type == TemplateToken.LITERAL:
            return node.content
        elif node.token_type == TemplateToken.VARIABLE:
            return self._render_variable(node, ctx)
        elif node.token_type == TemplateToken.LOOP:
            return self._render_loop(node, ctx)
        elif node.token_type == TemplateToken.INCLUDE:
            return self._render_include(node, ctx)
        return node.content

    def _render_variable(self, node: TemplateNode, ctx: RenderContext) -> str:
        parts = node.content.split(".")
        value = ctx.variables.get(parts[0], "")
        if len(parts) > 1:
            for part in parts[1:]:
                if isinstance(value, dict):
                    value = value.get(part, "")
                elif hasattr(value, part):
                    value = getattr(value, part, "")
        if "filter" in node.params:
            filt = node.params["filter"]
            if filt in self.BUILT_IN_FILTERS:
                value = self.BUILT_IN_FILTERS[filt](value)
        return str(value) if value is not None else ""

    def _render_loop(self, node: TemplateNode, ctx: RenderContext) -> str:
        if ctx.loop_depth >= ctx.max_loop_depth:
            return ""
        ctx.loop_depth += 1
        output = []
        for item in node.children:
            output.append(self._render_node(item, ctx))
        ctx.loop_depth -= 1
        return "\n".join(output)

    def _render_include(self, node: TemplateNode, ctx: RenderContext) -> str:
        tpl = self.parser._templates.get(node.content)
        if not tpl:
            return f"[Include: {node.content} not found]"
        return self.render(tpl, ctx.variables)


class TemplateEngine:
    """
    Template engine for generating automation workflows.
    """

    def __init__(self):
        self.parser = TemplateParser()
        self.renderer = TemplateRenderer(self.parser)

    def register_template(
        self,
        template_id: str,
        name: str,
        body: str,
        variables: Optional[List[str]] = None,
    ) -> Template:
        tpl = Template(
            template_id=template_id,
            name=name,
            body=body,
            variables=variables or [],
        )
        self.parser.register_template(tpl)
        return tpl

    def render(
        self,
        template_id: str,
        context: Dict[str, Any],
    ) -> str:
        tpl = self.parser._templates.get(template_id)
        if not tpl:
            raise ValueError(f"Template not found: {template_id}")
        return self.renderer.render(tpl, context)

    def create_workflow_from_template(
        self,
        template_id: str,
        context: Dict[str, Any],
        output_format: str = "json",
    ) -> str:
        rendered = self.render(template_id, context)
        if output_format == "json":
            return json.dumps({"workflow": rendered, "context": context}, indent=2)
        return rendered

    def add_filter(self, name: str, func: Callable) -> None:
        self.renderer.BUILT_IN_FILTERS[name] = func
