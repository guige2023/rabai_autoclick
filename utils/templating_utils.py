"""
Template rendering utilities.

Provides template engines with variable substitution, conditionals,
loops, filters, and inheritance support.
"""

from __future__ import annotations

import re
import json
from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field


@dataclass
class TemplateContext:
    """Context for template rendering with variable access."""
    data: dict[str, Any]
    filters: dict[str, Callable[..., Any]] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        keys = key.split(".")
        value = self.data
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif isinstance(value, list) and k.isdigit():
                idx = int(k)
                value = value[idx] if idx < len(value) else None
            else:
                return default
            if value is None:
                return default
        return value

    def apply_filter(self, value: Any, filter_name: str, *args: Any) -> Any:
        filter_func = self.filters.get(filter_name)
        if filter_func:
            return filter_func(value, *args)
        return value


class TemplateEngine:
    """Simple but powerful template engine."""

    def __init__(self, template: str):
        self.template = template
        self._blocks: dict[str, str] = {}
        self._parent: Optional[str] = None
        self._filters: dict[str, Callable[..., Any]] = {}
        self._register_default_filters()

    def _register_default_filters(self) -> None:
        self._filters = {
            "upper": lambda v: str(v).upper(),
            "lower": lambda v: str(v).lower(),
            "trim": lambda v: str(v).strip(),
            "length": lambda v: len(v),
            "default": lambda v, d="": v if v else d,
            "json": lambda v: json.dumps(v) if not isinstance(v, str) else v,
            "join": lambda v, sep=", ": return sep.join(str(x) for x in v) if hasattr(v, "__iter__") else str(v),
            "int": lambda v: int(v) if v else 0,
            "float": lambda v: float(v) if v else 0.0,
            "bool": lambda v: bool(v),
        }

    def add_filter(self, name: str, func: Callable[..., Any]) -> TemplateEngine:
        self._filters[name] = func
        return self

    def render(self, **variables: Any) -> str:
        """Render the template with provided variables."""
        context = TemplateContext(variables, self._filters)
        return self._render(self.template, context)

    def _render(self, text: str, context: TemplateContext) -> str:
        """Internal render method with context object."""
        text = self._render_extends(text, context)
        text = self._render_for_loops(text, context)
        text = self._render_conditionals(text, context)
        text = self._render_variables(text, context)
        text = self._render_filters(text, context)
        return text

    VAR_PATTERN = re.compile(r"\{\{(\s*[^{}]+?\s*)\}\}")
    FILTER_PATTERN = re.compile(r"\{\{(\s*[^{}]+?)\s*\|(\s*\w+(?:\([^)]*\))?\s*)\}\}")
    FOR_PATTERN = re.compile(
        r"\{% for (\w+) in ([^{}%]+) %\}([\s\S]*?)\{% endfor %\}",
        re.MULTILINE,
    )
    IF_PATTERN = re.compile(
        r"\{% if (\w+)(?:\s*(==|!=|>|<|>=|<=)\s*([^{}%]+))? %\}([\s\S]*?)(?:\{% else %\}([\s\S]*?))?\{% endif %\}",
        re.MULTILINE,
    )

    def _render_extends(self, text: str, context: TemplateContext) -> str:
        m = re.search(r"\{% extends ['\"]([^'\"]+) ['\"] %\}", text)
        if m:
            parent_name = m.group(1).strip()
            parent_blocks = self._blocks.get(parent_name, {})
            inner = text[m.end():]
            for name, block_content in parent_blocks.items():
                placeholder = f"{{{{ block.{name} }}}}"
                inner = inner.replace(placeholder, block_content)
            return inner
        return text

    def _render_variables(self, text: str, context: TemplateContext) -> str:
        def replacer(m: re.Match) -> str:
            expr = m.group(1).strip()
            value = self._eval_expr(expr, context)
            return str(value) if value is not None else ""

        return self.VAR_PATTERN.sub(replacer, text)

    def _render_filters(self, text: str, context: TemplateContext) -> str:
        def replacer(m: re.Match) -> str:
            expr = m.group(1).strip()
            filter_spec = m.group(2).strip()
            value = self._eval_expr(expr, context)
            value = self._apply_filter_chain(value, filter_spec)
            return str(value) if value is not None else ""

        return self.FILTER_PATTERN.sub(replacer, text)

    def _apply_filter_chain(self, value: Any, filter_spec: str) -> Any:
        filters = filter_spec.split("|")[1:]
        for f in filters:
            f = f.strip()
            m = re.match(r"(\w+)(?:\(([^)]*)\))?", f)
            if m:
                fname = m.group(1)
                fargs_str = m.group(2) or ""
                fargs = [x.strip() for x in fargs_str.split(",")] if fargs_str else []
                filter_func = self._filters.get(fname)
                if filter_func:
                    fargs = [value] + fargs
                    value = filter_func(*fargs)
                else:
                    fargs.insert(0, value)
                    return value
        return value

    def _render_for_loops(self, text: str, context: TemplateContext) -> str:
        def for_replacer(m: re.Match) -> str:
            var_name = m.group(1).strip()
            iterable_expr = m.group(2).strip()
            loop_body = m.group(3)
            items = self._eval_expr(iterable_expr, context)
            if not hasattr(items, "__iter__"):
                return ""
            result_lines = []
            for idx, item in enumerate(items):
                loop_context_data = dict(context.data)
                loop_context_data[var_name] = item
                loop_context_data["loop"] = {"index": idx, "index1": idx + 1, "first": idx == 0, "last": idx == len(items) - 1}
                loop_context = TemplateContext(loop_context_data, context.filters)
                rendered = self._render(loop_body, loop_context)
                result_lines.append(rendered)
            return "\n".join(result_lines)

        return self.FOR_PATTERN.sub(for_replacer, text)

    def _render_conditionals(self, text: str, context: TemplateContext) -> str:
        def if_replacer(m: re.Match) -> str:
            var_name = m.group(1).strip()
            operator = m.group(2)
            compare_value = m.group(3)
            if_body = m.group(4)
            else_body = m.group(5) or ""

            if operator and compare_value:
                actual = self._eval_expr(var_name, context)
                compare_value = compare_value.strip().strip("'\"")
                condition_met = self._compare(actual, operator, compare_value)
            else:
                condition_met = bool(context.get(var_name))

            chosen_body = if_body if condition_met else else_body
            return self._render(chosen_body, context)

        return self.IF_PATTERN.sub(if_replacer, text)

    def _eval_expr(self, expr: str, context: TemplateContext) -> Any:
        expr = expr.strip()
        if "." in expr:
            parts = expr.split(".")
            value = context.get(parts[0])
            for part in parts[1:]:
                if isinstance(value, dict):
                    value = value.get(part)
                elif isinstance(value, list) and part.isdigit():
                    value = value[int(part)] if int(part) < len(value) else None
                else:
                    return None
            return value
        return context.get(expr, None)

    def _compare(self, actual: Any, operator: str, expected: Any) -> bool:
        ops = {
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b,
        }
        op_func = ops.get(operator, ops["=="])
        try:
            return op_func(actual, expected)
        except TypeError:
            return False


def render(template: str, **variables: Any) -> str:
    """Convenience function to render a template string."""
    return TemplateEngine(template).render(**variables)


def from_file(path: str) -> TemplateEngine:
    """Load a template from a file."""
    with open(path) as f:
        return TemplateEngine(f.read())
