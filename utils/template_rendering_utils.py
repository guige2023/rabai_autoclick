"""
Template Rendering Utilities for UI Automation.

This module provides utilities for rendering templates with dynamic content,
useful for generating test data, reports, and configuration files.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


class TemplateSyntax(Enum):
    """Template syntax types."""
    SIMPLE = auto()     # ${variable}
    JINJA2 = auto()     # {{ variable }}
    PERCENT = auto()     # %(variable)s


@dataclass
class Template:
    """
    A template for rendering content.
    
    Attributes:
        template_str: The template string
        syntax: Syntax type to use
        strict: Whether to raise error on missing variables
    """
    template_str: str
    syntax: TemplateSyntax = TemplateSyntax.SIMPLE
    strict: bool = False
    
    def render(self, **kwargs) -> str:
        """
        Render the template with provided variables.
        
        Args:
            **kwargs: Variables to substitute
            
        Returns:
            Rendered string
        """
        if self.syntax == TemplateSyntax.SIMPLE:
            return self._render_simple(**kwargs)
        elif self.syntax == TemplateSyntax.JINJA2:
            return self._render_jinja2(**kwargs)
        elif self.syntax == TemplateSyntax.PERCENT:
            return self._render_percent(**kwargs)
        return self.template_str
    
    def _render_simple(self, **kwargs) -> str:
        """Render simple ${variable} syntax."""
        def replace(match):
            key = match.group(1)
            if key in kwargs:
                return str(kwargs[key])
            if self.strict:
                raise ValueError(f"Missing variable: {key}")
            return match.group(0)  # Keep placeholder if not found
        
        pattern = r'\$\{([^}]+)\}'
        return re.sub(pattern, replace, self.template_str)
    
    def _render_jinja2(self, **kwargs) -> str:
        """Render Jinja2-like {{ variable }} syntax."""
        def replace(match):
            key = match.group(1).strip()
            if key in kwargs:
                return str(kwargs[key])
            if self.strict:
                raise ValueError(f"Missing variable: {key}")
            return match.group(0)
        
        pattern = r'\{\{\s*([^}]+)\s*\}\}'
        return re.sub(pattern, replace, self.template_str)
    
    def _render_percent(self, **kwargs) -> str:
        """Render %(variable)s syntax."""
        def replace(match):
            key = match.group(1)
            if key in kwargs:
                return str(kwargs[key])
            if self.strict:
                raise ValueError(f"Missing variable: {key}")
            return match.group(0)
        
        pattern = r'%\(([^)]+)\)s'
        return re.sub(pattern, replace, self.template_str)


class TemplateEngine:
    """
    Template engine with variable management.
    
    Example:
        engine = TemplateEngine()
        engine.set("name", "World")
        engine.set("greeting", Template("${name}!"))
        
        result = engine.render("Hello ${name}!")
    """
    
    def __init__(self):
        self._variables: dict[str, Any] = {}
        self._templates: dict[str, Template] = {}
    
    def set(self, key: str, value: Any) -> None:
        """Set a variable."""
        self._variables[key] = value
    
    def set_template(self, name: str, template_str: str) -> None:
        """Define a named template."""
        self._templates[name] = Template(template_str)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a variable."""
        return self._variables.get(key, default)
    
    def render(self, template_str: str, **kwargs) -> str:
        """
        Render a template string.
        
        Args:
            template_str: Template to render
            **kwargs: Additional variables
            
        Returns:
            Rendered string
        """
        # Combine engine variables with kwargs
        variables = {**self._variables, **kwargs}
        
        # Find and render named templates first
        for name, template in self._templates.items():
            if f'${{{name}}}' in template_str or f'{{{{{name}}}}}' in template_str:
                template_str = template_str.replace(f'${{{name}}}', str(template.render(**variables)))
                template_str = template_str.replace(f'{{{{{name}}}}}', str(template.render(**variables)))
        
        # Render main template
        result = Template(template_str).render(**variables)
        
        return result
    
    def render_named(self, name: str, **kwargs) -> str:
        """
        Render a named template.
        
        Args:
            name: Template name
            **kwargs: Additional variables
            
        Returns:
            Rendered string
        """
        template = self._templates.get(name)
        if not template:
            raise ValueError(f"Template not found: {name}")
        
        variables = {**self._variables, **kwargs}
        return template.render(**variables)
    
    def clear(self) -> None:
        """Clear all variables and templates."""
        self._variables.clear()
        self._templates.clear()


@dataclass
class TestDataTemplate:
    """
    Template for generating test data.
    
    Example:
        template = TestDataTemplate(
            name="user",
            template="User: ${first_name} ${last_name}",
            generators={
                "first_name": lambda: random_name(),
                "last_name": lambda: random_name()
            }
        )
        
        for i in range(10):
            user = template.generate()
            print(user)
    """
    name: str
    template_str: str
    generators: dict[str, Callable[[], Any]] = field(default_factory=dict)
    
    def generate(self, **static_values) -> str:
        """
        Generate a single test data instance.
        
        Args:
            **static_values: Override generators with static values
            
        Returns:
            Generated string
        """
        values = {}
        
        for key, generator in self.generators.items():
            if key in static_values:
                values[key] = static_values[key]
            else:
                values[key] = generator()
        
        template = Template(self.template_str)
        return template.render(**values)
    
    def generate_batch(self, count: int, **static_values) -> list[str]:
        """
        Generate multiple test data instances.
        
        Args:
            count: Number of instances to generate
            **static_values: Override generators with static values
            
        Returns:
            List of generated strings
        """
        return [self.generate(**static_values) for _ in range(count)]


class HtmlReportBuilder:
    """
    HTML report builder using templates.
    
    Example:
        builder = HtmlReportBuilder()
        builder.add_header("Test Report")
        builder.add_table(headers=["Name", "Status"], rows=data)
        html = builder.build()
    """
    
    def __init__(self):
        self._sections: list[str] = []
    
    def add_header(self, text: str, level: int = 1) -> 'HtmlReportBuilder':
        """Add a header."""
        self._sections.append(f"<h{level}>{text}</h{level}>")
        return self
    
    def add_paragraph(self, text: str) -> 'HtmlReportBuilder':
        """Add a paragraph."""
        self._sections.append(f"<p>{text}</p>")
        return self
    
    def add_table(
        self,
        headers: list[str],
        rows: list[list[Any]],
        classes: Optional[dict[str, str]] = None
    ) -> 'HtmlReportBuilder':
        """
        Add a table.
        
        Args:
            headers: Column headers
            rows: Data rows
            classes: Optional CSS classes (header, row_odd, row_even)
        """
        header_html = "".join(f"<th>{h}</th>" for h in headers)
        
        row_htmls = []
        for i, row in enumerate(rows):
            row_class = ""
            if classes:
                if i % 2 == 0 and "row_even" in classes:
                    row_class = f' class="{classes["row_even"]}"'
                elif i % 2 == 1 and "row_odd" in classes:
                    row_class = f' class="{classes["row_odd"]}"'
            
            cells = "".join(f"<td>{cell}</td>" for cell in row)
            row_htmls.append(f"<tr{row_class}>{cells}</tr>")
        
        table_html = f"""
        <table>
            <thead><tr>{header_html}</tr></thead>
            <tbody>{"".join(row_htmls)}</tbody>
        </table>
        """
        
        self._sections.append(table_html)
        return self
    
    def add_list(self, items: list[str], ordered: bool = False) -> 'HtmlReportBuilder':
        """Add a list."""
        tag = "ol" if ordered else "ul"
        items_html = "".join(f"<li>{item}</li>" for item in items)
        self._sections.append(f"<{tag}>{items_html}</{tag}>")
        return self
    
    def build(self) -> str:
        """Build the final HTML."""
        return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        .row_even {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    {"".join(self._sections)}
</body>
</html>
"""
