"""
Automation Template Action Module.

Provides template management and rendering
for workflow automation templates.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
import re

logger = logging.getLogger(__name__)


class TemplateType(Enum):
    """Template types."""
    WORKFLOW = "workflow"
    EMAIL = "email"
    REPORT = "report"
    NOTIFICATION = "notification"
    WEBHOOK = "webhook"


@dataclass
class TemplateVariable:
    """Template variable definition."""
    name: str
    variable_type: str
    default_value: Any = None
    required: bool = False
    description: str = ""


@dataclass
class Template:
    """Automation template."""
    template_id: str
    name: str
    template_type: TemplateType
    content: str
    variables: List[TemplateVariable] = field(default_factory=list)
    version: str = "1.0"
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class RenderedTemplate:
    """Rendered template output."""
    template_id: str
    content: str
    variables_used: Dict[str, Any]
    rendered_at: datetime = field(default_factory=datetime.now)


class TemplateRenderer:
    """Renders templates with variables."""

    def __init__(self):
        self.filters: Dict[str, Callable] = {}
        self._register_default_filters()

    def _register_default_filters(self):
        """Register default template filters."""
        self.filters["upper"] = lambda x: str(x).upper() if x else ""
        self.filters["lower"] = lambda x: str(x).lower() if x else ""
        self.filters["title"] = lambda x: str(x).title() if x else ""
        self.filters["trim"] = lambda x: str(x).strip() if x else ""
        self.filters["default"] = lambda x, y="": x if x else y
        self.filters["join"] = lambda x, y=",": y.join(str(v) for v in x) if x else ""

    def register_filter(self, name: str, filter_func: Callable):
        """Register a custom filter."""
        self.filters[name] = filter_func

    def render(
        self,
        template: Template,
        variables: Dict[str, Any]
    ) -> RenderedTemplate:
        """Render a template with variables."""
        content = template.content

        missing_vars = []
        for var in template.variables:
            if var.required and var.name not in variables:
                if var.default_value is None:
                    missing_vars.append(var.name)

        if missing_vars:
            raise ValueError(f"Missing required variables: {missing_vars}")

        used_vars = {}
        for var in template.variables:
            value = variables.get(var.name, var.default_value)
            used_vars[var.name] = value
            content = content.replace(f"{{{{{var.name}}}}}", str(value))

        content = self._render_filters(content)

        return RenderedTemplate(
            template_id=template.template_id,
            content=content,
            variables_used=used_vars
        )

    def _render_filters(self, content: str) -> str:
        """Render filters in content."""
        pattern = r'\{\{([^|}]+)\|(\w+)(?::([^}]*))?\}\}'

        def replace_filter(match):
            var_name = match.group(1).strip()
            filter_name = match.group(2)
            filter_args = match.group(3)

            filter_func = self.filters.get(filter_name)
            if not filter_func:
                return match.group(0)

            try:
                if filter_args:
                    return str(filter_func(var_name, *filter_args.split(",")))
                return str(filter_func(var_name))
            except:
                return match.group(0)

        return re.sub(pattern, replace_filter, content)


class TemplateManager:
    """Manages templates."""

    def __init__(self, renderer: TemplateRenderer):
        self.templates: Dict[str, Template] = {}
        self.renderer = renderer
        self._handlers: Dict[TemplateType, Callable] = {}

    def add_template(self, template: Template):
        """Add a template."""
        self.templates[template.template_id] = template

    def remove_template(self, template_id: str) -> bool:
        """Remove a template."""
        if template_id in self.templates:
            del self.templates[template_id]
            return True
        return False

    def get_template(self, template_id: str) -> Optional[Template]:
        """Get a template by ID."""
        return self.templates.get(template_id)

    def list_templates(
        self,
        template_type: Optional[TemplateType] = None
    ) -> List[Template]:
        """List templates."""
        templates = self.templates.values()
        if template_type:
            templates = [t for t in templates if t.template_type == template_type]
        return list(templates)

    def register_handler(self, template_type: TemplateType, handler: Callable):
        """Register handler for template type."""
        self._handlers[template_type] = handler

    def render(
        self,
        template_id: str,
        variables: Dict[str, Any]
    ) -> RenderedTemplate:
        """Render a template."""
        template = self.templates.get(template_id)
        if not template:
            raise ValueError(f"Template not found: {template_id}")

        return self.renderer.render(template, variables)

    async def render_and_execute(
        self,
        template_id: str,
        variables: Dict[str, Any]
    ) -> Any:
        """Render template and execute handler."""
        rendered = self.render(template_id, variables)
        template = self.templates[template_id]

        handler = self._handlers.get(template.template_type)
        if not handler:
            return rendered.content

        if asyncio.iscoroutinefunction(handler):
            return await handler(rendered)
        return handler(rendered)


def main():
    """Demonstrate template management."""
    renderer = TemplateRenderer()
    manager = TemplateManager(renderer)

    template = Template(
        template_id="t1",
        name="Welcome Email",
        template_type=TemplateType.EMAIL,
        content="Hello {{name}}, welcome to {{company}}!",
        variables=[
            TemplateVariable(name="name", variable_type="string", required=True),
            TemplateVariable(name="company", variable_type="string", default_value="Our Company")
        ]
    )

    manager.add_template(template)

    rendered = manager.render("t1", {"name": "Alice"})
    print(f"Rendered: {rendered.content}")


if __name__ == "__main__":
    main()
