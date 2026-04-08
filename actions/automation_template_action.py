"""
Automation Template Action Module

Provides template rendering, management, and automation for templated workflows.
"""
from typing import Any, Optional, Callable, Literal
from dataclasses import dataclass, field
from datetime import datetime
from string import Template
import re
import json


@dataclass
class TemplateVariable:
    """Definition of a template variable."""
    name: str
    variable_type: type
    required: bool = True
    default: Any = None
    description: str = ""
    validation: Optional[Callable[[Any], bool]] = None
    transform: Optional[Callable[[Any], Any]] = None


@dataclass
class TemplateDefinition:
    """Definition of a template."""
    name: str
    template_str: str
    variables: dict[str, TemplateVariable]
    output_format: str = "text"  # text, json, yaml, html, markdown
    description: str = ""
    tags: list[str] = field(default_factory=list)
    version: str = "1.0.0"
    author: Optional[str] = None


@dataclass
class RenderResult:
    """Result of template rendering."""
    success: bool
    output: str
    variables_used: dict[str, Any]
    errors: list[str]
    warnings: list[str]
    duration_ms: float


class AutomationTemplateAction:
    """Main template automation action handler."""
    
    def __init__(self):
        self._templates: dict[str, TemplateDefinition] = {}
        self._variable_resolvers: dict[str, Callable[[str, Any], Any]] = {}
        self._render_stats: dict[str, dict] = {}
    
    def register_template(
        self,
        name: str,
        template_str: str,
        variables: Optional[dict[str, TemplateVariable]] = None,
        output_format: str = "text",
        description: str = "",
        tags: Optional[list[str]] = None,
        version: str = "1.0.0"
    ) -> "AutomationTemplateAction":
        """Register a template."""
        self._templates[name] = TemplateDefinition(
            name=name,
            template_str=template_str,
            variables=variables or {},
            output_format=output_format,
            description=description,
            tags=tags or [],
            version=version
        )
        return self
    
    def register_variable_resolver(
        self,
        name: str,
        resolver: Callable[[str, Any], Any]
    ) -> "AutomationTemplateAction":
        """Register a custom variable resolver."""
        self._variable_resolvers[name] = resolver
        return self
    
    async def render(
        self,
        template_name: str,
        variables: dict[str, Any],
        strict: bool = True
    ) -> RenderResult:
        """
        Render a template with variables.
        
        Args:
            template_name: Name of registered template
            variables: Variable values to substitute
            strict: If True, fail on missing required variables
            
        Returns:
            RenderResult with rendered output
        """
        start_time = datetime.now()
        errors = []
        warnings = []
        used_vars = {}
        
        if template_name not in self._templates:
            return RenderResult(
                success=False,
                output="",
                variables_used={},
                errors=[f"Template '{template_name}' not found"],
                warnings=[],
                duration_ms=0
            )
        
        template_def = self._templates[template_name]
        
        # Validate required variables
        for var_name, var_def in template_def.variables.items():
            if var_def.required and var_name not in variables:
                if var_def.default is None and strict:
                    errors.append(f"Required variable '{var_name}' is missing")
                elif var_def.default is not None:
                    variables[var_name] = var_def.default
                    warnings.append(f"Using default value for '{var_name}'")
        
        if errors and strict:
            return RenderResult(
                success=False,
                output="",
                variables_used={},
                errors=errors,
                warnings=warnings,
                duration_ms=0
            )
        
        # Process variables
        processed_vars = {}
        for var_name, var_def in template_def.variables.items():
            value = variables.get(var_name, var_def.default)
            
            # Apply transform if defined
            if var_def.transform and value is not None:
                try:
                    value = var_def.transform(value)
                except Exception as e:
                    errors.append(f"Transform failed for '{var_name}': {e}")
                    if strict:
                        return RenderResult(
                            success=False,
                            output="",
                            variables_used={},
                            errors=errors,
                            warnings=warnings,
                            duration_ms=0
                        )
            
            # Apply validation if defined
            if var_def.validation and value is not None:
                try:
                    if not var_def.validation(value):
                        errors.append(f"Validation failed for '{var_name}'")
                except Exception as e:
                    errors.append(f"Validation error for '{var_name}': {e}")
            
            processed_vars[var_name] = value
            used_vars[var_name] = value
        
        # Apply resolvers
        for var_name, value in processed_vars.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                resolver_name = value[2:-1]
                if resolver_name in self._variable_resolvers:
                    try:
                        processed_vars[var_name] = self._variable_resolvers[resolver_name](
                            var_name, variables
                        )
                    except Exception as e:
                        errors.append(f"Resolver failed for '{var_name}': {e}")
        
        # Render template
        try:
            if template_def.output_format == "json":
                output = self._render_json_template(template_def.template_str, processed_vars)
            elif template_def.output_format == "yaml":
                output = self._render_yaml_template(template_def.template_str, processed_vars)
            elif template_def.output_format == "html":
                output = self._render_html_template(template_def.template_str, processed_vars)
            elif template_def.output_format == "markdown":
                output = self._render_markdown_template(template_def.template_str, processed_vars)
            else:
                output = self._render_string_template(template_def.template_str, processed_vars)
        except Exception as e:
            errors.append(f"Template rendering failed: {e}")
            output = ""
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        # Update stats
        self._update_stats(template_name)
        
        return RenderResult(
            success=len(errors) == 0,
            output=output,
            variables_used=used_vars,
            errors=errors,
            warnings=warnings,
            duration_ms=duration_ms
        )
    
    def _render_string_template(self, template_str: str, variables: dict[str, Any]) -> str:
        """Render using Python string.Template."""
        template = Template(template_str)
        return template.safe_substitute(variables)
    
    def _render_json_template(self, template_str: str, variables: dict[str, Any]) -> str:
        """Render JSON template with variable substitution."""
        # Support ${variable} syntax in JSON
        result = template_str
        for var_name, value in variables.items():
            placeholder = f"${{{var_name}}}"
            if placeholder in result:
                result = result.replace(placeholder, json.dumps(value))
        return result
    
    def _render_yaml_template(self, template_str: str, variables: dict[str, Any]) -> str:
        """Render YAML template with variable substitution."""
        result = template_str
        for var_name, value in variables.items():
            placeholder = f"${{{var_name}}}"
            if placeholder in result:
                # Convert to YAML-compatible string
                if isinstance(value, (dict, list)):
                    import yaml
                    result = result.replace(placeholder, yaml.dump(value, default_flow_style=True).strip())
                else:
                    result = result.replace(placeholder, str(value))
        return result
    
    def _render_html_template(self, template_str: str, variables: dict[str, Any]) -> str:
        """Render HTML template with variable substitution."""
        result = template_str
        for var_name, value in variables.items():
            placeholder = f"${{{var_name}}}"
            if placeholder in result:
                if isinstance(value, dict):
                    # Render nested object as JSON
                    result = result.replace(placeholder, json.dumps(value))
                else:
                    result = result.replace(placeholder, str(value))
        return result
    
    def _render_markdown_template(self, template_str: str, variables: dict[str, Any]) -> str:
        """Render Markdown template with variable substitution."""
        result = template_str
        for var_name, value in variables.items():
            placeholder = f"${{{var_name}}}"
            if placeholder in result:
                if isinstance(value, list):
                    # Render list as markdown items
                    md_list = "\n".join(f"- {item}" for item in value)
                    result = result.replace(placeholder, md_list)
                elif isinstance(value, dict):
                    # Render dict as markdown table
                    if value:
                        md_table = "| Key | Value |\n|---|---|\n"
                        for k, v in value.items():
                            md_table += f"| {k} | {v} |\n"
                        result = result.replace(placeholder, md_table)
                else:
                    result = result.replace(placeholder, str(value))
        return result
    
    def _update_stats(self, template_name: str):
        """Update rendering statistics."""
        if template_name not in self._render_stats:
            self._render_stats[template_name] = {
                "render_count": 0,
                "total_duration_ms": 0,
                "last_rendered": None
            }
        
        stats = self._render_stats[template_name]
        stats["render_count"] += 1
        stats["last_rendered"] = datetime.now().isoformat()
    
    async def render_inline(
        self,
        template_str: str,
        variables: dict[str, Any],
        syntax: Literal["simple", "jinja2", "mustache"] = "simple"
    ) -> str:
        """
        Render an inline template string.
        
        Args:
            template_str: Template string to render
            variables: Variables for substitution
            syntax: Template syntax to use
            
        Returns:
            Rendered string
        """
        if syntax == "simple":
            return self._render_string_template(template_str, variables)
        elif syntax == "jinja2":
            return self._render_jinja_template(template_str, variables)
        elif syntax == "mustache":
            return self._render_mustache_template(template_str, variables)
        return template_str
    
    def _render_jinja_template(self, template_str: str, variables: dict[str, Any]) -> str:
        """Render using Jinja2-like syntax."""
        try:
            from jinja2 import Template as JinjaTemplate
            return JinjaTemplate(template_str).render(**variables)
        except ImportError:
            # Fall back to simple template
            return self._render_string_template(template_str, variables)
    
    def _render_mustache_template(self, template_str: str, variables: dict[str, Any]) -> str:
        """Render using Mustache-like syntax."""
        # Simple mustache implementation
        result = template_str
        
        # Handle conditionals {{#var}}...{{/var}}
        pattern = r'\{\{#(\w+)\}\}(.*?)\{\{/\1\}\}'
        
        def handle_conditional(match):
            var_name, content = match.groups()
            if variables.get(var_name):
                return content
            return ""
        
        result = re.sub(pattern, handle_conditional, result, flags=re.DOTALL)
        
        # Handle simple substitution {{var}}
        for var_name, value in variables.items():
            placeholder = f"{{{{{var_name}}}}}"
            if placeholder in result:
                if isinstance(value, (dict, list)):
                    result = result.replace(placeholder, json.dumps(value))
                else:
                    result = result.replace(placeholder, str(value))
        
        return result
    
    def get_template(self, name: str) -> Optional[TemplateDefinition]:
        """Get template definition."""
        return self._templates.get(name)
    
    def list_templates(
        self,
        tag: Optional[str] = None,
        include_hidden: bool = False
    ) -> list[TemplateDefinition]:
        """List registered templates."""
        templates = list(self._templates.values())
        
        if tag:
            templates = [t for t in templates if tag in t.tags]
        
        return templates
    
    def delete_template(self, name: str) -> bool:
        """Delete a registered template."""
        if name in self._templates:
            del self._templates[name]
            return True
        return False
    
    def get_render_stats(self, template_name: Optional[str] = None) -> dict[str, Any]:
        """Get rendering statistics."""
        if template_name:
            return self._render_stats.get(template_name, {})
        return dict(self._render_stats)
