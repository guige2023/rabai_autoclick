"""Template utilities for RabAI AutoClick.

Provides:
- String template rendering
- Template caching and compilation
- Variable interpolation helpers
"""

import re
import string
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Pattern,
    Union,
)


class Template:
    """A string template with variable interpolation.

    Supports ${var}, $var, and {{var}} syntaxes.
    """

    _DEFAULT_PATTERN = re.compile(r"\$\{([^}]+)\}|\$(\w+)|\{\{([^}]+)\}\}")

    def __init__(
        self,
        template: str,
        pattern: Optional[Pattern[str]] = None,
    ) -> None:
        """Initialize template.

        Args:
            template: Template string.
            pattern: Custom regex pattern with groups for var names.
        """
        self._template = template
        self._pattern = pattern or self._DEFAULT_PATTERN

    def render(
        self,
        **kwargs: Any,
    ) -> str:
        """Render the template with given variables.

        Args:
            **kwargs: Variables for interpolation.

        Returns:
            Rendered string.
        """
        def replacer(match: re.Match) -> str:
            # Group 1: ${var}, Group 2: $var, Group 3: {{var}}
            var_name = match.group(1) or match.group(2) or match.group(3)
            return str(kwargs.get(var_name, match.group(0)))

        return self._pattern.sub(replacer, self._template)

    def render_mapping(
        self,
        mapping: Mapping[str, Any],
    ) -> str:
        """Render the template with a mapping.

        Args:
            mapping: Mapping of variable names to values.

        Returns:
            Rendered string.
        """
        return self.render(**dict(mapping))

    @property
    def template(self) -> str:
        """Original template string."""
        return self._template

    def __repr__(self) -> str:
        return f"Template({self._template!r})"


class MultiTemplate:
    """Template that can have different formats for different contexts."""

    def __init__(
        self,
        templates: Dict[str, str],
        default_key: str = "default",
    ) -> None:
        """Initialize multi-template.

        Args:
            templates: Dictionary of named templates.
            default_key: Key for default template.
        """
        self._templates = {k: Template(v) for k, v in templates.items()}
        self._default_key = default_key

    def render(
        self,
        key: str,
        **kwargs: Any,
    ) -> str:
        """Render a named template.

        Args:
            key: Template key.
            **kwargs: Variables for interpolation.

        Returns:
            Rendered string.
        """
        template = self._templates.get(key, self._templates[self._default_key])
        return template.render(**kwargs)

    def add_template(
        self,
        key: str,
        template: str,
    ) -> None:
        """Add a new template.

        Args:
            key: Template key.
            template: Template string.
        """
        self._templates[key] = Template(template)


def render_template(
    template: str,
    **kwargs: Any,
) -> str:
    """Render a template string with variables.

    Args:
        template: Template string.
        **kwargs: Variables for interpolation.

    Returns:
        Rendered string.
    """
    return Template(template).render(**kwargs)


def render_template_mapping(
    template: str,
    mapping: Mapping[str, Any],
) -> str:
    """Render a template string with a mapping.

    Args:
        template: Template string.
        mapping: Variables mapping.

    Returns:
        Rendered string.
    """
    return Template(template).render_mapping(mapping)


class TemplateFormatter(string.Formatter):
    """Custom string.Formatter with safety features."""

    def __init__(
        self,
        default: str = "",
        missing_key_raise: bool = False,
    ) -> None:
        """Initialize formatter.

        Args:
            default: Default value for missing keys.
            missing_key_raise: If True, raise KeyError for missing keys.
        """
        super().__init__()
        self._default = default
        self._missing_key_raise = missing_key_raise

    def get_value(
        self,
        arg: Any,
        format_spec: str,
        conversion: Optional[str],
    ) -> Any:
        if arg is None:
            if self._missing_key_raise:
                raise KeyError("Missing template variable")
            return self._default
        return super().get_value(arg, format_spec, conversion)


def safe_format(
    template: str,
    **kwargs: Any,
) -> str:
    """Safely format a string, missing keys use empty string.

    Args:
        template: Format string.
        **kwargs: Variables.

    Returns:
        Formatted string.
    """
    formatter = TemplateFormatter(default="", missing_key_raise=False)
    return formatter.format(template, **kwargs)


def interpolate(
    text: str,
    variables: Dict[str, Any],
    prefix: str = "${",
    suffix: str = "}",
) -> str:
    """Simple variable interpolation.

    Args:
        text: Text with placeholders like ${name}.
        variables: Dictionary of variable values.
        prefix: Placeholder prefix.
        suffix: Placeholder suffix.

    Returns:
        Interpolated text.
    """
    result = text
    for key, value in variables.items():
        placeholder = f"{prefix}{key}{suffix}"
        result = result.replace(placeholder, str(value))
    return result


def extract_variables(
    template: str,
    pattern: Optional[Pattern[str]] = None,
) -> list[str]:
    """Extract variable names from a template.

    Args:
        template: Template string.
        pattern: Custom regex pattern.

    Returns:
        List of variable names found.
    """
    if pattern is None:
        pattern = Template._DEFAULT_PATTERN

    matches = pattern.findall(template)
    vars_list: list[str] = []
    seen: set[str] = set()

    for match in matches:
        var_name = match[0] or match[1] or match[2]
        if var_name and var_name not in seen:
            vars_list.append(var_name)
            seen.add(var_name)

    return vars_list


class LazyTemplate:
    """Template that evaluates variables lazily on render."""

    def __init__(self, template: str) -> None:
        self._template = template
        self._vars: Dict[str, Callable[[], Any]] = {}

    def set_lazy(self, name: str, getter: Callable[[], Any]) -> "LazyTemplate":
        """Set a lazy variable that calls a function on render.

        Args:
            name: Variable name.
            getter: Function to call for value.

        Returns:
            self for chaining.
        """
        self._vars[name] = getter
        return self

    def render(self, **kwargs: Any) -> str:
        """Render with both lazy and eager variables."""
        all_vars = dict(self._vars)
        all_vars.update(kwargs)
        return Template(self._template).render(**all_vars)


def batch_render(
    template: str,
    items: list[Dict[str, Any]],
) -> list[str]:
    """Render a template for multiple items.

    Args:
        template: Template string.
        items: List of variable dictionaries.

    Returns:
        List of rendered strings.
    """
    compiled = Template(template)
    return [compiled.render(**item) for item in items]
