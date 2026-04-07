"""Template rendering action module for RabAI AutoClick.

Provides template operations:
- TemplateRenderAction: Render template with data
- TemplateCompileAction: Pre-compile template
- TemplateFilterAction: Apply filters to template
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TemplateRenderAction(BaseAction):
    """Render template with data."""
    action_type = "template_render"
    display_name = "模板渲染"
    description = "渲染模板"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute template render."""
        template_str = params.get('template', '')
        template_file = params.get('template_file', None)
        data = params.get('data', {})
        engine = params.get('engine', 'jinja2')  # jinja2, string
        output_var = params.get('output_var', 'rendered_template')

        if not template_str and not template_file:
            return ActionResult(success=False, message="template or template_file is required")

        try:
            resolved_template = context.resolve_value(template_str) if context else template_str
            resolved_data = context.resolve_value(data) if context else data

            if template_file:
                resolved_file = context.resolve_value(template_file) if context else template_file
                with open(resolved_file, 'r') as f:
                    resolved_template = f.read()

            if engine.lower() == 'jinja2':
                try:
                    from jinja2 import Template
                    t = Template(resolved_template)
                    rendered = t.render(**(resolved_data or {}))
                except ImportError:
                    return ActionResult(success=False, message="jinja2 not installed. Run: pip install jinja2")
            else:
                # Simple string template with {{var}} syntax
                rendered = resolved_template
                for key, val in (resolved_data or {}).items():
                    placeholder = '{{' + key + '}}'
                    rendered = rendered.replace(placeholder, str(val))

            if context:
                context.set(output_var, rendered)
            return ActionResult(success=True, message=f"Template rendered ({len(rendered)} chars)", data={'output': rendered})
        except FileNotFoundError:
            return ActionResult(success=False, message=f"Template file not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Template render error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'template': '', 'template_file': None, 'data': {}, 'engine': 'jinja2', 'output_var': 'rendered_template'}


class TemplateCompileAction(BaseAction):
    """Pre-compile template."""
    action_type = "template_compile"
    display_name = "模板编译"
    description = "预编译模板"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute template compile."""
        template_str = params.get('template', '')
        template_file = params.get('template_file', None)
        engine = params.get('engine', 'jinja2')
        output_var = params.get('output_var', 'compiled_template')

        if not template_str and not template_file:
            return ActionResult(success=False, message="template or template_file is required")

        try:
            resolved_template = context.resolve_value(template_str) if context else template_str

            if template_file:
                resolved_file = context.resolve_value(template_file) if context else template_file
                with open(resolved_file, 'r') as f:
                    resolved_template = f.read()

            if engine.lower() == 'jinja2':
                from jinja2 import Template
                compiled = Template(resolved_template)
                compiled_str = str(compiled)
            else:
                compiled_str = resolved_template

            if context:
                context.set(output_var, compiled_str)
            return ActionResult(success=True, message="Template compiled", data={'compiled': compiled_str[:100]})
        except ImportError:
            return ActionResult(success=False, message="jinja2 not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Template compile error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'template': '', 'template_file': None, 'engine': 'jinja2', 'output_var': 'compiled_template'}


class TemplateFilterAction(BaseAction):
    """Apply template filters."""
    action_type = "template_filter"
    display_name = "模板过滤器"
    description = "应用模板过滤器"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute template filter."""
        value = params.get('value', '')
        filters = params.get('filters', [])  # ['upper', 'trim', 'length', etc]
        output_var = params.get('output_var', 'filtered_value')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved_value = context.resolve_value(value) if context else value
            resolved_filters = context.resolve_value(filters) if context else filters

            result = str(resolved_value)
            for f in resolved_filters:
                f_lower = f.lower()
                if f_lower == 'upper':
                    result = result.upper()
                elif f_lower == 'lower':
                    result = result.lower()
                elif f_lower == 'trim' or f_lower == 'strip':
                    result = result.strip()
                elif f_lower == 'length' or f_lower == 'len':
                    result = str(len(result))
                elif f_lower == 'capitalize':
                    result = result.capitalize()
                elif f_lower == 'title':
                    result = result.title()
                elif f_lower == 'reverse':
                    result = result[::-1]
                elif f_lower == 'md5':
                    import hashlib
                    result = hashlib.md5(result.encode()).hexdigest()
                elif f_lower == 'sha256':
                    import hashlib
                    result = hashlib.sha256(result.encode()).hexdigest()
                elif f_lower == 'base64_encode':
                    import base64
                    result = base64.b64encode(result.encode()).decode()
                elif f_lower == 'base64_decode':
                    import base64
                    result = base64.b64decode(result.encode()).decode()
                elif f_lower == 'url_encode':
                    from urllib.parse import quote
                    result = quote(result)
                elif f_lower == 'url_decode':
                    from urllib.parse import unquote
                    result = unquote(result)
                elif f_lower == 'json_dumps':
                    import json
                    result = json.dumps(result)
                elif f_lower == 'int':
                    result = str(int(result))
                elif f_lower == 'float':
                    result = str(float(result))

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Filtered: {result[:50]}", data={'output': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Template filter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'filters': [], 'output_var': 'filtered_value'}
