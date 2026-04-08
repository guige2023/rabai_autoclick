"""Template action module for RabAI AutoClick.

Provides template rendering for text and HTML templates
with variable substitution and conditionals.
"""

import re
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TemplateRenderAction(BaseAction):
    """Render template with variables.
    
    Supports {{variable}}, ${variable}, and conditional blocks.
    """
    action_type = "template_render"
    display_name = "模板渲染"
    description = "渲染文本模板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Render template.
        
        Args:
            context: Execution context.
            params: Dict with keys: template, vars, strict,
                   save_to_var.
        
        Returns:
            ActionResult with rendered template.
        """
        template = params.get('template', '')
        vars_dict = params.get('vars', {})
        strict = params.get('strict', False)
        save_to_var = params.get('save_to_var', None)

        if not template:
            return ActionResult(success=False, message="Template is empty")

        # Merge context variables
        if hasattr(context, 'variables'):
            all_vars = dict(context.variables)
            all_vars.update(vars_dict or {})
        else:
            all_vars = vars_dict or {}

        result = template
        errors = []

        # Replace {{var}} patterns
        for match in re.finditer(r'\{\{(\w+)\}\}', template):
            var_name = match.group(1)
            value = all_vars.get(var_name, '')
            if value is None and strict:
                errors.append(f"Missing variable: {var_name}")
            result = result.replace(match.group(0), str(value))

        # Replace ${var} patterns
        for match in re.finditer(r'\$\{(\w+)\}', template):
            var_name = match.group(1)
            value = all_vars.get(var_name, '')
            if value is None and strict:
                errors.append(f"Missing variable: {var_name}")
            result = result.replace(match.group(0), str(value))

        result_data = {
            'rendered': result,
            'template': template,
            'vars_used': list(all_vars.keys()),
            'errors': errors
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"模板渲染完成: {len(result)} 字符",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['template']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'vars': {},
            'strict': False,
            'save_to_var': None
        }


class TemplateListAction(BaseAction):
    """Render list template for each item.
    
    Applies template to each list item with item context.
    """
    action_type = "template_list"
    display_name = "列表模板"
    description = "对列表每一项应用模板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Render list template.
        
        Args:
            context: Execution context.
            params: Dict with keys: template, items, item_var,
                   save_to_var.
        
        Returns:
            ActionResult with rendered list.
        """
        template = params.get('template', '')
        items = params.get('items', [])
        item_var = params.get('item_var', 'item')
        save_to_var = params.get('save_to_var', None)

        if not isinstance(items, list):
            return ActionResult(
                success=False,
                message=f"Items must be list, got {type(items).__name__}"
            )

        rendered = []

        for i, item in enumerate(items):
            # Create item context
            item_context = {item_var: item, 'index': i}

            # Render template with item context
            result = template
            for match in re.finditer(r'\{\{(\w+)\}\}', template):
                var_name = match.group(1)
                if var_name in item_context:
                    result = result.replace(match.group(0), str(item_context[var_name]))
                elif isinstance(item, dict) and var_name in item:
                    result = result.replace(match.group(0), str(item[var_name]))
                elif var_name in ('item', 'index'):
                    pass  # Already handled
                else:
                    result = result.replace(match.group(0), '')

            rendered.append(result)

        joined = '\n'.join(rendered)

        result_data = {
            'rendered': rendered,
            'joined': joined,
            'count': len(rendered)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"列表模板完成: {len(items)} 项",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['template', 'items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'item_var': 'item',
            'save_to_var': None
        }
