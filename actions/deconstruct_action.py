"""Deconstruct action module for RabAI AutoClick.

Provides deconstruction operations:
- DeconstructListAction: Deconstruct list to variables
- DeconstructTupleAction: Deconstruct tuple to variables
- DeconstructDictAction: Deconstruct dict to variables
- DeconstructStringAction: Deconstruct string to characters
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DeconstructListAction(BaseAction):
    """Deconstruct list to variables."""
    action_type = "deconstruct_list"
    display_name = "解构列表"
    description = "将列表解构到多个变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deconstruct list.

        Args:
            context: Execution context.
            params: Dict with list_var, var_names.

        Returns:
            ActionResult with deconstructed variables.
        """
        list_var = params.get('list_var', '')
        var_names = params.get('var_names', [])

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(list_var)
            resolved_names = [context.resolve_value(v) for v in var_names]

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = [items]

            assigned = {}
            for i, name in enumerate(resolved_names):
                if i < len(items):
                    context.set(name, items[i])
                    assigned[name] = items[i]

            return ActionResult(
                success=True,
                message=f"列表解构: {len(assigned)} 变量",
                data={
                    'assigned_count': len(assigned),
                    'assigned_vars': list(assigned.keys())
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解构列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var', 'var_names']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class DeconstructTupleAction(BaseAction):
    """Deconstruct tuple to variables."""
    action_type = "deconstruct_tuple"
    display_name = "解构元组"
    description = "将元组解构到多个变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deconstruct tuple.

        Args:
            context: Execution context.
            params: Dict with tuple_var, var_names.

        Returns:
            ActionResult with deconstructed variables.
        """
        tuple_var = params.get('tuple_var', '')
        var_names = params.get('var_names', [])

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(tuple_var)
            resolved_names = [context.resolve_value(v) for v in var_names]

            items = context.get(resolved_var)
            if not isinstance(items, (list, tuple)):
                items = [items]

            assigned = {}
            for i, name in enumerate(resolved_names):
                if i < len(items):
                    context.set(name, items[i])
                    assigned[name] = items[i]

            return ActionResult(
                success=True,
                message=f"元组解构: {len(assigned)} 变量",
                data={
                    'assigned_count': len(assigned),
                    'assigned_vars': list(assigned.keys())
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解构元组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var', 'var_names']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class DeconstructDictAction(BaseAction):
    """Deconstruct dict to variables."""
    action_type = "deconstruct_dict"
    display_name = "解构字典"
    description = "将字典解构到多个变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deconstruct dict.

        Args:
            context: Execution context.
            params: Dict with dict_var, key_names.

        Returns:
            ActionResult with deconstructed variables.
        """
        dict_var = params.get('dict_var', '')
        key_names = params.get('key_names', [])

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(dict_var)
            resolved_keys = [context.resolve_value(k) for k in key_names]

            d = context.get(resolved_var)
            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是字典"
                )

            assigned = {}
            for key in resolved_keys:
                if key in d:
                    context.set(key, d[key])
                    assigned[key] = d[key]

            return ActionResult(
                success=True,
                message=f"字典解构: {len(assigned)} 变量",
                data={
                    'assigned_count': len(assigned),
                    'assigned_vars': list(assigned.keys())
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解构字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key_names']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class DeconstructStringAction(BaseAction):
    """Deconstruct string to characters."""
    action_type = "deconstruct_string"
    display_name = "解构字符串"
    description = "将字符串解构到多个变量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deconstruct string.

        Args:
            context: Execution context.
            params: Dict with string_var, var_names.

        Returns:
            ActionResult with deconstructed variables.
        """
        string_var = params.get('string_var', '')
        var_names = params.get('var_names', [])

        valid, msg = self.validate_type(string_var, str, 'string_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(string_var)
            resolved_names = [context.resolve_value(v) for v in var_names]

            s = context.get(resolved_var)
            if not isinstance(s, str):
                s = str(s)

            chars = list(s)
            assigned = {}
            for i, name in enumerate(resolved_names):
                if i < len(chars):
                    context.set(name, chars[i])
                    assigned[name] = chars[i]

            return ActionResult(
                success=True,
                message=f"字符串解构: {len(assigned)} 变量",
                data={
                    'assigned_count': len(assigned),
                    'assigned_vars': list(assigned.keys())
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解构字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['string_var', 'var_names']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}
