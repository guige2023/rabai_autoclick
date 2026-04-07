"""Shelve action module for RabAI AutoClick.

Provides shelve (persistent dict) operations:
- ShelveOpenAction: Open a shelve database
- ShelveGetAction: Get value from shelve
- ShelveSetAction: Set value in shelve
- ShelveDeleteAction: Delete key from shelve
- ShelveKeysAction: Get all keys from shelve
- ShelveCloseAction: Close shelve database
"""

from typing import Any, Dict, List, Optional, Union
import shelve
import os
import sys

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ShelveOpenAction(BaseAction):
    """Open a shelve database."""
    action_type = "shelve_open"
    display_name = "Shelve打开"
    description = "打开或创建shelve数据库"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute shelve open operation.

        Args:
            context: Execution context.
            params: Dict with db_path, writeback, output_var.

        Returns:
            ActionResult with shelve reference.
        """
        db_path = params.get('db_path', '')
        writeback = params.get('writeback', False)
        output_var = params.get('output_var', 'shelve_db')

        if not db_path:
            return ActionResult(success=False, message="db_path is required")

        try:
            resolved_path = context.resolve_value(db_path)
            resolved_wb = context.resolve_value(writeback)

            os.makedirs(resolved_path, exist_ok=True)

            db = shelve.open(resolved_path, writeback=resolved_wb)

            context.set(output_var, db)
            return ActionResult(success=True, data=db,
                               message=f"Opened shelve at {resolved_path}")

        except Exception as e:
            return ActionResult(success=False, message=f"Shelve open error: {str(e)}")


class ShelveGetAction(BaseAction):
    """Get value from shelve."""
    action_type = "shelve_get"
    display_name = "Shelve取值"
    description = "从shelve获取值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute shelve get operation.

        Args:
            context: Execution context.
            params: Dict with db, key, default, output_var.

        Returns:
            ActionResult with value.
        """
        db = params.get('db', None)
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'shelve_value')

        if db is None:
            return ActionResult(success=False, message="db is required")
        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            resolved_key = context.resolve_value(key)
            resolved_default = context.resolve_value(default) if default is not None else None
            resolved_db = context.resolve_value(db)

            if isinstance(resolved_db, shelve.Shelf):
                value = resolved_db.get(resolved_key, resolved_default)
                context.set(output_var, value)
                return ActionResult(success=True, data=value,
                                   message=f"Got key '{resolved_key}'")
            else:
                return ActionResult(success=False, message="db must be a shelve.Shelf")

        except KeyError:
            context.set(output_var, resolved_default)
            return ActionResult(success=True, data=resolved_default,
                               message=f"Key '{resolved_key}' not found, returned default")
        except Exception as e:
            return ActionResult(success=False, message=f"Shelve get error: {str(e)}")


class ShelveSetAction(BaseAction):
    """Set value in shelve."""
    action_type = "shelve_set"
    display_name = "Shelve设值"
    description = "在shelve中设置值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute shelve set operation.

        Args:
            context: Execution context.
            params: Dict with db, key, value, output_var.

        Returns:
            ActionResult with set status.
        """
        db = params.get('db', None)
        key = params.get('key', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'shelve_set_result')

        if db is None:
            return ActionResult(success=False, message="db is required")
        if not key:
            return ActionResult(success=False, message="key is required")
        if value is None:
            return ActionResult(success=False, message="value is required")

        try:
            resolved_db = context.resolve_value(db)
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)

            if isinstance(resolved_db, shelve.Shelf):
                resolved_db[resolved_key] = resolved_value
                resolved_db.sync()
                context.set(output_var, True)
                return ActionResult(success=True, data=True,
                                   message=f"Set key '{resolved_key}'")
            else:
                return ActionResult(success=False, message="db must be a shelve.Shelf")

        except Exception as e:
            return ActionResult(success=False, message=f"Shelve set error: {str(e)}")


class ShelveDeleteAction(BaseAction):
    """Delete key from shelve."""
    action_type = "shelve_delete"
    display_name = "Shelve删除"
    description = "从shelve删除键"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute shelve delete operation.

        Args:
            context: Execution context.
            params: Dict with db, key, output_var.

        Returns:
            ActionResult with delete status.
        """
        db = params.get('db', None)
        key = params.get('key', '')
        output_var = params.get('output_var', 'shelve_delete_result')

        if db is None:
            return ActionResult(success=False, message="db is required")
        if not key:
            return ActionResult(success=False, message="key is required")

        try:
            resolved_db = context.resolve_value(db)
            resolved_key = context.resolve_value(key)

            if isinstance(resolved_db, shelve.Shelf):
                if resolved_key in resolved_db:
                    del resolved_db[resolved_key]
                    resolved_db.sync()
                    context.set(output_var, True)
                    return ActionResult(success=True, data=True,
                                       message=f"Deleted key '{resolved_key}'")
                else:
                    context.set(output_var, False)
                    return ActionResult(success=False, data=False,
                                       message=f"Key '{resolved_key}' not found")
            else:
                return ActionResult(success=False, message="db must be a shelve.Shelf")

        except Exception as e:
            return ActionResult(success=False, message=f"Shelve delete error: {str(e)}")


class ShelveKeysAction(BaseAction):
    """Get all keys from shelve."""
    action_type = "shelve_keys"
    display_name = "Shelve键列表"
    description = "获取shelve所有键"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute shelve keys operation.

        Args:
            context: Execution context.
            params: Dict with db, filter_prefix, output_var.

        Returns:
            ActionResult with list of keys.
        """
        db = params.get('db', None)
        filter_prefix = params.get('filter_prefix', '')
        output_var = params.get('output_var', 'shelve_keys')

        if db is None:
            return ActionResult(success=False, message="db is required")

        try:
            resolved_db = context.resolve_value(db)

            if isinstance(resolved_db, shelve.Shelf):
                keys = list(resolved_db.keys())

                if filter_prefix:
                    resolved_prefix = context.resolve_value(filter_prefix)
                    keys = [k for k in keys if str(k).startswith(resolved_prefix)]

                context.set(output_var, keys)
                return ActionResult(success=True, data=keys,
                                   message=f"Found {len(keys)} keys in shelve")
            else:
                return ActionResult(success=False, message="db must be a shelve.Shelf")

        except Exception as e:
            return ActionResult(success=False, message=f"Shelve keys error: {str(e)}")


class ShelveCloseAction(BaseAction):
    """Close shelve database."""
    action_type = "shelve_close"
    display_name = "Shelve关闭"
    description = "关闭shelve数据库"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute shelve close operation.

        Args:
            context: Execution context.
            params: Dict with db, output_var.

        Returns:
            ActionResult with close status.
        """
        db = params.get('db', None)
        output_var = params.get('output_var', 'shelve_close_result')

        if db is None:
            return ActionResult(success=False, message="db is required")

        try:
            resolved_db = context.resolve_value(db)

            if isinstance(resolved_db, shelve.Shelf):
                resolved_db.close()
                context.set(output_var, True)
                return ActionResult(success=True, data=True,
                                   message="Closed shelve database")
            else:
                return ActionResult(success=False, message="db must be a shelve.Shelf")

        except Exception as e:
            return ActionResult(success=False, message=f"Shelve close error: {str(e)}")
