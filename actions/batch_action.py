"""Batch action module for RabAI AutoClick.

Provides batch operations:
- BatchStartAction: Start batch
- BatchAddAction: Add to batch
- BatchExecuteAction: Execute batch
- BatchEndAction: End batch
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BatchStartAction(BaseAction):
    """Start batch."""
    action_type = "batch_start"
    display_name = "开始批处理"
    description = "开始批处理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute start.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult indicating started.
        """
        name = params.get('name', 'batch')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            context.set(f'_batch_{resolved_name}_items', [])
            context.set(f'_batch_{resolved_name}_name', resolved_name)

            return ActionResult(
                success=True,
                message=f"批处理 {resolved_name} 开始",
                data={'name': resolved_name}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"开始批处理失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'batch'}


class BatchAddAction(BaseAction):
    """Add to batch."""
    action_type = "batch_add"
    display_name = "添加批处理项"
    description = "添加批处理项"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add.

        Args:
            context: Execution context.
            params: Dict with name, item.

        Returns:
            ActionResult indicating added.
        """
        name = params.get('name', 'batch')
        item = params.get('item', None)

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_batch_{resolved_name}_items', [])
            resolved_item = context.resolve_value(item) if item is not None else None
            items.append(resolved_item)
            context.set(f'_batch_{resolved_name}_items', items)

            return ActionResult(
                success=True,
                message=f"添加批处理项: {len(items)} 项",
                data={
                    'name': resolved_name,
                    'item': resolved_item,
                    'count': len(items)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加批处理项失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'batch', 'item': None}


class BatchExecuteAction(BaseAction):
    """Execute batch."""
    action_type = "batch_execute"
    display_name = "执行批处理"
    description = "执行批处理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch.

        Args:
            context: Execution context.
            params: Dict with name, output_var.

        Returns:
            ActionResult with batch results.
        """
        name = params.get('name', 'batch')
        output_var = params.get('output_var', 'batch_results')

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_batch_{resolved_name}_items', [])

            results = []
            for i, item in enumerate(items):
                results.append({
                    'index': i,
                    'item': item,
                    'success': True
                })

            context.set(output_var, results)

            return ActionResult(
                success=True,
                message=f"批处理执行完成: {len(results)} 项",
                data={
                    'name': resolved_name,
                    'count': len(results),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"执行批处理失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'batch', 'output_var': 'batch_results'}


class BatchEndAction(BaseAction):
    """End batch."""
    action_type = "batch_end"
    display_name = "结束批处理"
    description = "结束批处理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute end.

        Args:
            context: Execution context.
            params: Dict with name.

        Returns:
            ActionResult indicating ended.
        """
        name = params.get('name', 'batch')

        try:
            resolved_name = context.resolve_value(name)

            items = context.get(f'_batch_{resolved_name}_items', [])

            context.delete(f'_batch_{resolved_name}_items')
            context.delete(f'_batch_{resolved_name}_name')

            return ActionResult(
                success=True,
                message=f"批处理 {resolved_name} 结束: {len(items)} 项",
                data={
                    'name': resolved_name,
                    'total_items': len(items)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"结束批处理失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'name': 'batch'}
