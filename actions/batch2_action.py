"""Batch2 action module for RabAI AutoClick.

Provides additional batch operations:
- BatchCreateAction: Create batch
- BatchAddAction: Add to batch
- BatchRemoveAction: Remove from batch
- BatchSizeAction: Get batch size
- BatchClearAction: Clear batch
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BatchCreateAction(BaseAction):
    """Create batch."""
    action_type = "batch2_create"
    display_name = "创建批处理"
    description = "创建新的批处理"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with created batch.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'created_batch')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = list(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"批处理创建: {len(result)}个项目",
                data={
                    'batch': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建批处理失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'items': [], 'output_var': 'created_batch'}


class BatchAddAction(BaseAction):
    """Add to batch."""
    action_type = "batch2_add"
    display_name = "批处理添加"
    description = "向批处理添加项目"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add.

        Args:
            context: Execution context.
            params: Dict with batch, items, output_var.

        Returns:
            ActionResult with updated batch.
        """
        batch = params.get('batch', [])
        items = params.get('items', [])
        output_var = params.get('output_var', 'updated_batch')

        try:
            resolved_batch = context.resolve_value(batch)
            resolved_items = context.resolve_value(items)

            if not isinstance(resolved_batch, (list, tuple)):
                resolved_batch = [resolved_batch]
            if not isinstance(resolved_items, (list, tuple)):
                resolved_items = [resolved_items]

            result = list(resolved_batch) + list(resolved_items)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"批处理添加: 添加{len(resolved_items)}个项目",
                data={
                    'added_items': resolved_items,
                    'batch': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"批处理添加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'batch': [], 'output_var': 'updated_batch'}


class BatchRemoveAction(BaseAction):
    """Remove from batch."""
    action_type = "batch2_remove"
    display_name = "批处理移除"
    description: "从批处理移除项目"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove.

        Args:
            context: Execution context.
            params: Dict with batch, indices, output_var.

        Returns:
            ActionResult with updated batch.
        """
        batch = params.get('batch', [])
        indices = params.get('indices', [])
        output_var = params.get('output_var', 'updated_batch')

        try:
            resolved_batch = context.resolve_value(batch)
            resolved_indices = context.resolve_value(indices)

            if not isinstance(resolved_batch, (list, tuple)):
                resolved_batch = list(resolved_batch)
            if not isinstance(resolved_indices, (list, tuple)):
                resolved_indices = [resolved_indices]

            result = [item for i, item in enumerate(resolved_batch) if i not in resolved_indices]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"批处理移除: 移除{len(resolved_indices)}个项目",
                data={
                    'removed_indices': resolved_indices,
                    'batch': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"批处理移除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['batch', 'indices']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'updated_batch'}


class BatchSizeAction(BaseAction):
    """Get batch size."""
    action_type = "batch2_size"
    display_name = "获取批处理大小"
    description = "获取批处理的大小"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute size.

        Args:
            context: Execution context.
            params: Dict with batch, output_var.

        Returns:
            ActionResult with batch size.
        """
        batch = params.get('batch', [])
        output_var = params.get('output_var', 'batch_size')

        try:
            resolved = context.resolve_value(batch)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            size = len(resolved)

            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"批处理大小: {size}",
                data={
                    'size': size,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取批处理大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['batch']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'batch_size'}


class BatchClearAction(BaseAction):
    """Clear batch."""
    action_type = "batch2_clear"
    display_name = "清空批处理"
    description = "清空批处理"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with empty batch.
        """
        output_var = params.get('output_var', 'cleared_batch')

        try:
            result = []

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"批处理已清空",
                data={
                    'batch': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空批处理失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cleared_batch'}