"""Group utilities action module for RabAI AutoClick.

Provides grouping operations for collections
by key field with aggregation support.
"""

import sys
import os
from typing import Any, Dict, List, Optional
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GroupByAction(BaseAction):
    """Group list items by key field.
    
    Creates groups with list of matching items.
    """
    action_type = "group_by"
    display_name = "分组"
    description = "按字段分组列表项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Group items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, key_field,
                   preserve_order, save_to_var.
        
        Returns:
            ActionResult with grouped items.
        """
        items = params.get('items', [])
        key_field = params.get('key_field', '')
        preserve_order = params.get('preserve_order', True)
        save_to_var = params.get('save_to_var', None)

        if not items:
            return ActionResult(success=False, message="Items list is empty")

        if not key_field:
            return ActionResult(success=False, message="key_field is required")

        groups = defaultdict(list)
        order = []

        for item in items:
            if isinstance(item, dict):
                key = item.get(key_field)
            else:
                key = getattr(item, key_field, None)

            if key not in order:
                order.append(key)

            groups[key].append(item)

        # Convert to ordered dict if requested
        if preserve_order:
            groups = {k: groups[k] for k in order}

        result_data = {
            'groups': dict(groups),
            'group_count': len(groups),
            'total_items': len(items),
            'key_field': key_field
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"分组完成: {len(items)} 项 -> {len(groups)} 组",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['items', 'key_field']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'preserve_order': True,
            'save_to_var': None
        }


class PartitionAction(BaseAction):
    """Partition list into groups by size.
    
    Creates evenly sized partitions.
    """
    action_type = "partition"
    display_name = "分区"
    description = "将列表分为均匀大小的分区"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Partition items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, partition_size,
                   save_to_var.
        
        Returns:
            ActionResult with partitions.
        """
        items = params.get('items', [])
        partition_size = params.get('partition_size', 10)
        save_to_var = params.get('save_to_var', None)

        if not items:
            return ActionResult(success=False, message="Items list is empty")

        if partition_size <= 0:
            return ActionResult(
                success=False,
                message=f"partition_size must be > 0, got {partition_size}"
            )

        partitions = []
        for i in range(0, len(items), partition_size):
            partitions.append(items[i:i + partition_size])

        result_data = {
            'partitions': partitions,
            'count': len(partitions),
            'partition_size': partition_size,
            'total_items': len(items)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"分区完成: {len(items)} 项 -> {len(partitions)} 区",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['items', 'partition_size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'save_to_var': None}
