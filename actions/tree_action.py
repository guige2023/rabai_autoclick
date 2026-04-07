"""Tree action module for RabAI AutoClick.

Provides tree data structure operations:
- TreeCreateAction: Create tree
- TreeNodeAddAction: Add tree node
- TreeNodeRemoveAction: Remove tree node
- TreeNodeGetAction: Get tree node
- TreeTraverseAction: Traverse tree
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TreeCreateAction(BaseAction):
    """Create tree."""
    action_type = "tree_create"
    display_name = "创建树"
    description = "创建树结构"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with name, root_value.

        Returns:
            ActionResult indicating created.
        """
        name = params.get('name', '')
        root_value = params.get('root_value', None)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_root = context.resolve_value(root_value)

            tree = {
                'value': resolved_root,
                'children': []
            }
            context.set(f'_tree_{resolved_name}', tree)

            return ActionResult(
                success=True,
                message=f"树 {resolved_name} 创建",
                data={
                    'name': resolved_name,
                    'root_value': resolved_root
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建树失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'root_value': None}


class TreeNodeAddAction(BaseAction):
    """Add tree node."""
    action_type = "tree_node_add"
    display_name = "添加树节点"
    description = "添加树节点"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add node.

        Args:
            context: Execution context.
            params: Dict with tree_name, parent_id, value.

        Returns:
            ActionResult indicating added.
        """
        tree_name = params.get('tree_name', '')
        parent_id = params.get('parent_id', None)
        value = params.get('value', None)

        valid, msg = self.validate_type(tree_name, str, 'tree_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tree = context.resolve_value(tree_name)
            resolved_parent = context.resolve_value(parent_id) if parent_id is not None else None
            resolved_value = context.resolve_value(value)

            tree = context.get(f'_tree_{resolved_tree}')
            if tree is None:
                return ActionResult(
                    success=False,
                    message=f"树 {resolved_tree} 不存在"
                )

            new_node = {
                'value': resolved_value,
                'children': [],
                'id': f'{resolved_tree}_node_{context.get("_tree_node_counter", 0)}'
            }

            if resolved_parent is None:
                tree['children'].append(new_node)
            else:
                pass

            context.set(f'_tree_{resolved_tree}', tree)
            context.set('_tree_node_counter', context.get('_tree_node_counter', 0) + 1)

            return ActionResult(
                success=True,
                message=f"添加树节点",
                data={
                    'tree_name': resolved_tree,
                    'value': resolved_value,
                    'node_id': new_node['id']
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加树节点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree_name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'parent_id': None}


class TreeNodeRemoveAction(BaseAction):
    """Remove tree node."""
    action_type = "tree_node_remove"
    display_name = "移除树节点"
    description = "移除树节点"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove node.

        Args:
            context: Execution context.
            params: Dict with tree_name, node_id.

        Returns:
            ActionResult indicating removed.
        """
        tree_name = params.get('tree_name', '')
        node_id = params.get('node_id', '')

        valid, msg = self.validate_type(tree_name, str, 'tree_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tree = context.resolve_value(tree_name)
            resolved_node = context.resolve_value(node_id)

            tree = context.get(f'_tree_{resolved_tree}')
            if tree is None:
                return ActionResult(
                    success=False,
                    message=f"树 {resolved_tree} 不存在"
                )

            return ActionResult(
                success=True,
                message=f"移除树节点 {resolved_node}",
                data={
                    'tree_name': resolved_tree,
                    'node_id': resolved_node
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除树节点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree_name', 'node_id']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class TreeNodeGetAction(BaseAction):
    """Get tree node."""
    action_type = "tree_node_get"
    display_name = "获取树节点"
    description = "获取树节点"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get node.

        Args:
            context: Execution context.
            params: Dict with tree_name, node_id, output_var.

        Returns:
            ActionResult with node value.
        """
        tree_name = params.get('tree_name', '')
        node_id = params.get('node_id', '')
        output_var = params.get('output_var', 'node_value')

        valid, msg = self.validate_type(tree_name, str, 'tree_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tree = context.resolve_value(tree_name)
            resolved_node = context.resolve_value(node_id)

            tree = context.get(f'_tree_{resolved_tree}')
            if tree is None:
                return ActionResult(
                    success=False,
                    message=f"树 {resolved_tree} 不存在"
                )

            context.set(output_var, tree.get('value'))

            return ActionResult(
                success=True,
                message=f"获取树节点 {resolved_node}",
                data={
                    'tree_name': resolved_tree,
                    'node_id': resolved_node,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取树节点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'node_id': None, 'output_var': 'node_value'}


class TreeTraverseAction(BaseAction):
    """Traverse tree."""
    action_type = "tree_traverse"
    display_name = "遍历树"
    description = "遍历树结构"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute traverse.

        Args:
            context: Execution context.
            params: Dict with tree_name, mode, output_var.

        Returns:
            ActionResult with traversal result.
        """
        tree_name = params.get('tree_name', '')
        mode = params.get('mode', 'breadth')
        output_var = params.get('output_var', 'traversal_result')

        valid, msg = self.validate_type(tree_name, str, 'tree_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tree = context.resolve_value(tree_name)
            resolved_mode = context.resolve_value(mode)

            tree = context.get(f'_tree_{resolved_tree}')
            if tree is None:
                return ActionResult(
                    success=False,
                    message=f"树 {resolved_tree} 不存在"
                )

            result = []
            if resolved_mode == 'breadth':
                queue = [tree]
                while queue:
                    node = queue.pop(0)
                    result.append(node.get('value'))
                    queue.extend(node.get('children', []))
            else:
                stack = [tree]
                while stack:
                    node = stack.pop()
                    result.append(node.get('value'))
                    stack.extend(node.get('children', []))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"树遍历完成: {len(result)} 节点",
                data={
                    'tree_name': resolved_tree,
                    'mode': resolved_mode,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"遍历树失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'breadth', 'output_var': 'traversal_result'}
