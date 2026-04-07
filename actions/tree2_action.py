"""Tree2 action module for RabAI AutoClick.

Provides additional tree operations:
- TreeHeightAction: Calculate tree height
- TreeNodeCountAction: Count nodes
- TreeLeafCountAction: Count leaf nodes
- TreeInorderAction: Inorder traversal
- TreePostorderAction: Postorder traversal
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TreeHeightAction(BaseAction):
    """Calculate tree height."""
    action_type = "tree2_height"
    display_name = "计算树高度"
    description = "计算树的高度"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute height.

        Args:
            context: Execution context.
            params: Dict with tree, output_var.

        Returns:
            ActionResult with tree height.
        """
        tree = params.get('tree', None)
        output_var = params.get('output_var', 'tree_height')

        try:
            resolved = context.resolve_value(tree)

            if not isinstance(resolved, dict) or 'value' not in resolved:
                return ActionResult(
                    success=False,
                    message="计算树高度失败: 无效的树结构"
                )

            def height(node):
                if node is None:
                    return 0
                if 'children' not in node or not node['children']:
                    return 1
                return 1 + max(height(child) for child in node['children'])

            result = height(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"树高度: {result}",
                data={
                    'height': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算树高度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tree_height'}


class TreeNodeCountAction(BaseAction):
    """Count nodes."""
    action_type = "tree2_node_count"
    display_name = "计算节点数"
    description = "计算树的节点数"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute node count.

        Args:
            context: Execution context.
            params: Dict with tree, output_var.

        Returns:
            ActionResult with node count.
        """
        tree = params.get('tree', None)
        output_var = params.get('output_var', 'node_count')

        try:
            resolved = context.resolve_value(tree)

            if not isinstance(resolved, dict) or 'value' not in resolved:
                return ActionResult(
                    success=False,
                    message="计算节点数失败: 无效的树结构"
                )

            def count(node):
                if node is None:
                    return 0
                children_count = 0
                if 'children' in node and node['children']:
                    children_count = sum(count(child) for child in node['children'])
                return 1 + children_count

            result = count(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"节点数: {result}",
                data={
                    'node_count': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算节点数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'node_count'}


class TreeLeafCountAction(BaseAction):
    """Count leaf nodes."""
    action_type = "tree2_leaf_count"
    display_name = "计算叶子节点数"
    description = "计算树的叶子节点数"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute leaf count.

        Args:
            context: Execution context.
            params: Dict with tree, output_var.

        Returns:
            ActionResult with leaf count.
        """
        tree = params.get('tree', None)
        output_var = params.get('output_var', 'leaf_count')

        try:
            resolved = context.resolve_value(tree)

            if not isinstance(resolved, dict) or 'value' not in resolved:
                return ActionResult(
                    success=False,
                    message="计算叶子节点数失败: 无效的树结构"
                )

            def leaf_count(node):
                if node is None:
                    return 0
                if 'children' not in node or not node['children']:
                    return 1
                return sum(leaf_count(child) for child in node['children'])

            result = leaf_count(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"叶子节点数: {result}",
                data={
                    'leaf_count': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算叶子节点数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'leaf_count'}


class TreeInorderAction(BaseAction):
    """Inorder traversal."""
    action_type = "tree2_inorder"
    display_name = "中序遍历"
    description = "对树进行中序遍历"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute inorder.

        Args:
            context: Execution context.
            params: Dict with tree, output_var.

        Returns:
            ActionResult with inorder traversal.
        """
        tree = params.get('tree', None)
        output_var = params.get('output_var', 'inorder_result')

        try:
            resolved = context.resolve_value(tree)

            if not isinstance(resolved, dict) or 'value' not in resolved:
                return ActionResult(
                    success=False,
                    message="中序遍历失败: 无效的树结构"
                )

            def inorder(node, result):
                if node is None:
                    return
                if 'children' in node and node['children']:
                    if len(node['children']) > 0:
                        inorder(node['children'][0], result)
                result.append(node['value'])
                if 'children' in node and node['children']:
                    for child in node['children'][1:]:
                        inorder(child, result)

            result = []
            inorder(resolved, result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"中序遍历: {result}",
                data={
                    'traversal': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"中序遍历失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'inorder_result'}


class TreePostorderAction(BaseAction):
    """Postorder traversal."""
    action_type = "tree2_postorder"
    display_name = "后序遍历"
    description = "对树进行后序遍历"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute postorder.

        Args:
            context: Execution context.
            params: Dict with tree, output_var.

        Returns:
            ActionResult with postorder traversal.
        """
        tree = params.get('tree', None)
        output_var = params.get('output_var', 'postorder_result')

        try:
            resolved = context.resolve_value(tree)

            if not isinstance(resolved, dict) or 'value' not in resolved:
                return ActionResult(
                    success=False,
                    message="后序遍历失败: 无效的树结构"
                )

            def postorder(node, result):
                if node is None:
                    return
                if 'children' in node and node['children']:
                    for child in node['children']:
                        postorder(child, result)
                result.append(node['value'])

            result = []
            postorder(resolved, result)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"后序遍历: {result}",
                data={
                    'traversal': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"后序遍历失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'postorder_result'}