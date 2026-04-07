"""Tree14 action module for RabAI AutoClick.

Provides additional tree operations:
- TreeCreateAction: Create a tree
- TreeInsertAction: Insert node
- TreeDeleteAction: Delete node
- TreeSearchAction: Search node
- TreeTraverseAction: Traverse tree
- TreeHeightAction: Get tree height
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TreeNode:
    """Tree node class."""
    def __init__(self, value: Any, parent: Optional['TreeNode'] = None):
        self.value = value
        self.parent = parent
        self.children = []


class TreeCreateAction(BaseAction):
    """Create a tree."""
    action_type = "tree14_create"
    display_name = "创建树"
    description = "创建树结构"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tree create.

        Args:
            context: Execution context.
            params: Dict with tree_name, root_value, output_var.

        Returns:
            ActionResult with create result.
        """
        tree_name = params.get('tree_name', 'default')
        root_value = params.get('root_value', None)
        output_var = params.get('output_var', 'create_result')

        try:
            resolved_tree = context.resolve_value(tree_name) if tree_name else 'default'
            resolved_root = context.resolve_value(root_value) if root_value else None

            if not hasattr(context, '_trees'):
                context._trees = {}

            root = TreeNode(resolved_root)
            context._trees[resolved_tree] = {
                'root': root,
                'node_count': 1
            }

            context.set(output_var, {
                'tree': resolved_tree,
                'root': resolved_root,
                'node_count': 1
            })

            return ActionResult(
                success=True,
                message=f"创建树: {resolved_tree} (根={resolved_root})",
                data={
                    'tree': resolved_tree,
                    'root': resolved_root,
                    'node_count': 1,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建树失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree_name', 'root_value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'create_result'}


class TreeInsertAction(BaseAction):
    """Insert node."""
    action_type = "tree14_insert"
    display_name = "插入节点"
    description = "插入树节点"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute insert.

        Args:
            context: Execution context.
            params: Dict with tree_name, parent_value, value, output_var.

        Returns:
            ActionResult with insert result.
        """
        tree_name = params.get('tree_name', 'default')
        parent_value = params.get('parent_value', None)
        value = params.get('value', None)
        output_var = params.get('output_var', 'insert_result')

        try:
            resolved_tree = context.resolve_value(tree_name) if tree_name else 'default'
            resolved_parent = context.resolve_value(parent_value) if parent_value else None
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_trees') or resolved_tree not in context._trees:
                return ActionResult(
                    success=False,
                    message=f"树不存在: {resolved_tree}"
                )

            tree = context._trees[resolved_tree]

            def find_node(node, target):
                if node.value == target:
                    return node
                for child in node.children:
                    found = find_node(child, target)
                    if found:
                        return found
                return None

            if resolved_parent is None:
                parent_node = tree['root']
            else:
                parent_node = find_node(tree['root'], resolved_parent)

            if not parent_node:
                return ActionResult(
                    success=False,
                    message=f"父节点不存在: {resolved_parent}"
                )

            new_node = TreeNode(resolved_value, parent_node)
            parent_node.children.append(new_node)
            tree['node_count'] += 1

            context.set(output_var, {
                'inserted': resolved_value,
                'parent': resolved_parent,
                'node_count': tree['node_count']
            })

            return ActionResult(
                success=True,
                message=f"插入节点: {resolved_value} -> {resolved_parent or 'root'} ({tree['node_count']}节点)",
                data={
                    'tree': resolved_tree,
                    'inserted': resolved_value,
                    'parent': resolved_parent,
                    'node_count': tree['node_count'],
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"插入节点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree_name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'parent_value': None, 'output_var': 'insert_result'}


class TreeDeleteAction(BaseAction):
    """Delete node."""
    action_type = "tree14_delete"
    display_name = "删除节点"
    description = "删除树节点"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with tree_name, value, output_var.

        Returns:
            ActionResult with delete result.
        """
        tree_name = params.get('tree_name', 'default')
        value = params.get('value', None)
        output_var = params.get('output_var', 'delete_result')

        try:
            resolved_tree = context.resolve_value(tree_name) if tree_name else 'default'
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_trees') or resolved_tree not in context._trees:
                return ActionResult(
                    success=False,
                    message=f"树不存在: {resolved_tree}"
                )

            tree = context._trees[resolved_tree]

            if tree['root'].value == resolved_value:
                return ActionResult(
                    success=False,
                    message=f"不能删除根节点"
                )

            def find_and_delete(node, target):
                for i, child in enumerate(node.children):
                    if child.value == target:
                        node.children.pop(i)
                        return True
                    if find_and_delete(child, target):
                        return True
                return False

            deleted = find_and_delete(tree['root'], resolved_value)

            if deleted:
                tree['node_count'] -= 1

            context.set(output_var, {
                'deleted': deleted,
                'value': resolved_value,
                'node_count': tree['node_count']
            })

            return ActionResult(
                success=True,
                message=f"删除节点: {resolved_value} ({'成功' if deleted else '不存在'})",
                data={
                    'tree': resolved_tree,
                    'deleted': deleted,
                    'value': resolved_value,
                    'node_count': tree['node_count'],
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除节点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree_name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'delete_result'}


class TreeSearchAction(BaseAction):
    """Search node."""
    action_type = "tree14_search"
    display_name = "搜索节点"
    description = "搜索树节点"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute search.

        Args:
            context: Execution context.
            params: Dict with tree_name, value, output_var.

        Returns:
            ActionResult with search result.
        """
        tree_name = params.get('tree_name', 'default')
        value = params.get('value', None)
        output_var = params.get('output_var', 'search_result')

        try:
            resolved_tree = context.resolve_value(tree_name) if tree_name else 'default'
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_trees') or resolved_tree not in context._trees:
                return ActionResult(
                    success=False,
                    message=f"树不存在: {resolved_tree}"
                )

            tree = context._trees[resolved_tree]

            def find_node(node, target):
                if node.value == target:
                    return node
                for child in node.children:
                    found = find_node(child, target)
                    if found:
                        return found
                return None

            node = find_node(tree['root'], resolved_value)

            context.set(output_var, {
                'found': node is not None,
                'value': resolved_value,
                'parent': node.parent.value if node and node.parent else None
            })

            return ActionResult(
                success=True,
                message=f"搜索节点: {resolved_value} ({'找到' if node else '未找到'})",
                data={
                    'tree': resolved_tree,
                    'found': node is not None,
                    'value': resolved_value,
                    'parent': node.parent.value if node and node.parent else None,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"搜索节点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree_name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'search_result'}


class TreeTraverseAction(BaseAction):
    """Traverse tree."""
    action_type = "tree14_traverse"
    display_name = "遍历树"
    description = "遍历树节点"
    version = "14.0"

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
            ActionResult with traverse result.
        """
        tree_name = params.get('tree_name', 'default')
        mode = params.get('mode', 'inorder')
        output_var = params.get('output_var', 'traverse_result')

        try:
            resolved_tree = context.resolve_value(tree_name) if tree_name else 'default'
            resolved_mode = context.resolve_value(mode) if mode else 'inorder'

            if not hasattr(context, '_trees') or resolved_tree not in context._trees:
                return ActionResult(
                    success=False,
                    message=f"树不存在: {resolved_tree}"
                )

            tree = context._trees[resolved_tree]
            order = []

            def traverse_preorder(node):
                order.append(node.value)
                for child in node.children:
                    traverse_preorder(child)

            def traverse_postorder(node):
                for child in node.children:
                    traverse_postorder(child)
                order.append(node.value)

            def traverse_levelorder(node):
                from collections import deque
                queue = deque([node])
                while queue:
                    current = queue.popleft()
                    order.append(current.value)
                    queue.extend(current.children)

            if resolved_mode == 'preorder':
                traverse_preorder(tree['root'])
            elif resolved_mode == 'postorder':
                traverse_postorder(tree['root'])
            elif resolved_mode == 'levelorder':
                traverse_levelorder(tree['root'])
            else:
                traverse_preorder(tree['root'])

            context.set(output_var, order)

            return ActionResult(
                success=True,
                message=f"遍历树: {resolved_mode} ({len(order)}节点)",
                data={
                    'tree': resolved_tree,
                    'mode': resolved_mode,
                    'order': order,
                    'count': len(order),
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
        return {'mode': 'inorder', 'output_var': 'traverse_result'}


class TreeHeightAction(BaseAction):
    """Get tree height."""
    action_type = "tree14_height"
    display_name = "树高度"
    description = "获取树高度"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute height.

        Args:
            context: Execution context.
            params: Dict with tree_name, output_var.

        Returns:
            ActionResult with height result.
        """
        tree_name = params.get('tree_name', 'default')
        output_var = params.get('output_var', 'height_result')

        try:
            resolved_tree = context.resolve_value(tree_name) if tree_name else 'default'

            if not hasattr(context, '_trees') or resolved_tree not in context._trees:
                return ActionResult(
                    success=False,
                    message=f"树不存在: {resolved_tree}"
                )

            tree = context._trees[resolved_tree]

            def get_height(node):
                if not node.children:
                    return 1
                return 1 + max(get_height(child) for child in node.children)

            height = get_height(tree['root'])

            context.set(output_var, height)

            return ActionResult(
                success=True,
                message=f"树高度: {resolved_tree} = {height}",
                data={
                    'tree': resolved_tree,
                    'height': height,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"树高度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tree_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'height_result'}