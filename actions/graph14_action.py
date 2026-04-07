"""Graph14 action module for RabAI AutoClick.

Provides additional graph operations:
- GraphAddNodeAction: Add node to graph
- GraphAddEdgeAction: Add edge to graph
- GraphRemoveNodeAction: Remove node from graph
- GraphRemoveEdgeAction: Remove edge from graph
- GraphBFSAction: Breadth-first search
- GraphDFSAction: Depth-first search
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GraphAddNodeAction(BaseAction):
    """Add node to graph."""
    action_type = "graph14_add_node"
    display_name = "添加节点"
    description = "添加节点到图"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add node.

        Args:
            context: Execution context.
            params: Dict with graph_name, node, data, output_var.

        Returns:
            ActionResult with add result.
        """
        graph_name = params.get('graph_name', 'default')
        node = params.get('node', '')
        data = params.get('data', None)
        output_var = params.get('output_var', 'add_node_result')

        try:
            resolved_graph = context.resolve_value(graph_name) if graph_name else 'default'
            resolved_node = context.resolve_value(node) if node else ''
            resolved_data = context.resolve_value(data) if data else None

            if not hasattr(context, '_graphs'):
                context._graphs = {}

            if resolved_graph not in context._graphs:
                context._graphs[resolved_graph] = {
                    'nodes': set(),
                    'edges': {},
                    'adjacency': {}
                }

            graph = context._graphs[resolved_graph]
            graph['nodes'].add(resolved_node)
            if resolved_node not in graph['adjacency']:
                graph['adjacency'][resolved_node] = []

            context.set(output_var, {
                'node': resolved_node,
                'data': resolved_data,
                'node_count': len(graph['nodes'])
            })

            return ActionResult(
                success=True,
                message=f"添加节点: {resolved_node} -> {resolved_graph} ({len(graph['nodes'])}节点)",
                data={
                    'graph': resolved_graph,
                    'node': resolved_node,
                    'data': resolved_data,
                    'node_count': len(graph['nodes']),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加节点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph_name', 'node']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'data': None, 'output_var': 'add_node_result'}


class GraphAddEdgeAction(BaseAction):
    """Add edge to graph."""
    action_type = "graph14_add_edge"
    display_name = "添加边"
    description = "添加边到图"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add edge.

        Args:
            context: Execution context.
            params: Dict with graph_name, from_node, to_node, weight, output_var.

        Returns:
            ActionResult with add result.
        """
        graph_name = params.get('graph_name', 'default')
        from_node = params.get('from_node', '')
        to_node = params.get('to_node', '')
        weight = params.get('weight', 1)
        output_var = params.get('output_var', 'add_edge_result')

        try:
            resolved_graph = context.resolve_value(graph_name) if graph_name else 'default'
            resolved_from = context.resolve_value(from_node) if from_node else ''
            resolved_to = context.resolve_value(to_node) if to_node else ''
            resolved_weight = float(context.resolve_value(weight)) if weight else 1

            if not hasattr(context, '_graphs'):
                context._graphs = {}

            if resolved_graph not in context._graphs:
                context._graphs[resolved_graph] = {
                    'nodes': set(),
                    'edges': {},
                    'adjacency': {}
                }

            graph = context._graphs[resolved_graph]
            graph['nodes'].add(resolved_from)
            graph['nodes'].add(resolved_to)

            if resolved_from not in graph['adjacency']:
                graph['adjacency'][resolved_from] = []
            if resolved_to not in graph['adjacency']:
                graph['adjacency'][resolved_to] = []

            edge = (resolved_from, resolved_to, resolved_weight)
            if resolved_from not in graph['edges']:
                graph['edges'][resolved_from] = []
            graph['edges'][resolved_from].append(edge)
            graph['adjacency'][resolved_from].append(resolved_to)

            context.set(output_var, {
                'from': resolved_from,
                'to': resolved_to,
                'weight': resolved_weight,
                'edge_count': len(graph['edges'])
            })

            return ActionResult(
                success=True,
                message=f"添加边: {resolved_from} -> {resolved_to} (权重={resolved_weight})",
                data={
                    'graph': resolved_graph,
                    'from': resolved_from,
                    'to': resolved_to,
                    'weight': resolved_weight,
                    'edge_count': len(graph['edges']),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加边失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph_name', 'from_node', 'to_node']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'weight': 1, 'output_var': 'add_edge_result'}


class GraphRemoveNodeAction(BaseAction):
    """Remove node from graph."""
    action_type = "graph14_remove_node"
    display_name = "移除节点"
    description = "从图中移除节点"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove node.

        Args:
            context: Execution context.
            params: Dict with graph_name, node, output_var.

        Returns:
            ActionResult with remove result.
        """
        graph_name = params.get('graph_name', 'default')
        node = params.get('node', '')
        output_var = params.get('output_var', 'remove_node_result')

        try:
            resolved_graph = context.resolve_value(graph_name) if graph_name else 'default'
            resolved_node = context.resolve_value(node) if node else ''

            if not hasattr(context, '_graphs') or resolved_graph not in context._graphs:
                return ActionResult(
                    success=False,
                    message=f"图不存在: {resolved_graph}"
                )

            graph = context._graphs[resolved_graph]
            if resolved_node not in graph['nodes']:
                return ActionResult(
                    success=False,
                    message=f"节点不存在: {resolved_node}"
                )

            graph['nodes'].discard(resolved_node)
            graph['adjacency'].pop(resolved_node, None)

            for n in graph['adjacency']:
                graph['adjacency'][n] = [x for x in graph['adjacency'][n] if x != resolved_node]

            if resolved_node in graph['edges']:
                del graph['edges'][resolved_node]

            context.set(output_var, {
                'removed': resolved_node,
                'node_count': len(graph['nodes'])
            })

            return ActionResult(
                success=True,
                message=f"移除节点: {resolved_node} from {resolved_graph} ({len(graph['nodes'])}节点)",
                data={
                    'graph': resolved_graph,
                    'removed': resolved_node,
                    'node_count': len(graph['nodes']),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除节点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph_name', 'node']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'remove_node_result'}


class GraphRemoveEdgeAction(BaseAction):
    """Remove edge from graph."""
    action_type = "graph14_remove_edge"
    display_name = "移除边"
    description = "从图中移除边"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove edge.

        Args:
            context: Execution context.
            params: Dict with graph_name, from_node, to_node, output_var.

        Returns:
            ActionResult with remove result.
        """
        graph_name = params.get('graph_name', 'default')
        from_node = params.get('from_node', '')
        to_node = params.get('to_node', '')
        output_var = params.get('output_var', 'remove_edge_result')

        try:
            resolved_graph = context.resolve_value(graph_name) if graph_name else 'default'
            resolved_from = context.resolve_value(from_node) if from_node else ''
            resolved_to = context.resolve_value(to_node) if to_node else ''

            if not hasattr(context, '_graphs') or resolved_graph not in context._graphs:
                return ActionResult(
                    success=False,
                    message=f"图不存在: {resolved_graph}"
                )

            graph = context._graphs[resolved_graph]

            if resolved_from in graph['edges']:
                original_count = len(graph['edges'][resolved_from])
                graph['edges'][resolved_from] = [
                    (f, t, w) for f, t, w in graph['edges'][resolved_from]
                    if not (f == resolved_from and t == resolved_to)
                ]
                removed_count = original_count - len(graph['edges'][resolved_from])
            else:
                removed_count = 0

            if resolved_from in graph['adjacency']:
                graph['adjacency'][resolved_from] = [
                    x for x in graph['adjacency'][resolved_from] if x != resolved_to
                ]

            context.set(output_var, {
                'removed': removed_count > 0,
                'from': resolved_from,
                'to': resolved_to
            })

            return ActionResult(
                success=True,
                message=f"移除边: {resolved_from} -> {resolved_to} ({'成功' if removed_count else '不存在'})",
                data={
                    'graph': resolved_graph,
                    'from': resolved_from,
                    'to': resolved_to,
                    'removed': removed_count > 0,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除边失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph_name', 'from_node', 'to_node']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'remove_edge_result'}


class GraphBFSAction(BaseAction):
    """Breadth-first search."""
    action_type = "graph14_bfs"
    display_name = "广度优先搜索"
    description = "广度优先搜索"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute BFS.

        Args:
            context: Execution context.
            params: Dict with graph_name, start, target, output_var.

        Returns:
            ActionResult with BFS result.
        """
        graph_name = params.get('graph_name', 'default')
        start = params.get('start', '')
        target = params.get('target', None)
        output_var = params.get('output_var', 'bfs_result')

        try:
            from collections import deque

            resolved_graph = context.resolve_value(graph_name) if graph_name else 'default'
            resolved_start = context.resolve_value(start) if start else ''
            resolved_target = context.resolve_value(target) if target else None

            if not hasattr(context, '_graphs') or resolved_graph not in context._graphs:
                return ActionResult(
                    success=False,
                    message=f"图不存在: {resolved_graph}"
                )

            graph = context._graphs[resolved_graph]

            if resolved_start not in graph['nodes']:
                return ActionResult(
                    success=False,
                    message=f"起始节点不存在: {resolved_start}"
                )

            visited = set()
            queue = deque([resolved_start])
            visited.add(resolved_start)
            order = []

            while queue:
                node = queue.popleft()
                order.append(node)

                if resolved_target and node == resolved_target:
                    break

                for neighbor in graph['adjacency'].get(node, []):
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append(neighbor)

            context.set(output_var, order)

            return ActionResult(
                success=True,
                message=f"BFS: {'找到' if not resolved_target or resolved_target in order else '未找到'} {resolved_target or ''}",
                data={
                    'graph': resolved_graph,
                    'start': resolved_start,
                    'target': resolved_target,
                    'order': order,
                    'visited_count': len(order),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"BFS失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph_name', 'start']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'target': None, 'output_var': 'bfs_result'}


class GraphDFSAction(BaseAction):
    """Depth-first search."""
    action_type = "graph14_dfs"
    display_name = "深度优先搜索"
    description = "深度优先搜索"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute DFS.

        Args:
            context: Execution context.
            params: Dict with graph_name, start, target, output_var.

        Returns:
            ActionResult with DFS result.
        """
        graph_name = params.get('graph_name', 'default')
        start = params.get('start', '')
        target = params.get('target', None)
        output_var = params.get('output_var', 'dfs_result')

        try:
            resolved_graph = context.resolve_value(graph_name) if graph_name else 'default'
            resolved_start = context.resolve_value(start) if start else ''
            resolved_target = context.resolve_value(target) if target else None

            if not hasattr(context, '_graphs') or resolved_graph not in context._graphs:
                return ActionResult(
                    success=False,
                    message=f"图不存在: {resolved_graph}"
                )

            graph = context._graphs[resolved_graph]

            if resolved_start not in graph['nodes']:
                return ActionResult(
                    success=False,
                    message=f"起始节点不存在: {resolved_start}"
                )

            visited = set()
            order = []

            def dfs(node):
                visited.add(node)
                order.append(node)
                if resolved_target and node == resolved_target:
                    return True
                for neighbor in graph['adjacency'].get(node, []):
                    if neighbor not in visited:
                        if dfs(neighbor):
                            return True
                return False

            dfs(resolved_start)

            context.set(output_var, order)

            return ActionResult(
                success=True,
                message=f"DFS: {'找到' if not resolved_target or resolved_target in order else '未找到'} {resolved_target or ''}",
                data={
                    'graph': resolved_graph,
                    'start': resolved_start,
                    'target': resolved_target,
                    'order': order,
                    'visited_count': len(order),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"DFS失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph_name', 'start']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'target': None, 'output_var': 'dfs_result'}