"""Graph12 action module for RabAI AutoClick.

Provides additional graph operations:
- GraphCreateAction: Create graph
- GraphAddNodeAction: Add node
- GraphAddEdgeAction: Add edge
- GraphBFSAction: Breadth-first search
- GraphDFSAction: Depth-first search
- GraphShortestPathAction: Find shortest path
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GraphCreateAction(BaseAction):
    """Create graph."""
    action_type = "graph12_create"
    display_name = "创建图"
    description = "创建图"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create graph.

        Args:
            context: Execution context.
            params: Dict with directed, output_var.

        Returns:
            ActionResult with graph info.
        """
        directed = params.get('directed', False)
        output_var = params.get('output_var', 'graph_info')

        try:
            resolved_directed = context.resolve_value(directed) if directed else False

            result = {
                'nodes': [],
                'edges': [],
                'directed': resolved_directed,
                'adjacency_list': {}
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"创建图: {'有向' if resolved_directed else '无向'}",
                data={
                    'graph': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'directed': False, 'output_var': 'graph_info'}


class GraphAddNodeAction(BaseAction):
    """Add node."""
    action_type = "graph12_add_node"
    display_name = "添加节点"
    description = "添加节点到图"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add node.

        Args:
            context: Execution context.
            params: Dict with graph, node, data, output_var.

        Returns:
            ActionResult with add status.
        """
        graph = params.get('graph', {})
        node = params.get('node', '')
        data = params.get('data', None)
        output_var = params.get('output_var', 'add_status')

        try:
            resolved_graph = context.resolve_value(graph) if graph else {'nodes': [], 'edges': [], 'directed': False, 'adjacency_list': {}}
            resolved_node = context.resolve_value(node) if node else ''
            resolved_data = context.resolve_value(data) if data is not None else None

            if 'nodes' not in resolved_graph:
                resolved_graph['nodes'] = []
            if 'adjacency_list' not in resolved_graph:
                resolved_graph['adjacency_list'] = {}

            if resolved_node not in resolved_graph['nodes']:
                resolved_graph['nodes'].append(resolved_node)
                resolved_graph['adjacency_list'][resolved_node] = []

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"添加节点: {resolved_node}",
                data={
                    'node': resolved_node,
                    'graph': resolved_graph,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加节点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph', 'node']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'data': None, 'output_var': 'add_status'}


class GraphAddEdgeAction(BaseAction):
    """Add edge."""
    action_type = "graph12_add_edge"
    display_name = "添加边"
    description: "添加边到图"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add edge.

        Args:
            context: Execution context.
            params: Dict with graph, from_node, to_node, weight, output_var.

        Returns:
            ActionResult with add status.
        """
        graph = params.get('graph', {})
        from_node = params.get('from_node', '')
        to_node = params.get('to_node', '')
        weight = params.get('weight', 1)
        output_var = params.get('output_var', 'add_edge_status')

        try:
            resolved_graph = context.resolve_value(graph) if graph else {'nodes': [], 'edges': [], 'directed': False, 'adjacency_list': {}}
            resolved_from = context.resolve_value(from_node) if from_node else ''
            resolved_to = context.resolve_value(to_node) if to_node else ''
            resolved_weight = float(context.resolve_value(weight)) if weight else 1

            if 'edges' not in resolved_graph:
                resolved_graph['edges'] = []
            if 'adjacency_list' not in resolved_graph:
                resolved_graph['adjacency_list'] = {}

            edge = {'from': resolved_from, 'to': resolved_to, 'weight': resolved_weight}
            resolved_graph['edges'].append(edge)

            if resolved_from not in resolved_graph['adjacency_list']:
                resolved_graph['adjacency_list'][resolved_from] = []
            resolved_graph['adjacency_list'][resolved_from].append({'node': resolved_to, 'weight': resolved_weight})

            if not resolved_graph.get('directed', False):
                if resolved_to not in resolved_graph['adjacency_list']:
                    resolved_graph['adjacency_list'][resolved_to] = []
                resolved_graph['adjacency_list'][resolved_to].append({'node': resolved_from, 'weight': resolved_weight})

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"添加边: {resolved_from} -> {resolved_to}",
                data={
                    'edge': edge,
                    'graph': resolved_graph,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加边失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph', 'from_node', 'to_node']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'weight': 1, 'output_var': 'add_edge_status'}


class GraphBFSAction(BaseAction):
    """Breadth-first search."""
    action_type = "graph12_bfs"
    display_name = "广度优先搜索"
    description = "广度优先搜索"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute BFS.

        Args:
            context: Execution context.
            params: Dict with graph, start, output_var.

        Returns:
            ActionResult with BFS result.
        """
        graph = params.get('graph', {})
        start = params.get('start', '')
        output_var = params.get('output_var', 'bfs_result')

        try:
            from collections import deque

            resolved_graph = context.resolve_value(graph) if graph else {'nodes': [], 'edges': [], 'directed': False, 'adjacency_list': {}}
            resolved_start = context.resolve_value(start) if start else ''

            if 'adjacency_list' not in resolved_graph:
                resolved_graph['adjacency_list'] = {}

            visited = set()
            queue = deque([resolved_start])
            result = []

            while queue:
                node = queue.popleft()
                if node not in visited:
                    visited.add(node)
                    result.append(node)
                    for neighbor in resolved_graph['adjacency_list'].get(node, []):
                        if neighbor['node'] not in visited:
                            queue.append(neighbor['node'])

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"BFS: {len(result)}节点",
                data={
                    'start': resolved_start,
                    'visited': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"BFS失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph', 'start']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bfs_result'}


class GraphDFSAction(BaseAction):
    """Depth-first search."""
    action_type = "graph12_dfs"
    display_name = "深度优先搜索"
    description = "深度优先搜索"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute DFS.

        Args:
            context: Execution context.
            params: Dict with graph, start, output_var.

        Returns:
            ActionResult with DFS result.
        """
        graph = params.get('graph', {})
        start = params.get('start', '')
        output_var = params.get('output_var', 'dfs_result')

        try:
            resolved_graph = context.resolve_value(graph) if graph else {'nodes': [], 'edges': [], 'directed': False, 'adjacency_list': {}}
            resolved_start = context.resolve_value(start) if start else ''

            if 'adjacency_list' not in resolved_graph:
                resolved_graph['adjacency_list'] = {}

            visited = set()
            result = []

            def dfs(node):
                if node not in visited:
                    visited.add(node)
                    result.append(node)
                    for neighbor in resolved_graph['adjacency_list'].get(node, []):
                        dfs(neighbor['node'])

            dfs(resolved_start)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"DFS: {len(result)}节点",
                data={
                    'start': resolved_start,
                    'visited': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"DFS失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph', 'start']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dfs_result'}


class GraphShortestPathAction(BaseAction):
    """Find shortest path."""
    action_type = "graph12_shortest_path"
    display_name = "最短路径"
    description = "查找最短路径"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute shortest path.

        Args:
            context: Execution context.
            params: Dict with graph, from_node, to_node, output_var.

        Returns:
            ActionResult with shortest path.
        """
        graph = params.get('graph', {})
        from_node = params.get('from_node', '')
        to_node = params.get('to_node', '')
        output_var = params.get('output_var', 'shortest_path_result')

        try:
            import heapq
            from collections import deque

            resolved_graph = context.resolve_value(graph) if graph else {'nodes': [], 'edges': [], 'directed': False, 'adjacency_list': {}}
            resolved_from = context.resolve_value(from_node) if from_node else ''
            resolved_to = context.resolve_value(to_node) if to_node else ''

            if 'adjacency_list' not in resolved_graph:
                resolved_graph['adjacency_list'] = {}

            # Dijkstra's algorithm
            distances = {node: float('inf') for node in resolved_graph['nodes']}
            distances[resolved_from] = 0
            previous = {node: None for node in resolved_graph['nodes']}
            pq = [(0, resolved_from)]

            while pq:
                current_dist, current = heapq.heappop(pq)
                if current == resolved_to:
                    break
                if current_dist > distances[current]:
                    continue
                for neighbor in resolved_graph['adjacency_list'].get(current, []):
                    neighbor_node = neighbor['node']
                    weight = neighbor['weight']
                    distance = current_dist + weight
                    if distance < distances[neighbor_node]:
                        distances[neighbor_node] = distance
                        previous[neighbor_node] = current
                        heapq.heappush(pq, (distance, neighbor_node))

            # Reconstruct path
            path = []
            current = resolved_to
            while current is not None:
                path.append(current)
                current = previous[current]
            path.reverse()

            if distances[resolved_to] == float('inf'):
                result = {'path': [], 'distance': None}
            else:
                result = {'path': path, 'distance': distances[resolved_to]}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最短路径: {len(path)}节点" if path else "无路径",
                data={
                    'from': resolved_from,
                    'to': resolved_to,
                    'path': result['path'],
                    'distance': result['distance'],
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"最短路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph', 'from_node', 'to_node']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'shortest_path_result'}