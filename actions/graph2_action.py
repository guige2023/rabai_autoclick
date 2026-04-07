"""Graph2 action module for RabAI AutoClick.

Provides additional graph operations:
- GraphBFSAction: Breadth-first search
- GraphDFSAction: Depth-first search
- GraphShortestPathAction: Find shortest path
- GraphHasCycleAction: Check if graph has cycle
- GraphTopologicalSortAction: Topological sort
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GraphBFSAction(BaseAction):
    """Breadth-first search."""
    action_type = "graph2_bfs"
    display_name = "广度优先搜索"
    description = "广度优先搜索图"
    version = "2.0"

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
            ActionResult with BFS traversal order.
        """
        graph = params.get('graph', {})
        start = params.get('start', 0)
        output_var = params.get('output_var', 'bfs_result')

        try:
            from collections import deque

            resolved_graph = context.resolve_value(graph)
            resolved_start = context.resolve_value(start)

            if not isinstance(resolved_graph, dict):
                return ActionResult(
                    success=False,
                    message="BFS失败: 图必须是字典格式"
                )

            visited = set()
            queue = deque([resolved_start])
            result = []

            while queue:
                node = queue.popleft()
                if node not in visited:
                    visited.add(node)
                    result.append(node)
                    if node in resolved_graph:
                        for neighbor in resolved_graph[node]:
                            if neighbor not in visited:
                                queue.append(neighbor)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"BFS完成: 访问{len(result)}个节点",
                data={
                    'graph': resolved_graph,
                    'start': resolved_start,
                    'traversal': result,
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
    action_type = "graph2_dfs"
    display_name = "深度优先搜索"
    description = "深度优先搜索图"
    version = "2.0"

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
            ActionResult with DFS traversal order.
        """
        graph = params.get('graph', {})
        start = params.get('start', 0)
        output_var = params.get('output_var', 'dfs_result')

        try:
            resolved_graph = context.resolve_value(graph)
            resolved_start = context.resolve_value(start)

            if not isinstance(resolved_graph, dict):
                return ActionResult(
                    success=False,
                    message="DFS失败: 图必须是字典格式"
                )

            visited = set()
            result = []

            def dfs(node):
                if node not in visited:
                    visited.add(node)
                    result.append(node)
                    if node in resolved_graph:
                        for neighbor in resolved_graph[node]:
                            dfs(neighbor)

            dfs(resolved_start)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"DFS完成: 访问{len(result)}个节点",
                data={
                    'graph': resolved_graph,
                    'start': resolved_start,
                    'traversal': result,
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
    action_type = "graph2_shortest_path"
    display_name = "最短路径"
    description = "使用BFS查找最短路径"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute shortest path.

        Args:
            context: Execution context.
            params: Dict with graph, start, end, output_var.

        Returns:
            ActionResult with shortest path.
        """
        graph = params.get('graph', {})
        start = params.get('start', 0)
        end = params.get('end', 0)
        output_var = params.get('output_var', 'shortest_path')

        try:
            from collections import deque

            resolved_graph = context.resolve_value(graph)
            resolved_start = context.resolve_value(start)
            resolved_end = context.resolve_value(end)

            if not isinstance(resolved_graph, dict):
                return ActionResult(
                    success=False,
                    message="最短路径失败: 图必须是字典格式"
                )

            visited = {resolved_start: [resolved_start]}
            queue = deque([resolved_start])

            while queue:
                node = queue.popleft()
                if node == resolved_end:
                    break
                if node in resolved_graph:
                    for neighbor in resolved_graph[node]:
                        if neighbor not in visited:
                            visited[neighbor] = visited[node] + [neighbor]
                            queue.append(neighbor)

            result = visited.get(resolved_end, None)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最短路径: {' -> '.join(map(str, result)) if result else '无路径'}",
                data={
                    'graph': resolved_graph,
                    'start': resolved_start,
                    'end': resolved_end,
                    'path': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"最短路径失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph', 'start', 'end']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'shortest_path'}


class GraphHasCycleAction(BaseAction):
    """Check if graph has cycle."""
    action_type = "graph2_has_cycle"
    display_name = "检测环"
    description = "检测图是否有环"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute has cycle.

        Args:
            context: Execution context.
            params: Dict with graph, output_var.

        Returns:
            ActionResult with has cycle result.
        """
        graph = params.get('graph', {})
        output_var = params.get('output_var', 'has_cycle')

        try:
            resolved_graph = context.resolve_value(graph)

            if not isinstance(resolved_graph, dict):
                return ActionResult(
                    success=False,
                    message="检测环失败: 图必须是字典格式"
                )

            visited = set()
            rec_stack = set()

            def has_cycle_util(node):
                visited.add(node)
                rec_stack.add(node)
                if node in resolved_graph:
                    for neighbor in resolved_graph[node]:
                        if neighbor not in visited:
                            if has_cycle_util(neighbor):
                                return True
                        elif neighbor in rec_stack:
                            return True
                rec_stack.remove(node)
                return False

            result = False
            for node in resolved_graph:
                if node not in visited:
                    if has_cycle_util(node):
                        result = True
                        break

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"检测环: {'有环' if result else '无环'}",
                data={
                    'graph': resolved_graph,
                    'has_cycle': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检测环失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'has_cycle'}


class GraphTopologicalSortAction(BaseAction):
    """Topological sort."""
    action_type = "graph2_topological_sort"
    display_name = "拓扑排序"
    description = "对图进行拓扑排序"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute topological sort.

        Args:
            context: Execution context.
            params: Dict with graph, output_var.

        Returns:
            ActionResult with topological order.
        """
        graph = params.get('graph', {})
        output_var = params.get('output_var', 'topo_order')

        try:
            resolved_graph = context.resolve_value(graph)

            if not isinstance(resolved_graph, dict):
                return ActionResult(
                    success=False,
                    message="拓扑排序失败: 图必须是字典格式"
                )

            in_degree = {node: 0 for node in resolved_graph}
            for node in resolved_graph:
                for neighbor in resolved_graph[node]:
                    if neighbor in in_degree:
                        in_degree[neighbor] += 1

            queue = [node for node in in_degree if in_degree[node] == 0]
            result = []

            while queue:
                node = queue.pop(0)
                result.append(node)
                if node in resolved_graph:
                    for neighbor in resolved_graph[node]:
                        if neighbor in in_degree:
                            in_degree[neighbor] -= 1
                            if in_degree[neighbor] == 0:
                                queue.append(neighbor)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"拓扑排序: {' -> '.join(map(str, result))}",
                data={
                    'graph': resolved_graph,
                    'topo_order': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"拓扑排序失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'topo_order'}