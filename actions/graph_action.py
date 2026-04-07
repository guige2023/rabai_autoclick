"""Graph action module for RabAI AutoClick.

Provides graph data structure operations:
- GraphCreateAction: Create graph
- GraphAddNodeAction: Add graph node
- GraphAddEdgeAction: Add graph edge
- GraphRemoveNodeAction: Remove graph node
- GraphTraverseAction: Traverse graph
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GraphCreateAction(BaseAction):
    """Create graph."""
    action_type = "graph_create"
    display_name = "创建图"
    description = "创建图结构"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with name, directed.

        Returns:
            ActionResult indicating created.
        """
        name = params.get('name', '')
        directed = params.get('directed', False)

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_directed = bool(context.resolve_value(directed))

            graph = {
                'nodes': {},
                'edges': [],
                'directed': resolved_directed
            }
            context.set(f'_graph_{resolved_name}', graph)

            return ActionResult(
                success=True,
                message=f"图 {resolved_name} 创建",
                data={
                    'name': resolved_name,
                    'directed': resolved_directed
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'directed': False}


class GraphAddNodeAction(BaseAction):
    """Add graph node."""
    action_type = "graph_add_node"
    display_name = "添加图节点"
    description = "添加图节点"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add node.

        Args:
            context: Execution context.
            params: Dict with graph_name, node_id, value.

        Returns:
            ActionResult indicating added.
        """
        graph_name = params.get('graph_name', '')
        node_id = params.get('node_id', '')
        value = params.get('value', None)

        valid, msg = self.validate_type(graph_name, str, 'graph_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(node_id, str, 'node_id')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_graph = context.resolve_value(graph_name)
            resolved_node = context.resolve_value(node_id)
            resolved_value = context.resolve_value(value) if value is not None else resolved_node

            graph = context.get(f'_graph_{resolved_graph}')
            if graph is None:
                return ActionResult(
                    success=False,
                    message=f"图 {resolved_graph} 不存在"
                )

            graph['nodes'][resolved_node] = resolved_value

            context.set(f'_graph_{resolved_graph}', graph)

            return ActionResult(
                success=True,
                message=f"添加图节点 {resolved_node}",
                data={
                    'graph_name': resolved_graph,
                    'node_id': resolved_node,
                    'value': resolved_value
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加图节点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph_name', 'node_id']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': None}


class GraphAddEdgeAction(BaseAction):
    """Add graph edge."""
    action_type = "graph_add_edge"
    display_name = "添加图边"
    description = "添加图边"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add edge.

        Args:
            context: Execution context.
            params: Dict with graph_name, from_node, to_node, weight.

        Returns:
            ActionResult indicating added.
        """
        graph_name = params.get('graph_name', '')
        from_node = params.get('from_node', '')
        to_node = params.get('to_node', '')
        weight = params.get('weight', 1)

        valid, msg = self.validate_type(graph_name, str, 'graph_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(from_node, str, 'from_node')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(to_node, str, 'to_node')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_graph = context.resolve_value(graph_name)
            resolved_from = context.resolve_value(from_node)
            resolved_to = context.resolve_value(to_node)
            resolved_weight = float(context.resolve_value(weight))

            graph = context.get(f'_graph_{resolved_graph}')
            if graph is None:
                return ActionResult(
                    success=False,
                    message=f"图 {resolved_graph} 不存在"
                )

            edge = {
                'from': resolved_from,
                'to': resolved_to,
                'weight': resolved_weight
            }
            graph['edges'].append(edge)

            context.set(f'_graph_{resolved_graph}', graph)

            return ActionResult(
                success=True,
                message=f"添加图边 {resolved_from} -> {resolved_to}",
                data={
                    'graph_name': resolved_graph,
                    'from': resolved_from,
                    'to': resolved_to,
                    'weight': resolved_weight
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"添加图边失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph_name', 'from_node', 'to_node']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'weight': 1}


class GraphRemoveNodeAction(BaseAction):
    """Remove graph node."""
    action_type = "graph_remove_node"
    display_name = "移除图节点"
    description = "移除图节点"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove node.

        Args:
            context: Execution context.
            params: Dict with graph_name, node_id.

        Returns:
            ActionResult indicating removed.
        """
        graph_name = params.get('graph_name', '')
        node_id = params.get('node_id', '')

        valid, msg = self.validate_type(graph_name, str, 'graph_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(node_id, str, 'node_id')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_graph = context.resolve_value(graph_name)
            resolved_node = context.resolve_value(node_id)

            graph = context.get(f'_graph_{resolved_graph}')
            if graph is None:
                return ActionResult(
                    success=False,
                    message=f"图 {resolved_graph} 不存在"
                )

            if resolved_node in graph['nodes']:
                del graph['nodes'][resolved_node]

            graph['edges'] = [
                e for e in graph['edges']
                if e['from'] != resolved_node and e['to'] != resolved_node
            ]

            context.set(f'_graph_{resolved_graph}', graph)

            return ActionResult(
                success=True,
                message=f"移除图节点 {resolved_node}",
                data={
                    'graph_name': resolved_graph,
                    'node_id': resolved_node
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除图节点失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph_name', 'node_id']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class GraphTraverseAction(BaseAction):
    """Traverse graph."""
    action_type = "graph_traverse"
    display_name = "遍历图"
    description = "遍历图结构"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute traverse.

        Args:
            context: Execution context.
            params: Dict with graph_name, start_node, mode, output_var.

        Returns:
            ActionResult with traversal result.
        """
        graph_name = params.get('graph_name', '')
        start_node = params.get('start_node', None)
        mode = params.get('mode', 'depth')
        output_var = params.get('output_var', 'traversal_result')

        valid, msg = self.validate_type(graph_name, str, 'graph_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_graph = context.resolve_value(graph_name)
            resolved_start = context.resolve_value(start_node) if start_node is not None else None
            resolved_mode = context.resolve_value(mode)

            graph = context.get(f'_graph_{resolved_graph}')
            if graph is None:
                return ActionResult(
                    success=False,
                    message=f"图 {resolved_graph} 不存在"
                )

            nodes = list(graph['nodes'].keys())
            if not nodes:
                context.set(output_var, [])
                return ActionResult(
                    success=True,
                    message=f"图为空",
                    data={'count': 0}
                )

            if resolved_mode == 'depth':
                visited = set()
                stack = [resolved_start] if resolved_start else [nodes[0]]
                result = []

                while stack:
                    node = stack.pop()
                    if node not in visited:
                        visited.add(node)
                        result.append(node)
                        for edge in graph['edges']:
                            if edge['from'] == node and edge['to'] not in visited:
                                stack.append(edge['to'])
            else:
                visited = set()
                queue = [resolved_start] if resolved_start else [nodes[0]]
                result = []

                while queue:
                    node = queue.pop(0)
                    if node not in visited:
                        visited.add(node)
                        result.append(node)
                        for edge in graph['edges']:
                            if edge['from'] == node and edge['to'] not in visited:
                                queue.append(edge['to'])

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"图遍历完成: {len(result)} 节点",
                data={
                    'graph_name': resolved_graph,
                    'mode': resolved_mode,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"遍历图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['graph_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start_node': None, 'mode': 'depth', 'output_var': 'traversal_result'}
