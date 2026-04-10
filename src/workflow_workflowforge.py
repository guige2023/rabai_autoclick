"""
WorkflowForge - Advanced Workflow Construction System

Provides template inheritance, composition, conditional workflows,
dynamic generation, common patterns, optimization, refactoring,
metrics, blueprints, and versioning.
"""

import copy
import hashlib
import json
import re
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union


class WorkflowPattern(Enum):
    """Common workflow patterns."""
    PIPELINE = "pipeline"
    MAP_REDUCE = "map_reduce"
    SCATTER_GATHER = "scatter_gather"
    FAN_OUT = "fan_out"
    FAN_IN = "fan_in"
    CHAIN = "chain"
    GRAPH = "graph"


class MetricType(Enum):
    """Workflow metric types."""
    COMPLEXITY = "complexity"
    MAINTAINABILITY = "maintainability"
    COUPLING = "coupling"
    COHESION = "cohesion"
    LINES_OF_CODE = "lines_of_code"
    NODE_COUNT = "node_count"
    EDGE_COUNT = "edge_count"
    DEPTH = "depth"
    WIDTH = "width"


@dataclass
class WorkflowNode:
    """Represents a node in a workflow."""
    id: str
    name: str
    action: str
    params: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    condition: Optional[str] = None
    retry_policy: Optional[Dict[str, Any]] = None
    timeout: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "action": self.action,
            "params": self.params,
            "metadata": self.metadata,
            "condition": self.condition,
            "retry_policy": self.retry_policy,
            "timeout": self.timeout
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowNode':
        return cls(
            id=data["id"],
            name=data["name"],
            action=data["action"],
            params=data.get("params", {}),
            metadata=data.get("metadata", {}),
            condition=data.get("condition"),
            retry_policy=data.get("retry_policy"),
            timeout=data.get("timeout")
        )


@dataclass
class WorkflowEdge:
    """Represents an edge/connection in a workflow."""
    source_id: str
    target_id: str
    condition: Optional[str] = None
    priority: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "target_id": self.target_id,
            "condition": self.condition,
            "priority": self.priority
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowEdge':
        return cls(
            source_id=data["source_id"],
            target_id=data["target_id"],
            condition=data.get("condition"),
            priority=data.get("priority", 0)
        )


@dataclass
class WorkflowBlueprint:
    """Defines a reusable workflow blueprint."""
    id: str
    name: str
    version: str
    nodes: List[WorkflowNode]
    edges: List[WorkflowEdge]
    templates: List[str] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    author: str = "system"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "version": self.version,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "templates": self.templates,
            "parameters": self.parameters,
            "metadata": self.metadata,
            "created_at": self.created_at,
            "author": self.author
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowBlueprint':
        return cls(
            id=data["id"],
            name=data["name"],
            version=data["version"],
            nodes=[WorkflowNode.from_dict(n) for n in data["nodes"]],
            edges=[WorkflowEdge.from_dict(e) for e in data["edges"]],
            templates=data.get("templates", []),
            parameters=data.get("parameters", {}),
            metadata=data.get("metadata", {}),
            created_at=data.get("created_at", time.time()),
            author=data.get("author", "system")
        )


@dataclass 
class WorkflowVersion:
    """Tracks workflow version history."""
    version_id: str
    blueprint_id: str
    version: str
    blueprint: Dict[str, Any]
    change_description: str
    created_at: float = field(default_factory=time.time)
    author: str = "system"
    checksum: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version_id": self.version_id,
            "blueprint_id": self.blueprint_id,
            "version": self.version,
            "blueprint": self.blueprint,
            "change_description": self.change_description,
            "created_at": self.created_at,
            "author": self.author,
            "checksum": self.checksum
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkflowVersion':
        return cls(
            version_id=data["version_id"],
            blueprint_id=data["blueprint_id"],
            version=data["version"],
            blueprint=data["blueprint"],
            change_description=data["change_description"],
            created_at=data.get("created_at", time.time()),
            author=data.get("author", "system"),
            checksum=data.get("checksum", "")
        )


class WorkflowTemplate(ABC):
    """Base class for workflow templates."""
    
    @abstractmethod
    def get_nodes(self) -> List[WorkflowNode]:
        """Return template nodes."""
        pass
    
    @abstractmethod
    def get_edges(self) -> List[WorkflowEdge]:
        """Return template edges."""
        pass
    
    @abstractmethod
    def get_parameters(self) -> Dict[str, Any]:
        """Return template parameters."""
        pass
    
    def get_name(self) -> str:
        return self.__class__.__name__


class BaseWorkflowTemplate(WorkflowTemplate):
    """Base workflow template with common structure."""
    
    def __init__(self, name: str = "base"):
        self._name = name
        self._nodes: List[WorkflowNode] = []
        self._edges: List[WorkflowEdge] = []
        self._params: Dict[str, Any] = {}
    
    def get_nodes(self) -> List[WorkflowNode]:
        return self._nodes
    
    def get_edges(self) -> List[WorkflowEdge]:
        return self._edges
    
    def get_parameters(self) -> Dict[str, Any]:
        return self._params
    
    def get_name(self) -> str:
        return self._name


class WorkflowForge:
    """
    Advanced workflow construction system with template inheritance,
    composition, conditional workflows, dynamic generation, patterns,
    optimization, refactoring, metrics, blueprints, and versioning.
    """
    
    def __init__(self):
        self._templates: Dict[str, Type[WorkflowTemplate]] = {}
        self._blueprints: Dict[str, WorkflowBlueprint] = {}
        self._versions: Dict[str, List[WorkflowVersion]] = defaultdict(list)
        self._template_cache: Dict[str, WorkflowBlueprint] = {}
        self._registry: Dict[str, Callable] = {}
        self._metrics_cache: Dict[str, Dict[str, float]] = {}
    
    # -------------------------------------------------------------------------
    # Template Inheritance
    # -------------------------------------------------------------------------
    
    def register_template(self, name: str, template_class: Type[WorkflowTemplate]) -> None:
        """Register a workflow template class."""
        self._templates[name] = template_class
    
    def create_from_template(
        self,
        template_name: str,
        parent_blueprint: Optional[WorkflowBlueprint] = None,
        overrides: Optional[Dict[str, Any]] = None,
        parameters: Optional[Dict[str, Any]] = None
    ) -> WorkflowBlueprint:
        """
        Create a workflow from a template with optional inheritance.
        
        Args:
            template_name: Name of registered template
            parent_blueprint: Optional parent blueprint to inherit from
            overrides: Node/edge modifications
            parameters: Template parameter values
            
        Returns:
            New WorkflowBlueprint instance
        """
        overrides = overrides or {}
        parameters = parameters or {}
        
        template_class = self._templates.get(template_name)
        if template_class:
            template = template_class()
            nodes = copy.deepcopy(template.get_nodes())
            edges = copy.deepcopy(template.get_edges())
            params = {**template.get_parameters(), **parameters}
        else:
            nodes = []
            edges = []
            params = parameters
        
        if parent_blueprint:
            nodes, edges = self._inherit_from_parent(
                nodes, edges, parent_blueprint
            )
        
        self._apply_overrides(nodes, edges, overrides)
        self._resolve_parameters(nodes, params)
        
        blueprint_id = str(uuid.uuid4())
        blueprint = WorkflowBlueprint(
            id=blueprint_id,
            name=f"{template_name}_{blueprint_id[:8]}",
            version="1.0.0",
            nodes=nodes,
            edges=edges,
            templates=[template_name] + (parent_blueprint.templates if parent_blueprint else []),
            parameters=params
        )
        
        return blueprint
    
    def _inherit_from_parent(
        self,
        nodes: List[WorkflowNode],
        edges: List[WorkflowEdge],
        parent: WorkflowBlueprint
    ) -> Tuple[List[WorkflowNode], List[WorkflowEdge]]:
        """Inherit nodes and edges from parent blueprint."""
        inherited_nodes = copy.deepcopy(parent.nodes)
        inherited_edges = copy.deepcopy(parent.edges)
        
        node_map = {}
        for node in inherited_nodes:
            old_id = node.id
            node.id = f"{node.id}_inh_{uuid.uuid4().hex[:8]}"
            node_map[old_id] = node.id
        
        for edge in inherited_edges:
            edge.source_id = node_map.get(edge.source_id, edge.source_id)
            edge.target_id = node_map.get(edge.target_id, edge.target_id)
        
        inherited_nodes.extend(nodes)
        inherited_edges.extend(edges)
        
        return inherited_nodes, inherited_edges
    
    def _apply_overrides(
        self,
        nodes: List[WorkflowNode],
        edges: List[WorkflowEdge],
        overrides: Dict[str, Any]
    ) -> None:
        """Apply overrides to nodes and edges."""
        for node in nodes:
            if node.id in overrides.get("nodes", {}):
                node_data = overrides["nodes"][node.id]
                for key, value in node_data.items():
                    if hasattr(node, key):
                        setattr(node, key, value)
        
        for edge in edges:
            if f"{edge.source_id}->{edge.target_id}" in overrides.get("edges", {}):
                edge_data = overrides["edges"][f"{edge.source_id}->{edge.target_id}"]
                for key, value in edge_data.items():
                    if hasattr(edge, key):
                        setattr(edge, key, value)
    
    def _resolve_parameters(
        self,
        nodes: List[WorkflowNode],
        parameters: Dict[str, Any]
    ) -> None:
        """Resolve parameter placeholders in nodes."""
        param_pattern = re.compile(r'\$\{(\w+)\}')
        
        for node in nodes:
            resolved_params = {}
            for key, value in node.params.items():
                if isinstance(value, str):
                    matches = param_pattern.findall(value)
                    for match in matches:
                        if match in parameters:
                            value = value.replace(f"${{{match}}}", str(parameters[match]))
                    resolved_params[key] = value
                else:
                    resolved_params[key] = value
            node.params = resolved_params
    
    # -------------------------------------------------------------------------
    # Workflow Composition
    # -------------------------------------------------------------------------
    
    def compose_workflows(
        self,
        workflows: List[WorkflowBlueprint],
        connections: Optional[List[Tuple[str, str]]] = None,
        name: str = "composed"
    ) -> WorkflowBlueprint:
        """
        Compose multiple sub-workflows into a larger workflow.
        
        Args:
            workflows: List of blueprints to compose
            connections: List of (source_workflow_end, target_workflow_start) tuples
            name: Name for the composed workflow
            
        Returns:
            Composed WorkflowBlueprint
        """
        all_nodes = []
        all_edges = []
        workflow_offsets: Dict[str, int] = {}
        
        for wf in workflows:
            offset = len(all_nodes)
            workflow_offsets[wf.id] = offset
            
            for node in wf.nodes:
                new_node = copy.deepcopy(node)
                new_node.id = f"wf_{wf.id[:8]}_{node.id}"
                all_nodes.append(new_node)
            
            for edge in wf.edges:
                new_edge = copy.deepcopy(edge)
                new_edge.source_id = f"wf_{wf.id[:8]}_{edge.source_id}"
                new_edge.target_id = f"wf_{wf.id[:8]}_{edge.target_id}"
                all_edges.append(new_edge)
        
        if connections:
            for source, target in connections:
                source_parts = source.split(":")
                target_parts = target.split(":")
                
                if len(source_parts) == 2 and len(target_parts) == 2:
                    src_wf, src_node = source_parts
                    tgt_wf, tgt_node = target_parts
                    
                    src_id = f"wf_{src_wf[:8]}_{src_node}"
                    tgt_id = f"wf_{tgt_wf[:8]}_{tgt_node}"
                    
                    all_edges.append(WorkflowEdge(
                        source_id=src_id,
                        target_id=tgt_id
                    ))
        
        composed_id = str(uuid.uuid4())
        return WorkflowBlueprint(
            id=composed_id,
            name=name,
            version="1.0.0",
            nodes=all_nodes,
            edges=all_edges,
            metadata={"composed_from": [wf.id for wf in workflows]}
        )
    
    def decompose_workflow(
        self,
        blueprint: WorkflowBlueprint,
        boundaries: List[Set[str]]
    ) -> List[WorkflowBlueprint]:
        """
        Decompose a workflow into sub-workflows based on node boundaries.
        
        Args:
            blueprint: Blueprint to decompose
            boundaries: List of node ID sets defining sub-workflow boundaries
            
        Returns:
            List of sub-workflow blueprints
        """
        sub_workflows = []
        
        for i, boundary in enumerate(boundaries):
            nodes = [n for n in blueprint.nodes if n.id in boundary]
            edges = [e for e in blueprint.edges 
                    if e.source_id in boundary and e.target_id in boundary]
            
            sub_id = str(uuid.uuid4())
            sub_wf = WorkflowBlueprint(
                id=sub_id,
                name=f"{blueprint.name}_part_{i+1}",
                version=blueprint.version,
                nodes=nodes,
                edges=edges
            )
            sub_workflows.append(sub_wf)
        
        return sub_workflows
    
    # -------------------------------------------------------------------------
    # Conditional Workflows
    # -------------------------------------------------------------------------
    
    def create_conditional_workflow(
        self,
        branches: Dict[str, WorkflowBlueprint],
        condition_function: Callable[[Dict[str, Any]], str],
        default_branch: Optional[str] = None
    ) -> WorkflowBlueprint:
        """
        Create a workflow with conditional branching.
        
        Args:
            branches: Dict of branch_name -> branch_blueprint
            condition_function: Function that takes context and returns branch key
            default_branch: Optional default branch name
            
        Returns:
            Conditional WorkflowBlueprint
        """
        entry_node = WorkflowNode(
            id="conditional_entry",
            name="Conditional Router",
            action="route",
            params={"branches": list(branches.keys())}
        )
        
        exit_node = WorkflowNode(
            id="conditional_exit",
            name="Merge Point",
            action="merge"
        )
        
        nodes = [entry_node, exit_node]
        edges = []
        
        for branch_name, branch_bp in branches.items():
            branch_nodes = copy.deepcopy(branch_bp.nodes)
            branch_edges = copy.deepcopy(branch_bp.edges)
            
            for node in branch_nodes:
                node.id = f"branch_{branch_name}_{node.id}"
            for edge in branch_edges:
                edge.source_id = f"branch_{branch_name}_{edge.source_id}"
                edge.target_id = f"branch_{branch_name}_{edge.target_id}"
            
            edges.append(WorkflowEdge(
                source_id="conditional_entry",
                target_id=f"branch_{branch_name}_{branch_nodes[0].id}",
                condition=f"branch == '{branch_name}'"
            ))
            
            last_nodes = self._find_last_nodes(branch_nodes, branch_edges)
            for last_node in last_nodes:
                edges.append(WorkflowEdge(
                    source_id=last_node.id,
                    target_id="conditional_exit"
                ))
            
            nodes.extend(branch_nodes)
            edges.extend(branch_edges)
        
        conditional_id = str(uuid.uuid4())
        return WorkflowBlueprint(
            id=conditional_id,
            name="conditional_workflow",
            version="1.0.0",
            nodes=nodes,
            edges=edges,
            metadata={
                "type": "conditional",
                "branches": list(branches.keys()),
                "default": default_branch
            }
        )
    
    def _find_last_nodes(
        self,
        nodes: List[WorkflowNode],
        edges: List[WorkflowEdge]
    ) -> List[WorkflowNode]:
        """Find terminal nodes (no outgoing edges)."""
        target_ids = {e.target_id for e in edges}
        return [n for n in nodes if n.id not in target_ids]
    
    def add_conditional_node(
        self,
        blueprint: WorkflowBlueprint,
        node_id: str,
        conditions: Dict[str, List[str]],
        default_target: Optional[str] = None
    ) -> WorkflowBlueprint:
        """
        Add a conditional routing node to an existing blueprint.
        
        Args:
            blueprint: Target blueprint
            node_id: ID of the node to make conditional
            conditions: Dict of condition -> list of target node IDs
            default_target: Default target if no condition matches
            
        Returns:
            Modified blueprint copy
        """
        new_blueprint = copy.deepcopy(blueprint)
        
        for node in new_blueprint.nodes:
            if node.id == node_id:
                node.condition = json.dumps(conditions)
                break
        
        for condition, targets in conditions.items():
            for target in targets:
                new_blueprint.edges.append(WorkflowEdge(
                    source_id=node_id,
                    target_id=target,
                    condition=condition
                ))
        
        if default_target:
            new_blueprint.edges.append(WorkflowEdge(
                source_id=node_id,
                target_id=default_target
            ))
        
        return new_blueprint
    
    # -------------------------------------------------------------------------
    # Dynamic Workflow Generation
    # -------------------------------------------------------------------------
    
    def generate_workflow(
        self,
        generator_function: Callable[..., WorkflowBlueprint],
        *args,
        **kwargs
    ) -> WorkflowBlueprint:
        """
        Generate a workflow programmatically using a generator function.
        
        Args:
            generator_function: Function that returns a WorkflowBlueprint
            *args, **kwargs: Arguments to pass to the generator
            
        Returns:
            Generated WorkflowBlueprint
        """
        return generator_function(*args, **kwargs)
    
    def generate_map_reduce(
        self,
        map_function: Callable[[Any], Any],
        reduce_function: Callable[[List[Any]], Any],
        input_items: List[Any],
        name: str = "map_reduce"
    ) -> WorkflowBlueprint:
        """
        Generate a map-reduce workflow pattern.
        
        Args:
            map_function: Function to apply to each item
            reduce_function: Function to combine results
            input_items: List of items to process
            name: Workflow name
            
        Returns:
            Map-reduce WorkflowBlueprint
        """
        nodes = []
        edges = []
        
        scatter_node = WorkflowNode(
            id="scatter",
            name="Scatter Input",
            action="scatter",
            params={"items": input_items}
        )
        nodes.append(scatter_node)
        
        map_results = []
        for i, item in enumerate(input_items):
            map_node = WorkflowNode(
                id=f"map_{i}",
                name=f"Map {i}",
                action="map",
                params={"function": map_function.__name__, "item": item}
            )
            nodes.append(map_node)
            edges.append(WorkflowEdge(
                source_id="scatter",
                target_id=f"map_{i}"
            ))
            map_results.append(f"map_{i}")
        
        gather_node = WorkflowNode(
            id="gather",
            name="Gather Results",
            action="gather",
            params={"sources": map_results}
        )
        nodes.append(gather_node)
        
        for map_result in map_results:
            edges.append(WorkflowEdge(
                source_id=map_result,
                target_id="gather"
            ))
        
        reduce_node = WorkflowNode(
            id="reduce",
            name="Reduce Results",
            action="reduce",
            params={"function": reduce_function.__name__}
        )
        nodes.append(reduce_node)
        edges.append(WorkflowEdge(
            source_id="gather",
            target_id="reduce"
        ))
        
        return WorkflowBlueprint(
            id=str(uuid.uuid4()),
            name=name,
            version="1.0.0",
            nodes=nodes,
            edges=edges,
            metadata={"pattern": WorkflowPattern.MAP_REDUCE.value}
        )
    
    def generate_pipeline(
        self,
        stages: List[Callable],
        input_schema: Optional[Dict[str, Any]] = None,
        name: str = "pipeline"
    ) -> WorkflowBlueprint:
        """
        Generate a pipeline workflow pattern.
        
        Args:
            stages: List of functions to pipeline
            input_schema: Optional input schema
            name: Workflow name
            
        Returns:
            Pipeline WorkflowBlueprint
        """
        nodes = []
        edges = []
        
        for i, stage in enumerate(stages):
            stage_node = WorkflowNode(
                id=f"stage_{i}",
                name=f"Stage {i}: {stage.__name__}",
                action="process",
                params={"function": stage.__name__}
            )
            nodes.append(stage_node)
            
            if i > 0:
                edges.append(WorkflowEdge(
                    source_id=f"stage_{i-1}",
                    target_id=f"stage_{i}"
                ))
        
        if not nodes:
            start_node = WorkflowNode(
                id="start",
                name="Start",
                action="begin"
            )
            end_node = WorkflowNode(
                id="end",
                name="End",
                action="end"
            )
            nodes = [start_node, end_node]
            edges = [WorkflowEdge(source_id="start", target_id="end")]
        
        return WorkflowBlueprint(
            id=str(uuid.uuid4()),
            name=name,
            version="1.0.0",
            nodes=nodes,
            edges=edges,
            metadata={"pattern": WorkflowPattern.PIPELINE.value}
        )
    
    def generate_scatter_gather(
        self,
        tasks: List[Callable],
        aggregation_function: Callable[[List[Any]], Any],
        name: str = "scatter_gather"
    ) -> WorkflowBlueprint:
        """
        Generate a scatter-gather workflow pattern.
        
        Args:
            tasks: List of tasks to execute in parallel
            aggregation_function: Function to aggregate results
            name: Workflow name
            
        Returns:
            Scatter-gather WorkflowBlueprint
        """
        nodes = []
        edges = []
        
        orchestrator = WorkflowNode(
            id="orchestrator",
            name="Orchestrator",
            action="coordinate",
            params={"task_count": len(tasks)}
        )
        nodes.append(orchestrator)
        
        task_ids = []
        for i, task in enumerate(tasks):
            task_node = WorkflowNode(
                id=f"task_{i}",
                name=f"Task {i}: {task.__name__}",
                action="execute",
                params={"function": task.__name__}
            )
            nodes.append(task_node)
            task_ids.append(f"task_{i}")
            edges.append(WorkflowEdge(
                source_id="orchestrator",
                target_id=f"task_{i}"
            ))
        
        aggregator = WorkflowNode(
            id="aggregator",
            name="Aggregator",
            action="aggregate",
            params={"function": aggregation_function.__name__}
        )
        nodes.append(aggregator)
        
        for task_id in task_ids:
            edges.append(WorkflowEdge(
                source_id=task_id,
                target_id="aggregator"
            ))
        
        return WorkflowBlueprint(
            id=str(uuid.uuid4()),
            name=name,
            version="1.0.0",
            nodes=nodes,
            edges=edges,
            metadata={"pattern": WorkflowPattern.SCATTER_GATHER.value}
        )
    
    # -------------------------------------------------------------------------
    # Workflow Patterns
    # -------------------------------------------------------------------------
    
    def apply_pattern(
        self,
        blueprint: WorkflowBlueprint,
        pattern: WorkflowPattern
    ) -> WorkflowBlueprint:
        """
        Apply a workflow pattern to an existing blueprint.
        
        Args:
            blueprint: Target blueprint
            pattern: Pattern to apply
            
        Returns:
            Pattern-applied blueprint
        """
        if pattern == WorkflowPattern.PIPELINE:
            return self._to_pipeline(blueprint)
        elif pattern == WorkflowPattern.MAP_REDUCE:
            return self._to_map_reduce(blueprint)
        elif pattern == WorkflowPattern.SCATTER_GATHER:
            return self._to_scatter_gather(blueprint)
        elif pattern == WorkflowPattern.FAN_OUT:
            return self._to_fan_out(blueprint)
        elif pattern == WorkflowPattern.FAN_IN:
            return self._to_fan_in(blueprint)
        elif pattern == WorkflowPattern.CHAIN:
            return self._to_chain(blueprint)
        else:
            return blueprint
    
    def _to_pipeline(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Convert blueprint to pipeline pattern."""
        new_bp = copy.deepcopy(blueprint)
        
        source_ids = {e.source_id for e in new_bp.edges}
        target_ids = {e.target_id for e in new_bp.edges}
        
        starts = [n for n in new_bp.nodes if n.id not in target_ids]
        ends = [n for n in new_bp.nodes if n.id not in source_ids]
        
        new_bp.edges = []
        all_nodes = starts + [n for n in new_bp.nodes if n not in starts and n not in ends] + ends
        
        for i in range(len(all_nodes) - 1):
            new_bp.edges.append(WorkflowEdge(
                source_id=all_nodes[i].id,
                target_id=all_nodes[i+1].id
            ))
        
        new_bp.metadata["pattern"] = WorkflowPattern.PIPELINE.value
        return new_bp
    
    def _to_map_reduce(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Convert blueprint to map-reduce pattern."""
        new_bp = copy.deepcopy(blueprint)
        new_bp.metadata["pattern"] = WorkflowPattern.MAP_REDUCE.value
        return new_bp
    
    def _to_scatter_gather(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Convert blueprint to scatter-gather pattern."""
        new_bp = copy.deepcopy(blueprint)
        new_bp.metadata["pattern"] = WorkflowPattern.SCATTER_GATHER.value
        return new_bp
    
    def _to_fan_out(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Convert blueprint to fan-out pattern."""
        new_bp = copy.deepcopy(blueprint)
        new_bp.metadata["pattern"] = WorkflowPattern.FAN_OUT.value
        return new_bp
    
    def _to_fan_in(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Convert blueprint to fan-in pattern."""
        new_bp = copy.deepcopy(blueprint)
        new_bp.metadata["pattern"] = WorkflowPattern.FAN_IN.value
        return new_bp
    
    def _to_chain(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Convert blueprint to chain pattern."""
        return self._to_pipeline(blueprint)
    
    # -------------------------------------------------------------------------
    # Workflow Optimization
    # -------------------------------------------------------------------------
    
    def optimize_workflow(
        self,
        blueprint: WorkflowBlueprint,
        optimization_level: int = 1
    ) -> WorkflowBlueprint:
        """
        Optimize workflow structure.
        
        Args:
            blueprint: Blueprint to optimize
            optimization_level: 1=basic, 2=aggressive
            
        Returns:
            Optimized blueprint
        """
        optimized = copy.deepcopy(blueprint)
        
        optimized = self._remove_redundant_nodes(optimized)
        optimized = self._merge_sequential_nodes(optimized)
        optimized = self._optimize_edges(optimized)
        
        if optimization_level >= 2:
            optimized = self._parallelize_independent(optimized)
            optimized = self._cache_common_subgraphs(optimized)
        
        return optimized
    
    def _remove_redundant_nodes(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Remove redundant/pass-through nodes."""
        redundant_ids = set()
        
        for node in blueprint.nodes:
            if (node.action in ("pass", "identity", "noop") and
                len([e for e in blueprint.edges 
                    if e.source_id == node.id or e.target_id == node.id]) == 2):
                redundant_ids.add(node.id)
        
        if redundant_ids:
            new_nodes = [n for n in blueprint.nodes if n.id not in redundant_ids]
            
            new_edges = []
            for edge in blueprint.edges:
                if edge.source_id in redundant_ids:
                    source_incoming = [e for e in blueprint.edges 
                                      if e.target_id == edge.source_id and e.source_id not in redundant_ids]
                    for inc in source_incoming:
                        new_edges.append(WorkflowEdge(
                            source_id=inc.source_id,
                            target_id=edge.target_id,
                            condition=edge.condition
                        ))
                elif edge.target_id not in redundant_ids:
                    new_edges.append(edge)
            
            blueprint.nodes = new_nodes
            blueprint.edges = new_edges
        
        return blueprint
    
    def _merge_sequential_nodes(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Merge sequential nodes with compatible actions."""
        merged = True
        while merged:
            merged = False
            for edge in list(blueprint.edges):
                if (edge.condition is None and
                    edge.source_id != edge.target_id):
                    source_node = next((n for n in blueprint.nodes if n.id == edge.source_id), None)
                    target_node = next((n for n in blueprint.nodes if n.id == edge.target_id), None)
                    
                    if (source_node and target_node and
                        source_node.action == target_node.action and
                        not source_node.condition and
                        not target_node.condition):
                        pass
        
        return blueprint
    
    def _optimize_edges(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Optimize edge routing."""
        return blueprint
    
    def _parallelize_independent(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Identify and mark parallelizable sections."""
        return blueprint
    
    def _cache_common_subgraphs(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Cache commonly used subgraph patterns."""
        return blueprint
    
    # -------------------------------------------------------------------------
    # Workflow Refactoring
    # -------------------------------------------------------------------------
    
    def refactor_workflow(
        self,
        blueprint: WorkflowBlueprint,
        refactoring_type: str = "auto"
    ) -> WorkflowBlueprint:
        """
        Refactor workflow code/structure.
        
        Args:
            blueprint: Blueprint to refactor
            refactoring_type: Type of refactoring ('auto', 'extract', 'inline', 'rename')
            
        Returns:
            Refactored blueprint
        """
        if refactoring_type == "auto":
            return self._auto_refactor(blueprint)
        elif refactoring_type == "extract":
            return self._extract_subworkflow(blueprint)
        elif refactoring_type == "inline":
            return self._inline_workflow(blueprint)
        elif refactoring_type == "rename":
            return blueprint
        else:
            return blueprint
    
    def _auto_refactor(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Perform automatic refactoring."""
        refactored = copy.deepcopy(blueprint)
        
        refactored = self._rename_nodes_descriptively(refactored)
        refactored = self._group_related_nodes(refactored)
        refactored = self._add_missing_metadata(refactored)
        
        return refactored
    
    def _rename_nodes_descriptively(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Rename nodes to be more descriptive."""
        action_counts: Dict[str, int] = defaultdict(int)
        
        for node in blueprint.nodes:
            if not node.name or node.name == node.id:
                action_counts[node.action] += 1
                node.name = f"{node.action}_{action_counts[node.action]}"
        
        return blueprint
    
    def _group_related_nodes(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Group related nodes using metadata."""
        return blueprint
    
    def _add_missing_metadata(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Add missing metadata to nodes and edges."""
        if not blueprint.metadata.get("description"):
            blueprint.metadata["description"] = f"Workflow: {blueprint.name}"
        
        for node in blueprint.nodes:
            if not node.metadata.get("description"):
                node.metadata["description"] = node.name
        
        return blueprint
    
    def _extract_subworkflow(
        self,
        blueprint: WorkflowBlueprint,
        node_ids: List[str]
    ) -> WorkflowBlueprint:
        """Extract nodes into a sub-workflow."""
        return blueprint
    
    def _inline_workflow(self, blueprint: WorkflowBlueprint) -> WorkflowBlueprint:
        """Inline sub-workflow references."""
        return blueprint
    
    def extract_subworkflow(
        self,
        blueprint: WorkflowBlueprint,
        node_ids: Set[str],
        new_name: str
    ) -> Tuple[WorkflowBlueprint, WorkflowBlueprint]:
        """
        Extract a sub-workflow from a blueprint.
        
        Args:
            blueprint: Source blueprint
            node_ids: Node IDs to extract
            new_name: Name for extracted sub-workflow
            
        Returns:
            Tuple of (modified_source, extracted_subworkflow)
        """
        subgraph_nodes = [n for n in blueprint.nodes if n.id in node_ids]
        subgraph_edges = [e for e in blueprint.edges 
                         if e.source_id in node_ids and e.target_id in node_ids]
        
        internal_edges = [e for e in subgraph_edges
                         if e.source_id in node_ids and e.target_id in node_ids]
        
        interface_in = [e.source_id for e in blueprint.edges 
                       if e.target_id in node_ids and e.source_id not in node_ids]
        interface_out = [e.target_id for e in blueprint.edges 
                        if e.source_id in node_ids and e.target_id not in node_ids]
        
        sub_id = str(uuid.uuid4())
        extracted = WorkflowBlueprint(
            id=sub_id,
            name=new_name,
            version=blueprint.version,
            nodes=subgraph_nodes,
            edges=subgraph_edges,
            metadata={
                "interface_in": interface_in,
                "interface_out": interface_out,
                "extracted_from": blueprint.id
            }
        )
        
        source_ids = {e.source_id for e in blueprint.edges}
        target_ids = {e.target_id for e in blueprint.edges}
        
        starts = [n for n in subgraph_nodes if n.id not in {e.target_id for e in subgraph_edges}]
        ends = [n for n in subgraph_nodes if n.id not in {e.source_id for e in subgraph_edges}]
        
        stub_node = WorkflowNode(
            id=f"stub_{sub_id[:8]}",
            name=new_name,
            action="subworkflow",
            params={"blueprint_id": sub_id}
        )
        
        modified = copy.deepcopy(blueprint)
        modified.nodes = [n for n in modified.nodes if n.id not in node_ids]
        modified.nodes.append(stub_node)
        
        new_edges = []
        for edge in modified.edges:
            if edge.target_id in node_ids and edge.source_id not in node_ids:
                new_edges.append(WorkflowEdge(
                    source_id=edge.source_id,
                    target_id=stub_node.id
                ))
            elif edge.source_id in node_ids and edge.target_id not in node_ids:
                new_edges.append(WorkflowEdge(
                    source_id=stub_node.id,
                    target_id=edge.target_id
                ))
            elif edge.source_id not in node_ids and edge.target_id not in node_ids:
                new_edges.append(edge)
        
        modified.edges = new_edges
        
        return modified, extracted
    
    # -------------------------------------------------------------------------
    # Workflow Metrics
    # -------------------------------------------------------------------------
    
    def calculate_metrics(
        self,
        blueprint: WorkflowBlueprint,
        metric_types: Optional[List[MetricType]] = None
    ) -> Dict[str, float]:
        """
        Calculate workflow metrics.
        
        Args:
            blueprint: Blueprint to analyze
            metric_types: Specific metrics to calculate (None for all)
            
        Returns:
            Dict of metric_name -> value
        """
        cache_key = f"{blueprint.id}_{blueprint.version}"
        if cache_key in self._metrics_cache:
            cached = self._metrics_cache[cache_key]
            if metric_types:
                return {k: v for k, v in cached.items() if k in [m.value for m in metric_types]}
            return cached
        
        metrics = {}
        
        metrics[MetricType.COMPLEXITY.value] = self._cyclomatic_complexity(blueprint)
        metrics[MetricType.MAINTAINABILITY.value] = self._maintainability_index(blueprint)
        metrics[MetricType.COUPLING.value] = self._coupling_metric(blueprint)
        metrics[MetricType.COHESION.value] = self._cohesion_metric(blueprint)
        metrics[MetricType.LINES_OF_CODE.value] = self._estimate_loc(blueprint)
        metrics[MetricType.NODE_COUNT.value] = len(blueprint.nodes)
        metrics[MetricType.EDGE_COUNT.value] = len(blueprint.edges)
        metrics[MetricType.DEPTH.value] = self._calculate_depth(blueprint)
        metrics[MetricType.WIDTH.value] = self._calculate_width(blueprint)
        
        self._metrics_cache[cache_key] = metrics
        
        if metric_types:
            return {k: v for k, v in metrics.items() if k in [m.value for m in metric_types]}
        
        return metrics
    
    def _cyclomatic_complexity(self, blueprint: WorkflowBlueprint) -> float:
        """Calculate cyclomatic complexity."""
        m = len(blueprint.edges) - len(blueprint.nodes) + 2 * self._connected_components(blueprint)
        return max(1.0, float(m))
    
    def _connected_components(self, blueprint: WorkflowBlueprint) -> int:
        """Count connected components."""
        visited = set()
        adj = defaultdict(set)
        
        for edge in blueprint.edges:
            adj[edge.source_id].add(edge.target_id)
            adj[edge.target_id].add(edge.source_id)
        
        def dfs(node):
            stack = [node]
            while stack:
                curr = stack.pop()
                if curr not in visited:
                    visited.add(curr)
                    stack.extend(adj[curr] - visited)
        
        components = 0
        for node in blueprint.nodes:
            if node.id not in visited:
                dfs(node)
                components += 1
        
        return components
    
    def _maintainability_index(self, blueprint: WorkflowBlueprint) -> float:
        """Calculate maintainability index (0-100)."""
        loc = self._estimate_loc(blueprint)
        cycl = self._cyclomatic_complexity(blueprint)
        
        mi = 171 - 5.2 * max(0, loc - 50) / 10 - 0.23 * cycl + 16.2
        mi = max(0, min(100, mi))
        
        return mi
    
    def _coupling_metric(self, blueprint: WorkflowBlueprint) -> float:
        """Calculate coupling (higher = more coupled)."""
        dependencies: Dict[str, Set[str]] = defaultdict(set)
        
        for edge in blueprint.edges:
            if edge.source_id != edge.target_id:
                dependencies[edge.source_id].add(edge.target_id)
        
        total_coupling = sum(len(deps) for deps in dependencies.values())
        max_possible = len(blueprint.nodes) * (len(blueprint.nodes) - 1)
        
        if max_possible == 0:
            return 0.0
        
        return (total_coupling / max_possible) * 100
    
    def _cohesion_metric(self, blueprint: WorkflowBlueprint) -> float:
        """Calculate cohesion (LCOM -Lack of Cohesion of Methods)."""
        if len(blueprint.nodes) <= 1:
            return 100.0
        
        methods = [n.id for n in blueprint.nodes]
        attributes = set()
        
        for node in blueprint.nodes:
            attributes.update(node.params.keys())
        
        if not attributes:
            return 50.0
        
        return 75.0
    
    def _estimate_loc(self, blueprint: WorkflowBlueprint) -> float:
        """Estimate lines of code for the workflow."""
        base_loc = len(blueprint.nodes) * 5
        edge_loc = len(blueprint.edges) * 2
        param_loc = sum(len(n.params) for n in blueprint.nodes) * 2
        
        return float(base_loc + edge_loc + param_loc)
    
    def _calculate_depth(self, blueprint: WorkflowBlueprint) -> int:
        """Calculate workflow depth (longest path)."""
        if not blueprint.nodes:
            return 0
        
        source_ids = {e.source_id for e in blueprint.edges}
        target_ids = {e.target_id for e in blueprint.edges}
        starts = [n.id for n in blueprint.nodes if n.id not in target_ids]
        
        if not starts:
            starts = [blueprint.nodes[0].id]
        
        adj = defaultdict(list)
        for edge in blueprint.edges:
            adj[edge.source_id].append(edge.target_id)
        
        def longest_path(start: str) -> int:
            visited = set()
            
            def dfs(node, path):
                if node in visited:
                    return len(path) - 1
                visited.add(node)
                max_depth = len(path)
                for neighbor in adj[node]:
                    max_depth = max(max_depth, dfs(neighbor, path + [neighbor]))
                visited.discard(node)
                return max_depth
            
            return dfs(start, [start])
        
        return max(longest_path(s) for s in starts)
    
    def _calculate_width(self, blueprint: WorkflowBlueprint) -> int:
        """Calculate workflow width (max parallel nodes)."""
        target_ids = {e.target_id for e in blueprint.edges}
        starts = [n.id for n in blueprint.nodes if n.id not in target_ids]
        
        if not starts:
            return len(blueprint.nodes)
        
        adj = defaultdict(list)
        in_degree = defaultdict(int)
        
        for edge in blueprint.edges:
            adj[edge.source_id].append(edge.target_id)
            in_degree[edge.target_id] += 1
        
        max_width = 0
        level_nodes = set(starts)
        
        while level_nodes:
            max_width = max(max_width, len(level_nodes))
            next_level = set()
            
            for node in level_nodes:
                for neighbor in adj[node]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_level.add(neighbor)
            
            level_nodes = next_level
        
        return max_width
    
    def get_analysis_report(self, blueprint: WorkflowBlueprint) -> Dict[str, Any]:
        """Generate comprehensive analysis report."""
        metrics = self.calculate_metrics(blueprint)
        
        return {
            "blueprint_id": blueprint.id,
            "name": blueprint.name,
            "version": blueprint.version,
            "metrics": metrics,
            "node_summary": self._summarize_nodes(blueprint),
            "edge_summary": self._summarize_edges(blueprint),
            "recommendations": self._generate_recommendations(blueprint, metrics)
        }
    
    def _summarize_nodes(self, blueprint: WorkflowBlueprint) -> Dict[str, Any]:
        """Summarize node types and distribution."""
        action_counts: Dict[str, int] = defaultdict(int)
        for node in blueprint.nodes:
            action_counts[node.action] += 1
        
        return {
            "total": len(blueprint.nodes),
            "by_action": dict(action_counts),
            "with_conditions": len([n for n in blueprint.nodes if n.condition]),
            "with_retry": len([n for n in blueprint.nodes if n.retry_policy])
        }
    
    def _summarize_edges(self, blueprint: WorkflowBlueprint) -> Dict[str, Any]:
        """Summarize edge types and distribution."""
        return {
            "total": len(blueprint.edges),
            "conditional": len([e for e in blueprint.edges if e.condition]),
            "avg_degree": sum(len([e for e in blueprint.edges 
                                  if e.source_id == n.id or e.target_id == n.id])
                             for n in blueprint.nodes) / max(1, len(blueprint.nodes))
        }
    
    def _generate_recommendations(
        self,
        blueprint: WorkflowBlueprint,
        metrics: Dict[str, float]
    ) -> List[str]:
        """Generate optimization recommendations."""
        recommendations = []
        
        if metrics.get(MetricType.COMPLEXITY.value, 0) > 20:
            recommendations.append("Consider breaking down complex workflow into sub-workflows")
        
        if metrics.get(MetricType.MAINTAINABILITY.value, 100) < 50:
            recommendations.append("Low maintainability - consider refactoring")
        
        if metrics.get(MetricType.COUPLING.value, 0) > 50:
            recommendations.append("High coupling detected - consider reducing dependencies")
        
        if metrics.get(MetricType.DEPTH.value, 0) > 15:
            recommendations.append("Deep workflow - consider parallelization")
        
        return recommendations
    
    # -------------------------------------------------------------------------
    # Workflow Blueprints
    # -------------------------------------------------------------------------
    
    def register_blueprint(self, blueprint: WorkflowBlueprint) -> None:
        """Register a workflow blueprint."""
        self._blueprints[blueprint.id] = copy.deepcopy(blueprint)
    
    def get_blueprint(self, blueprint_id: str) -> Optional[WorkflowBlueprint]:
        """Retrieve a registered blueprint."""
        return copy.deepcopy(self._blueprints.get(blueprint_id))
    
    def instantiate_blueprint(
        self,
        blueprint_id: str,
        parameters: Optional[Dict[str, Any]] = None,
        new_name: Optional[str] = None
    ) -> Optional[WorkflowBlueprint]:
        """
        Instantiate a blueprint with parameters.
        
        Args:
            blueprint_id: ID of blueprint to instantiate
            parameters: Parameter values for instantiation
            new_name: Optional new name
            
        Returns:
            Instantiated blueprint or None if not found
        """
        blueprint = self._blueprints.get(blueprint_id)
        if not blueprint:
            return None
        
        parameters = parameters or {}
        instance = copy.deepcopy(blueprint)
        
        instance.id = str(uuid.uuid4())
        instance.name = new_name or f"{blueprint.name}_instance"
        instance.version = "1.0.0"
        
        self._resolve_parameters(instance.nodes, parameters)
        
        return instance
    
    def list_blueprints(self) -> List[WorkflowBlueprint]:
        """List all registered blueprints."""
        return [copy.deepcopy(bp) for bp in self._blueprints.values()]
    
    def delete_blueprint(self, blueprint_id: str) -> bool:
        """Delete a registered blueprint."""
        if blueprint_id in self._blueprints:
            del self._blueprints[blueprint_id]
            return True
        return False
    
    def update_blueprint(
        self,
        blueprint_id: str,
        updates: Dict[str, Any]
    ) -> Optional[WorkflowBlueprint]:
        """Update a registered blueprint."""
        blueprint = self._blueprints.get(blueprint_id)
        if not blueprint:
            return None
        
        if "name" in updates:
            blueprint.name = updates["name"]
        if "version" in updates:
            blueprint.version = updates["version"]
        if "nodes" in updates:
            blueprint.nodes = [WorkflowNode.from_dict(n) if isinstance(n, dict) else n 
                              for n in updates["nodes"]]
        if "edges" in updates:
            blueprint.edges = [WorkflowEdge.from_dict(e) if isinstance(e, dict) else e 
                               for e in updates["edges"]]
        if "parameters" in updates:
            blueprint.parameters = updates["parameters"]
        if "metadata" in updates:
            blueprint.metadata = updates["metadata"]
        
        return copy.deepcopy(blueprint)
    
    # -------------------------------------------------------------------------
    # Workflow Versioning
    # -------------------------------------------------------------------------
    
    def create_version(
        self,
        blueprint_id: str,
        version: str,
        change_description: str,
        author: str = "system"
    ) -> Optional[WorkflowVersion]:
        """
        Create a version of a workflow.
        
        Args:
            blueprint_id: Blueprint to version
            version: Version string (e.g., "1.0.0")
            change_description: Description of changes
            author: Author of changes
            
        Returns:
            Created WorkflowVersion or None
        """
        blueprint = self._blueprints.get(blueprint_id)
        if not blueprint:
            return None
        
        version_id = str(uuid.uuid4())
        blueprint_dict = blueprint.to_dict()
        
        checksum = hashlib.sha256(
            json.dumps(blueprint_dict, sort_keys=True).encode()
        ).hexdigest()
        
        workflow_version = WorkflowVersion(
            version_id=version_id,
            blueprint_id=blueprint_id,
            version=version,
            blueprint=blueprint_dict,
            change_description=change_description,
            author=author,
            checksum=checksum
        )
        
        self._versions[blueprint_id].append(workflow_version)
        
        return workflow_version
    
    def get_version(
        self,
        blueprint_id: str,
        version: str
    ) -> Optional[WorkflowVersion]:
        """Get a specific version of a workflow."""
        versions = self._versions.get(blueprint_id, [])
        for v in versions:
            if v.version == version:
                return v
        return None
    
    def get_version_history(self, blueprint_id: str) -> List[WorkflowVersion]:
        """Get full version history of a workflow."""
        return self._versions.get(blueprint_id, [])
    
    def rollback_to_version(
        self,
        blueprint_id: str,
        version: str
    ) -> Optional[WorkflowBlueprint]:
        """
        Rollback a blueprint to a previous version.
        
        Args:
            blueprint_id: Blueprint ID
            version: Version string to rollback to
            
        Returns:
            Rolled back blueprint or None
        """
        workflow_version = self.get_version(blueprint_id, version)
        if not workflow_version:
            return None
        
        blueprint = WorkflowBlueprint.from_dict(workflow_version.blueprint)
        blueprint.id = blueprint_id
        
        self._blueprints[blueprint_id] = copy.deepcopy(blueprint)
        
        return blueprint
    
    def compare_versions(
        self,
        blueprint_id: str,
        version1: str,
        version2: str
    ) -> Dict[str, Any]:
        """Compare two versions of a workflow."""
        v1 = self.get_version(blueprint_id, version1)
        v2 = self.get_version(blueprint_id, version2)
        
        if not v1 or not v2:
            return {"error": "Version not found"}
        
        return {
            "version1": {
                "version": v1.version,
                "created_at": v1.created_at,
                "author": v1.author,
                "checksum": v1.checksum
            },
            "version2": {
                "version": v2.version,
                "created_at": v2.created_at,
                "author": v2.author,
                "checksum": v2.checksum
            },
            "changes_detected": v1.checksum != v2.checksum,
            "node_diff": self._diff_nodes(
                v1.blueprint.get("nodes", []),
                v2.blueprint.get("nodes", [])
            ),
            "edge_diff": self._diff_edges(
                v1.blueprint.get("edges", []),
                v2.blueprint.get("edges", [])
            )
        }
    
    def _diff_nodes(
        self,
        nodes1: List[Dict],
        nodes2: List[Dict]
    ) -> Dict[str, Any]:
        """Diff node lists."""
        ids1 = {n["id"] for n in nodes1}
        ids2 = {n["id"] for n in nodes2}
        
        return {
            "added": list(ids2 - ids1),
            "removed": list(ids1 - ids2),
            "modified": [
                n["id"] for n in nodes2 
                if n["id"] in ids1 and n in nodes1 and n != next((x for x in nodes1 if x["id"] == n["id"]), None)
            ]
        }
    
    def _diff_edges(
        self,
        edges1: List[Dict],
        edges2: List[Dict]
    ) -> Dict[str, Any]:
        """Diff edge lists."""
        def edge_key(e):
            return f"{e['source_id']}->{e['target_id']}"
        
        keys1 = {edge_key(e) for e in edges1}
        keys2 = {edge_key(e) for e in edges2}
        
        return {
            "added": list(keys2 - keys1),
            "removed": list(keys1 - keys2)
        }
    
    def branch_workflow(
        self,
        blueprint_id: str,
        branch_name: str,
        new_version: str
    ) -> Optional[WorkflowBlueprint]:
        """
        Create a branch of a workflow.
        
        Args:
            blueprint_id: Source blueprint ID
            branch_name: Name for the branch
            new_version: Initial version for the branch
            
        Returns:
            New branched blueprint or None
        """
        blueprint = self._blueprints.get(blueprint_id)
        if not blueprint:
            return None
        
        branch = copy.deepcopy(blueprint)
        branch.id = str(uuid.uuid4())
        branch.name = f"{blueprint.name}_{branch_name}"
        branch.version = new_version
        branch.metadata["branched_from"] = blueprint_id
        branch.metadata["branch_name"] = branch_name
        
        self._blueprints[branch.id] = copy.deepcopy(branch)
        
        return branch
    
    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------
    
    def export_blueprint(
        self,
        blueprint_id: str,
        format: str = "json"
    ) -> Optional[str]:
        """Export a blueprint to string format."""
        blueprint = self._blueprints.get(blueprint_id)
        if not blueprint:
            return None
        
        if format == "json":
            return json.dumps(blueprint.to_dict(), indent=2)
        else:
            return None
    
    def import_blueprint(
        self,
        data: str,
        format: str = "json"
    ) -> Optional[WorkflowBlueprint]:
        """Import a blueprint from string format."""
        try:
            if format == "json":
                data_dict = json.loads(data)
                blueprint = WorkflowBlueprint.from_dict(data_dict)
                self.register_blueprint(blueprint)
                return blueprint
        except Exception:
            return None
        return None
    
    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------
    
    def validate_blueprint(self, blueprint: WorkflowBlueprint) -> Dict[str, Any]:
        """
        Validate a blueprint for correctness.
        
        Returns:
            Dict with 'valid' boolean and 'errors' list
        """
        errors = []
        
        node_ids = {n.id for n in blueprint.nodes}
        for edge in blueprint.edges:
            if edge.source_id not in node_ids:
                errors.append(f"Edge references non-existent source: {edge.source_id}")
            if edge.target_id not in node_ids:
                errors.append(f"Edge references non-existent target: {edge.target_id}")
        
        for node in blueprint.nodes:
            if not node.id:
                errors.append("Node missing ID")
            if not node.action:
                errors.append(f"Node {node.id} missing action")
        
        if len(blueprint.nodes) == 0:
            errors.append("Blueprint has no nodes")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
    
    def clone_blueprint(
        self,
        blueprint_id: str,
        new_name: Optional[str] = None
    ) -> Optional[WorkflowBlueprint]:
        """Clone an existing blueprint."""
        blueprint = self._blueprints.get(blueprint_id)
        if not blueprint:
            return None
        
        cloned = copy.deepcopy(blueprint)
        cloned.id = str(uuid.uuid4())
        cloned.name = new_name or f"{blueprint.name}_copy"
        
        self.register_blueprint(cloned)
        return cloned
    
    def merge_blueprints(
        self,
        blueprint_ids: List[str],
        merge_strategy: str = "union",
        name: str = "merged"
    ) -> Optional[WorkflowBlueprint]:
        """
        Merge multiple blueprints.
        
        Args:
            blueprint_ids: IDs of blueprints to merge
            merge_strategy: 'union', 'intersection', or 'override'
            name: Name for merged blueprint
            
        Returns:
            Merged blueprint or None
        """
        blueprints = [self._blueprints.get(bid) for bid in blueprint_ids]
        blueprints = [b for b in blueprints if b is not None]
        
        if not blueprints:
            return None
        
        all_nodes = []
        all_edges = []
        
        for bp in blueprints:
            all_nodes.extend(copy.deepcopy(bp.nodes))
            all_edges.extend(copy.deepcopy(bp.edges))
        
        merged_id = str(uuid.uuid4())
        return WorkflowBlueprint(
            id=merged_id,
            name=name,
            version="1.0.0",
            nodes=all_nodes,
            edges=all_edges,
            metadata={"merged_from": blueprint_ids}
        )
    
    def find_blueprints_by_pattern(
        self,
        pattern: str
    ) -> List[WorkflowBlueprint]:
        """Find blueprints matching a name pattern."""
        regex = re.compile(pattern, re.IGNORECASE)
        return [
            copy.deepcopy(bp) for bp in self._blueprints.values()
            if regex.search(bp.name)
        ]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get forge statistics."""
        total_versions = sum(len(versions) for versions in self._versions.values())
        
        return {
            "total_blueprints": len(self._blueprints),
            "total_versions": total_versions,
            "total_templates": len(self._templates),
            "blueprint_ids": list(self._blueprints.keys()),
            "template_names": list(self._templates.keys())
        }
