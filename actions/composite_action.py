"""Composite action module for RabAI AutoClick.

Provides composite pattern implementation:
- Component: Abstract component interface
- Leaf: Individual element
- Composite: Container of components
- TreeBuilder: Build component trees
"""

from typing import Any, Callable, Dict, List, Optional
from abc import ABC, abstractmethod
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Component(ABC):
    """Abstract component."""

    @property
    @abstractmethod
    def id(self) -> str:
        """Get component ID."""
        pass

    @abstractmethod
    def execute(self, context: Any = None) -> Any:
        """Execute component."""
        pass

    @abstractmethod
    def get_children(self) -> List["Component"]:
        """Get child components."""
        pass


class Leaf(Component):
    """Leaf component - no children."""

    def __init__(self, leaf_id: str, name: str = "", action_fn: Optional[Callable] = None):
        self._id = leaf_id
        self._name = name or leaf_id
        self._action_fn = action_fn or (lambda: None)
        self._metadata: Dict[str, Any] = {}

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    def execute(self, context: Any = None) -> Any:
        """Execute leaf action."""
        return self._action_fn(context)

    def get_children(self) -> List[Component]:
        """Leaf has no children."""
        return []

    def set_metadata(self, key: str, value: Any) -> None:
        """Set metadata."""
        self._metadata[key] = value

    def get_metadata(self, key: str) -> Any:
        """Get metadata."""
        return self._metadata.get(key)


class Composite(Component):
    """Composite component - contains children."""

    def __init__(self, composite_id: str, name: str = "", strategy: str = "sequence"):
        self._id = composite_id
        self._name = name or composite_id
        self._children: List[Component] = []
        self._strategy = strategy
        self._metadata: Dict[str, Any] = {}

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    def add(self, component: Component) -> None:
        """Add a child component."""
        self._children.append(component)

    def remove(self, component_id: str) -> bool:
        """Remove a child component."""
        for i, child in enumerate(self._children):
            if child.id == component_id:
                self._children.pop(i)
                return True
        return False

    def get_children(self) -> List[Component]:
        """Get all children."""
        return self._children.copy()

    def execute(self, context: Any = None) -> List[Any]:
        """Execute all children based on strategy."""
        if self._strategy == "sequence":
            return self._execute_sequence(context)
        elif self._strategy == "parallel":
            return self._execute_parallel(context)
        elif self._strategy == "race":
            return self._execute_race(context)
        elif self._strategy == "first_success":
            return self._execute_first_success(context)
        else:
            return self._execute_sequence(context)

    def _execute_sequence(self, context: Any) -> List[Any]:
        """Execute children in sequence."""
        results = []
        for child in self._children:
            result = child.execute(context)
            results.append(result)
        return results

    def _execute_parallel(self, context: Any) -> List[Any]:
        """Execute children in parallel."""
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self._children)) as executor:
            futures = {executor.submit(child.execute, context): child for child in self._children}
            results = []
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
        return results

    def _execute_race(self, context: Any) -> List[Any]:
        """Execute children in race - return first completed."""
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self._children)) as executor:
            futures = {executor.submit(child.execute, context): child for child in self._children}
            done, _ = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            results = []
            for future in done:
                results.append(future.result())
        return results

    def _execute_first_success(self, context: Any) -> List[Any]:
        """Execute until first success."""
        results = []
        for child in self._children:
            try:
                result = child.execute(context)
                if result is not None:
                    return [result]
                results.append(result)
            except Exception:
                results.append(None)
        return results

    def set_metadata(self, key: str, value: Any) -> None:
        """Set metadata."""
        self._metadata[key] = value

    def find(self, component_id: str) -> Optional[Component]:
        """Find component by ID."""
        if self._id == component_id:
            return self
        for child in self._children:
            if child.id == component_id:
                return child
            if isinstance(child, Composite):
                found = child.find(component_id)
                if found:
                    return found
        return None

    def find_all(self, predicate: Callable[[Component], bool]) -> List[Component]:
        """Find all components matching predicate."""
        results = []
        if predicate(self):
            results.append(self)
        for child in self._children:
            if predicate(child):
                results.append(child)
            if isinstance(child, Composite):
                results.extend(child.find_all(predicate))
        return results


class TreeBuilder:
    """Build component trees from config."""

    @staticmethod
    def build(config: Dict[str, Any]) -> Component:
        """Build tree from config."""
        component_type = config.get("type", "leaf")

        if component_type == "leaf":
            action_fn = config.get("action")
            return Leaf(
                leaf_id=config.get("id", str(uuid.uuid4())),
                name=config.get("name", ""),
                action_fn=action_fn if callable(action_fn) else None,
            )
        elif component_type == "composite":
            composite = Composite(
                composite_id=config.get("id", str(uuid.uuid4())),
                name=config.get("name", ""),
                strategy=config.get("strategy", "sequence"),
            )
            for child_config in config.get("children", []):
                child = TreeBuilder.build(child_config)
                composite.add(child)
            return composite
        else:
            return Leaf(leaf_id=str(uuid.uuid4()))

    @staticmethod
    def build_from_list(items: List[Dict], strategy: str = "sequence") -> Composite:
        """Build composite from list of configs."""
        composite = Composite(composite_id=str(uuid.uuid4()), strategy=strategy)
        for item in items:
            component = TreeBuilder.build(item)
            composite.add(component)
        return composite


class CompositeAction(BaseAction):
    """Composite pattern action."""
    action_type = "composite"
    display_name = "组合模式"
    description = "树形结构组件"

    def __init__(self):
        super().__init__()
        self._components: Dict[str, Component] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "execute")

            if operation == "create_leaf":
                return self._create_leaf(params)
            elif operation == "create_composite":
                return self._create_composite(params)
            elif operation == "add":
                return self._add_component(params)
            elif operation == "remove":
                return self._remove_component(params)
            elif operation == "execute":
                return self._execute_component(params)
            elif operation == "execute_tree":
                return self._execute_tree(params)
            elif operation == "find":
                return self._find_component(params)
            elif operation == "build":
                return self._build_tree(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Composite error: {str(e)}")

    def _create_leaf(self, params: Dict[str, Any]) -> ActionResult:
        """Create a leaf component."""
        leaf_id = params.get("leaf_id", str(uuid.uuid4()))
        name = params.get("name", leaf_id)
        action_fn = params.get("action_fn")

        leaf = Leaf(leaf_id=leaf_id, name=name, action_fn=action_fn)
        self._components[leaf_id] = leaf

        return ActionResult(success=True, message=f"Leaf created: {leaf_id}", data={"id": leaf_id})

    def _create_composite(self, params: Dict[str, Any]) -> ActionResult:
        """Create a composite component."""
        composite_id = params.get("composite_id", str(uuid.uuid4()))
        name = params.get("name", composite_id)
        strategy = params.get("strategy", "sequence")

        composite = Composite(composite_id=composite_id, name=name, strategy=strategy)
        self._components[composite_id] = composite

        return ActionResult(success=True, message=f"Composite created: {composite_id}", data={"id": composite_id})

    def _add_component(self, params: Dict[str, Any]) -> ActionResult:
        """Add component to composite."""
        parent_id = params.get("parent_id")
        child_id = params.get("child_id")

        if not parent_id or not child_id:
            return ActionResult(success=False, message="parent_id and child_id are required")

        parent = self._components.get(parent_id)
        child = self._components.get(child_id)

        if not parent:
            return ActionResult(success=False, message=f"Parent not found: {parent_id}")
        if not child:
            return ActionResult(success=False, message=f"Child not found: {child_id}")

        if isinstance(parent, Composite):
            parent.add(child)
            return ActionResult(success=True, message=f"Added {child_id} to {parent_id}")
        else:
            return ActionResult(success=False, message="Parent is not a composite")

    def _remove_component(self, params: Dict[str, Any]) -> ActionResult:
        """Remove component from composite."""
        parent_id = params.get("parent_id")
        child_id = params.get("child_id")

        if not parent_id or not child_id:
            return ActionResult(success=False, message="parent_id and child_id are required")

        parent = self._components.get(parent_id)

        if not parent:
            return ActionResult(success=False, message=f"Parent not found: {parent_id}")

        if isinstance(parent, Composite):
            success = parent.remove(child_id)
            return ActionResult(success=success, message="Component removed" if success else "Child not found")
        else:
            return ActionResult(success=False, message="Parent is not a composite")

    def _execute_component(self, params: Dict[str, Any]) -> ActionResult:
        """Execute a component."""
        component_id = params.get("component_id")
        ctx = params.get("context")

        if not component_id:
            return ActionResult(success=False, message="component_id is required")

        component = self._components.get(component_id)
        if not component:
            return ActionResult(success=False, message=f"Component not found: {component_id}")

        try:
            result = component.execute(ctx)
            return ActionResult(success=True, message="Component executed", data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=f"Execution failed: {e}")

    def _execute_tree(self, params: Dict[str, Any]) -> ActionResult:
        """Execute entire component tree."""
        root_id = params.get("root_id")
        ctx = params.get("context")

        if not root_id:
            return ActionResult(success=False, message="root_id is required")

        root = self._components.get(root_id)
        if not root:
            return ActionResult(success=False, message=f"Root not found: {root_id}")

        try:
            results = root.execute(ctx)
            return ActionResult(success=True, message="Tree executed", data={"results": results})
        except Exception as e:
            return ActionResult(success=False, message=f"Tree execution failed: {e}")

    def _find_component(self, params: Dict[str, Any]) -> ActionResult:
        """Find a component."""
        component_id = params.get("component_id")

        if not component_id:
            return ActionResult(success=False, message="component_id is required")

        for comp in self._components.values():
            if comp.id == component_id:
                return ActionResult(success=True, message="Component found", data={"id": comp.id, "type": type(comp).__name__})

        return ActionResult(success=False, message="Component not found")

    def _build_tree(self, params: Dict[str, Any]) -> ActionResult:
        """Build tree from config."""
        config = params.get("config")

        if not config:
            return ActionResult(success=False, message="config is required")

        try:
            root = TreeBuilder.build(config)
            self._components[root.id] = root
            return ActionResult(success=True, message="Tree built", data={"root_id": root.id})
        except Exception as e:
            return ActionResult(success=False, message=f"Build failed: {e}")
