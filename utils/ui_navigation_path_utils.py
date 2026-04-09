"""
UI Navigation Path Utilities - Path planning and navigation for UI automation.

This module provides utilities for planning navigation paths through
UI elements, finding optimal routes, and executing sequential
navigation through complex UI structures.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


@dataclass
class NavigationStep:
    """Represents a single step in a navigation path.
    
    Attributes:
        id: Unique identifier for this step.
        element_id: Target element ID.
        action: Action to perform (click, type, scroll, etc.).
        element_name: Optional human-readable name.
        order: Step order in the path.
        estimated_duration: Estimated time in seconds.
        metadata: Additional step data.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    element_id: str = ""
    action: str = "click"
    element_name: Optional[str] = None
    order: int = 0
    estimated_duration: float = 0.5
    metadata: dict = field(default_factory=dict)


@dataclass
class NavigationPath:
    """Represents a complete navigation path.
    
    Attributes:
        id: Unique identifier for this path.
        name: Human-readable path name.
        steps: Ordered list of navigation steps.
        start_element: Starting element ID.
        end_element: Ending element ID.
        metadata: Additional path data.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: Optional[str] = None
    steps: list[NavigationStep] = field(default_factory=list)
    start_element: Optional[str] = None
    end_element: Optional[str] = None
    metadata: dict = field(default_factory=dict)
    
    @property
    def total_duration(self) -> float:
        """Get total estimated path duration."""
        return sum(step.estimated_duration for step in self.steps)
    
    @property
    def step_count(self) -> int:
        """Get number of steps in path."""
        return len(self.steps)
    
    def add_step(
        self,
        element_id: str,
        action: str = "click",
        element_name: Optional[str] = None
    ) -> NavigationStep:
        """Add a step to the path.
        
        Args:
            element_id: Target element ID.
            action: Action to perform.
            element_name: Optional name.
            
        Returns:
            The created NavigationStep.
        """
        step = NavigationStep(
            element_id=element_id,
            action=action,
            element_name=element_name,
            order=len(self.steps)
        )
        self.steps.append(step)
        
        if not self.start_element:
            self.start_element = element_id
        self.end_element = element_id
        
        return step
    
    def get_step(self, index: int) -> Optional[NavigationStep]:
        """Get step at index.
        
        Args:
            index: Step index.
            
        Returns:
            NavigationStep if exists.
        """
        if 0 <= index < len(self.steps):
            return self.steps[index]
        return None
    
    def reverse(self) -> NavigationPath:
        """Create a reversed version of this path.
        
        Returns:
            New NavigationPath in reverse order.
        """
        reversed_path = NavigationPath(
            name=f"{self.name} (reversed)" if self.name else None,
            start_element=self.end_element,
            end_element=self.start_element
        )
        
        for step in reversed(self.steps):
            reversed_path.steps.append(NavigationStep(
                element_id=step.element_id,
                action=step.action,
                element_name=step.element_name,
                order=len(reversed_path.steps)
            ))
        
        return reversed_path


class NavigationPathBuilder:
    """Builder for creating navigation paths.
    
    Provides a fluent API for constructing navigation paths
    through UI element sequences.
    
    Example:
        >>> builder = NavigationPathBuilder()
        >>> path = (builder
        ...     .named("login_flow")
        ...     .click("username_field")
        ...     .type("user@example.com")
        ...     .click("password_field")
        ...     .type("password123")
        ...     .click("submit_btn")
        ...     .build())
    """
    
    def __init__(self) -> None:
        """Initialize the path builder."""
        self._path = NavigationPath()
    
    def named(self, name: str) -> NavigationPathBuilder:
        """Set the path name.
        
        Args:
            name: Path name.
            
        Returns:
            Self for chaining.
        """
        self._path.name = name
        return self
    
    def click(
        self,
        element_id: str,
        element_name: Optional[str] = None
    ) -> NavigationPathBuilder:
        """Add a click step.
        
        Args:
            element_id: Target element ID.
            element_name: Optional element name.
            
        Returns:
            Self for chaining.
        """
        self._path.add_step(element_id, "click", element_name)
        return self
    
    def type_text(
        self,
        element_id: str,
        text: str,
        element_name: Optional[str] = None
    ) -> NavigationPathBuilder:
        """Add a type text step.
        
        Args:
            element_id: Target element ID.
            text: Text to type.
            element_name: Optional element name.
            
        Returns:
            Self for chaining.
        """
        step = self._path.add_step(element_id, "type", element_name)
        step.metadata["text"] = text
        step.estimated_duration = max(0.1, len(text) * 0.05)
        return self
    
    def scroll(
        self,
        direction: str = "down",
        amount: int = 1
    ) -> NavigationPathBuilder:
        """Add a scroll step.
        
        Args:
            direction: Scroll direction.
            amount: Scroll amount.
            
        Returns:
            Self for chaining.
        """
        step = NavigationStep(
            element_id="scroll",
            action="scroll",
            order=len(self._path.steps)
        )
        step.metadata["direction"] = direction
        step.metadata["amount"] = amount
        step.estimated_duration = 0.3
        self._path.steps.append(step)
        return self
    
    def wait(
        self,
        duration: float = 1.0
    ) -> NavigationPathBuilder:
        """Add a wait step.
        
        Args:
            duration: Wait duration in seconds.
            
        Returns:
            Self for chaining.
        """
        step = NavigationStep(
            element_id="wait",
            action="wait",
            order=len(self._path.steps)
        )
        step.estimated_duration = duration
        self._path.steps.append(step)
        return self
    
    def custom(
        self,
        element_id: str,
        action: str,
        metadata: Optional[dict] = None
    ) -> NavigationPathBuilder:
        """Add a custom action step.
        
        Args:
            element_id: Target element ID.
            action: Action name.
            metadata: Additional metadata.
            
        Returns:
            Self for chaining.
        """
        step = NavigationStep(
            element_id=element_id,
            action=action,
            order=len(self._path.steps)
        )
        if metadata:
            step.metadata.update(metadata)
        self._path.steps.append(step)
        return self
    
    def build(self) -> NavigationPath:
        """Build and return the navigation path.
        
        Returns:
            The constructed NavigationPath.
        """
        return self._path
    
    def reset(self) -> NavigationPathBuilder:
        """Reset the builder for a new path.
        
        Returns:
            Self for chaining.
        """
        self._path = NavigationPath()
        return self


class PathExecutor:
    """Executes navigation paths with monitoring.
    
    Provides methods for executing navigation paths and
    tracking execution progress and results.
    
    Example:
        >>> executor = PathExecutor()
        >>> result = executor.execute(path, element_provider)
    """
    
    def __init__(self) -> None:
        """Initialize the path executor."""
        self._current_step: int = 0
        self._completed_steps: list[int] = []
        self._failed_steps: list[int] = []
    
    def execute(
        self,
        path: NavigationPath,
        element_provider: Callable[[str], Optional[dict]],
        step_executor: Callable[[NavigationStep, dict], bool]
    ) -> ExecutionResult:
        """Execute a navigation path.
        
        Args:
            path: Path to execute.
            element_provider: Function to get element by ID.
            step_executor: Function to execute a step.
            
        Returns:
            ExecutionResult with outcome details.
        """
        self._current_step = 0
        self._completed_steps = []
        self._failed_steps = []
        
        for step in path.steps:
            element = element_provider(step.element_id)
            
            if element is None:
                self._failed_steps.append(step.order)
                return ExecutionResult(
                    success=False,
                    completed_steps=self._completed_steps,
                    failed_step=step.order,
                    error=f"Element not found: {step.element_id}"
                )
            
            success = step_executor(step, element)
            
            if success:
                self._completed_steps.append(step.order)
            else:
                self._failed_steps.append(step.order)
                return ExecutionResult(
                    success=False,
                    completed_steps=self._completed_steps,
                    failed_step=step.order
                )
            
            self._current_step = step.order + 1
        
        return ExecutionResult(
            success=True,
            completed_steps=self._completed_steps,
            failed_step=None
        )
    
    def get_progress(self, path: NavigationPath) -> float:
        """Get execution progress as percentage.
        
        Args:
            path: Path being executed.
            
        Returns:
            Progress from 0.0 to 1.0.
        """
        if path.step_count == 0:
            return 1.0
        return len(self._completed_steps) / path.step_count


@dataclass
class ExecutionResult:
    """Result of a path execution attempt.
    
    Attributes:
        success: Whether execution succeeded.
        completed_steps: List of completed step indices.
        failed_step: Index of failed step, if any.
        error: Error message if failed.
    """
    success: bool = False
    completed_steps: list[int] = field(default_factory=list)
    failed_step: Optional[int] = None
    error: Optional[str] = None


class PathFinder:
    """Finds optimal paths between UI elements.
    
    Provides methods for finding navigation paths through
    UI structures using various strategies.
    
    Example:
        >>> finder = PathFinder()
        >>> path = finder.find_path(start_id, end_id, elements)
    """
    
    def __init__(self) -> None:
        """Initialize the path finder."""
        self._adjacency: dict[str, list[str]] = {}
    
    def set_adjacency(self, adjacency: dict[str, list[str]]) -> None:
        """Set adjacency map for elements.
        
        Args:
            adjacency: Map of element ID to list of reachable IDs.
        """
        self._adjacency = adjacency
    
    def add_connection(self, from_id: str, to_id: str) -> None:
        """Add a connection between elements.
        
        Args:
            from_id: Source element ID.
            to_id: Target element ID.
        """
        if from_id not in self._adjacency:
            self._adjacency[from_id] = []
        if to_id not in self._adjacency[from_id]:
            self._adjacency[from_id].append(to_id)
    
    def find_path(
        self,
        start_id: str,
        end_id: str
    ) -> Optional[NavigationPath]:
        """Find a path using BFS.
        
        Args:
            start_id: Starting element ID.
            end_id: Target element ID.
            
        Returns:
            NavigationPath if found, None otherwise.
        """
        if start_id == end_id:
            path = NavigationPath(start_element=start_id, end_element=end_id)
            path.add_step(start_id)
            return path
        
        visited: set[str] = {start_id}
        queue: list[tuple[str, list[str]]] = [(start_id, [start_id])]
        
        while queue:
            current, path = queue.pop(0)
            
            for neighbor in self._adjacency.get(current, []):
                if neighbor == end_id:
                    result_path = NavigationPath(
                        start_element=start_id,
                        end_element=end_id
                    )
                    for element_id in path + [neighbor]:
                        result_path.add_step(element_id)
                    return result_path
                
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))
        
        return None
    
    def find_shortest_path(
        self,
        start_id: str,
        end_id: str,
        weights: Optional[dict[tuple[str, str], float]] = None
    ) -> Optional[NavigationPath]:
        """Find shortest weighted path.
        
        Args:
            start_id: Starting element ID.
            end_id: Target element ID.
            weights: Optional edge weights.
            
        Returns:
            NavigationPath if found.
        """
        import heapq
        
        if start_id == end_id:
            path = NavigationPath(start_element=start_id, end_element=end_id)
            path.add_step(start_id)
            return path
        
        distances: dict[str, float] = {start_id: 0}
        previous: dict[str, str] = {}
        visited: set[str] = set()
        heap: list[tuple[float, str]] = [(0, start_id)]
        
        while heap:
            dist, current = heapq.heappop(heap)
            
            if current in visited:
                continue
            visited.add(current)
            
            if current == end_id:
                break
            
            for neighbor in self._adjacency.get(current, []):
                weight = weights.get((current, neighbor), 1.0)
                new_dist = dist + weight
                
                if neighbor not in distances or new_dist < distances[neighbor]:
                    distances[neighbor] = new_dist
                    previous[neighbor] = current
                    heapq.heappush(heap, (new_dist, neighbor))
        
        if end_id not in previous:
            return None
        
        path_ids = []
        current = end_id
        while current in previous:
            path_ids.append(current)
            current = previous[current]
        path_ids.append(start_id)
        
        result_path = NavigationPath(
            start_element=start_id,
            end_element=end_id
        )
        for element_id in reversed(path_ids):
            result_path.add_step(element_id)
        
        return result_path
