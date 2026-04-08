"""UI element hierarchy traversal utilities.

Provides tree-walking, filtering, and path-finding operations
for UI element hierarchies (accessibility trees, DOM, widget trees).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Generator,
    Iterator,
    Optional,
    Sequence,
)


class TraversalOrder(Enum):
    """Depth-first traversal order variants."""
    PRE_ORDER = auto()   # Parent before children
    POST_ORDER = auto()  # Children before parent
    LEVEL_ORDER = auto()  # Breadth-first


@dataclass
class UIElement:
    """A node in a UI element hierarchy.

    Attributes:
        element_id: Unique identifier for this element.
        role: The element's role/kind (e.g., "button", "window").
        name: Accessible name or label.
        value: Current value (for inputs, sliders, etc.).
        description: Additional description text.
        children: Child element references.
        parent: Reference to parent element.
        depth: Depth level in the tree (0 = root).
        bounds: Bounding box as (x, y, width, height).
        states: Set of current states (focused, selected, etc.).
        attributes: Additional element attributes.
        is_enabled: Whether the element is interactive.
        is_visible: Whether the element is visible.
    """
    role: str
    name: str = ""
    element_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    value: Any = None
    description: str = ""
    children: list[UIElement] = field(default_factory=list)
    parent: Optional[UIElement] = None
    depth: int = 0
    bounds: tuple[float, float, float, float] = (0, 0, 0, 0)
    states: set[str] = field(default_factory=set)
    attributes: dict[str, Any] = field(default_factory=dict)
    is_enabled: bool = True
    is_visible: bool = True

    @property
    def x(self) -> float:
        return self.bounds[0]

    @property
    def y(self) -> float:
        return self.bounds[1]

    @property
    def width(self) -> float:
        return self.bounds[2]

    @property
    def height(self) -> float:
        return self.bounds[3]

    def has_state(self, state: str) -> bool:
        """Check if element has a given state."""
        return state in self.states

    def add_state(self, state: str) -> None:
        """Add a state to this element."""
        self.states.add(state)

    def is_interactive(self) -> bool:
        """Return True if this element appears interactive."""
        interactive_roles = {
            "button", "link", "menuitem", "checkbox", "radiobutton",
            "textbox", "combobox", "slider", "tab", "switch",
            "toggle", "spinbutton", "scrollbar",
        }
        return self.role.lower() in interactive_roles and self.is_enabled

    def is_focusable(self) -> bool:
        """Return True if this element can receive focus."""
        return self.has_state("focusable") or self.is_interactive()

    def get_path(self) -> list[UIElement]:
        """Return the path from root to this element."""
        path: list[UIElement] = []
        current: Optional[UIElement] = self
        while current:
            path.append(current)
            current = current.parent
        return list(reversed(path))

    def get_ancestors(self) -> list[UIElement]:
        """Return ancestor elements (excluding self)."""
        return self.get_path()[:-1]

    def get_depth(self) -> int:
        """Return the depth of this element in the tree."""
        return len(self.get_ancestors())


class UITreeWalker:
    """Walk and query UI element hierarchies.

    Supports depth-first (pre/post-order) and breadth-first traversal,
    element filtering, and path finding.
    """

    def __init__(self, root: Optional[UIElement] = None) -> None:
        """Initialize walker with optional root element."""
        self._root = root

    @property
    def root(self) -> Optional[UIElement]:
        """Get the current root element."""
        return self._root

    @root.setter
    def root(self, element: UIElement) -> None:
        """Set the root element."""
        self._root = element

    def walk(
        self,
        order: TraversalOrder = TraversalOrder.PRE_ORDER,
    ) -> Generator[UIElement, None, None]:
        """Walk the tree yielding elements in the given order."""
        if not self._root:
            return
        if order == TraversalOrder.PRE_ORDER:
            yield from self._walk_preorder(self._root)
        elif order == TraversalOrder.POST_ORDER:
            yield from self._walk_postorder(self._root)
        elif order == TraversalOrder.LEVEL_ORDER:
            yield from self._walk_levelorder(self._root)

    def _walk_preorder(self, node: UIElement) -> Generator[UIElement, None, None]:
        """Pre-order: yield node, then children."""
        yield node
        for child in node.children:
            yield from self._walk_preorder(child)

    def _walk_postorder(self, node: UIElement) -> Generator[UIElement, None, None]:
        """Post-order: yield children, then node."""
        for child in node.children:
            yield from self._walk_postorder(child)
        yield node

    def _walk_levelorder(self, node: UIElement) -> Generator[UIElement, None, None]:
        """Level-order (breadth-first)."""
        queue: list[UIElement] = [node]
        while queue:
            current = queue.pop(0)
            yield current
            queue.extend(current.children)

    def filter(
        self,
        predicate: Callable[[UIElement], bool],
        order: TraversalOrder = TraversalOrder.PRE_ORDER,
    ) -> Generator[UIElement, None, None]:
        """Yield elements matching the predicate."""
        for element in self.walk(order):
            if predicate(element):
                yield element

    def find_first(
        self,
        predicate: Callable[[UIElement], bool],
        order: TraversalOrder = TraversalOrder.PRE_ORDER,
    ) -> Optional[UIElement]:
        """Return the first element matching predicate."""
        for element in self.walk(order):
            if predicate(element):
                return element
        return None

    def find_all(
        self,
        predicate: Callable[[UIElement], bool],
        order: TraversalOrder = TraversalOrder.PRE_ORDER,
    ) -> list[UIElement]:
        """Return all elements matching predicate."""
        return list(self.filter(predicate, order))

    def find_by_role(
        self,
        role: str,
        exact: bool = True,
    ) -> list[UIElement]:
        """Find all elements with a given role."""
        role_lower = role.lower()
        if exact:
            return self.find_all(
                lambda e: e.role.lower() == role_lower
            )
        return self.find_all(
            lambda e: role_lower in e.role.lower()
        )

    def find_by_name(
        self,
        name: str,
        exact: bool = True,
    ) -> list[UIElement]:
        """Find all elements with a matching name."""
        name_lower = name.lower()
        if exact:
            return self.find_all(
                lambda e: e.name.lower() == name_lower
            )
        return self.find_all(
            lambda e: name_lower in e.name.lower()
        )

    def find_interactive(self) -> list[UIElement]:
        """Find all interactive elements."""
        return self.find_all(lambda e: e.is_interactive())

    def find_focused(self) -> list[UIElement]:
        """Find all focused elements."""
        return self.find_all(lambda e: e.has_state("focused"))

    def find_enabled(self) -> list[UIElement]:
        """Find all enabled elements."""
        return self.find_all(lambda e: e.is_enabled)

    def find_visible(self) -> list[UIElement]:
        """Find all visible elements."""
        return self.find_all(lambda e: e.is_visible)

    def find_at_point(
        self,
        x: float,
        y: float,
    ) -> Optional[UIElement]:
        """Find the deepest element at a given point."""
        candidates = self.find_all(
            lambda e: e.is_visible
            and e.x <= x < e.x + e.width
            and e.y <= y < e.y + e.height
        )
        if not candidates:
            return None
        return max(candidates, key=lambda e: e.get_depth())

    def get_path_to(
        self,
        target: UIElement,
    ) -> list[UIElement]:
        """Get the traversal path from root to target element."""
        return target.get_path()

    def get_siblings(self, element: UIElement) -> list[UIElement]:
        """Get sibling elements."""
        if not element.parent:
            return []
        return [
            e for e in element.parent.children
            if e.element_id != element.element_id
        ]

    def count(self) -> int:
        """Return total element count."""
        return sum(1 for _ in self.walk())

    def count_by_role(self) -> dict[str, int]:
        """Return count of elements grouped by role."""
        counts: dict[str, int] = {}
        for element in self.walk():
            counts[element.role] = counts.get(element.role, 0) + 1
        return counts


class UIPathBuilder:
    """Build and use paths to locate elements in a UI hierarchy.

    Paths are expressed as sequences of role/name filters that
    navigate from a root to a target element.
    """

    @dataclass
    class PathStep:
        """A single step in a path specification."""
        role: Optional[str] = None
        name: Optional[str] = None
        index: int = 0
        state: Optional[str] = None

    def __init__(self) -> None:
        """Initialize path builder."""
        self._steps: list[UIPathBuilder.PathStep] = []

    def role(self, role: str, index: int = 0) -> UIPathBuilder:
        """Add a role filter step."""
        self._steps.append(UIPathBuilder.PathStep(role=role, index=index))
        return self

    def name(self, name: str, index: int = 0) -> UIPathBuilder:
        """Add a name filter step."""
        self._steps.append(UIPathBuilder.PathStep(name=name, index=index))
        return self

    def state(self, state: str, index: int = 0) -> UIPathBuilder:
        """Add a state filter step."""
        self._steps.append(UIPathBuilder.PathStep(state=state, index=index))
        return self

    def build(self) -> list[UIPathBuilder.PathStep]:
        """Return the built path."""
        return list(self._steps)

    def match(self, element: UIElement, step: PathStep) -> bool:
        """Test if an element matches a single step."""
        if step.role and element.role.lower() != step.role.lower():
            return False
        if step.name:
            name_lower = step.name.lower()
            if name_lower not in element.name.lower():
                return False
        if step.state and step.state not in element.states:
            return False
        return True

    def resolve(self, root: UIElement) -> Optional[UIElement]:
        """Resolve this path from a root element."""
        current: list[UIElement] = [root]
        for step in self._steps:
            next_current: list[UIElement] = []
            for element in current:
                next_current.extend(element.children)
            if step.state:
                next_current = [e for e in next_current if step.state in e.states]
            if step.role:
                next_current = [
                    e for e in next_current
                    if e.role.lower() == step.role.lower()
                ]
            if step.name:
                name_lower = step.name.lower()
                next_current = [
                    e for e in next_current
                    if name_lower in e.name.lower()
                ]
            current = next_current
            if not current:
                return None
        index = 0
        for step in self._steps:
            if step.index == 0:
                break
            index = step.index
        if 0 <= index < len(current):
            return current[index]
        return current[0] if current else None
