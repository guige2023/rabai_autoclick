"""Scroll behavior configuration and utilities."""

from typing import Tuple, Optional, Callable, List
from enum import Enum
import math


class ScrollDirection(Enum):
    """Scroll direction."""
    UP = "up"
    DOWN = "down"
    LEFT = "left"
    RIGHT = "right"


class ScrollBehavior:
    """Configure scroll behavior for different UI scenarios."""

    def __init__(
        self,
        direction: ScrollDirection = ScrollDirection.DOWN,
        step_size: int = 3,
        scroll_amount: int = 100,
        acceleration: float = 1.0,
        momentum: float = 0.0,
        reverse: bool = False
    ):
        """Initialize scroll behavior.
        
        Args:
            direction: Scroll direction.
            step_size: Number of scroll steps per action.
            scroll_amount: Pixels per scroll step.
            acceleration: Acceleration multiplier.
            momentum: Momentum decay factor (0-1).
            reverse: If True, reverse the scroll direction.
        """
        self.direction = direction
        self.step_size = step_size
        self.scroll_amount = scroll_amount
        self.acceleration = acceleration
        self.momentum = momentum
        self.reverse = reverse
        self._velocity = 0.0

    def get_scroll_delta(self, delta_multiplier: float = 1.0) -> Tuple[int, int]:
        """Get scroll delta as (dx, dy).
        
        Args:
            delta_multiplier: Additional multiplier for delta.
        
        Returns:
            Tuple of (dx, dy) scroll amounts.
        """
        amount = self.scroll_amount * self.step_size * self.acceleration * delta_multiplier
        if self.reverse:
            amount = -amount
        if self.direction == ScrollDirection.UP:
            return (0, int(-amount))
        if self.direction == ScrollDirection.DOWN:
            return (0, int(amount))
        if self.direction == ScrollDirection.LEFT:
            return (int(-amount), 0)
        return (int(amount), 0)

    def apply_momentum(self, delta: float) -> float:
        """Apply momentum to scroll delta.
        
        Args:
            delta: Base scroll delta.
        
        Returns:
            Adjusted delta with momentum.
        """
        self._velocity = delta * (1.0 - self.momentum) + self._velocity * self.momentum
        return self._velocity

    def reset_momentum(self) -> None:
        """Reset momentum velocity."""
        self._velocity = 0.0

    def with_direction(self, direction: ScrollDirection) -> "ScrollBehavior":
        """Create new instance with different direction."""
        b = ScrollBehavior(
            direction=direction,
            step_size=self.step_size,
            scroll_amount=self.scroll_amount,
            acceleration=self.acceleration,
            momentum=self.momentum,
            reverse=self.reverse
        )
        return b


def scroll_to_element(
    element_bounds: Tuple[int, int, int, int],
    viewport_size: Tuple[int, int],
    scroll_behavior: Optional[ScrollBehavior] = None
) -> Tuple[int, int]:
    """Calculate scroll needed to bring element into view.
    
    Args:
        element_bounds: (x, y, width, height) of element.
        viewport_size: (width, height) of viewport.
        scroll_behavior: Optional scroll behavior config.
    
    Returns:
        Tuple of (scroll_x, scroll_y) needed.
    """
    elem_x, elem_y, elem_w, elem_h = element_bounds
    vp_w, vp_h = viewport_size
    target_x = elem_x - vp_w // 2 + elem_w // 2
    target_y = elem_y - vp_h // 2 + elem_h // 2
    return (target_x, target_y)


def smooth_scroll_steps(
    current_scroll: Tuple[int, int],
    target_scroll: Tuple[int, int],
    num_steps: int = 10
) -> List[Tuple[int, int]]:
    """Generate smooth scroll steps from current to target.
    
    Args:
        current_scroll: Current scroll position (x, y).
        target_scroll: Target scroll position (x, y).
        num_steps: Number of intermediate steps.
    
    Returns:
        List of (x, y) scroll positions.
    """
    cx, cy = current_scroll
    tx, ty = target_scroll
    steps = []
    for i in range(1, num_steps + 1):
        ratio = i / num_steps
        eased_ratio = _ease_in_out(ratio)
        x = int(cx + (tx - cx) * eased_ratio)
        y = int(cy + (ty - cy) * eased_ratio)
        steps.append((x, y))
    return steps


def _ease_in_out(t: float) -> float:
    """Ease in-out function."""
    return t * t * (3 - 2 * t) if t < 0.5 else 1 - ((t - 1) * 2) ** 2 / 2


class ScrollSequence:
    """Sequence of scroll actions for complex scrolling."""

    def __init__(self):
        """Initialize scroll sequence."""
        self.steps: List[Tuple[int, int, int]] = []

    def add_step(
        self,
        dx: int,
        dy: int,
        duration_ms: int = 100
    ) -> "ScrollSequence":
        """Add scroll step.
        
        Args:
            dx: Horizontal scroll amount.
            dy: Vertical scroll amount.
            duration_ms: Duration for this step.
        
        Returns:
            Self for chaining.
        """
        self.steps.append((dx, dy, duration_ms))
        return self

    def scroll_page_down(self, pages: int = 1) -> "ScrollSequence":
        """Add page-down scroll."""
        for _ in range(pages):
            self.add_step(0, -600, 150)
        return self

    def scroll_to_top(self) -> "ScrollSequence":
        """Add scroll to top."""
        self.add_step(0, 10000, 500)
        return self

    def execute(self, executor: Callable[[int, int], None]) -> None:
        """Execute scroll sequence.
        
        Args:
            executor: Function that takes (dx, dy) and performs scroll.
        """
        import time
        for dx, dy, duration in self.steps:
            executor(dx, dy)
            time.sleep(duration / 1000.0)
