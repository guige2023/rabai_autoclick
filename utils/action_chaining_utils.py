"""
Action chaining utilities for building complex automation sequences.

Provides composable action chains with error handling, retry logic,
and state management for GUI automation workflows.
"""

from __future__ import annotations

import time
from typing import Callable, Optional, List, Any, Dict, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps


T = TypeVar('T')


class ActionStatus(Enum):
    """Action execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class ActionResult:
    """Result of action execution."""
    status: ActionStatus
    action_name: str
    duration: float
    error: Optional[str] = None
    retry_count: int = 0
    data: Optional[Any] = None


@dataclass
class ChainContext:
    """Shared context for action chain execution."""
    vars: Dict[str, Any] = field(default_factory=dict)
    results: List[ActionResult] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class Action(Generic[T]):
    """Base action class."""
    
    def __init__(self, name: str, max_retries: int = 0, retry_delay: float = 1.0):
        """
        Initialize action.
        
        Args:
            name: Action name for logging.
            max_retries: Max retry attempts on failure.
            retry_delay: Delay between retries.
        """
        self.name = name
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    def execute(self, ctx: ChainContext) -> ActionResult:
        """Execute the action."""
        start = time.time()
        for attempt in range(self.max_retries + 1):
            try:
                result = self._execute_impl(ctx)
                return ActionResult(
                    status=ActionStatus.SUCCESS,
                    action_name=self.name,
                    duration=time.time() - start,
                    data=result,
                    retry_count=attempt
                )
            except Exception as e:
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)
                    continue
                return ActionResult(
                    status=ActionStatus.FAILED,
                    action_name=self.name,
                    duration=time.time() - start,
                    error=str(e),
                    retry_count=attempt + 1
                )
        return ActionResult(
            status=ActionStatus.FAILED,
            action_name=self.name,
            duration=time.time() - start,
            retry_count=self.max_retries + 1
        )
    
    def _execute_impl(self, ctx: ChainContext) -> T:
        """Override in subclasses."""
        raise NotImplementedError


class ClickAction(Action):
    """Click at coordinates action."""
    
    def __init__(self, x: int, y: int, button: str = "left", **kwargs):
        super().__init__(f"click({x},{y})", **kwargs)
        self.x = x
        self.y = y
        self.button = button
    
    def _execute_impl(self, ctx: ChainContext) -> bool:
        import Quartz
        from Quartz import CGEvent, CGEventCreateMouseEvent
        
        button = Quartz.kCGEventLeftMouseDown if self.button == "left" else Quartz.kCGEventRightMouseDown
        click_type = Quartz.kCGEventLeftMouseDown if "down" in self.name else Quartz.kCGEventLeftMouseUp
        
        event = CGEventCreateMouseEvent(None, button, (self.x, self.y), Quartz.kCGMouseButtonLeft)
        CGEvent.post(Quartz.kCGHIDEventTap, event)
        return True


class TypeAction(Action):
    """Type text action."""
    
    def __init__(self, text: str, **kwargs):
        super().__init__(f"type({text[:10]}...)", **kwargs)
        self.text = text
    
    def _execute_impl(self, ctx: ChainContext) -> bool:
        import Quartz
        from Quartz import CGEventCreateKeyboardEvent
        
        for char in self.text:
            key_code = ord(char)
            down = CGEventCreateKeyboardEvent(None, key_code, True)
            up = CGEventCreateKeyboardEvent(None, key_code, False)
            CGEvent.post(Quartz.kCGHIDEventTap, down)
            CGEvent.post(Quartz.kCGHIDEventTap, up)
        return True


class WaitAction(Action):
    """Wait action."""
    
    def __init__(self, seconds: float, **kwargs):
        super().__init__(f"wait({seconds}s)", **kwargs)
        self.seconds = seconds
    
    def _execute_impl(self, ctx: ChainContext) -> bool:
        time.sleep(self.seconds)
        return True


class ActionChain:
    """Chain of actions with error handling."""
    
    def __init__(self, name: str = "unnamed"):
        self.name = name
        self.actions: List[Action] = []
        self.error_handler: Optional[Callable] = None
        self._continue_on_error = False
    
    def add(self, action: Action) -> "ActionChain":
        """Add action to chain."""
        self.actions.append(action)
        return self
    
    def then(self, action: Action) -> "ActionChain":
        """Add action (alias for add)."""
        return self.add(action)
    
    def on_error(self, handler: Callable[[ActionResult, ChainContext], None]) -> "ActionChain":
        """Set error handler."""
        self.error_handler = handler
        return self
    
    def continue_on_error(self, value: bool = True) -> "ActionChain":
        """Set continue on error flag."""
        self._continue_on_error = value
        return self
    
    def execute(self, ctx: Optional[ChainContext] = None) -> ChainContext:
        """
        Execute the action chain.
        
        Args:
            ctx: Optional pre-existing context.
            
        Returns:
            ChainContext with all results.
        """
        if ctx is None:
            ctx = ChainContext()
        
        for action in self.actions:
            result = action.execute(ctx)
            ctx.results.append(result)
            
            if result.status == ActionStatus.FAILED:
                if self.error_handler:
                    self.error_handler(result, ctx)
                
                if not self._continue_on_error:
                    break
        
        return ctx
    
    def wait(self, seconds: float) -> "ActionChain":
        """Add wait action."""
        return self.add(WaitAction(seconds))
    
    def click(self, x: int, y: int, **kwargs) -> "ActionChain":
        """Add click action."""
        return self.add(ClickAction(x, y, **kwargs))
    
    def type_text(self, text: str, **kwargs) -> "ActionChain":
        """Add type text action."""
        return self.add(TypeAction(text, **kwargs))
    
    def when(self, condition: Callable[[ChainContext], bool],
            then_chain: "ActionChain") -> "ActionChain":
        """Add conditional execution."""
        def cond_action(ctx: ChainContext) -> Any:
            if condition(ctx):
                then_chain.execute(ctx)
            return None
        return self


class ChainBuilder:
    """Builder for action chains with fluent API."""
    
    def __init__(self, name: str = "builder"):
        self.chain = ActionChain(name)
    
    @classmethod
    def create(cls, name: str = "unnamed") -> "ChainBuilder":
        return cls(name)
    
    def click(self, x: int, y: int, button: str = "left") -> "ChainBuilder":
        self.chain.add(ClickAction(x, y, button))
        return self
    
    def double_click(self, x: int, y: int) -> "ChainBuilder":
        self.chain.add(ClickAction(x, y, "left"))
        self.chain.add(WaitAction(0.05))
        self.chain.add(ClickAction(x, y, "left"))
        return self
    
    def right_click(self, x: int, y: int) -> "ChainBuilder":
        self.chain.add(ClickAction(x, y, "right"))
        return self
    
    def type(self, text: str) -> "ChainBuilder":
        self.chain.add(TypeAction(text))
        return self
    
    def wait(self, seconds: float) -> "ChainBuilder":
        self.chain.add(WaitAction(seconds))
        return self
    
    def build(self) -> ActionChain:
        return self.chain
