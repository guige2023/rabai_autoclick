"""Action coalescing buffer for batching rapid input events."""
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from collections import deque
import time
import threading
import logging

logger = logging.getLogger(__name__)


@dataclass
class CoalescingRule:
    """Rule defining how actions of the same type should be coalesced."""
    action_type: str
    max_size: int = 100
    flush_interval: float = 0.1
    merge_fn: Optional[Callable[[List[Any]], Any]] = None


@dataclass 
class BufferedAction:
    """An action held in the coalescing buffer."""
    action_type: str
    data: Any
    timestamp: float = field(default_factory=time.time)
    priority: int = 0


class ActionCoalescingBuffer:
    """Buffers and coalesces rapid input actions to reduce overhead.
    
    Combines multiple similar actions occurring in quick succession into
    a single batched action, with configurable rules per action type.
    
    Example:
        buffer = ActionCoalescingBuffer()
        buffer.add_rule(CoalescingRule("click", max_size=10, flush_interval=0.05))
        buffer.add_rule(CoalescingRule("scroll", max_size=50, flush_interval=0.02))
        
        # Add actions - they'll be coalesced automatically
        buffer.add_action("click", {"x": 100, "y": 200})
        buffer.add_action("click", {"x": 101, "y": 201})
        
        # Get coalesced actions
        actions = buffer.flush("click")
    """

    def __init__(self) -> None:
        """Initialize the coalescing buffer."""
        self._buffers: Dict[str, deque] = {}
        self._rules: Dict[str, CoalescingRule] = {}
        self._lock = threading.RLock()
        self._timers: Dict[str, threading.Timer] = {}
        self._handlers: Dict[str, Callable[[List[Any]], None]] = {}

    def add_rule(self, rule: CoalescingRule) -> None:
        """Add or update a coalescing rule for an action type.
        
        Args:
            rule: Coalescing configuration for this action type.
        """
        with self._lock:
            self._rules[rule.action_type] = rule
            if rule.action_type not in self._buffers:
                self._buffers[rule.action_type] = deque()
            logger.debug("Added coalescing rule for %s: max_size=%d, flush_interval=%.3f",
                        rule.action_type, rule.max_size, rule.flush_interval)

    def set_handler(self, action_type: str, handler: Callable[[List[Any]], None]) -> None:
        """Set a handler to be called when actions are flushed.
        
        Args:
            action_type: Action type this handler processes.
            handler: Callback receiving list of coalesced action data.
        """
        with self._lock:
            self._handlers[action_type] = handler

    def add_action(self, action_type: str, data: Any, priority: int = 0) -> int:
        """Add an action to the buffer.
        
        Args:
            action_type: Type identifier for the action.
            data: Action payload data.
            priority: Higher priority actions flushed first.
            
        Returns:
            Number of actions currently buffered for this type.
        """
        with self._lock:
            if action_type not in self._buffers:
                self._buffers[action_type] = deque()
            
            buffer = self._buffers[action_type]
            buffered = BufferedAction(action_type, data, time.time(), priority)
            buffer.append(buffered)
            
            # Trigger immediate flush if buffer exceeds max_size
            rule = self._rules.get(action_type)
            if rule and len(buffer) >= rule.max_size:
                self._schedule_flush(action_type)
            
            return len(buffer)

    def flush(self, action_type: str) -> List[Any]:
        """Flush and return all buffered actions of this type.
        
        Args:
            action_type: Type of actions to flush.
            
        Returns:
            List of action data payloads.
        """
        with self._lock:
            buffer = self._buffers.get(action_type)
            if not buffer:
                return []
            
            # Sort by priority (highest first) then timestamp
            items = sorted(buffer, key=lambda x: (-x.priority, x.timestamp))
            result = [item.data for item in items]
            
            buffer.clear()
            self._cancel_timer(action_type)
            
            if result:
                logger.debug("Flushed %d %s actions", len(result), action_type)
            
            return result

    def flush_all(self) -> Dict[str, List[Any]]:
        """Flush all buffers and return all coalesced actions.
        
        Returns:
            Dictionary mapping action types to their flushed data lists.
        """
        with self._lock:
            result = {}
            for action_type in list(self._buffers.keys()):
                flushed = self.flush(action_type)
                if flushed:
                    result[action_type] = flushed
            return result

    def peek(self, action_type: str, count: Optional[int] = None) -> List[Any]:
        """Peek at buffered actions without removing them.
        
        Args:
            action_type: Type of actions to peek at.
            count: Maximum number to return (None for all).
            
        Returns:
            List of action data payloads.
        """
        with self._lock:
            buffer = self._buffers.get(action_type, deque())
            items = sorted(buffer, key=lambda x: (-x.priority, x.timestamp))
            data = [item.data for item in items]
            return data[:count] if count else data

    def size(self, action_type: Optional[str] = None) -> int:
        """Get the number of buffered actions.
        
        Args:
            action_type: Specific type to check, or None for total.
            
        Returns:
            Number of buffered actions.
        """
        with self._lock:
            if action_type:
                return len(self._buffers.get(action_type, deque()))
            return sum(len(b) for b in self._buffers.values())

    def clear(self, action_type: Optional[str] = None) -> None:
        """Clear buffered actions.
        
        Args:
            action_type: Specific type to clear, or None for all.
        """
        with self._lock:
            if action_type:
                self._buffers.get(action_type, deque()).clear()
                self._cancel_timer(action_type)
            else:
                for buffer in self._buffers.values():
                    buffer.clear()
                for timer in self._timers.values():
                    timer.cancel()
                self._timers.clear()

    def _schedule_flush(self, action_type: str) -> None:
        """Schedule a timed flush for an action type.
        
        Args:
            action_type: Type of actions to schedule flush for.
        """
        self._cancel_timer(action_type)
        rule = self._rules.get(action_type)
        if not rule:
            return
        
        timer = threading.Timer(rule.flush_interval, self._timed_flush, args=[action_type])
        self._timers[action_type] = timer
        timer.start()

    def _timed_flush(self, action_type: str) -> None:
        """Callback for timed flush.
        
        Args:
            action_type: Type of actions that triggered flush.
        """
        with self._lock:
            actions = self.flush(action_type)
            if actions and action_type in self._handlers:
                try:
                    self._handlers[action_type](actions)
                except Exception as e:
                    logger.error("Error in flush handler for %s: %s", action_type, e)

    def _cancel_timer(self, action_type: str) -> None:
        """Cancel a pending flush timer.
        
        Args:
            action_type: Type whose timer to cancel.
        """
        timer = self._timers.pop(action_type, None)
        if timer:
            timer.cancel()
