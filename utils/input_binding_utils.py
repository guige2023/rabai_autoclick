"""
Input binding utilities.

Bind input actions to callback functions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional, Any


@dataclass
class InputBinding:
    """A binding between input and callback."""
    binding_id: str
    input_type: str
    callback: Callable
    priority: int = 0
    enabled: bool = True
    metadata: dict = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class InputBindingManager:
    """Manage input bindings."""
    
    def __init__(self):
        self._bindings: dict[str, list[InputBinding]] = {}
        self._global_callbacks: list[Callable] = []
    
    def bind(
        self,
        input_type: str,
        callback: Callable,
        binding_id: Optional[str] = None,
        priority: int = 0
    ) -> InputBinding:
        """Bind a callback to an input type."""
        bid = binding_id or f"{input_type}_{len(self._bindings.get(input_type, []))}"
        
        binding = InputBinding(
            binding_id=bid,
            input_type=input_type,
            callback=callback,
            priority=priority
        )
        
        if input_type not in self._bindings:
            self._bindings[input_type] = []
        
        self._bindings[input_type].append(binding)
        self._bindings[input_type].sort(key=lambda b: -b.priority)
        
        return binding
    
    def unbind(self, binding_id: str) -> bool:
        """Unbind a specific binding."""
        for bindings in self._bindings.values():
            for i, binding in enumerate(bindings):
                if binding.binding_id == binding_id:
                    bindings.pop(i)
                    return True
        return False
    
    def unbind_all(self, input_type: str) -> int:
        """Unbind all bindings for an input type."""
        if input_type in self._bindings:
            count = len(self._bindings[input_type])
            self._bindings[input_type].clear()
            return count
        return 0
    
    def trigger(self, input_type: str, event_data: Optional[dict] = None) -> list[Any]:
        """Trigger all bindings for an input type."""
        results = []
        bindings = self._bindings.get(input_type, [])
        
        for binding in bindings:
            if binding.enabled:
                result = binding.callback(event_data or {})
                results.append(result)
        
        for callback in self._global_callbacks:
            callback(input_type, event_data or {})
        
        return results
    
    def get_binding(self, binding_id: str) -> Optional[InputBinding]:
        """Get a binding by ID."""
        for bindings in self._bindings.values():
            for binding in bindings:
                if binding.binding_id == binding_id:
                    return binding
        return None
    
    def enable(self, binding_id: str) -> bool:
        """Enable a binding."""
        binding = self.get_binding(binding_id)
        if binding:
            binding.enabled = True
            return True
        return False
    
    def disable(self, binding_id: str) -> bool:
        """Disable a binding."""
        binding = self.get_binding(binding_id)
        if binding:
            binding.enabled = False
            return True
        return False
    
    def on_any_input(self, callback: Callable) -> None:
        """Register global callback for any input."""
        self._global_callbacks.append(callback)
