"""Decorator action module for RabAI AutoClick.

Provides decorator pattern implementation:
- Component: Abstract component interface
- ConcreteComponent: Specific component
- Decorator: Abstract decorator
- ConcreteDecorator: Specific decorators
"""

from typing import Any, Callable, Dict, List, Optional
from abc import ABC, abstractmethod
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Component(ABC):
    """Abstract component."""

    @abstractmethod
    def operation(self, data: Any) -> Any:
        """Perform operation."""
        pass


class ConcreteComponent(Component):
    """Concrete component."""

    def __init__(self, component_id: str = ""):
        self.component_id = component_id or str(uuid.uuid4())

    def operation(self, data: Any) -> Any:
        """Perform operation."""
        return {"result": data, "component": self.component_id}


class Decorator(Component):
    """Abstract decorator."""

    def __init__(self, wrapped: Component):
        self._wrapped = wrapped

    def operation(self, data: Any) -> Any:
        """Delegate to wrapped component."""
        return self._wrapped.operation(data)


class LoggingDecorator(Decorator):
    """Decorator that adds logging."""

    def __init__(self, wrapped: Component, logger: Optional[Callable] = None):
        super().__init__(wrapped)
        self._logger = logger or print

    def operation(self, data: Any) -> Any:
        """Log before and after operation."""
        self._logger(f"[LOG] Before operation: {data}")
        result = self._wrapped.operation(data)
        self._logger(f"[LOG] After operation: {result}")
        return result


class TimingDecorator(Decorator):
    """Decorator that adds timing."""

    def __init__(self, wrapped: Component):
        super().__init__(wrapped)
        self.total_time = 0.0

    def operation(self, data: Any) -> Any:
        """Time the operation."""
        start = time.time()
        result = self._wrapped.operation(data)
        duration = time.time() - start
        self.total_time += duration
        return {"result": result, "duration": duration}


class CachingDecorator(Decorator):
    """Decorator that adds caching."""

    def __init__(self, wrapped: Component, cache_size: int = 100):
        super().__init__(wrapped)
        self._cache: Dict[str, Any] = {}
        self._cache_size = cache_size
        self._hits = 0
        self._misses = 0

    def operation(self, data: Any) -> Any:
        """Cache results."""
        cache_key = str(data)
        if cache_key in self._cache:
            self._hits += 1
            return {"result": self._cache[cache_key], "cached": True}
        self._misses += 1
        result = self._wrapped.operation(data)
        if len(self._cache) >= self._cache_size:
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]
        self._cache[cache_key] = result.get("result") if isinstance(result, dict) else result
        return {"result": result, "cached": False}

    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {"hits": self._hits, "misses": self._misses, "size": len(self._cache)}


class ValidationDecorator(Decorator):
    """Decorator that adds validation."""

    def __init__(self, wrapped: Component, validator: Callable[[Any], bool]):
        super().__init__(wrapped)
        self._validator = validator
        self._failed_validations = 0

    def operation(self, data: Any) -> Any:
        """Validate before operation."""
        if not self._validator(data):
            self._failed_validations += 1
            return {"error": "Validation failed", "valid": False}
        return self._wrapped.operation(data)


class RetryDecorator(Decorator):
    """Decorator that adds retry logic."""

    def __init__(self, wrapped: Component, max_retries: int = 3, delay: float = 1.0):
        super().__init__(wrapped)
        self._max_retries = max_retries
        self._delay = delay
        self._retries = 0

    def operation(self, data: Any) -> Any:
        """Retry on failure."""
        last_error = None
        for attempt in range(self._max_retries):
            try:
                result = self._wrapped.operation(data)
                if isinstance(result, dict) and "error" in result:
                    last_error = result["error"]
                    self._retries += 1
                    if attempt < self._max_retries - 1:
                        time.sleep(self._delay)
                    continue
                return result
            except Exception as e:
                last_error = str(e)
                self._retries += 1
                if attempt < self._max_retries - 1:
                    time.sleep(self._delay)

        return {"error": f"All retries failed: {last_error}", "attempts": self._retries}


class TransformDecorator(Decorator):
    """Decorator that transforms input/output."""

    def __init__(self, wrapped: Component, input_transform: Callable, output_transform: Callable):
        super().__init__(wrapped)
        self._input_transform = input_transform
        self._output_transform = output_transform

    def operation(self, data: Any) -> Any:
        """Transform input, call, transform output."""
        transformed_input = self._input_transform(data)
        result = self._wrapped.operation(transformed_input)
        return self._output_transform(result)


class DecoratorChain:
    """Chain of decorators."""

    def __init__(self, base: Component):
        self._base = base
        self._decorators: List[Decorator] = []

    def add(self, decorator: Decorator) -> "DecoratorChain":
        """Add a decorator to the chain."""
        self._decorators.append(decorator)
        return self

    def execute(self, data: Any) -> Any:
        """Execute through chain."""
        result = data
        for decorator in reversed(self._decorators):
            result = decorator.operation(result)
        if not self._decorators:
            result = self._base.operation(data)
        return result

    def remove_last(self) -> Optional[Decorator]:
        """Remove last decorator."""
        if self._decorators:
            return self._decorators.pop()
        return None

    def get_decorators(self) -> List[str]:
        """Get decorator names."""
        return [type(d).__name__ for d in self._decorators]


class DecoratorAction(BaseAction):
    """Decorator pattern action."""
    action_type = "decorator"
    display_name = "装饰器模式"
    description = "功能增强装饰器"

    def __init__(self):
        super().__init__()
        self._components: Dict[str, Component] = {}
        self._chains: Dict[str, DecoratorChain] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "execute")

            if operation == "create":
                return self._create_component(params)
            elif operation == "decorate":
                return self._decorate(params)
            elif operation == "chain":
                return self._create_chain(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "stats":
                return self._get_stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Decorator error: {str(e)}")

    def _create_component(self, params: Dict[str, Any]) -> ActionResult:
        """Create a component."""
        component_id = params.get("component_id", str(uuid.uuid4()))

        component = ConcreteComponent(component_id)
        self._components[component_id] = component

        return ActionResult(success=True, message=f"Component created: {component_id}", data={"component_id": component_id})

    def _decorate(self, params: Dict[str, Any]) -> ActionResult:
        """Add decorator to component."""
        component_id = params.get("component_id")
        decorator_type = params.get("type", "logging")

        if not component_id:
            return ActionResult(success=False, message="component_id is required")

        component = self._components.get(component_id)
        if not component:
            return ActionResult(success=False, message=f"Component not found: {component_id}")

        if decorator_type == "logging":
            decorator = LoggingDecorator(component)
        elif decorator_type == "timing":
            decorator = TimingDecorator(component)
        elif decorator_type == "caching":
            decorator = CachingDecorator(component)
        elif decorator_type == "retry":
            decorator = RetryDecorator(component)
        else:
            return ActionResult(success=False, message=f"Unknown decorator type: {decorator_type}")

        self._components[component_id] = decorator

        return ActionResult(success=True, message=f"Decorator added: {decorator_type}")

    def _create_chain(self, params: Dict[str, Any]) -> ActionResult:
        """Create a decorator chain."""
        chain_id = params.get("chain_id", str(uuid.uuid4()))
        component_id = params.get("component_id")

        if not component_id:
            return ActionResult(success=False, message="component_id is required")

        component = self._components.get(component_id)
        if not component:
            return ActionResult(success=False, message=f"Component not found: {component_id}")

        chain = DecoratorChain(component)
        self._chains[chain_id] = chain

        return ActionResult(success=True, message=f"Chain created: {chain_id}", data={"chain_id": chain_id})

    def _execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute through decorator."""
        component_id = params.get("component_id")
        chain_id = params.get("chain_id")
        data = params.get("data")

        if chain_id:
            chain = self._chains.get(chain_id)
            if not chain:
                return ActionResult(success=False, message=f"Chain not found: {chain_id}")
            result = chain.execute(data)
            return ActionResult(success=True, message="Chain executed", data={"result": result})

        if component_id:
            component = self._components.get(component_id)
            if not component:
                return ActionResult(success=False, message=f"Component not found: {component_id}")
            result = component.operation(data)
            return ActionResult(success=True, message="Component executed", data={"result": result})

        return ActionResult(success=False, message="component_id or chain_id required")

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get decorator statistics."""
        component_id = params.get("component_id")

        if not component_id:
            return ActionResult(success=False, message="component_id is required")

        component = self._components.get(component_id)
        if not component:
            return ActionResult(success=False, message=f"Component not found: {component_id}")

        stats = {}
        if isinstance(component, TimingDecorator):
            stats["total_time"] = component.total_time
        if isinstance(component, CachingDecorator):
            stats["cache"] = component.get_stats()
        if isinstance(component, RetryDecorator):
            stats["retries"] = component._retries
        if isinstance(component, ValidationDecorator):
            stats["failed_validations"] = component._failed_validations

        return ActionResult(success=True, message="Stats retrieved", data={"stats": stats})
