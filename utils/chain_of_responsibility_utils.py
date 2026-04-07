"""
Chain of Responsibility Pattern Implementation

Passes a request along a chain of handlers until one of them handles it.
Each handler decides to process the request or pass it to the next handler.
"""

from __future__ import annotations

import copy
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass
class HandlerResult:
    """Result from a handler in the chain."""
    handled: bool
    processed: bool = False
    data: Any = None
    message: str = ""
    handler_name: str = ""

    @property
    def should_pass_on(self) -> bool:
        """Check if request should be passed to next handler."""
        return not self.handled and not self.processed


class Handler(ABC, Generic[T]):
    """
    Abstract base class for handlers.

    Type Parameters:
        T: The type of request this handler processes.
    """

    def __init__(self, name: str = ""):
        self._name = name or self.__class__.__name__
        self._next_handler: Handler[T] | None = None
        self._metrics: dict[str, Any] = {
            "processed": 0,
            "passed_on": 0,
            "rejected": 0,
        }

    @property
    def name(self) -> str:
        """Get handler name."""
        return self._name

    @property
    def next_handler(self) -> Handler[T] | None:
        """Get the next handler in the chain."""
        return self._next_handler

    def set_next(self, handler: Handler[T]) -> Handler[T]:
        """
        Set the next handler in the chain.

        Returns:
            The next handler (for chaining).
        """
        self._next_handler = handler
        return handler

    def chain(self, handler: Handler[T]) -> Handler[T]:
        """
        Add a handler to the end of the chain.

        Returns:
            The first handler in the chain.
        """
        current = self
        while current._next_handler is not None:
            current = current._next_handler
        current._next_handler = handler
        return self

    @abstractmethod
    def handle(self, request: T) -> HandlerResult:
        """
        Handle the request.

        Returns:
            HandlerResult indicating what happened.
        """
        pass

    def process(self, request: T) -> HandlerResult:
        """
        Process a request through this handler.

        This method can be overridden to add pre/post processing.
        """
        result = self.handle(request)

        if result.should_pass_on and self._next_handler is not None:
            self._metrics["passed_on"] += 1
            return self._next_handler.process(request)

        if result.handled or result.processed:
            self._metrics["processed"] += 1
        else:
            self._metrics["rejected"] += 1

        result.handler_name = self._name
        return result

    @property
    def metrics(self) -> dict[str, Any]:
        """Get handler metrics."""
        return copy.copy(self._metrics)

    def reset_metrics(self) -> None:
        """Reset handler metrics."""
        self._metrics = {"processed": 0, "passed_on": 0, "rejected": 0}


class SyncHandler(Handler[T]):
    """Synchronous handler implementation."""

    def __init__(
        self,
        name: str = "",
        process_func: Callable[[T], HandlerResult] | None = None,
        should_handle: Callable[[T], bool] | None = None,
    ):
        super().__init__(name)
        self._process_func = process_func
        self._should_handle = should_handle

    def handle(self, request: T) -> HandlerResult:
        """Handle the request."""
        if self._should_handle and not self._should_handle(request):
            return HandlerResult(handled=False, message="Condition not met")

        if self._process_func:
            return self._process_func(request)

        return HandlerResult(handled=False, message=f"{self._name} did not handle request")


@dataclass
class ChainMetrics:
    """Metrics for the entire chain."""
    total_requests: int = 0
    handled_requests: int = 0
    passed_requests: int = 0
    rejected_requests: int = 0
    by_handler: dict[str, dict[str, int]] = field(default_factory=dict)


class MeasuredChain(Handler[T]):
    """Handler chain with metrics collection."""

    def __init__(self, name: str = ""):
        super().__init__(name)
        self._chain_metrics = ChainMetrics()

    def process(self, request: T) -> HandlerResult:
        """Process with metrics collection."""
        self._chain_metrics.total_requests += 1

        result = super().process(request)

        if result.handled:
            self._chain_metrics.handled_requests += 1
        elif result.processed:
            self._chain_metrics.handled_requests += 1
        elif self._next_handler is None:
            self._chain_metrics.rejected_requests += 1

        return result

    def record_handler_metrics(self, handler_name: str, metrics: dict[str, Any]) -> None:
        """Record metrics from a handler."""
        if handler_name not in self._chain_metrics.by_handler:
            self._chain_metrics.by_handler[handler_name] = {"processed": 0, "passed_on": 0}
        for key, value in metrics.items():
            if key in self._chain_metrics.by_handler[handler_name]:
                self._chain_metrics.by_handler[handler_name][key] += value

    @property
    def metrics(self) -> ChainMetrics:
        """Get chain metrics."""
        return self._chain_metrics


class ChainBuilder(Generic[T]):
    """
    Builder for constructing handler chains.
    """

    def __init__(self):
        self._handlers: list[Handler[T]] = []
        self._default_handler: Handler[T] | None = None

    def add(self, handler: Handler[T]) -> ChainBuilder[T]:
        """Add a handler to the chain."""
        self._handlers.append(handler)
        return self

    def add_first(
        self,
        should_handle: Callable[[T], bool],
        process: Callable[[T], Any],
        name: str = "",
    ) -> ChainBuilder[T]:
        """Add a conditional handler at the start."""
        handler = SyncHandler(
            name=name,
            process_func=lambda req: HandlerResult(
                handled=True,
                processed=True,
                data=process(req),
                message="Processed",
            ),
            should_handle=should_handle,
        )
        self._handlers.insert(0, handler)
        return self

    def with_default(self, handler: Handler[T]) -> ChainBuilder[T]:
        """Set a default handler for unmatched requests."""
        self._default_handler = handler
        return self

    def build(self) -> Handler[T] | None:
        """Build and return the chain."""
        if not self._handlers:
            return self._default_handler

        head = self._handlers[0]
        current = head

        for handler in self._handlers[1:]:
            current.set_next(handler)
            current = handler

        if self._default_handler:
            current.set_next(self._default_handler)

        return head


class ConditionalHandler(Handler[T]):
    """
    Handler that conditionally processes requests.
    """

    def __init__(
        self,
        name: str = "",
        condition: Callable[[T], bool] | None = None,
        processor: Callable[[T], Any] | None = None,
        on_success: Callable[[T, Any], None] | None = None,
        on_failure: Callable[[T, Exception], None] | None = None,
    ):
        super().__init__(name)
        self._condition = condition
        self._processor = processor
        self._on_success = on_success
        self._on_failure = on_failure

    def handle(self, request: T) -> HandlerResult:
        """Handle the request conditionally."""
        if self._condition and not self._condition(request):
            return HandlerResult(handled=False, message="Condition not met")

        try:
            result_data = self._processor(request) if self._processor else None

            if self._on_success:
                self._on_success(request, result_data)

            return HandlerResult(
                handled=True,
                processed=True,
                data=result_data,
                message=f"{self._name} processed request",
            )

        except Exception as e:
            if self._on_failure:
                self._on_failure(request, e)

            return HandlerResult(
                handled=False,
                processed=False,
                message=f"{self._name} failed: {str(e)}",
            )


@dataclass
class ChainLink:
    """Metadata for a chain link."""
    index: int
    handler: Handler
    condition: str | None = None
    enabled: bool = True


class ChainRegistry(Generic[T]):
    """
    Registry for managing and reusing handler chains.
    """

    def __init__(self):
        self._chains: dict[str, list[ChainLink]] = {}
        self._default_chain: str = ""

    def register_chain(self, name: str, links: list[ChainLink], default: bool = False) -> None:
        """Register a named chain."""
        self._chains[name] = links
        if default or not self._default_chain:
            self._default_chain = name

    def get_chain(self, name: str) -> list[ChainLink] | None:
        """Get a chain by name."""
        return self._chains.get(name)

    def build_chain(self, name: str) -> Handler[T] | None:
        """Build a handler chain by name."""
        links = self._chains.get(name)
        if not links:
            return None

        enabled_links = [link for link in links if link.enabled]
        if not enabled_links:
            return None

        head = enabled_links[0].handler
        current = head

        for link in enabled_links[1:]:
            current.set_next(link.handler)
            current = link.handler

        return head

    def list_chains(self) -> list[str]:
        """List all registered chain names."""
        return list(self._chains.keys())

    def enable_link(self, chain_name: str, index: int, enabled: bool = True) -> bool:
        """Enable or disable a link in a chain."""
        links = self._chains.get(chain_name)
        if links and 0 <= index < len(links):
            links[index].enabled = enabled
            return True
        return False
