"""
API Middleware Action Module.

Middleware chain for API request/response processing,
supports authentication, logging, rate limiting, and transformation.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
import time

logger = logging.getLogger(__name__)


class MiddlewarePhase(Enum):
    """Middleware execution phases."""
    BEFORE_REQUEST = "before_request"
    AFTER_REQUEST = "after_request"
    ON_ERROR = "on_error"


@dataclass
class MiddlewareContext:
    """Context passed through middleware chain."""
    request: dict[str, Any]
    response: Optional[dict[str, Any]] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


@dataclass
class MiddlewareConfig:
    """Middleware configuration."""
    name: str
    phase: MiddlewarePhase
    enabled: bool = True
    order: int = 0


MiddlewareFunc = Callable[[MiddlewareContext], MiddlewareContext]


class APIMiddlewareAction:
    """
    Middleware chain for API request/response processing.

    Supports composable middleware for auth, logging,
    transformation, and more.

    Example:
        mw = APIMiddlewareAction()
        mw.use(auth_middleware, phase=MiddlewarePhase.BEFORE_REQUEST)
        mw.use(logging_middleware)
        ctx = mw.process(request_data)
    """

    def __init__(self) -> None:
        self._middleware: list[tuple[MiddlewareConfig, MiddlewareFunc]] = []

    def add(
        self,
        name: str,
        func: MiddlewareFunc,
        phase: MiddlewarePhase = MiddlewarePhase.BEFORE_REQUEST,
        order: int = 0,
        enabled: bool = True,
    ) -> "APIMiddlewareAction":
        """Register a middleware function."""
        config = MiddlewareConfig(
            name=name,
            phase=phase,
            order=order,
            enabled=enabled,
        )
        self._middleware.append((config, func))
        self._middleware.sort(key=lambda x: (x[0].phase.value, x[0].order))
        return self

    def use(
        self,
        func: MiddlewareFunc,
        phase: MiddlewarePhase = MiddlewarePhase.BEFORE_REQUEST,
        order: int = 0,
    ) -> "APIMiddlewareAction":
        """Alias for add()."""
        return self.add(func.__name__, func, phase, order)

    def process(
        self,
        request: dict[str, Any],
        response: Optional[dict[str, Any]] = None,
    ) -> MiddlewareContext:
        """Process request through middleware chain."""
        ctx = MiddlewareContext(
            request=request,
            response=response,
        )

        before = [m for m in self._middleware
                  if m[0].phase == MiddlewarePhase.BEFORE_REQUEST and m[0].enabled]

        for config, func in before:
            try:
                ctx = func(ctx)
            except Exception as e:
                logger.error("Middleware '%s' error: %s", config.name, e)
                ctx.errors.append(f"{config.name}: {str(e)}")

        return ctx

    def process_response(
        self,
        ctx: MiddlewareContext,
        response: dict[str, Any],
    ) -> MiddlewareContext:
        """Process response through middleware chain."""
        ctx.response = response

        after = [m for m in self._middleware
                 if m[0].phase == MiddlewarePhase.AFTER_REQUEST and m[0].enabled]

        for config, func in after:
            try:
                ctx = func(ctx)
            except Exception as e:
                logger.error("Middleware '%s' error: %s", config.name, e)
                ctx.errors.append(f"{config.name}: {str(e)}")

        return ctx

    def remove(self, name: str) -> bool:
        """Remove middleware by name."""
        for i, (config, _) in enumerate(self._middleware):
            if config.name == name:
                del self._middleware[i]
                return True
        return False

    def disable(self, name: str) -> bool:
        """Disable middleware by name."""
        for config, _ in self._middleware:
            if config.name == name:
                config.enabled = False
                return True
        return False

    def enable(self, name: str) -> bool:
        """Enable middleware by name."""
        for config, _ in self._middleware:
            if config.name == name:
                config.enabled = True
                return True
        return False

    def clear(self) -> None:
        """Remove all middleware."""
        self._middleware.clear()

    def list_middleware(self) -> list[str]:
        """List all registered middleware names."""
        return [config.name for config, _ in self._middleware]


def auth_middleware(token_field: str = "Authorization") -> MiddlewareFunc:
    """Create authentication middleware."""
    def middleware(ctx: MiddlewareContext) -> MiddlewareContext:
        headers = ctx.request.get("headers", {})
        if token_field in headers:
            ctx.metadata["authenticated"] = True
        return ctx
    return middleware


def logging_middleware() -> MiddlewareFunc:
    """Create request logging middleware."""
    def middleware(ctx: MiddlewareContext) -> MiddlewareContext:
        start = time.time()
        ctx.metadata["request_start"] = start
        logger.debug("Processing request to %s", ctx.request.get("url", "unknown"))
        return ctx
    return middleware
