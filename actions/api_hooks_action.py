"""API Hooks Action Module.

Provides pre-request, post-request, and lifecycle hooks for API interactions.
Supports synchronous and asynchronous hook execution with error handling.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class HookType(Enum):
    """Hook execution points."""
    PRE_REQUEST = "pre_request"
    POST_REQUEST = "post_request"
    ON_SUCCESS = "on_success"
    ON_ERROR = "on_error"
    ON_COMPLETE = "on_complete"
    ON_RETRY = "on_retry"


@dataclass
class HookContext:
    """Context passed to each hook execution."""
    url: str
    method: str
    headers: Dict[str, str]
    params: Optional[Dict[str, Any]]
    body: Optional[Any]
    attempt: int = 1
    max_attempts: int = 3
    start_time: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def elapsed_ms(self) -> float:
        """Return elapsed time since hook context creation."""
        return (time.time() - self.start_time) * 1000


@dataclass
class HookResult:
    """Result from a hook execution."""
    hook_type: HookType
    handler_name: str
    success: bool
    duration_ms: float
    error: Optional[str] = None
    modified_context: Optional[HookContext] = None
    response_data: Optional[Any] = None


class HookHandler:
    """Base hook handler."""

    def __init__(self, name: str, order: int = 0):
        self.name = name
        self.order = order

    async def execute(
        self,
        hook_type: HookType,
        context: HookContext,
        response: Optional[Any] = None,
        error: Optional[Exception] = None
    ) -> HookResult:
        """Execute the hook handler."""
        start = time.time()
        try:
            result = await self._execute(hook_type, context, response, error)
            return HookResult(
                hook_type=hook_type,
                handler_name=self.name,
                success=True,
                duration_ms=(time.time() - start) * 1000,
                response_data=result
            )
        except Exception as e:
            logger.exception(f"Hook {self.name} failed: {e}")
            return HookResult(
                hook_type=hook_type,
                handler_name=self.name,
                success=False,
                duration_ms=(time.time() - start) * 1000,
                error=str(e)
            )

    async def _execute(
        self,
        hook_type: HookType,
        context: HookContext,
        response: Optional[Any],
        error: Optional[Exception]
    ) -> Any:
        """Subclass-specific hook execution."""
        raise NotImplementedError


class LoggingHookHandler(HookHandler):
    """Logs request and response details."""

    def __init__(self, name: str = "logging_hook", log_level: str = "INFO"):
        super().__init__(name)
        self.log_level = getattr(logging, log_level.upper())

    async def _execute(
        self,
        hook_type: HookType,
        context: HookContext,
        response: Optional[Any],
        error: Optional[Exception]
    ) -> None:
        msg = f"[{hook_type.value}] {context.method} {context.url}"
        if error:
            msg += f" | Error: {error}"
        logger.log(self.log_level, msg)


class HeaderInjectionHookHandler(HookHandler):
    """Injects custom headers before request."""

    def __init__(self, headers_to_inject: Dict[str, str]):
        super().__init__("header_injection_hook")
        self.headers_to_inject = headers_to_inject

    async def _execute(
        self,
        hook_type: HookType,
        context: HookContext,
        response: Optional[Any],
        error: Optional[Exception]
    ) -> None:
        if hook_type == HookType.PRE_REQUEST:
            context.headers.update(self.headers_to_inject)


class AuthRefreshHookHandler(HookHandler):
    """Automatically refreshes authentication tokens."""

    def __init__(
        self,
        refresh_token_func: Callable[[], asyncio.Future],
        token_header: str = "Authorization"
    ):
        super().__init__("auth_refresh_hook")
        self.refresh_token_func = refresh_token_func
        self.token_header = token_header
        self._token_cache: Optional[str] = None

    async def _execute(
        self,
        hook_type: HookType,
        context: HookContext,
        response: Optional[Any],
        error: Optional[Exception]
    ) -> None:
        if hook_type == HookType.PRE_REQUEST and self._token_cache:
            context.headers[self.token_header] = self._token_cache
        elif hook_type == HookType.ON_ERROR and response and hasattr(response, "status_code"):
            if response.status_code == 401:
                new_token = await self.refresh_token_func()
                self._token_cache = new_token
                context.headers[self.token_header] = new_token


class MetricsCollectionHookHandler(HookHandler):
    """Collects request/response metrics."""

    def __init__(self):
        super().__init__("metrics_hook")
        self.metrics: List[Dict[str, Any]] = []

    async def _execute(
        self,
        hook_type: HookType,
        context: HookContext,
        response: Optional[Any],
        error: Optional[Exception]
    ) -> None:
        if hook_type in (HookType.ON_SUCCESS, HookType.ON_ERROR):
            self.metrics.append({
                "url": context.url,
                "method": context.method,
                "hook_type": hook_type.value,
                "duration_ms": context.elapsed_ms(),
                "attempt": context.attempt,
                "error": str(error) if error else None,
                "timestamp": time.time()
            })

    def get_metrics(self) -> List[Dict[str, Any]]:
        """Return collected metrics."""
        return self.metrics.copy()

    def clear_metrics(self) -> None:
        """Clear collected metrics."""
        self.metrics.clear()


class APIMetric:
    """Action metric for API hooks."""

    def __init__(self):
        self.total_requests: int = 0
        self.successful_requests: int = 0
        self.failed_requests: int = 0
        self.total_duration_ms: float = 0.0
        self.hook_errors: int = 0

    def record_request(self, success: bool, duration_ms: float) -> None:
        """Record a request result."""
        self.total_requests += 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
        self.total_duration_ms += duration_ms

    def record_hook_error(self) -> None:
        """Record a hook error."""
        self.hook_errors += 1

    def average_duration_ms(self) -> float:
        """Return average request duration."""
        if self.total_requests == 0:
            return 0.0
        return self.total_duration_ms / self.total_requests

    def success_rate(self) -> float:
        """Return success rate as percentage."""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100


class APIHooksAction:
    """Main action class for API hooks management."""

    def __init__(self):
        self._hooks: Dict[HookType, List[HookHandler]] = {t: [] for t in HookType}
        self._metrics = APIMetric()
        self._enabled = True

    def register_hook(self, hook_type: HookType, handler: HookHandler) -> None:
        """Register a hook handler for a specific hook type."""
        self._hooks[hook_type].append(handler)
        self._hooks[hook_type].sort(key=lambda h: h.order)

    def unregister_hook(self, hook_type: HookType, handler_name: str) -> bool:
        """Unregister a hook handler by name."""
        hooks = self._hooks[hook_type]
        for i, h in enumerate(hooks):
            if h.name == handler_name:
                hooks.pop(i)
                return True
        return False

    def clear_hooks(self, hook_type: Optional[HookType] = None) -> None:
        """Clear all hooks or hooks for a specific type."""
        if hook_type:
            self._hooks[hook_type].clear()
        else:
            for t in HookType:
                self._hooks[t].clear()

    def enable(self) -> None:
        """Enable hook execution."""
        self._enabled = True

    def disable(self) -> None:
        """Disable hook execution."""
        self._enabled = False

    async def execute_hooks(
        self,
        hook_type: HookType,
        context: HookContext,
        response: Optional[Any] = None,
        error: Optional[Exception] = None
    ) -> List[HookResult]:
        """Execute all registered hooks for a given type."""
        if not self._enabled:
            return []

        results = []
        handlers = self._hooks.get(hook_type, [])

        for handler in handlers:
            try:
                result = await handler.execute(hook_type, context, response, error)
                results.append(result)
                if not result.success:
                    self._metrics.record_hook_error()
                if result.modified_context:
                    context = result.modified_context
            except Exception as e:
                logger.exception(f"Hook execution failed for {handler.name}: {e}")
                self._metrics.record_hook_error()

        return results

    def get_metrics(self) -> APIMetric:
        """Return current metrics."""
        return self._metrics

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the API hooks action.

        Args:
            context: Dictionary containing:
                - hook_type: HookType value to trigger
                - url: Request URL
                - method: HTTP method
                - headers: Request headers
                - params: Query parameters
                - body: Request body
                - response: Optional response object
                - error: Optional exception

        Returns:
            Dictionary with execution results and updated context.
        """
        hook_type_str = context.get("hook_type", "pre_request")
        try:
            hook_type = HookType(hook_type_str)
        except ValueError:
            raise ValueError(f"Invalid hook_type: {hook_type_str}")

        url = context.get("url", "")
        method = context.get("method", "GET")
        headers = context.get("headers", {})
        params = context.get("params")
        body = context.get("body")
        response = context.get("response")
        error = context.get("error")

        hook_context = HookContext(
            url=url,
            method=method,
            headers=dict(headers),
            params=params,
            body=body
        )

        results = await self.execute_hooks(
            hook_type, hook_context, response, error
        )

        return {
            "success": all(r.success for r in results),
            "hook_type": hook_type_str,
            "results": [
                {
                    "handler": r.handler_name,
                    "success": r.success,
                    "duration_ms": r.duration_ms,
                    "error": r.error
                }
                for r in results
            ],
            "modified_headers": hook_context.headers,
            "metrics": {
                "total_requests": self._metrics.total_requests,
                "success_rate": self._metrics.success_rate()
            }
        }
