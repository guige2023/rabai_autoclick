"""
API Error Handler Module.

Provides structured error handling, mapping, retry logic,
and error recovery patterns for API clients.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Generic
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ErrorCategory(Enum):
    """Error category classification."""
    NETWORK = "network"
    TIMEOUT = "timeout"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    RATE_LIMIT = "rate_limit"
    VALIDATION = "validation"
    SERVER = "server"
    CLIENT = "client"
    UNKNOWN = "unknown"


@dataclass
class APIError(Exception):
    """Base API error class."""
    message: str
    status_code: Optional[int] = None
    category: ErrorCategory = ErrorCategory.UNKNOWN
    details: Dict[str, Any] = field(default_factory=dict)
    retryable: bool = False
    original_error: Optional[Exception] = None
    
    def __str__(self) -> str:
        return f"[{self.category.value}] {self.status_code}: {self.message}"


@dataclass
class ErrorHandlerConfig:
    """Configuration for error handler."""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_categories: List[ErrorCategory] = field(default_factory=lambda: [
        ErrorCategory.NETWORK,
        ErrorCategory.TIMEOUT,
        ErrorCategory.SERVER,
        ErrorCategory.RATE_LIMIT,
    ])
    error_mapping: Dict[int, Tuple[ErrorCategory, str]] = field(default_factory=lambda: {
        400: (ErrorCategory.CLIENT, "Bad Request"),
        401: (ErrorCategory.AUTHENTICATION, "Unauthorized"),
        403: (ErrorCategory.AUTHORIZATION, "Forbidden"),
        404: (ErrorCategory.CLIENT, "Not Found"),
        429: (ErrorCategory.RATE_LIMIT, "Rate Limit Exceeded"),
        500: (ErrorCategory.SERVER, "Internal Server Error"),
        502: (ErrorCategory.SERVER, "Bad Gateway"),
        503: (ErrorCategory.SERVER, "Service Unavailable"),
        504: (ErrorCategory.SERVER, "Gateway Timeout"),
    })


@dataclass
class RetryResult:
    """Result of a retry operation."""
    success: bool
    attempts: int
    final_error: Optional[APIError]
    total_time: float
    results: List[Any] = field(default_factory=list)


class ErrorHandler:
    """
    Structured error handling with retry logic.
    
    Example:
        handler = ErrorHandler(ErrorHandlerConfig(
            max_retries=3,
            retryable_categories=[ErrorCategory.NETWORK, ErrorCategory.SERVER]
        ))
        
        try:
            result = await handler.execute_with_retry(api_call)
        except APIError as e:
            print(f"Failed after {e.attempts} attempts: {e.message}")
    """
    
    def __init__(self, config: Optional[ErrorHandlerConfig] = None) -> None:
        """
        Initialize error handler.
        
        Args:
            config: Error handler configuration.
        """
        self.config = config or ErrorHandlerConfig()
        self._error_counts: Dict[ErrorCategory, int] = {}
        
    def categorize_error(
        self,
        error: Exception,
        status_code: Optional[int] = None,
    ) -> APIError:
        """
        Categorize an error.
        
        Args:
            error: The exception to categorize.
            status_code: HTTP status code if available.
            
        Returns:
            Categorized APIError.
        """
        if isinstance(error, APIError):
            return error
            
        message = str(error)
        
        # Map by status code
        if status_code and status_code in self.config.error_mapping:
            category, default_msg = self.config.error_mapping[status_code]
            retryable = category in self.config.retryable_categories
        else:
            # Infer from error message
            error_str = str(error).lower()
            
            if "timeout" in error_str:
                category = ErrorCategory.TIMEOUT
            elif "connection" in error_str or "network" in error_str:
                category = ErrorCategory.NETWORK
            elif "auth" in error_str or "token" in error_str:
                category = ErrorCategory.AUTHENTICATION
            elif "rate limit" in error_str or "429" in error_str:
                category = ErrorCategory.RATE_LIMIT
            else:
                category = ErrorCategory.UNKNOWN
                
            retryable = category in self.config.retryable_categories
            
        return APIError(
            message=message,
            status_code=status_code,
            category=category,
            retryable=retryable,
            original_error=error,
        )
        
    def is_retryable(self, error: APIError) -> bool:
        """Check if an error is retryable."""
        return error.retryable and error.category in self.config.retryable_categories
        
    async def execute_with_retry(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Execute function with retry logic.
        
        Args:
            func: Async function to execute.
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.
            
        Returns:
            Result of successful execution.
            
        Raises:
            APIError: If all retries fail.
        """
        last_error: Optional[APIError] = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = await asyncio.to_thread(func, *args, **kwargs)
                    
                if attempt > 0:
                    logger.info(f"Succeeded after {attempt + 1} attempts")
                    
                return result
                
            except Exception as e:
                status_code = getattr(e, "status_code", None)
                api_error = self.categorize_error(e, status_code)
                last_error = api_error
                
                if not self.is_retryable(api_error):
                    logger.warning(f"Non-retryable error: {api_error}")
                    raise api_error
                    
                if attempt < self.config.max_retries:
                    delay = self._calculate_delay(attempt)
                    logger.warning(
                        f"Attempt {attempt + 1} failed ({api_error.category.value}), "
                        f"retrying in {delay:.1f}s: {api_error.message}"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"All {self.config.max_retries + 1} attempts failed")
                    
        raise last_error or APIError("All retries failed")
        
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate retry delay with exponential backoff and jitter."""
        delay = min(
            self.config.base_delay * (self.config.exponential_base ** attempt),
            self.config.max_delay
        )
        
        if self.config.jitter:
            import random
            delay *= 0.5 + random.random()  # 50-150% of calculated delay
            
        return delay


class ErrorRecovery:
    """
    Error recovery strategies for failed operations.
    
    Example:
        recovery = ErrorRecovery()
        
        strategies = {
            ErrorCategory.RATE_LIMIT: recovery.rate_limit_recovery,
            ErrorCategory.AUTHENTICATION: recovery.auth_recovery,
            ErrorCategory.SERVER: recovery.server_recovery,
        }
        
        result = await recovery.execute_with_recovery(
            api_call,
            strategies=strategies
        )
    """
    
    def __init__(self) -> None:
        """Initialize error recovery."""
        self._recovery_strategies: Dict[ErrorCategory, Callable[..., Any]] = {}
        
    def register_strategy(
        self,
        category: ErrorCategory,
        strategy: Callable[..., Any],
    ) -> None:
        """
        Register a recovery strategy for an error category.
        
        Args:
            category: Error category to handle.
            strategy: Recovery function.
        """
        self._recovery_strategies[category] = strategy
        
    async def recover(
        self,
        error: APIError,
        context: Dict[str, Any],
    ) -> bool:
        """
        Attempt to recover from an error.
        
        Args:
            error: The error to recover from.
            context: Recovery context.
            
        Returns:
            True if recovery was successful.
        """
        strategy = self._recovery_strategies.get(error.category)
        
        if not strategy:
            logger.warning(f"No recovery strategy for {error.category.value}")
            return False
            
        try:
            result = strategy(error, context)
            if asyncio.iscoroutinefunction(strategy):
                result = await result
            return bool(result)
        except Exception as e:
            logger.error(f"Recovery strategy failed: {e}")
            return False
            
    async def rate_limit_recovery(
        self,
        error: APIError,
        context: Dict[str, Any],
    ) -> bool:
        """
        Recover from rate limit errors.
        
        Args:
            error: Rate limit error.
            context: Recovery context.
            
        Returns:
            True if recovered.
        """
        # Check for Retry-After header
        retry_after = error.details.get("retry_after")
        
        if retry_after:
            wait_time = float(retry_after)
        else:
            # Exponential backoff
            wait_time = context.get("base_delay", 1.0) * 2 ** context.get("attempts", 0)
            
        logger.info(f"Rate limited, waiting {wait_time}s")
        await asyncio.sleep(wait_time)
        
        return True
        
    async def auth_recovery(
        self,
        error: APIError,
        context: Dict[str, Any],
    ) -> bool:
        """
        Recover from authentication errors.
        
        Args:
            error: Auth error.
            context: Recovery context with token manager.
            
        Returns:
            True if recovered.
        """
        token_manager = context.get("token_manager")
        
        if token_manager:
            logger.info("Refreshing authentication token")
            await token_manager.get_valid_token()
            return True
            
        return False
        
    async def server_recovery(
        self,
        error: APIError,
        context: Dict[str, Any],
    ) -> bool:
        """
        Recover from server errors.
        
        Args:
            error: Server error.
            context: Recovery context.
            
        Returns:
            True if recovered.
        """
        # Wait and retry
        wait_time = context.get("base_delay", 1.0) * 2 ** context.get("attempts", 0)
        await asyncio.sleep(min(wait_time, 60.0))
        
        return True
