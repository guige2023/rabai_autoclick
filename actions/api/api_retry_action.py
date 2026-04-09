"""API Retry Action Module.

Provides retry logic specifically designed for API operations
with support for exponential backoff, rate limiting, and error classification.

Example:
    >>> from actions.api.api_retry_action import APIRetryAction
    >>> retry = APIRetryAction()
    >>> result = await retry.execute_with_retry(api_call)
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, Union
import threading


T = TypeVar('T')


class RetryCategory(Enum):
    """Categories of errors for retry decisions."""
    TRANSIENT = "transient"     # Temporary, worth retrying
    RATE_LIMIT = "rate_limit"   # Rate limited, retry with backoff
    CLIENT_ERROR = "client_error"  # Bad request, don't retry
    SERVER_ERROR = "server_error"  # Server issue, retry
    AUTH_ERROR = "auth_error"    # Authentication issue, may need refresh
    NETWORK_ERROR = "network_error"  # Network issue, retry


class RetryStrategy(Enum):
    """Retry strategy types."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_WITH_JITTER = "exponential_with_jitter"


@dataclass
class RetryPolicy:
    """Policy for retry behavior.
    
    Attributes:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        strategy: Backoff strategy
        jitter_factor: Jitter as fraction of delay
        retryable_categories: Categories to retry
        timeout: Per-call timeout in seconds
    """
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    jitter_factor: float = 0.1
    retryable_categories: Set[RetryCategory] = field(default_factory=lambda: {
        RetryCategory.TRANSIENT,
        RetryCategory.RATE_LIMIT,
        RetryCategory.SERVER_ERROR,
        RetryCategory.NETWORK_ERROR
    })
    timeout: float = 30.0


@dataclass
class RetryAttempt:
    """Record of a single retry attempt.
    
    Attributes:
        attempt_number: Which attempt this was
        category: Error category
        error_message: Error message
        duration: How long the attempt took
        timestamp: When the attempt occurred
    """
    attempt_number: int
    category: Optional[RetryCategory]
    error_message: str
    duration: float
    timestamp: datetime


@dataclass
class RetryResult:
    """Result of a retry operation.
    
    Attributes:
        success: Whether the operation succeeded
        result: Result value if successful
        attempts: List of all attempts
        total_duration: Total time spent
        final_category: Category of final error if failed
    """
    success: bool
    result: Any = None
    attempts: List[RetryAttempt] = field(default_factory=list)
    total_duration: float = 0.0
    final_category: Optional[RetryCategory] = None
    final_error: Optional[str] = None


class APIRetryAction:
    """Handles retry logic for API operations.
    
    Provides sophisticated retry mechanisms with error classification,
    rate limit handling, and various backoff strategies.
    
    Attributes:
        policy: Current retry policy
        error_classifiers: Custom error classifiers
    
    Example:
        >>> retry = APIRetryAction(RetryPolicy(max_retries=5))
        >>> result = await retry.execute_with_retry(api_call)
    """
    
    def __init__(self, policy: Optional[RetryPolicy] = None):
        """Initialize the API retry action.
        
        Args:
            policy: Retry policy. Uses defaults if not provided.
        """
        self.policy = policy or RetryPolicy()
        self._error_classifiers: Dict[type, RetryCategory] = {}
        self._status_code_categories: Dict[int, RetryCategory] = {
            # 2xx - Success
            200: RetryCategory.TRANSIENT, 201: RetryCategory.TRANSIENT,
            204: RetryCategory.TRANSIENT,
            # 4xx - Client errors
            400: RetryCategory.CLIENT_ERROR,
            401: RetryCategory.AUTH_ERROR,
            403: RetryCategory.AUTH_ERROR,
            404: RetryCategory.CLIENT_ERROR,
            422: RetryCategory.CLIENT_ERROR,
            429: RetryCategory.RATE_LIMIT,  # Too many requests
            # 5xx - Server errors
            500: RetryCategory.SERVER_ERROR,
            502: RetryCategory.SERVER_ERROR,
            503: RetryCategory.SERVER_ERROR,
            504: RetryCategory.SERVER_ERROR,
        }
        self._lock = threading.RLock()
        self._call_counter = 0
    
    def register_error_classifier(
        self,
        error_type: type,
        category: RetryCategory
    ) -> "APIRetryAction":
        """Register a custom error classifier.
        
        Args:
            error_type: Exception type
            category: Category to classify as
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._error_classifiers[error_type] = category
            return self
    
    def register_status_code(
        self,
        status_code: int,
        category: RetryCategory
    ) -> "APIRetryAction":
        """Register how a status code should be categorized.
        
        Args:
            status_code: HTTP status code
            category: Category to classify as
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._status_code_categories[status_code] = category
            return self
    
    def classify_error(
        self,
        error: Exception,
        status_code: Optional[int] = None
    ) -> RetryCategory:
        """Classify an error into a retry category.
        
        Args:
            error: The exception to classify
            status_code: Optional HTTP status code
        
        Returns:
            RetryCategory for the error
        """
        # Check status code first
        if status_code is not None:
            return self._status_code_categories.get(
                status_code,
                RetryCategory.TRANSIENT
            )
        
        # Check registered classifiers
        error_type = type(error)
        for cls in error_type.__mro__:
            if cls in self._error_classifiers:
                return self._error_classifiers[cls]
        
        # Default classifications based on error message
        error_msg = str(error).lower()
        
        if "rate limit" in error_msg or "429" in error_msg:
            return RetryCategory.RATE_LIMIT
        elif "timeout" in error_msg or "timed out" in error_msg:
            return RetryCategory.TRANSIENT
        elif "network" in error_msg or "connection" in error_msg:
            return RetryCategory.NETWORK_ERROR
        elif "auth" in error_msg or "token" in error_msg:
            return RetryCategory.AUTH_ERROR
        elif "500" in error_msg or "502" in error_msg or "503" in error_msg:
            return RetryCategory.SERVER_ERROR
        elif "400" in error_msg or "404" in error_msg:
            return RetryCategory.CLIENT_ERROR
        
        return RetryCategory.TRANSIENT
    
    def should_retry(self, category: RetryCategory) -> bool:
        """Determine if an error category should be retried.
        
        Args:
            category: Error category
        
        Returns:
            True if the error should be retried
        """
        return category in self.policy.retryable_categories
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay before next retry.
        
        Args:
            attempt: Current attempt number
        
        Returns:
            Delay in seconds
        """
        base_delay = min(
            self.policy.initial_delay * (2 ** attempt),
            self.policy.max_delay
        )
        
        if self.policy.strategy == RetryStrategy.FIXED:
            delay = self.policy.initial_delay
        elif self.policy.strategy == RetryStrategy.LINEAR:
            delay = self.policy.initial_delay * attempt
        elif self.policy.strategy == RetryStrategy.EXPONENTIAL:
            delay = base_delay
        elif self.policy.strategy == RetryStrategy.EXPONENTIAL_WITH_JITTER:
            jitter = base_delay * self.policy.jitter_factor
            delay = base_delay + random.uniform(-jitter, jitter)
        else:
            delay = base_delay
        
        return min(max(delay, 0.1), self.policy.max_delay)
    
    async def execute_with_retry(
        self,
        task: Callable[..., Any],
        *args: Any,
        status_code_extractor: Optional[Callable[[Any], int]] = None,
        **kwargs: Any
    ) -> Any:
        """Execute a task with retry logic.
        
        Args:
            task: Task to execute
            *args: Positional arguments
            status_code_extractor: Optional function to extract status code from result
            **kwargs: Keyword arguments
        
        Returns:
            Task result
        
        Raises:
            Exception: If all retries are exhausted
        """
        attempts: List[RetryAttempt] = []
        last_error: Optional[Exception] = None
        last_category: Optional[RetryCategory] = None
        
        for attempt in range(self.policy.max_retries + 1):
            start_time = time.time()
            
            try:
                if asyncio.iscoroutinefunction(task):
                    result = await asyncio.wait_for(
                        task(*args, **kwargs),
                        timeout=self.policy.timeout
                    )
                else:
                    result = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: task(*args, **kwargs)
                    )
                
                # Check for error in result (non-exception errors)
                status_code = None
                if status_code_extractor:
                    try:
                        status_code = status_code_extractor(result)
                    except Exception:
                        pass
                
                duration = time.time() - start_time
                
                # Success
                if status_code is None or 200 <= status_code < 300:
                    return RetryResult(
                        success=True,
                        result=result,
                        attempts=attempts,
                        total_duration=sum(a.duration for a in attempts) + duration
                    )
                
                # HTTP error - treat as exception
                error_msg = f"HTTP {status_code}"
                last_error = Exception(error_msg)
                last_category = self.classify_error(last_error, status_code)
                
            except asyncio.TimeoutError:
                duration = time.time() - start_time
                last_error = Exception("Request timed out")
                last_category = RetryCategory.TRANSIENT
                duration = time.time() - start_time
                
            except Exception as e:
                duration = time.time() - start_time
                last_error = e
                last_category = self.classify_error(e)
            
            # Record attempt
            attempts.append(RetryAttempt(
                attempt_number=attempt + 1,
                category=last_category,
                error_message=str(last_error),
                duration=duration,
                timestamp=datetime.now()
            ))
            
            # Check if we should retry
            if not self.should_retry(last_category):
                break
            
            if attempt < self.policy.max_retries:
                delay = self.calculate_delay(attempt)
                
                # Extra delay for rate limiting
                if last_category == RetryCategory.RATE_LIMIT:
                    delay = max(delay, 5.0)  # At least 5 seconds for rate limits
                
                await asyncio.sleep(delay)
        
        return RetryResult(
            success=False,
            attempts=attempts,
            total_duration=sum(a.duration for a in attempts),
            final_category=last_category,
            final_error=str(last_error)
        )
    
    def execute_sync_with_retry(
        self,
        task: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """Synchronous version of execute_with_retry.
        
        Args:
            task: Task to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
        
        Returns:
            Task result
        """
        return asyncio.run(self.execute_with_retry(task, *args, **kwargs))
    
    def get_retry_stats(self) -> Dict[str, Any]:
        """Get retry statistics.
        
        Returns:
            Dictionary with retry stats
        """
        return {
            "policy": {
                "max_retries": self.policy.max_retries,
                "initial_delay": self.policy.initial_delay,
                "strategy": self.policy.strategy.value,
            },
            "registered_errors": len(self._error_classifiers),
            "registered_status_codes": len(self._status_code_categories)
        }
