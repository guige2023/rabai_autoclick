"""Timeout guard for element operations with automatic fallback."""
from typing import Optional, Callable, Any, TypeVar, Dict
from dataclasses import dataclass
import time
import threading
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class TimeoutResult:
    """Result of a timed operation."""
    success: bool
    value: Optional[Any] = None
    error: Optional[str] = None
    elapsed: float = 0.0
    attempts: int = 1


class ElementTimeoutGuard:
    """Guards element operations with timeout and automatic retry/fallback.
    
    Provides timeout protection for element queries and actions, with
    configurable retry logic and fallback behaviors.
    
    Example:
        guard = ElementTimeoutGuard(default_timeout=5.0, max_retries=3)
        
        result = guard.execute(
            lambda: element.click(),
            fallback=lambda: fallback_element.click(),
        )
        
        if result.success:
            print(f"Click succeeded in {result.elapsed:.2f}s")
        else:
            print(f"Failed: {result.error}")
    """

    def __init__(
        self,
        default_timeout: float = 10.0,
        max_retries: int = 3,
        retry_delay: float = 0.5,
        exponential_backoff: bool = True,
    ) -> None:
        """Initialize the timeout guard.
        
        Args:
            default_timeout: Default timeout in seconds.
            max_retries: Maximum retry attempts.
            retry_delay: Delay between retries in seconds.
            exponential_backoff: Use exponential backoff for retries.
        """
        self._default_timeout = default_timeout
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._exponential_backoff = exponential_backoff
        self._operation_hooks: Dict[str, Callable] = {}

    def execute(
        self,
        operation: Callable[[], T],
        timeout: Optional[float] = None,
        fallback: Optional[Callable[[], T]] = None,
        on_timeout: Optional[Callable[[], None]] = None,
        on_retry: Optional[Callable[[int, Exception], None]] = None,
    ) -> TimeoutResult[T]:
        """Execute an operation with timeout and retry protection.
        
        Args:
            operation: Function to execute.
            timeout: Operation timeout in seconds (uses default if None).
            fallback: Optional fallback function if all retries fail.
            on_timeout: Optional callback on timeout.
            on_retry: Optional callback on each retry (attempt_num, exception).
            
        Returns:
            TimeoutResult with success status and value/error.
        """
        timeout_val = timeout if timeout is not None else self._default_timeout
        start_time = time.time()
        last_error: Optional[Exception] = None
        
        for attempt in range(self._max_retries + 1):
            try:
                result = self._with_timeout(operation, timeout_val)
                elapsed = time.time() - start_time
                return TimeoutResult(
                    success=True,
                    value=result,
                    elapsed=elapsed,
                    attempts=attempt + 1,
                )
            except TimeoutError:
                elapsed = time.time() - start_time
                logger.warning("Operation timed out on attempt %d/%d", attempt + 1, self._max_retries + 1)
                if on_timeout:
                    on_timeout()
                
                if attempt < self._max_retries:
                    last_error = TimeoutError(f"Timed out after {timeout_val}s")
                    if on_retry:
                        on_retry(attempt + 1, last_error)
                    
                    delay = self._retry_delay * (2 ** attempt if self._exponential_backoff else 1)
                    time.sleep(delay)
                else:
                    break
            except Exception as e:
                elapsed = time.time() - start_time
                logger.warning("Operation failed on attempt %d/%d: %s", attempt + 1, self._max_retries + 1, e)
                last_error = e
                
                if attempt < self._max_retries:
                    if on_retry:
                        on_retry(attempt + 1, e)
                    delay = self._retry_delay * (2 ** attempt if self._exponential_backoff else 1)
                    time.sleep(delay)
                else:
                    break
        
        # Try fallback if provided
        if fallback is not None:
            try:
                fallback_start = time.time()
                result = self._with_timeout(fallback, timeout_val)
                elapsed = time.time() - start_time
                logger.info("Fallback succeeded")
                return TimeoutResult(
                    success=True,
                    value=result,
                    elapsed=elapsed,
                    attempts=self._max_retries + 1,
                )
            except Exception as e:
                logger.error("Fallback also failed: %s", e)
        
        elapsed = time.time() - start_time
        return TimeoutResult(
            success=False,
            error=str(last_error) if last_error else "Unknown error",
            elapsed=elapsed,
            attempts=self._max_retries + 1,
        )

    def execute_async(
        self,
        operation: Callable[[], T],
        timeout: Optional[float] = None,
    ) -> tuple[bool, Optional[T], Optional[str]]:
        """Execute operation in background thread with timeout.
        
        Args:
            operation: Function to execute.
            timeout: Operation timeout in seconds.
            
        Returns:
            Tuple of (success, value, error).
        """
        result_container: Dict[str, Any] = {"value": None, "error": None, "done": False}
        
        def worker():
            try:
                result_container["value"] = operation()
            except Exception as e:
                result_container["error"] = str(e)
            finally:
                result_container["done"] = True
        
        thread = threading.Thread(target=worker)
        thread.start()
        
        timeout_val = timeout if timeout is not None else self._default_timeout
        thread.join(timeout_val)
        
        if not result_container["done"]:
            return (False, None, f"Timed out after {timeout_val}s")
        
        if result_container["error"]:
            return (False, None, result_container["error"])
        
        return (True, result_container["value"], None)

    def _with_timeout(self, operation: Callable[[], T], timeout: float) -> T:
        """Execute operation with threading-based timeout.
        
        Args:
            operation: Function to execute.
            timeout: Timeout in seconds.
            
        Returns:
            Operation result.
            
        Raises:
            TimeoutError: If operation exceeds timeout.
        """
        result_container: Dict[str, Any] = {"value": None, "error": None, "done": False}
        
        def worker():
            try:
                result_container["value"] = operation()
            except Exception as e:
                result_container["error"] = e
            finally:
                result_container["done"] = True
        
        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout)
        
        if not result_container["done"]:
            raise TimeoutError()
        
        if result_container["error"]:
            raise result_container["error"]
        
        return result_container["value"]

    def with_timeout(self, timeout: Optional[float] = None) -> Callable:
        """Decorator to add timeout to any function.
        
        Args:
            timeout: Timeout in seconds.
            
        Returns:
            Decorated function.
        """
        def decorator(func: Callable[..., T]) -> Callable[..., TimeoutResult[T]]:
            def wrapper(*args, **kwargs) -> TimeoutResult[T]:
                return self.execute(
                    lambda: func(*args, **kwargs),
                    timeout=timeout,
                )
            return wrapper
        return decorator
