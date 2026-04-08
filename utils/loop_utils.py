"""
Loop iteration and batch processing utilities.

Provides utilities for efficient loop operations,
batch processing, chunking, and iteration patterns.
"""

from __future__ import annotations

import time
from typing import List, TypeVar, Callable, Optional, Iterator, Any, Dict, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed


T = TypeVar('T')
R = TypeVar('R')


@dataclass
class BatchResult:
    """Result of a batch operation."""
    total: int
    successful: int
    failed: int
    results: List[Any]
    errors: List[Exception]
    duration_seconds: float


def chunk_list(items: List[T], chunk_size: int) -> List[List[T]]:
    """Split a list into chunks of specified size.
    
    Args:
        items: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def chunk_iterator(items: List[T], chunk_size: int) -> Iterator[List[T]]:
    """Iterator version of chunk_list.
    
    Args:
        items: List to chunk
        chunk_size: Size of each chunk
        
    Yields:
        Chunks of items
    """
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def batch_process(
    items: List[T],
    process_func: Callable[[T], R],
    batch_size: int = 10,
    stop_on_error: bool = False
) -> BatchResult:
    """Process items in batches.
    
    Args:
        items: Items to process
        process_func: Function to apply to each item
        batch_size: Items per batch
        stop_on_error: Stop processing on first error
        
    Returns:
        BatchResult with all results
    """
    start_time = time.time()
    results = []
    errors = []
    successful = 0
    failed = 0
    
    for chunk in chunk_iterator(items, batch_size):
        for item in chunk:
            try:
                result = process_func(item)
                results.append(result)
                successful += 1
            except Exception as e:
                errors.append(e)
                failed += 1
                if stop_on_error:
                    return BatchResult(
                        total=len(items),
                        successful=successful,
                        failed=failed,
                        results=results,
                        errors=errors,
                        duration_seconds=time.time() - start_time
                    )
    
    return BatchResult(
        total=len(items),
        successful=successful,
        failed=failed,
        results=results,
        errors=errors,
        duration_seconds=time.time() - start_time
    )


def parallel_map(
    items: List[T],
    process_func: Callable[[T], R],
    max_workers: int = 4,
    timeout: Optional[float] = None
) -> List[Optional[R]]:
    """Map function over items in parallel.
    
    Args:
        items: Items to process
        process_func: Function to apply
        max_workers: Maximum parallel workers
        timeout: Optional timeout per item
        
    Returns:
        List of results (None for failed items)
    """
    results = [None] * len(items)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {
            executor.submit(process_func, item): i
            for i, item in enumerate(items)
        }
        
        for future in as_completed(future_to_index, timeout=timeout):
            index = future_to_index[future]
            try:
                results[index] = future.result()
            except Exception:
                results[index] = None
    
    return results


def retry_loop(
    func: Callable[[], R],
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[type, ...] = (Exception,)
) -> Optional[R]:
    """Retry a function with exponential backoff.
    
    Args:
        func: Function to retry
        max_attempts: Maximum number of attempts
        delay: Initial delay between retries
        backoff: Backoff multiplier
        exceptions: Tuple of exceptions to catch
        
    Returns:
        Function result or None if all attempts failed
    """
    current_delay = delay
    
    for attempt in range(max_attempts):
        try:
            return func()
        except exceptions as e:
            if attempt == max_attempts - 1:
                return None
            
            time.sleep(current_delay)
            current_delay *= backoff
    
    return None


def sliding_window(
    items: List[T],
    window_size: int,
    step: int = 1
) -> Iterator[List[T]]:
    """Create sliding windows over a list.
    
    Args:
        items: List to window
        window_size: Size of each window
        step: Step size between windows
        
    Yields:
        Windows of items
    """
    for i in range(0, len(items) - window_size + 1, step):
        yield items[i:i + window_size]


def window_with_index(
    items: List[T],
    window_size: int
) -> Iterator[Tuple[int, List[T]]]:
    """Create windows with their starting index.
    
    Args:
        items: List to window
        window_size: Size of each window
        
    Yields:
        Tuples of (start_index, window)
    """
    for i in range(0, len(items) - window_size + 1):
        yield i, items[i:i + window_size]


def group_by(
    items: List[T],
    key_func: Callable[[T], Any]
) -> Dict[Any, List[T]]:
    """Group items by a key function.
    
    Args:
        items: Items to group
        key_func: Function to extract key from item
        
    Returns:
        Dictionary mapping keys to lists of items
    """
    groups: Dict[Any, List[T]] = {}
    
    for item in items:
        key = key_func(item)
        if key not in groups:
            groups[key] = []
        groups[key].append(item)
    
    return groups


def distribute_work(
    items: List[T],
    num_workers: int
) -> List[List[T]]:
    """Distribute items evenly across workers.
    
    Args:
        items: Items to distribute
        num_workers: Number of workers
        
    Returns:
        List of item lists, one per worker
    """
    if num_workers <= 0:
        return []
    
    if len(items) == 0:
        return [[] for _ in range(num_workers)]
    
    # Calculate items per worker
    base_count = len(items) // num_workers
    remainder = len(items) % num_workers
    
    distributions = []
    index = 0
    
    for worker in range(num_workers):
        count = base_count + (1 if worker < remainder else 0)
        distributions.append(items[index:index + count])
        index += count
    
    return distributions


class RateLimiter:
    """Rate limiter for controlling iteration speed."""
    
    def __init__(self, max_per_second: float):
        """Initialize rate limiter.
        
        Args:
            max_per_second: Maximum operations per second
        """
        self.max_per_second = max_per_second
        self.min_interval = 1.0 / max_per_second if max_per_second > 0 else 0
        self._last_time = 0.0
    
    def wait(self) -> None:
        """Wait if necessary to maintain rate limit."""
        if self.min_interval <= 0:
            return
        
        current = time.time()
        elapsed = current - self._last_time
        
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        
        self._last_time = time.time()
    
    def __call__(self, func: Callable[[T], R]) -> Callable[[T], R]:
        """Decorator to rate-limit a function."""
        def wrapper(item: T) -> R:
            self.wait()
            return func(item)
        return wrapper


def throttle(
    items: List[T],
    process_func: Callable[[T], Any],
    max_per_second: float,
    max_total_time: Optional[float] = None
) -> List[Any]:
    """Process items with rate limiting.
    
    Args:
        items: Items to process
        process_func: Processing function
        max_per_second: Maximum items per second
        max_total_time: Optional maximum total time
        
    Returns:
        List of results
    """
    limiter = RateLimiter(max_per_second)
    results = []
    start_time = time.time()
    
    for item in items:
        if max_total_time and (time.time() - start_time) > max_total_time:
            break
        limiter.wait()
        try:
            results.append(process_func(item))
        except Exception:
            results.append(None)
    
    return results


def iterate_with_progress(
    items: List[T],
    callback: Optional[Callable[[int, int, T], None]] = None
) -> Iterator[T]:
    """Iterate with progress reporting.
    
    Args:
        items: Items to iterate
        callback: Optional callback(index, total, item)
        
    Yields:
        Items with progress reporting
    """
    total = len(items)
    for i, item in enumerate(items):
        if callback:
            callback(i, total, item)
        yield item


def drain_iterator(
    iterator: Iterator[T],
    max_items: Optional[int] = None
) -> List[T]:
    """Drain an iterator into a list.
    
    Args:
        iterator: Iterator to drain
        max_items: Maximum items to drain
        
    Returns:
        List of items
    """
    items = []
    
    for i, item in enumerate(iterator):
        if max_items and i >= max_items:
            break
        items.append(item)
    
    return items


def round_robin(*iterables: List[List[T]]) -> Iterator[T]:
    """Round-robin through multiple iterables.
    
    Args:
        *iterables: Multiple lists to interleave
        
    Yields:
        Items in round-robin order
    """
    iterators = [iter(it) for it in iterables]
    running = True
    
    while running:
        running = False
        for it in iterators:
            try:
                item = next(it)
                yield item
                running = True
            except StopIteration:
                pass


def flatten(nested: List[List[T]]) -> List[T]:
    """Flatten a nested list.
    
    Args:
        nested: Nested list structure
        
    Returns:
        Flattened list
    """
    result = []
    for item in nested:
        if isinstance(item, list):
            result.extend(flatten(item))
        else:
            result.append(item)
    return result


def batch_with_remainder(
    items: List[T],
    batch_size: int
) -> Tuple[List[List[T]], List[T]]:
    """Split items into full batches and remainder.
    
    Args:
        items: Items to batch
        batch_size: Size of each batch
        
    Returns:
        Tuple of (full_batches, remainder)
    """
    full_batches = []
    remainder = []
    
    for i in range(0, len(items), batch_size):
        chunk = items[i:i + batch_size]
        if len(chunk) == batch_size:
            full_batches.append(chunk)
        else:
            remainder = chunk
    
    return full_batches, remainder


def interleave(
    list1: List[T],
    list2: List[T],
    interleaved: bool = True
) -> List[T]:
    """Interleave or concatenate two lists.
    
    Args:
        list1: First list
        list2: Second list
        interleaved: If True, alternate items; if False, concatenate
        
    Returns:
        Resulting list
    """
    if not interleaved:
        return list1 + list2
    
    result = []
    max_len = max(len(list1), len(list2))
    
    for i in range(max_len):
        if i < len(list1):
            result.append(list1[i])
        if i < len(list2):
            result.append(list2[i])
    
    return result
