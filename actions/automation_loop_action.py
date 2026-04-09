"""
Automation Loop Action Module.

Provides loop constructs for automation workflows including for loops,
while loops, map, reduce, and parallel iteration patterns.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


T = TypeVar('T')
R = TypeVar('R')


class LoopType(Enum):
    """Types of loop constructs."""
    FOR = "for"
    WHILE = "while"
    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    PARALLEL_FOR = "parallel_for"
    DO_WHILE = "do_while"


@dataclass
class LoopConfig:
    """Configuration for loop execution."""
    loop_type: LoopType = LoopType.FOR
    max_iterations: int = 1000
    parallel_workers: int = 4
    break_condition: Optional[Callable] = None
    continue_condition: Optional[Callable] = None
    timeout: Optional[float] = None


@dataclass
class LoopResult:
    """Result of a loop execution."""
    loop_type: LoopType
    total_iterations: int
    successful_iterations: int
    failed_iterations: int
    results: List[Any]
    duration_seconds: float
    errors: List[str]
    broken: bool = False


class ForLoop:
    """
    Classic for loop iterator.
    
    Example:
        loop = ForLoop(items=[1, 2, 3, 4, 5])
        loop.each(lambda x: print(x * 2))
        result = loop.execute()
    """
    
    def __init__(self, items: List[T]):
        self.items = items
        self._action: Optional[Callable] = None
        self._filter_fn: Optional[Callable] = None
        self._max_iterations = 1000
        self._break_fn: Optional[Callable] = None
    
    def each(self, action: Callable[[T], Any]) -> "ForLoop":
        """Set action for each item."""
        self._action = action
        return self
    
    def filter(self, predicate: Callable[[T], bool]) -> "ForLoop":
        """Filter items before iteration."""
        self._filter_fn = predicate
        return self
    
    def take(self, count: int) -> "ForLoop":
        """Take only first n items."""
        self.items = self.items[:count]
        return self
    
    def skip(self, count: int) -> "ForLoop":
        """Skip first n items."""
        self.items = self.items[count:]
        return self
    
    def break_when(self, condition: Callable[[T, Any], bool]) -> "ForLoop":
        """Break when condition is met."""
        self._break_fn = condition
        return self
    
    def execute(self) -> LoopResult:
        """Execute the loop."""
        import time
        start_time = time.monotonic()
        
        if self._filter_fn:
            items = [x for x in self.items if self._filter_fn(x)]
        else:
            items = self.items
        
        results = []
        errors = []
        broken = False
        
        for i, item in enumerate(items[:self._max_iterations]):
            try:
                if self._action:
                    result = self._action(item)
                    results.append(result)
                
                # Check break condition
                if self._break_fn and self._break_fn(item, result if 'result' in locals() else None):
                    broken = True
                    break
            
            except Exception as e:
                errors.append(str(e))
        
        duration = time.monotonic() - start_time
        
        return LoopResult(
            loop_type=LoopType.FOR,
            total_iterations=len(items),
            successful_iterations=len(results),
            failed_iterations=len(errors),
            results=results,
            duration_seconds=duration,
            errors=errors,
            broken=broken
        )


class WhileLoop:
    """
    While loop construct.
    
    Example:
        loop = WhileLoop(lambda: count < 10)
        loop.body(lambda: increment_and_return(count))
        result = loop.execute()
    """
    
    def __init__(self, condition: Callable[[], bool]):
        self._condition = condition
        self._body: Optional[Callable] = None
        self._max_iterations = 1000
        self._iteration_count = 0
    
    def body(self, action: Callable[[], Any]) -> "WhileLoop":
        """Set body action."""
        self._body = action
        return self
    
    def max_iterations(self, count: int) -> "WhileLoop":
        """Set maximum iterations."""
        self._max_iterations = count
        return self
    
    def execute(self) -> LoopResult:
        """Execute the while loop."""
        import time
        start_time = time.monotonic()
        
        results = []
        errors = []
        
        self._iteration_count = 0
        
        while self._condition():
            if self._iteration_count >= self._max_iterations:
                break
            
            self._iteration_count += 1
            
            try:
                if self._body:
                    result = self._body()
                    results.append(result)
            except Exception as e:
                errors.append(str(e))
        
        duration = time.monotonic() - start_time
        
        return LoopResult(
            loop_type=LoopType.WHILE,
            total_iterations=self._iteration_count,
            successful_iterations=len(results),
            failed_iterations=len(errors),
            results=results,
            duration_seconds=duration,
            errors=errors
        )


class DoWhileLoop:
    """
    Do-while loop (executes at least once).
    
    Example:
        loop = DoWhileLoop(lambda: count < 10)
        loop.body(lambda: increment())
        result = loop.execute()
    """
    
    def __init__(self, condition: Callable[[], bool]):
        self._condition = condition
        self._body: Optional[Callable] = None
        self._max_iterations = 1000
        self._iteration_count = 0
    
    def body(self, action: Callable[[], Any]) -> "DoWhileLoop":
        """Set body action."""
        self._body = action
        return self
    
    def execute(self) -> LoopResult:
        """Execute the do-while loop."""
        import time
        start_time = time.monotonic()
        
        results = []
        errors = []
        self._iteration_count = 0
        
        while True:
            if self._iteration_count >= self._max_iterations:
                break
            
            self._iteration_count += 1
            
            try:
                if self._body:
                    result = self._body()
                    results.append(result)
            except Exception as e:
                errors.append(str(e))
            
            if not self._condition():
                break
        
        duration = time.monotonic() - start_time
        
        return LoopResult(
            loop_type=LoopType.DO_WHILE,
            total_iterations=self._iteration_count,
            successful_iterations=len(results),
            failed_iterations=len(errors),
            results=results,
            duration_seconds=duration,
            errors=errors
        )


class ParallelForLoop:
    """
    Parallel for loop using thread pool.
    
    Example:
        loop = ParallelForLoop(items=[1, 2, 3, 4], workers=4)
        loop.each(lambda x: expensive_operation(x))
        result = loop.execute()
    """
    
    def __init__(self, items: List[T], workers: int = 4):
        self.items = items
        self.workers = workers
        self._action: Optional[Callable] = None
        self._max_results = 1000
    
    def each(self, action: Callable[[T], Any]) -> "ParallelForLoop":
        """Set action for each item."""
        self._action = action
        return self
    
    def limit(self, count: int) -> "ParallelForLoop":
        """Limit number of parallel executions."""
        self.workers = min(count, self.workers)
        return self
    
    def execute(self) -> LoopResult:
        """Execute the parallel loop."""
        import time
        start_time = time.monotonic()
        
        results = []
        errors = []
        
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = []
            
            for item in self.items[:self._max_results]:
                if self._action:
                    future = executor.submit(self._action, item)
                    futures.append((item, future))
            
            for item, future in futures:
                try:
                    result = future.result(timeout=30)
                    results.append(result)
                except Exception as e:
                    errors.append(f"{item}: {str(e)}")
        
        duration = time.monotonic() - start_time
        
        return LoopResult(
            loop_type=LoopType.PARALLEL_FOR,
            total_iterations=len(self.items),
            successful_iterations=len(results),
            failed_iterations=len(errors),
            results=results,
            duration_seconds=duration,
            errors=errors
        )


class MapReduce:
    """
    Map-Reduce pattern implementation.
    
    Example:
        mr = MapReduce(data=[1, 2, 3, 4, 5])
        mr.mapper(lambda x: (x % 2, x))  # (key, value)
        mr.reducer(lambda key, values: sum(values))
        result = mr.execute()
    """
    
    def __init__(self, data: List[T]):
        self.data = data
        self._mapper: Optional[Callable] = None
        self._reducer: Optional[Callable] = None
        self._filter_fn: Optional[Callable] = None
    
    def mapper(self, func: Callable[[T], tuple]) -> "MapReduce":
        """Set mapper function that returns (key, value)."""
        self._mapper = func
        return self
    
    def reducer(self, func: Callable[[Any, List], Any]) -> "MapReduce":
        """Set reducer function that takes (key, values)."""
        self._reducer = func
        return self
    
    def filter(self, predicate: Callable[[T], bool]) -> "MapReduce":
        """Filter input data."""
        self._filter_fn = predicate
        return self
    
    def execute(self) -> Dict[str, Any]:
        """Execute map-reduce."""
        import time
        start_time = time.monotonic()
        
        # Filter
        data = self.data
        if self._filter_fn:
            data = [x for x in data if self._filter_fn(x)]
        
        # Map
        mapped = {}
        for item in data:
            if self._mapper:
                key, value = self._mapper(item)
                if key not in mapped:
                    mapped[key] = []
                mapped[key].append(value)
        
        # Reduce
        results = {}
        if self._reducer:
            for key, values in mapped.items():
                results[key] = self._reducer(key, values)
        else:
            results = mapped
        
        duration = time.monotonic() - start_time
        
        return {
            "results": results,
            "input_count": len(self.data),
            "output_count": len(results),
            "duration_seconds": duration
        }


class LoopExecutor:
    """
    Unified loop executor interface.
    
    Example:
        executor = LoopExecutor()
        executor.loop_type(LoopType.FOR)
        executor.items([1, 2, 3])
        executor.action(lambda x: x * 2)
        result = executor.execute()
    """
    
    def __init__(self):
        self._loop_type = LoopType.FOR
        self._items: List[Any] = []
        self._condition: Optional[Callable] = None
        self._action: Optional[Callable] = None
        self._workers = 4
        self._max_iterations = 1000
    
    def loop_type(self, loop_type: LoopType) -> "LoopExecutor":
        """Set loop type."""
        self._loop_type = loop_type
        return self
    
    def items(self, items: List[Any]) -> "LoopExecutor":
        """Set items to iterate over."""
        self._items = items
        return self
    
    def condition(self, cond: Callable) -> "LoopExecutor":
        """Set condition function."""
        self._condition = cond
        return self
    
    def action(self, action: Callable) -> "LoopExecutor":
        """Set action function."""
        self._action = action
        return self
    
    def workers(self, count: int) -> "LoopExecutor":
        """Set number of workers for parallel loops."""
        self._workers = count
        return self
    
    def max_iterations(self, count: int) -> "LoopExecutor":
        """Set maximum iterations."""
        self._max_iterations = count
        return self
    
    def execute(self) -> LoopResult:
        """Execute the loop."""
        if self._loop_type == LoopType.FOR:
            loop = ForLoop(self._items)
            if self._action:
                loop.each(self._action)
            loop._max_iterations = self._max_iterations
            return loop.execute()
        
        elif self._loop_type == LoopType.WHILE and self._condition:
            loop = WhileLoop(self._condition)
            if self._action:
                loop.body(self._action)
            loop.max_iterations(self._max_iterations)
            return loop.execute()
        
        elif self._loop_type == LoopType.DO_WHILE and self._condition:
            loop = DoWhileLoop(self._condition)
            if self._action:
                loop.body(self._action)
            return loop.execute()
        
        elif self._loop_type == LoopType.PARALLEL_FOR:
            loop = ParallelForLoop(self._items, self._workers)
            if self._action:
                loop.each(self._action)
            loop._max_results = self._max_iterations
            return loop.execute()
        
        elif self._loop_type == LoopType.MAP:
            loop = ParallelForLoop(self._items, self._workers)
            if self._action:
                loop.each(self._action)
            return loop.execute()
        
        else:
            return LoopResult(
                loop_type=self._loop_type,
                total_iterations=0,
                successful_iterations=0,
                failed_iterations=0,
                results=[],
                duration_seconds=0,
                errors=["Invalid loop configuration"]
            )


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class AutomationLoopAction(BaseAction):
    """
    Loop action for workflow iteration.
    
    Parameters:
        operation: Operation type (execute/map/reduce/parallel)
        loop_type: Type of loop (for/while/map/parallel_for)
        items: List of items to iterate
        action: Action function reference
        max_iterations: Maximum iterations
        workers: Number of parallel workers
    
    Example:
        action = AutomationLoopAction()
        result = action.execute({}, {
            "operation": "execute",
            "loop_type": "for",
            "items": [1, 2, 3, 4, 5]
        })
    """
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute loop operation."""
        operation = params.get("operation", "execute")
        loop_type_str = params.get("loop_type", "for")
        items = params.get("items", [])
        max_iterations = params.get("max_iterations", 1000)
        workers = params.get("workers", 4)
        
        loop_type = LoopType(loop_type_str)
        
        if operation == "execute":
            executor = LoopExecutor()
            executor.loop_type(loop_type)
            executor.items(items)
            executor.max_iterations(max_iterations)
            executor.workers(workers)
            
            result = executor.execute()
            
            return {
                "success": True,
                "operation": "execute",
                "loop_type": loop_type_str,
                "total_iterations": result.total_iterations,
                "successful_iterations": result.successful_iterations,
                "failed_iterations": result.failed_iterations,
                "broken": result.broken,
                "duration_seconds": result.duration_seconds
            }
        
        elif operation == "map":
            mr = MapReduce(items)
            
            def map_fn(x):
                return x  # Placeholder
            
            mr.mapper(map_fn)
            
            result = mr.execute()
            
            return {
                "success": True,
                "operation": "map",
                "results": result.get("results", {}),
                "duration_seconds": result.get("duration_seconds", 0)
            }
        
        elif operation == "parallel":
            loop = ParallelForLoop(items, workers)
            
            def action_fn(x):
                return x * 2
            
            loop.each(action_fn)
            result = loop.execute()
            
            return {
                "success": True,
                "operation": "parallel",
                "total_iterations": result.total_iterations,
                "successful_iterations": result.successful_iterations,
                "duration_seconds": result.duration_seconds
            }
        
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
