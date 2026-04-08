"""Batch input operations for multiple actions in sequence."""

from typing import List, Callable, Any, Optional, Dict
import time


class InputBatch:
    """Container for batched input operations."""

    def __init__(self, delay_between: float = 0.05):
        """Initialize batch processor.
        
        Args:
            delay_between: Delay in seconds between operations.
        """
        self.delay_between = delay_between
        self.operations: List[Dict[str, Any]] = []

    def add_click(self, x: int, y: int, button: str = "left") -> "InputBatch":
        """Add a click operation to the batch."""
        self.operations.append({
            "type": "click",
            "x": x,
            "y": y,
            "button": button,
        })
        return self

    def add_drag(
        self,
        x1: int, y1: int,
        x2: int, y2: int,
        duration: float = 0.5
    ) -> "InputBatch":
        """Add a drag operation to the batch."""
        self.operations.append({
            "type": "drag",
            "x1": x1,
            "y1": y1,
            "x2": x2,
            "y2": y2,
            "duration": duration,
        })
        return self

    def add_key(self, key: str, action: str = "press") -> "InputBatch":
        """Add a key operation to the batch."""
        self.operations.append({
            "type": "key",
            "key": key,
            "action": action,
        })
        return self

    def add_type(self, text: str, interval: float = 0.05) -> "InputBatch":
        """Add a type text operation to the batch."""
        self.operations.append({
            "type": "type",
            "text": text,
            "interval": interval,
        })
        return self

    def add_wait(self, duration: float) -> "InputBatch":
        """Add a wait operation to the batch."""
        self.operations.append({
            "type": "wait",
            "duration": duration,
        })
        return self

    def add_custom(
        self,
        func: Callable[[], Any],
        name: Optional[str] = None
    ) -> "InputBatch":
        """Add a custom function call to the batch."""
        self.operations.append({
            "type": "custom",
            "func": func,
            "name": name or func.__name__,
        })
        return self

    def execute(self, executor: Callable[[Dict], Any]) -> List[Any]:
        """Execute all operations using the provided executor.
        
        Args:
            executor: Function that takes an operation dict and executes it.
        
        Returns:
            List of results from each operation.
        """
        results = []
        for op in self.operations:
            result = executor(op)
            results.append(result)
            if op["type"] != "custom":
                time.sleep(self.delay_between)
        return results

    def __len__(self) -> int:
        return len(self.operations)


def execute_batch(
    operations: List[Dict[str, Any]],
    executor: Callable[[Dict], Any],
    delay_between: float = 0.05
) -> List[Any]:
    """Execute a list of operations through an executor function.
    
    Args:
        operations: List of operation dicts.
        executor: Function that executes a single operation.
        delay_between: Delay in seconds between operations.
    
    Returns:
        List of results.
    """
    batch = InputBatch(delay_between=delay_between)
    batch.operations = operations
    return batch.execute(executor)
