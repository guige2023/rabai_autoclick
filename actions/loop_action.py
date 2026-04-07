"""Loop control action for iteration management.

This module provides loop operations including
iteration, batching, and chunking of data.

Example:
    >>> action = LoopAction()
    >>> result = action.execute(operation="range", start=0, end=10)
"""

from __future__ import annotations

from typing import Any, Callable, Optional


class LoopAction:
    """Loop control and iteration action.

    Provides iteration operations including
    range generation, batching, and iteration control.

    Example:
        >>> action = LoopAction()
        >>> result = action.execute(
        ...     operation="repeat",
        ...     value="x",
        ...     times=5
        ... )
    """

    def __init__(self) -> None:
        """Initialize loop action."""
        pass

    def execute(
        self,
        operation: str,
        start: int = 0,
        end: int = 0,
        value: Any = None,
        times: int = 1,
        data: Optional[list] = None,
        chunk_size: int = 1,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute loop operation.

        Args:
            operation: Operation (range, repeat, batch, etc.).
            start: Start value.
            end: End value.
            value: Value to repeat.
            times: Number of times.
            data: List to process.
            chunk_size: Size of chunks.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "range":
            step = kwargs.get("step", 1)
            result["items"] = list(range(start, end + 1, step))
            result["count"] = len(result["items"])

        elif op == "range_exclusive":
            step = kwargs.get("step", 1)
            result["items"] = list(range(start, end, step))
            result["count"] = len(result["items"])

        elif op == "repeat":
            result["items"] = [value] * times
            result["count"] = times

        elif op == "cycle":
            if not data:
                raise ValueError("data required for 'cycle'")
            result["items"] = []
            for _ in range(times):
                result["items"].extend(data)
            result["count"] = len(result["items"])

        elif op == "batch":
            if not data:
                raise ValueError("data required for 'batch'")
            result["batches"] = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
            result["count"] = len(result["batches"])

        elif op == "chunk":
            if not data:
                raise ValueError("data required for 'chunk'")
            result["chunks"] = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
            result["count"] = len(result["chunks"])

        elif op == "enumerate":
            if not data:
                raise ValueError("data required for 'enumerate'")
            result["items"] = list(enumerate(data))
            result["count"] = len(result["items"])

        elif op == "zip":
            other = kwargs.get("other", [])
            if not data:
                raise ValueError("data required for 'zip'")
            result["items"] = list(zip(data, other))
            result["count"] = len(result["items"])

        elif op == "product":
            other = kwargs.get("other", [])
            if not data:
                raise ValueError("data required for 'product'")
            import itertools
            result["items"] = list(itertools.product(data, other))
            result["count"] = len(result["items"])

        elif op == "combinations":
            if not data:
                raise ValueError("data required for 'combinations'")
            r = kwargs.get("r", 2)
            import itertools
            result["items"] = list(itertools.combinations(data, r))
            result["count"] = len(result["items"])

        elif op == "permutations":
            if not data:
                raise ValueError("data required for 'permutations'")
            r = kwargs.get("r", None)
            import itertools
            result["items"] = list(itertools.permutations(data, r))
            result["count"] = len(result["items"])

        elif op == "while":
            condition = kwargs.get("condition")
            max_iter = kwargs.get("max_iter", 1000)
            if not condition:
                raise ValueError("condition required for 'while'")
            items = []
            i = 0
            current = start
            while condition(current) and i < max_iter:
                items.append(current)
                current += 1
                i += 1
            result["items"] = items
            result["count"] = len(result["items"])

        elif op == "times":
            func = kwargs.get("func")
            if not func:
                raise ValueError("func required for 'times'")
            results = []
            for i in range(times):
                results.append(func(i))
            result["results"] = results
            result["count"] = len(results)

        elif op == "each":
            if not data:
                raise ValueError("data required for 'each'")
            func = kwargs.get("func")
            if func:
                result["results"] = [func(item) for item in data]
            else:
                result["results"] = data
            result["count"] = len(result["results"])

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result
