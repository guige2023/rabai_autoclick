"""
API Bulk Action - Bulk API operations.

This module provides bulk API operation capabilities for
efficient batch processing of API requests.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class BulkOperation:
    """A single bulk operation."""
    operation_id: str
    method: str
    url: str
    body: Any = None


@dataclass
class BulkResult:
    """Result of bulk operation."""
    operation_id: str
    success: bool
    status_code: int | None = None
    data: Any = None
    error: str | None = None


class BulkAPIClient:
    """Client for bulk API operations."""
    
    def __init__(self, max_concurrent: int = 5) -> None:
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
    
    async def execute(
        self,
        operations: list[BulkOperation],
        executor: Callable[[BulkOperation], Any],
    ) -> list[BulkResult]:
        """Execute bulk operations."""
        results = []
        
        async def execute_with_semaphore(op: BulkOperation) -> BulkResult:
            async with self._semaphore:
                try:
                    result = await executor(op)
                    return result
                except Exception as e:
                    return BulkResult(operation_id=op.operation_id, success=False, error=str(e))
        
        tasks = [execute_with_semaphore(op) for op in operations]
        results = await asyncio.gather(*tasks)
        
        return list(results)


class APIBulkAction:
    """API bulk action for automation workflows."""
    
    def __init__(self, max_concurrent: int = 5) -> None:
        self.client = BulkAPIClient(max_concurrent)
    
    async def execute(
        self,
        operations: list[BulkOperation],
        executor: Callable[[BulkOperation], Any],
    ) -> list[BulkResult]:
        """Execute bulk operations."""
        return await self.client.execute(operations, executor)


__all__ = ["BulkOperation", "BulkResult", "BulkAPIClient", "APIBulkAction"]
