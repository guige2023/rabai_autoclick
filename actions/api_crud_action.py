"""
API CRUD Action Module.

Generic CRUD (Create, Read, Update, Delete) operations for REST APIs
 with automatic pagination and filtering support.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CRUDOperation(Enum):
    """CRUD operation type."""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LIST = "list"
    SEARCH = "search"


@dataclass
class PaginationConfig:
    """Pagination configuration."""
    page_size: int = 20
    max_pages: int = 100
    cursor_field: Optional[str] = None
    offset_field: Optional[str] = None


@dataclass
class FilterConfig:
    """Filter configuration for list/search operations."""
    field: str
    operator: str = "eq"
    value: Any = None


@dataclass
class SortConfig:
    """Sort configuration."""
    field: str
    direction: str = "asc"


@dataclass
class CRUDResult:
    """Result of a CRUD operation."""
    success: bool
    operation: CRUDOperation
    data: Any = None
    total_count: Optional[int] = None
    page: Optional[int] = None
    page_size: Optional[int] = None
    error: Optional[str] = None


class APICRUDAction(Generic[T]):
    """
    Generic CRUD operation handler for REST APIs.

    Provides consistent create, read, update, delete, list, and search
    operations with automatic pagination and filtering.

    Example:
        crud = APICRUDAction(api_client)
        result = await crud.create({"name": "John", "email": "john@example.com"})
        result = await crud.list(filters=[FilterConfig("status", "eq", "active")])
        result = await crud.update("123", {"name": "Jane"})
    """

    def __init__(
        self,
        api_client: Any,
        resource_endpoint: str,
        id_field: str = "id",
        pagination: Optional[PaginationConfig] = None,
    ) -> None:
        self.api_client = api_client
        self.resource_endpoint = resource_endpoint.rstrip("/")
        self.id_field = id_field
        self.pagination = pagination or PaginationConfig()

    async def create(
        self,
        data: dict[str, Any],
        params: Optional[dict[str, Any]] = None,
    ) -> CRUDResult:
        """Create a new resource."""
        try:
            response = await self.api_client.request(
                method="POST",
                url=self.resource_endpoint,
                json=data,
                params=params,
            )
            return CRUDResult(
                success=True,
                operation=CRUDOperation.CREATE,
                data=response,
            )
        except Exception as e:
            logger.error(f"Create failed: {e}")
            return CRUDResult(
                success=False,
                operation=CRUDOperation.CREATE,
                error=str(e),
            )

    async def read(
        self,
        resource_id: str,
        params: Optional[dict[str, Any]] = None,
    ) -> CRUDResult:
        """Read a single resource by ID."""
        try:
            url = f"{self.resource_endpoint}/{resource_id}"
            response = await self.api_client.request(
                method="GET",
                url=url,
                params=params,
            )
            return CRUDResult(
                success=True,
                operation=CRUDOperation.READ,
                data=response,
            )
        except Exception as e:
            logger.error(f"Read failed: {e}")
            return CRUDResult(
                success=False,
                operation=CRUDOperation.READ,
                error=str(e),
            )

    async def update(
        self,
        resource_id: str,
        data: dict[str, Any],
        params: Optional[dict[str, Any]] = None,
    ) -> CRUDResult:
        """Update an existing resource."""
        try:
            url = f"{self.resource_endpoint}/{resource_id}"
            response = await self.api_client.request(
                method="PUT",
                url=url,
                json=data,
                params=params,
            )
            return CRUDResult(
                success=True,
                operation=CRUDOperation.UPDATE,
                data=response,
            )
        except Exception as e:
            logger.error(f"Update failed: {e}")
            return CRUDResult(
                success=False,
                operation=CRUDOperation.UPDATE,
                error=str(e),
            )

    async def patch(
        self,
        resource_id: str,
        data: dict[str, Any],
        params: Optional[dict[str, Any]] = None,
    ) -> CRUDResult:
        """Partially update a resource."""
        try:
            url = f"{self.resource_endpoint}/{resource_id}"
            response = await self.api_client.request(
                method="PATCH",
                url=url,
                json=data,
                params=params,
            )
            return CRUDResult(
                success=True,
                operation=CRUDOperation.UPDATE,
                data=response,
            )
        except Exception as e:
            logger.error(f"Patch failed: {e}")
            return CRUDResult(
                success=False,
                operation=CRUDOperation.UPDATE,
                error=str(e),
            )

    async def delete(
        self,
        resource_id: str,
        params: Optional[dict[str, Any]] = None,
    ) -> CRUDResult:
        """Delete a resource by ID."""
        try:
            url = f"{self.resource_endpoint}/{resource_id}"
            await self.api_client.request(
                method="DELETE",
                url=url,
                params=params,
            )
            return CRUDResult(
                success=True,
                operation=CRUDOperation.DELETE,
            )
        except Exception as e:
            logger.error(f"Delete failed: {e}")
            return CRUDResult(
                success=False,
                operation=CRUDOperation.DELETE,
                error=str(e),
            )

    async def list(
        self,
        filters: Optional[list[FilterConfig]] = None,
        sort: Optional[SortConfig] = None,
        page: int = 1,
        page_size: Optional[int] = None,
    ) -> CRUDResult:
        """List resources with optional filtering and sorting."""
        try:
            params: dict[str, Any] = {}

            if filters:
                for f in filters:
                    op_map = {"eq": "", "ne": "_ne", "gt": "_gt", "lt": "_lt"}
                    suffix = op_map.get(f.operator, "")
                    params[f"{f.field}{suffix}"] = f.value

            if sort:
                order_map = {"asc": "", "desc": "-"}
                prefix = order_map.get(sort.direction, "")
                params["sort"] = f"{prefix}{sort.field}"

            ps = page_size or self.pagination.page_size
            params["limit"] = ps
            params["offset"] = (page - 1) * ps

            response = await self.api_client.request(
                method="GET",
                url=self.resource_endpoint,
                params=params,
            )

            items = response if isinstance(response, list) else response.get("data", [])
            total = len(items)

            if isinstance(response, dict):
                total = response.get("total", len(items))

            return CRUDResult(
                success=True,
                operation=CRUDOperation.LIST,
                data=items,
                total_count=total,
                page=page,
                page_size=ps,
            )

        except Exception as e:
            logger.error(f"List failed: {e}")
            return CRUDResult(
                success=False,
                operation=CRUDOperation.LIST,
                error=str(e),
            )

    async def search(
        self,
        query: str,
        fields: Optional[list[str]] = None,
        page: int = 1,
        page_size: Optional[int] = None,
    ) -> CRUDResult:
        """Search resources by query string."""
        try:
            params: dict[str, Any] = {"q": query}

            if fields:
                params["fields"] = ",".join(fields)

            ps = page_size or self.pagination.page_size
            params["limit"] = ps
            params["offset"] = (page - 1) * ps

            response = await self.api_client.request(
                method="GET",
                url=f"{self.resource_endpoint}/search",
                params=params,
            )

            items = response if isinstance(response, list) else response.get("data", [])
            total = len(items)

            if isinstance(response, dict):
                total = response.get("total", len(items))

            return CRUDResult(
                success=True,
                operation=CRUDOperation.SEARCH,
                data=items,
                total_count=total,
                page=page,
                page_size=ps,
            )

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return CRUDResult(
                success=False,
                operation=CRUDOperation.SEARCH,
                error=str(e),
            )

    async def batch_create(
        self,
        items: list[dict[str, Any]],
    ) -> list[CRUDResult]:
        """Create multiple resources in batch."""
        tasks = [self.create(item) for item in items]
        return await asyncio.gather(*tasks)

    async def batch_delete(
        self,
        resource_ids: list[str],
    ) -> list[CRUDResult]:
        """Delete multiple resources in batch."""
        tasks = [self.delete(id) for id in resource_ids]
        return await asyncio.gather(*tasks)


import asyncio
