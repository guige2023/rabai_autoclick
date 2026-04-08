# Copyright (c) 2024. coded by claude
"""API Response Formatter Action Module.

Formats API responses with support for multiple output formats,
pagination, field filtering, and response envelope generation.
"""
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ResponseFormat(Enum):
    JSON = "json"
    XML = "xml"
    CSV = "csv"


@dataclass
class PaginationConfig:
    page: int = 1
    page_size: int = 20
    total_count: Optional[int] = None
    total_pages: Optional[int] = None


@dataclass
class ResponseEnvelope:
    success: bool
    data: Any
    error: Optional[Dict[str, Any]] = None
    pagination: Optional[PaginationConfig] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResponseFormattingConfig:
    format: ResponseFormat = ResponseFormat.JSON
    include_envelope: bool = True
    include_metadata: bool = True
    exclude_fields: List[str] = field(default_factory=list)
    pretty_print: bool = False


class APIResponseFormatter:
    def __init__(self, config: Optional[ResponseFormattingConfig] = None):
        self.config = config or ResponseFormattingConfig()

    def format_success(self, data: Any, pagination: Optional[PaginationConfig] = None, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if self.config.include_envelope:
            return {
                "success": True,
                "data": self._filter_fields(data),
                "pagination": self._format_pagination(pagination) if pagination else None,
                "metadata": metadata if self.config.include_metadata else None,
            }
        return self._filter_fields(data)

    def format_error(self, code: str, message: str, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if self.config.include_envelope:
            return {
                "success": False,
                "error": {
                    "code": code,
                    "message": message,
                    "details": details,
                },
            }
        return {
            "error": {
                "code": code,
                "message": message,
                "details": details,
            },
        }

    def paginate(self, items: List[Any], page: int = 1, page_size: int = 20, total_count: Optional[int] = None) -> tuple:
        start = (page - 1) * page_size
        end = start + page_size
        paginated_items = items[start:end]
        total_pages = (total_count or len(items) + page_size - 1) // page_size if total_count else None
        pagination = PaginationConfig(
            page=page,
            page_size=page_size,
            total_count=total_count or len(items),
            total_pages=total_pages,
        )
        return paginated_items, pagination

    def _filter_fields(self, data: Any) -> Any:
        if not self.config.exclude_fields:
            return data
        if isinstance(data, dict):
            return {k: self._filter_fields(v) for k, v in data.items() if k not in self.config.exclude_fields}
        elif isinstance(data, list):
            return [self._filter_fields(item) for item in data]
        return data

    def _format_pagination(self, pagination: Optional[PaginationConfig]) -> Optional[Dict[str, Any]]:
        if not pagination:
            return None
        return {
            "page": pagination.page,
            "page_size": pagination.page_size,
            "total_count": pagination.total_count,
            "total_pages": pagination.total_pages,
            "has_next": pagination.page < (pagination.total_pages or pagination.page),
            "has_prev": pagination.page > 1,
        }

    def format_paginated_response(self, items: List[Any], page: int = 1, page_size: int = 20, total_count: Optional[int] = None, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        paginated_items, pagination = self.paginate(items, page, page_size, total_count)
        return {
            "success": True,
            "data": self._filter_fields(paginated_items),
            "pagination": self._format_pagination(pagination),
            "metadata": metadata if self.config.include_metadata else None,
        }
