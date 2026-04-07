"""
SQLAlchemy utilities for ORM operations, query building, and session management.

Provides helpers for model definition, CRUD operations, relationship loading,
pagination, soft deletes, optimistic locking, and multi-tenant patterns.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Generator, Optional, Type, TypeVar

from sqlalchemy import (
    Column, DateTime, Boolean, Integer, String, Text, create_engine,
    select, update, delete, func, and_, or_, inspect,
)
from sqlalchemy.orm import (
    Session, sessionmaker, DeclarativeBase, Mapped, mapped_column,
    relationship, joinedload, selectinload, defer,
)
from sqlalchemy.pool import StaticPool, QueuePool

logger = logging.getLogger(__name__)

T = TypeVar("T", bound="BaseModel")


class BaseModel(DeclarativeBase):
    """Abstract base model with common fields."""

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)


class OrderDirection(Enum):
    ASC = auto()
    DESC = auto()


@dataclass
class PaginationParams:
    """Pagination parameters."""
    page: int = 1
    per_page: int = 20
    order_by: Optional[str] = None
    order_dir: OrderDirection = OrderDirection.DESC

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.per_page

    @property
    def limit(self) -> int:
        return self.per_page


@dataclass
class PaginatedResult:
    """Paginated query result."""
    items: list[Any]
    total: int
    page: int
    per_page: int
    pages: int

    @property
    def has_next(self) -> bool:
        return self.page < self.pages

    @property
    def has_prev(self) -> bool:
        return self.page > 1


class DatabaseManager:
    """Manages SQLAlchemy engine and session lifecycle."""

    def __init__(
        self,
        database_url: str = "sqlite:///./app.db",
        pool_size: int = 5,
        max_overflow: int = 10,
        echo: bool = False,
    ) -> None:
        if database_url.startswith("sqlite"):
            self.engine = create_engine(
                database_url,
                echo=echo,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        else:
            self.engine = create_engine(
                database_url,
                echo=echo,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_pre_ping=True,
            )
        self._session_factory = sessionmaker(
            bind=self.engine, expire_on_commit=False
        )

    def create_all(self) -> None:
        """Create all tables."""
        BaseModel.metadata.create_all(self.engine)

    def drop_all(self) -> None:
        """Drop all tables."""
        BaseModel.metadata.drop_all(self.engine)

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Provide a transactional scope for a session."""
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_session(self) -> Session:
        """Get a new session (caller must close)."""
        return self._session_factory()


class SoftDeleteMixin:
    """Mixin for soft delete pattern."""

    @classmethod
    def active(cls, session: Session) -> select:
        """Query only non-deleted records."""
        return select(cls).where(cls.is_deleted == False)  # noqa: E712

    def soft_delete(self, session: Session) -> None:
        """Mark record as deleted."""
        self.is_deleted = True
        session.add(self)


class CRUDMixin:
    """CRUD operations mixin for models."""

    @classmethod
    def create(cls: Type[T], session: Session, **kwargs: Any) -> T:
        """Create a new record."""
        instance = cls(**kwargs)
        session.add(instance)
        session.flush()
        return instance

    @classmethod
    def get(cls: Type[T], session: Session, record_id: int) -> Optional[T]:
        """Get a record by ID."""
        return session.get(cls, record_id)

    @classmethod
    def get_all(cls: Type[T], session: Session) -> list[T]:
        """Get all records."""
        return list(session.execute(select(cls)).scalars().all())

    @classmethod
    def update(cls: Type[T], session: Session, record_id: int, **kwargs: Any) -> Optional[T]:
        """Update a record by ID."""
        record = session.get(cls, record_id)
        if record:
            for key, value in kwargs.items():
                if hasattr(record, key):
                    setattr(record, key, value)
            session.flush()
        return record

    @classmethod
    def delete(cls: Type[T], session: Session, record_id: int) -> bool:
        """Delete a record by ID."""
        record = session.get(cls, record_id)
        if record:
            session.delete(record)
            session.flush()
            return True
        return False

    @classmethod
    def paginate(
        cls: Type[T],
        session: Session,
        params: PaginationParams,
        filters: Optional[list[Any]] = None,
    ) -> PaginatedResult:
        """Paginate query results."""
        query = select(cls)
        if filters:
            query = query.where(and_(*filters))

        count_query = select(func.count()).select_from(query.subquery())
        total = session.execute(count_query).scalar() or 0

        if params.order_by and hasattr(cls, params.order_by):
            col = getattr(cls, params.order_by)
            if params.order_dir == OrderDirection.DESC:
                query = query.order_by(col.desc())
            else:
                query = query.order_by(col.asc())

        query = query.offset(params.offset).limit(params.limit)
        items = list(session.execute(query).scalars().all())

        pages = (total + params.per_page - 1) // params.per_page if params.per_page > 0 else 0
        return PaginatedResult(
            items=items,
            total=total,
            page=params.page,
            per_page=params.per_page,
            pages=pages,
        )


class QueryBuilder:
    """Fluent query builder for SQLAlchemy."""

    def __init__(self, model: Type[T]) -> None:
        self._model = model
        self._query = select(model)
        self._filters: list[Any] = []
        self._eager_loads: list[Any] = []
        self._order_by_col: Optional[Any] = None
        self._order_dir: OrderDirection = OrderDirection.DESC
        self._limit_val: Optional[int] = None
        self._offset_val: Optional[int] = None

    def filter(self, *conditions: Any) -> "QueryBuilder":
        """Add filter conditions."""
        self._filters.extend(conditions)
        return self

    def filter_by(self, **kwargs: Any) -> "QueryBuilder":
        """Add equality filter conditions."""
        for key, value in kwargs.items():
            self._filters.append(getattr(self._model, key) == value)
        return self

    def search(self, column: Column, term: str) -> "QueryBuilder":
        """Add ILIKE/LIKE search filter."""
        self._filters.append(column.ilike(f"%{term}%"))
        return self

    def eager(self, *relationships: str) -> "QueryBuilder":
        """Add eager loading for relationships."""
        for rel in relationships:
            if "." in rel:
                parts = rel.split(".")
                self._eager_loads.append(joinedload(parts[0]).joinedload(parts[1]))
            else:
                self._eager_loads.append(joinedload(rel))
        return self

    def order_by(self, column: Column, direction: OrderDirection = OrderDirection.DESC) -> "QueryBuilder":
        """Set ordering."""
        self._order_by_col = column
        self._order_dir = direction
        return self

    def limit(self, n: int) -> "QueryBuilder":
        self._limit_val = n
        return self

    def offset(self, n: int) -> "QueryBuilder":
        self._offset_val = n
        return self

    def build(self) -> select:
        """Build the final query."""
        if self._filters:
            self._query = self._query.where(and_(*self._filters))
        if self._eager_loads:
            self._query = self._query.options(*self._eager_loads)
        if self._order_by_col:
            col = self._order_by_col
            if self._order_dir == OrderDirection.DESC:
                self._query = self._query.order_by(col.desc())
            else:
                self._query = self._query.order_by(col.asc())
        if self._limit_val is not None:
            self._query = self._query.limit(self._limit_val)
        if self._offset_val is not None:
            self._query = self._query.offset(self._offset_val)
        return self._query

    def paginate(self, session: Session, params: PaginationParams) -> PaginatedResult:
        """Execute paginated query."""
        self._query = self._query.where(and_(*self._filters)) if self._filters else self._query
        count_query = select(func.count()).select_from(self._query.subquery())
        total = session.execute(count_query).scalar() or 0

        if self._order_by_col:
            col = self._order_by_col
            self._query = self._query.order_by(col.desc() if self._order_dir == OrderDirection.DESC else col.asc())

        self._query = self._query.offset(params.offset).limit(params.limit)
        if self._eager_loads:
            self._query = self._query.options(*self._eager_loads)

        items = list(session.execute(self._query).scalars().all())
        pages = (total + params.per_page - 1) // params.per_page if params.per_page > 0 else 0
        return PaginatedResult(items=items, total=total, page=params.page, per_page=params.per_page, pages=pages)
