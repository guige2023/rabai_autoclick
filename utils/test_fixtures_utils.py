"""Test fixtures and data factories for unit/integration testing."""

from __future__ import annotations

import json
import random
import string
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Generator, Generic, TypeVar

__all__ = [
    "FixtureFactory",
    "FixtureContext",
    "register_fixture",
    "tmpdir_fixture",
    "timer_fixture",
]


T = TypeVar("T")


@dataclass
class Fixture(Generic[T]):
    """A test fixture with setup/teardown."""
    name: str
    factory: Callable[[], T]
    cleanup: Callable[[T], None] | None = None


class FixtureRegistry:
    """Registry for test fixtures."""

    def __init__(self) -> None:
        self._fixtures: dict[str, Fixture[Any]] = {}

    def register(
        self,
        name: str,
        factory: Callable[[], T],
        cleanup: Callable[[T], None] | None = None,
    ) -> None:
        self._fixtures[name] = Fixture(name, factory, cleanup)

    def get(self, name: str) -> Any:
        fixture = self._fixtures[name]
        return fixture.factory()

    def cleanup_all(self) -> None:
        for fixture in self._fixtures.values():
            if fixture.cleanup:
                try:
                    fixture.cleanup(fixture.factory())
                except Exception:
                    pass


class FixtureFactory:
    """Factory for generating test data: users, posts, orders, etc."""

    @staticmethod
    def random_string(length: int = 10, charset: str | None = None) -> str:
        charset = charset or string.ascii_letters + string.digits
        return "".join(random.choices(charset, k=length))

    @staticmethod
    def random_email() -> str:
        name = FixtureFactory.random_string(8).lower()
        domains = ["example.com", "test.org", "demo.io"]
        return f"{name}@{random.choice(domains)}"

    @staticmethod
    def random_uuid() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def random_int(min_val: int = 0, max_val: int = 1000) -> int:
        return random.randint(min_val, max_val)

    @staticmethod
    def random_float(min_val: float = 0.0, max_val: float = 1000.0) -> float:
        return random.uniform(min_val, max_val)

    @staticmethod
    def random_bool() -> bool:
        return random.choice([True, False])

    @staticmethod
    def random_date(
        start_days_ago: int = 365,
        end_days_ahead: int = 0,
    ) -> datetime:
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=start_days_ago)
        end = now + timedelta(days=end_days_ahead)
        delta = end - start
        return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

    @staticmethod
    def random_choice(choices: list[T]) -> T:
        return random.choice(choices)

    @staticmethod
    def build_user(**overrides: Any) -> dict[str, Any]:
        """Generate a fake user dict."""
        return {
            "id": FixtureFactory.random_uuid(),
            "name": FixtureFactory.random_string(12),
            "email": FixtureFactory.random_email(),
            "age": FixtureFactory.random_int(18, 80),
            "is_active": FixtureFactory.random_bool(),
            "created_at": FixtureFactory.random_date().isoformat(),
            "tags": [FixtureFactory.random_string(5) for _ in range(FixtureFactory.random_int(1, 5))],
            **overrides,
        }

    @staticmethod
    def build_users(count: int, **overrides: Any) -> list[dict[str, Any]]:
        return [FixtureFactory.build_user(**overrides) for _ in range(count)]

    @staticmethod
    def build_order(**overrides: Any) -> dict[str, Any]:
        return {
            "id": FixtureFactory.random_uuid(),
            "user_id": FixtureFactory.random_uuid(),
            "amount": round(FixtureFactory.random_float(1.0, 500.0), 2),
            "currency": FixtureFactory.random_choice(["USD", "EUR", "CNY"]),
            "status": FixtureFactory.random_choice(["pending", "paid", "shipped", "delivered"]),
            "created_at": FixtureFactory.random_date().isoformat(),
            **overrides,
        }

    @staticmethod
    def build_orders(count: int, **overrides: Any) -> list[dict[str, Any]]:
        return [FixtureFactory.build_order(**overrides) for _ in range(count)]

    @staticmethod
    def build_json(count: int = 10) -> str:
        data = FixtureFactory.build_users(count)
        return json.dumps(data)

    @staticmethod
    def build_csv_row(columns: list[str]) -> str:
        def escape(v: Any) -> str:
            s = str(v)
            if "," in s or '"' in s or "\n" in s:
                return f'"{s.replace('"', '""')}"'
            return s
        return ",".join(escape(FixtureFactory.random_string(10)) for _ in columns)


class FixtureContext:
    """Context manager for test fixtures with auto-cleanup."""

    def __init__(self) -> None:
        self._created: list[tuple[Any, Callable[[Any], None] | None]] = []
        self._registry = FixtureRegistry()

    def register(
        self,
        name: str,
        factory: Callable[[], T],
        cleanup: Callable[[T], None] | None = None,
    ) -> T:
        obj = factory()
        self._created.append((obj, cleanup))
        return obj

    def cleanup(self) -> None:
        for obj, cleanup in reversed(self._created):
            if cleanup:
                try:
                    cleanup(obj)
                except Exception:
                    pass


@contextmanager
def tmpdir_fixture(name: str = "tmp") -> Generator[str, None, None]:
    """Create a temporary directory for tests."""
    import tempfile
    tmp = tempfile.mkdtemp(prefix=f"{name}_")
    try:
        yield tmp
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


@contextmanager
def timer_fixture() -> Generator[dict[str, float], None, None]:
    """Time the execution of a test block."""
    import time
    start = time.perf_counter()
    result: dict[str, float] = {}
    try:
        yield result
    finally:
        result["elapsed_ms"] = (time.perf_counter() - start) * 1000


_global_registry = FixtureRegistry()


def register_fixture(
    name: str,
    factory: Callable[[], T],
    cleanup: Callable[[T], None] | None = None,
) -> None:
    _global_registry.register(name, factory, cleanup)
