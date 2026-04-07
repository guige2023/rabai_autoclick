"""Pytest plugin utilities: hooks, fixtures, markers, and custom assertions."""

from __future__ import annotations

import time
from typing import Any, Callable

__all__ = [
    "register_plugin",
    "pytest_configure",
    "pytest_collection_modifyitems",
    "retry_marker",
    "slow_marker",
    "integration_marker",
    "SnapshotAssertion",
    "assert_snapshot",
    "register_fixture",
]


def register_plugin() -> dict[str, Any]:
    """Return pytest plugin registration dict."""
    return {
        "hooks": {
            "pytest_configure": pytest_configure,
            "pytest_collection_modifyitems": pytest_collection_modifyitems,
            "pytest_runtest_logreport": pytest_runtest_logreport,
        },
        "fixtures": {},
    }


def pytest_configure(config: Any) -> None:
    """Register custom markers."""
    config.addinivalue_line("markers", "slow: marks tests as slow")
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "retry: marks tests that should be retried on failure")
    config.addinivalue_line("markers", "snapshot: marks tests that use snapshot assertions")


def pytest_collection_modifyitems(config: Any, items: list[Any]) -> None:
    """Modify test collection - add markers based on naming conventions."""
    for item in items:
        if "slow" in item.nodeid.lower():
            item.add_marker("slow")
        if "integration" in item.nodeid.lower():
            item.add_marker("integration")
        if "snapshot" in item.nodeid.lower():
            item.add_marker("snapshot")


def pytest_runtest_logreport(report: Any) -> None:
    """Hook to capture test results for reporting."""
    if report.when == "call":
        pass


def slow_marker(func: Callable) -> Callable:
    """Decorator to mark a test as slow."""
    if hasattr(func, "pytestmark"):
        func.pytestmark.append("slow")
    else:
        func.pytestmark = ["slow"]  # type: ignore
    return func


def integration_marker(func: Callable) -> Callable:
    """Decorator to mark a test as an integration test."""
    if hasattr(func, "pytestmark"):
        func.pytestmark.append("integration")
    else:
        func.pytestmark = ["integration"]  # type: ignore
    return func


def retry_marker(max_attempts: int = 3):
    """Decorator to retry a test on failure."""
    def decorator(func: Callable) -> Callable:
        func._retry_max_attempts = max_attempts  # type: ignore
        if hasattr(func, "pytestmark"):
            func.pytestmark.append("retry")
        else:
            func.pytestmark = ["retry"]  # type: ignore
        return func
    return decorator


class SnapshotAssertion:
    """Snapshot assertion utility for tests."""

    def __init__(self, snapshot_dir: str = "__snapshots__") -> None:
        self.snapshot_dir = snapshot_dir
        self._cache: dict[str, Any] = {}

    def assert_match(self, actual: Any, name: str) -> None:
        """Assert actual value matches the stored snapshot."""
        import os
        import json

        os.makedirs(self.snapshot_dir, exist_ok=True)
        path = os.path.join(self.snapshot_dir, f"{name}.json")

        if os.path.exists(path):
            with open(path) as f:
                expected = json.load(f)
            if actual != expected:
                raise SnapshotMismatchError(name, expected, actual)
        else:
            with open(path, "w") as f:
                json.dump(actual, f, indent=2, default=str)

    def update(self, actual: Any, name: str) -> None:
        """Update snapshot with actual value."""
        import os
        import json

        os.makedirs(self.snapshot_dir, exist_ok=True)
        path = os.path.join(self.snapshot_dir, f"{name}.json")
        with open(path, "w") as f:
            json.dump(actual, f, indent=2, default=str)


class SnapshotMismatchError(AssertionError):
    """Raised when snapshot assertion fails."""

    def __init__(self, name: str, expected: Any, actual: Any) -> None:
        self.name = name
        self.expected = expected
        self.actual = actual
        super().__init__(f"Snapshot '{name}' mismatch.\nExpected: {expected}\nActual: {actual}")


def assert_snapshot(actual: Any, name: str, snapshot_dir: str = "__snapshots__") -> None:
    """Convenience function for snapshot assertions."""
    snap = SnapshotAssertion(snapshot_dir)
    snap.assert_match(actual, name)


def register_fixture(
    name: str,
    scope: str = "function",
) -> Callable[[Callable], Callable]:
    """Decorator to register a pytest fixture."""
    def decorator(func: Callable) -> Callable:
        func._pytest_fixture = True  # type: ignore
        func._fixture_scope = scope  # type: ignore
        func._fixture_name = name  # type: ignore
        return func
    return decorator


class TimerFixture:
    """Fixture that tracks test execution time."""

    def __init__(self) -> None:
        self.start_time: float = 0.0
        self.end_time: float | None = None

    def start(self) -> None:
        self.start_time = time.perf_counter()

    def stop(self) -> float:
        self.end_time = time.perf_counter()
        return self.elapsed()

    def elapsed(self) -> float:
        if self.end_time:
            return self.end_time - self.start_time
        return time.perf_counter() - self.start_time
