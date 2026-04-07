"""Tests for singleton utilities."""

import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.singleton import (
    singleton,
    singleton_with_lock,
    SingletonMeta,
    LazySingleton,
    ThreadLocalSingleton,
    threaded_singleton,
)


class TestSingletonDecorator:
    """Tests for singleton decorator."""

    def test_singleton_single_instance(self) -> None:
        """Test singleton returns same instance."""
        @singleton
        class MySingleton:
            def __init__(self):
                self.value = 42

        s1 = MySingleton()
        s2 = MySingleton()
        assert s1 is s2

    def test_singleton_shares_state(self) -> None:
        """Test singleton shares state."""
        @singleton
        class MySingleton:
            def __init__(self):
                self.counter = 0

        s1 = MySingleton()
        s1.counter += 1
        s2 = MySingleton()
        assert s2.counter == 1

    def test_singleton_preserves_class_name(self) -> None:
        """Test singleton preserves class name."""
        @singleton
        class MyClass:
            pass

        assert MyClass.__name__ == "MyClass"


class TestSingletonWithLock:
    """Tests for singleton_with_lock."""

    def test_custom_lock(self) -> None:
        """Test singleton with custom lock."""
        lock = threading.Lock()

        @singleton_with_lock(lock)
        class MySingleton:
            def __init__(self):
                self.value = 100

        s1 = MySingleton()
        s2 = MySingleton()
        assert s1 is s2


class TestSingletonMeta:
    """Tests for SingletonMeta."""

    def test_singleton_meta_single_instance(self) -> None:
        """Test SingletonMeta returns same instance."""
        class MySingleton(metaclass=SingletonMeta):
            def __init__(self):
                self.value = 42

        s1 = MySingleton()
        s2 = MySingleton()
        assert s1 is s2

    def test_singleton_meta_separate_classes(self) -> None:
        """Test separate classes have separate instances."""
        class SingletonA(metaclass=SingletonMeta):
            def __init__(self):
                self.value = 1

        class SingletonB(metaclass=SingletonMeta):
            def __init__(self):
                self.value = 2

        a = SingletonA()
        b = SingletonB()
        assert a is not b
        assert a.value == 1
        assert b.value == 2


class TestLazySingleton:
    """Tests for LazySingleton."""

    def test_creates_instance_on_first_access(self) -> None:
        """Test instance created on first access."""
        created = []

        def factory():
            created.append(1)
            return {"value": 42}

        lazy = LazySingleton(factory)
        assert len(created) == 0
        instance = lazy.get_instance()
        assert len(created) == 1
        assert instance["value"] == 42

    def test_same_instance_on_multiple_access(self) -> None:
        """Test same instance returned on multiple access."""
        lazy = LazySingleton(lambda: {"value": 42})
        instance1 = lazy.get_instance()
        instance2 = lazy.get_instance()
        assert instance1 is instance2

    def test_reset(self) -> None:
        """Test reset allows recreation."""
        call_count = [0]

        def factory():
            call_count[0] += 1
            return {"id": call_count[0]}

        lazy = LazySingleton(factory)
        instance1 = lazy.get_instance()
        lazy.reset()
        instance2 = lazy.get_instance()
        assert instance1 is not instance2
        assert call_count[0] == 2

    def test_callable(self) -> None:
        """Test lazy singleton is callable."""
        lazy = LazySingleton(lambda: 42)
        result = lazy()
        assert result == 42


class TestThreadLocalSingleton:
    """Tests for ThreadLocalSingleton."""

    def test_different_threads_different_instances(self) -> None:
        """Test different threads get different instances."""
        call_count = [0]
        instances = {}

        def factory():
            call_count[0] += 1
            return {"id": call_count[0]}

        tls = ThreadLocalSingleton(factory)

        def get_from_thread():
            instances[threading.current_thread().name] = tls.get_instance()

        threads = [threading.Thread(target=get_from_thread, name=f"t{i}") for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        unique_ids = {inst["id"] for inst in instances.values()}
        assert len(unique_ids) == 3  # Each thread got its own instance

    def test_callable(self) -> None:
        """Test thread local singleton is callable."""
        tls = ThreadLocalSingleton(lambda: 42)
        result = tls()
        assert result == 42


class TestThreadedSingleton:
    """Tests for threaded_singleton decorator."""

    def test_threaded_singleton_single_instance(self) -> None:
        """Test threaded singleton returns same instance."""
        @threaded_singleton
        class MySingleton:
            def __init__(self):
                self.value = 42

        s1 = MySingleton()
        s2 = MySingleton()
        assert s1 is s2

    def test_threaded_singleton_thread_safety(self) -> None:
        """Test threaded singleton is thread safe."""
        @threaded_singleton
        class Counter:
            def __init__(self):
                self.count = 0

            def increment(self):
                self.count += 1

        instances = []

        def get_instance():
            inst = Counter()
            instances.append(inst)

        threads = [threading.Thread(target=get_instance) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(inst is instances[0] for inst in instances)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])