"""Tests for mutable utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.mutable import Mutable, MutableList, MutableDict, Lazy, Ref


class TestMutable:
    """Tests for Mutable."""

    def test_initial_value(self) -> None:
        """Test initial value."""
        m = Mutable(5)
        assert m.value == 5

    def test_set_value(self) -> None:
        """Test setting value."""
        m = Mutable(5)
        m.value = 10
        assert m.value == 10

    def test_get_set_methods(self) -> None:
        """Test get and set methods."""
        m = Mutable(5)
        assert m.get() == 5
        m.set(20)
        assert m.get() == 20

    def test_mutate(self) -> None:
        """Test mutate method."""
        m = Mutable(5)
        m.mutate(lambda x: x * 2)
        assert m.value == 10

    def test_equality(self) -> None:
        """Test equality."""
        m1 = Mutable(5)
        m2 = Mutable(5)
        m3 = Mutable(10)

        assert m1 == m2
        assert m1 != m3


class TestMutableList:
    """Tests for MutableList."""

    def test_append(self) -> None:
        """Test append."""
        mlist = MutableList()
        mlist.append(1)
        assert len(mlist) == 1

    def test_on_change(self) -> None:
        """Test change callback."""
        mlist = MutableList()
        changes = []

        def on_change(items):
            changes.append(len(items))

        mlist.on_change(on_change)
        mlist.append(1)
        mlist.append(2)

        assert len(changes) == 2


class TestMutableDict:
    """Tests for MutableDict."""

    def test_setitem(self) -> None:
        """Test setting item."""
        md = MutableDict()
        md["key"] = "value"
        assert md["key"] == "value"

    def test_on_change(self) -> None:
        """Test change callback."""
        md = MutableDict()
        changes = []

        def on_change(d):
            changes.append(dict(d))

        md.on_change(on_change)
        md["key"] = "value"

        assert len(changes) == 1


class TestLazy:
    """Tests for Lazy."""

    def test_lazy_evaluation(self) -> None:
        """Test lazy evaluation."""
        computed = [False]

        def factory():
            computed[0] = True
            return 42

        lazy_val = Lazy(factory)
        assert not computed[0]

        result = lazy_val.value
        assert computed[0]
        assert result == 42


class TestRef:
    """Tests for Ref."""

    def test_basic(self) -> None:
        """Test basic Ref operations."""
        r = Ref(5)
        assert r.get == 5

    def test_map(self) -> None:
        """Test map operation."""
        r = Ref(5)
        r2 = r.map(lambda x: x * 2)
        assert r2.get == 10


if __name__ == "__main__":
    pytest.main([__file__, "-v"])