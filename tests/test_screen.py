"""Tests for screen region utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.screen import (
    Region,
    ScreenRegions,
    RegionMatcher,
    RegionCache,
    RegionSelector,
)


class TestRegion:
    """Tests for Region."""

    def test_create(self) -> None:
        """Test creating region."""
        r = Region(x=10, y=20, width=100, height=50)
        assert r.x == 10
        assert r.y == 20
        assert r.width == 100
        assert r.height == 50

    def test_left_right(self) -> None:
        """Test left and right edges."""
        r = Region(x=10, y=20, width=100, height=50)
        assert r.left == 10
        assert r.right == 110

    def test_top_bottom(self) -> None:
        """Test top and bottom edges."""
        r = Region(x=10, y=20, width=100, height=50)
        assert r.top == 20
        assert r.bottom == 70

    def test_center(self) -> None:
        """Test center calculation."""
        r = Region(x=0, y=0, width=100, height=100)
        cx, cy = r.center
        assert cx == 50
        assert cy == 50

    def test_contains(self) -> None:
        """Test point containment."""
        r = Region(x=0, y=0, width=100, height=100)
        assert r.contains(50, 50) is True
        assert r.contains(150, 150) is False

    def test_overlaps(self) -> None:
        """Test region overlap."""
        r1 = Region(x=0, y=0, width=100, height=100)
        r2 = Region(x=50, y=50, width=100, height=100)
        assert r1.overlaps(r2) is True

    def test_no_overlap(self) -> None:
        """Test non-overlapping regions."""
        r1 = Region(x=0, y=0, width=100, height=100)
        r2 = Region(x=200, y=200, width=100, height=100)
        assert r1.overlaps(r2) is False

    def test_intersection(self) -> None:
        """Test intersection."""
        r1 = Region(x=0, y=0, width=100, height=100)
        r2 = Region(x=50, y=50, width=100, height=100)
        inter = r1.intersection(r2)
        assert inter is not None
        assert inter.x == 50
        assert inter.y == 50
        assert inter.width == 50
        assert inter.height == 50

    def test_intersection_no_overlap(self) -> None:
        """Test intersection with no overlap."""
        r1 = Region(x=0, y=0, width=100, height=100)
        r2 = Region(x=200, y=200, width=100, height=100)
        assert r1.intersection(r2) is None

    def test_union(self) -> None:
        """Test union."""
        r1 = Region(x=0, y=0, width=100, height=100)
        r2 = Region(x=50, y=50, width=100, height=100)
        union = r1.union(r2)
        assert union.x == 0
        assert union.y == 0
        assert union.width == 150
        assert union.height == 150

    def test_scale(self) -> None:
        """Test scaling."""
        r = Region(x=50, y=50, width=100, height=100)
        scaled = r.scale(2.0)
        assert scaled.width == 200
        assert scaled.height == 200

    def test_translate(self) -> None:
        """Test translation."""
        r = Region(x=10, y=20, width=100, height=50)
        translated = r.translate(5, 10)
        assert translated.x == 15
        assert translated.y == 30
        assert translated.width == 100

    def test_to_tuple(self) -> None:
        """Test conversion to tuple."""
        r = Region(x=10, y=20, width=100, height=50)
        t = r.to_tuple()
        assert t == (10, 20, 100, 50)


class TestScreenRegions:
    """Tests for ScreenRegions."""

    def test_full_screen(self) -> None:
        """Test full screen region."""
        r = ScreenRegions.full_screen()
        assert r.width > 0
        assert r.height > 0

    def test_primary_monitor(self) -> None:
        """Test primary monitor region."""
        r = ScreenRegions.primary_monitor()
        assert r.width > 0
        assert r.height > 0


class TestRegionMatcher:
    """Tests for RegionMatcher."""

    def test_create(self) -> None:
        """Test creating matcher."""
        m = RegionMatcher(threshold=0.9)
        assert m.threshold == 0.9

    def test_find_region_no_template(self) -> None:
        """Test find with nonexistent template."""
        m = RegionMatcher()
        result = m.find_region("/nonexistent/template.png")
        assert result is None


class TestRegionCache:
    """Tests for RegionCache."""

    def test_create(self) -> None:
        """Test creating cache."""
        cache = RegionCache(max_size=5)
        assert cache._max_size == 5
        assert len(cache._cache) == 0

    def test_put_and_get(self) -> None:
        """Test putting and getting."""
        cache = RegionCache()
        r = Region(x=0, y=0, width=100, height=100)
        cache.put(r, "data")
        assert cache.get(r) == "data"

    def test_get_miss(self) -> None:
        """Test cache miss."""
        cache = RegionCache()
        r1 = Region(x=0, y=0, width=100, height=100)
        r2 = Region(x=200, y=200, width=100, height=100)
        cache.put(r1, "data")
        assert cache.get(r2) is None

    def test_clear(self) -> None:
        """Test clearing cache."""
        cache = RegionCache()
        r = Region(x=0, y=0, width=100, height=100)
        cache.put(r, "data")
        cache.clear()
        assert len(cache._cache) == 0


class TestRegionSelector:
    """Tests for RegionSelector."""

    def test_create(self) -> None:
        """Test creating selector."""
        s = RegionSelector()
        assert s._start_x is None

    def test_mouse_down(self) -> None:
        """Test mouse down."""
        s = RegionSelector()
        s.on_mouse_down(100, 200)
        assert s._start_x == 100
        assert s._start_y == 200

    def test_mouse_up(self) -> None:
        """Test mouse up."""
        s = RegionSelector()
        s.on_mouse_down(50, 50)
        region = s.on_mouse_up(150, 150)
        assert region is not None
        assert region.x == 50
        assert region.y == 50
        assert region.width == 100
        assert region.height == 100

    def test_reset(self) -> None:
        """Test reset."""
        s = RegionSelector()
        s.on_mouse_down(100, 200)
        s.reset()
        assert s._start_x is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])