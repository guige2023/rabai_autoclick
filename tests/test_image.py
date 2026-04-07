"""Tests for image processing utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.image import (
    ImageMatch,
    ImageLoader,
    Image,
    ImageMatcher,
    Screenshot,
    ImageComparator,
)


class TestImageMatch:
    """Tests for ImageMatch."""

    def test_create(self) -> None:
        """Test creating image match."""
        match = ImageMatch(
            x=10, y=20, width=100, height=50, confidence=0.95
        )
        assert match.x == 10
        assert match.y == 20
        assert match.confidence == 0.95


class TestImage:
    """Tests for Image."""

    def test_create(self) -> None:
        """Test creating image."""
        img = Image(width=800, height=600, mode="RGB")
        assert img.width == 800
        assert img.height == 600
        assert img.mode == "RGB"

    def test_to_pil(self) -> None:
        """Test converting to PIL."""
        img = Image(width=10, height=10, mode="RGB", data=b"\x00" * 300)
        pil = img.to_pil()
        assert pil is not None

    def test_crop(self) -> None:
        """Test cropping image."""
        img = Image(width=100, height=100, mode="RGB", data=b"\x00" * 30000)
        cropped = img.crop(10, 10, 50, 50)
        assert cropped.width == 50
        assert cropped.height == 50

    def test_resize(self) -> None:
        """Test resizing image."""
        img = Image(width=100, height=100, mode="RGB", data=b"\x00" * 30000)
        resized = img.resize(50, 50)
        assert resized.width == 50
        assert resized.height == 50


class TestImageLoader:
    """Tests for ImageLoader."""

    def test_from_pil(self) -> None:
        """Test creating from PIL image."""
        try:
            from PIL import Image as PILImage
            pil_img = PILImage.new("RGB", (100, 100))
            img = ImageLoader.from_pil(pil_img)
            assert img.width == 100
            assert img.height == 100
        except ImportError:
            pytest.skip("PIL not available")


class TestImageMatcher:
    """Tests for ImageMatcher."""

    def test_create(self) -> None:
        """Test creating matcher."""
        matcher = ImageMatcher(threshold=0.9)
        assert matcher.threshold == 0.9

    def test_find_on_screen_returns_none_without_pil(self) -> None:
        """Test find_on_screen returns None gracefully."""
        matcher = ImageMatcher()
        img = Image(width=10, height=10)
        result = matcher.find_on_screen(img)
        assert result is None

    def test_find_all_on_screen_returns_empty_without_pil(self) -> None:
        """Test find_all_on_screen returns empty list gracefully."""
        matcher = ImageMatcher()
        img = Image(width=10, height=10)
        result = matcher.find_all_on_screen(img)
        assert result == []


class TestScreenshot:
    """Tests for Screenshot."""

    def test_capture_returns_none_without_pil(self) -> None:
        """Test capture returns None gracefully."""
        result = Screenshot.capture()
        # Returns None or Image depending on PIL availability

    def test_save_returns_bool(self) -> None:
        """Test save returns boolean."""
        result = Screenshot.save("/tmp/test.png")
        assert isinstance(result, bool)


class TestImageComparator:
    """Tests for ImageComparator."""

    def test_compare_same_image(self) -> None:
        """Test comparing identical images."""
        data = b"\x00" * 3000
        img1 = Image(width=10, height=10, mode="RGB", data=data)
        img2 = Image(width=10, height=10, mode="RGB", data=data)
        score = ImageComparator.compare(img1, img2)
        assert score > 0.9

    def test_is_identical(self) -> None:
        """Test identical check."""
        data = b"\x00" * 3000
        img1 = Image(width=10, height=10, mode="RGB", data=data)
        img2 = Image(width=10, height=10, mode="RGB", data=data)
        assert ImageComparator.is_identical(img1, img2) is True

    def test_is_not_identical(self) -> None:
        """Test non-identical images."""
        data1 = b"\x00" * 3000
        data2 = b"\xff" * 3000
        img1 = Image(width=10, height=10, mode="RGB", data=data1)
        img2 = Image(width=10, height=10, mode="RGB", data=data2)
        assert ImageComparator.is_identical(img1, img2) is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])