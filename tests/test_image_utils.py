"""Tests for image utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.image_utils import (
    ImageSize,
    load_image,
    save_image,
    get_image_size,
    resize_image,
    crop_image,
    grayscale,
    blur,
    threshold,
    find_edges,
    match_template,
    screenshot,
    save_screenshot,
    compare_images,
    find_template_on_screen,
)


class TestImageSize:
    """Tests for ImageSize."""

    def test_create(self) -> None:
        """Test creating ImageSize."""
        size = ImageSize(width=1920, height=1080)
        assert size.width == 1920
        assert size.height == 1080

    def test_aspect_ratio(self) -> None:
        """Test aspect ratio calculation."""
        size = ImageSize(width=1920, height=1080)
        assert size.aspect_ratio == pytest.approx(16/9, rel=0.01)

    def test_aspect_ratio_zero_height(self) -> None:
        """Test aspect ratio with zero height."""
        size = ImageSize(width=1920, height=0)
        assert size.aspect_ratio == 0.0

    def test_area(self) -> None:
        """Test area calculation."""
        size = ImageSize(width=1920, height=1080)
        assert size.area == 1920 * 1080


class TestLoadImage:
    """Tests for load_image."""

    def test_load_nonexistent(self) -> None:
        """Test loading nonexistent image."""
        result = load_image("/nonexistent/image.png")
        assert result is None


class TestSaveImage:
    """Tests for save_image."""

    def test_save_invalid(self) -> None:
        """Test saving invalid image."""
        result = save_image("/tmp/test.png", None)
        assert result is False


class TestGetImageSize:
    """Tests for get_image_size."""

    def test_get_size_invalid(self) -> None:
        """Test getting size of invalid image."""
        result = get_image_size(None)
        assert result is None


class TestResizeImage:
    """Tests for resize_image."""

    def test_resize_invalid(self) -> None:
        """Test resizing invalid image."""
        result = resize_image(None, width=100, height=100)
        assert result is None

    def test_resize_no_params(self) -> None:
        """Test resize with no params."""
        result = resize_image(None)
        assert result is None


class TestCropImage:
    """Tests for crop_image."""

    def test_crop_invalid(self) -> None:
        """Test cropping invalid image."""
        result = crop_image(None, 0, 0, 100, 100)
        assert result is None


class TestGrayscale:
    """Tests for grayscale."""

    def test_grayscale_invalid(self) -> None:
        """Test grayscaling invalid image."""
        result = grayscale(None)
        assert result is None


class TestBlur:
    """Tests for blur."""

    def test_blur_invalid(self) -> None:
        """Test blurring invalid image."""
        result = blur(None)
        assert result is None


class TestThreshold:
    """Tests for threshold."""

    def test_threshold_invalid(self) -> None:
        """Test thresholding invalid image."""
        result = threshold(None)
        assert result is None


class TestFindEdges:
    """Tests for find_edges."""

    def test_find_edges_invalid(self) -> None:
        """Test finding edges on invalid image."""
        result = find_edges(None)
        assert result is None


class TestMatchTemplate:
    """Tests for match_template."""

    def test_match_template_invalid(self) -> None:
        """Test matching with invalid images."""
        result = match_template(None, None)
        assert result == []


class TestScreenshot:
    """Tests for screenshot."""

    def test_screenshot_returns_none_or_array(self) -> None:
        """Test screenshot returns None or array."""
        # This may return None in headless environments
        result = screenshot()
        # Result is either None or a numpy array
        assert result is None or hasattr(result, 'shape')


class TestSaveScreenshot:
    """Tests for save_screenshot."""

    def test_save_screenshot_nonexistent(self) -> None:
        """Test saving screenshot to nonexistent path."""
        result = save_screenshot("/nonexistent/path/screenshot.png")
        assert result is False


class TestCompareImages:
    """Tests for compare_images."""

    def test_compare_invalid(self) -> None:
        """Test comparing invalid images."""
        result = compare_images(None, None)
        assert result == 0.0

    def test_compare_same(self) -> None:
        """Test comparing same invalid images."""
        result = compare_images(None, None)
        assert isinstance(result, float)


class TestFindTemplateOnScreen:
    """Tests for find_template_on_screen."""

    def test_find_template_invalid(self) -> None:
        """Test finding template on invalid screen."""
        result = find_template_on_screen("/nonexistent/template.png")
        assert result is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])