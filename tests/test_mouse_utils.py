"""Tests for mouse utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.mouse_utils import (
    IS_MACOS,
    VALID_BUTTONS,
    macos_click,
)


class TestMouseUtils:
    """Tests for mouse_utils."""

    def test_is_macos(self) -> None:
        """Test macOS detection."""
        assert isinstance(IS_MACOS, bool)

    def test_valid_buttons(self) -> None:
        """Test valid buttons tuple."""
        assert 'left' in VALID_BUTTONS
        assert 'right' in VALID_BUTTONS
        assert 'middle' in VALID_BUTTONS

    def test_macos_click_invalid_button(self) -> None:
        """Test clicking with invalid button raises."""
        with pytest.raises(ValueError, match="Invalid button"):
            macos_click(100, 100, button='invalid')

    def test_macos_click_invalid_count(self) -> None:
        """Test clicking with invalid count raises."""
        with pytest.raises(ValueError, match="click_count must be >= 1"):
            macos_click(100, 100, click_count=0)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])