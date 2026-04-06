"""Shared mouse utilities for cross-platform click automation."""

import platform
from typing import Literal


# Platform detection
IS_MACOS: bool = platform.system() == 'Darwin'

# Valid mouse buttons
VALID_BUTTONS: tuple = ('left', 'right', 'middle')


def macos_click(
    x: int, 
    y: int, 
    click_count: int = 1, 
    button: Literal['left', 'right', 'middle'] = 'left'
) -> None:
    """Cross-platform click with proper double-click support.
    
    On macOS, double-click must use pyautogui.doubleClick() because
    mouseDown+Up × 2 is interpreted as two separate clicks, not a double-click.
    
    Args:
        x: X coordinate to click at.
        y: Y coordinate to click at.
        click_count: 1 for single click, 2 for double-click.
        button: Mouse button: 'left', 'right', or 'middle'.
        
    Raises:
        ValueError: If button is not valid or click_count < 1.
    """
    import pyautogui
    
    if button not in VALID_BUTTONS:
        raise ValueError(
            f"Invalid button '{button}'. Must be one of {VALID_BUTTONS}"
        )
    if click_count < 1:
        raise ValueError(
            f"click_count must be >= 1, got {click_count}"
        )
    
    if click_count == 2:
        pyautogui.doubleClick(x, y, button=button)
    else:
        pyautogui.click(x, y, button=button)
