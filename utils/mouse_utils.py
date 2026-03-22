"""
Shared mouse utilities for cross-platform click automation.
"""
import time
import platform

IS_MACOS = platform.system() == 'Darwin'


def macos_click(x: int, y: int, click_count: int = 1, button: str = 'left'):
    """
    Cross-platform click with proper double-click support.
    
    On macOS, double-click must use pyautogui.doubleClick() because
    mouseDown+Up × 2 is interpreted as two separate clicks, not a double-click.
    
    Args:
        x: X coordinate
        y: Y coordinate
        click_count: 1 for single click, 2 for double-click
        button: 'left', 'right', or 'middle'
    """
    import pyautogui
    
    if click_count == 2:
        pyautogui.doubleClick(x, y, button=button)
    else:
        pyautogui.click(x, y, button=button)
