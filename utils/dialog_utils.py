"""Dialog handling utilities for automation of system and application dialogs.

Provides helpers for dismissing, accepting, and interacting with
modal dialogs, alerts, file open/save dialogs, and other
native dialog windows.

Example:
    >>> from utils.dialog_utils import dismiss_dialog, accept_dialog, click_dialog_button
    >>> dismiss_dialog()  # press Escape
    >>> accept_dialog()   # press Return
    >>> click_dialog_button('Save')
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional

__all__ = [
    "dismiss_dialog",
    "accept_dialog",
    "cancel_dialog",
    "click_dialog_button",
    "get_dialog_text",
    "wait_for_dialog",
    "dialog_exists",
    "press_button_in_sheet",
]


def _keystroke(key: str, modifiers: Optional[list[str]] = None) -> bool:
    """Send a keystroke using osascript."""
    mod_str = ""
    if modifiers:
        mod_str = " using " + ",".join(f"{m} down" for m in modifiers)

    script = f'tell application "System Events" to keystroke "{key}"{mod_str}'
    try:
        subprocess.run(["osascript", "-e", script], timeout=5, check=True)
        return True
    except Exception:
        return False


def _press_key(key_code: int, modifiers: Optional[list[str]] = None) -> bool:
    """Send a raw key code using osascript."""
    mod_str = ""
    if modifiers:
        mod_str = " using " + ",".join(f"{m} down" for m in modifiers)

    script = f'tell application "System Events" to keystroke (ASCII character {key_code}){mod_str}'
    try:
        subprocess.run(["osascript", "-e", script], timeout=5, check=True)
        return True
    except Exception:
        return False


def dismiss_dialog() -> bool:
    """Dismiss a dialog by pressing Escape (Cancel).

    Returns:
        True if successful.
    """
    script = 'tell application "System Events" to keystroke (ASCII character 27)'
    try:
        subprocess.run(["osascript", "-e", script], timeout=5, check=True)
        return True
    except Exception:
        return False


def accept_dialog() -> bool:
    """Accept a dialog by pressing Return (OK/Default action).

    Returns:
        True if successful.
    """
    script = 'tell application "System Events" to keystroke (ASCII character 13)'
    try:
        subprocess.run(["osascript", "-e", script], timeout=5, check=True)
        return True
    except Exception:
        return False


def cancel_dialog() -> bool:
    """Cancel a dialog by pressing Cmd+.

    Returns:
        True if successful.
    """
    return dismiss_dialog()


def click_dialog_button(button_name: str, timeout: float = 2.0) -> bool:
    """Click a button in the frontmost dialog by its label.

    Uses the accessibility API to find and click the button.

    Args:
        button_name: Label of the button to click (e.g., 'Save', 'Cancel').
        timeout: Seconds to wait for the dialog.

    Returns:
        True if the button was found and clicked.
    """
    import sys

    if sys.platform != "darwin":
        return False

    script = f"""
    tell application "System Events"
        tell process "FrontMost"
            try
                set btn to (first button of front window whose name contains "{button_name}")
                click btn
                return true
            on error
                return false
            end try
        end tell
    end tell
    """
    try:
        result = subprocess.check_output(
            ["osascript", "-e", script],
            timeout=timeout,
        )
        return result.strip() == "true"
    except Exception:
        return False


def get_dialog_text() -> Optional[str]:
    """Get the text content of the frontmost dialog.

    Returns:
        Dialog text as a string, or None.
    """
    script = """
    tell application "System Events"
        tell process "FrontMost"
            try
                return description of front window
            on error
                return ""
            end try
        end tell
    end tell
    """
    try:
        result = subprocess.check_output(
            ["osascript", "-e", script],
            timeout=5,
        )
        text = result.decode().strip()
        return text if text else None
    except Exception:
        return None


def dialog_exists(timeout: float = 1.0) -> bool:
    """Check if a modal dialog is currently displayed.

    Args:
        timeout: Time to wait before returning.

    Returns:
        True if a dialog is visible.
    """
    script = """
    tell application "System Events"
        tell process "FrontMost"
            try
                set w to front window
                set winRole to role of w
                if winRole is "AXDialog" or winRole is "AXSheet" then
                    return true
                end if
            on error
                return false
            end try
        end tell
    end tell
    return false
    """
    try:
        result = subprocess.check_output(
            ["osascript", "-e", script],
            timeout=timeout,
        )
        return result.strip() == "true"
    except Exception:
        return False


def wait_for_dialog(
    timeout: float = 10.0,
    poll_interval: float = 0.2,
) -> bool:
    """Wait for a dialog to appear.

    Args:
        timeout: Maximum wait time.
        poll_interval: Time between checks.

    Returns:
        True if a dialog appeared within the timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if dialog_exists(timeout=poll_interval):
            return True
        time.sleep(poll_interval)
    return False


def press_button_in_sheet(button_label: str) -> bool:
    """Click a button in a sheet (document modal dialog).

    Args:
        button_label: Button label to click.

    Returns:
        True if successful.
    """
    script = f"""
    tell application "System Events"
        tell process "FrontMost"
            try
                set btn to (first button of sheet of front window whose name contains "{button_label}")
                click btn
                return true
            on error
                return false
            end try
        end tell
    end tell
    """
    try:
        result = subprocess.check_output(
            ["osascript", "-e", script],
            timeout=5,
        )
        return result.strip() == "true"
    except Exception:
        return False


def type_in_dialog(text: str, interval: float = 0.01) -> bool:
    """Type text into a text field of a dialog.

    Args:
        text: Text to type.
        interval: Delay between characters.

    Returns:
        True if successful.
    """
    for char in text:
        script = f'tell application "System Events" to keystroke "{char}"'
        try:
            subprocess.run(["osascript", "-e", script], timeout=2, check=True)
            if interval > 0:
                time.sleep(interval)
        except Exception:
            return False
    return True
