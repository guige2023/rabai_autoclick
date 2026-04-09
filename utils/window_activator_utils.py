"""
Window activation and focusing utilities.

Provides utilities for bringing windows to the foreground, activating
applications, and managing focus across multiple windows.

Example:
    >>> from utils.window_activator_utils import activate_window, bring_to_front
    >>> activate_window("Safari")
    >>> bring_to_front("Finder")
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional, List

try:
    from dataclasses import dataclass
except ImportError:
    from typing import dataclass


# ----------------------------------------------------------------------
# Data Structures
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class ActivationResult:
    """Result of a window activation attempt."""
    success: bool
    window_name: str
    window_id: Optional[str] = None
    error_message: Optional[str] = None
    duration_ms: float = 0.0

    @property
    def failed(self) -> bool:
        """Return True if the activation failed."""
        return not self.success


@dataclass
class ActivationPolicy:
    """Policy controlling how window activation behaves."""
    raise_all: bool = True
    animate: bool = True
    timeout_seconds: float = 5.0


# ----------------------------------------------------------------------
# Platform Detection
# ----------------------------------------------------------------------


def get_platform() -> str:
    """Get the current operating system platform."""
    import sys
    return sys.platform


# ----------------------------------------------------------------------
# Window ID Resolution
# ----------------------------------------------------------------------


def find_window_id(window_name: str) -> Optional[str]:
    """
    Find a window ID by its name using accessibility APIs.

    Args:
        window_name: Name or partial name of the window.

    Returns:
        Window ID as string if found, None otherwise.
    """
    if get_platform() == "darwin":
        script = f'''
        tell application "System Events"
            set winList to every window of (every process)
            repeat with win in winList
                if name of win contains "{window_name}" then
                    return (unix id of win) as string
                end if
            end repeat
        end tell
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            pass
    return None


def find_app_pid(application_name: str) -> Optional[int]:
    """
    Find the process ID of an application by name.

    Args:
        application_name: Name of the application.

    Returns:
        Process ID if found, None otherwise.
    """
    if get_platform() == "darwin":
        script = f'''
        tell application "System Events"
            set targetApp to first process whose ¬
                name contains "{application_name}"
            return unix id of targetApp as string
        end tell
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip().isdigit():
                return int(result.stdout.strip())
        except (subprocess.TimeoutExpired, ValueError, FileNotFoundError, OSError):
            pass
    return None


# ----------------------------------------------------------------------
# Core Activation Functions
# ----------------------------------------------------------------------


def activate_window(
    window_identifier: str,
    policy: Optional[ActivationPolicy] = None,
) -> ActivationResult:
    """
    Activate a specific window and bring it to the foreground.

    Args:
        window_identifier: Window name, window ID, or process name.
        policy: Optional activation policy.

    Returns:
        ActivationResult describing the outcome.

    Example:
        >>> result = activate_window("Safari")
        >>> if result.success:
        ...     print(f"Activated {result.window_name}")
    """
    import time as time_module

    policy = policy or ActivationPolicy()
    start_time = time_module.time()

    # Resolve to window ID if needed
    wid = window_identifier
    if not wid.isdigit():
        wid = find_window_id(window_identifier)
        if wid is None:
            return ActivationResult(
                success=False,
                window_name=window_identifier,
                error_message="Window not found",
                duration_ms=0.0,
            )

    if get_platform() == "darwin":
        # Try activating via process name first
        script = f'''
        tell application "System Events"
            set targetWindow to first window whose unix id is {wid}
            set targetProcess to first process of ¬
                (every process whose windows contains targetWindow)

            -- Activate the application
            set frontmost of targetProcess to true

            -- Small delay to allow activation to settle
            delay 0.1

            return name of targetProcess
        end tell
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=int(policy.timeout_seconds),
            )
            duration_ms = (time_module.time() - start_time) * 1000

            if result.returncode == 0:
                return ActivationResult(
                    success=True,
                    window_name=result.stdout.strip(),
                    window_id=wid,
                    duration_ms=duration_ms,
                )
            else:
                return ActivationResult(
                    success=False,
                    window_name=window_identifier,
                    window_id=wid,
                    error_message=result.stderr.strip() or "Unknown error",
                    duration_ms=duration_ms,
                )
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
            return ActivationResult(
                success=False,
                window_name=window_identifier,
                window_id=wid,
                error_message=str(e),
                duration_ms=(time_module.time() - start_time) * 1000,
            )

    return ActivationResult(
        success=False,
        window_name=window_identifier,
        error_message="Unsupported platform",
        duration_ms=(time_module.time() - start_time) * 1000,
    )


def bring_to_front(window_identifier: str) -> bool:
    """
    Bring a window to the front of the z-order stack.

    Args:
        window_identifier: Window name, window ID, or process name.

    Returns:
        True if successful, False otherwise.

    Example:
        >>> bring_to_front("Notes")
    """
    result = activate_window(window_identifier)
    return result.success


def activate_application(application_name: str) -> bool:
    """
    Activate an application by name (all its windows).

    Args:
        application_name: Name of the application.

    Returns:
        True if the application was activated, False otherwise.

    Example:
        >>> activate_application("Safari")
    """
    if get_platform() == "darwin":
        script = f'''
        tell application "{application_name}"
            activate
        end tell
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            return False
    return False


# ----------------------------------------------------------------------
# Batch Operations
# ----------------------------------------------------------------------


def activate_all_windows(process_name: str) -> List[ActivationResult]:
    """
    Activate all windows belonging to a specific process.

    Args:
        process_name: Name of the process.

    Returns:
        List of ActivationResult for each window found.

    Example:
        >>> results = activate_all_windows("Safari")
    """
    if get_platform() != "darwin":
        return []

    script = f'''
    tell application "System Events"
        set procList to every process whose name contains "{process_name}"
        set results to {{}}
        repeat with proc in procList
            set frontmost of proc to true
            repeat with win in windows of proc
                set end of results to (unix id of win as string)
            end repeat
        end repeat
        return results
    end tell
    '''
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return []

        results = []
        for wid in result.stdout.strip().split("\n"):
            wid = wid.strip()
            if not wid:
                continue
            act_result = activate_window(wid)
            results.append(act_result)

        return results
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return []


# ----------------------------------------------------------------------
# Focus Helpers
# ----------------------------------------------------------------------


def ensure_window_visible(window_identifier: str) -> bool:
    """
    Ensure a window is visible and not minimized or hidden.

    Args:
        window_identifier: Window name, window ID, or process name.

    Returns:
        True if the window is now visible, False otherwise.
    """
    if get_platform() == "darwin":
        wid = window_identifier
        if not wid.isdigit():
            wid = find_window_id(window_identifier)
            if wid is None:
                return False

        script = f'''
        tell application "System Events"
            tell (first window whose unix id is {wid})
                set miniaturized of it to false
                set visible of it to true
            end tell
        end tell
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            return False
    return False


def is_window_active(window_identifier: str) -> bool:
    """
    Check if a window is currently the active (frontmost) window.

    Args:
        window_identifier: Window name, window ID, or process name.

    Returns:
        True if the window is active, False otherwise.
    """
    if get_platform() == "darwin":
        wid = window_identifier
        if not wid.isdigit():
            wid = find_window_id(window_identifier)
            if wid is None:
                return False

        script = f'''
        tell application "System Events"
            set targetWindow to first window whose unix id is {wid}
            set targetProcess to first process of ¬
                (every process whose windows contains targetWindow)
            return frontmost of targetProcess
        end tell
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.stdout.strip().lower() == "true"
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            return False
    return False
