"""Utilities for macOS application switching and management.

This module provides utilities for discovering running applications,
switching between apps, managing app windows, and querying app states.
"""

from __future__ import annotations

import subprocess
from typing import Sequence
from dataclasses import dataclass
from enum import Enum


class AppState(Enum):
    """Application running state."""
    RUNNING = "running"
    HIDDEN = "hidden"
    NOT_RUNNING = "not_running"


@dataclass
class AppInfo:
    """Information about a running application.

    Attributes:
        name: Human-readable app name.
        bundle_id: macOS bundle identifier (e.g., 'com.apple.Safari').
        pid: Process ID of the app.
        state: Current running state.
        frontmost: Whether this app is currently frontmost.
    """
    name: str
    bundle_id: str
    pid: int
    state: AppState
    frontmost: bool = False


def get_running_apps() -> list[AppInfo]:
    """Get list of currently running applications.

    Returns:
        List of AppInfo for all running apps.
    """
    script = """
    tell application "System Events"
        set appList to every application process
        set resultList to {}
        repeat with appProc in appList
            try
                set appName to name of appProc
                set appBundle to ""
                try
                    tell application appName
                        set appBundle to id
                    end tell
                end try
                set appPID to unix id of appProc
                set appVisible to visible of appProc
                set appFrontmost to frontmost of appProc
                set appState to "running"
                if appVisible is false then set appState to "hidden"
                set end of resultList to appName & "|" & appBundle & "|" & appPID & "|" & appState & "|" & appFrontmost
            end try
        end repeat
        return resultList
    end tell
    """

    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=10
        )

        apps: list[AppInfo] = []
        for line in result.stdout.strip().split("\n"):
            if not line or "|" not in line:
                continue

            parts = line.split("|")
            if len(parts) < 4:
                continue

            name, bundle_id, pid_str, state_str = parts[0], parts[1], parts[2], parts[3]
            frontmost = len(parts) > 4 and parts[4] == "true"

            try:
                state = AppState(state_str)
            except ValueError:
                state = AppState.RUNNING

                apps.append(AppInfo(
                    name=name,
                    bundle_id=bundle_id,
                    pid=int(pid_str),
                    state=state,
                    frontmost=frontmost
                ))

        return apps

    except (subprocess.TimeoutExpired, subprocess.SubprocessError, OSError) as e:
        return []


def get_frontmost_app() -> AppInfo | None:
    """Get the currently frontmost application.

    Returns:
        AppInfo of frontmost app, or None if unavailable.
    """
    script = """
    tell application "System Events"
        set frontApp to first application process whose frontmost is true
        set appName to name of frontApp
        set appBundle to ""
        try
            tell application appName
                set appBundle to id
            end tell
        end try
        set appPID to unix id of frontApp
        return appName & "|" & appBundle & "|" & appPID
    end tell
    """

    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=5
        )

        parts = result.stdout.strip().split("|")
        if len(parts) < 3:
            return None

        return AppInfo(
            name=parts[0],
            bundle_id=parts[1],
            pid=int(parts[2]),
            state=AppState.RUNNING,
            frontmost=True
        )

    except (subprocess.SubprocessError, OSError, ValueError):
        return None


def switch_to_app(bundle_id: str | None = None, name: str | None = None) -> bool:
    """Switch to a specific application by bundle ID or name.

    Args:
        bundle_id: macOS bundle identifier (preferred).
        name: Human-readable app name (fallback).

    Returns:
        True if switch succeeded, False otherwise.
    """
    if bundle_id:
        script = f'''
        tell application id "{bundle_id}"
            activate
        end tell
        '''
    elif name:
        script = f'''
        tell application "{name}"
            activate
        end tell
        '''
    else:
        return False

    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, OSError):
        return False


def hide_app(bundle_id: str | None = None, name: str | None = None) -> bool:
    """Hide a specific application.

    Args:
        bundle_id: macOS bundle identifier (preferred).
        name: Human-readable app name (fallback).

    Returns:
        True if hide succeeded, False otherwise.
    """
    if bundle_id:
        script = f'''
        tell application id "{bundle_id}"
            set visible to false
        end tell
        '''
    elif name:
        script = f'''
        tell application "{name}"
            set visible to false
        end tell
        '''
    else:
        return False

    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, OSError):
        return False


def quit_app(bundle_id: str | None = None, name: str | None = None, force: bool = False) -> bool:
    """Quit a specific application.

    Args:
        bundle_id: macOS bundle identifier (preferred).
        name: Human-readable app name (fallback).
        force: If True, force quit (SIGKILL). Otherwise graceful quit.

    Returns:
        True if quit succeeded, False otherwise.
    """
    if bundle_id:
        app_arg = f'id "{bundle_id}"'
    elif name:
        app_arg = f'"{name}"'
    else:
        return False

    if force:
        script = f'''
        tell application {app_arg}
            quit
        end tell
        '''
        try:
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5
            )
        except (subprocess.SubprocessError, OSError):
            pass

        try:
            subprocess.run(
                ["pkill", "-9", "-f", bundle_id or name or ""],
                capture_output=True,
                timeout=5
            )
            return True
        except (subprocess.SubprocessError, OSError):
            return False
    else:
        script = f'''
        tell application {app_arg}
            quit
        end tell
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.returncode == 0
        except (subprocess.SubprocessError, OSError):
            return False


def get_app_windows(bundle_id: str | None = None, name: str | None = None) -> list[dict]:
    """Get list of windows for a specific application.

    Args:
        bundle_id: macOS bundle identifier (preferred).
        name: Human-readable app name (fallback).

    Returns:
        List of window info dicts with keys: title, id, bounds.
    """
    if bundle_id:
        app_clause = f'application id "{bundle_id}"'
    elif name:
        app_clause = f'application "{name}"'
    else:
        return []

    script = f'''
    tell application {app_clause}
        set windowList to {}
        try
            set windowList to every window
        end try
        set resultList to {}
        repeat with w in windowList
            set wTitle to name of w
            set wID to id of w
            set wBounds to bounds of w
            set end of resultList to wTitle & "|||" & wID & "|||" & (item 1 of wBounds as string) & "," & (item 2 of wBounds as string) & "," & (item 3 of wBounds as string) & "," & (item 4 of wBounds as string)
        end repeat
        return resultList
    end tell
    '''

    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=10
        )

        windows: list[dict] = []
        for line in result.stdout.strip().split("\n"):
            if not line or "|||" not in line:
                continue

            parts = line.split("|||")
            if len(parts) < 3:
                continue

            title, wid_str, bounds_str = parts[0], parts[1], parts[2]
            bounds_parts = bounds_str.split(",")

            if len(bounds_parts) != 4:
                continue

            try:
                windows.append({
                    "title": title,
                    "id": int(wid_str),
                    "bounds": {
                        "x": int(bounds_parts[0]),
                        "y": int(bounds_parts[1]),
                        "width": int(bounds_parts[2]) - int(bounds_parts[0]),
                        "height": int(bounds_parts[3]) - int(bounds_parts[1])
                    }
                })
            except ValueError:
                continue

        return windows

    except (subprocess.SubprocessError, OSError):
        return []


def launch_app(bundle_id: str | None = None, name: str | None = None, args: Sequence[str] | None = None) -> bool:
    """Launch an application by bundle ID or name.

    Args:
        bundle_id: macOS bundle identifier (preferred).
        name: Human-readable app name (fallback).
        args: Optional arguments to pass to the app.

    Returns:
        True if launch succeeded, False otherwise.
    """
    cmd = ["open"]

    if bundle_id:
        cmd.extend(["-b", bundle_id])
    elif name:
        cmd.extend(["-a", name])
    else:
        return False

    if args:
        cmd.append("--args")
        cmd.extend(args)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, OSError):
        return False


def is_app_running(bundle_id: str | None = None, name: str | None = None) -> bool:
    """Check if an application is currently running.

    Args:
        bundle_id: macOS bundle identifier (preferred).
        name: Human-readable app name (fallback).

    Returns:
        True if app is running.
    """
    apps = get_running_apps()

    if bundle_id:
        return any(app.bundle_id == bundle_id for app in apps)
    elif name:
        return any(app.name == name for app in apps)

    return False
