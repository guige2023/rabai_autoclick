"""Window State Persistence Utilities.

Saves and restores window positions, sizes, and states.
Useful for preserving layout across sessions and restoring UI state.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional


class WindowState(Enum):
    """Window state flags."""

    NORMAL = auto()
    MINIMIZED = auto()
    MAXIMIZED = auto()
    FULLSCREEN = auto()
    SNAPPED = auto()


@dataclass
class WindowSnapshot:
    """Complete snapshot of a window's state.

    Attributes:
        window_id: Unique window identifier.
        title: Window title.
        bounds: Window bounds (x, y, width, height).
        state: Current WindowState.
        monitor: Monitor/display index.
        z_order: Z-order position.
        is_focused: Whether window has focus.
        is_visible: Whether window is visible.
        timestamp: When snapshot was taken.
    """

    window_id: str
    title: str
    bounds: tuple[int, int, int, int] = (0, 0, 800, 600)
    state: WindowState = WindowState.NORMAL
    monitor: int = 0
    z_order: int = 0
    is_focused: bool = False
    is_visible: bool = True
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "window_id": self.window_id,
            "title": self.title,
            "bounds": list(self.bounds),
            "state": self.state.name,
            "monitor": self.monitor,
            "z_order": self.z_order,
            "is_focused": self.is_focused,
            "is_visible": self.is_visible,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "WindowSnapshot":
        """Create from dictionary."""
        return cls(
            window_id=data["window_id"],
            title=data["title"],
            bounds=tuple(data["bounds"]),
            state=WindowState[data.get("state", "NORMAL")],
            monitor=data.get("monitor", 0),
            z_order=data.get("z_order", 0),
            is_focused=data.get("is_focused", False),
            is_visible=data.get("is_visible", True),
            timestamp=data.get("timestamp", time.time()),
        )


class WindowStateStorage:
    """Persistent storage for window states.

    Example:
        storage = WindowStateStorage("/tmp/window_states.json")
        storage.save(snapshot)
        restored = storage.load("window_1")
    """

    def __init__(self, storage_path: str | Path):
        """Initialize the storage.

        Args:
            storage_path: Path to JSON file for persistence.
        """
        self.storage_path = Path(storage_path)
        self._cache: dict[str, WindowSnapshot] = {}
        self._load()

    def _load(self) -> None:
        """Load states from disk."""
        if not self.storage_path.exists():
            return

        try:
            with open(self.storage_path) as f:
                data = json.load(f)
            for window_id, snapshot_data in data.items():
                self._cache[window_id] = WindowSnapshot.from_dict(snapshot_data)
        except (json.JSONDecodeError, KeyError):
            pass

    def _save(self) -> None:
        """Save states to disk."""
        data = {wid: snapshot.to_dict() for wid, snapshot in self._cache.items()}
        with open(self.storage_path, "w") as f:
            json.dump(data, f, indent=2)

    def save(self, snapshot: WindowSnapshot) -> None:
        """Save a window snapshot.

        Args:
            snapshot: WindowSnapshot to save.
        """
        self._cache[snapshot.window_id] = snapshot
        self._save()

    def load(self, window_id: str) -> Optional[WindowSnapshot]:
        """Load a window snapshot.

        Args:
            window_id: Window identifier.

        Returns:
            WindowSnapshot or None if not found.
        """
        return self._cache.get(window_id)

    def load_by_title(self, title: str) -> Optional[WindowSnapshot]:
        """Load a window snapshot by title.

        Args:
            title: Window title to search for.

        Returns:
            First matching WindowSnapshot or None.
        """
        for snapshot in self._cache.values():
            if snapshot.title == title:
                return snapshot
        return None

    def delete(self, window_id: str) -> bool:
        """Delete a window snapshot.

        Args:
            window_id: Window identifier.

        Returns:
            True if snapshot was deleted.
        """
        if window_id in self._cache:
            del self._cache[window_id]
            self._save()
            return True
        return False

    def list_windows(self) -> list[str]:
        """List all saved window IDs.

        Returns:
            List of window identifiers.
        """
        return list(self._cache.keys())

    def clear(self) -> None:
        """Clear all saved snapshots."""
        self._cache.clear()
        self._save()


class WindowStateRestorer:
    """Restores window states from snapshots.

    Example:
        restorer = WindowStateRestorer(platform_adapter)
        restorer.restore(snapshot, focus=True)
    """

    def __init__(self, platform_adapter=None):
        """Initialize the restorer.

        Args:
            platform_adapter: Platform-specific window control adapter.
        """
        self._adapter = platform_adapter

    def restore(
        self,
        snapshot: WindowSnapshot,
        focus: bool = True,
        raise_window: bool = True,
    ) -> bool:
        """Restore a window to a saved state.

        Args:
            snapshot: WindowSnapshot to restore.
            focus: Whether to focus the window after restore.
            raise_window: Whether to bring window to front.

        Returns:
            True if restoration was successful.
        """
        if self._adapter:
            try:
                # Restore bounds
                self._adapter.set_bounds(snapshot.window_id, snapshot.bounds)

                # Restore state
                if snapshot.state == WindowState.MINIMIZED:
                    self._adapter.minimize(snapshot.window_id)
                elif snapshot.state == WindowState.MAXIMIZED:
                    self._adapter.maximize(snapshot.window_id)

                # Bring to front if requested
                if raise_window:
                    self._adapter.raise_window(snapshot.window_id)

                # Set focus if requested
                if focus:
                    self._adapter.set_focus(snapshot.window_id)

                return True
            except Exception:
                return False

        return False

    def restore_all(
        self,
        snapshots: list[WindowSnapshot],
        focus_first: bool = True,
    ) -> int:
        """Restore multiple windows.

        Args:
            snapshots: List of WindowSnapshots to restore.
            focus_first: Whether to focus the first restored window.

        Returns:
            Number of windows successfully restored.
        """
        restored = 0
        for i, snapshot in enumerate(snapshots):
            should_focus = focus_first and (i == 0)
            if self.restore(snapshot, focus=should_focus):
                restored += 1
        return restored


class WindowLayout:
    """Represents a saved window layout.

    A layout contains the arrangement of multiple windows.

    Attributes:
        name: Layout name.
        windows: List of window snapshots in the layout.
        created_at: When the layout was created.
    """

    def __init__(
        self,
        name: str,
        windows: Optional[list[WindowSnapshot]] = None,
    ):
        """Initialize the layout.

        Args:
            name: Layout name.
            windows: List of window snapshots.
        """
        self.name = name
        self.windows = windows or []
        self.created_at = time.time()

    def add_window(self, snapshot: WindowSnapshot) -> None:
        """Add a window to the layout.

        Args:
            snapshot: WindowSnapshot to add.
        """
        self.windows.append(snapshot)

    def remove_window(self, window_id: str) -> bool:
        """Remove a window from the layout.

        Args:
            window_id: Window identifier to remove.

        Returns:
            True if window was found and removed.
        """
        for i, w in enumerate(self.windows):
            if w.window_id == window_id:
                self.windows.pop(i)
                return True
        return False

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "created_at": self.created_at,
            "windows": [w.to_dict() for w in self.windows],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "WindowLayout":
        """Create from dictionary."""
        windows = [WindowSnapshot.from_dict(w) for w in data.get("windows", [])]
        layout = cls(name=data["name"], windows=windows)
        layout.created_at = data.get("created_at", time.time())
        return layout


class LayoutManager:
    """Manages saved window layouts.

    Example:
        manager = LayoutManager("/tmp/layouts.json")
        manager.save_current_layout("my_layout", window_ids=["win1", "win2"])
        manager.load_layout("my_layout")
    """

    def __init__(self, storage_path: str | Path):
        """Initialize the layout manager.

        Args:
            storage_path: Path to JSON file for layout storage.
        """
        self.storage_path = Path(storage_path)
        self._layouts: dict[str, WindowLayout] = {}
        self._load()

    def _load(self) -> None:
        """Load layouts from disk."""
        if not self.storage_path.exists():
            return

        try:
            with open(self.storage_path) as f:
                data = json.load(f)
            for name, layout_data in data.items():
                self._layouts[name] = WindowLayout.from_dict(layout_data)
        except (json.JSONDecodeError, KeyError):
            pass

    def _save(self) -> None:
        """Save layouts to disk."""
        data = {name: layout.to_dict() for name, layout in self._layouts.items()}
        with open(self.storage_path, "w") as f:
            json.dump(data, f, indent=2)

    def save_layout(
        self,
        name: str,
        windows: list[WindowSnapshot],
    ) -> None:
        """Save a window layout.

        Args:
            name: Layout name.
            windows: List of window snapshots.
        """
        self._layouts[name] = WindowLayout(name=name, windows=windows)
        self._save()

    def load_layout(self, name: str) -> Optional[WindowLayout]:
        """Load a saved layout.

        Args:
            name: Layout name.

        Returns:
            WindowLayout or None if not found.
        """
        return self._layouts.get(name)

    def delete_layout(self, name: str) -> bool:
        """Delete a saved layout.

        Args:
            name: Layout name.

        Returns:
            True if layout was deleted.
        """
        if name in self._layouts:
            del self._layouts[name]
            self._save()
            return True
        return False

    def list_layouts(self) -> list[str]:
        """List all saved layout names.

        Returns:
            List of layout names.
        """
        return list(self._layouts.keys())

    def get_layout_names_with_timestamps(self) -> dict[str, float]:
        """Get layout names with their creation timestamps.

        Returns:
            Dictionary of name to created_at timestamp.
        """
        return {name: layout.created_at for name, layout in self._layouts.items()}
