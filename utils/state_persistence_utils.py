"""State persistence utilities for saving and restoring automation session state.

Provides tools for serializing automation state (recorded actions,
workflow context, UI snapshots) to disk, enabling recovery after
interruptions and resumable automation sessions.

Example:
    >>> from utils.state_persistence_utils import StateStore, save_state, restore_state
    >>> store = StateStore('/tmp/autoclick_state.json')
    >>> store.save({'step': 5, 'actions': [...]})
    >>> state = store.load()
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

__all__ = [
    "StateStore",
    "save_state",
    "restore_state",
    "list_saved_states",
    "StateError",
]


class StateError(Exception):
    """Raised when state operations fail."""
    pass


class StateStore:
    """Persistent storage for automation session state.

    Supports atomic writes, versioning, and state snapshots.

    Example:
        >>> store = StateStore('/tmp/state.json')
        >>> store.save({'step': 3, 'data': {...}})
        >>> state = store.load()
    """

    def __init__(
        self,
        path: str | Path,
        atomic: bool = True,
        backup: bool = True,
        indent: int = 2,
    ):
        self.path = Path(path)
        self.atomic = atomic
        self.backup = backup
        self.indent = indent
        self._lock_path = self.path.with_suffix(".lock")
        self._backup_path = self.path.with_suffix(".bak")

    def save(self, state: dict, metadata: Optional[dict] = None) -> None:
        """Save state to disk.

        Args:
            state: State dictionary to persist.
            metadata: Optional metadata (timestamp, version, etc.).

        Raises:
            StateError: If saving fails.
        """
        if metadata is None:
            metadata = {}

        payload = {
            "version": "1.0",
            "timestamp": time.time(),
            "metadata": metadata,
            "state": state,
        }

        content = json.dumps(payload, indent=self.indent)

        if self.atomic:
            # Write to temp file first, then rename
            import tempfile

            tmp_fd, tmp_path = tempfile.mkstemp(
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
            )
            try:
                os = __import__("os")
                os.write(tmp_fd, content.encode())
                os.close(tmp_fd)

                # Backup existing file
                if self.backup and self.path.exists():
                    import shutil

                    shutil.copy2(self.path, self._backup_path)

                # Atomic rename
                os.replace(tmp_path, self.path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass
                raise
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(content)

    def load(self, default: Optional[dict] = None) -> dict:
        """Load state from disk.

        Args:
            default: Default state if file doesn't exist.

        Returns:
            State dictionary.

        Raises:
            StateError: If loading fails due to corruption.
        """
        if not self.path.exists():
            if default is not None:
                return default
            return {}

        try:
            content = self.path.read_text()
            payload = json.loads(content)
            return payload.get("state", {})
        except json.JSONDecodeError as e:
            # Try to restore from backup
            if self.backup and self._backup_path.exists():
                try:
                    content = self._backup_path.read_text()
                    payload = json.loads(content)
                    return payload.get("state", {})
                except Exception:
                    pass
            raise StateError(f"Failed to load state: {e}")

    def exists(self) -> bool:
        """Check if state file exists."""
        return self.path.exists()

    def delete(self) -> None:
        """Delete the state file and its backup."""
        if self.path.exists():
            self.path.unlink()
        if self._backup_path.exists():
            self._backup_path.unlink()
        if self._lock_path.exists():
            self._lock_path.unlink()

    def backup_named(self, name: str) -> Path:
        """Create a named backup of current state.

        Args:
            name: Backup name (added to filename).

        Returns:
            Path to the backup file.
        """
        if not self.path.exists():
            raise StateError("No state file to back up")

        import shutil

        backup_file = self.path.parent / f"{self.path.stem}.{name}{self.path.suffix}"
        shutil.copy2(self.path, backup_file)
        return backup_file

    def get_metadata(self) -> Optional[dict]:
        """Get the metadata from the last save.

        Returns:
            Metadata dict, or None if not available.
        """
        if not self.path.exists():
            return None

        try:
            content = self.path.read_text()
            payload = json.loads(content)
            return payload.get("metadata", {})
        except Exception:
            return None


def save_state(
    state: dict,
    path: str | Path,
    **kwargs,
) -> bool:
    """Convenience function to save state.

    Args:
        state: State to save.
        path: File path.
        **kwargs: Additional arguments for StateStore.

    Returns:
        True if saved successfully.
    """
    try:
        store = StateStore(path, **kwargs)
        store.save(state)
        return True
    except Exception:
        return False


def restore_state(
    path: str | Path,
    default: Optional[dict] = None,
) -> Optional[dict]:
    """Convenience function to restore state.

    Args:
        path: File path.
        default: Default state if not found.

    Returns:
        Restored state, or default.
    """
    try:
        store = StateStore(path)
        return store.load(default)
    except Exception:
        return default


def list_saved_states(directory: str | Path, pattern: str = "*.json") -> list[Path]:
    """List all saved state files in a directory.

    Args:
        directory: Directory to search.
        pattern: Glob pattern for files.

    Returns:
        List of Path objects.
    """
    return list(Path(directory).glob(pattern))
