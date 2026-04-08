"""
Hotkey Recorder Utility

Records and replays keyboard shortcuts for automation.
Maps key combinations to named actions.

Example:
    >>> recorder = HotkeyRecorder()
    >>> recorder.bind("cmd+s", "save_document")
    >>> recorder.bind("cmd+shift+s", "save_as")
    >>> recorder.unlock("save_document")
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class HotkeyBinding:
    """A hotkey binding to an action."""
    key_combo: str  # e.g., "cmd+s", "ctrl+shift+delete"
    action_name: str
    callback: Optional[Callable[[], None]] = None
    description: str = ""
    enabled: bool = True
    last_triggered: float = 0.0
    trigger_count: int = 0


class HotkeyRecorder:
    """
    Manages keyboard shortcut bindings for automation.

    Bindings map key combinations to named actions with callbacks.
    """

    def __init__(self) -> None:
        self._bindings: dict[str, HotkeyBinding] = {}
        self._lock = threading.Lock()
        self._global_enabled = True

    def bind(
        self,
        key_combo: str,
        action_name: str,
        callback: Optional[Callable[[], None]] = None,
        description: str = "",
    ) -> None:
        """
        Bind a key combination to an action.

        Args:
            key_combo: Key combination string like "cmd+s" or "ctrl+shift+delete".
            action_name: Name of the action.
            callback: Optional function to call when triggered.
            description: Human-readable description.
        """
        normalized = self._normalize_combo(key_combo)
        with self._lock:
            self._bindings[normalized] = HotkeyBinding(
                key_combo=key_combo,
                action_name=action_name,
                callback=callback,
                description=description,
            )

    def unbind(self, key_combo: str) -> bool:
        """
        Remove a binding.

        Args:
            key_combo: Key combination to remove.

        Returns:
            True if binding existed and was removed.
        """
        normalized = self._normalize_combo(key_combo)
        with self._lock:
            if normalized in self._bindings:
                del self._bindings[normalized]
                return True
        return False

    def trigger(
        self,
        key_combo: str,
        check_enabled: bool = True,
    ) -> bool:
        """
        Trigger an action by key combination.

        Args:
            key_combo: Key combination that was pressed.
            check_enabled: Whether to check if binding is enabled.

        Returns:
            True if action was triggered, False otherwise.
        """
        normalized = self._normalize_combo(key_combo)
        with self._lock:
            binding = self._bindings.get(normalized)
            if not binding:
                return False
            if check_enabled and not binding.enabled:
                return False
            if not self._global_enabled:
                return False

            binding.last_triggered = time.time()
            binding.trigger_count += 1

            if binding.callback:
                try:
                    binding.callback()
                except Exception:
                    pass

            return True

    def enable(self, key_combo: str) -> bool:
        """Enable a specific binding."""
        normalized = self._normalize_combo(key_combo)
        with self._lock:
            if normalized in self._bindings:
                self._bindings[normalized].enabled = True
                return True
        return False

    def disable(self, key_combo: str) -> bool:
        """Disable a specific binding."""
        normalized = self._normalize_combo(key_combo)
        with self._lock:
            if normalized in self._bindings:
                self._bindings[normalized].enabled = False
                return True
        return False

    def enable_all(self) -> None:
        """Enable all bindings."""
        with self._lock:
            self._global_enabled = True

    def disable_all(self) -> None:
        """Disable all bindings."""
        with self._lock:
            self._global_enabled = False

    def get_binding(self, key_combo: str) -> Optional[HotkeyBinding]:
        """Get binding info for a key combination."""
        normalized = self._normalize_combo(key_combo)
        with self._lock:
            return self._bindings.get(normalized)

    def list_bindings(self) -> list[HotkeyBinding]:
        """List all registered bindings."""
        with self._lock:
            return list(self._bindings.values())

    def find_by_action(self, action_name: str) -> list[HotkeyBinding]:
        """Find all bindings for an action name."""
        with self._lock:
            return [
                b for b in self._bindings.values()
                if b.action_name == action_name
            ]

    def _normalize_combo(self, combo: str) -> str:
        """Normalize a key combination string."""
        # Standard modifiers
        replacements = {
            "command": "cmd",
            "option": "opt",
            "control": "ctrl",
            "shift": "shift",
            "meta": "cmd",
            "super": "cmd",
        }

        parts = combo.lower().replace(" ", "").split("+")
        normalized = []
        for part in parts:
            normalized.append(replacements.get(part, part))
        return "+".join(sorted(normalized))

    def parse_key_combo(self, combo: str) -> dict[str, bool]:
        """
        Parse a key combo string into modifier flags.

        Args:
            combo: Key combination like "cmd+shift+s".

        Returns:
            Dict with 'cmd', 'ctrl', 'opt', 'shift', 'key' keys.
        """
        parts = combo.lower().replace(" ", "").split("+")
        result: dict[str, bool] = {
            "cmd": False,
            "ctrl": False,
            "opt": False,
            "shift": False,
            "key": "",
        }

        for part in parts:
            if part in result:
                result[part] = True
            else:
                result["key"] = part

        return result
