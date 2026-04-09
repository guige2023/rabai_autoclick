"""
Focus Management Module.

Provides utilities for managing keyboard and UI focus across multiple
windows and applications, including focus tracking, restoration, and
focus traversal management.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable


logger = logging.getLogger(__name__)


class FocusTargetType(Enum):
    """Types of focus targets."""
    WINDOW = auto()
    ELEMENT = auto()
    APPLICATION = auto()
    CUSTOM = auto()


@dataclass
class FocusTarget:
    """Represents a focusable target."""
    id: str
    type: FocusTargetType
    name: str
    bundle_id: str | None = None
    process_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FocusState:
    """Current state of focus management."""
    focused_target: FocusTarget | None = None
    focus_history: list[FocusTarget] = field(default_factory=list)
    locked: bool = False
    lock_reason: str | None = None


class FocusTracker:
    """
    Tracks focus changes across the system.

    Example:
        >>> tracker = FocusTracker()
        >>> tracker.start_tracking()
        >>> current = tracker.get_focused()
    """

    def __init__(self) -> None:
        """Initialize the focus tracker."""
        self._state = FocusState()
        self._listeners: list[Callable[[FocusTarget | None], None]] = []
        self._tracking: bool = False

    def start_tracking(self) -> None:
        """Start tracking focus changes."""
        self._tracking = True
        logger.info("Focus tracking started")

    def stop_tracking(self) -> None:
        """Stop tracking focus changes."""
        self._tracking = False
        logger.info("Focus tracking stopped")

    def is_tracking(self) -> bool:
        """Check if tracking is active."""
        return self._tracking

    def get_focused(self) -> FocusTarget | None:
        """
        Get the currently focused target.

        Returns:
            Current FocusTarget or None.
        """
        return self._state.focused_target

    def set_focused(self, target: FocusTarget | None) -> None:
        """
        Set the currently focused target.

        Args:
            target: The new focused target or None.
        """
        if self._state.locked:
            logger.warning(
                f"Focus locked ({self._state.lock_reason}), ignoring set_focused"
            )
            return

        if target != self._state.focused_target:
            if self._state.focused_target:
                self._state.focus_history.append(self._state.focused_target)

            self._state.focused_target = target
            self._notify_listeners(target)

    def get_history(self, limit: int = 10) -> list[FocusTarget]:
        """
        Get recent focus history.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of recently focused targets.
        """
        return self._state.focus_history[-limit:]

    def lock(self, reason: str) -> None:
        """
        Lock focus to prevent changes.

        Args:
            reason: Reason for locking.
        """
        self._state.locked = True
        self._state.lock_reason = reason
        logger.info(f"Focus locked: {reason}")

    def unlock(self) -> None:
        """Unlock focus to allow changes."""
        self._state.locked = False
        self._state.lock_reason = None
        logger.info("Focus unlocked")

    def add_listener(
        self,
        listener: Callable[[FocusTarget | None], None]
    ) -> None:
        """
        Add a focus change listener.

        Args:
            listener: Callback function.
        """
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(
        self,
        listener: Callable[[FocusTarget | None], None]
    ) -> None:
        """
        Remove a focus change listener.

        Args:
            listener: Callback to remove.
        """
        if listener in self._listeners:
            self._listeners.remove(listener)

    def _notify_listeners(self, target: FocusTarget | None) -> None:
        """Notify all listeners of focus change."""
        for listener in self._listeners:
            try:
                listener(target)
            except Exception as e:
                logger.error(f"Focus listener error: {e}")


class FocusRestorer:
    """
    Manages focus restoration from history.

    Example:
        >>> restorer = FocusRestorer(tracker)
        >>> restorer.save_current()
        >>> # ... focus changes ...
        >>> restorer.restore_previous()
    """

    def __init__(self, tracker: FocusTracker) -> None:
        """
        Initialize the focus restorer.

        Args:
            tracker: FocusTracker to use.
        """
        self.tracker = tracker
        self._saved_target: FocusTarget | None = None

    def save_current(self) -> None:
        """Save the current focus target."""
        self._saved_target = self.tracker.get_focused()
        logger.debug(f"Saved focus target: {self._saved_target}")

    def restore_previous(self) -> bool:
        """
        Restore the previously saved focus target.

        Returns:
            True if restoration succeeded.
        """
        if self._saved_target is None:
            logger.warning("No saved focus target to restore")
            return False

        self.tracker.set_focused(self._saved_target)
        logger.info(f"Restored focus to: {self._saved_target.name}")
        return True

    def save_and_lock(self, reason: str) -> None:
        """
        Save current focus and lock it.

        Args:
            reason: Reason for locking.
        """
        self.save_current()
        self.tracker.lock(reason)

    def clear_saved(self) -> None:
        """Clear the saved focus target."""
        self._saved_target = None


class FocusTraversalManager:
    """
    Manages focus traversal order for keyboard navigation.

    Supports custom traversal orders, grouping, and cycling.
    """

    def __init__(self) -> None:
        """Initialize the focus traversal manager."""
        self._groups: dict[str, list[FocusTarget]] = {}
        self._current_index: dict[str, int] = {}
        self._cycling: dict[str, bool] = {}

    def create_group(
        self,
        group_id: str,
        targets: list[FocusTarget],
        cycle: bool = True
    ) -> None:
        """
        Create a focus traversal group.

        Args:
            group_id: Unique group identifier.
            targets: List of targets in traversal order.
            cycle: Whether to cycle back to start.
        """
        self._groups[group_id] = targets
        self._current_index[group_id] = 0
        self._cycling[group_id] = cycle

    def get_next(self, group_id: str) -> FocusTarget | None:
        """
        Get the next target in the traversal order.

        Args:
            group_id: Group identifier.

        Returns:
            Next FocusTarget or None.
        """
        if group_id not in self._groups:
            return None

        targets = self._groups[group_id]
        if not targets:
            return None

        current = self._current_index[group_id]
        next_index = current + 1

        if next_index >= len(targets):
            if self._cycling[group_id]:
                next_index = 0
            else:
                return None

        self._current_index[group_id] = next_index
        return targets[next_index]

    def get_previous(self, group_id: str) -> FocusTarget | None:
        """
        Get the previous target in the traversal order.

        Args:
            group_id: Group identifier.

        Returns:
            Previous FocusTarget or None.
        """
        if group_id not in self._groups:
            return None

        targets = self._groups[group_id]
        if not targets:
            return None

        current = self._current_index[group_id]
        prev_index = current - 1

        if prev_index < 0:
            if self._cycling[group_id]:
                prev_index = len(targets) - 1
            else:
                return None

        self._current_index[group_id] = prev_index
        return targets[prev_index]

    def reset(self, group_id: str) -> None:
        """
        Reset traversal to the beginning.

        Args:
            group_id: Group identifier.
        """
        if group_id in self._current_index:
            self._current_index[group_id] = 0

    def get_current(self, group_id: str) -> FocusTarget | None:
        """
        Get the current target in the traversal order.

        Args:
            group_id: Group identifier.

        Returns:
            Current FocusTarget or None.
        """
        if group_id not in self._groups:
            return None

        targets = self._groups[group_id]
        if not targets:
            return None

        index = self._current_index.get(group_id, 0)
        if 0 <= index < len(targets):
            return targets[index]

        return None

    def set_current_by_id(self, group_id: str, target_id: str) -> bool:
        """
        Set the current target by its ID.

        Args:
            group_id: Group identifier.
            target_id: Target identifier to find.

        Returns:
            True if target was found and set.
        """
        if group_id not in self._groups:
            return False

        targets = self._groups[group_id]
        for i, target in enumerate(targets):
            if target.id == target_id:
                self._current_index[group_id] = i
                return True

        return False


class FocusLock:
    """
    Context manager for temporary focus locking.

    Example:
        >>> lock = FocusLock(tracker, "modal open")
        >>> with lock:
        ...     # focus is locked
        ...     pass
        >>> # focus is unlocked
    """

    def __init__(
        self,
        tracker: FocusTracker,
        reason: str
    ) -> None:
        """
        Initialize the focus lock.

        Args:
            tracker: FocusTracker to use.
            reason: Reason for locking.
        """
        self.tracker = tracker
        self.reason = reason
        self._restorer = FocusRestorer(tracker)

    def __enter__(self) -> None:
        """Enter the context and lock focus."""
        self._restorer.save_current()
        self.tracker.lock(self.reason)

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context and unlock focus."""
        self.tracker.unlock()
        self._restorer.restore_previous()


@dataclass
class FocusPolicy:
    """Policy configuration for focus behavior."""
    allow_foreground_activation: bool = True
    track_all_applications: bool = False
    restore_on_deactivate: bool = True
    focus_follows_mouse: bool = False
    raise_on_click: bool = True


class FocusPolicyManager:
    """
    Manages focus policies across the system.

    Allows configuring and enforcing focus behavior rules.
    """

    def __init__(self) -> None:
        """Initialize the focus policy manager."""
        self._policies: dict[str, FocusPolicy] = {}
        self._default_policy = FocusPolicy()

    def set_policy(self, name: str, policy: FocusPolicy) -> None:
        """
        Set a named policy.

        Args:
            name: Policy name.
            policy: FocusPolicy configuration.
        """
        self._policies[name] = policy
        logger.info(f"Set focus policy: {name}")

    def get_policy(self, name: str) -> FocusPolicy:
        """
        Get a named policy.

        Args:
            name: Policy name.

        Returns:
            FocusPolicy (default if not found).
        """
        return self._policies.get(name, self._default_policy)

    def remove_policy(self, name: str) -> None:
        """
        Remove a named policy.

        Args:
            name: Policy name to remove.
        """
        if name in self._policies:
            del self._policies[name]

    def apply_policy(
        self,
        tracker: FocusTracker,
        policy_name: str
    ) -> bool:
        """
        Apply a policy to a tracker.

        Args:
            tracker: FocusTracker to apply policy to.
            policy_name: Name of policy to apply.

        Returns:
            True if policy was applied.
        """
        policy = self.get_policy(policy_name)

        if policy.raise_on_click:
            logger.debug("Policy: raise_on_click enabled")

        if policy.focus_follows_mouse:
            logger.debug("Policy: focus_follows_mouse enabled")

        return True
