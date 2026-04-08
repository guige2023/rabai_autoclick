"""Window stack and z-order utilities.

Provides window management utilities for tracking
window z-order and stack operations.
"""

from typing import List, Optional, Set


class WindowStack:
    """Represents the window z-order stack.

    Example:
        stack = WindowStack()
        stack.push("window1")
        stack.push("window2")
        print(stack.above("window1"))  # "window2"
    """

    def __init__(self) -> None:
        self._stack: List[str] = []
        self._set: Set[str] = set()

    def push(self, window_id: str) -> None:
        """Push window to top of stack.

        Args:
            window_id: Window identifier.
        """
        if window_id in self._set:
            self._stack.remove(window_id)
        self._stack.append(window_id)
        self._set.add(window_id)

    def pop(self) -> Optional[str]:
        """Pop window from top of stack.

        Returns:
            Window ID or None if empty.
        """
        if not self._stack:
            return None
        window_id = self._stack.pop()
        self._set.discard(window_id)
        return window_id

    def remove(self, window_id: str) -> bool:
        """Remove window from stack.

        Args:
            window_id: Window to remove.

        Returns:
            True if window was found.
        """
        if window_id not in self._set:
            return False
        self._stack.remove(window_id)
        self._set.discard(window_id)
        return True

    def above(self, window_id: str) -> Optional[str]:
        """Get window directly above given window.

        Args:
            window_id: Window to query.

        Returns:
            Window above, or None if at top.
        """
        if window_id not in self._set:
            return None
        index = self._stack.index(window_id)
        if index < len(self._stack) - 1:
            return self._stack[index + 1]
        return None

    def below(self, window_id: str) -> Optional[str]:
        """Get window directly below given window.

        Args:
            window_id: Window to query.

        Returns:
            Window below, or None if at bottom.
        """
        if window_id not in self._set:
            return None
        index = self._stack.index(window_id)
        if index > 0:
            return self._stack[index - 1]
        return None

    def top(self) -> Optional[str]:
        """Get window at top of stack.

        Returns:
            Top window ID or None if empty.
        """
        if self._stack:
            return self._stack[-1]
        return None

    def bottom(self) -> Optional[str]:
        """Get window at bottom of stack.

        Returns:
            Bottom window ID or None if empty.
        """
        if self._stack:
            return self._stack[0]
        return None

    def position(self, window_id: str) -> int:
        """Get position of window in stack (0 = bottom).

        Args:
            window_id: Window to query.

        Returns:
            Position or -1 if not found.
        """
        if window_id in self._set:
            return self._stack.index(window_id)
        return -1

    def windows_above(self, window_id: str) -> List[str]:
        """Get all windows above given window.

        Args:
            window_id: Window to query.

        Returns:
            List of window IDs above.
        """
        if window_id not in self._set:
            return []
        index = self._stack.index(window_id)
        return list(reversed(self._stack[index + 1:]))

    def windows_below(self, window_id: str) -> List[str]:
        """Get all windows below given window.

        Args:
            window_id: Window to query.

        Returns:
            List of window IDs below.
        """
        if window_id not in self._set:
            return []
        index = self._stack.index(window_id)
        return self._stack[:index]

    def __len__(self) -> int:
        return len(self._stack)

    def __contains__(self, window_id: str) -> bool:
        return window_id in self._set

    def __iter__(self):
        return iter(reversed(self._stack))

    def to_list(self) -> List[str]:
        """Get windows from top to bottom."""
        return list(reversed(self._stack))


class WindowManager:
    """Manages window stacking and relationships.

    Example:
        wm = WindowManager()
        wm.activate("window1")
        wm.activate("window2")
        print(wm.focused)  # "window2"
    """

    def __init__(self) -> None:
        self._stack = WindowStack()
        self._focused: Optional[str] = None
        self._callbacks: dict = {}

    @property
    def focused(self) -> Optional[str]:
        """Get currently focused window."""
        return self._focused

    @property
    def stack(self) -> WindowStack:
        """Get window stack."""
        return self._stack

    def activate(self, window_id: str) -> None:
        """Activate (focus) a window.

        Args:
            window_id: Window to activate.
        """
        self._stack.push(window_id)
        self._focused = window_id

    def minimize(self, window_id: str) -> bool:
        """Minimize a window (remove from stack).

        Args:
            window_id: Window to minimize.

        Returns:
            True if window was in stack.
        """
        result = self._stack.remove(window_id)
        if self._focused == window_id:
            self._focused = self._stack.top()
        return result

    def restore(self, window_id: str) -> None:
        """Restore a minimized window.

        Args:
            window_id: Window to restore.
        """
        self._stack.push(window_id)

    def raise_window(self, window_id: str) -> None:
        """Raise window to top.

        Args:
            window_id: Window to raise.
        """
        self._stack.remove(window_id)
        self._stack.push(window_id)

    def lower_window(self, window_id: str) -> None:
        """Lower window to bottom.

        Args:
            window_id: Window to lower.
        """
        self._stack.remove(window_id)
        self._stack._stack.insert(0, window_id)  # type: ignore
        self._stack._set.add(window_id)

    def swap(self, window1: str, window2: str) -> bool:
        """Swap positions of two windows.

        Args:
            window1: First window.
            window2: Second window.

        Returns:
            True if successful.
        """
        if window1 not in self._stack or window2 not in self._stack:
            return False

        pos1 = self._stack.position(window1)
        pos2 = self._stack.position(window2)

        self._stack.remove(window1)
        self._stack.remove(window2)

        if pos1 < pos2:
            self._stack._stack.insert(pos1, window2)
            self._stack._stack.insert(pos2, window1)
        else:
            self._stack._stack.insert(pos2, window1)
            self._stack._stack.insert(pos1, window2)

        self._stack._set.add(window1)
        self._stack._set.add(window2)

        return True
