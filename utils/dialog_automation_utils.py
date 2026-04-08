"""
Dialog Automation Utilities.

Utilities for automating standard macOS dialogs including
file open/save dialogs, color pickers, font panels, and more.

Usage:
    from utils.dialog_automation_utils import DialogAutomator, FileDialogAutomator

    automator = FileDialogAutomator()
    automator.open_file("/path/to/file.txt")
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    pass


@dataclass
class DialogField:
    """A field in a dialog (text field, checkbox, etc.)."""
    name: str
    role: str
    value: Any = None
    element: Optional[Dict[str, Any]] = None


class DialogAutomator:
    """
    Base class for automating dialogs.

    Provides common utilities for interacting with standard
    macOS dialog elements.

    Example:
        automator = DialogAutomator(bridge)
        automator.click_button("Cancel")
        automator.enter_text("file_name.txt", in_field="Save As:")
    """

    def __init__(self, bridge: Any) -> None:
        """
        Initialize the dialog automator.

        Args:
            bridge: An AccessibilityBridge instance.
        """
        self._bridge = bridge

    def find_button(
        self,
        label: str,
        exact: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Find a button in the current dialog by label.

        Args:
            label: Button label to search for.
            exact: If True, require exact match.

        Returns:
            Button element dictionary or None.
        """
        dialog = self._find_dialog()
        if dialog is None:
            return None

        return self._search_element(
            dialog,
            lambda e: e.get("role") == "button" and self._label_matches(e, label, exact),
        )

    def _label_matches(
        self,
        element: Dict[str, Any],
        label: str,
        exact: bool,
    ) -> bool:
        """Check if an element's label matches."""
        title = element.get("title", "")
        if exact:
            return title.lower() == label.lower()
        return label.lower() in title.lower()

    def click_button(
        self,
        label: str,
        exact: bool = False,
    ) -> bool:
        """
        Click a button in the current dialog.

        Args:
            label: Button label.
            exact: Require exact label match.

        Returns:
            True if successful.
        """
        button = self.find_button(label, exact)
        if button is None:
            return False

        try:
            bounds = button.get("rect", {})
            cx = bounds.get("x", 0) + bounds.get("width", 0) / 2
            cy = bounds.get("y", 0) + bounds.get("height", 0) / 2
            self._bridge.click_at(cx, cy)
            return True
        except Exception:
            return False

    def enter_text(
        self,
        text: str,
        in_field: Optional[str] = None,
    ) -> bool:
        """
        Enter text, optionally in a specific text field.

        Args:
            text: Text to enter.
            in_field: Optional field label to enter text into.

        Returns:
            True if successful.
        """
        if in_field:
            field = self._find_text_field(in_field)
            if field:
                try:
                    self._bridge.click_element(field)
                    import time
                    time.sleep(0.1)
                except Exception:
                    pass

        self._bridge.type_text(text)
        return True

    def _find_dialog(self) -> Optional[Dict[str, Any]]:
        """Find the current dialog element."""
        app = self._bridge.get_frontmost_app()
        if app is None:
            return None

        tree = self._bridge.build_accessibility_tree(app)
        return self._search_element(
            tree,
            lambda e: e.get("role") in ("dialog", "sheet", "alert", "window"),
        )

    def _find_text_field(
        self,
        label: str,
    ) -> Optional[Dict[str, Any]]:
        """Find a text field by its label."""
        dialog = self._find_dialog()
        if dialog is None:
            return None

        return self._search_element(
            dialog,
            lambda e: e.get("role") == "text_field" and
                     label.lower() in e.get("title", "").lower(),
        )

    def _search_element(
        self,
        node: Dict[str, Any],
        predicate: Callable[[Dict[str, Any]], bool],
    ) -> Optional[Dict[str, Any]]:
        """Recursively search for an element matching a predicate."""
        if predicate(node):
            return node

        for child in node.get("children", []):
            result = self._search_element(child, predicate)
            if result:
                return result

        return None


class FileDialogAutomator(DialogAutomator):
    """
    Automate file open and file save dialogs.

    Example:
        automator = FileDialogAutomator(bridge)
        automator.open_file("/Users/name/Documents/file.txt")
    """

    def open_file(
        self,
        path: str,
        dialog_type: str = "open",
    ) -> bool:
        """
        Open a file using the file dialog.

        Args:
            path: Full path to the file.
            dialog_type: "open" or "save".

        Returns:
            True if successful.
        """
        import os
        dirname = os.path.dirname(path)
        filename = os.path.basename(path)

        if dirname:
            self.enter_text(dirname, in_field="Go to folder:") or \
            self.enter_text(dirname, in_field="Path:") or \
            self.enter_text(dirname, in_field="")

        import time
        time.sleep(0.2)

        self.enter_text(filename, in_field="") or self.enter_text(filename)

        time.sleep(0.1)

        button = "Open" if dialog_type == "open" else "Save"
        return self.click_button(button, exact=True)

    def save_file(
        self,
        path: str,
        overwrite: bool = False,
    ) -> bool:
        """
        Save a file using the save dialog.

        Args:
            path: Full path for the save location.
            overwrite: Whether to overwrite if file exists.

        Returns:
            True if successful.
        """
        return self.open_file(path, dialog_type="save")

    def select_from_list(
        self,
        filename: str,
    ) -> bool:
        """
        Select a file from the file list.

        Args:
            filename: Name of the file to select.

        Returns:
            True if successful.
        """
        dialog = self._find_dialog()
        if dialog is None:
            return False

        table = self._search_element(
            dialog,
            lambda e: e.get("role") == "table",
        )

        if table is None:
            return False

        for row in table.get("children", []):
            if row.get("title") == filename or row.get("name") == filename:
                try:
                    bounds = row.get("rect", {})
                    cx = bounds.get("x", 0) + bounds.get("width", 0) / 2
                    cy = bounds.get("y", 0) + bounds.get("height", 0) / 2
                    self._bridge.double_click_at(cx, cy)
                    return True
                except Exception:
                    pass

        return False


class ColorPickerAutomator:
    """
    Automate the macOS color picker dialog.

    Example:
        picker = ColorPickerAutomator(bridge)
        picker.set_hex_color("#FF5500")
    """

    def __init__(self, bridge: Any) -> None:
        self._bridge = bridge

    def set_hex_color(self, hex_color: str) -> bool:
        """
        Set color using hex string.

        Args:
            hex_color: Color in #RRGGBB or #RGB format.

        Returns:
            True if successful.
        """
        hex_color = hex_color.lstrip("#")
        if len(hex_color) == 3:
            hex_color = "".join(c * 2 for c in hex_color)

        try:
            r = int(hex_color[0:2], 16)
            g = int(hex_color[2:4], 16)
            b = int(hex_color[4:6], 16)
        except ValueError:
            return False

        return self.set_rgb_color(r, g, b)

    def set_rgb_color(
        self,
        r: int,
        g: int,
        b: int,
    ) -> bool:
        """
        Set color using RGB values (0-255).

        Args:
            r: Red component.
            g: Green component.
            b: Blue component.

        Returns:
            True if successful.
        """
        self._bridge.type_text(str(r))
        return True


class FontPanelAutomator:
    """
    Automate the macOS font panel.

    Example:
        automator = FontPanelAutomator(bridge)
        automator.set_font_family("Helvetica")
        automator.set_font_size(14)
    """

    def __init__(self, bridge: Any) -> None:
        self._bridge = bridge

    def set_font_family(self, family: str) -> bool:
        """Set the font family in the font panel."""
        try:
            self._bridge.click_button_with_title("Font Family")
            import time
            time.sleep(0.1)
            self._bridge.type_text(family)
            return True
        except Exception:
            return False

    def set_font_size(self, size: int) -> bool:
        """Set the font size in the font panel."""
        try:
            size_field = self._find_size_field()
            if size_field:
                self._bridge.click_element(size_field)
                import time
                time.sleep(0.1)
                self._bridge.type_text(str(size))
                return True
        except Exception:
            return False
        return False

    def _find_size_field(self) -> Optional[Dict[str, Any]]:
        """Find the font size text field."""
        return None
