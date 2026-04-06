"""Teaching mode utilities for RabAI AutoClick.

Provides on-screen key and mouse position display widgets for
recording and teaching automation workflows.
"""

import threading
import time
from typing import Dict, List, Optional

from PyQt5.QtCore import Qt, pyqtSignal, QTimer, QObject
from PyQt5.QtGui import QFont, QPainter, QPen, QBrush
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QApplication
)


# Check pynput availability
try:
    from pynput import mouse, keyboard
    PYNPUT_AVAILABLE: bool = True
except ImportError:
    PYNPUT_AVAILABLE = False


class KeyDisplayWidget(QWidget):
    """Widget that displays pressed keys on screen."""
    
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        """Initialize the key display widget.
        
        Args:
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self._keys: List[str] = []
        self._max_keys: int = 5
        self._key_labels: List[QLabel] = []
        self._init_ui()
    
    def _init_ui(self) -> None:
        """Initialize the widget UI."""
        self.setWindowFlags(
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.FramelessWindowHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating)
        
        self._layout = QHBoxLayout(self)
        self._layout.setContentsMargins(10, 5, 10, 5)
        self._layout.setSpacing(5)
        
        for _ in range(self._max_keys):
            label = QLabel()
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            label.setMinimumSize(50, 50)
            label.setFont(QFont('Arial', 14, QFont.Weight.Bold))
            label.setStyleSheet("""
                QLabel {
                    background-color: rgba(0, 0, 0, 180);
                    color: white;
                    border-radius: 8px;
                    padding: 5px 10px;
                }
            """)
            label.hide()
            self._layout.addWidget(label)
            self._key_labels.append(label)
        
        self.adjustSize()
    
    def add_key(self, key: str) -> None:
        """Add a key to the display.
        
        Args:
            key: Key name to display.
        """
        key = self._format_key(key)
        
        if len(self._keys) >= self._max_keys:
            self._keys.pop(0)
        
        self._keys.append(key)
        self._update_display()
        self.show()
    
    def remove_key(self, key: str) -> None:
        """Remove a key from the display.
        
        Args:
            key: Key name to remove.
        """
        key = self._format_key(key)
        if key in self._keys:
            self._keys.remove(key)
        self._update_display()
        
        if not self._keys:
            self.hide()
    
    @staticmethod
    def _format_key(key: str) -> str:
        """Format a key name for display with Unicode symbols.
        
        Args:
            key: Raw key name.
            
        Returns:
            Formatted key name with Unicode symbols.
        """
        key_map: Dict[str, str] = {
            'ctrl': 'Ctrl',
            'shift': 'Shift',
            'alt': 'Alt',
            'cmd': '⌘',
            'command': '⌘',
            'space': '␣',
            'enter': '↵',
            'return': '↵',
            'tab': '⇥',
            'backspace': '⌫',
            'delete': '⌦',
            'escape': 'Esc',
            'up': '↑',
            'down': '↓',
            'left': '←',
            'right': '→',
            'caps_lock': '⇪',
        }
        return key_map.get(key.lower(), key.upper())
    
    def _update_display(self) -> None:
        """Update the key labels based on current keys."""
        for i, label in enumerate(self._key_labels):
            if i < len(self._keys):
                label.setText(self._keys[i])
                label.show()
            else:
                label.hide()
        self.adjustSize()
    
    def clear(self) -> None:
        """Clear all displayed keys and hide the widget."""
        self._keys.clear()
        self._update_display()
        self.hide()


class MousePositionWidget(QWidget):
    """Widget that displays current mouse position on screen."""
    
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        """Initialize the mouse position widget.
        
        Args:
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self._init_ui()
    
    def _init_ui(self) -> None:
        """Initialize the widget UI."""
        self.setWindowFlags(
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.FramelessWindowHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating)
        
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(10, 5, 10, 5)
        
        self._position_label = QLabel("鼠标: (0, 0)")
        self._position_label.setFont(QFont('Arial', 12))
        self._position_label.setStyleSheet("""
            QLabel {
                background-color: rgba(0, 120, 215, 200);
                color: white;
                border-radius: 8px;
                padding: 8px 12px;
            }
        """)
        self._layout.addWidget(self._position_label)
        
        self._tip_label = QLabel("按 ESC 关闭显示")
        self._tip_label.setFont(QFont('Arial', 10))
        self._tip_label.setStyleSheet("""
            QLabel {
                background-color: rgba(100, 100, 100, 180);
                color: white;
                border-radius: 5px;
                padding: 5px 10px;
            }
        """)
        self._layout.addWidget(self._tip_label)
        
        self.adjustSize()
    
    def update_position(self, x: int, y: int) -> None:
        """Update the displayed mouse position.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
        """
        self._position_label.setText(f"鼠标: ({x}, {y})")
        self.adjustSize()


class KeyDisplayManager(QObject):
    """Manages key and mouse position display widgets."""
    
    def __init__(self) -> None:
        """Initialize the key display manager."""
        super().__init__()
        self._enabled: bool = False
    
    def enable(self) -> bool:
        """Enable the key display (stub - actual implementation in subclass).
        
        Returns:
            True if enabled successfully.
        """
        self._enabled = True
        return True
    
    def disable(self) -> None:
        """Disable the key display."""
        self._enabled = False
    
    def is_enabled(self) -> bool:
        """Check if key display is enabled.
        
        Returns:
            True if enabled.
        """
        return self._enabled
    
    def toggle(self) -> bool:
        """Toggle key display on/off.
        
        Returns:
            True if enabled after toggle.
        """
        if self._enabled:
            self.disable()
            return False
        else:
            self.enable()
            return True
