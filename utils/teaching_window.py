#!/usr/bin/env python3
"""Teaching window utilities for RabAI AutoClick.

Provides a floating overlay window that displays pressed keys
and click animations for teaching and demonstration mode.
"""

import os
import sys
from typing import Any, Dict, List, Optional

from PyQt5.QtCore import QPoint, Qt, QTimer
from PyQt5.QtGui import QBrush, QColor, QFont, QPainter, QPen
from PyQt5.QtWidgets import (
    QApplication, QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget
)


# Check pynput availability
try:
    from pynput import mouse, keyboard
    PYNPUT_AVAILABLE: bool = True
except ImportError:
    PYNPUT_AVAILABLE = False


class TeachingWindow(QWidget):
    """Floating teaching window that displays keys and click animations.
    
    Shows pressed keyboard keys and animated click indicators
    for teaching and demonstration purposes.
    """
    
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        """Initialize the teaching window.
        
        Args:
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self._keys: List[str] = []
        self._max_keys: int = 8
        self._click_animations: List[Dict[str, Any]] = []
        self._init_ui()
    
    def _init_ui(self) -> None:
        """Initialize the window UI."""
        self.setWindowFlags(
            Qt.Window |
            Qt.FramelessWindowHint |
            Qt.WindowStaysOnTopHint |
            Qt.Tool
        )
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setAttribute(Qt.WA_ShowWithoutActivating)
        
        self.setFixedSize(400, 80)
        self.setStyleSheet("""
            QWidget {
                background-color: rgba(0, 0, 0, 180);
                border-radius: 10px;
            }
        """)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)
        
        self.keys_container = QWidget()
        keys_layout = QHBoxLayout(self.keys_container)
        keys_layout.setSpacing(3)
        keys_layout.addStretch()
        layout.addWidget(self.keys_container)
        
        self.status_label = QLabel("教学模式已开启")
        self.status_label.setStyleSheet("""
            QLabel {
                color: white;
                background-color: rgba(244, 67, 54, 200);
                padding: 5px 12px;
                border-radius: 5px;
                font-size: 12px;
            }
        """)
        self.status_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.status_label)
        
        self.minimize_btn = QPushButton("─")
        self.minimize_btn.setFixedSize(30, 20)
        self.minimize_btn.setStyleSheet("""
            QPushButton {
                background-color: rgba(255, 255, 255, 100);
                border: none;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: rgba(255, 255, 255, 200);
            }
        """)
        self.minimize_btn.clicked.connect(self.showMinimized)
        layout.addWidget(self.minimize_btn, 0, Qt.AlignRight)
        
        self._update_keys_display()
    
    def add_key(self, key: str) -> None:
        """Add a key to the display.
        
        Args:
            key: Key name to display.
        """
        key = self._format_key(key)
        
        if key in self._keys:
            self._keys.remove(key)
        
        if len(self._keys) >= self._max_keys:
            self._keys.pop(0)
        
        self._keys.append(key)
        self._update_keys_display()
    
    def remove_key(self, key: str) -> None:
        """Remove a key from the display.
        
        Args:
            key: Key name to remove.
        """
        key = self._format_key(key)
        
        if key in self._keys:
            self._keys.remove(key)
            self._update_keys_display()
    
    def _format_key(self, key: str) -> str:
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
            'f1': 'F1',
            'f2': 'F2',
            'f3': 'F3',
            'f4': 'F4',
            'f5': 'F5',
            'f6': 'F6',
            'f7': 'F7',
            'f8': 'F8',
            'f9': 'F9',
            'f10': 'F10',
            'f11': 'F11',
            'f12': 'F12',
        }
        return key_map.get(key.lower(), key.upper())
    
    def _update_keys_display(self) -> None:
        """Update the displayed key labels."""
        for i in range(self.keys_container.layout().count()):
            widget = self.keys_container.layout().itemAt(i).widget()
            widget.deleteLater()
        
        keys_layout = QHBoxLayout(self.keys_container)
        keys_layout.setSpacing(3)
        keys_layout.addStretch()
        
        for key in self._keys:
            label = QLabel(key)
            label.setStyleSheet("""
                QLabel {
                    color: white;
                    background-color: rgba(33, 150, 243, 220);
                    padding: 8px 12px;
                    border-radius: 6px;
                    font-size: 14px;
                    font-weight: bold;
                    min-width: 40px;
                }
            """)
            label.setAlignment(Qt.AlignCenter)
            keys_layout.addWidget(label)
        
        self.keys_container.setLayout(keys_layout)
    
    def show_click(self, x: int, y: int, button: str) -> None:
        """Show a click animation at the specified position.
        
        Args:
            x: X coordinate of click.
            y: Y coordinate of click.
            button: Button name ('left', 'right', 'middle').
        """
        colors: Dict[str, str] = {
            'left': '#4CAF50',
            'right': '#f44336',
            'middle': '#2196F3'
        }
        color = colors.get(button, '#2196F3')
        
        animation: Dict[str, Any] = {
            'x': x,
            'y': y,
            'button': button,
            'color': color,
            'radius': 30,
            'alpha': 255,
            'start_time': 0
        }
        
        self._click_animations.append(animation)
        QTimer.singleShot(0, lambda: self._animate_click(animation))
    
    def _animate_click(self, animation: Dict[str, Any]) -> None:
        """Run a click animation.
        
        Args:
            animation: Animation parameters dictionary.
        """
        animation['start_time'] = animation['start_time'] or 0
        elapsed = 0
        duration = 300
        
        def update() -> None:
            nonlocal elapsed
            elapsed += 16
            progress = min(elapsed / duration, 1.0)
            alpha = int(255 * (1 - progress))
            radius = int(animation['radius'] * progress)
            
            self.update()
            
            if elapsed < duration:
                QTimer.singleShot(16, update)
            else:
                if animation in self._click_animations:
                    self._click_animations.remove(animation)
        
        update()
    
    def paintEvent(self, event) -> None:
        """Paint click animations.
        
        Args:
            event: Paint event.
        """
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        for animation in self._click_animations:
            elapsed = animation['start_time']
            if elapsed:
                current_time = elapsed
            else:
                current_time = 0
            
            anim_duration = 300
            progress = min(current_time / anim_duration, 1.0)
            alpha = int(255 * (1 - progress))
            radius = int(animation['radius'] * progress)
            
            color = QColor(animation['color'])
            color.setAlpha(alpha)
            
            painter.setPen(QPen(color, 3))
            painter.setBrush(QBrush(color))
            painter.drawEllipse(
                animation['x'] - radius,
                animation['y'] - radius,
                radius * 2,
                radius * 2
            )
        
        painter.end()
    
    def clear_keys(self) -> None:
        """Clear all displayed keys."""
        self._keys.clear()
        self._update_keys_display()
    
    def clear_clicks(self) -> None:
        """Clear all click animations."""
        self._click_animations.clear()
        self.update()


class TeachingModeManager:
    """Manager for teaching mode functionality.
    
    Singleton class that manages the teaching window and
    pynput listeners for capturing input.
    """
    
    _instance: Optional['TeachingModeManager'] = None
    
    def __new__(cls) -> 'TeachingModeManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self) -> None:
        """Initialize the teaching mode manager."""
        self._window: Optional[TeachingWindow] = None
        self._enabled: bool = False
        self._mouse_listener: Optional[Any] = None
        self._keyboard_listener: Optional[Any] = None
    
    def is_enabled(self) -> bool:
        """Check if teaching mode is enabled.
        
        Returns:
            True if enabled.
        """
        return self._enabled
    
    def enable(self) -> bool:
        """Enable teaching mode.
        
        Returns:
            True if enabled successfully.
        """
        if not PYNPUT_AVAILABLE:
            return False
        
        if self._enabled:
            return True
        
        app = QApplication.instance()
        if app is None:
            return False
        
        self._window = TeachingWindow()
        self._window.show()
        
        try:
            self._mouse_listener = mouse.Listener(
                on_click=self._on_mouse_click
            )
            self._keyboard_listener = keyboard.Listener(
                on_press=self._on_key_press,
                on_release=self._on_key_release
            )
            
            self._mouse_listener.start()
            self._keyboard_listener.start()
            
            self._enabled = True
            return True
        except Exception as e:
            print(f"教学模式启动失败: {e}")
            return False
    
    def disable(self) -> None:
        """Disable teaching mode."""
        if not self._enabled:
            return
        
        if self._mouse_listener:
            self._mouse_listener.stop()
            self._mouse_listener = None
        
        if self._keyboard_listener:
            self._keyboard_listener.stop()
            self._keyboard_listener = None
        
        if self._window:
            self._window.close()
            self._window = None
        
        self._enabled = False
    
    def toggle(self) -> bool:
        """Toggle teaching mode on/off.
        
        Returns:
            True if enabled after toggle.
        """
        if self._enabled:
            self.disable()
            return False
        else:
            return self.enable()
    
    def _on_mouse_click(
        self,
        x: int,
        y: int,
        button: mouse.Button,
        pressed: bool
    ) -> None:
        """Handle mouse click event.
        
        Args:
            x: Mouse X coordinate.
            y: Mouse Y coordinate.
            button: Mouse button.
            pressed: True if button was pressed.
        """
        if not self._enabled or not pressed:
            return
        
        button_name = (
            'left' if button == mouse.Button.left
            else 'right' if button == mouse.Button.right
            else 'middle'
        )
        self._window.show_click(x, y, button_name)
    
    def _on_key_press(self, key: keyboard.Key) -> None:
        """Handle key press event.
        
        Args:
            key: Key that was pressed.
        """
        if not self._enabled:
            return
        
        try:
            if hasattr(key, 'char') and key.char:
                key_name = key.char
            elif hasattr(key, 'name'):
                key_name = key.name
            else:
                return
            
            self._window.add_key(key_name)
        except Exception:
            pass
    
    def _on_key_release(self, key: keyboard.Key) -> None:
        """Handle key release event.
        
        Args:
            key: Key that was released.
        """
        if not self._enabled:
            return
        
        try:
            if hasattr(key, 'char') and key.char:
                key_name = key.char
            elif hasattr(key, 'name'):
                key_name = key.name
            else:
                return
            
            self._window.remove_key(key_name)
        except Exception:
            pass


# Global singleton instance
teaching_mode_manager: TeachingModeManager = TeachingModeManager()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    
    window = TeachingWindow()
    window.show()
    
    sys.exit(app.exec_())
