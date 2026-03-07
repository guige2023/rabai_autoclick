import sys
import threading
import time
from typing import Dict, List, Optional
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QApplication
)
from PyQt5.QtCore import Qt, pyqtSignal, QTimer, QObject
from PyQt5.QtGui import QFont, QColor, QPainter, QPen, QBrush

try:
    from pynput import mouse, keyboard
    PYNPUT_AVAILABLE = True
except ImportError:
    PYNPUT_AVAILABLE = False


class KeyDisplayWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._keys: List[str] = []
        self._max_keys = 5
        self._init_ui()
    
    def _init_ui(self):
        self.setWindowFlags(
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.FramelessWindowHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating)
        
        self._layout = QHBoxLayout(self)
        self._layout.setContentsMargins(10, 5, 10, 5)
        self._layout.setSpacing(5)
        
        self._key_labels: List[QLabel] = []
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
    
    def add_key(self, key: str):
        key = self._format_key(key)
        
        if len(self._keys) >= self._max_keys:
            self._keys.pop(0)
        
        self._keys.append(key)
        self._update_display()
        self.show()
    
    def remove_key(self, key: str):
        key = self._format_key(key)
        if key in self._keys:
            self._keys.remove(key)
        self._update_display()
        
        if not self._keys:
            self.hide()
    
    def _format_key(self, key: str) -> str:
        key_map = {
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
    
    def _update_display(self):
        for i, label in enumerate(self._key_labels):
            if i < len(self._keys):
                label.setText(self._keys[i])
                label.show()
            else:
                label.hide()
        self.adjustSize()
    
    def clear(self):
        self._keys.clear()
        self._update_display()
        self.hide()


class MousePositionWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()
    
    def _init_ui(self):
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
    
    def update_position(self, x: int, y: int):
        self._position_label.setText(f"鼠标: ({x}, {y})")
        self.adjustSize()


class KeyDisplayManager(QObject):
    _instance = None
    _lock = threading.Lock()
    
    toggled = pyqtSignal(bool)
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        super().__init__()
        self._initialized = True
        
        self._enabled = False
        self._key_display: Optional[KeyDisplayWidget] = None
        self._mouse_position_widget: Optional[MousePositionWidget] = None
        self._position_timer: Optional[QTimer] = None
        
        self._pressed_keys: Dict[str, bool] = {}
    
    def is_enabled(self) -> bool:
        return self._enabled
    
    def enable(self) -> bool:
        if self._enabled:
            return True
        
        try:
            app = QApplication.instance()
            if app is None:
                return False
            
            self._key_display = KeyDisplayWidget()
            self._mouse_position_widget = MousePositionWidget()
            
            screen = app.primaryScreen()
            if screen:
                geometry = screen.geometry()
                self._key_display.move(geometry.width() - 400, 50)
                self._mouse_position_widget.move(20, geometry.height() - 100)
            
            self._mouse_position_widget.show()
            
            self._position_timer = QTimer()
            self._position_timer.timeout.connect(self._update_mouse_position)
            self._position_timer.start(50)
            
            self._enabled = True
            self.toggled.emit(True)
            
            return True
            
        except Exception as e:
            print(f"[KeyDisplay] Error enabling: {e}")
            import traceback
            traceback.print_exc()
            self._cleanup()
            return False
    
    def disable(self):
        if not self._enabled:
            return
        
        self._cleanup()
        self._enabled = False
        self.toggled.emit(False)
    
    def toggle(self) -> bool:
        if self._enabled:
            self.disable()
            return False
        else:
            return self.enable()
    
    def _cleanup(self):
        if self._position_timer:
            self._position_timer.stop()
            self._position_timer.deleteLater()
            self._position_timer = None
        
        if self._key_display:
            self._key_display.hide()
            self._key_display.deleteLater()
            self._key_display = None
        
        if self._mouse_position_widget:
            self._mouse_position_widget.hide()
            self._mouse_position_widget.deleteLater()
            self._mouse_position_widget = None
        
        self._pressed_keys.clear()
    
    def _update_mouse_position(self):
        if not self._enabled:
            return
        
        try:
            cursor = QApplication.instance().queryKeyboardModifiers()
            pos = QApplication.instance().desktop().cursor().pos()
            if self._mouse_position_widget:
                self._mouse_position_widget.update_position(pos.x(), pos.y())
        except Exception:
            pass
    
    def on_key_press(self, key_name: str):
        if not self._enabled:
            return
        
        if key_name not in self._pressed_keys:
            self._pressed_keys[key_name] = True
            if self._key_display:
                self._key_display.add_key(key_name)
    
    def on_key_release(self, key_name: str):
        if not self._enabled:
            return
        
        if key_name in self._pressed_keys:
            del self._pressed_keys[key_name]
            if self._key_display:
                self._key_display.remove_key(key_name)


key_display_manager = KeyDisplayManager()
