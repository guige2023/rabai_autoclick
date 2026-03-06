import sys
import os
import threading
import time
from typing import Dict, List, Optional, Callable
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QFrame, QApplication, QSizePolicy
)
from PyQt5.QtCore import Qt, pyqtSignal, QTimer, QPropertyAnimation, QEasingCurve, QPoint, QRect
from PyQt5.QtGui import QFont, QColor, QPainter, QPen, QBrush, QLinearGradient, QPainterPath

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
        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setAttribute(Qt.WA_ShowWithoutActivating)
        
        self._layout = QHBoxLayout(self)
        self._layout.setContentsMargins(10, 5, 10, 5)
        self._layout.setSpacing(5)
        
        self._key_labels: List[QLabel] = []
        for _ in range(self._max_keys):
            label = QLabel()
            label.setAlignment(Qt.AlignCenter)
            label.setMinimumSize(50, 50)
            label.setFont(QFont('Microsoft YaHei', 14, QFont.Bold))
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


class MouseIndicatorWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._click_animation = False
        self._click_button = None
        self._init_ui()
    
    def _init_ui(self):
        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setAttribute(Qt.WA_ShowWithoutActivating)
        self.setAttribute(Qt.WA_TransparentForMouseEvents)
        
        self.setFixedSize(60, 60)
        self.hide()
    
    def show_click(self, button: str, x: int, y: int):
        self._click_button = button
        self._click_animation = True
        self.move(x - 30, y - 30)
        self.show()
        
        QTimer.singleShot(300, self._hide_click)
    
    def _hide_click(self):
        self._click_animation = False
        self.hide()
    
    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        
        if self._click_animation:
            if self._click_button == 'left':
                color = QColor(76, 175, 80, 200)
            elif self._click_button == 'right':
                color = QColor(244, 67, 54, 200)
            else:
                color = QColor(33, 150, 243, 200)
            
            painter.setPen(QPen(color, 3))
            painter.setBrush(QBrush(color))
            
            painter.drawEllipse(5, 5, 50, 50)
            
            painter.setPen(QPen(QColor(255, 255, 255, 200), 2))
            painter.setBrush(Qt.NoBrush)
            painter.drawEllipse(10, 10, 40, 40)


class RecordingOverlay(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()
    
    def _init_ui(self):
        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setAttribute(Qt.WA_ShowWithoutActivating)
        
        layout = QVBoxLayout(self)
        
        self.recording_label = QLabel("● 录制中")
        self.recording_label.setStyleSheet("""
            QLabel {
                background-color: rgba(244, 67, 54, 200);
                color: white;
                padding: 8px 16px;
                border-radius: 15px;
                font-size: 14px;
                font-weight: bold;
            }
        """)
        layout.addWidget(self.recording_label)
        
        self.timer_label = QLabel("00:00")
        self.timer_label.setStyleSheet("""
            QLabel {
                background-color: rgba(0, 0, 0, 150);
                color: white;
                padding: 5px 10px;
                border-radius: 10px;
                font-size: 12px;
            }
        """)
        layout.addWidget(self.timer_label)
        
        self._start_time = 0
        self._timer = QTimer()
        self._timer.timeout.connect(self._update_timer)
    
    def start_recording(self):
        self._start_time = time.time()
        self._timer.start(1000)
        self.show()
    
    def stop_recording(self):
        self._timer.stop()
        self.hide()
    
    def _update_timer(self):
        elapsed = int(time.time() - self._start_time)
        minutes = elapsed // 60
        seconds = elapsed % 60
        self.timer_label.setText(f"{minutes:02d}:{seconds:02d}")


class TeachingModeManager:
    _instance = None
    _lock = threading.Lock()
    
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
        self._initialized = True
        
        self._enabled = False
        self._key_display: Optional[KeyDisplayWidget] = None
        self._mouse_indicator: Optional[MouseIndicatorWidget] = None
        self._recording_overlay: Optional[RecordingOverlay] = None
        
        self._mouse_listener = None
        self._keyboard_listener = None
        
        self._pressed_keys: Dict[str, bool] = {}
    
    def is_enabled(self) -> bool:
        return self._enabled
    
    def enable(self) -> bool:
        if not PYNPUT_AVAILABLE:
            return False
        
        if self._enabled:
            return True
        
        app = QApplication.instance()
        if app is None:
            return False
        
        self._key_display = KeyDisplayWidget()
        self._mouse_indicator = MouseIndicatorWidget()
        self._recording_overlay = RecordingOverlay()
        
        screen = app.primaryScreen()
        if screen:
            geometry = screen.geometry()
            self._key_display.move(geometry.width() - 400, 50)
            self._recording_overlay.move(20, 20)
        
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
    
    def disable(self):
        if not self._enabled:
            return
        
        if self._mouse_listener:
            self._mouse_listener.stop()
            self._mouse_listener = None
        
        if self._keyboard_listener:
            self._keyboard_listener.stop()
            self._keyboard_listener = None
        
        if self._key_display:
            self._key_display.hide()
            self._key_display.deleteLater()
            self._key_display = None
        
        if self._mouse_indicator:
            self._mouse_indicator.hide()
            self._mouse_indicator.deleteLater()
            self._mouse_indicator = None
        
        if self._recording_overlay:
            self._recording_overlay.hide()
            self._recording_overlay.deleteLater()
            self._recording_overlay = None
        
        self._pressed_keys.clear()
        self._enabled = False
    
    def toggle(self) -> bool:
        if self._enabled:
            self.disable()
            return False
        else:
            return self.enable()
    
    def _on_mouse_click(self, x, y, button, pressed):
        if not self._enabled or not pressed:
            return
        
        button_name = 'left' if button == mouse.Button.left else 'right' if button == mouse.Button.right else 'middle'
        
        if self._mouse_indicator:
            QTimer.singleShot(0, lambda: self._mouse_indicator.show_click(button_name, x, y))
    
    def _on_key_press(self, key):
        if not self._enabled:
            return
        
        try:
            if hasattr(key, 'char') and key.char:
                key_name = key.char
            elif hasattr(key, 'name'):
                key_name = key.name
            else:
                return
            
            if key_name not in self._pressed_keys:
                self._pressed_keys[key_name] = True
                if self._key_display:
                    QTimer.singleShot(0, lambda k=key_name: self._key_display.add_key(k))
        except Exception:
            pass
    
    def _on_key_release(self, key):
        if not self._enabled:
            return
        
        try:
            if hasattr(key, 'char') and key.char:
                key_name = key.char
            elif hasattr(key, 'name'):
                key_name = key.name
            else:
                return
            
            if key_name in self._pressed_keys:
                del self._pressed_keys[key_name]
                if self._key_display:
                    QTimer.singleShot(0, lambda k=key_name: self._key_display.remove_key(k))
        except Exception:
            pass
    
    def show_recording_overlay(self):
        if self._recording_overlay:
            self._recording_overlay.start_recording()
    
    def hide_recording_overlay(self):
        if self._recording_overlay:
            self._recording_overlay.stop_recording()


teaching_mode_manager = TeachingModeManager()
