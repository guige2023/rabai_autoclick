from PyQt5.QtWidgets import (
    QMessageBox, QDialog, QVBoxLayout, QHBoxLayout, QLabel, 
    QPushButton, QTextEdit, QWidget, QApplication
)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal
from PyQt5.QtGui import QFont, QIcon
from typing import Optional, Callable
import sys


class ToastWidget(QWidget):
    closed = pyqtSignal()
    
    def __init__(self, message: str, level: str = 'info', duration: int = 3000, parent=None):
        super().__init__(parent)
        self._duration = duration
        self._init_ui(message, level)
        
        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint)
        self.setAttribute(Qt.WA_TranslucentBackground)
        
        QTimer.singleShot(duration, self.close)
    
    def _init_ui(self, message: str, level: str):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(15, 10, 15, 10)
        
        colors = {
            'info': '#2196F3',
            'success': '#4CAF50',
            'warning': '#FF9800',
            'error': '#f44336'
        }
        icons = {
            'info': 'ℹ',
            'success': '✓',
            'warning': '⚠',
            'error': '✗'
        }
        
        color = colors.get(level, colors['info'])
        icon = icons.get(level, icons['info'])
        
        self.setStyleSheet(f"""
            QWidget {{
                background-color: {color};
                border-radius: 8px;
            }}
            QLabel {{
                color: white;
                font-size: 14px;
            }}
        """)
        
        icon_label = QLabel(icon)
        icon_label.setFont(QFont('Segoe UI', 16))
        icon_label.setStyleSheet("color: white;")
        layout.addWidget(icon_label)
        
        msg_label = QLabel(message)
        msg_label.setWordWrap(True)
        msg_label.setMaximumWidth(400)
        layout.addWidget(msg_label)
    
    def show_at_corner(self):
        screen = QApplication.primaryScreen()
        if screen:
            screen_geometry = screen.availableGeometry()
            self.move(
                screen_geometry.width() - self.width() - 20,
                screen_geometry.height() - self.height() - 60
            )
        self.show()


class MessageManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._parent_widget = None
    
    def set_parent(self, parent: QWidget):
        self._parent_widget = parent
    
    def info(self, title: str, message: str, parent: QWidget = None) -> None:
        parent = parent or self._parent_widget
        QMessageBox.information(parent, title, message)
    
    def success(self, title: str, message: str, parent: QWidget = None) -> None:
        parent = parent or self._parent_widget
        msg_box = QMessageBox(parent)
        msg_box.setIcon(QMessageBox.Information)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        msg_box.setStyleSheet("""
            QMessageBox {
                background-color: #f0f9eb;
            }
            QPushButton {
                background-color: #4CAF50;
                color: white;
                padding: 8px 20px;
                border: none;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """)
        msg_box.exec_()
    
    def warning(self, title: str, message: str, parent: QWidget = None) -> None:
        parent = parent or self._parent_widget
        QMessageBox.warning(parent, title, message)
    
    def error(self, title: str, message: str, parent: QWidget = None) -> None:
        parent = parent or self._parent_widget
        msg_box = QMessageBox(parent)
        msg_box.setIcon(QMessageBox.Critical)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        msg_box.setStyleSheet("""
            QMessageBox {
                background-color: #fef0f0;
            }
            QPushButton {
                background-color: #f44336;
                color: white;
                padding: 8px 20px;
                border: none;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #da190b;
            }
        """)
        msg_box.exec_()
    
    def question(self, title: str, message: str, parent: QWidget = None) -> bool:
        parent = parent or self._parent_widget
        reply = QMessageBox.question(
            parent, title, message,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        return reply == QMessageBox.Yes
    
    def confirm(self, title: str, message: str, parent: QWidget = None) -> bool:
        return self.question(title, message, parent)
    
    def toast(self, message: str, level: str = 'info', duration: int = 3000) -> None:
        toast = ToastWidget(message, level, duration)
        toast.show_at_corner()
    
    def toast_success(self, message: str, duration: int = 3000) -> None:
        self.toast(message, 'success', duration)
    
    def toast_error(self, message: str, duration: int = 3000) -> None:
        self.toast(message, 'error', duration)
    
    def toast_warning(self, message: str, duration: int = 3000) -> None:
        self.toast(message, 'warning', duration)
    
    def toast_info(self, message: str, duration: int = 3000) -> None:
        self.toast(message, 'info', duration)


message_manager = MessageManager()


def show_error(title: str, message: str, parent: QWidget = None):
    message_manager.error(title, message, parent)


def show_success(title: str, message: str, parent: QWidget = None):
    message_manager.success(title, message, parent)


def show_warning(title: str, message: str, parent: QWidget = None):
    message_manager.warning(title, message, parent)


def show_info(title: str, message: str, parent: QWidget = None):
    message_manager.info(title, message, parent)


def show_question(title: str, message: str, parent: QWidget = None) -> bool:
    return message_manager.question(title, message, parent)


def show_toast(message: str, level: str = 'info', duration: int = 3000):
    message_manager.toast(message, level, duration)
