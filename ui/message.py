"""Message and notification utilities for RabAI AutoClick.

Provides toast notifications, message boxes, and a centralized
message manager for user feedback with animations.
"""

from typing import Dict, Optional

from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QEasingCurve, QPropertyAnimation
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import (
    QApplication, QLabel, QMessageBox, QWidget, QGraphicsOpacityEffect
)

from ui.theme import theme_manager, ThemeType


# Message level types
MessageLevel = str  # Literal['info', 'success', 'warning', 'error']


class ToastWidget(QWidget):
    """Floating toast notification widget with fade animations."""

    closed = pyqtSignal()

    # Icon mappings for different message levels (colors come from theme)
    ICONS: Dict[str, str] = {
        'info': 'ℹ',
        'success': '✓',
        'warning': '⚠',
        'error': '✗'
    }

    def __init__(
        self,
        message: str,
        level: MessageLevel = 'info',
        duration: int = 3000,
        parent: Optional[QWidget] = None
    ) -> None:
        """Initialize a toast notification.

        Args:
            message: Text message to display.
            level: Message level ('info', 'success', 'warning', 'error').
            duration: Display duration in milliseconds.
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self._duration: int = duration
        self._level: MessageLevel = level
        self._init_ui(message, level)

        self.setWindowFlags(
            Qt.Window |
            Qt.FramelessWindowHint |
            Qt.WindowStaysOnTopHint
        )
        self.setAttribute(Qt.WA_TranslucentBackground)

        # Setup fade animations
        self._setup_animations()

        # Auto-close after duration
        QTimer.singleShot(duration, self._fade_out)

    def _setup_animations(self) -> None:
        """Setup fade in/out animations."""
        # Opacity effect for fade animations
        self._opacity_effect = QGraphicsOpacityEffect(self)
        self.setGraphicsEffect(self._opacity_effect)
        self._opacity_effect.setOpacity(0)

        # Fade in animation
        self._fade_in_animation = QPropertyAnimation(self._opacity_effect, b'opacity')
        self._fade_in_animation.setDuration(300)
        self._fade_in_animation.setStartValue(0.0)
        self._fade_in_animation.setEndValue(1.0)
        self._fade_in_animation.setEasingCurve(QEasingCurve.InOutQuad)

        # Fade out animation
        self._fade_out_animation = QPropertyAnimation(self._opacity_effect, b'opacity')
        self._fade_out_animation.setDuration(200)
        self._fade_out_animation.setStartValue(1.0)
        self._fade_out_animation.setEndValue(0.0)
        self._fade_out_animation.setEasingCurve(QEasingCurve.InOutQuad)
        self._fade_out_animation.finished.connect(self.close)

    def _fade_in(self) -> None:
        """Start fade in animation."""
        self._fade_in_animation.start()

    def _fade_out(self) -> None:
        """Start fade out animation."""
        self._fade_out_animation.start()

    def _init_ui(self, message: str, level: MessageLevel) -> None:
        """Initialize the toast UI.

        Args:
            message: Text message to display.
            level: Message level for styling.
        """
        layout = QHBoxLayout(self)
        layout.setContentsMargins(15, 10, 15, 10)

        # Use theme-aware colors
        color = self._get_level_color(level)
        icon = self.ICONS.get(level, self.ICONS['info'])

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

    def _get_level_color(self, level: MessageLevel) -> str:
        """Get color for message level from theme manager.

        Args:
            level: Message level.

        Returns:
            Hex color string.
        """
        color_map = {
            'info': theme_manager.get_color('primary'),
            'success': theme_manager.get_color('success'),
            'warning': theme_manager.get_color('warning'),
            'error': theme_manager.get_color('error'),
        }
        return color_map.get(level, theme_manager.get_color('primary'))

    def show_at_corner(self) -> None:
        """Show the toast in the bottom-right corner of the screen."""
        screen = QApplication.primaryScreen()
        if screen:
            screen_geometry = screen.availableGeometry()
            self.move(
                screen_geometry.width() - self.width() - 20,
                screen_geometry.height() - self.height() - 60
            )
        self.show()
        self._fade_in()


class MessageManager:
    """Centralized message manager for user notifications.
    
    Provides singleton access to various message types:
    - Standard message boxes (info, success, warning, error)
    - Toast notifications
    - Confirmation dialogs
    """
    
    _instance: Optional['MessageManager'] = None
    
    def __new__(cls) -> 'MessageManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._parent_widget: Optional[QWidget] = None
    
    def set_parent(self, parent: QWidget) -> None:
        """Set the default parent widget for messages.
        
        Args:
            parent: Default parent widget.
        """
        self._parent_widget = parent
    
    def info(
        self,
        title: str,
        message: str,
        parent: Optional[QWidget] = None
    ) -> None:
        """Show an information message box.
        
        Args:
            title: Dialog title.
            message: Message text.
            parent: Optional parent widget override.
        """
        parent = parent or self._parent_widget
        QMessageBox.information(parent, title, message)
    
    def success(
        self,
        title: str,
        message: str,
        parent: Optional[QWidget] = None
    ) -> None:
        """Show a success message box with themed styling.

        Args:
            title: Dialog title.
            message: Message text.
            parent: Optional parent widget override.
        """
        parent = parent or self._parent_widget
        msg_box = QMessageBox(parent)
        msg_box.setIcon(QMessageBox.Information)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        colors = theme_manager.colors
        msg_box.setStyleSheet(f"""
            QMessageBox {{
                background-color: {colors['bg_widget']};
            }}
            QPushButton {{
                background-color: {colors['success']};
                color: white;
                padding: 8px 20px;
                border: none;
                border-radius: 4px;
            }}
            QPushButton:hover {{
                background-color: {colors['success_hover']};
            }}
        """)
        msg_box.exec_()

    def warning(
        self,
        title: str,
        message: str,
        parent: Optional[QWidget] = None
    ) -> None:
        """Show a warning message box.

        Args:
            title: Dialog title.
            message: Message text.
            parent: Optional parent widget override.
        """
        parent = parent or self._parent_widget
        QMessageBox.warning(parent, title, message)

    def error(
        self,
        title: str,
        message: str,
        parent: Optional[QWidget] = None
    ) -> None:
        """Show an error message box with themed styling.

        Args:
            title: Dialog title.
            message: Message text.
            parent: Optional parent widget override.
        """
        parent = parent or self._parent_widget
        msg_box = QMessageBox(parent)
        msg_box.setIcon(QMessageBox.Critical)
        msg_box.setWindowTitle(title)
        msg_box.setText(message)
        colors = theme_manager.colors
        msg_box.setStyleSheet(f"""
            QMessageBox {{
                background-color: {colors['bg_widget']};
            }}
            QPushButton {{
                background-color: {colors['error']};
                color: white;
                padding: 8px 20px;
                border: none;
                border-radius: 4px;
            }}
            QPushButton:hover {{
                background-color: {colors['error_hover']};
            }}
        """)
        msg_box.exec_()
    
    def question(
        self,
        title: str,
        message: str,
        parent: Optional[QWidget] = None
    ) -> bool:
        """Show a yes/no question dialog.
        
        Args:
            title: Dialog title.
            message: Question text.
            parent: Optional parent widget override.
            
        Returns:
            True if user clicked Yes, False otherwise.
        """
        parent = parent or self._parent_widget
        reply = QMessageBox.question(
            parent, title, message,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        return reply == QMessageBox.Yes
    
    def confirm(
        self,
        title: str,
        message: str,
        parent: Optional[QWidget] = None
    ) -> bool:
        """Show a confirmation dialog.
        
        Args:
            title: Dialog title.
            message: Confirmation question.
            parent: Optional parent widget override.
            
        Returns:
            True if user confirmed, False otherwise.
        """
        return self.question(title, message, parent)
    
    def toast(
        self,
        message: str,
        level: MessageLevel = 'info',
        duration: int = 3000
    ) -> None:
        """Show a toast notification.
        
        Args:
            message: Notification text.
            level: Message level ('info', 'success', 'warning', 'error').
            duration: Display duration in milliseconds.
        """
        toast = ToastWidget(message, level, duration)
        toast.show_at_corner()
    
    def toast_success(self, message: str, duration: int = 3000) -> None:
        """Show a success toast notification.
        
        Args:
            message: Notification text.
            duration: Display duration in milliseconds.
        """
        self.toast(message, 'success', duration)
    
    def toast_error(self, message: str, duration: int = 3000) -> None:
        """Show an error toast notification.
        
        Args:
            message: Notification text.
            duration: Display duration in milliseconds.
        """
        self.toast(message, 'error', duration)
    
    def toast_warning(self, message: str, duration: int = 3000) -> None:
        """Show a warning toast notification.
        
        Args:
            message: Notification text.
            duration: Display duration in milliseconds.
        """
        self.toast(message, 'warning', duration)
    
    def toast_info(self, message: str, duration: int = 3000) -> None:
        """Show an info toast notification.
        
        Args:
            message: Notification text.
            duration: Display duration in milliseconds.
        """
        self.toast(message, 'info', duration)


# Global singleton instance
message_manager: MessageManager = MessageManager()


# Convenience functions
def show_error(
    title: str,
    message: str,
    parent: Optional[QWidget] = None
) -> None:
    """Show an error message box."""
    message_manager.error(title, message, parent)


def show_success(
    title: str,
    message: str,
    parent: Optional[QWidget] = None
) -> None:
    """Show a success message box."""
    message_manager.success(title, message, parent)


def show_warning(
    title: str,
    message: str,
    parent: Optional[QWidget] = None
) -> None:
    """Show a warning message box."""
    message_manager.warning(title, message, parent)


def show_info(
    title: str,
    message: str,
    parent: Optional[QWidget] = None
) -> None:
    """Show an information message box."""
    message_manager.info(title, message, parent)


def show_question(
    title: str,
    message: str,
    parent: Optional[QWidget] = None
) -> bool:
    """Show a yes/no question dialog."""
    return message_manager.question(title, message, parent)


def show_toast(
    message: str,
    level: MessageLevel = 'info',
    duration: int = 3000
) -> None:
    """Show a toast notification."""
    message_manager.toast(message, level, duration)
