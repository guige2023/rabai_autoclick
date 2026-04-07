"""Mini toolbar widget for RabAI AutoClick.

Provides a compact floating toolbar for quick access to
workflow controls and common actions.
"""

from typing import Optional

from PyQt5.QtCore import Qt, QPoint, pyqtSignal
from PyQt5.QtGui import QFont, QPainter, QPen, QColor, QBrush, QRadialGradient
from PyQt5.QtWidgets import (
    QAction, QFrame, QHBoxLayout, QLabel, QMenu, QPushButton, QWidget
)

from ui.theme import theme_manager, ThemeType, ThemeColors


class MiniToolbar(QWidget):
    """Compact floating toolbar widget with shadow effect.

    Provides quick access to workflow controls including
    run, stop, region selection, window selection, and settings.
    """

    run_clicked = pyqtSignal()
    stop_clicked = pyqtSignal()
    region_clicked = pyqtSignal()
    window_clicked = pyqtSignal()
    settings_clicked = pyqtSignal()
    switch_to_full = pyqtSignal()

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        """Initialize the mini toolbar.

        Args:
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self._drag_pos: QPoint = QPoint()
        self._is_dark_theme = True  # Mini toolbar stays dark
        theme_manager.theme_changed.connect(self._on_theme_changed)
        self._init_ui()

    def _on_theme_changed(self, theme: ThemeType) -> None:
        """Handle theme changes."""
        self._is_dark_theme = theme == ThemeType.DARK
        self._apply_stylesheet()

    def _init_ui(self) -> None:
        """Initialize the toolbar UI."""
        self.setWindowFlags(
            Qt.WindowType.Window |
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.FramelessWindowHint
        )
        self.setAttribute(
            Qt.WidgetAttribute.WA_MacAlwaysShowToolWindow, True
        )
        self.setFixedHeight(40)
        self.setMinimumWidth(400)
        self.setStyleSheet("background: transparent;")

        self._apply_stylesheet()

    def _init_ui(self) -> None:
        """Initialize the toolbar UI."""
        self.setWindowFlags(
            Qt.WindowType.Window |
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.FramelessWindowHint
        )
        self.setAttribute(
            Qt.WidgetAttribute.WA_MacAlwaysShowToolWindow, True
        )
        self.setFixedHeight(40)
        self.setMinimumWidth(400)

        self._apply_stylesheet()

        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 4, 8, 4)
        layout.setSpacing(6)

        # Title
        self.title_label = QLabel("🤖 RabAI")
        self.title_label.setFont(QFont("Arial", 11, QFont.Weight.Bold))
        layout.addWidget(self.title_label)

        layout.addWidget(self._create_separator())

        # Run button
        self.run_btn = QPushButton("▶ 运行")
        self.run_btn.setObjectName("run_btn")
        self.run_btn.clicked.connect(self.run_clicked.emit)
        layout.addWidget(self.run_btn)

        # Stop button
        self.stop_btn = QPushButton("⏹ 停止")
        self.stop_btn.setObjectName("stop_btn")
        self.stop_btn.clicked.connect(self.stop_clicked.emit)
        layout.addWidget(self.stop_btn)

        layout.addWidget(self._create_separator())

        # Region button
        self.region_btn = QPushButton("📐 区域")
        self.region_btn.clicked.connect(self.region_clicked.emit)
        layout.addWidget(self.region_btn)

        # Window button
        self.window_btn = QPushButton("🪟 窗口")
        self.window_btn.clicked.connect(self.window_clicked.emit)
        layout.addWidget(self.window_btn)

        layout.addWidget(self._create_separator())

        # Status label
        self.status_label = QLabel("就绪")
        layout.addWidget(self.status_label)

        layout.addStretch()

        # Menu button
        self.menu_btn = QPushButton("☰")
        self.menu_btn.setFixedWidth(30)
        self.menu_btn.clicked.connect(self._show_menu)
        layout.addWidget(self.menu_btn)

        self._create_menu()

    def _apply_stylesheet(self) -> None:
        """Apply themed stylesheet to the toolbar."""
        styles = theme_manager.get_stylesheet("mini_toolbar")
        self.setStyleSheet(styles)
        # Menu needs its own dark stylesheet since it floats
        dark_colors = ThemeColors.DARK
        self.menu.setStyleSheet(f"""
            QMenu {{
                background-color: {dark_colors['bg_dark_widget']};
                color: white;
                border: 1px solid {dark_colors['border_dark']};
                border-radius: 4px;
                transition: opacity 0.2s ease;
            }}
            QMenu::item {{
                padding: 8px 20px;
                transition: background-color 0.2s ease;
            }}
            QMenu::item:selected {{
                background-color: {dark_colors['bg_dark_hover']};
            }}
        """)

    def _create_separator(self) -> QFrame:
        """Create a vertical separator line.

        Returns:
            QFrame configured as a vertical separator.
        """
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.VLine)
        dark_colors = ThemeColors.DARK
        sep.setStyleSheet(f"background-color: {dark_colors['border_dark']};")
        sep.setFixedWidth(1)
        return sep

    def _create_menu(self) -> None:
        """Create the toolbar context menu."""
        self.menu = QMenu(self)
        self._apply_stylesheet()

        self.full_mode_action = QAction("🖥 完整窗口", self)
        self.full_mode_action.triggered.connect(self.switch_to_full.emit)
        self.menu.addAction(self.full_mode_action)

        self.menu.addSeparator()

        self.settings_action = QAction("⚙ 设置", self)
        self.settings_action.triggered.connect(self.settings_clicked.emit)
        self.menu.addAction(self.settings_action)

    def _show_menu(self) -> None:
        """Show the context menu."""
        self.menu.exec_(
            self.menu_btn.mapToGlobal(
                self.menu_btn.rect().bottomLeft()
            )
        )

    def set_status(self, text: str, color: str = "white") -> None:
        """Set the status label text and color.

        Args:
            text: Status text to display.
            color: CSS color string for text color.
        """
        self.status_label.setText(text)
        self.status_label.setStyleSheet(f"color: {color};")

    def set_region(self, x: int, y: int, w: int, h: int) -> None:
        """Set the region button to show selected region.

        Args:
            x: Region X coordinate.
            y: Region Y coordinate.
            w: Region width.
            h: Region height.
        """
        colors = theme_manager.colors
        self.region_btn.setText(f"📐 ({x},{y},{w}x{h})")
        self.region_btn.setStyleSheet(f"background-color: {colors['success']};")

    def set_window(self, name: str) -> None:
        """Set the window button to show selected window.

        Args:
            name: Window name/title.
        """
        colors = theme_manager.colors
        self.window_btn.setText(f"🪟 {name[:8]}")
        self.window_btn.setStyleSheet(f"background-color: {colors['primary']};")

    def reset_region(self) -> None:
        """Reset the region button to default state."""
        self.region_btn.setText("📐 区域")
        self.region_btn.setStyleSheet("")

    def reset_window(self) -> None:
        """Reset the window button to default state."""
        self.window_btn.setText("🪟 窗口")
        self.window_btn.setStyleSheet("")

    def mousePressEvent(self, event) -> None:
        """Handle mouse press for window dragging.

        Args:
            event: Mouse press event.
        """
        if event.button() == Qt.MouseButton.LeftButton:
            self._drag_pos = (
                event.globalPosition().toPoint() -
                self.frameGeometry().topLeft()
            )

    def mouseMoveEvent(self, event) -> None:
        """Handle mouse move for window dragging.

        Args:
            event: Mouse move event.
        """
        if event.buttons() & Qt.MouseButton.LeftButton:
            self.move(
                event.globalPosition().toPoint() - self._drag_pos
            )

    def mouseDoubleClickEvent(self, event) -> None:
        """Handle double-click to switch to full mode.

        Args:
            event: Mouse double-click event.
        """
        self.switch_to_full.emit()
