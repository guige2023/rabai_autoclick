"""Region and position selector for RabAI AutoClick.

Provides full-screen widgets for selecting screen regions
and positions on screen for automation workflows.
"""

import os
import sys
from typing import Optional, Tuple

from PyQt5.QtCore import Qt, QPoint, QRect, pyqtSignal
from PyQt5.QtGui import (
    QBrush, QColor, QCursor, QFont, QPainter, QPen, QPixmap, QScreen
)
from PyQt5.QtWidgets import QApplication, QRubberBand, QWidget

from ui.theme import theme_manager


# Add project root to path
sys.path.insert(0, os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
))


class RegionSelector(QWidget):
    """Full-screen region selector widget.
    
    Allows users to drag and select a rectangular region on screen.
    Emits signals for region selection, position selection, or cancellation.
    """
    
    region_selected = pyqtSignal(int, int, int, int)
    position_selected = pyqtSignal(int, int)
    cancelled = pyqtSignal()
    
    def __init__(
        self,
        mode: str = 'region',
        parent: Optional[QWidget] = None
    ) -> None:
        """Initialize the region selector.
        
        Args:
            mode: Selection mode - 'region' for drag selection, 'position' for click.
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self.mode: str = mode
        self._origin: QPoint = QPoint()
        self._rubber_band: Optional[QRubberBand] = None
        self._screenshot: Optional[QPixmap] = None
        self._selection_rect: QRect = QRect()
        self._has_selected: bool = False
        self._is_closing: bool = False
        
        self._init_ui()
    
    def _init_ui(self) -> None:
        """Initialize the widget UI."""
        screen = QApplication.primaryScreen()
        self._screenshot = screen.grabWindow(0)
        
        self.setWindowFlags(
            Qt.WindowType.Window |
            Qt.WindowType.FramelessWindowHint |
            Qt.WindowType.WindowStaysOnTopHint
        )
        self.setWindowState(Qt.WindowState.WindowFullScreen)
        self.setCursor(Qt.CursorShape.CrossCursor)
        
        self._rubber_band = QRubberBand(
            QRubberBand.Shape.Rectangle, self
        )
        
        self.show()
        self.activateWindow()
        self.raise_()
    
    def paintEvent(self, event) -> None:
        """Paint the widget with screenshot overlay and selection UI.
        
        Args:
            event: Paint event.
        """
        painter = QPainter(self)
        
        painter.drawPixmap(0, 0, self._screenshot)
        
        if not self._selection_rect.isNull():
            # Draw dark overlay outside selection
            overlay = QColor(0, 0, 0, 120)
            painter.fillRect(
                0, 0, self.width(), self._selection_rect.top(), overlay
            )
            painter.fillRect(
                0, self._selection_rect.bottom(),
                self.width(), self.height() - self._selection_rect.bottom(),
                overlay
            )
            painter.fillRect(
                0, self._selection_rect.top(),
                self._selection_rect.left(), self._selection_rect.height(),
                overlay
            )
            painter.fillRect(
                self._selection_rect.right(), self._selection_rect.top(),
                self.width() - self._selection_rect.right(),
                self._selection_rect.height(),
                overlay
            )

            # Draw selection border with theme colors
            colors = theme_manager.colors
            primary_color = QColor(colors['primary'])
            pen = QPen(primary_color, 2)
            painter.setPen(pen)
            painter.drawRect(self._selection_rect)

            # Draw corner handles for better visual feedback
            corner_size = 8
            corner_color = QColor(colors['primary'])
            corners = [
                # Top-left
                (self._selection_rect.topLeft(), 1, 1),
                # Top-right
                (self._selection_rect.topRight(), -1, 1),
                # Bottom-left
                (self._selection_rect.bottomLeft(), 1, -1),
                # Bottom-right
                (self._selection_rect.bottomRight(), -1, -1),
            ]
            painter.setPen(corner_color)
            painter.setBrush(corner_color)
            for corner_pos, dx, dy in corners:
                painter.drawRect(
                    corner_pos.x() + (dx * corner_size) - corner_size // 2,
                    corner_pos.y() + (dy * corner_size) - corner_size // 2,
                    corner_size,
                    corner_size
                )
        
        self._draw_info(painter)
    
    def _draw_info(self, painter: QPainter) -> None:
        """Draw informational text overlay.
        
        Args:
            painter: QPainter to draw with.
        """
        painter.setPen(QColor(255, 255, 255))
        painter.setFont(QFont('Microsoft YaHei', 10))
        
        if self.mode == 'region':
            info_text = "拖拽选择区域 | Enter确认 | Esc取消"
        else:
            info_text = "点击选择位置 | Esc取消"

        text_rect = painter.fontMetrics().boundingRect(info_text)
        x = (self.width() - text_rect.width()) // 2
        y = 30

        painter.fillRect(
            x - 10, y - 5, text_rect.width() + 20,
            text_rect.height() + 10, QColor(33, 33, 33, 200)
        )
        painter.drawText(x, y + text_rect.height() - 5, info_text)
        
        if not self._selection_rect.isNull():
            rect_info = (
                f"位置: ({self._selection_rect.x()}, {self._selection_rect.y()}) "
                f"大小: {self._selection_rect.width()} x "
                f"{self._selection_rect.height()}"
            )
            
            if self.mode == 'region':
                center = self._selection_rect.center()
                rect_info += f" | 中心: ({center.x()}, {center.y()})"
            
            text_rect2 = painter.fontMetrics().boundingRect(rect_info)
            x2 = self._selection_rect.x()
            y2 = self._selection_rect.y() - 25
            
            if y2 < 20:
                y2 = (
                    self._selection_rect.y() +
                    self._selection_rect.height() + 5
                )
            
            painter.fillRect(
                x2 - 5, y2 - 3, text_rect2.width() + 10,
                text_rect2.height() + 6, QColor(33, 33, 33, 200)
            )
            painter.drawText(
                x2, y2 + text_rect2.height() - 3, rect_info
            )
    
    def mousePressEvent(self, event) -> None:
        """Handle mouse press to start selection.
        
        Args:
            event: Mouse press event.
        """
        if event.button() == Qt.LeftButton:
            self._origin = event.pos()
            self._selection_rect = QRect(self._origin, self._origin)
            self._rubber_band.setGeometry(self._selection_rect)
            self._rubber_band.show()
            self.update()
        elif event.button() == Qt.RightButton:
            self._cancel()
    
    def mouseMoveEvent(self, event) -> None:
        """Handle mouse move to update selection rectangle.
        
        Args:
            event: Mouse move event.
        """
        if event.buttons() & Qt.LeftButton:
            self._selection_rect = (
                QRect(self._origin, event.pos()).normalized()
            )
            self._rubber_band.setGeometry(self._selection_rect)
            self.update()
    
    def mouseReleaseEvent(self, event) -> None:
        """Handle mouse release for position mode.
        
        Args:
            event: Mouse release event.
        """
        if event.button() == Qt.LeftButton:
            if self.mode == 'position':
                self.position_selected.emit(
                    event.pos().x(), event.pos().y()
                )
                self._has_selected = True
                self.close()
    
    def keyPressEvent(self, event) -> None:
        """Handle key press for confirmation or cancellation.
        
        Args:
            event: Key press event.
        """
        if event.key() == Qt.Key_Escape:
            self._cancel()
        elif event.key() in (Qt.Key_Return, Qt.Key_Enter):
            if not self._selection_rect.isNull() and self.mode == 'region':
                self._confirm_selection()
    
    def _confirm_selection(self) -> None:
        """Confirm and emit the selected region."""
        if not self._selection_rect.isNull():
            self._has_selected = True
            self.region_selected.emit(
                self._selection_rect.x(),
                self._selection_rect.y(),
                self._selection_rect.width(),
                self._selection_rect.height()
            )
            self._safe_close()
    
    def _cancel(self) -> None:
        """Cancel selection and close."""
        self._has_selected = True
        self.cancelled.emit()
        self._safe_close()
    
    def _safe_close(self) -> None:
        """Safely close the widget."""
        if self._is_closing:
            return
        self._is_closing = True
        
        if self._rubber_band:
            self._rubber_band.hide()
            self._rubber_band.deleteLater()
            self._rubber_band = None
        
        self.hide()
        self.deleteLater()
    
    def closeEvent(self, event) -> None:
        """Handle widget close event.
        
        Args:
            event: Close event.
        """
        if not self._has_selected:
            self.cancelled.emit()
        event.accept()


class PositionSelector(QWidget):
    """Full-screen position selector widget.
    
    Allows users to click to select a single screen position.
    """
    
    position_selected = pyqtSignal(int, int)
    cancelled = pyqtSignal()
    
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        """Initialize the position selector.
        
        Args:
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self._screenshot: Optional[QPixmap] = None
        self._has_selected: bool = False
        self._init_ui()
    
    def _init_ui(self) -> None:
        """Initialize the widget UI."""
        screen = QApplication.primaryScreen()
        self._screenshot = screen.grabWindow(0)
        
        self.setWindowFlags(
            Qt.Window |
            Qt.FramelessWindowHint |
            Qt.WindowStaysOnTopHint
        )
        self.setWindowState(Qt.WindowFullScreen)
        self.setCursor(Qt.CrossCursor)
        
        self.show()
        self.activateWindow()
    
    def paintEvent(self, event) -> None:
        """Paint the widget with screenshot and position info.
        
        Args:
            event: Paint event.
        """
        painter = QPainter(self)
        painter.drawPixmap(0, 0, self._screenshot)
        
        painter.setPen(QColor(255, 255, 255))
        painter.setFont(QFont('Microsoft YaHei', 10))
        
        info_text = "点击选择位置 | Esc取消"
        text_rect = painter.fontMetrics().boundingRect(info_text)
        x = (self.width() - text_rect.width()) // 2
        y = 30
        painter.fillRect(
            x - 10, y - 5, text_rect.width() + 20,
            text_rect.height() + 10, QColor(33, 33, 33, 200)
        )
        painter.drawText(x, y + text_rect.height() - 5, info_text)

        cursor_pos = self.mapFromGlobal(QCursor.pos())
        pos_text = f"位置: ({cursor_pos.x()}, {cursor_pos.y()})"
        painter.fillRect(
            cursor_pos.x() + 15, cursor_pos.y() - 20,
            150, 25, QColor(33, 33, 33, 200)
        )
        painter.drawText(cursor_pos.x() + 20, cursor_pos.y() - 3, pos_text)
    
    def mousePressEvent(self, event) -> None:
        """Handle mouse press to select position.
        
        Args:
            event: Mouse press event.
        """
        if event.button() == Qt.LeftButton:
            self.position_selected.emit(event.pos().x(), event.pos().y())
            self._has_selected = True
            self.close()
    
    def keyPressEvent(self, event) -> None:
        """Handle key press for cancellation.
        
        Args:
            event: Key press event.
        """
        if event.key() == Qt.Key_Escape:
            self.cancelled.emit()
            self.close()
    
    def closeEvent(self, event) -> None:
        """Handle widget close event.
        
        Args:
            event: Close event.
        """
        if not self._has_selected:
            self.cancelled.emit()
        event.accept()


def select_region() -> Optional[Tuple[int, int, int, int]]:
    """Show a full-screen region selector and wait for selection.
    
    Returns:
        Tuple of (x, y, width, height) if selected, None if cancelled.
    """
    result: list = [None]
    
    def on_region_selected(x: int, y: int, w: int, h: int) -> None:
        result[0] = (x, y, w, h)
    
    def on_cancelled() -> None:
        pass
    
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    
    selector = RegionSelector(mode='region')
    selector.region_selected.connect(on_region_selected)
    selector.cancelled.connect(on_cancelled)
    
    while selector.isVisible():
        app.processEvents()
    
    return result[0]


def select_position() -> Optional[Tuple[int, int]]:
    """Show a full-screen position selector and wait for selection.
    
    Returns:
        Tuple of (x, y) if selected, None if cancelled.
    """
    result: list = [None]
    
    def on_position_selected(x: int, y: int) -> None:
        result[0] = (x, y)
    
    def on_cancelled() -> None:
        pass
    
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    
    selector = PositionSelector()
    selector.position_selected.connect(on_position_selected)
    selector.cancelled.connect(on_cancelled)
    
    while selector.isVisible():
        app.processEvents()
    
    return result[0]
