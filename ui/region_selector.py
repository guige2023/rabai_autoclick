import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PyQt5.QtWidgets import QWidget, QApplication, QRubberBand, QMessageBox
from PyQt5.QtCore import Qt, QRect, QPoint, pyqtSignal
from PyQt5.QtGui import QScreen, QPixmap, QPainter, QColor, QPen, QFont, QCursor, QBrush
from typing import Optional, Tuple


class RegionSelector(QWidget):
    region_selected = pyqtSignal(int, int, int, int)
    position_selected = pyqtSignal(int, int)
    cancelled = pyqtSignal()
    
    def __init__(self, mode: str = 'region', parent=None):
        super().__init__(parent)
        self.mode = mode
        self._origin = QPoint()
        self._rubber_band: Optional[QRubberBand] = None
        self._screenshot: Optional[QPixmap] = None
        self._selection_rect = QRect()
        self._has_selected = False
        
        self._init_ui()
    
    def _init_ui(self):
        screen = QApplication.primaryScreen()
        self._screenshot = screen.grabWindow(0)
        
        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint)
        self.setWindowState(Qt.WindowFullScreen)
        self.setCursor(Qt.CrossCursor)
        
        self._rubber_band = QRubberBand(QRubberBand.Rectangle, self)
        
        self.show()
        self.activateWindow()
    
    def paintEvent(self, event):
        painter = QPainter(self)
        
        painter.drawPixmap(0, 0, self._screenshot)
        
        if not self._selection_rect.isNull():
            overlay = QColor(0, 0, 0, 120)
            painter.fillRect(0, 0, self.width(), self._selection_rect.top(), overlay)
            painter.fillRect(0, self._selection_rect.bottom(), self.width(), self.height() - self._selection_rect.bottom(), overlay)
            painter.fillRect(0, self._selection_rect.top(), self._selection_rect.left(), self._selection_rect.height(), overlay)
            painter.fillRect(self._selection_rect.right(), self._selection_rect.top(), self.width() - self._selection_rect.right(), self._selection_rect.height(), overlay)
            
            pen = QPen(QColor(0, 120, 215), 2)
            painter.setPen(pen)
            painter.drawRect(self._selection_rect)
        
        self._draw_info(painter)
    
    def _draw_info(self, painter: QPainter):
        painter.setPen(QColor(255, 255, 255))
        painter.setFont(QFont('Microsoft YaHei', 10))
        
        if self.mode == 'region':
            info_text = "拖拽选择区域 | Enter确认 | Esc取消"
        else:
            info_text = "点击选择位置 | Esc取消"
        
        text_rect = painter.fontMetrics().boundingRect(info_text)
        x = (self.width() - text_rect.width()) // 2
        y = 30
        
        painter.fillRect(x - 10, y - 5, text_rect.width() + 20, text_rect.height() + 10, QColor(0, 0, 0, 180))
        painter.drawText(x, y + text_rect.height() - 5, info_text)
        
        if not self._selection_rect.isNull():
            rect_info = f"位置: ({self._selection_rect.x()}, {self._selection_rect.y()}) "
            rect_info += f"大小: {self._selection_rect.width()} x {self._selection_rect.height()}"
            
            if self.mode == 'region':
                center = self._selection_rect.center()
                rect_info += f" | 中心: ({center.x()}, {center.y()})"
            
            text_rect2 = painter.fontMetrics().boundingRect(rect_info)
            x2 = self._selection_rect.x()
            y2 = self._selection_rect.y() - 25
            
            if y2 < 20:
                y2 = self._selection_rect.y() + self._selection_rect.height() + 5
            
            painter.fillRect(x2 - 5, y2 - 3, text_rect2.width() + 10, text_rect2.height() + 6, QColor(0, 0, 0, 180))
            painter.drawText(x2, y2 + text_rect2.height() - 3, rect_info)
    
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self._origin = event.pos()
            self._selection_rect = QRect(self._origin, self._origin)
            self._rubber_band.setGeometry(self._selection_rect)
            self._rubber_band.show()
            self.update()
        elif event.button() == Qt.RightButton:
            self._cancel()
    
    def mouseMoveEvent(self, event):
        if event.buttons() & Qt.LeftButton:
            self._selection_rect = QRect(self._origin, event.pos()).normalized()
            self._rubber_band.setGeometry(self._selection_rect)
            self.update()
    
    def mouseReleaseEvent(self, event):
        if event.button() == Qt.LeftButton:
            if self.mode == 'position':
                self.position_selected.emit(event.pos().x(), event.pos().y())
                self._has_selected = True
                self.close()
    
    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Escape:
            self._cancel()
        elif event.key() == Qt.Key_Return or event.key() == Qt.Key_Enter:
            if not self._selection_rect.isNull() and self.mode == 'region':
                self._confirm_selection()
    
    def _confirm_selection(self):
        if not self._selection_rect.isNull():
            self.region_selected.emit(
                self._selection_rect.x(),
                self._selection_rect.y(),
                self._selection_rect.width(),
                self._selection_rect.height()
            )
            self._has_selected = True
            self.close()
    
    def _cancel(self):
        self.cancelled.emit()
        self.close()
    
    def closeEvent(self, event):
        if not self._has_selected:
            self.cancelled.emit()
        event.accept()


class PositionSelector(QWidget):
    position_selected = pyqtSignal(int, int)
    cancelled = pyqtSignal()
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._screenshot: Optional[QPixmap] = None
        self._has_selected = False
        self._init_ui()
    
    def _init_ui(self):
        screen = QApplication.primaryScreen()
        self._screenshot = screen.grabWindow(0)
        
        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint)
        self.setWindowState(Qt.WindowFullScreen)
        self.setCursor(Qt.CrossCursor)
        
        self.show()
        self.activateWindow()
    
    def paintEvent(self, event):
        painter = QPainter(self)
        painter.drawPixmap(0, 0, self._screenshot)
        
        painter.setPen(QColor(255, 255, 255))
        painter.setFont(QFont('Microsoft YaHei', 10))
        
        info_text = "点击选择位置 | Esc取消"
        text_rect = painter.fontMetrics().boundingRect(info_text)
        x = (self.width() - text_rect.width()) // 2
        y = 30
        painter.fillRect(x - 10, y - 5, text_rect.width() + 20, text_rect.height() + 10, QColor(0, 0, 0, 180))
        painter.drawText(x, y + text_rect.height() - 5, info_text)
        
        cursor_pos = self.mapFromGlobal(QCursor.pos())
        pos_text = f"位置: ({cursor_pos.x()}, {cursor_pos.y()})"
        painter.fillRect(cursor_pos.x() + 15, cursor_pos.y() - 20, 150, 25, QColor(0, 0, 0, 180))
        painter.drawText(cursor_pos.x() + 20, cursor_pos.y() - 3, pos_text)
    
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.position_selected.emit(event.pos().x(), event.pos().y())
            self._has_selected = True
            self.close()
    
    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Escape:
            self.cancelled.emit()
            self.close()
    
    def closeEvent(self, event):
        if not self._has_selected:
            self.cancelled.emit()
        event.accept()


def select_region() -> Optional[Tuple[int, int, int, int]]:
    result = [None]
    
    def on_region_selected(x, y, w, h):
        result[0] = (x, y, w, h)
    
    def on_cancelled():
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
    result = [None]
    
    def on_position_selected(x, y):
        result[0] = (x, y)
    
    def on_cancelled():
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
