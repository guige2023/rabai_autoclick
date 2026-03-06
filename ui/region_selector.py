import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PyQt5.QtWidgets import QWidget, QApplication
from PyQt5.QtCore import Qt, QRect, QPoint, pyqtSignal, QTimer
from PyQt5.QtGui import QScreen, QPixmap, QPainter, QColor, QPen, QFont, QCursor, QRegion
from typing import Optional


class RegionSelector(QWidget):
    region_selected = pyqtSignal(int, int, int, int)
    position_selected = pyqtSignal(int, int)
    cancelled = pyqtSignal()
    
    def __init__(self, mode: str = 'region', parent=None):
        super().__init__(parent)
        self.mode = mode
        self._origin = QPoint()
        self._screenshot: Optional[QPixmap] = None
        self._selection_rect = QRect()
        self._has_selected = False
        self._is_selecting = False
        self._parent_window = None
        
        self._init_ui()
    
    def _init_ui(self):
        self._parent_window = QApplication.activeWindow()
        
        screen = QApplication.primaryScreen()
        if screen:
            self._screenshot = screen.grabWindow(0)
        
        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground, True)
        self.setAttribute(Qt.WA_DeleteOnClose, False)
        self.setWindowState(Qt.WindowFullScreen)
        self.setCursor(Qt.CrossCursor)
        
        self.show()
        self.activateWindow()
        self.raise_()
        self.setFocus()
    
    def paintEvent(self, event):
        painter = QPainter(self)
        
        if self._screenshot:
            painter.drawPixmap(0, 0, self._screenshot)
        
        if not self._selection_rect.isNull() and self._is_selecting:
            try:
                full_region = QRegion(self.rect())
                selection_region = QRegion(self._selection_rect)
                outer_region = full_region - selection_region
                
                for rect in outer_region.rects():
                    painter.fillRect(rect, QColor(0, 0, 0, 100))
            except:
                pass
            
            pen = QPen(QColor(0, 150, 255), 2)
            painter.setPen(pen)
            painter.setBrush(Qt.NoBrush)
            painter.drawRect(self._selection_rect)
            
            center = self._selection_rect.center()
            painter.setPen(QPen(QColor(255, 50, 50), 2))
            painter.drawLine(center.x() - 10, center.y(), center.x() + 10, center.y())
            painter.drawLine(center.x(), center.y() - 10, center.x(), center.y() + 10)
        
        self._draw_info(painter)
    
    def _draw_info(self, painter: QPainter):
        painter.setPen(QColor(255, 255, 255))
        font = QFont()
        font.setPixelSize(14)
        font.setBold(True)
        painter.setFont(font)
        
        if self.mode == 'center':
            info_text = "拖拽选择区域（取中心点）| Esc取消"
        else:
            info_text = "拖拽选择区域 | Enter确认 | Esc取消"
        
        text_rect = painter.fontMetrics().boundingRect(info_text)
        x = (self.width() - text_rect.width()) // 2
        y = 35
        
        bg_rect = QRect(x - 12, y - 8, text_rect.width() + 24, text_rect.height() + 16)
        painter.fillRect(bg_rect, QColor(0, 0, 0, 180))
        painter.drawText(x, y + text_rect.height() - 4, info_text)
        
        if not self._selection_rect.isNull() and self._is_selecting:
            center = self._selection_rect.center()
            rect_info = f"位置: ({self._selection_rect.x()}, {self._selection_rect.y()}) "
            rect_info += f"大小: {self._selection_rect.width()} x {self._selection_rect.height()}"
            rect_info += f" | 中心: ({center.x()}, {center.y()})"
            
            text_rect2 = painter.fontMetrics().boundingRect(rect_info)
            x2 = self._selection_rect.x()
            y2 = self._selection_rect.y() - 28
            
            if y2 < 25:
                y2 = self._selection_rect.y() + self._selection_rect.height() + 8
            
            bg_rect2 = QRect(x2 - 6, y2 - 6, text_rect2.width() + 12, text_rect2.height() + 12)
            painter.fillRect(bg_rect2, QColor(0, 0, 0, 180))
            painter.drawText(x2, y2 + text_rect2.height() - 4, rect_info)
    
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self._origin = event.pos()
            self._selection_rect = QRect(self._origin, self._origin)
            self._is_selecting = True
            self.update()
        elif event.button() == Qt.RightButton:
            self._cancel()
    
    def mouseMoveEvent(self, event):
        if event.buttons() & Qt.LeftButton:
            self._selection_rect = QRect(self._origin, event.pos()).normalized()
            self.update()
    
    def mouseReleaseEvent(self, event):
        if event.button() == Qt.LeftButton:
            if self.mode == 'position':
                self._emit_position(event.pos().x(), event.pos().y())
            elif self.mode == 'center' and not self._selection_rect.isNull():
                if self._selection_rect.width() > 5 and self._selection_rect.height() > 5:
                    center = self._selection_rect.center()
                    self._emit_position(center.x(), center.y())
    
    def _emit_position(self, x, y):
        if self._has_selected:
            return
        self._has_selected = True
        self.position_selected.emit(x, y)
        self._close()
    
    def _emit_region(self):
        if self._has_selected:
            return
        if not self._selection_rect.isNull():
            self._has_selected = True
            self.region_selected.emit(
                self._selection_rect.x(),
                self._selection_rect.y(),
                self._selection_rect.width(),
                self._selection_rect.height()
            )
            self._close()
    
    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Escape:
            self._cancel()
        elif event.key() == Qt.Key_Return or event.key() == Qt.Key_Enter:
            if not self._selection_rect.isNull() and self.mode == 'region':
                self._emit_region()
            elif not self._selection_rect.isNull() and self.mode == 'center':
                if self._selection_rect.width() > 5 and self._selection_rect.height() > 5:
                    center = self._selection_rect.center()
                    self._emit_position(center.x(), center.y())
    
    def _cancel(self):
        if self._has_selected:
            return
        self._has_selected = True
        self.cancelled.emit()
        self._close()
    
    def _close(self):
        self.hide()
        self.releaseMouse()
        self.releaseKeyboard()
        if self._parent_window:
            self._parent_window.show()
            self._parent_window.activateWindow()
            self._parent_window.raise_()
        QTimer.singleShot(100, self.close)
    
    def closeEvent(self, event):
        if not self._has_selected:
            self.cancelled.emit()
        self._screenshot = None
        event.accept()


class PositionSelector(QWidget):
    position_selected = pyqtSignal(int, int)
    cancelled = pyqtSignal()
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._screenshot: Optional[QPixmap] = None
        self._has_selected = False
        self._parent_window = None
        self._init_ui()
    
    def _init_ui(self):
        self._parent_window = QApplication.activeWindow()
        
        screen = QApplication.primaryScreen()
        if screen:
            self._screenshot = screen.grabWindow(0)
        
        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint | Qt.Tool)
        self.setAttribute(Qt.WA_TranslucentBackground, True)
        self.setAttribute(Qt.WA_DeleteOnClose, False)
        self.setWindowState(Qt.WindowFullScreen)
        self.setCursor(Qt.CrossCursor)
        
        self.show()
        self.activateWindow()
        self.raise_()
        self.setFocus()
    
    def paintEvent(self, event):
        painter = QPainter(self)
        if self._screenshot:
            painter.drawPixmap(0, 0, self._screenshot)
        
        painter.setPen(QColor(255, 255, 255))
        font = QFont()
        font.setPixelSize(14)
        font.setBold(True)
        painter.setFont(font)
        
        info_text = "点击选择位置 | Esc取消"
        text_rect = painter.fontMetrics().boundingRect(info_text)
        x = (self.width() - text_rect.width()) // 2
        y = 35
        bg_rect = QRect(x - 12, y - 8, text_rect.width() + 24, text_rect.height() + 16)
        painter.fillRect(bg_rect, QColor(0, 0, 0, 180))
        painter.drawText(x, y + text_rect.height() - 4, info_text)
        
        cursor_pos = self.mapFromGlobal(QCursor.pos())
        pos_text = f"位置: ({cursor_pos.x()}, {cursor_pos.y()})"
        bg_rect2 = QRect(cursor_pos.x() + 12, cursor_pos.y() - 22, 140, 26)
        painter.fillRect(bg_rect2, QColor(0, 0, 0, 180))
        painter.drawText(cursor_pos.x() + 16, cursor_pos.y() - 4, pos_text)
        
        painter.setPen(QPen(QColor(255, 50, 50), 2))
        painter.drawLine(cursor_pos.x() - 12, cursor_pos.y(), cursor_pos.x() + 12, cursor_pos.y())
        painter.drawLine(cursor_pos.x(), cursor_pos.y() - 12, cursor_pos.x(), cursor_pos.y() + 12)
    
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            if self._has_selected:
                return
            self._has_selected = True
            self.position_selected.emit(event.pos().x(), event.pos().y())
            self._close()
    
    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Escape:
            if self._has_selected:
                return
            self._has_selected = True
            self.cancelled.emit()
            self._close()
    
    def _close(self):
        self.hide()
        self.releaseMouse()
        self.releaseKeyboard()
        if self._parent_window:
            self._parent_window.show()
            self._parent_window.activateWindow()
            self._parent_window.raise_()
        QTimer.singleShot(100, self.close)
    
    def closeEvent(self, event):
        if not self._has_selected:
            self.cancelled.emit()
        self._screenshot = None
        event.accept()
