import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Optional, List, Dict, Any
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QListWidget, QListWidgetItem,
    QPushButton, QLabel, QLineEdit, QGroupBox, QFormLayout, QDialogButtonBox
)
from PyQt5.QtCore import Qt, pyqtSignal, QTimer
from PyQt5.QtGui import QCursor

try:
    import pygetwindow as gw
    GW_AVAILABLE = True
except ImportError:
    GW_AVAILABLE = False

try:
    import win32gui
    import win32con
    WIN32_AVAILABLE = True
except ImportError:
    WIN32_AVAILABLE = False


class WindowInfo:
    def __init__(self, title: str, hwnd: int = None, left: int = 0, top: int = 0, 
                 width: int = 0, height: int = 0):
        self.title = title
        self.hwnd = hwnd
        self.left = left
        self.top = top
        self.width = width
        self.height = height
    
    @property
    def region(self) -> tuple:
        return (self.left, self.top, self.width, self.height)
    
    @property
    def center(self) -> tuple:
        return (self.left + self.width // 2, self.top + self.height // 2)


def get_all_windows() -> List[WindowInfo]:
    windows = []
    
    if WIN32_AVAILABLE:
        def enum_windows_proc(hwnd, lParam):
            if win32gui.IsWindowVisible(hwnd):
                title = win32gui.GetWindowText(hwnd)
                if title:
                    rect = win32gui.GetWindowRect(hwnd)
                    left, top, right, bottom = rect
                    windows.append(WindowInfo(
                        title=title,
                        hwnd=hwnd,
                        left=left,
                        top=top,
                        width=right - left,
                        height=bottom - top
                    ))
            return True
        
        win32gui.EnumWindows(enum_windows_proc, None)
    
    elif GW_AVAILABLE:
        for win in gw.getAllWindows():
            if win.title:
                windows.append(WindowInfo(
                    title=win.title,
                    left=win.left,
                    top=win.top,
                    width=win.width,
                    height=win.height
                ))
    
    windows.sort(key=lambda w: w.title.lower())
    return windows


def get_window_by_title(title: str) -> Optional[WindowInfo]:
    windows = get_all_windows()
    for win in windows:
        if title.lower() in win.title.lower():
            return win
    return None


def get_active_window() -> Optional[WindowInfo]:
    if WIN32_AVAILABLE:
        hwnd = win32gui.GetForegroundWindow()
        if hwnd:
            title = win32gui.GetWindowText(hwnd)
            rect = win32gui.GetWindowRect(hwnd)
            left, top, right, bottom = rect
            return WindowInfo(
                title=title,
                hwnd=hwnd,
                left=left,
                top=top,
                width=right - left,
                height=bottom - top
            )
    elif GW_AVAILABLE:
        win = gw.getActiveWindow()
        if win:
            return WindowInfo(
                title=win.title,
                left=win.left,
                top=win.top,
                width=win.width,
                height=win.height
            )
    return None


def focus_window(window: WindowInfo) -> bool:
    if WIN32_AVAILABLE and window.hwnd:
        try:
            win32gui.ShowWindow(window.hwnd, win32con.SW_RESTORE)
            win32gui.SetForegroundWindow(window.hwnd)
            return True
        except:
            pass
    return False


class WindowSelectorDialog(QDialog):
    window_selected = pyqtSignal(object)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("选择目标窗口")
        self.setMinimumSize(500, 400)
        self.selected_window = None
        
        self._init_ui()
        self._refresh_windows()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        
        info_label = QLabel("选择一个窗口作为执行目标，OCR将在该窗口区域内识别")
        info_label.setStyleSheet("color: #666; margin-bottom: 10px;")
        layout.addWidget(info_label)
        
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("搜索:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("输入窗口标题关键词...")
        self.search_edit.textChanged.connect(self._filter_windows)
        search_layout.addWidget(self.search_edit)
        layout.addLayout(search_layout)
        
        self.window_list = QListWidget()
        self.window_list.itemDoubleClicked.connect(self._on_select)
        layout.addWidget(self.window_list)
        
        info_group = QGroupBox("窗口信息")
        info_layout = QFormLayout()
        
        self.title_label = QLabel("-")
        self.position_label = QLabel("-")
        self.size_label = QLabel("-")
        
        info_layout.addRow("标题:", self.title_label)
        info_layout.addRow("位置:", self.position_label)
        info_layout.addRow("大小:", self.size_label)
        
        info_group.setLayout(info_layout)
        layout.addWidget(info_group)
        
        self.window_list.currentRowChanged.connect(self._on_window_highlight)
        
        btn_layout = QHBoxLayout()
        
        refresh_btn = QPushButton("🔄 刷新列表")
        refresh_btn.clicked.connect(self._refresh_windows)
        btn_layout.addWidget(refresh_btn)
        
        btn_layout.addStretch()
        
        select_btn = QPushButton("✓ 选择")
        select_btn.clicked.connect(self._on_select)
        select_btn.setStyleSheet("background-color: #4CAF50; color: white; padding: 8px 16px;")
        btn_layout.addWidget(select_btn)
        
        cancel_btn = QPushButton("取消")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(cancel_btn)
        
        layout.addLayout(btn_layout)
    
    def _refresh_windows(self):
        self.window_list.clear()
        self._windows = get_all_windows()
        
        for win in self._windows:
            item = QListWidgetItem(f"🪟 {win.title}")
            item.setData(Qt.UserRole, win)
            self.window_list.addItem(item)
        
        self.search_edit.clear()
    
    def _filter_windows(self, text: str):
        text = text.lower()
        for i in range(self.window_list.count()):
            item = self.window_list.item(i)
            win = item.data(Qt.UserRole)
            visible = text in win.title.lower() if text else True
            item.setHidden(not visible)
    
    def _on_window_highlight(self, index):
        if index >= 0:
            item = self.window_list.item(index)
            win = item.data(Qt.UserRole)
            
            self.title_label.setText(win.title[:50] + "..." if len(win.title) > 50 else win.title)
            self.position_label.setText(f"({win.left}, {win.top})")
            self.size_label.setText(f"{win.width} × {win.height}")
    
    def _on_select(self):
        current = self.window_list.currentItem()
        if current:
            self.selected_window = current.data(Qt.UserRole)
            self.accept()
    
    def get_selected_window(self) -> Optional[WindowInfo]:
        return self.selected_window


class QuickWindowPicker(QDialog):
    window_picked = pyqtSignal(object)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("点击目标窗口")
        self.setFixedSize(400, 150)
        self.setWindowFlags(self.windowFlags() | Qt.WindowStaysOnTopHint)
        
        self.selected_window = None
        
        layout = QVBoxLayout(self)
        
        label = QLabel("点击下方按钮后，在3秒内点击目标窗口")
        label.setAlignment(Qt.AlignCenter)
        label.setStyleSheet("font-size: 14px; margin: 10px;")
        layout.addWidget(label)
        
        self.status_label = QLabel("准备就绪")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("color: #666;")
        layout.addWidget(self.status_label)
        
        btn = QPushButton("🎯 开始选取")
        btn.setStyleSheet("font-size: 16px; padding: 15px; background-color: #2196F3; color: white;")
        btn.clicked.connect(self._start_pick)
        layout.addWidget(btn)
    
    def _start_pick(self):
        self.status_label.setText("3秒后自动获取当前活动窗口...")
        self.status_label.setStyleSheet("color: #f44336; font-weight: bold;")
        
        self.hide()
        
        QTimer.singleShot(3000, self._get_active_window)
    
    def _get_active_window(self):
        self.selected_window = get_active_window()
        
        if self.selected_window:
            self.status_label.setText(f"已选择: {self.selected_window.title[:30]}")
            self.status_label.setStyleSheet("color: #4CAF50;")
            self.accept()
        else:
            self.status_label.setText("未能获取窗口，请重试")
            self.status_label.setStyleSheet("color: #f44336;")
            self.show()
    
    def get_selected_window(self) -> Optional[WindowInfo]:
        return self.selected_window
