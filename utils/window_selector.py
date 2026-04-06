"""Window selector utilities for RabAI AutoClick.

Provides cross-platform window enumeration and selection dialogs
for targeting automation actions to specific windows.
"""

import os
import platform
import subprocess
import sys
from typing import List, Optional, Tuple

from PyQt5.QtCore import Qt, pyqtSignal, QTimer
from PyQt5.QtGui import QCursor
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QListWidget, QListWidgetItem,
    QPushButton, QLabel, QLineEdit, QGroupBox, QFormLayout, 
    QDialogButtonBox
)


# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Platform detection
IS_MACOS: bool = platform.system() == 'Darwin'

# Library availability flags
try:
    import pygetwindow as gw
    GW_AVAILABLE: bool = True
except ImportError:
    GW_AVAILABLE = False

try:
    import win32gui
    import win32con
    WIN32_AVAILABLE: bool = True
except ImportError:
    WIN32_AVAILABLE = False

try:
    from AppKit import NSWorkspace
    APPKIT_AVAILABLE: bool = True
except ImportError:
    APPKIT_AVAILABLE = False


class WindowInfo:
    """Represents information about a window."""
    
    def __init__(
        self,
        title: str,
        hwnd: Optional[int] = None,
        left: int = 0,
        top: int = 0,
        width: int = 0,
        height: int = 0
    ) -> None:
        """Initialize WindowInfo.
        
        Args:
            title: Window title.
            hwnd: Window handle (platform-specific).
            left: Left edge X coordinate.
            top: Top edge Y coordinate.
            width: Window width.
            height: Window height.
        """
        self.title: str = title
        self.hwnd: Optional[int] = hwnd
        self.left: int = left
        self.top: int = top
        self.width: int = width
        self.height: int = height
    
    @property
    def region(self) -> Tuple[int, int, int, int]:
        """Get the window region as (left, top, width, height).
        
        Returns:
            Tuple of (left, top, width, height).
        """
        return (self.left, self.top, self.width, self.height)
    
    @property
    def center(self) -> Tuple[int, int]:
        """Get the window center coordinates.
        
        Returns:
            Tuple of (center_x, center_y).
        """
        return (self.left + self.width // 2, self.top + self.height // 2)


def _get_macos_windows() -> List[WindowInfo]:
    """Get all visible windows on macOS.
    
    Returns:
        List of WindowInfo objects for visible windows.
    """
    windows: List[WindowInfo] = []
    
    if APPKIT_AVAILABLE:
        try:
            workspace = NSWorkspace.sharedWorkspace()
            apps = workspace.runningApplications()
            for app in apps:
                if app.activationPolicy() == 0:  # Regular app
                    title = app.localizedName()
                    if title:
                        windows.append(WindowInfo(title=title))
            return windows
        except Exception as e:
            print(f"[WindowSelector] AppKit error: {e}")
    
    try:
        result = subprocess.run(
            ['osascript', '-e', 
             'tell application "System Events" to get name of every process '
             'whose background only is false'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0 and result.stdout.strip():
            app_names = [
                name.strip() for name in result.stdout.strip().split(', ') 
                if name
            ]
            windows = [WindowInfo(title=name) for name in app_names]
    except Exception as e:
        print(f"[WindowSelector] AppleScript error: {e}")
    
    return windows


def get_all_windows() -> List[WindowInfo]:
    """Get all visible windows on the current platform.
    
    Returns:
        List of WindowInfo objects, sorted by title.
    """
    windows: List[WindowInfo] = []
    
    try:
        if IS_MACOS:
            windows = _get_macos_windows()
        
        elif WIN32_AVAILABLE:
            def enum_windows_proc(hwnd: int, lParam: None) -> bool:
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
    except Exception as e:
        print(f"[WindowSelector] Error getting windows: {e}")
    
    windows.sort(key=lambda w: w.title.lower())
    return windows


def get_window_by_title(title: str) -> Optional[WindowInfo]:
    """Find a window by title substring.
    
    Args:
        title: Title substring to search for.
        
    Returns:
        First matching WindowInfo, or None if not found.
    """
    windows = get_all_windows()
    for win in windows:
        if title.lower() in win.title.lower():
            return win
    return None


def get_active_window() -> Optional[WindowInfo]:
    """Get the currently active/focused window.
    
    Returns:
        WindowInfo for the active window, or None if unavailable.
    """
    try:
        if IS_MACOS:
            script = '''
            tell application "System Events"
                set frontApp to name of first application process 
                    whose frontmost is true
            end tell
            '''
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0 and result.stdout.strip():
                return WindowInfo(title=result.stdout.strip())
        
        elif WIN32_AVAILABLE:
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
    except Exception as e:
        print(f"[WindowSelector] Error getting active window: {e}")
    return None


def focus_window(window: WindowInfo) -> bool:
    """Bring a window to the foreground.
    
    Args:
        window: WindowInfo of the window to focus.
        
    Returns:
        True if focus was successful.
    """
    if IS_MACOS and window.title:
        try:
            app_name = (
                window.title.split(' - ')[0] 
                if ' - ' in window.title 
                else window.title
            )
            subprocess.run(
                ['osascript', '-e', 
                 f'tell application "{app_name}" to activate'],
                timeout=5
            )
            return True
        except Exception as e:
            import logging
            logging.getLogger('RabAI').debug(f'激活macOS窗口失败: {e}')
    elif WIN32_AVAILABLE and window.hwnd:
        try:
            win32gui.ShowWindow(window.hwnd, win32con.SW_RESTORE)
            win32gui.SetForegroundWindow(window.hwnd)
            return True
        except Exception as e:
            import logging
            logging.getLogger('RabAI').debug(f'激活Win32窗口失败: {e}')
    return False


class WindowSelectorDialog(QDialog):
    """Dialog for selecting a target window from a list."""
    
    window_selected = pyqtSignal(object)
    
    def __init__(self, parent: Optional[QDialog] = None) -> None:
        """Initialize the window selector dialog.
        
        Args:
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self.setWindowTitle("选择目标窗口")
        self.setMinimumSize(500, 400)
        self.selected_window: Optional[WindowInfo] = None
        self._windows: List[WindowInfo] = []
        
        self._init_ui()
        self._refresh_windows()
    
    def _init_ui(self) -> None:
        """Initialize the dialog UI components."""
        layout = QVBoxLayout(self)
        
        info_label = QLabel(
            "选择一个窗口作为执行目标，OCR将在该窗口区域内识别"
        )
        info_label.setStyleSheet("color: #666; margin-bottom: 10px;")
        layout.addWidget(info_label)
        
        # Search bar
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("搜索:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("输入窗口标题关键词...")
        self.search_edit.textChanged.connect(self._filter_windows)
        search_layout.addWidget(self.search_edit)
        layout.addLayout(search_layout)
        
        # Window list
        self.window_list = QListWidget()
        self.window_list.itemDoubleClicked.connect(self._on_select)
        layout.addWidget(self.window_list)
        
        # Info group
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
        
        # Buttons
        btn_layout = QHBoxLayout()
        
        refresh_btn = QPushButton("🔄 刷新列表")
        refresh_btn.clicked.connect(self._refresh_windows)
        btn_layout.addWidget(refresh_btn)
        
        btn_layout.addStretch()
        
        select_btn = QPushButton("✓ 选择")
        select_btn.clicked.connect(self._on_select)
        select_btn.setStyleSheet(
            "background-color: #4CAF50; color: white; padding: 8px 16px;"
        )
        btn_layout.addWidget(select_btn)
        
        cancel_btn = QPushButton("取消")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(cancel_btn)
        
        layout.addLayout(btn_layout)
    
    def _refresh_windows(self) -> None:
        """Refresh the window list."""
        self.window_list.clear()
        self._windows = get_all_windows()
        
        if not self._windows:
            item = QListWidgetItem("⚠️ 未找到窗口，请确保有应用程序在运行")
            item.setData(Qt.UserRole, None)
            self.window_list.addItem(item)
            return
        
        for win in self._windows:
            item = QListWidgetItem(f"🪟 {win.title}")
            item.setData(Qt.UserRole, win)
            self.window_list.addItem(item)
        
        self.search_edit.clear()
    
    def _filter_windows(self, text: str) -> None:
        """Filter the window list by search text.
        
        Args:
            text: Search text to filter by.
        """
        text = text.lower()
        for i in range(self.window_list.count()):
            item = self.window_list.item(i)
            win = item.data(Qt.UserRole)
            visible = text in win.title.lower() if text else True
            item.setHidden(not visible)
    
    def _on_window_highlight(self, index: int) -> None:
        """Handle window list selection change.
        
        Args:
            index: Index of selected item.
        """
        if index >= 0:
            item = self.window_list.item(index)
            win = item.data(Qt.UserRole)
            
            if win:
                title_text = (
                    win.title[:50] + "..." 
                    if len(win.title) > 50 
                    else win.title
                )
                self.title_label.setText(title_text)
                self.position_label.setText(f"({win.left}, {win.top})")
                self.size_label.setText(f"{win.width} × {win.height}")
            else:
                self.title_label.setText("-")
                self.position_label.setText("-")
                self.size_label.setText("-")
    
    def _on_select(self) -> None:
        """Handle window selection."""
        current = self.window_list.currentItem()
        if current:
            self.selected_window = current.data(Qt.UserRole)
            self.accept()
    
    def get_selected_window(self) -> Optional[WindowInfo]:
        """Get the selected window.
        
        Returns:
            WindowInfo of selected window, or None.
        """
        return self.selected_window


class QuickWindowPicker(QDialog):
    """Quick picker dialog that picks the active window."""
    
    window_picked = pyqtSignal(object)
    
    def __init__(self, parent: Optional[QDialog] = None) -> None:
        """Initialize the quick window picker.
        
        Args:
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self.setWindowTitle("点击目标窗口")
        self.setFixedSize(400, 150)
        self.setWindowFlags(
            self.windowFlags() | Qt.WindowStaysOnTopHint
        )
        
        self.selected_window: Optional[WindowInfo] = None
        
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
        btn.setStyleSheet(
            "font-size: 16px; padding: 15px; "
            "background-color: #2196F3; color: white;"
        )
        btn.clicked.connect(self._start_pick)
        layout.addWidget(btn)
    
    def _start_pick(self) -> None:
        """Start the window picking process."""
        self.status_label.setText("3秒后自动获取当前活动窗口...")
        self.status_label.setStyleSheet("color: #f44336; font-weight: bold;")
        
        self.hide()
        QTimer.singleShot(3000, self._get_active_window)
    
    def _get_active_window(self) -> None:
        """Get the active window after delay."""
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
        """Get the selected window.
        
        Returns:
            WindowInfo of selected window, or None.
        """
        return self.selected_window
