from PyQt5.QtWidgets import (
    QWidget, QHBoxLayout, QPushButton, QLabel, QFrame, QMenu, QAction
)
from PyQt5.QtCore import Qt, pyqtSignal, QPoint
from PyQt5.QtGui import QFont, QCursor


class MiniToolbar(QWidget):
    run_clicked = pyqtSignal()
    stop_clicked = pyqtSignal()
    region_clicked = pyqtSignal()
    window_clicked = pyqtSignal()
    settings_clicked = pyqtSignal()
    switch_to_full = pyqtSignal()
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._drag_pos = QPoint()
        self._init_ui()
    
    def _init_ui(self):
        self.setWindowFlags(
            Qt.WindowType.Window | 
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.FramelessWindowHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_MacAlwaysShowToolWindow, True)
        self.setFixedHeight(40)
        self.setMinimumWidth(400)
        
        self.setStyleSheet("""
            QWidget {
                background-color: #2d2d2d;
                border-radius: 8px;
            }
            QPushButton {
                background-color: #3d3d3d;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 6px 12px;
                font-size: 12px;
                min-width: 60px;
            }
            QPushButton:hover {
                background-color: #4d4d4d;
            }
            QPushButton:pressed {
                background-color: #5d5d5d;
            }
            QPushButton#run_btn {
                background-color: #4CAF50;
            }
            QPushButton#run_btn:hover {
                background-color: #5CBF60;
            }
            QPushButton#stop_btn {
                background-color: #f44336;
            }
            QPushButton#stop_btn:hover {
                background-color: #ff5346;
            }
            QLabel {
                color: white;
                font-size: 12px;
                padding: 0 8px;
            }
        """)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 4, 8, 4)
        layout.setSpacing(6)
        
        self.title_label = QLabel("🤖 RabAI")
        self.title_label.setFont(QFont("Arial", 11, QFont.Weight.Bold))
        layout.addWidget(self.title_label)
        
        layout.addWidget(self._create_separator())
        
        self.run_btn = QPushButton("▶ 运行")
        self.run_btn.setObjectName("run_btn")
        self.run_btn.clicked.connect(self.run_clicked.emit)
        layout.addWidget(self.run_btn)
        
        self.stop_btn = QPushButton("⏹ 停止")
        self.stop_btn.setObjectName("stop_btn")
        self.stop_btn.clicked.connect(self.stop_clicked.emit)
        layout.addWidget(self.stop_btn)
        
        layout.addWidget(self._create_separator())
        
        self.region_btn = QPushButton("📐 区域")
        self.region_btn.clicked.connect(self.region_clicked.emit)
        layout.addWidget(self.region_btn)
        
        self.window_btn = QPushButton("🪟 窗口")
        self.window_btn.clicked.connect(self.window_clicked.emit)
        layout.addWidget(self.window_btn)
        
        layout.addWidget(self._create_separator())
        
        self.status_label = QLabel("就绪")
        layout.addWidget(self.status_label)
        
        layout.addStretch()
        
        self.menu_btn = QPushButton("☰")
        self.menu_btn.setFixedWidth(30)
        self.menu_btn.clicked.connect(self._show_menu)
        layout.addWidget(self.menu_btn)
        
        self._create_menu()
    
    def _create_separator(self) -> QFrame:
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.VLine)
        sep.setStyleSheet("background-color: #555;")
        sep.setFixedWidth(1)
        return sep
    
    def _create_menu(self):
        self.menu = QMenu(self)
        self.menu.setStyleSheet("""
            QMenu {
                background-color: #3d3d3d;
                color: white;
                border: 1px solid #555;
                border-radius: 4px;
            }
            QMenu::item {
                padding: 8px 20px;
            }
            QMenu::item:selected {
                background-color: #4d4d4d;
            }
        """)
        
        self.full_mode_action = QAction("🖥 完整窗口", self)
        self.full_mode_action.triggered.connect(self.switch_to_full.emit)
        self.menu.addAction(self.full_mode_action)
        
        self.menu.addSeparator()
        
        self.settings_action = QAction("⚙ 设置", self)
        self.settings_action.triggered.connect(self.settings_clicked.emit)
        self.menu.addAction(self.settings_action)
    
    def _show_menu(self):
        self.menu.exec_(self.menu_btn.mapToGlobal(self.menu_btn.rect().bottomLeft()))
    
    def set_status(self, text: str, color: str = "white"):
        self.status_label.setText(text)
        self.status_label.setStyleSheet(f"color: {color};")
    
    def set_region(self, x: int, y: int, w: int, h: int):
        self.region_btn.setText(f"📐 ({x},{y},{w}x{h})")
        self.region_btn.setStyleSheet("background-color: #4CAF50;")
    
    def set_window(self, name: str):
        self.window_btn.setText(f"🪟 {name[:8]}")
        self.window_btn.setStyleSheet("background-color: #2196F3;")
    
    def reset_region(self):
        self.region_btn.setText("📐 区域")
        self.region_btn.setStyleSheet("")
    
    def reset_window(self):
        self.window_btn.setText("🪟 窗口")
        self.window_btn.setStyleSheet("")
    
    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
    
    def mouseMoveEvent(self, event):
        if event.buttons() & Qt.MouseButton.LeftButton:
            self.move(event.globalPosition().toPoint() - self._drag_pos)
    
    def mouseDoubleClickEvent(self, event):
        self.switch_to_full.emit()
