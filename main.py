"""
RabAI AutoClick v22 - 整合版主入口
结合原始项目的执行引擎和v22的高级功能
"""

import sys
import os
import platform
import multiprocessing as mp

if platform.system() == 'Darwin':
    mp.set_start_method('spawn', force=True)

os.environ['FLAGS_use_mkldnn'] = '0'
os.environ['FLAGS_enable_onednn_backend'] = '0'
os.environ['FLAGS_allocator_strategy'] = 'naive_best_fit'

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import Qt
from ui.main_window_v22 import MainWindow


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("RabAI AutoClick")
    app.setApplicationVersion("22.0.0")
    app.setStyle("Fusion")
    
    app.setStyleSheet("""
        QMainWindow {
            background-color: #f5f5f5;
        }
        QTabWidget::pane {
            border: 1px solid #ddd;
            background-color: white;
        }
        QTabBar::tab {
            padding: 8px 16px;
            margin-right: 2px;
            background-color: #e0e0e0;
            border: 1px solid #ccc;
            border-bottom: none;
            border-top-left-radius: 4px;
            border-top-right-radius: 4px;
        }
        QTabBar::tab:selected {
            background-color: white;
            border-bottom: 1px solid white;
        }
        QPushButton {
            padding: 6px 12px;
            border-radius: 4px;
            background-color: #2196F3;
            color: white;
            border: none;
        }
        QPushButton:hover {
            background-color: #1976D2;
        }
        QPushButton:pressed {
            background-color: #0D47A1;
        }
        QPushButton:disabled {
            background-color: #BDBDBD;
        }
        QTreeWidget {
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        QTextEdit {
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        QLineEdit {
            padding: 4px 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        QSpinBox {
            padding: 4px 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        QGroupBox {
            font-weight: bold;
            border: 1px solid #ddd;
            border-radius: 4px;
            margin-top: 8px;
            padding-top: 8px;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 5px;
        }
    """)
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
