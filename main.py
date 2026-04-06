#!/usr/bin/env python3
"""RabAI AutoClick v22 - Main Application Entry Point.

This is the integrated main entry point that combines the original
execution engine with v22 advanced features.
"""

import os
import platform
import sys
from typing import List, Optional

# Set multiprocessing start method on macOS
if platform.system() == 'Darwin':
    import multiprocessing as mp
    mp.set_start_method('spawn', force=True)

# Disable oneDNN/MKL for compatibility
os.environ['FLAGS_use_mkldnn'] = '0'
os.environ['FLAGS_enable_onednn_backend'] = '0'
os.environ['FLAGS_allocator_strategy'] = 'naive_best_fit'

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication
from ui.main_window_v22 import MainWindow


def main() -> int:
    """Initialize and run the RabAI AutoClick application.
    
    Returns:
        Application exit code.
    """
    app = QApplication(sys.argv)
    app.setApplicationName("RabAI AutoClick")
    app.setApplicationVersion("22.0.0")
    app.setStyle("Fusion")
    
    # Global application stylesheet
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
    
    # Create and show main window
    window = MainWindow()
    window.show()
    
    # Run application event loop
    return app.exec_()


if __name__ == "__main__":
    sys.exit(main())
