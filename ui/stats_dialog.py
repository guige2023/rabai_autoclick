"""Statistics dialog for RabAI AutoClick.

Displays execution statistics, step performance metrics,
and workflow execution history.
"""

import os
import sys
import contextlib
from typing import Dict, Any, List, Optional

from PyQt5.QtCore import Qt
from PyQt5.QtGui import QColor, QFont
from PyQt5.QtWidgets import (
    QComboBox, QDateTimeEdit, QDialog, QFormLayout, QGroupBox,
    QHBoxLayout, QHeaderView, QLabel, QPushButton, QTabWidget,
    QTableWidget, QTableWidgetItem, QTextEdit, QVBoxLayout, QWidget
)


@contextlib.contextmanager
def _batch_updates(widget):
    """Context manager to batch UI updates for performance."""
    widget.setUpdatesEnabled(False)
    try:
        yield
    finally:
        widget.setUpdatesEnabled(True)
        widget.update()


# Add project root to path
sys.path.insert(0, os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
))

from utils.execution_stats import execution_stats
from ui.theme import theme_manager


class StatsDialog(QDialog):
    """Dialog displaying execution statistics and performance metrics."""

    def __init__(self, parent: Optional[QDialog] = None) -> None:
        """Initialize the stats dialog.

        Args:
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self.setWindowTitle("执行统计分析")
        self.setMinimumSize(800, 600)
        self._colors = theme_manager.colors
        theme_manager.theme_changed.connect(self._on_theme_changed)

        self._init_ui()
        self._load_data()

    def _on_theme_changed(self, theme):
        """Handle theme changes to update colors."""
        self._colors = theme_manager.colors
        self._apply_stylesheet()

    def _apply_stylesheet(self) -> None:
        """Apply themed stylesheet to the dialog."""
        colors = self._colors
        self.setStyleSheet(f"""
            QDialog {{
                background-color: {colors['bg_widget']};
            }}
            QLabel {{
                color: {colors['text_primary']};
            }}
            QGroupBox {{
                font-weight: bold;
                border: 1px solid {colors['border']};
                border-radius: 4px;
                margin-top: 8px;
                padding-top: 8px;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                color: {colors['text_primary']};
            }}
            QTabWidget::pane {{
                border: 1px solid {colors['border']};
                background-color: {colors['bg_widget']};
            }}
            QTabBar::tab {{
                padding: 6px 12px;
                background-color: {colors['bg_toolbar']};
                color: {colors['text_primary']};
            }}
            QTabBar::tab:selected {{
                background-color: {colors['bg_widget']};
            }}
            QTableWidget {{
                background-color: {colors['bg_widget']};
                color: {colors['text_primary']};
                border: 1px solid {colors['border']};
            }}
            QHeaderView::section {{
                background-color: {colors['bg_toolbar']};
                color: {colors['text_primary']};
                padding: 4px;
                border: 1px solid {colors['border']};
            }}
            QTextEdit {{
                background-color: {colors['bg_widget']};
                color: {colors['text_primary']};
                border: 1px solid {colors['border']};
            }}
        """)

    def _init_ui(self) -> None:
        """Initialize the dialog UI components."""
        layout = QVBoxLayout(self)
        self._apply_stylesheet()
        
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)
        
        self._create_overview_tab()
        self._create_steps_tab()
        self._create_history_tab()
        
        # Bottom buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        
        refresh_btn = QPushButton("🔄 刷新数据")
        refresh_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))
        refresh_btn.clicked.connect(self._load_data)
        btn_layout.addWidget(refresh_btn)

        clear_btn = QPushButton("🗑 清除历史")
        clear_btn.setStyleSheet(theme_manager.get_button_stylesheet('danger'))
        clear_btn.clicked.connect(self._clear_history)
        btn_layout.addWidget(clear_btn)

        close_btn = QPushButton("关闭")
        close_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))
        close_btn.clicked.connect(self.accept)
        btn_layout.addWidget(close_btn)

        layout.addLayout(btn_layout)
    
    def _create_overview_tab(self) -> None:
        """Create the overview tab with summary statistics."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Summary statistics
        summary_group = QGroupBox("总体统计")
        form = QFormLayout()
        
        self.total_sessions_label = QLabel("0")
        self.total_sessions_label.setFont(QFont("Microsoft YaHei", 12, QFont.Bold))
        form.addRow("执行次数:", self.total_sessions_label)
        
        self.total_loops_label = QLabel("0")
        self.total_loops_label.setFont(QFont("Microsoft YaHei", 12))
        form.addRow("总循环次数:", self.total_loops_label)
        
        self.total_duration_label = QLabel("0秒")
        self.total_duration_label.setFont(QFont("Microsoft YaHei", 12))
        form.addRow("总运行时间:", self.total_duration_label)
        
        self.avg_duration_label = QLabel("0秒")
        self.avg_duration_label.setFont(QFont("Microsoft YaHei", 12))
        form.addRow("平均执行时间:", self.avg_duration_label)
        
        self.success_rate_label = QLabel("0%")
        self.success_rate_label.setFont(QFont("Microsoft YaHei", 12))
        form.addRow("成功率:", self.success_rate_label)
        
        summary_group.setLayout(form)
        layout.addWidget(summary_group)
        
        # Step type statistics
        step_group = QGroupBox("步骤类型统计")
        step_layout = QVBoxLayout()
        
        self.step_table = QTableWidget()
        self.step_table.setColumnCount(5)
        self.step_table.setHorizontalHeaderLabels([
            "步骤类型", "执行次数", "平均耗时", "总耗时", "成功率"
        ])
        self.step_table.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )
        step_layout.addWidget(self.step_table)
        
        step_group.setLayout(step_layout)
        layout.addWidget(step_group)
        
        self.tabs.addTab(tab, "📊 总览")
    
    def _create_steps_tab(self) -> None:
        """Create the step performance tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        self.step_perf_table = QTableWidget()
        self.step_perf_table.setColumnCount(7)
        self.step_perf_table.setHorizontalHeaderLabels([
            "步骤类型", "执行次数", "平均耗时", "最小耗时",
            "最大耗时", "成功率", "错误数"
        ])
        self.step_perf_table.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )
        layout.addWidget(self.step_perf_table)
        
        # Common errors section
        error_group = QGroupBox("常见错误")
        error_layout = QVBoxLayout()
        
        self.error_text = QTextEdit()
        self.error_text.setReadOnly(True)
        self.error_text.setMaximumHeight(150)
        error_layout.addWidget(self.error_text)
        
        error_group.setLayout(error_layout)
        layout.addWidget(error_group)
        
        self.tabs.addTab(tab, "⚡ 步骤性能")
    
    def _create_history_tab(self) -> None:
        """Create the execution history tab."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        self.history_table = QTableWidget()
        self.history_table.setColumnCount(6)
        self.history_table.setHorizontalHeaderLabels([
            "时间", "工作流", "循环次数", "总耗时", "平均循环耗时", "状态"
        ])
        self.history_table.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )
        layout.addWidget(self.history_table)
        
        self.tabs.addTab(tab, "📜 执行历史")
    
    def _load_data(self) -> None:
        """Load and display statistics data."""
        summary = execution_stats.get_summary()
        
        # Update summary labels
        self.total_sessions_label.setText(str(summary['total_sessions']))
        self.total_loops_label.setText(str(summary['total_loops']))
        
        # Format duration display
        total_dur = summary['total_duration']
        if total_dur > 3600:
            self.total_duration_label.setText(f"{total_dur/3600:.1f}小时")
        elif total_dur > 60:
            self.total_duration_label.setText(f"{total_dur/60:.1f}分钟")
        else:
            self.total_duration_label.setText(f"{total_dur:.1f}秒")
        
        avg_dur = summary['avg_duration']
        if avg_dur > 60:
            self.avg_duration_label.setText(f"{avg_dur/60:.1f}分钟")
        else:
            self.avg_duration_label.setText(f"{avg_dur:.1f}秒")
        
        # Format success rate with color coding
        rate = summary['success_rate']
        self.success_rate_label.setText(f"{rate:.1f}%")
        if rate >= 90:
            self.success_rate_label.setStyleSheet(
                f"color: {self._colors['success']}; font-weight: bold;"
            )
        elif rate >= 70:
            self.success_rate_label.setStyleSheet(
                f"color: {self._colors['warning']}; font-weight: bold;"
            )
        else:
            self.success_rate_label.setStyleSheet(
                f"color: {self._colors['error']}; font-weight: bold;"
            )
        
        # Populate step statistics table
        with _batch_updates(self.step_table):
            self.step_table.setRowCount(0)
            for stype, stats in summary['step_stats'].items():
                row = self.step_table.rowCount()
                self.step_table.insertRow(row)
                self.step_table.setItem(
                    row, 0, QTableWidgetItem(stype)
                )
                self.step_table.setItem(
                    row, 1, QTableWidgetItem(str(stats['count']))
                )
                self.step_table.setItem(
                    row, 2, QTableWidgetItem(f"{stats['avg_duration']:.2f}秒")
                )
                self.step_table.setItem(
                    row, 3, QTableWidgetItem(f"{stats['total_duration']:.1f}秒")
                )
                self.step_table.setItem(
                    row, 4, QTableWidgetItem(f"{stats['success_rate']:.1f}%")
                )

        # Populate step performance table
        step_perf = execution_stats.get_step_performance()
        with _batch_updates(self.step_perf_table):
            self.step_perf_table.setRowCount(0)

            all_errors: List[str] = []
            for stype, stats in step_perf.items():
                row = self.step_perf_table.rowCount()
                self.step_perf_table.insertRow(row)
                self.step_perf_table.setItem(
                    row, 0, QTableWidgetItem(stype)
                )
                self.step_perf_table.setItem(
                    row, 1, QTableWidgetItem(str(stats['count']))
                )
                self.step_perf_table.setItem(
                    row, 2, QTableWidgetItem(f"{stats['avg_duration']:.2f}秒")
                )
                self.step_perf_table.setItem(
                    row, 3, QTableWidgetItem(f"{stats['min_duration']:.2f}秒")
                )
                self.step_perf_table.setItem(
                    row, 4, QTableWidgetItem(f"{stats['max_duration']:.2f}秒")
                )
                self.step_perf_table.setItem(
                    row, 5, QTableWidgetItem(f"{stats['success_rate']:.1f}%")
                )
                self.step_perf_table.setItem(
                    row, 6, QTableWidgetItem(str(stats['error_count']))
                )

                if stats['common_errors']:
                    all_errors.extend(
                        [f"[{stype}] {e}" for e in stats['common_errors']]
                    )

        self.error_text.setText(
            '\n'.join(all_errors[-20:]) if all_errors else "暂无错误记录"
        )

        # Populate history table
        recent = execution_stats.get_recent_sessions(30)
        with _batch_updates(self.history_table):
            self.history_table.setRowCount(0)

            for session in recent:
                row = self.history_table.rowCount()
                self.history_table.insertRow(row)

                self.history_table.setItem(
                    row, 0, QTableWidgetItem(session.get('date', '-'))
                )
                self.history_table.setItem(
                    row, 1,
                    QTableWidgetItem(session.get('workflow_name', '-')[:20])
                )
                self.history_table.setItem(
                    row, 2, QTableWidgetItem(str(session.get('loop_count', 1)))
                )

                total_dur = session.get('total_duration', 0)
                self.history_table.setItem(
                    row, 3, QTableWidgetItem(f"{total_dur:.1f}秒")
                )

                avg_dur = session.get('avg_loop_duration', 0)
                self.history_table.setItem(
                    row, 4, QTableWidgetItem(f"{avg_dur:.1f}秒")
                )

                status = "✓ 成功" if session.get('success', False) else "✗ 失败"
                status_item = QTableWidgetItem(status)
                if session.get('success', False):
                    status_item.setForeground(QColor(self._colors['success']))
                else:
                    status_item.setForeground(QColor(self._colors['error']))
                self.history_table.setItem(row, 5, status_item)

    def _clear_history(self) -> None:
        """Clear all execution history after confirmation."""
        from PyQt5.QtWidgets import QMessageBox
        
        reply = QMessageBox.question(
            self, '确认清除',
            '确定要清除所有执行历史记录吗？\n此操作不可恢复！',
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            execution_stats.clear_history()
            self._load_data()
