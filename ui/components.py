"""
Reusable GUI components for RabAI AutoClick.
Provides styled widgets and specialized components.
"""

import json
import logging
from typing import Dict, Any, Optional, List

try:
    from PyQt5.QtWidgets import (
        QWidget, QPushButton, QLineEdit, QComboBox, QTextEdit,
        QVBoxLayout, QHBoxLayout, QFormLayout, QLabel, QGroupBox,
        QListWidget, QListWidgetItem, QTableWidget, QTableWidgetItem,
        QHeaderView, QSpinBox, QDoubleSpinBox, QCheckBox, QFrame,
        QProgressBar, QScrollArea, QGridLayout, QSplitter
    )
    from PyQt5.QtCore import Qt, pyqtSignal, QTimer
    from PyQt5.QtGui import QFont, QColor
    PYQT_AVAILABLE = True
except ImportError:
    PYQT_AVAILABLE = False

# Import theme system
try:
    from ui.themes import get_theme_manager, ThemeConfig
    THEME_AVAILABLE = True
except ImportError:
    THEME_AVAILABLE = False
    get_theme_manager = None
    ThemeConfig = None

logger = logging.getLogger(__name__)


# =============================================================================
# Styled Basic Widgets
# =============================================================================

class StyledButton(QPushButton):
    """A styled button with theme support."""
    
    def __init__(self, text: str = "", is_primary: bool = False, parent=None):
        super().__init__(text, parent)
        self._is_primary = is_primary
        if is_primary:
            self.setProperty("class", "primary")
        self._apply_style()
    
    def _apply_style(self):
        if THEME_AVAILABLE:
            theme = get_theme_manager().get_current_theme()
            if theme:
                styles = self._generate_styles(theme)
                self.setStyleSheet(styles)
    
    def _generate_styles(self, theme: ThemeConfig) -> str:
        if self._is_primary:
            return f"""
                QPushButton {{
                    background-color: {theme.colors.primary};
                    border: 1px solid {theme.colors.primary};
                    border-radius: {theme.spacing.border_radius}px;
                    padding: 6px 16px;
                    color: {theme.colors.text_on_primary};
                    font-weight: bold;
                }}
                QPushButton:hover {{
                    background-color: {theme.colors.primary_hover};
                    border-color: {theme.colors.primary_hover};
                }}
                QPushButton:pressed {{
                    background-color: {theme.colors.primary_pressed};
                    border-color: {theme.colors.primary_pressed};
                }}
                QPushButton:disabled {{
                    background-color: {theme.colors.bg_secondary};
                    border-color: {theme.colors.border};
                    color: {theme.colors.text_disabled};
                }}
            """
        else:
            return f"""
                QPushButton {{
                    background-color: {theme.colors.bg_secondary};
                    border: 1px solid {theme.colors.border};
                    border-radius: {theme.spacing.border_radius}px;
                    padding: 6px 16px;
                    color: {theme.colors.text_primary};
                }}
                QPushButton:hover {{
                    background-color: {theme.colors.bg_tertiary};
                    border-color: {theme.colors.border_focus};
                }}
                QPushButton:pressed {{
                    background-color: {theme.colors.bg_tertiary};
                }}
                QPushButton:disabled {{
                    color: {theme.colors.text_disabled};
                }}
            """
    
    def set_primary(self, is_primary: bool) -> None:
        self._is_primary = is_primary
        if is_primary:
            self.setProperty("class", "primary")
        else:
            self.setProperty("class", "")
        self._apply_style()


class StyledLineEdit(QLineEdit):
    """A styled line edit with theme support."""
    
    def __init__(self, placeholder: str = "", parent=None):
        super().__init__(parent)
        self.setPlaceholderText(placeholder)
        self._apply_style()
    
    def _apply_style(self):
        if THEME_AVAILABLE:
            theme = get_theme_manager().get_current_theme()
            if theme:
                self.setStyleSheet(f"""
                    QLineEdit {{
                        background-color: {theme.colors.bg_input};
                        border: 1px solid {theme.colors.border};
                        border-radius: {theme.spacing.border_radius}px;
                        padding: 6px;
                        color: {theme.colors.text_primary};
                    }}
                    QLineEdit:focus {{
                        border-color: {theme.colors.border_focus};
                    }}
                    QLineEdit:disabled {{
                        background-color: {theme.colors.bg_secondary};
                        color: {theme.colors.text_disabled};
                    }}
                """)


class StyledComboBox(QComboBox):
    """A styled combobox with theme support."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._apply_style()
    
    def _apply_style(self):
        if THEME_AVAILABLE:
            theme = get_theme_manager().get_current_theme()
            if theme:
                self.setStyleSheet(f"""
                    QComboBox {{
                        background-color: {theme.colors.bg_input};
                        border: 1px solid {theme.colors.border};
                        border-radius: {theme.spacing.border_radius}px;
                        padding: 6px;
                        color: {theme.colors.text_primary};
                    }}
                    QComboBox:focus {{
                        border-color: {theme.colors.border_focus};
                    }}
                    QComboBox::drop-down {{
                        border: none;
                        width: 20px;
                    }}
                    QComboBox QAbstractItemView {{
                        background-color: {theme.colors.bg_input};
                        border: 1px solid {theme.colors.border};
                        color: {theme.colors.text_primary};
                        selection-background-color: {theme.colors.selected};
                    }}
                """)


# =============================================================================
# Workflow Components
# =============================================================================

class WorkflowStepCard(QWidget):
    """
    A card widget that displays a workflow step with its configuration.
    """
    edit_requested = pyqtSignal(int)  # step_id
    delete_requested = pyqtSignal(int)  # step_id
    move_up_requested = pyqtSignal(int)  # step_id
    move_down_requested = pyqtSignal(int)  # step_id
    
    def __init__(self, step_data: Dict[str, Any], step_index: int, parent=None):
        super().__init__(parent)
        self.step_data = step_data
        self.step_index = step_index
        self.step_id = step_data.get('id', 0)
        self._init_ui()
    
    def _init_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Left side - step info
        info_layout = QVBoxLayout()
        
        self.type_label = QLabel(self.step_data.get('type', 'unknown'))
        self.type_label.setStyleSheet("font-weight: bold; font-size: 11pt;")
        
        self.desc_label = QLabel(self._get_description())
        self.desc_label.setStyleSheet("color: #666; font-size: 9pt;")
        self.desc_label.setWordWrap(True)
        
        info_layout.addWidget(self.type_label)
        info_layout.addWidget(self.desc_label)
        
        # Right side - action buttons
        btn_layout = QVBoxLayout()
        btn_layout.setSpacing(4)
        
        self.edit_btn = QPushButton("编辑")
        self.edit_btn.setMaximumWidth(60)
        self.delete_btn = QPushButton("删除")
        self.delete_btn.setMaximumWidth(60)
        
        nav_layout = QHBoxLayout()
        self.up_btn = QPushButton("↑")
        self.up_btn.setMaximumWidth(30)
        self.down_btn = QPushButton("↓")
        self.down_btn.setMaximumWidth(30)
        nav_layout.addWidget(self.up_btn)
        nav_layout.addWidget(self.down_btn)
        
        btn_layout.addWidget(self.edit_btn)
        btn_layout.addWidget(self.delete_btn)
        btn_layout.addLayout(nav_layout)
        
        layout.addLayout(info_layout, 1)
        layout.addLayout(btn_layout)
        
        # Apply card style
        self._apply_card_style()
        
        # Connect signals
        self.edit_btn.clicked.connect(lambda: self.edit_requested.emit(self.step_id))
        self.delete_btn.clicked.connect(lambda: self.delete_requested.emit(self.step_id))
        self.up_btn.clicked.connect(lambda: self.move_up_requested.emit(self.step_id))
        self.down_btn.clicked.connect(lambda: self.move_down_requested.emit(self.step_id))
    
    def _apply_card_style(self):
        if THEME_AVAILABLE and get_theme_manager():
            theme = get_theme_manager().get_current_theme()
            if theme:
                self.setStyleSheet(f"""
                    QWidget {{
                        background-color: {theme.colors.bg_secondary};
                        border: 1px solid {theme.colors.border};
                        border-radius: {theme.spacing.border_radius}px;
                    }}
                    QPushButton {{
                        background-color: {theme.colors.bg_tertiary};
                        border: 1px solid {theme.colors.border};
                        border-radius: {theme.spacing.border_radius}px;
                        padding: 4px 8px;
                    }}
                    QPushButton:hover {{
                        background-color: {theme.colors.primary};
                        color: {theme.colors.text_on_primary};
                    }}
                """)
    
    def _get_description(self) -> str:
        """Generate a description from step config."""
        config = self.step_data.copy()
        config.pop('id', None)
        config.pop('type', None)
        
        if not config:
            return "无参数"
        
        parts = []
        for key, value in list(config.items())[:3]:
            if value is not None and value != "":
                if isinstance(value, str) and len(value) > 20:
                    value = value[:17] + "..."
                parts.append(f"{key}: {value}")
        
        return ", ".join(parts) if parts else "无参数"
    
    def update_data(self, step_data: Dict[str, Any]) -> None:
        """Update the card with new step data."""
        self.step_data = step_data
        self.type_label.setText(step_data.get('type', 'unknown'))
        self.desc_label.setText(self._get_description())


class ActionParameterEditor(QWidget):
    """
    Editor widget for action parameters.
    Supports various parameter types with appropriate input widgets.
    """
    config_changed = pyqtSignal()
    
    def __init__(self, action_info: Dict[str, Any], parent=None):
        super().__init__(parent)
        self.action_info = action_info
        self.widgets = {}
        self._init_ui()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(5)
        
        required_params = self.action_info.get('required_params', [])
        optional_params = self.action_info.get('optional_params', {})
        
        if required_params:
            required_group = QGroupBox("必填参数")
            required_layout = QFormLayout()
            required_layout.setSpacing(5)
            
            for param in required_params:
                widget = self._create_param_widget(param, None)
                self.widgets[param] = widget
                
                row_widget = QWidget()
                row_layout = QVBoxLayout(row_widget)
                row_layout.setContentsMargins(0, 0, 0, 0)
                row_layout.setSpacing(2)
                row_layout.addWidget(widget)
                
                desc = self._get_param_description(param)
                if desc:
                    help_label = QLabel(desc)
                    help_label.setStyleSheet("color: #666; font-size: 10px;")
                    help_label.setWordWrap(True)
                    row_layout.addWidget(help_label)
                
                required_layout.addRow(f"{param}:", row_widget)
            
            required_group.setLayout(required_layout)
            layout.addWidget(required_group)
        
        if optional_params:
            optional_group = QGroupBox("可选参数")
            optional_layout = QFormLayout()
            optional_layout.setSpacing(5)
            
            for param, default_value in optional_params.items():
                widget = self._create_param_widget(param, default_value)
                self.widgets[param] = widget
                
                row_widget = QWidget()
                row_layout = QVBoxLayout(row_widget)
                row_layout.setContentsMargins(0, 0, 0, 0)
                row_layout.setSpacing(2)
                row_layout.addWidget(widget)
                
                desc = self._get_param_description(param)
                if desc:
                    help_label = QLabel(desc)
                    help_label.setStyleSheet("color: #666; font-size: 10px;")
                    help_label.setWordWrap(True)
                    row_layout.addWidget(help_label)
                
                optional_layout.addRow(f"{param}:", row_widget)
            
            optional_group.setLayout(optional_layout)
            layout.addWidget(optional_group)
        
        layout.addStretch()
    
    def _get_param_description(self, param: str) -> str:
        """Get description for a parameter."""
        descriptions = {
            'x': '屏幕X坐标（水平位置）',
            'y': '屏幕Y坐标（垂直位置）',
            'start_x': '拖拽起始点X坐标',
            'start_y': '拖拽起始点Y坐标',
            'end_x': '拖拽结束点X坐标',
            'end_y': '拖拽结束点Y坐标',
            'region': '识别区域，格式: x,y,宽度,高度',
            'text': '要输入的文字内容',
            'interval': '操作间隔时间，单位: 秒',
            'duration': '持续时间，单位: 秒',
            'button': '鼠标按钮: left(左键)、right(右键)、middle(中键)',
            'direction': '滚轮方向: up(向上滚动)、down(向下滚动)',
            'key': '键盘按键',
            'keys': '组合键，用 + 连接',
            'template': '模板图片路径',
            'confidence': '匹配置信度，0-1之间',
            'click_text': 'OCR识别后点击包含此文字的区域',
            'contains': '只检测是否存在，不执行点击操作',
            'clicks': '点击次数，默认1次',
            'count': '循环次数',
            'delay': '延时时间，单位: 秒',
            'relative': '是否相对坐标',
            'enter_after': '输入后是否按回车键',
            'script': 'Python脚本代码',
            'command': '系统命令',
            'url': '网页地址',
        }
        return descriptions.get(param, '')
    
    def _create_param_widget(self, param: str, default_value: Any) -> QWidget:
        """Create appropriate widget for a parameter type."""
        if param in ('x', 'y', 'start_x', 'start_y', 'end_x', 'end_y'):
            spin = QSpinBox()
            spin.setRange(0, 9999)
            spin.setValue(int(default_value) if default_value else 0)
            spin.valueChanged.connect(self.config_changed.emit)
            return spin
        
        if param == 'region':
            line_edit = QLineEdit()
            line_edit.setPlaceholderText("x,y,w,h")
            if default_value:
                line_edit.setText(str(default_value))
            line_edit.textChanged.connect(self.config_changed.emit)
            return line_edit
        
        if isinstance(default_value, bool):
            widget = QCheckBox()
            widget.setChecked(default_value)
            widget.stateChanged.connect(self.config_changed.emit)
            return widget
        
        if param == 'button':
            widget = QComboBox()
            widget.addItem("左键", "left")
            widget.addItem("右键", "right")
            widget.addItem("中键", "middle")
            idx = widget.findData(default_value) if default_value else 0
            widget.setCurrentIndex(idx if idx >= 0 else 0)
            widget.currentIndexChanged.connect(self.config_changed.emit)
            return widget
        
        if param == 'direction':
            widget = QComboBox()
            widget.addItem("向下滚动", "down")
            widget.addItem("向上滚动", "up")
            idx = widget.findData(default_value) if default_value else 0
            widget.setCurrentIndex(idx if idx >= 0 else 0)
            widget.currentIndexChanged.connect(self.config_changed.emit)
            return widget
        
        if isinstance(default_value, int):
            widget = QSpinBox()
            widget.setRange(-99999, 99999)
            widget.setValue(default_value)
            widget.valueChanged.connect(self.config_changed.emit)
            return widget
        
        if isinstance(default_value, float):
            widget = QDoubleSpinBox()
            widget.setRange(-99999, 99999)
            widget.setValue(default_value)
            widget.setSingleStep(0.1)
            widget.valueChanged.connect(self.config_changed.emit)
            return widget
        
        widget = QLineEdit()
        if default_value is not None:
            widget.setText(str(default_value))
        widget.textChanged.connect(self.config_changed.emit)
        return widget
    
    def get_config(self) -> Dict[str, Any]:
        """Get the current configuration."""
        config = {}
        
        for param, widget in self.widgets.items():
            if isinstance(widget, QCheckBox):
                config[param] = widget.isChecked()
            elif isinstance(widget, QSpinBox):
                config[param] = widget.value()
            elif isinstance(widget, QDoubleSpinBox):
                config[param] = widget.value()
            elif isinstance(widget, QComboBox):
                config[param] = widget.currentData()
            elif isinstance(widget, QLineEdit):
                text = widget.text().strip()
                if text:
                    try:
                        if text.startswith('[') or text.startswith('{'):
                            config[param] = json.loads(text)
                        else:
                            config[param] = text
                    except json.JSONDecodeError:
                        config[param] = text
        
        return config
    
    def set_config(self, config: Dict[str, Any]) -> None:
        """Set the configuration."""
        for param, widget in self.widgets.items():
            if param not in config:
                continue
            
            value = config[param]
            
            if isinstance(widget, QCheckBox):
                widget.setChecked(bool(value))
            elif isinstance(widget, QSpinBox):
                widget.setValue(int(value))
            elif isinstance(widget, QDoubleSpinBox):
                widget.setValue(float(value))
            elif isinstance(widget, QComboBox):
                idx = widget.findData(value)
                if idx >= 0:
                    widget.setCurrentIndex(idx)
                elif widget.isEditable():
                    widget.setCurrentText(str(value))
            elif isinstance(widget, QLineEdit):
                if isinstance(value, (list, tuple, dict)):
                    widget.setText(json.dumps(value))
                else:
                    widget.setText(str(value))


# =============================================================================
# Log Viewer Widget
# =============================================================================

class LogViewer(QWidget):
    """
    Widget for displaying application logs with color-coded log levels.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()
        self._connect_logger()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Filter toolbar
        filter_layout = QHBoxLayout()
        
        self.filter_combo = QComboBox()
        self.filter_combo.addItem("全部", "all")
        self.filter_combo.addItem("调试", "DEBUG")
        self.filter_combo.addItem("信息", "INFO")
        self.filter_combo.addItem("成功", "SUCCESS")
        self.filter_combo.addItem("警告", "WARNING")
        self.filter_combo.addItem("错误", "ERROR")
        self.filter_combo.addItem("严重", "CRITICAL")
        
        self.clear_btn = QPushButton("清空")
        self.export_btn = QPushButton("导出")
        
        filter_layout.addWidget(QLabel("过滤:"))
        filter_layout.addWidget(self.filter_combo)
        filter_layout.addWidget(self.clear_btn)
        filter_layout.addWidget(self.export_btn)
        filter_layout.addStretch()
        
        layout.addLayout(filter_layout)
        
        # Log text area
        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)
        layout.addWidget(self.text_edit)
        
        # Apply styling
        self._apply_style()
        
        # Connect signals
        self.clear_btn.clicked.connect(self.clear)
        self.export_btn.clicked.connect(self._export_log)
        self.filter_combo.currentIndexChanged.connect(self._on_filter_changed)
        
        self._current_filter = "all"
    
    def _apply_style(self):
        if THEME_AVAILABLE and get_theme_manager():
            theme = get_theme_manager().get_current_theme()
            if theme:
                self.text_edit.setStyleSheet(f"""
                    QTextEdit {{
                        background-color: #1e1e1e;
                        color: #d4d4d4;
                        font-family: Consolas, 'Microsoft YaHei';
                        font-size: 12px;
                        border: 1px solid {theme.colors.border};
                        border-radius: {theme.spacing.border_radius}px;
                    }}
                """)
    
    def _connect_logger(self):
        """Connect to the application logger."""
        try:
            from utils.app_logger import app_logger
            app_logger.add_listener(self._on_log_entry)
        except ImportError:
            logger.warning("Could not connect to app_logger")
    
    def _on_log_entry(self, entry):
        """Handle a new log entry."""
        if self._current_filter != "all" and entry.level != self._current_filter:
            return
        
        colors = {
            'DEBUG': '#888888',
            'INFO': '#4fc3f7',
            'SUCCESS': '#81c784',
            'WARNING': '#ffb74d',
            'ERROR': '#e57373',
            'CRITICAL': '#f44336'
        }
        color = colors.get(entry.level, '#d4d4d4')
        timestamp = entry.timestamp.strftime("%H:%M:%S")
        html = f'<span style="color: #888;">[{timestamp}]</span> <span style="color: {color};">[{entry.level}]</span> {entry.message}'
        self.text_edit.append(html)
    
    def _on_filter_changed(self, index):
        """Handle filter change."""
        self._current_filter = self.filter_combo.itemData(index)
    
    def _export_log(self):
        """Export logs to a file."""
        try:
            from PyQt5.QtWidgets import QFileDialog
            filepath, _ = QFileDialog.getSaveFileName(
                self, "导出日志", "", "JSON文件 (*.json);;文本文件 (*.txt)"
            )
            if filepath:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(self.text_edit.toPlainText())
        except Exception as e:
            logger.error(f"Failed to export log: {e}")
    
    def clear(self):
        """Clear the log display."""
        self.text_edit.clear()
        try:
            from utils.app_logger import app_logger
            app_logger.clear()
        except ImportError:
            pass
    
    def append_log(self, message: str, level: str = "INFO"):
        """Manually append a log entry."""
        self._on_log_entry(type('LogEntry', (), {
            'level': level,
            'message': message,
            'timestamp': __import__('datetime').datetime.now()
        })())


# =============================================================================
# Metrics Dashboard Widget
# =============================================================================

class MetricsDashboard(QWidget):
    """
    Dashboard widget for displaying execution metrics and statistics.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()
        self._start_auto_refresh()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        
        # Title
        title = QLabel("执行统计")
        title.setStyleSheet("font-size: 14pt; font-weight: bold;")
        layout.addWidget(title)
        
        # Stats grid
        grid_layout = QGridLayout()
        
        self.stat_labels = {}
        
        stats = [
            ("总执行次数", "total_runs"),
            ("成功次数", "success_count"),
            ("失败次数", "fail_count"),
            ("成功率", "success_rate"),
            ("平均执行时间", "avg_duration"),
            ("总耗时", "total_duration"),
            ("步骤总数", "step_count"),
            ("当前循环", "current_loop"),
        ]
        
        for i, (label_text, key) in enumerate(stats):
            row = i // 2
            col = (i % 2) * 2
            
            label = QLabel(label_text + ":")
            value_label = QLabel("0")
            value_label.setObjectName(key)
            
            grid_layout.addWidget(label, row, col)
            grid_layout.addWidget(value_label, row, col + 1)
            
            self.stat_labels[key] = value_label
        
        grid_layout.setColumnStretch(0, 1)
        grid_layout.setColumnStretch(1, 2)
        
        layout.addLayout(grid_layout)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        layout.addWidget(QLabel("执行进度:"))
        layout.addWidget(self.progress_bar)
        
        # History list
        history_label = QLabel("最近执行:")
        layout.addWidget(history_label)
        
        self.history_list = QListWidget()
        self.history_list.setMaximumHeight(150)
        layout.addWidget(self.history_list)
        
        layout.addStretch()
        
        self._apply_style()
    
    def _apply_style(self):
        if THEME_AVAILABLE and get_theme_manager():
            theme = get_theme_manager().get_current_theme()
            if theme:
                self.setStyleSheet(f"""
                    QGroupBox {{
                        background-color: {theme.colors.bg_secondary};
                        border: 1px solid {theme.colors.border};
                        border-radius: {theme.spacing.border_radius}px;
                        padding: {theme.spacing.padding_medium}px;
                    }}
                    QLabel {{
                        color: {theme.colors.text_primary};
                    }}
                """)
    
    def _start_auto_refresh(self):
        """Start automatic refresh of metrics."""
        self._refresh_timer = QTimer()
        self._refresh_timer.timeout.connect(self._refresh_stats)
        self._refresh_timer.start(2000)
    
    def _refresh_stats(self):
        """Refresh the displayed statistics."""
        try:
            from utils.execution_stats import execution_stats
            
            stats = execution_stats.get_summary()
            
            self.stat_labels["total_runs"].setText(str(stats.get('total_runs', 0)))
            self.stat_labels["success_count"].setText(str(stats.get('success_count', 0)))
            self.stat_labels["fail_count"].setText(str(stats.get('fail_count', 0)))
            
            rate = stats.get('success_rate', 0)
            self.stat_labels["success_rate"].setText(f"{rate:.1f}%")
            
            avg_dur = stats.get('avg_duration', 0)
            self.stat_labels["avg_duration"].setText(f"{avg_dur:.2f}秒")
            
            total_dur = stats.get('total_duration', 0)
            self.stat_labels["total_duration"].setText(f"{total_dur:.1f}秒")
            
            self.stat_labels["step_count"].setText(str(stats.get('step_count', 0)))
            self.stat_labels["current_loop"].setText(str(stats.get('current_loop', 0)))
            
        except ImportError:
            pass
        except Exception as e:
            logger.debug(f"Could not refresh stats: {e}")
    
    def update_progress(self, current: int, total: int) -> None:
        """Update the progress bar."""
        if total > 0:
            self.progress_bar.setRange(0, total)
            self.progress_bar.setValue(current)
        else:
            self.progress_bar.setRange(0, 1)
            self.progress_bar.setValue(0)
    
    def add_history_item(self, text: str) -> None:
        """Add an item to the history list."""
        self.history_list.insertItem(0, text)
        if self.history_list.count() > 20:
            self.history_list.takeItem(self.history_list.count() - 1)
    
    def clear_history(self) -> None:
        """Clear the history list."""
        self.history_list.clear()
    
    def close(self):
        """Stop the refresh timer when widget is closed."""
        if hasattr(self, '_refresh_timer'):
            self._refresh_timer.stop()
        super().close()
