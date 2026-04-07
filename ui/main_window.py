import sys
import os
import platform
import time
import copy
import logging
import contextlib
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from typing import Dict, Any, Optional, List
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QLabel, QListWidget, QListWidgetItem, QStackedWidget, QFormLayout,
    QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox, QCheckBox,
    QTextEdit, QGroupBox, QSplitter, QMessageBox, QFileDialog,
    QTabWidget, QTableWidget, QTableWidgetItem, QHeaderView,
    QAbstractItemView, QDialog, QDialogButtonBox, QProgressBar,
    QMenu, QAction, QToolBar, QStatusBar, QCheckBox
)
from PyQt5.QtCore import Qt, pyqtSignal, QTimer, QThread, QObject
from PyQt5.QtGui import QIcon, QColor, QFont, QCursor

from core.engine import FlowEngine
from core.base_action import ActionResult
from utils.hotkey import HotkeyManager
from utils.app_logger import app_logger
from utils.memory import memory_manager, image_cache
from utils.recording import RecordingManager, RecordingEditor, RecordedAction, PYNPUT_AVAILABLE
from utils.history import WorkflowHistoryManager, HistoryDialog, QuickSaveDialog
from utils.key_display import key_display_window
from utils.execution_stats import execution_stats
from ui.hotkey_dialog import HotkeySettingsDialog
from ui.region_selector import RegionSelector, PositionSelector
from ui.message import message_manager, show_error, show_success, show_warning, show_toast
from ui.stats_dialog import StatsDialog
from ui.theme import theme_manager, ThemeType

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def batch_updates(widget: QWidget):
    """Context manager to batch UI updates for performance.

    Disables updates before a batch of changes, then re-enables
    and performs a single repaint after.

    Args:
        widget: Widget to batch updates for.
    """
    widget.setUpdatesEnabled(False)
    try:
        yield
    finally:
        widget.setUpdatesEnabled(True)
        widget.update()

IS_MACOS = platform.system() == 'Darwin'
IS_WINDOWS = platform.system() == 'Windows'


class EngineSignals(QObject):
    step_start = pyqtSignal(dict)
    step_end = pyqtSignal(dict, object)
    workflow_end = pyqtSignal(bool)
    error = pyqtSignal(dict, str)


PARAM_DESCRIPTIONS = {
    'x': '屏幕X坐标（水平位置），点击"选取"按钮可在屏幕上点选',
    'y': '屏幕Y坐标（垂直位置），点击"选取"按钮可在屏幕上点选',
    'start_x': '拖拽起始点X坐标',
    'start_y': '拖拽起始点Y坐标',
    'end_x': '拖拽结束点X坐标',
    'end_y': '拖拽结束点Y坐标',
    'region': '识别区域，格式: x,y,宽度,高度。留空则全屏。 点击"框选"可拖拽选择',
    'text': '要输入的文字内容，支持中文和英文',
    'interval': '操作间隔时间，单位: 秒。如点击间隔、输入间隔',
    'duration': '持续时间，单位: 秒。如鼠标移动动画时长',
    'button': '鼠标按钮: left(左键)、right(右键)、middle(中键)',
    'direction': '滚轮方向: up(向上滚动)、down(向下滚动)',
    'key': '键盘按键，如: enter、escape、tab、f1-f12、a-z 等',
    'keys': '组合键，用 + 连接，如: ctrl+a、ctrl+c、alt+f4',
    'template': '模板图片路径，用于图像匹配点击',
    'confidence': '匹配置信度，0-1之间。越高越严格，默认0.8',
    'click_text': '【点击文字】OCR识别后点击包含此文字的区域',
    'click_index': '当有多个匹配时，点击第几个。0=第一个，1=第二个',
    'exact_match': '是否精确匹配。勾选=完全一致，不勾选=包含即可',
    'contains': '【检测文字】只检测是否存在，不执行点击操作',
    'move_duration': '鼠标移动动画时长，单位: 秒',
    'preprocess_mode': '图像预处理模式。auto=快速(推荐)、all=全部模式(慢但准确)、contrast=对比度、binary=二值化',
    'retry_count': 'OCR重试次数。auto模式仅1次，all模式可设置多次重试',
    'times': '重复执行次数',
    'count': '循环次数',
    'delay': '延时时间，单位: 秒',
    'clicks': '点击次数，默认1次。双击则填2',
    'relative': '是否相对坐标。勾选后x,y为相对于当前位置的偏移',
    'enter_after': '输入后是否按回车键。勾选则输入完成后自动按回车',
    'find_all': '是否查找所有匹配。勾选返回所有匹配位置',
    'script': 'Python脚本代码，可使用context变量',
    'command': '系统命令，如打开程序、执行批处理',
    'app_path': '应用程序完整路径，如: C:\\Program Files\\app.exe',
    'url': '网页地址，会自动用浏览器打开',
    'wait_time': '等待时间，单位: 秒',
    'var_name': '变量名，用于存储或读取变量',
    'value': '变量值',
    'filename': '文件名或路径',
    'content': '文件内容或文本内容',
    'seconds': '等待秒数',
    'milliseconds': '等待毫秒数',
}

PARAM_EXAMPLES = {
    'click_text': '示例: "确定"、"提交"、"取消" → 点击包含这些文字的区域',
    'contains': '示例: 检查页面是否有"成功"二字，不执行点击',
    'exact_match': '示例: 文字是"配置"，勾选后只匹配"配置"，不勾选则"未配置"也会匹配',
    'button': '示例: left=左键点击, right=右键点击, middle=中键点击',
    'direction': '示例: down=向下滚动, up=向上滚动',
    'key': '示例: enter=回车, escape=退出, f5=刷新, tab=制表符',
    'keys': '示例: ctrl+a=全选, ctrl+c=复制, alt+f4=关闭窗口',
    'preprocess_mode': '推荐: auto (快速，平衡速度和识别率)',
    'template': '示例: D:\\images\\button.png → 匹配并点击这个图片',
    'confidence': '示例: 0.9=高精度匹配, 0.7=宽松匹配',
    'region': '示例: 100,200,300,400 → 从坐标(100,200)开始，宽300高400的区域',
    'relative': '示例: 勾选后 x=10,y=20 表示从当前位置向右10像素、向下20像素',
    'enter_after': '示例: 勾选后输入文字会自动按回车提交',
}


class ActionConfigWidget(QWidget):
    config_changed = pyqtSignal()

    def __init__(self, action_info: dict, parent=None):
        super().__init__(parent)
        self.action_info = action_info
        self.widgets = {}
        self._colors = theme_manager.colors
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

                desc = PARAM_DESCRIPTIONS.get(param, '')
                example = PARAM_EXAMPLES.get(param, '')
                if desc or example:
                    help_text = desc
                    if example:
                        help_text += f"\n{example}"
                    help_label = QLabel(help_text)
                    help_label.setStyleSheet(f"color: {self._colors['text_secondary']}; font-size: 10px;")
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

                desc = PARAM_DESCRIPTIONS.get(param, '')
                example = PARAM_EXAMPLES.get(param, '')
                if desc or example:
                    help_text = desc
                    if example:
                        help_text += f"\n{example}"
                    help_label = QLabel(help_text)
                    help_label.setStyleSheet(f"color: {self._colors['text_secondary']}; font-size: 10px;")
                    help_label.setWordWrap(True)
                    row_layout.addWidget(help_label)

                optional_layout.addRow(f"{param}:", row_widget)

            optional_group.setLayout(optional_layout)
            layout.addWidget(optional_group)

        common_group = QGroupBox("通用设置")
        common_layout = QFormLayout()
        common_layout.setSpacing(5)

        self.pre_delay = QDoubleSpinBox()
        self.pre_delay.setRange(0, 60)
        self.pre_delay.setValue(0)
        self.pre_delay.setSingleStep(0.1)
        common_layout.addRow("前置延时(秒):", self.pre_delay)

        self.post_delay = QDoubleSpinBox()
        self.post_delay.setRange(0, 60)
        self.post_delay.setValue(0)
        self.post_delay.setSingleStep(0.1)
        common_layout.addRow("后置延时(秒):", self.post_delay)

        self.output_var = QLineEdit()
        self.output_var.setPlaceholderText("可选，保存结果的变量名")
        common_layout.addRow("输出变量:", self.output_var)

        common_group.setLayout(common_layout)
        layout.addWidget(common_group)

        layout.addStretch()
    
    def _create_param_widget(self, param: str, default_value: Any) -> QWidget:
        if param in ('x', 'y', 'start_x', 'start_y', 'end_x', 'end_y'):
            container = QWidget()
            h_layout = QHBoxLayout(container)
            h_layout.setContentsMargins(0, 0, 0, 0)
            
            spin = QSpinBox()
            spin.setRange(0, 9999)
            spin.setValue(int(default_value) if default_value else 0)
            spin.valueChanged.connect(self.config_changed.emit)
            
            pick_btn = QPushButton("选取")
            pick_btn.setMaximumWidth(50)
            pick_btn.clicked.connect(lambda: self._pick_position(param, spin))
            
            h_layout.addWidget(spin)
            h_layout.addWidget(pick_btn)
            
            self.widgets[f"{param}_spin"] = spin
            return container
        
        if param in ('region',):
            container = QWidget()
            h_layout = QHBoxLayout(container)
            h_layout.setContentsMargins(0, 0, 0, 0)
            
            line_edit = QLineEdit()
            line_edit.setPlaceholderText("x,y,w,h")
            if default_value:
                line_edit.setText(str(default_value))
            line_edit.textChanged.connect(self.config_changed.emit)
            
            pick_btn = QPushButton("框选")
            pick_btn.setMaximumWidth(50)
            pick_btn.clicked.connect(lambda: self._pick_region(line_edit))
            
            h_layout.addWidget(line_edit)
            h_layout.addWidget(pick_btn)
            
            self.widgets[f"{param}_edit"] = line_edit
            return container
        
        if isinstance(default_value, bool):
            widget = QCheckBox()
            widget.setChecked(default_value)
            widget.stateChanged.connect(self.config_changed.emit)
            return widget
        elif param == 'button':
            widget = QComboBox()
            widget.addItem("左键", "left")
            widget.addItem("右键", "right")
            widget.addItem("中键", "middle")
            idx = widget.findData(default_value) if default_value else 0
            widget.setCurrentIndex(idx if idx >= 0 else 0)
            widget.currentIndexChanged.connect(self.config_changed.emit)
            return widget
        elif param == 'direction':
            widget = QComboBox()
            widget.addItem("向下滚动", "down")
            widget.addItem("向上滚动", "up")
            idx = widget.findData(default_value) if default_value else 0
            widget.setCurrentIndex(idx if idx >= 0 else 0)
            widget.currentIndexChanged.connect(self.config_changed.emit)
            return widget
        elif param == 'preprocess_mode':
            widget = QComboBox()
            widget.addItem("自动多模式 (推荐)", "auto")
            widget.addItem("全部预处理", "all")
            widget.addItem("对比度增强", "contrast")
            widget.addItem("二值化处理", "binary")
            widget.addItem("降噪处理", "denoise")
            widget.addItem("原始图像", "none")
            idx = widget.findData(default_value) if default_value else 0
            widget.setCurrentIndex(idx if idx >= 0 else 0)
            widget.currentIndexChanged.connect(self.config_changed.emit)
            return widget
        elif param == 'key' and isinstance(default_value, str):
            widget = QComboBox()
            widget.setEditable(True)
            common_keys = ['enter', 'escape', 'tab', 'space', 'backspace', 'delete', 
                          'f1', 'f2', 'f3', 'f4', 'f5', 'f6', 'f7', 'f8', 'f9', 'f10', 'f11', 'f12',
                          'up', 'down', 'left', 'right', 'home', 'end', 'pageup', 'pagedown']
            for key in common_keys:
                widget.addItem(key, key)
            if default_value:
                widget.setCurrentText(str(default_value))
            widget.currentTextChanged.connect(self.config_changed.emit)
            return widget
        elif isinstance(default_value, int):
            widget = QSpinBox()
            widget.setRange(-99999, 99999)
            widget.setValue(default_value)
            widget.valueChanged.connect(self.config_changed.emit)
            return widget
        elif isinstance(default_value, float):
            widget = QDoubleSpinBox()
            widget.setRange(-99999, 99999)
            widget.setValue(default_value)
            widget.setSingleStep(0.1)
            widget.valueChanged.connect(self.config_changed.emit)
            return widget
        elif isinstance(default_value, list):
            widget = QLineEdit()
            widget.setText(str(default_value))
            widget.setPlaceholderText("JSON数组格式")
            widget.textChanged.connect(self.config_changed.emit)
            return widget
        elif isinstance(default_value, tuple):
            widget = QLineEdit()
            widget.setText(str(default_value))
            widget.setPlaceholderText("JSON数组格式")
            widget.textChanged.connect(self.config_changed.emit)
            return widget
        else:
            widget = QLineEdit()
            if default_value is not None:
                widget.setText(str(default_value))
            widget.textChanged.connect(self.config_changed.emit)
            return widget
    
    def _pick_position(self, param: str, spin_widget: QSpinBox):
        self.window().hide()
        QTimer.singleShot(200, lambda: self._do_pick_position(param, spin_widget))
    
    def _do_pick_position(self, param: str, spin_widget: QSpinBox):
        selector = PositionSelector()
        
        def on_selected(x, y):
            spin_widget.setValue(x if 'x' in param else y)
            self.window().show()
            show_toast(f"已选择位置: ({x}, {y})", 'success')
        
        def on_cancelled():
            self.window().show()
        
        selector.position_selected.connect(on_selected)
        selector.cancelled.connect(on_cancelled)
    
    def _pick_region(self, line_edit: QLineEdit):
        self.window().hide()
        QTimer.singleShot(200, lambda: self._do_pick_region(line_edit))
    
    def _do_pick_region(self, line_edit: QLineEdit):
        selector = RegionSelector(mode='region')
        
        def on_selected(x, y, w, h):
            line_edit.setText(f"{x},{y},{w},{h}")
            self.window().show()
            show_toast(f"已选择区域: ({x}, {y}, {w}, {h})", 'success')
        
        def on_cancelled():
            self.window().show()
        
        selector.region_selected.connect(on_selected)
        selector.cancelled.connect(on_cancelled)
    
    def get_config(self) -> Dict[str, Any]:
        config = {}
        
        for param, widget in self.widgets.items():
            if param.endswith('_spin') or param.endswith('_edit'):
                continue
            
            if isinstance(widget, QWidget) and widget.__class__.__name__ == 'QWidget':
                spin = self.widgets.get(f"{param}_spin")
                if spin:
                    config[param] = spin.value()
                continue
            
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
        
        config['pre_delay'] = self.pre_delay.value()
        config['post_delay'] = self.post_delay.value()
        
        output_var = self.output_var.text().strip()
        if output_var:
            config['output_var'] = output_var
        
        return config
    
    def set_config(self, config: Dict[str, Any]) -> None:
        for param, widget in self.widgets.items():
            if param.endswith('_spin') or param.endswith('_edit'):
                continue
            
            if param in config:
                value = config[param]
                
                spin = self.widgets.get(f"{param}_spin")
                if spin and isinstance(value, (int, float)):
                    spin.setValue(int(value))
                    continue
                
                edit = self.widgets.get(f"{param}_edit")
                if edit and isinstance(value, (list, tuple)):
                    edit.setText(','.join(map(str, value)) if isinstance(value, list) else str(value))
                    continue
                
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
        
        if 'pre_delay' in config:
            self.pre_delay.setValue(config['pre_delay'])
        if 'post_delay' in config:
            self.post_delay.setValue(config['post_delay'])
        if 'output_var' in config:
            self.output_var.setText(config['output_var'])


class StepListWidget(QWidget):
    step_selected = pyqtSignal(int)
    step_moved = pyqtSignal(int, int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.list_widget = QListWidget()
        self.list_widget.setDragDropMode(QAbstractItemView.InternalMove)
        self.list_widget.setContextMenuPolicy(Qt.CustomContextMenu)
        self.list_widget.customContextMenuRequested.connect(self._show_context_menu)
        self.list_widget.currentRowChanged.connect(self.step_selected.emit)
        layout.addWidget(self.list_widget)

        btn_layout = QHBoxLayout()

        self.add_btn = QPushButton("添加步骤")
        self.add_btn.setStyleSheet(theme_manager.get_button_stylesheet('success'))
        self.remove_btn = QPushButton("删除")
        self.remove_btn.setStyleSheet(theme_manager.get_button_stylesheet('danger'))
        self.up_btn = QPushButton("↑")
        self.up_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))
        self.down_btn = QPushButton("↓")
        self.down_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))

        self.up_btn.setMaximumWidth(40)
        self.down_btn.setMaximumWidth(40)

        btn_layout.addWidget(self.add_btn)
        btn_layout.addWidget(self.remove_btn)
        btn_layout.addWidget(self.up_btn)
        btn_layout.addWidget(self.down_btn)

        layout.addLayout(btn_layout)

    def _show_context_menu(self, position):
        item = self.list_widget.itemAt(position)
        if item:
            menu = QMenu()
            duplicate_action = menu.addAction("复制此步骤")
            delete_action = menu.addAction("删除此步骤")

            action = menu.exec_(self.list_widget.mapToGlobal(position))
            if action == duplicate_action:
                data = item.data(Qt.UserRole)
                self.add_step(data['id'] + 1000, data['type'], item.text().split('] ')[1] if '] ' in item.text() else data['type'])
            elif action == delete_action:
                self.remove_step(self.list_widget.row(item))

    def add_step(self, step_id: int, action_type: str, display_name: str):
        item = QListWidgetItem(f"[{step_id}] {display_name}")
        item.setData(Qt.UserRole, {'id': step_id, 'type': action_type})
        self.list_widget.addItem(item)

    def add_steps_batch(self, steps: list):
        """Add multiple steps with batch updates for performance.

        Args:
            steps: List of tuples (step_id, action_type, display_name).
        """
        with batch_updates(self.list_widget):
            for step_id, action_type, display_name in steps:
                item = QListWidgetItem(f"[{step_id}] {display_name}")
                item.setData(Qt.UserRole, {'id': step_id, 'type': action_type})
                self.list_widget.addItem(item)

    def get_current_index(self) -> int:
        return self.list_widget.currentRow()

    def set_current_index(self, index: int):
        self.list_widget.setCurrentRow(index)

    def remove_step(self, index: int):
        self.list_widget.takeItem(index)

    def clear(self):
        with batch_updates(self.list_widget):
            self.list_widget.clear()

    def get_step_count(self) -> int:
        return self.list_widget.count()


class VariablesWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setColumnCount(2)
        self.table.setHorizontalHeaderLabels(["变量名", "值"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table)

        btn_layout = QHBoxLayout()
        self.add_btn = QPushButton("添加变量")
        self.add_btn.setStyleSheet(theme_manager.get_button_stylesheet('success'))
        self.remove_btn = QPushButton("删除变量")
        self.remove_btn.setStyleSheet(theme_manager.get_button_stylesheet('danger'))
        btn_layout.addWidget(self.add_btn)
        btn_layout.addWidget(self.remove_btn)
        layout.addLayout(btn_layout)

        self.add_btn.clicked.connect(self._add_variable)
        self.remove_btn.clicked.connect(self._remove_variable)
    
    def _add_variable(self):
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.table.setItem(row, 0, QTableWidgetItem(""))
        self.table.setItem(row, 1, QTableWidgetItem(""))
    
    def _remove_variable(self):
        current_row = self.table.currentRow()
        if current_row >= 0:
            self.table.removeRow(current_row)
    
    def get_variables(self) -> Dict[str, Any]:
        variables = {}
        for row in range(self.table.rowCount()):
            name_item = self.table.item(row, 0)
            value_item = self.table.item(row, 1)
            if name_item and value_item:
                name = name_item.text().strip()
                value = value_item.text()
                if name:
                    try:
                        value = json.loads(value)
                    except Exception as e:
                        import logging
                        logging.getLogger('RabAI').warning(f'解析变量JSON失败: {e}')
                    variables[name] = value
        return variables
    
    def set_variables(self, variables: Dict[str, Any]) -> None:
        with batch_updates(self.table):
            self.table.setRowCount(0)
            for name, value in variables.items():
                row = self.table.rowCount()
                self.table.insertRow(row)
                self.table.setItem(row, 0, QTableWidgetItem(name))
                self.table.setItem(row, 1, QTableWidgetItem(json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)))


class LogWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._log_colors = theme_manager.get_log_colors()
        theme_manager.theme_changed.connect(self._on_theme_changed)
        self._init_ui()

    def _on_theme_changed(self, theme) -> None:
        """Handle theme change to update log colors."""
        self._log_colors = theme_manager.get_log_colors()
        self._apply_log_style()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)
        self._apply_log_style()
        layout.addWidget(self.text_edit)

        btn_layout = QHBoxLayout()
        clear_btn = QPushButton("清空日志")
        clear_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))
        export_btn = QPushButton("导出日志")
        export_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))
        btn_layout.addWidget(clear_btn)
        btn_layout.addWidget(export_btn)
        layout.addLayout(btn_layout)

        clear_btn.clicked.connect(self.clear)
        export_btn.clicked.connect(self._export_log)

        app_logger.add_listener(self._on_log_entry)

    def _apply_log_style(self):
        """Apply log widget style based on current theme."""
        colors = theme_manager.colors
        self.text_edit.setStyleSheet(f"""
            QTextEdit {{
                background-color: {colors['status_bar']};
                color: {colors['status_text']};
                font-family: Consolas, 'Microsoft YaHei', monospace;
                font-size: 12px;
                border: none;
                padding: 4px;
            }}
        """)

    def _on_log_entry(self, entry):
        color = self._log_colors.get(entry.level, theme_manager.get_color('text_secondary'))
        timestamp = entry.timestamp.strftime("%H:%M:%S")
        timestamp_color = theme_manager.get_color('text_secondary')
        html = f'<span style="color: {timestamp_color};">[{timestamp}]</span> <span style="color: {color};">[{entry.level}]</span> {entry.message}'
        self.text_edit.append(html)
    
    def append_log(self, message: str, level: str = "INFO"):
        app_logger.info(message) if level == "INFO" else \
        app_logger.warning(message) if level == "WARN" else \
        app_logger.error(message) if level == "ERROR" else \
        app_logger.success(message) if level == "SUCCESS" else \
        app_logger.debug(message)
    
    def _export_log(self):
        filepath, _ = QFileDialog.getSaveFileName(self, "导出日志", "", "JSON文件 (*.json)")
        if filepath:
            if app_logger.export_to_file(filepath):
                show_toast("日志导出成功", 'success')
            else:
                show_error("导出失败", "无法导出日志文件")
    
    def clear(self):
        self.text_edit.clear()
        app_logger.clear()


class RecordingWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._recording_manager = RecordingManager()
        self._init_ui()
        self._connect_signals()

    def _init_ui(self):
        layout = QVBoxLayout(self)

        info_label = QLabel("录屏功能：录制鼠标和键盘操作，自动生成工作流步骤")
        colors = theme_manager.colors
        info_label.setStyleSheet(f"color: {colors['text_secondary']}; font-size: 11px;")
        layout.addWidget(info_label)

        btn_layout = QHBoxLayout()

        self.record_btn = QPushButton("🔴 开始录制")
        self.record_btn.setStyleSheet(theme_manager.get_button_stylesheet('danger'))
        self.stop_btn = QPushButton("⏹ 停止录制")
        self.stop_btn.setEnabled(False)
        self.stop_btn.setStyleSheet(theme_manager.get_button_stylesheet('danger'))
        self.clear_btn = QPushButton("清空")
        self.clear_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))
        self.optimize_btn = QPushButton("优化")
        self.optimize_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))

        btn_layout.addWidget(self.record_btn)
        btn_layout.addWidget(self.stop_btn)
        btn_layout.addWidget(self.clear_btn)
        btn_layout.addWidget(self.optimize_btn)
        layout.addLayout(btn_layout)

        self.action_list = QListWidget()
        layout.addWidget(self.action_list)

        self.status_label = QLabel("状态: 就绪")
        layout.addWidget(self.status_label)

        if not PYNPUT_AVAILABLE:
            self.record_btn.setEnabled(False)
            self.status_label.setText("状态: pynput未安装，录屏功能不可用")
            self.status_label.setStyleSheet(f"color: {colors['error']};")

    def _connect_signals(self):
        self.record_btn.clicked.connect(self._on_record)
        self.stop_btn.clicked.connect(self._on_stop)
        self.clear_btn.clicked.connect(self._on_clear)
        self.optimize_btn.clicked.connect(self._on_optimize)

        self._recording_manager.action_recorded.connect(self._on_action_recorded)
        self._recording_manager.recording_started.connect(self._on_recording_started)
        self._recording_manager.recording_stopped.connect(self._on_recording_stopped)

    def _on_record(self):
        if self._recording_manager.start_recording():
            self.record_btn.setEnabled(False)
            self.stop_btn.setEnabled(True)
            self.status_label.setText("状态: 录制中...")
            show_toast("开始录制操作", 'info')
        else:
            show_error("录制失败", "无法启动录制")

    def _on_stop(self) -> None:
        actions = self._recording_manager.stop_recording()
        self.record_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.status_label.setText(f"状态: 已录制 {len(actions)} 个操作")
        show_toast(f"录制完成，共 {len(actions)} 个操作", 'success')

    def _on_clear(self) -> None:
        self._recording_manager.clear_actions()
        self.action_list.clear()
        self.status_label.setText("状态: 就绪")

    def _on_optimize(self):
        actions = self._recording_manager.get_actions()
        if not actions:
            show_warning("提示", "没有可优化的操作")
            return

        editor = RecordingEditor(actions)
        merged = editor.merge_consecutive_types()
        optimized = editor.optimize_delays()

        self._recording_manager._actions = editor.get_actions()
        self._refresh_action_list()

        show_toast(f"优化完成：合并 {merged} 个，优化 {optimized} 个延时", 'success')

    def _on_action_recorded(self, action_type: str, params: dict):
        self.action_list.addItem(f"{action_type}: {params}")

    def _on_recording_started(self):
        self.action_list.clear()

    def _on_recording_stopped(self, actions):
        pass

    def _refresh_action_list(self):
        with batch_updates(self.action_list):
            self.action_list.clear()
            for action in self._recording_manager.get_actions():
                self.action_list.addItem(f"{action.action_type}: {action.params}")
    
    def get_workflow(self) -> Dict[str, Any]:
        return self._recording_manager.to_workflow()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("RabAI AutoClick - 桌面自动化工具 v1.2")
        self.setGeometry(100, 100, 1300, 850)
        
        self.engine = FlowEngine()
        self.current_workflow = {'variables': {}, 'steps': []}
        self.next_step_id = 1
        self.step_configs = {}
        
        self.config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json')
        self.hotkey_manager = HotkeyManager()
        self.hotkey_manager.set_config_path(self.config_path)
        self.current_hotkeys = self.hotkey_manager.load_config()
        
        self.history_manager = WorkflowHistoryManager()
        
        self._always_on_top = False
        self._teaching_mode = False
        self._target_region = None
        self._window_mode = False
        self._loop_count = 1
        self._loop_interval = 1.0
        self._current_loop = 0
        self._is_looping = False
        
        message_manager.set_parent(self)
        
        self._init_ui()
        self._setup_hotkeys()
        self._connect_signals()
        self._update_button_texts()
        
        app_logger.info("程序启动", "Main")
    
    def _init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(5)

        toolbar = QHBoxLayout()

        self.new_btn = QPushButton("📄 新建")
        self.new_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))
        self.open_btn = QPushButton("📂 打开")
        self.open_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))
        self.save_btn = QPushButton("💾 保存")
        self.save_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))
        self.history_btn = QPushButton("📚 记录")
        self.history_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))

        colors = theme_manager.colors
        self.run_btn = QPushButton("▶ 运行")
        self.run_btn.setStyleSheet(theme_manager.get_button_stylesheet('success'))
        self.stop_btn = QPushButton("⏹ 停止")
        self.stop_btn.setStyleSheet(theme_manager.get_button_stylesheet('danger'))
        self.pause_btn = QPushButton("⏸ 暂停")
        self.pause_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))

        self.on_top_btn = QPushButton("📌 置顶")
        self.on_top_btn.setCheckable(True)
        self.teaching_btn = QPushButton("🎓 教学")
        self.teaching_btn.setCheckable(True)
        self.hotkey_btn = QPushButton("⌨ 快捷键")
        self.window_btn = QPushButton("🪟 窗口")
        self.region_btn = QPushButton("📐 区域")
        self.loop_btn = QPushButton("🔄 循环")
        self.stats_btn = QPushButton("📊 统计")
        self.memory_btn = QPushButton("💾 内存")
        self.theme_btn = QPushButton("🌙 深色")
        self.theme_btn.setCheckable(True)
        self.theme_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))

        self.stop_btn.setEnabled(False)
        self.pause_btn.setEnabled(False)
        
        toolbar.addWidget(self.new_btn)
        toolbar.addWidget(self.open_btn)
        toolbar.addWidget(self.save_btn)
        toolbar.addWidget(self.history_btn)
        toolbar.addSpacing(20)
        toolbar.addWidget(self.run_btn)
        toolbar.addWidget(self.stop_btn)
        toolbar.addWidget(self.pause_btn)
        toolbar.addSpacing(20)
        toolbar.addWidget(self.on_top_btn)
        toolbar.addWidget(self.teaching_btn)
        toolbar.addWidget(self.hotkey_btn)
        toolbar.addWidget(self.window_btn)
        toolbar.addWidget(self.region_btn)
        toolbar.addWidget(self.loop_btn)
        toolbar.addWidget(self.stats_btn)
        toolbar.addWidget(self.memory_btn)
        toolbar.addWidget(self.theme_btn)
        toolbar.addStretch()
        
        self.memory_label = QLabel()
        toolbar.addWidget(self.memory_label)
        
        main_layout.addLayout(toolbar)
        
        self.splitter = QSplitter(Qt.Horizontal)
        
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        self.step_list = StepListWidget()
        left_layout.addWidget(QLabel("步骤列表:"))
        left_layout.addWidget(self.step_list)
        
        self.splitter.addWidget(left_panel)

        right_panel = QTabWidget()

        editor_widget = QWidget()
        editor_layout = QVBoxLayout(editor_widget)

        self.action_list = QListWidget()
        self.action_list.setMaximumHeight(120)
        editor_layout.addWidget(QLabel("选择动作类型:"))
        editor_layout.addWidget(self.action_list)

        self.config_stack = QStackedWidget()
        editor_layout.addWidget(QLabel("动作配置:"))
        editor_layout.addWidget(self.config_stack)

        right_panel.addTab(editor_widget, "步骤编辑")

        self.variables_widget = VariablesWidget()
        right_panel.addTab(self.variables_widget, "变量")

        self.recording_widget = RecordingWidget()
        right_panel.addTab(self.recording_widget, "录屏")

        self.log_widget = LogWidget()
        right_panel.addTab(self.log_widget, "日志")

        self.splitter.addWidget(right_panel)
        self.splitter.setSizes([350, 950])

        main_layout.addWidget(self.splitter)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        
        self.statusBar().showMessage("就绪 | F6运行 | F7停止")
        
        self._load_actions()
        
        self._memory_timer = QTimer()
        self._memory_timer.timeout.connect(self._update_memory_display)
        self._memory_timer.start(5000)
    
    def _update_memory_display(self):
        try:
            mem = memory_manager.get_memory_usage()
            self.memory_label.setText(f"内存: {mem['rss']} MB")
        except Exception as e:
            import logging
            logging.getLogger('RabAI').warning(f'内存显示更新失败: {e}')
            self.memory_label.setText("")
    
    def _load_actions(self):
        action_info = self.engine.get_action_info()

        with batch_updates(self.action_list):
            for action_type, info in action_info.items():
                item = QListWidgetItem(f"{info['display_name']}")
                item.setData(Qt.UserRole, action_type)
                item.setToolTip(f"{info['description']}\n类型: {action_type}")
                self.action_list.addItem(item)

        # Use lazy loading for config widgets to improve startup time
        self._action_info = action_info
        self.config_widgets = {}
        # Config widgets will be created on-demand when selected
        # Signal connections will be made when widgets are created

    def _ensure_config_widget(self, action_type: str) -> Optional[ActionConfigWidget]:
        """Lazily create and cache a config widget for an action type.

        Args:
            action_type: The action type to get/create widget for.

        Returns:
            The ActionConfigWidget for the action type.
        """
        if action_type not in self.config_widgets:
            info = self._action_info.get(action_type)
            if info:
                with batch_updates(self.config_stack):
                    widget = ActionConfigWidget(info)
                    self.config_widgets[action_type] = widget
                    self.config_stack.addWidget(widget)
                    # Connect config_changed signal for lazy-loaded widget
                    widget.config_changed.connect(self._on_config_changed)
        return self.config_widgets.get(action_type)
    
    def _setup_hotkeys(self):
        if self.hotkey_manager.is_available():
            self.hotkey_manager.register_hotkeys(
                start_key=self.current_hotkeys.get('start', 'f6'),
                stop_key=self.current_hotkeys.get('stop', 'f7'),
                pause_key=self.current_hotkeys.get('pause', 'f8')
            )
            self.hotkey_manager.start_triggered.connect(self._on_run)
            self.hotkey_manager.stop_triggered.connect(self._on_stop)
            self.hotkey_manager.pause_triggered.connect(self._on_pause)
            app_logger.info(f"快捷键已启用: 运行={self.current_hotkeys.get('start', 'f6').upper()}, "
                           f"停止={self.current_hotkeys.get('stop', 'f7').upper()}", "Hotkey")
        else:
            app_logger.warning("keyboard模块未安装，全局热键不可用", "Hotkey")
    
    def _update_button_texts(self):
        start_key = self.current_hotkeys.get('start', 'f6').upper()
        stop_key = self.current_hotkeys.get('stop', 'f7').upper()
        pause_key = self.current_hotkeys.get('pause', 'f8').upper()
        
        self.run_btn.setText(f"▶ 运行 ({start_key})")
        self.stop_btn.setText(f"⏹ 停止 ({stop_key})")
        self.pause_btn.setText(f"⏸ 暂停 ({pause_key})")
    
    def _connect_signals(self):
        self.new_btn.clicked.connect(self._on_new)
        self.open_btn.clicked.connect(self._on_open)
        self.save_btn.clicked.connect(self._on_save)
        self.history_btn.clicked.connect(self._on_history)
        self.run_btn.clicked.connect(self._on_run)
        self.stop_btn.clicked.connect(self._on_stop)
        self.pause_btn.clicked.connect(self._on_pause)
        self.on_top_btn.clicked.connect(self._toggle_always_on_top)
        self.teaching_btn.clicked.connect(self._toggle_teaching_mode)
        self.hotkey_btn.clicked.connect(self._on_hotkey_settings)
        self.window_btn.clicked.connect(self._on_select_window)
        self.region_btn.clicked.connect(self._on_select_region)
        self.loop_btn.clicked.connect(self._on_loop_settings)
        self.stats_btn.clicked.connect(self._on_show_stats)
        self.memory_btn.clicked.connect(self._on_memory_optimize)
        self.theme_btn.clicked.connect(self._toggle_theme)

        self.action_list.currentRowChanged.connect(self._on_action_selected)
        
        self.step_list.add_btn.clicked.connect(self._on_add_step)
        self.step_list.remove_btn.clicked.connect(self._on_remove_step)
        self.step_list.up_btn.clicked.connect(self._on_move_up)
        self.step_list.down_btn.clicked.connect(self._on_move_down)
        self.step_list.step_selected.connect(self._on_step_selected)

        # Note: config_widget signals are connected lazily when widgets are created
        # This improves startup time by deferring widget creation

        self.engine_signals = EngineSignals()
        # Use QueuedConnection for thread-safe signal-slot communication
        self.engine_signals.step_start.connect(
            self._on_engine_step_start, type=Qt.QueuedConnection
        )
        self.engine_signals.step_end.connect(
            self._on_engine_step_end, type=Qt.QueuedConnection
        )
        self.engine_signals.workflow_end.connect(
            self._on_engine_workflow_end, type=Qt.QueuedConnection
        )
        self.engine_signals.error.connect(
            self._on_engine_error, type=Qt.QueuedConnection
        )

        self.engine.set_callbacks(
            on_step_start=lambda step: self.engine_signals.step_start.emit(step),
            on_step_end=lambda step, result: self.engine_signals.step_end.emit(step, result),
            on_workflow_end=lambda success: self.engine_signals.workflow_end.emit(success),
            on_error=lambda step, msg: self.engine_signals.error.emit(step, msg)
        )

    def _on_new(self) -> None:
        if self.step_list.get_step_count() > 0:
            if not show_question("确认", "是否新建工作流？当前未保存的内容将丢失。"):
                return
        
        self.current_workflow = {'variables': {}, 'steps': []}
        self.step_configs = {}
        self.next_step_id = 1
        self.step_list.clear()
        self.variables_widget.set_variables({})
        self.log_widget.clear()
        app_logger.info("新建工作流", "Workflow")
        show_toast("新建工作流", 'info')

    def _on_open(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self, "打开工作流", "", "JSON文件 (*.json)"
        )
        if file_path:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    self.current_workflow = json.load(f)
                
                self.step_list.clear()
                self.step_configs = {}
                self.next_step_id = 1
                
                for step in self.current_workflow.get('steps', []):
                    step_id = step.get('id', self.next_step_id)
                    action_type = step.get('type', '')
                    
                    action_info = self.engine.get_action_info().get(action_type, {})
                    display_name = action_info.get('display_name', action_type)
                    
                    self.step_list.add_step(step_id, action_type, display_name)
                    self.step_configs[step_id] = step.copy()
                    
                    self.next_step_id = max(self.next_step_id, step_id + 1)
                
                self.variables_widget.set_variables(self.current_workflow.get('variables', {}))
                app_logger.info(f"打开工作流: {file_path}", "Workflow")
                show_toast(f"已打开: {os.path.basename(file_path)}", 'success')
            except Exception as e:
                show_error("打开失败", f"无法打开文件: {str(e)}")

    def _on_save(self) -> None:
        menu = QMenu(self)
        save_file_action = menu.addAction("💾 保存到文件...")
        quick_save_action = menu.addAction("⚡ 快速保存到记录")
        
        action = menu.exec_(QCursor.pos())
        
        if action == save_file_action:
            self._save_to_file()
        elif action == quick_save_action:
            self._quick_save()
    
    def _save_to_file(self):
        file_path, _ = QFileDialog.getSaveFileName(
            self, "保存工作流", "", "JSON文件 (*.json)"
        )
        if file_path:
            try:
                self._build_workflow()
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(self.current_workflow, f, ensure_ascii=False, indent=2)
                app_logger.info(f"保存工作流: {file_path}", "Workflow")
                show_toast(f"已保存: {os.path.basename(file_path)}", 'success')
            except Exception as e:
                show_error("保存失败", f"无法保存文件: {str(e)}")
    
    def _quick_save(self):
        self._build_workflow()
        
        if not self.current_workflow.get('steps'):
            show_warning("提示", "工作流中没有步骤")
            return
        
        dialog = QuickSaveDialog(self)
        if dialog.exec_() == QDialog.Accepted:
            name, tags = dialog.get_data()
            filepath = self.history_manager.save_workflow(name, self.current_workflow, tags)
            app_logger.info(f"快速保存工作流: {name}", "Workflow")
            show_toast(f"已保存: {name}", 'success')
    
    def _build_workflow(self):
        steps = []
        for i in range(self.step_list.get_step_count()):
            item = self.step_list.list_widget.item(i)
            data = item.data(Qt.UserRole)
            step_id = data['id']
            
            if step_id in self.step_configs:
                step = self.step_configs[step_id].copy()
                step['id'] = step_id
                steps.append(step)
        
        self.current_workflow['steps'] = steps
        self.current_workflow['variables'] = self.variables_widget.get_variables()
    
    def _on_run(self):
        if self.engine.is_running():
            return
        
        self._build_workflow()
        
        if not self.current_workflow.get('steps'):
            show_warning("提示", "工作流中没有步骤")
            return
        
        if self._target_region:
            for step in self.current_workflow.get('steps', []):
                if step.get('type') == 'ocr' and not step.get('region'):
                    step['region'] = self._target_region
            app_logger.info(f"使用识别区域: {self._target_region}", "Workflow")
        
        self._current_loop = 0
        self._is_looping = True
        
        workflow_name = "未命名工作流"
        execution_stats.start_session(workflow_name, self._loop_count)
        
        self._run_single_loop()
    
    def _run_single_loop(self):
        if not self._is_looping:
            return
        
        if self._current_loop >= self._loop_count:
            return
        
        self._current_loop += 1
        self._loop_start_time = time.time()
        
        app_logger.info(f"执行第 {self._current_loop}/{self._loop_count} 次循环", "Workflow")
        
        self.run_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.pause_btn.setEnabled(True)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, len(self.current_workflow['steps']))
        self.progress_bar.setValue(0)
        self.progress_bar.setStyleSheet(theme_manager.get_stylesheet("progress_animated"))
        
        if self._current_loop == 1:
            self.showMinimized()
        
        self.engine.load_workflow_from_dict(self.current_workflow)
        self.engine.run_async()
    
    def _on_stop(self):
        self._is_looping = False
        self._current_loop = 0
        self.engine.stop()
        self.run_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.pause_btn.setEnabled(False)
        self.pause_btn.setText("⏸ 暂停")
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("")
        self.showNormal()
        self.activateWindow()
        app_logger.warning("停止工作流", "Workflow")
        show_toast("工作流已停止", 'warning')
    
    def _on_pause(self):
        if self.engine.is_paused():
            self.engine.resume()
            self.pause_btn.setText("⏸ 暂停")
            app_logger.info("继续运行", "Workflow")
        else:
            self.engine.pause()
            self.pause_btn.setText("▶ 继续")
            app_logger.info("已暂停", "Workflow")
    
    def _toggle_always_on_top(self):
        self._always_on_top = not self._always_on_top
        colors = theme_manager.colors

        if self._always_on_top:
            self.setWindowFlags(self.windowFlags() | Qt.WindowStaysOnTopHint)
            self.on_top_btn.setChecked(True)
            self.on_top_btn.setStyleSheet(f"background-color: {colors['primary']}; color: white;")
            show_toast("窗口已置顶", 'info')
        else:
            self.setWindowFlags(self.windowFlags() & ~Qt.WindowStaysOnTopHint)
            self.on_top_btn.setChecked(False)
            self.on_top_btn.setStyleSheet("")
            show_toast("窗口取消置顶", 'info')

        self.show()
        app_logger.info(f"窗口置顶: {self._always_on_top}", "UI")

    def _toggle_teaching_mode(self):
        self._teaching_mode = key_display_window.toggle()
        colors = theme_manager.colors

        if self._teaching_mode:
            self.teaching_btn.setChecked(True)
            self.teaching_btn.setStyleSheet(f"background-color: {colors['warning']}; color: white;")
            show_toast("教学模式已开启 - 按键和鼠标点击将显示在屏幕上", 'success')
        else:
            self.teaching_btn.setChecked(False)
            self.teaching_btn.setStyleSheet("")
            show_toast("教学模式已关闭", 'info')
        
        app_logger.info(f"教学模式: {self._teaching_mode}", "UI")
    
    def _on_history(self):
        dialog = HistoryDialog(self.history_manager, self)
        dialog.workflow_selected.connect(self._load_workflow_from_history)
        dialog.exec_()
    
    def _load_workflow_from_history(self, workflow: Dict[str, Any]):
        self.current_workflow = workflow
        self.step_configs = {}
        self.next_step_id = 1

        with batch_updates(self.step_list):
            self.step_list.clear()

            for step in self.current_workflow.get('steps', []):
                step_id = step.get('id', self.next_step_id)
                action_type = step.get('type', '')

                action_info = self.engine.get_action_info().get(action_type, {})
                display_name = action_info.get('display_name', action_type)

                self.step_list.add_step(step_id, action_type, display_name)
                self.step_configs[step_id] = step.copy()

                self.next_step_id = max(self.next_step_id, step_id + 1)

        self.variables_widget.set_variables(self.current_workflow.get('variables', {}))
        show_toast("已加载历史记录", 'success')
        app_logger.info("从历史记录加载工作流", "Workflow")
    
    def _on_action_selected(self, index):
        if index >= 0:
            item = self.action_list.item(index)
            action_type = item.data(Qt.UserRole)
            widget = self._ensure_config_widget(action_type)
            if widget:
                self.config_stack.setCurrentWidget(widget)

    def _on_add_step(self):
        current_row = self.action_list.currentRow()
        if current_row < 0:
            show_warning("提示", "请先选择一个动作类型")
            return

        item = self.action_list.item(current_row)
        action_type = item.data(Qt.UserRole)
        action_info = self.engine.get_action_info().get(action_type, {})

        step_id = self.next_step_id
        self.next_step_id += 1

        # Use lazy loading to get or create config widget
        config_widget = self._ensure_config_widget(action_type)
        if config_widget:
            config = config_widget.get_config()
        else:
            config = {}

        config['id'] = step_id
        config['type'] = action_type
        self.step_configs[step_id] = config

        display_name = action_info.get('display_name', action_type)
        self.step_list.add_step(step_id, action_type, display_name)
        self.step_list.set_current_index(self.step_list.get_step_count() - 1)

        app_logger.info(f"添加步骤: {display_name}", "Editor")
    
    def _on_remove_step(self):
        index = self.step_list.get_current_index()
        if index >= 0:
            item = self.step_list.list_widget.item(index)
            data = item.data(Qt.UserRole)
            step_id = data['id']
            
            if step_id in self.step_configs:
                del self.step_configs[step_id]
            
            self.step_list.remove_step(index)
            app_logger.info(f"删除步骤 [{step_id}]", "Editor")
    
    def _on_move_up(self):
        index = self.step_list.get_current_index()
        if index > 0:
            item = self.step_list.list_widget.takeItem(index)
            self.step_list.list_widget.insertItem(index - 1, item)
            self.step_list.set_current_index(index - 1)
    
    def _on_move_down(self):
        index = self.step_list.get_current_index()
        if index < self.step_list.get_step_count() - 1:
            item = self.step_list.list_widget.takeItem(index)
            self.step_list.list_widget.insertItem(index + 1, item)
            self.step_list.set_current_index(index + 1)
    
    def _on_step_selected(self, index):
        if index >= 0:
            item = self.step_list.list_widget.item(index)
            data = item.data(Qt.UserRole)
            step_id = data['id']
            action_type = data['type']

            # Use lazy loading to get or create config widget
            widget = self._ensure_config_widget(action_type)
            if widget:
                self.config_stack.setCurrentWidget(widget)

                if step_id in self.step_configs:
                    widget.set_config(self.step_configs[step_id])
    
    def _on_config_changed(self):
        index = self.step_list.get_current_index()
        if index >= 0:
            item = self.step_list.list_widget.item(index)
            data = item.data(Qt.UserRole)
            step_id = data['id']
            action_type = data['type']
            
            config_widget = self.config_widgets.get(action_type)
            if config_widget:
                config = config_widget.get_config()
                config['id'] = step_id
                config['type'] = action_type
                self.step_configs[step_id] = config
    
    def _on_engine_step_start(self, step):
        try:
            app_logger.info(f"执行步骤 [{step.get('id')}]: {step.get('type')}", "Engine")
            current = self.progress_bar.value()
            self.progress_bar.setValue(current + 1)
        except Exception as e:
            logger.error(f"Step start error: {e}")
    
    def _on_engine_step_end(self, step, result):
        try:
            step_duration = getattr(result, 'duration', 0) or 0
            execution_stats.record_step(
                step.get('type', 'unknown'),
                step_duration,
                result.success,
                result.message
            )
            
            if result.success:
                app_logger.success(f"步骤 [{step.get('id')}] 完成: {result.message} ({step_duration:.2f}秒)", "Engine")
            else:
                app_logger.error(f"步骤 [{step.get('id')}] 失败: {result.message}", "Engine")
                execution_stats.record_error(step.get('type', 'unknown'), result.message)
        except Exception as e:
            logger.error(f"Step end error: {e}")
    
    def _on_engine_workflow_end(self, success):
        try:
            if not self._is_looping:
                return
            
            loop_duration = 0
            if hasattr(self, '_loop_start_time'):
                loop_duration = time.time() - self._loop_start_time
            
            execution_stats.record_loop(self._current_loop, loop_duration, success, 
                                        len(self.current_workflow.get('steps', [])))
            
            if not success:
                self._is_looping = False
            
            if self._is_looping and self._current_loop < self._loop_count:
                app_logger.info(f"循环 {self._current_loop} 完成，等待 {self._loop_interval} 秒后继续...", "Workflow")
                QTimer.singleShot(int(self._loop_interval * 1000), self._run_single_loop)
                return
            
            session_result = execution_stats.end_session(success)
            total_duration = session_result.get('total_duration', 0) if session_result else 0
            avg_duration = session_result.get('avg_loop_duration', 0) if session_result else 0
            
            self.run_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.pause_btn.setEnabled(False)
            self.pause_btn.setText("⏸ 暂停")
            self.progress_bar.setVisible(False)
            self.progress_bar.setStyleSheet("")

            self.showNormal()
            self.activateWindow()
            
            loop_info = f" (共{self._loop_count}次循环)" if self._loop_count > 1 else ""
            time_info = f" | 总耗时: {total_duration:.1f}秒"
            if avg_duration > 0 and self._loop_count > 1:
                time_info += f" | 平均每循环: {avg_duration:.1f}秒"
            
            if success:
                app_logger.success(f"工作流执行完成{loop_info}{time_info}", "Workflow")
                show_toast(f"执行完成{loop_info} ({total_duration:.1f}秒)", 'success')
            else:
                app_logger.warning("工作流已停止", "Workflow")
        except Exception as e:
            logger.error(f"Workflow end error: {e}")
    
    def _on_engine_error(self, step, message: str):
        try:
            app_logger.error(f"步骤 [{step.get('id')}] 错误: {message}", "Engine")
            execution_stats.record_error(step.get('type', 'unknown'), message)
        except Exception as e:
            logger.error(f"Engine error: {e}")

    def _on_hotkey_settings(self) -> None:
        dialog = HotkeySettingsDialog(self.current_hotkeys, self)
        if dialog.exec_() == QDialog.Accepted:
            new_hotkeys = dialog.get_hotkeys()
            self.current_hotkeys = new_hotkeys
            
            self.hotkey_manager.save_config(new_hotkeys)
            self.hotkey_manager.unregister_hotkeys()
            self.hotkey_manager.register_hotkeys(
                start_key=new_hotkeys.get('start', 'f6'),
                stop_key=new_hotkeys.get('stop', 'f7'),
                pause_key=new_hotkeys.get('pause', 'f8')
            )
            
            self._update_button_texts()
            app_logger.info(f"快捷键已更新", "Hotkey")
            show_toast("快捷键设置已保存", 'success')
    
    def _on_select_window(self):
        """选择目标窗口"""
        from utils.window_selector import WindowSelectorDialog
        
        if self._target_region and self.region_btn.text() != "📐 区域":
            reply = QMessageBox.question(
                self, '确认',
                '已选择区域，选择窗口将清除当前区域设置。\n是否继续？',
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            if reply == QMessageBox.No:
                return
        
        menu = QMenu(self)
        list_action = menu.addAction("🪟 从列表选择窗口")
        clear_action = menu.addAction("❌ 清除窗口选择")
        
        action = menu.exec_(QCursor.pos())
        
        if action == list_action:
            dialog = WindowSelectorDialog(self)
            if dialog.exec_() == QDialog.Accepted:
                window = dialog.get_selected_window()
                if window:
                    self._set_target_window(window)
        elif action == clear_action:
            self._clear_target_window()
    
    def _set_target_window(self, window):
        """设置目标窗口"""
        colors = theme_manager.colors
        self._target_region = window.region
        self._window_mode = True
        self.window_btn.setText(f"🪟 {window.title[:10]}...")
        self.window_btn.setStyleSheet(f"background-color: {colors['primary']}; color: white;")
        self.region_btn.setText("📐 区域")
        self.region_btn.setStyleSheet("")
        show_toast(f"已选择窗口: {window.title}", 'success')
        app_logger.info(f"设置目标窗口: {window.title}", "UI")
    
    def _clear_target_window(self):
        """清除目标窗口"""
        self._target_region = None
        self._window_mode = False
        self.window_btn.setText("🪟 窗口")
        self.window_btn.setStyleSheet("")
        show_toast("已清除窗口选择", 'info')
    
    def _on_select_region(self):
        """选择识别区域"""
        if self._target_region and self._window_mode:
            reply = QMessageBox.question(
                self, '确认',
                '已选择窗口，选择区域将清除当前窗口设置。\n是否继续？',
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            if reply == QMessageBox.No:
                return
        
        menu = QMenu(self)
        select_action = menu.addAction("📐 框选识别区域")
        clear_action = menu.addAction("❌ 清除区域")
        
        action = menu.exec_(QCursor.pos())
        
        if action == select_action:
            self._do_select_region()
        elif action == clear_action:
            self._clear_region()
    
    def _do_select_region(self):
        """执行区域选择"""
        self.showMinimized()
        QTimer.singleShot(300, self._create_region_selector)
    
    def _create_region_selector(self):
        """创建区域选择器"""
        try:
            self._region_selector = RegionSelector(mode='region')
            self._region_selector.region_selected.connect(self._on_region_selected)
            self._region_selector.cancelled.connect(self._on_region_cancelled)
        except Exception as e:
            self.showNormal()
            show_error("错误", f"创建区域选择器失败: {str(e)}")
    
    def _on_region_selected(self, x, y, w, h):
        """区域选择完成"""
        colors = theme_manager.colors
        self._target_region = (x, y, w, h)
        self._window_mode = False
        self.region_btn.setText(f"📐 ({x},{y},{w}x{h})")
        self.region_btn.setStyleSheet(f"background-color: {colors['success']}; color: white;")
        self.window_btn.setText("🪟 窗口")
        self.window_btn.setStyleSheet("")
        self.showNormal()
        self.activateWindow()
        show_toast(f"已选择区域: ({x}, {y}, {w}x{h})", 'success')
        app_logger.info(f"设置OCR区域: ({x}, {y}, {w}x{h})", "UI")
    
    def _on_region_cancelled(self):
        """区域选择取消"""
        self.showNormal()
        self.activateWindow()
    
    def _clear_region(self):
        """清除区域"""
        self._target_region = None
        self.region_btn.setText("📐 区域")
        self.region_btn.setStyleSheet("")
        show_toast("已清除识别区域", 'info')
    
    def _on_loop_settings(self):
        """循环执行设置"""
        from PyQt5.QtWidgets import QSpinBox, QDoubleSpinBox, QFormLayout
        
        dialog = QDialog(self)
        dialog.setWindowTitle("循环执行设置")
        dialog.setMinimumWidth(300)
        
        layout = QVBoxLayout(dialog)
        form = QFormLayout()
        
        loop_spin = QSpinBox()
        loop_spin.setRange(1, 9999)
        loop_spin.setValue(self._loop_count)
        form.addRow("循环次数:", loop_spin)
        
        interval_spin = QDoubleSpinBox()
        interval_spin.setRange(0, 3600)
        interval_spin.setValue(self._loop_interval)
        interval_spin.setSingleStep(0.5)
        form.addRow("循环间隔(秒):", interval_spin)
        
        layout.addLayout(form)

        colors = theme_manager.colors
        info_label = QLabel("提示: 循环次数>1时，工作流将重复执行")
        info_label.setStyleSheet(f"color: {colors['text_secondary']}; font-size: 11px;")
        layout.addWidget(info_label)
        
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)
        
        if dialog.exec_() == QDialog.Accepted:
            self._loop_count = loop_spin.value()
            self._loop_interval = interval_spin.value()

            if self._loop_count > 1:
                self.loop_btn.setText(f"🔄 x{self._loop_count}")
                self.loop_btn.setStyleSheet(f"background-color: {colors['warning']}; color: white;")
            else:
                self.loop_btn.setText("🔄 循环")
                self.loop_btn.setStyleSheet("")

            show_toast(f"循环设置: {self._loop_count}次, 间隔{self._loop_interval}秒", 'success')
            app_logger.info(f"循环设置: {self._loop_count}次, 间隔{self._loop_interval}秒", "UI")
    
    def _on_show_stats(self):
        dialog = StatsDialog(self)
        dialog.exec_()
    
    def _on_memory_optimize(self):
        """内存优化"""
        menu = QMenu(self)
        optimize_action = menu.addAction("🧹 立即优化")
        clear_cache_action = menu.addAction("🗑 清除缓存")
        status_action = menu.addAction("📊 内存状态")
        
        action = menu.exec_(QCursor.pos())
        
        if action == optimize_action:
            result = memory_manager.optimize()
            freed = result.get('freed', 0)
            after = result.get('after', 0)
            show_toast(f"内存优化完成，释放 {freed}MB，当前 {after}MB", 'success')
            app_logger.info(f"内存优化: 释放 {freed}MB, 当前 {after}MB", "Memory")
            self._update_memory_display()
        elif action == clear_cache_action:
            memory_manager.clear_cache()
            image_cache.clear()
            show_toast("缓存已清除", 'success')
            self._update_memory_display()
        elif action == status_action:
            usage = memory_manager.get_memory_usage()
            from PyQt5.QtWidgets import QMessageBox
            QMessageBox.information(self, "内存状态",
                f"物理内存 (RSS): {usage['rss']} MB\n"
                f"虚拟内存 (VMS): {usage['vms']} MB\n"
                f"缓存数量: {usage['cache_size']}")

    def _toggle_theme(self):
        """Toggle between light and dark themes."""
        new_theme = theme_manager.toggle_theme()

        if new_theme == ThemeType.DARK:
            self.theme_btn.setText("☀️ 浅色")
            self.theme_btn.setChecked(True)
        else:
            self.theme_btn.setText("🌙 深色")
            self.theme_btn.setChecked(False)

        self._apply_theme()
        show_toast(f"主题已切换为{'深色' if new_theme == ThemeType.DARK else '浅色'}模式", 'info')
        app_logger.info(f"主题切换: {new_theme.value}", "UI")

    def _apply_theme(self) -> None:
        """Apply the current theme to all UI components."""
        self.setStyleSheet(theme_manager.get_stylesheet("main_window"))
        self.log_widget.text_edit.setStyleSheet(theme_manager.get_stylesheet("log"))

        # Update animated button styles
        self.run_btn.setStyleSheet(theme_manager.get_button_stylesheet('success'))
        self.stop_btn.setStyleSheet(theme_manager.get_button_stylesheet('danger'))
        self.pause_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))
        self.theme_btn.setStyleSheet(theme_manager.get_button_stylesheet('default'))

        # Apply splitter styling
        self.splitter.setStyleSheet(theme_manager.get_stylesheet("splitter"))

    def changeEvent(self, event):
        """Handle window state changes to optimize timer usage."""
        if event.type() == event.WindowStateChange:
            if self.windowState() & Qt.WindowMinimized:
                # Pause memory timer when minimized to save resources
                self._memory_timer.stop()
            elif self.windowState() & Qt.WindowNoState:
                # Resume when restored
                if not self._memory_timer.isActive():
                    self._memory_timer.start()
        super().changeEvent(event)

    def closeEvent(self, event):
        self._memory_timer.stop()
        self.hotkey_manager.unregister_hotkeys()
        
        if hasattr(self, 'engine_signals'):
            try:
                self.engine_signals.deleteLater()
            except Exception as e:
                import logging
                logging.getLogger('RabAI').debug(f'清理engine_signals失败: {e}')
        
        if self.engine.is_running():
            self.engine.stop()
        
        memory_manager.clear_cache()
        image_cache.clear()
        memory_manager.optimize()
        
        app_logger.info("程序退出", "Main")
        event.accept()


def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    app.setApplicationName('RabAI AutoClick')
    app.setApplicationVersion('1.1.0')
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
