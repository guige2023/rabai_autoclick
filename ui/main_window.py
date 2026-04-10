import sys
import os
import platform
import time
import copy
import logging
import yaml
import io
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
    QMenu, QAction, QToolBar, QStatusBar, QCheckBox,
    QGraphicsView, QGraphicsScene, QGraphicsRectItem, QGraphicsTextItem,
    QGraphicsLineItem, QGraphicsProxyWidget, QGraphicsItem,
    QLineEdit, QSlider, QListView, QAbstractItemView, QSizePolicy,
    QGraphicsSimpleTextItem, QPen, QBrush, QColor, QFont, QCursor,
    QShortcut, QKeySequenceEdit, QColorDialog, QInputDialog,
    QApplication, QFrame, QScrollArea, QDockWidget, QTextBrowser
)
from PyQt5.QtCore import Qt, pyqtSignal, QTimer, QThread, QObject, QRectF, QPointF
from PyQt5.QtGui import QIcon, QColor, QFont, QCursor, QPainter, QPen, QBrush, QKeySequence

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

logger = logging.getLogger(__name__)

IS_MACOS = platform.system() == 'Darwin'
IS_WINDOWS = platform.system() == 'Windows'

# YAML availability flag
YAML_AVAILABLE = True
try:
    yaml.safe_dump({'test': 1})
except AttributeError:
    YAML_AVAILABLE = False


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
                
                desc = PARAM_DESCRIPTIONS.get(param, '')
                example = PARAM_EXAMPLES.get(param, '')
                if desc or example:
                    help_text = desc
                    if example:
                        help_text += f"\n{example}"
                    help_label = QLabel(help_text)
                    help_label.setStyleSheet("color: #666; font-size: 10px;")
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
    breakpoint_toggled = pyqtSignal(int, bool)
    
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
        self.remove_btn = QPushButton("删除")
        self.up_btn = QPushButton("↑")
        self.down_btn = QPushButton("↓")
        
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
            breakpoint_action = menu.addAction("切换断点" if not self._has_breakpoint(item) else "取消断点")
            
            action = menu.exec_(self.list_widget.mapToGlobal(position))
            if action == duplicate_action:
                data = item.data(Qt.UserRole)
                self.add_step(data['id'] + 1000, data['type'], item.text().split('] ')[1] if '] ' in item.text() else data['type'])
            elif action == delete_action:
                self.remove_step(self.list_widget.row(item))
            elif action == breakpoint_action:
                data = item.data(Qt.UserRole)
                self.breakpoint_toggled.emit(data['id'], not self._has_breakpoint(item))
    
    def _has_breakpoint(self, item):
        return item.font().bold() and "🔴" in item.text()
    
    def add_step(self, step_id: int, action_type: str, display_name: str, has_breakpoint: bool = False):
        prefix = "🔴 " if has_breakpoint else ""
        item = QListWidgetItem(f"{prefix}[{step_id}] {display_name}")
        item.setData(Qt.UserRole, {'id': step_id, 'type': action_type, 'breakpoint': has_breakpoint})
        if has_breakpoint:
            font = item.font()
            font.setBold(True)
            item.setFont(font)
        self.list_widget.addItem(item)
    
    def set_breakpoint(self, step_id: int, has_breakpoint: bool):
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            data = item.data(Qt.UserRole)
            if data['id'] == step_id:
                data['breakpoint'] = has_breakpoint
                text = item.text()
                if has_breakpoint and not text.startswith("🔴"):
                    item.setText(f"🔴 {text}")
                    font = item.font()
                    font.setBold(True)
                    item.setFont(font)
                elif not has_breakpoint and text.startswith("🔴"):
                    item.setText(text[2:])
                    font = item.font()
                    font.setBold(False)
                    item.setFont(font)
                break
    
    def get_current_index(self) -> int:
        return self.list_widget.currentRow()
    
    def set_current_index(self, index: int):
        self.list_widget.setCurrentRow(index)
    
    def remove_step(self, index: int):
        self.list_widget.takeItem(index)
    
    def clear(self):
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
        self.table.setColumnCount(2)
        self.table.setHorizontalHeaderLabels(["变量名", "值"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.table)
        
        btn_layout = QHBoxLayout()
        self.add_btn = QPushButton("添加变量")
        self.remove_btn = QPushButton("删除变量")
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
        self.table.setRowCount(0)
        for name, value in variables.items():
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.table.setItem(row, 0, QTableWidgetItem(name))
            self.table.setItem(row, 1, QTableWidgetItem(json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)))


class VariableInspectorWidget(QWidget):
    """Variable inspector for runtime debugging"""
    variable_changed = pyqtSignal(str, object)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._context = {}
        self._watched_vars = []
        self._init_ui()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        
        # Watch section
        watch_label = QLabel("监控变量:")
        watch_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(watch_label)
        
        self.watch_table = QTableWidget()
        self.watch_table.setColumnCount(2)
        self.watch_table.setHorizontalHeaderLabels(["变量名", "当前值"])
        self.watch_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.watch_table.setMaximumHeight(120)
        layout.addWidget(self.watch_table)
        
        # Add watch row
        watch_add_layout = QHBoxLayout()
        self.watch_input = QLineEdit()
        self.watch_input.setPlaceholderText("输入变量名添加监控...")
        self.watch_add_btn = QPushButton("+ 添加")
        self.watch_add_btn.clicked.connect(self._add_watch)
        watch_add_layout.addWidget(self.watch_input)
        watch_add_layout.addWidget(self.watch_add_btn)
        layout.addLayout(watch_add_layout)
        
        # Context section
        context_label = QLabel("执行上下文:")
        context_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(context_label)
        
        self.context_table = QTableWidget()
        self.context_table.setColumnCount(2)
        self.context_table.setHorizontalHeaderLabels(["变量名", "值"])
        self.context_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.context_table)
        
        # Refresh button
        refresh_btn = QPushButton("刷新")
        refresh_btn.clicked.connect(self._refresh_context)
        layout.addWidget(refresh_btn)
    
    def _add_watch(self):
        var_name = self.watch_input.text().strip()
        if var_name and var_name not in self._watched_vars:
            self._watched_vars.append(var_name)
            row = self.watch_table.rowCount()
            self.watch_table.insertRow(row)
            self.watch_table.setItem(row, 0, QTableWidgetItem(var_name))
            self.watch_table.setItem(row, 1, QTableWidgetItem(""))
            self.watch_input.clear()
    
    def update_context(self, context: Dict[str, Any]):
        """Update the context during execution"""
        self._context = context
        self.context_table.setRowCount(0)
        for name, value in context.items():
            row = self.context_table.rowCount()
            self.context_table.insertRow(row)
            value_str = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
            self.context_table.setItem(row, 0, QTableWidgetItem(name))
            self.context_table.setItem(row, 1, QTableWidgetItem(value_str))
        
        # Update watched variables
        for i, var_name in enumerate(self._watched_vars):
            if i < self.watch_table.rowCount():
                value = context.get(var_name, "<undefined>")
                value_str = json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)
                self.watch_table.setItem(i, 1, QTableWidgetItem(value_str))
    
    def _refresh_context(self):
        # Emit signal to request context update from engine
        pass
    
    def clear(self):
        self._context = {}
        self.context_table.setRowCount(0)


class LogWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                color: #d4d4d4;
                font-family: Consolas, 'Microsoft YaHei';
                font-size: 12px;
            }
        """)
        layout.addWidget(self.text_edit)
        
        btn_layout = QHBoxLayout()
        clear_btn = QPushButton("清空日志")
        export_btn = QPushButton("导出日志")
        btn_layout.addWidget(clear_btn)
        btn_layout.addWidget(export_btn)
        layout.addLayout(btn_layout)
        
        clear_btn.clicked.connect(self.clear)
        export_btn.clicked.connect(self._export_log)
        
        app_logger.add_listener(self._on_log_entry)
    
    def _on_log_entry(self, entry):
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
        info_label.setStyleSheet("color: gray; font-size: 11px;")
        layout.addWidget(info_label)
        
        btn_layout = QHBoxLayout()
        
        self.record_btn = QPushButton("🔴 开始录制")
        self.record_btn.setStyleSheet("background-color: #f44336; color: white; font-weight: bold;")
        self.stop_btn = QPushButton("⏹ 停止录制")
        self.stop_btn.setEnabled(False)
        self.clear_btn = QPushButton("清空")
        self.optimize_btn = QPushButton("优化")
        
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
            self.status_label.setStyleSheet("color: red;")
    
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
    
    def _on_stop(self):
        actions = self._recording_manager.stop_recording()
        self.record_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.status_label.setText(f"状态: 已录制 {len(actions)} 个操作")
        show_toast(f"录制完成，共 {len(actions)} 个操作", 'success')
    
    def _on_clear(self):
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
        self.action_list.clear()
        for action in self._recording_manager.get_actions():
            self.action_list.addItem(f"{action.action_type}: {action.params}")
    
    def get_workflow(self) -> Dict[str, Any]:
        return self._recording_manager.to_workflow()


class WorkflowCanvasNode(QGraphicsRectItem):
    """Node representation for workflow canvas"""
    
    def __init__(self, step_id: int, action_type: str, display_name: str, x: float, y: float):
        super().__init__(0, 0, 160, 60)
        self.step_id = step_id
        self.action_type = action_type
        self.display_name = display_name
        self.has_breakpoint = False
        
        self.setPos(x, y)
        self.setBrush(QBrush(QColor("#2d5a8a")))
        self.setPen(QPen(QColor("#4a90d9"), 2))
        self.setFlag(QGraphicsItem.ItemIsMovable)
        self.setFlag(QGraphicsItem.ItemIsSelectable)
        
        # Node title
        self.title_item = QGraphicsSimpleTextItem(display_name, self)
        self.title_item.setPos(10, 10)
        self.title_item.setBrush(QBrush(QColor("#ffffff")))
        
        # Step ID
        self.id_item = QGraphicsSimpleTextItem(f"#{step_id}", self)
        self.id_item.setPos(10, 35)
        self.id_item.setBrush(QBrush(QColor("#a0c4e8")))
        
        # Input/output connection points
        self.input_point = QGraphicsRectItem(-5, 25, 10, 10, self)
        self.input_point.setBrush(QBrush(QColor("#ffcc00")))
        self.input_point.setFlag(QGraphicsItem.ItemIsMovable, False)
        
        self.output_point = QGraphicsRectItem(155, 25, 10, 10, self)
        self.output_point.setBrush(QBrush(QColor("#00cc00")))
        self.output_point.setFlag(QGraphicsItem.ItemIsMovable, False)
    
    def set_breakpoint(self, has_breakpoint: bool):
        self.has_breakpoint = has_breakpoint
        if has_breakpoint:
            self.setBrush(QBrush(QColor("#8a2d2d")))
            self.setPen(QPen(QColor("#d94a4a"), 3))
        else:
            self.setBrush(QBrush(QColor("#2d5a8a")))
            self.setPen(QPen(QColor("#4a90d9"), 2))
    
    def set_highlight(self, highlighted: bool):
        if highlighted:
            self.setPen(QPen(QColor("#00ff00"), 3))
        else:
            self.setPen(QPen(QColor("#4a90d9"), 2))


class WorkflowArrow(QGraphicsLineItem):
    """Arrow connection between nodes"""
    
    def __init__(self, start_node: WorkflowCanvasNode, end_node: WorkflowCanvasNode):
        super().__init__()
        self.start_node = start_node
        self.end_node = end_node
        self.setFlag(QGraphicsItem.ItemIsMovable, False)
        self.update_line()
    
    def update_line(self):
        start = self.start_node.scenePos() + QPointF(160, 30)
        end = self.end_node.scenePos() + QPointF(0, 30)
        self.setLine(start.x(), start.y(), end.x(), end.y())
        self.setPen(QPen(QColor("#888888"), 2))


class WorkflowCanvas(QGraphicsView):
    """Visual workflow editor canvas with drag-drop support"""
    
    node_selected = pyqtSignal(int)
    node_moved = pyqtSignal(int, float, float)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.scene = QGraphicsScene(self)
        self.setScene(self.scene)
        self.setAcceptDrops(True)
        self.setRenderHint(QPainter.Antialiasing)
        self.setDragMode(QGraphicsView.RubberBandDrag)
        
        self.nodes = {}
        self.arrows = []
        self.node_positions = {}
        
        self.scene.setSceneRect(-500, -500, 2000, 2000)
        self.setBackgroundBrush(QBrush(QColor("#2b2b2b")))
        
        # Grid
        self._draw_grid()
    
    def _draw_grid(self):
        """Draw grid background"""
        grid_pen = QPen(QColor("#3a3a3a"), 0.5)
        for x in range(-500, 1500, 50):
            self.scene.addLine(x, -500, x, 1500, grid_pen)
        for y in range(-500, 1500, 50):
            self.scene.addLine(-500, y, 1500, y, grid_pen)
    
    def add_node(self, step_id: int, action_type: str, display_name: str, x: float = None, y: float = None):
        if x is None:
            x = (len(self.nodes) % 5) * 200 + 100
        if y is None:
            y = (len(self.nodes) // 5) * 100 + 100
        
        node = WorkflowCanvasNode(step_id, action_type, display_name, x, y)
        self.scene.addItem(node)
        self.nodes[step_id] = node
        self.node_positions[step_id] = (x, y)
        
        # Add connections to previous nodes
        if len(self.nodes) > 1:
            prev_node = list(self.nodes.values())[-2]
            arrow = WorkflowArrow(prev_node, node)
            self.scene.addItem(arrow)
            self.arrows.append(arrow)
        
        return node
    
    def update_connections(self):
        """Update all arrow connections"""
        for arrow in self.arrows:
            arrow.update_line()
    
    def set_breakpoint(self, step_id: int, has_breakpoint: bool):
        if step_id in self.nodes:
            self.nodes[step_id].set_breakpoint(has_breakpoint)
    
    def highlight_node(self, step_id: int, highlighted: bool):
        if step_id in self.nodes:
            self.nodes[step_id].set_highlight(highlighted)
    
    def clear(self):
        self.scene.clear()
        self.nodes = {}
        self.arrows = []
        self._draw_grid()
    
    def dragEnterEvent(self, event):
        if event.mimeData().hasText():
            event.acceptProposedAction()
    
    def dropEvent(self, event):
        # Handle drop from action palette
        if event.mimeData().hasText():
            action_type = event.mimeData().text()
            pos = self.mapToScene(event.pos())
            # Emit signal to add step at position
            self.window()._on_canvas_drop(action_type, pos.x(), pos.y())


class ActionPaletteWidget(QWidget):
    """Action palette with search functionality"""
    
    action_selected = pyqtSignal(str)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        
        # Search box
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("搜索动作...")
        self.search_box.textChanged.connect(self._filter_actions)
        layout.addWidget(self.search_box)
        
        # Action list
        self.action_list = QListWidget()
        self.action_list.setDragEnabled(True)
        self.action_list.setDragDropMode(QAbstractItemView.DragOnly)
        self.action_list.doubleClicked.connect(lambda: self._on_double_click())
        layout.addWidget(self.action_list)
    
    def set_actions(self, action_info: Dict[str, dict]):
        self.action_info = action_info
        self._populate_list()
    
    def _populate_list(self, filter_text: str = ""):
        self.action_list.clear()
        for action_type, info in self.action_info.items():
            if not filter_text or filter_text.lower() in info.get('display_name', '').lower() or filter_text.lower() in action_type.lower():
                item = QListWidgetItem(f"📦 {info.get('display_name', action_type)}")
                item.setData(Qt.UserRole, action_type)
                item.setToolTip(f"{info.get('description', '')}\n类型: {action_type}")
                self.action_list.addItem(item)
    
    def _filter_actions(self, text: str):
        self._populate_list(text)
    
    def _on_double_click(self):
        current_item = self.action_list.currentItem()
        if current_item:
            action_type = current_item.data(Qt.UserRole)
            self.action_selected.emit(action_type)


class RecentWorkflowsMenu(QMenu):
    """Recent workflows menu"""
    
    workflow_selected = pyqtSignal(str)
    
    def __init__(self, title: str = "最近工作流", parent=None):
        super().__init__(title, parent)
        self.recent_files = []
        self.max_recent = 10
        self._load_recent()
        self._build_menu()
    
    def _load_recent(self):
        """Load recent workflows from config"""
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json')
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self.recent_files = config.get('recent_workflows', [])
        except Exception:
            self.recent_files = []
    
    def _save_recent(self):
        """Save recent workflows to config"""
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json')
        try:
            config = {}
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            config['recent_workflows'] = self.recent_files[:self.max_recent]
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    
    def add_recent(self, filepath: str):
        """Add a workflow to recent list"""
        if filepath in self.recent_files:
            self.recent_files.remove(filepath)
        self.recent_files.insert(0, filepath)
        self.recent_files = self.recent_files[:self.max_recent]
        self._save_recent()
        self._build_menu()
    
    def _build_menu(self):
        self.clear()
        if not self.recent_files:
            self.addAction("无最近工作流")
        else:
            for i, filepath in enumerate(self.recent_files):
                basename = os.path.basename(filepath)
                action = self.addAction(f"{i+1}. {basename}")
                action.setData(filepath)
                action.triggered.connect(lambda checked, fp=filepath: self.workflow_selected.emit(fp))
            self.addSeparator()
            self.addAction("清除记录").triggered.connect(self._clear_recent)
    
    def _clear_recent(self):
        self.recent_files = []
        self._save_recent()
        self._build_menu()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("RabAI AutoClick - 桌面自动化工具 v1.3")
        self.setGeometry(100, 100, 1400, 900)
        
        self.engine = FlowEngine()
        self.current_workflow = {'variables': {}, 'steps': []}
        self.next_step_id = 1
        self.step_configs = {}
        self.breakpoints = set()
        
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
        
        # Theme
        self.current_theme = "dark"
        self.themes = {
            "dark": {
                "bg": "#1e1e1e", "panel": "#252526", "text": "#d4d4d4",
                "accent": "#0078d4", "border": "#3a3a3a", "highlight": "#264f78"
            },
            "light": {
                "bg": "#ffffff", "panel": "#f3f3f3", "text": "#000000",
                "accent": "#0078d4", "border": "#cccccc", "highlight": "#e5f3ff"
            },
            "high-contrast": {
                "bg": "#000000", "panel": "#000000", "text": "#ffffff",
                "accent": "#ffff00", "border": "#ffffff", "highlight": "#ffff00"
            }
        }
        
        # Execution tracking
        self._execution_start_time = None
        self._current_step_index = -1
        self._total_actions = 0
        
        message_manager.set_parent(self)
        
        self._init_ui()
        self._setup_toolbar()
        self._setup_statusbar()
        self._setup_keyboard_shortcuts()
        self._setup_hotkeys()
        self._connect_signals()
        self._update_button_texts()
        self._apply_theme(self.current_theme)
        
        app_logger.info("程序启动", "Main")
    
    def _init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(5)
        
        # Create main splitter
        splitter = QSplitter(Qt.Horizontal)
        
        # Left panel - Step list and canvas
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        # Step list with breakpoint support
        self.step_list = StepListWidget()
        left_layout.addWidget(QLabel("步骤列表:"))
        left_layout.addWidget(self.step_list)
        
        splitter.addWidget(left_panel)
        
        # Right panel - Tabs
        right_panel = QTabWidget()
        
        # Steps editor tab
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
        
        # Action palette tab
        self.action_palette = ActionPaletteWidget()
        self.action_palette.action_selected.connect(self._on_palette_action_selected)
        right_panel.addTab(self.action_palette, "动作面板")
        
        # Variables tab
        self.variables_widget = VariablesWidget()
        right_panel.addTab(self.variables_widget, "变量")
        
        # Recording tab
        self.recording_widget = RecordingWidget()
        right_panel.addTab(self.recording_widget, "录屏")
        
        # Log tab
        self.log_widget = LogWidget()
        right_panel.addTab(self.log_widget, "日志")
        
        splitter.addWidget(right_panel)
        splitter.setSizes([350, 1050])
        
        main_layout.addWidget(splitter)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        
        self._load_actions()
        
        self._memory_timer = QTimer()
        self._memory_timer.timeout.connect(self._update_memory_display)
        self._memory_timer.start(5000)
    
    def _setup_toolbar(self):
        """Setup toolbar with quick actions"""
        toolbar = QToolBar("主工具栏")
        toolbar.setMovable(False)
        self.addToolBar(toolbar)
        
        # File operations
        self.new_action = QAction("📄 新建", self)
        self.new_action.setToolTip("新建工作流 (Ctrl+N)")
        toolbar.addAction(self.new_action)
        
        self.open_action = QAction("📂 打开", self)
        self.open_action.setToolTip("打开工作流 (Ctrl+O)")
        toolbar.addAction(self.open_action)
        
        self.save_action = QAction("💾 保存", self)
        self.save_action.setToolTip("保存工作流 (Ctrl+S)")
        toolbar.addAction(self.save_action)
        
        self.import_action = QAction("📥 导入", self)
        self.import_action.setToolTip("导入工作流")
        toolbar.addAction(self.import_action)
        
        self.export_action = QAction("📤 导出", self)
        self.export_action.setToolTip("导出工作流")
        toolbar.addAction(self.export_action)
        
        toolbar.addSeparator()
        
        # Execution controls
        self.run_action = QAction("▶ 运行", self)
        self.run_action.setToolTip("运行工作流 (Ctrl+R)")
        self.run_action.setShortcut("Ctrl+R")
        toolbar.addAction(self.run_action)
        
        self.stop_action = QAction("⏹ 停止", self)
        self.stop_action.setToolTip("停止执行 (Ctrl+Shift+S)")
        self.stop_action.setEnabled(False)
        toolbar.addAction(self.stop_action)
        
        self.pause_action = QAction("⏸ 暂停", self)
        self.pause_action.setToolTip("暂停/继续执行 (Ctrl+P)")
        self.pause_action.setEnabled(False)
        toolbar.addAction(self.pause_action)
        
        self.step_action = QAction("⏭ 单步", self)
        self.step_action.setToolTip("单步执行 (F10)")
        toolbar.addAction(self.step_action)
        
        toolbar.addSeparator()
        
        # View toggles
        self.canvas_action = QAction("🎨 画布", self)
        self.canvas_action.setToolTip("切换画布视图")
        self.canvas_action.setCheckable(True)
        toolbar.addAction(self.canvas_action)
        
        self.inspector_action = QAction("🔍 变量监控", self)
        self.inspector_action.setToolTip("切换变量监控面板")
        self.inspector_action.setCheckable(True)
        toolbar.addAction(self.inspector_action)
        
        toolbar.addSeparator()
        
        # Theme switcher
        theme_label = QLabel("主题:")
        toolbar.addWidget(theme_label)
        
        self.theme_combo = QComboBox()
        self.theme_combo.addItems(["暗色", "亮色", "高对比度"])
        self.theme_combo.setCurrentIndex(0)
        self.theme_combo.currentIndexChanged.connect(self._on_theme_changed)
        toolbar.addWidget(self.theme_combo)
        
        toolbar.addWidget(QLabel())  # Spacer
        
        # Memory display
        self.memory_label = QLabel()
        toolbar.addWidget(self.memory_label)
    
    def _setup_statusbar(self):
        """Setup enhanced status bar"""
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        
        # Current step
        self.status_step_label = QLabel("步骤: -")
        self.status_bar.addPermanentWidget(self.status_step_label)
        
        # Progress
        self.status_progress_label = QLabel("进度: 0/0")
        self.status_bar.addPermanentWidget(self.status_progress_label)
        
        # Elapsed time
        self.status_time_label = QLabel("耗时: 0.0s")
        self.status_bar.addPermanentWidget(self.status_time_label)
        
        # Action count
        self.status_action_label = QLabel("动作: 0")
        self.status_bar.addPermanentWidget(self.status_action_label)
        
        # Status message
        self.status_bar.showMessage("就绪 | Ctrl+R运行 | Ctrl+S保存")
    
    def _setup_keyboard_shortcuts(self):
        """Setup keyboard shortcuts"""
        shortcuts = [
            ("Ctrl+N", self._on_new),
            ("Ctrl+O", self._on_open),
            ("Ctrl+S", self._on_save),
            ("Ctrl+R", self._on_run),
            ("Ctrl+Shift+S", self._on_stop),
            ("Ctrl+P", self._on_pause),
            ("F5", self._on_run),
            ("F6", self._on_stop),
            ("F10", self._on_step),
        ]
        
        for key_seq, handler in shortcuts:
            shortcut = QShortcut(QKeySequence(key_seq), self)
            shortcut.activated.connect(handler)
    
    def _apply_theme(self, theme_name: str):
        """Apply theme to the application"""
        if theme_name not in self.themes:
            return
        
        theme = self.themes[theme_name]
        self.current_theme = theme_name
        
        stylesheet = f"""
            QMainWindow, QWidget {{
                background-color: {theme['bg']};
                color: {theme['text']};
            }}
            QGroupBox {{
                border: 1px solid {theme['border']};
                margin-top: 8px;
                padding-top: 8px;
            }}
            QPushButton {{
                background-color: {theme['panel']};
                border: 1px solid {theme['border']};
                padding: 5px 10px;
            }}
            QPushButton:hover {{
                background-color: {theme['highlight']};
            }}
            QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox {{
                background-color: {theme['panel']};
                border: 1px solid {theme['border']};
            }}
            QTabWidget::pane {{
                border: 1px solid {theme['border']};
            }}
            QTabBar::tab {{
                background-color: {theme['panel']};
                padding: 5px 10px;
            }}
            QTabBar::tab:selected {{
                background-color: {theme['highlight']};
            }}
            QListWidget, QTableWidget {{
                background-color: {theme['panel']};
                border: 1px solid {theme['border']};
            }}
            QToolBar {{
                background-color: {theme['panel']};
                border: none;
                spacing: 3px;
            }}
            QStatusBar {{
                background-color: {theme['panel']};
            }}
            QLabel {{
                color: {theme['text']};
            }}
        """
        self.setStyleSheet(stylesheet)
    
    def _on_theme_changed(self, index):
        themes = ["dark", "light", "high-contrast"]
        if index < len(themes):
            self._apply_theme(themes[index])
    
    def _update_statusbar(self):
        """Update status bar information"""
        total_steps = self.step_list.get_step_count()
        
        if self._current_step_index >= 0 and self._current_step_index < total_steps:
            self.status_step_label.setText(f"步骤: {self._current_step_index + 1}/{total_steps}")
        else:
            self.status_step_label.setText("步骤: -")
        
        self.status_progress_label.setText(f"进度: {self._current_step_index}/{total_steps}")
        
        if self._execution_start_time:
            elapsed = time.time() - self._execution_start_time
            self.status_time_label.setText(f"耗时: {elapsed:.1f}s")
        
        self.status_action_label.setText(f"动作: {self._total_actions}")
    
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
        
        for action_type, info in action_info.items():
            item = QListWidgetItem(f"{info['display_name']}")
            item.setData(Qt.UserRole, action_type)
            item.setToolTip(f"{info['description']}\n类型: {action_type}")
            self.action_list.addItem(item)
        
        # Setup action palette
        self.action_palette.set_actions(action_info)
        
        self.config_widgets = {}
        for action_type, info in action_info.items():
            widget = ActionConfigWidget(info)
            self.config_widgets[action_type] = widget
            self.config_stack.addWidget(widget)
    
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
        
        self.run_action.setText(f"▶ 运行 ({start_key})")
        self.stop_action.setText(f"⏹ 停止 ({stop_key})")
        self.pause_action.setText(f"⏸ 暂停 ({pause_key})")
    
    def _connect_signals(self):
        # Toolbar actions
        self.new_action.triggered.connect(self._on_new)
        self.open_action.triggered.connect(self._on_open)
        self.save_action.triggered.connect(self._on_save)
        self.import_action.triggered.connect(self._on_import)
        self.export_action.triggered.connect(self._on_export)
        self.run_action.triggered.connect(self._on_run)
        self.stop_action.triggered.connect(self._on_stop)
        self.pause_action.triggered.connect(self._on_pause)
        self.step_action.triggered.connect(self._on_step)
        
        # Step list signals
        self.step_list.add_btn.clicked.connect(self._on_add_step)
        self.step_list.remove_btn.clicked.connect(self._on_remove_step)
        self.step_list.up_btn.clicked.connect(self._on_move_up)
        self.step_list.down_btn.clicked.connect(self._on_move_down)
        self.step_list.step_selected.connect(self._on_step_selected)
        self.step_list.breakpoint_toggled.connect(self._on_breakpoint_toggled)
        
        # Action list
        self.action_list.currentRowChanged.connect(self._on_action_selected)
        
        # Config widgets
        for widget in self.config_widgets.values():
            widget.config_changed.connect(self._on_config_changed)
        
        # Engine signals
        self.engine_signals = EngineSignals()
        self.engine_signals.step_start.connect(self._on_engine_step_start)
        self.engine_signals.step_end.connect(self._on_engine_step_end)
        self.engine_signals.workflow_end.connect(self._on_engine_workflow_end)
        self.engine_signals.error.connect(self._on_engine_error)
        
        self.engine.set_callbacks(
            on_step_start=lambda step: self.engine_signals.step_start.emit(step),
            on_step_end=lambda step, result: self.engine_signals.step_end.emit(step, result),
            on_workflow_end=lambda success: self.engine_signals.workflow_end.emit(success),
            on_error=lambda step, msg: self.engine_signals.error.emit(step, msg)
        )
    
    def _on_palette_action_selected(self, action_type: str):
        """Handle action selected from palette"""
        # Find and select in action list
        for i in range(self.action_list.count()):
            item = self.action_list.item(i)
            if item.data(Qt.UserRole) == action_type:
                self.action_list.setCurrentRow(i)
                break
    
    def _on_breakpoint_toggled(self, step_id: int, has_breakpoint: bool):
        """Toggle breakpoint on a step"""
        if has_breakpoint:
            self.breakpoints.add(step_id)
        else:
            self.breakpoints.discard(step_id)
        
        self.step_list.set_breakpoint(step_id, has_breakpoint)
        
        step_name = f"步骤 #{step_id}"
        if has_breakpoint:
            app_logger.info(f"已设置断点: {step_name}", "Breakpoint")
        else:
            app_logger.info(f"已取消断点: {step_name}", "Breakpoint")
    
    def _on_import(self):
        """Import workflow from JSON or YAML"""
        file_path, selected_filter = QFileDialog.getOpenFileName(
            self, "导入工作流", "",
            "所有支持格式 (*.json *.yaml *.yml);;JSON文件 (*.json);;YAML文件 (*.yaml *.yml)"
        )
        
        if not file_path:
            return
        
        if not os.path.exists(file_path):
            show_error("导入失败", "文件不存在")
            return
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                if file_path.endswith(('.yaml', '.yml')):
                    if not YAML_AVAILABLE:
                        show_error("导入失败", "PyYAML未安装，无法导入YAML文件")
                        return
                    workflow = yaml.safe_load(f)
                else:
                    workflow = json.load(f)
            
            self._load_workflow_data(workflow)
            app_logger.info(f"成功导入: {file_path}", "Workflow")
            show_toast(f"已导入: {os.path.basename(file_path)}", 'success')
            
        except Exception as e:
            show_error("导入失败", f"无法导入文件: {str(e)}")
    
    def _on_export(self):
        """Export workflow to JSON or YAML"""
        self._build_workflow()
        
        if not self.current_workflow.get('steps'):
            show_warning("提示", "工作流中没有步骤")
            return
        
        file_path, selected_filter = QFileDialog.getSaveFileName(
            self, "导出工作流", "",
            "JSON文件 (*.json);;YAML文件 (*.yaml)"
        )
        
        if not file_path:
            return
        
        try:
            # Determine format
            is_yaml = file_path.endswith(('.yaml', '.yml'))
            
            if is_yaml:
                if not YAML_AVAILABLE:
                    show_error("导出失败", "PyYAML未安装，无法导出YAML文件")
                    return
                with open(file_path, 'w', encoding='utf-8') as f:
                    yaml.safe_dump(self.current_workflow, f, allow_unicode=True, default_flow_style=False)
            else:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(self.current_workflow, f, ensure_ascii=False, indent=2)
            
            app_logger.info(f"成功导出: {file_path}", "Workflow")
            show_toast(f"已导出: {os.path.basename(file_path)}", 'success')
            
        except Exception as e:
            show_error("导出失败", f"无法导出文件: {str(e)}")
    
    def _load_workflow_data(self, workflow: Dict[str, Any]):
        """Load workflow data into the editor"""
        self.current_workflow = workflow
        self.step_list.clear()
        self.step_configs = {}
        self.breakpoints = {}
        self.next_step_id = 1
        
        for step in self.current_workflow.get('steps', []):
            step_id = step.get('id', self.next_step_id)
            action_type = step.get('type', '')
            has_breakpoint = step.get('breakpoint', False)
            
            action_info = self.engine.get_action_info().get(action_type, {})
            display_name = action_info.get('display_name', action_type)
            
            self.step_list.add_step(step_id, action_type, display_name, has_breakpoint)
            self.step_configs[step_id] = step.copy()
            
            if has_breakpoint:
                self.breakpoints[step_id] = True
            
            self.next_step_id = max(self.next_step_id, step_id + 1)
        
        self.variables_widget.set_variables(self.current_workflow.get('variables', {}))
    
    def _on_new(self):
        if self.step_list.get_step_count() > 0:
            if not show_question("确认", "是否新建工作流？当前未保存的内容将丢失。"):
                return
        
        self.current_workflow = {'variables': {}, 'steps': []}
        self.step_configs = {}
        self.breakpoints = {}
        self.next_step_id = 1
        self.step_list.clear()
        self.variables_widget.set_variables({})
        self._reset_execution_state()
        app_logger.info("新建工作流", "Workflow")
        show_toast("新建工作流", 'info')
    
    def _on_open(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "打开工作流", "", "所有支持格式 (*.json *.yaml *.yml);;JSON文件 (*.json);;YAML文件 (*.yaml *.yml)"
        )
        if file_path:
            self._open_workflow_file(file_path)
    
    def _open_workflow_file(self, file_path: str):
        """Open a workflow file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                if file_path.endswith(('.yaml', '.yml')):
                    if not YAML_AVAILABLE:
                        show_error("打开失败", "PyYAML未安装，无法打开YAML文件")
                        return
                    self.current_workflow = yaml.safe_load(f)
                else:
                    self.current_workflow = json.load(f)
            
            self.step_list.clear()
            self.step_configs = {}
            self.breakpoints = {}
            self.next_step_id = 1
            
            for step in self.current_workflow.get('steps', []):
                step_id = step.get('id', self.next_step_id)
                action_type = step.get('type', '')
                has_breakpoint = step.get('breakpoint', False)
                
                action_info = self.engine.get_action_info().get(action_type, {})
                display_name = action_info.get('display_name', action_type)
                
                self.step_list.add_step(step_id, action_type, display_name, has_breakpoint)
                self.step_configs[step_id] = step.copy()
                
                if has_breakpoint:
                    self.breakpoints[step_id] = True
                
                self.next_step_id = max(self.next_step_id, step_id + 1)
            
            self.variables_widget.set_variables(self.current_workflow.get('variables', {}))
            app_logger.info(f"打开工作流: {file_path}", "Workflow")
            show_toast(f"已打开: {os.path.basename(file_path)}", 'success')
            
            # Add to recent
            self._add_to_recent(file_path)
            
        except Exception as e:
            show_error("打开失败", f"无法打开文件: {str(e)}")
    
    def _add_to_recent(self, filepath: str):
        """Add file to recent workflows"""
        if hasattr(self, 'recent_menu'):
            self.recent_menu.add_recent(filepath)
    
    def _on_save(self):
        menu = QMenu(self)
        save_file_action = menu.addAction("💾 保存到文件... (Ctrl+S)")
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
            self._do_save_to_file(file_path)
    
    def _do_save_to_file(self, file_path: str, is_yaml: bool = False):
        """Save workflow to file"""
        try:
            self._build_workflow()
            with open(file_path, 'w', encoding='utf-8') as f:
                if is_yaml:
                    yaml.safe_dump(self.current_workflow, f, allow_unicode=True, default_flow_style=False)
                else:
                    json.dump(self.current_workflow, f, ensure_ascii=False, indent=2)
            app_logger.info(f"保存工作流: {file_path}", "Workflow")
            show_toast(f"已保存: {os.path.basename(file_path)}", 'success')
            self._add_to_recent(file_path)
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
                step['breakpoint'] = data.get('breakpoint', False)
                steps.append(step)
        
        self.current_workflow['steps'] = steps
        self.current_workflow['variables'] = self.variables_widget.get_variables()
    
    def _reset_execution_state(self):
        """Reset execution tracking state"""
        self._execution_start_time = None
        self._current_step_index = -1
        self._total_actions = 0
        self._update_statusbar()
    
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
        self._execution_start_time = time.time()
        self._total_actions = 0
        
        workflow_name = "未命名工作流"
        execution_stats.start_session(workflow_name, self._loop_count)
        
        self._run_single_loop()
    
    def _run_single_loop(self):
        if not self._is_looping:
            return
        
        if self._current_loop >= self._loop_count:
            return
        
        self._current_loop += 1
        
        app_logger.info(f"执行第 {self._current_loop}/{self._loop_count} 次循环", "Workflow")
        
        self._update_execution_buttons(running=True)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, len(self.current_workflow['steps']))
        self.progress_bar.setValue(0)
        
        if self._current_loop == 1:
            self.showMinimized()
        
        self.engine.load_workflow_from_dict(self.current_workflow)
        self.engine.run_async()
    
    def _update_execution_buttons(self, running: bool):
        """Update button states during execution"""
        self.run_action.setEnabled(not running)
        self.stop_action.setEnabled(running)
        self.pause_action.setEnabled(running)
    
    def _on_stop(self):
        self._is_looping = False
        self._current_loop = 0
        self.engine.stop()
        self._update_execution_buttons(running=False)
        self._reset_execution_state()
        self.showNormal()
        self.activateWindow()
        app_logger.warning("停止工作流", "Workflow")
        show_toast("工作流已停止", 'warning')
    
    def _on_pause(self):
        if self.engine.is_paused():
            self.engine.resume()
            self.pause_action.setText("⏸ 暂停")
            app_logger.info("继续运行", "Workflow")
        else:
            self.engine.pause()
            self.pause_action.setText("▶ 继续")
            app_logger.info("已暂停", "Workflow")
    
    def _on_step(self):
        """Execute single step"""
        show_toast("单步执行功能开发中", 'info')
    
    def _on_engine_step_start(self, step):
        try:
            step_id = step.get('id')
            step_type = step.get('type')
            
            # Check for breakpoint
            if step_id in self.breakpoints:
                self.engine.pause()
                app_logger.warning(f"断点命中: 步骤 [{step_id}] {step_type}", "Breakpoint")
                show_toast(f"断点命中: 步骤 #{step_id}", 'warning')
                return
            
            app_logger.info(f"执行步骤 [{step_id}]: {step_type}", "Engine")
            
            # Update progress
            current = self.progress_bar.value()
            self.progress_bar.setValue(current + 1)
            
            # Track step index
            self._current_step_index = current
            self._total_actions += 1
            
            self._update_statusbar()
            
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
            
            self._update_statusbar()
            
        except Exception as e:
            logger.error(f"Step end error: {e}")
    
    def _on_engine_workflow_end(self, success):
        try:
            if not self._is_looping:
                return
            
            loop_duration = 0
            if hasattr(self, '_loop_start_time') and self._loop_start_time:
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
            
            self._update_execution_buttons(running=False)
            self._reset_execution_state()
            
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
    
    def _on_action_selected(self, index):
        if index >= 0:
            item = self.action_list.item(index)
            action_type = item.data(Qt.UserRole)
            if action_type in self.config_widgets:
                self.config_stack.setCurrentWidget(self.config_widgets[action_type])
    
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
        
        config_widget = self.config_widgets.get(action_type)
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
            
            if step_id in self.breakpoints:
                del self.breakpoints[step_id]
            
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
            
            if action_type in self.config_widgets:
                self.config_stack.setCurrentWidget(self.config_widgets[action_type])
                
                if step_id in self.step_configs:
                    self.config_widgets[action_type].set_config(self.step_configs[step_id])
    
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
    
    def _on_hotkey_settings(self):
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
        self._target_region = window.region
        self._window_mode = True
        app_logger.info(f"设置目标窗口: {window.title}", "UI")
    
    def _clear_target_window(self):
        """清除目标窗口"""
        self._target_region = None
        self._window_mode = False
    
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
        self._target_region = (x, y, w, h)
        self._window_mode = False
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
        show_toast("已清除识别区域", 'info')
    
    def _on_loop_settings(self):
        """循环执行设置"""
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
        
        info_label = QLabel("提示: 循环次数>1时，工作流将重复执行")
        info_label.setStyleSheet("color: #666; font-size: 11px;")
        layout.addWidget(info_label)
        
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)
        
        if dialog.exec_() == QDialog.Accepted:
            self._loop_count = loop_spin.value()
            self._loop_interval = interval_spin.value()
            
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
            QMessageBox.information(self, "内存状态", 
                f"物理内存 (RSS): {usage['rss']} MB\n"
                f"虚拟内存 (VMS): {usage['vms']} MB\n"
                f"缓存数量: {usage['cache_size']}")
    
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
    app.setApplicationVersion('1.3.0')
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
