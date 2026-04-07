"""
RabAI AutoClick v22 - 增强版主窗口
整合原始项目的执行引擎和v22的高级功能
"""

import sys
import os
import platform
import time
import copy
import logging
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
    QMenu, QAction, QToolBar, QStatusBar, QCheckBox, QFrame,
    QScrollArea, QGridLayout, QTreeWidget, QTreeWidgetItem
)
from PyQt5.QtCore import Qt, pyqtSignal, QTimer, QThread, QObject
from PyQt5.QtGui import QIcon, QColor, QFont, QCursor

IS_MACOS = platform.system() == 'Darwin'
IS_WINDOWS = platform.system() == 'Windows'

logger = logging.getLogger(__name__)

from core.engine import FlowEngine
from core.base_action import ActionResult
from utils.hotkey import HotkeyManager
from utils.app_logger import app_logger
from utils.memory import memory_manager, image_cache
from utils.recording import RecordingManager, RecordingEditor, RecordedAction, PYNPUT_AVAILABLE, check_pynput_permission
if IS_MACOS and PYNPUT_AVAILABLE:
    from utils.recording_mac import MacRecordingManager, MacPermissionChecker, create_recording_manager, check_recording_permission, request_recording_permission
from utils.history import WorkflowHistoryManager, HistoryDialog, QuickSaveDialog
from utils.key_display import key_display_window
from utils.execution_stats import execution_stats
from ui.hotkey_dialog import HotkeySettingsDialog
from ui.region_selector import RegionSelector, PositionSelector
from ui.message import message_manager, show_error, show_success, show_warning, show_toast
from ui.stats_dialog import StatsDialog
from ui.theme import theme_manager

from src.predictive_engine import PredictiveAutomationEngine, create_predictive_engine

import contextlib


@contextlib.contextmanager
def batch_updates(widget):
    """Context manager to batch UI updates for performance."""
    widget.setUpdatesEnabled(False)
    try:
        yield
    finally:
        widget.setUpdatesEnabled(True)
        widget.update()
from src.self_healing_system import SelfHealingSystem, create_self_healing_system
from src.workflow_diagnostics import WorkflowDiagnosticsV2, create_diagnostics, HealthLevel
from src.workflow_share import WorkflowShareSystem, create_share_system, ShareType
from src.pipeline_mode import PipelineRunner, create_pipeline_runner, PipeMode
from src.screen_recorder import ScreenRecorderConverter, create_screen_recorder


def show_question(title: str, message: str) -> bool:
    reply = QMessageBox.question(None, title, message,
                                  QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
    return reply == QMessageBox.Yes


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
    'click_count': '点击次数: 1=单击, 2=双击',
    'double_click': '是否双击。勾选则执行双击操作',
    'button': '鼠标按键: left=左键, right=右键, middle=中键',
    'relative': '是否相对坐标。勾选后x,y为相对于当前位置的偏移',
    'enter_after': '输入后是否按回车键。勾选则输入完成后自动按回车',
    'find_all': '是否查找所有匹配。勾选返回所有匹配位置',
    'script': 'Python脚本代码，可使用context变量',
    'command': '系统命令，如打开程序、执行批处理',
    'app_path': '应用程序完整路径，如: C:\\Program Files\\app.exe',
    'url': '网页地址，会自动用浏览器打开',
    'wait_time': '等待时间，单位: 秒',
    'var_name': '变量名，用于存储或读取变量',
    'value': '变量值或表达式',
    'filename': '文件名或路径',
    'content': '文件内容或文本内容',
    'seconds': '等待秒数',
    'milliseconds': '等待毫秒数',
    'name': '变量名称，用于设置或获取变量值',
    'value_type': '变量值类型: string(字符串)、int(整数)、float(小数)、bool(布尔)、expression(表达式)',
    'condition': '条件表达式，如: ${var} > 10 或 ${var} == "success"',
    'true_next': '条件为真时跳转的步骤ID',
    'false_next': '条件为假时跳转的步骤ID',
    'loop_id': '循环标识符，用于区分不同的循环',
    'loop_start': '循环开始时跳转的步骤ID',
    'loop_end': '循环结束时跳转的步骤ID',
    'click_offset': '点击偏移量，格式: x,y。相对于匹配中心点的偏移',
    'offset_x': 'X轴偏移量，正数向右，负数向左',
    'offset_y': 'Y轴偏移量，正数向下，负数向上',
    'click_center': '是否点击中心点。勾选则点击匹配区域的中心',
    'output_var': '输出变量名，将结果保存到指定变量',
    'pre_delay': '前置延时，执行动作前的等待时间（秒）',
    'post_delay': '后置延时，执行动作后的等待时间（秒）',
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
            
            coord_params = []
            other_params = []
            for param in required_params:
                if param in ('x', 'y', 'start_x', 'start_y', 'end_x', 'end_y'):
                    coord_params.append(param)
                else:
                    other_params.append(param)
            
            if coord_params:
                coord_group = ['x', 'y']
                start_group = ['start_x', 'start_y']
                end_group = ['end_x', 'end_y']
                
                if 'x' in coord_params or 'y' in coord_params:
                    coord_widget = self._create_coord_widget(['x', 'y'], required_params)
                    required_layout.addRow("坐标:", coord_widget)
                
                if 'start_x' in coord_params or 'start_y' in coord_params:
                    start_widget = self._create_coord_widget(['start_x', 'start_y'], required_params)
                    required_layout.addRow("起始坐标:", start_widget)
                
                if 'end_x' in coord_params or 'end_y' in coord_params:
                    end_widget = self._create_coord_widget(['end_x', 'end_y'], required_params)
                    required_layout.addRow("结束坐标:", end_widget)
            
            for param in other_params:
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
                    help_label.setStyleSheet(f"color: {colors['text_secondary']}; font-size: 10px;")
                    help_label.setWordWrap(True)
                    row_layout.addWidget(help_label)

                required_layout.addRow(f"{param}:", row_widget)
            
            required_group.setLayout(required_layout)
            layout.addWidget(required_group)
        
        if optional_params:
            optional_group = QGroupBox("可选参数")
            optional_layout = QFormLayout()
            optional_layout.setSpacing(5)
            
            coord_param_groups = [
                (['x', 'y'], "坐标"),
                (['start_x', 'start_y'], "起始坐标"),
                (['end_x', 'end_y'], "结束坐标")
            ]
            
            processed_params = set()
            
            for param_group, label in coord_param_groups:
                if all(p in optional_params for p in param_group):
                    coord_widget = self._create_coord_widget_optional(param_group, optional_params)
                    optional_layout.addRow(f"{label}:", coord_widget)
                    processed_params.update(param_group)
                elif any(p in optional_params for p in param_group):
                    for p in param_group:
                        if p in optional_params and p not in processed_params:
                            widget = self._create_param_widget(p, optional_params[p])
                            self.widgets[p] = widget
                            
                            row_widget = QWidget()
                            row_layout = QVBoxLayout(row_widget)
                            row_layout.setContentsMargins(0, 0, 0, 0)
                            row_layout.setSpacing(2)
                            row_layout.addWidget(widget)
                            
                            desc = PARAM_DESCRIPTIONS.get(p, '')
                            if desc:
                                help_label = QLabel(desc)
                                help_label.setStyleSheet(f"color: {colors['text_secondary']}; font-size: 10px;")
                                help_label.setWordWrap(True)
                                row_layout.addWidget(help_label)
                            
                            optional_layout.addRow(f"{p}:", row_widget)
                            processed_params.add(p)
            
            for param, default_value in optional_params.items():
                if param in processed_params:
                    continue
                    
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
                    help_label.setStyleSheet(f"color: {colors['text_secondary']}; font-size: 10px;")
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
        
        output_layout = QHBoxLayout()
        self.output_var = QComboBox()
        self.output_var.setEditable(True)
        self.output_var.setPlaceholderText("可选，保存结果的变量名")
        self.output_var.lineEdit().textChanged.connect(lambda: self.config_changed.emit())
        output_layout.addWidget(self.output_var)
        
        refresh_btn = QPushButton("🔄")
        refresh_btn.setFixedWidth(30)
        refresh_btn.setToolTip("刷新变量列表")
        refresh_btn.clicked.connect(self._refresh_output_variables)
        output_layout.addWidget(refresh_btn)
        
        common_layout.addRow("输出变量:", output_layout)
        
        common_group.setLayout(common_layout)
        layout.addWidget(common_group)
        
        layout.addStretch()
    
    def _refresh_output_variables(self):
        if hasattr(self.parent(), 'variables_widget'):
            var_names = self.parent().variables_widget.get_variable_names()
            current_text = self.output_var.currentText()
            self.output_var.clear()
            self.output_var.addItem("")
            for name in var_names:
                self.output_var.addItem(name)
            idx = self.output_var.findText(current_text)
            if idx >= 0:
                self.output_var.setCurrentIndex(idx)
    
    def _create_coord_widget(self, params: list, required_params: list) -> QWidget:
        container = QWidget()
        h_layout = QHBoxLayout(container)
        h_layout.setContentsMargins(0, 0, 0, 0)
        
        labels = []
        for i, param in enumerate(params):
            if param in required_params:
                label = QLabel(f"{param.split('_')[-1].upper()}:")
                spin = QSpinBox()
                spin.setRange(0, 9999)
                spin.setValue(0)
                spin.valueChanged.connect(self.config_changed.emit)
                spin.setMaximumWidth(80)
                
                self.widgets[f"{param}_spin"] = spin
                
                h_layout.addWidget(label)
                h_layout.addWidget(spin)
                
                if i < len(params) - 1:
                    h_layout.addSpacing(10)
        
        pick_btn = QPushButton("选取位置")
        pick_btn.clicked.connect(lambda: self._pick_coords(params))
        h_layout.addWidget(pick_btn)
        
        import_btn = QPushButton("变量")
        import_btn.setToolTip("从变量导入坐标")
        import_btn.clicked.connect(lambda: self._import_coord_variable(params))
        h_layout.addWidget(import_btn)
        
        h_layout.addStretch()
        return container
    
    def _create_coord_widget_optional(self, params: list, optional_params: dict) -> QWidget:
        container = QWidget()
        h_layout = QHBoxLayout(container)
        h_layout.setContentsMargins(0, 0, 0, 0)
        
        for i, param in enumerate(params):
            label = QLabel(f"{param.split('_')[-1].upper()}:")
            spin = QSpinBox()
            spin.setRange(0, 9999)
            default_val = optional_params.get(param, 0)
            spin.setValue(int(default_val) if default_val else 0)
            spin.valueChanged.connect(self.config_changed.emit)
            spin.setMaximumWidth(80)
            
            self.widgets[f"{param}_spin"] = spin
            
            h_layout.addWidget(label)
            h_layout.addWidget(spin)
            
            if i < len(params) - 1:
                h_layout.addSpacing(10)
        
        pick_btn = QPushButton("选取位置")
        pick_btn.clicked.connect(lambda: self._pick_coords(params))
        h_layout.addWidget(pick_btn)
        
        import_btn = QPushButton("变量")
        import_btn.setToolTip("从变量导入坐标")
        import_btn.clicked.connect(lambda: self._import_coord_variable(params))
        h_layout.addWidget(import_btn)
        
        h_layout.addStretch()
        return container
    
    def _import_coord_variable(self, params: list):
        from PyQt5.QtWidgets import QInputDialog
        
        if hasattr(self.parent(), 'variables_widget'):
            var_names = self.parent().variables_widget.get_variable_names()
            coord_vars = [v for v in var_names]
            
            if not coord_vars:
                show_toast("没有可用的变量", 'warning')
                return
            
            var_name, ok = QInputDialog.getItem(self, "导入变量", "选择变量:", coord_vars, 0, False)
            if ok and var_name:
                variables = self.parent().variables_widget.get_variables()
                var_data = variables.get(var_name, {})
                var_type = var_data.get('type', 'string')
                
                if var_type == 'coordinate':
                    value = var_data.get('default_value', (0, 0))
                    if isinstance(value, (tuple, list)) and len(value) >= 2:
                        if len(params) >= 1 and f"{params[0]}_spin" in self.widgets:
                            self.widgets[f"{params[0]}_spin"].setValue(int(value[0]))
                        if len(params) >= 2 and f"{params[1]}_spin" in self.widgets:
                            self.widgets[f"{params[1]}_spin"].setValue(int(value[1]))
                        show_toast(f"已导入变量: {var_name}", 'success')
                elif var_type == 'region':
                    value = var_data.get('default_value', (0, 0, 100, 100))
                    if isinstance(value, (tuple, list)) and len(value) >= 4:
                        for i, param in enumerate(params[:4]):
                            if f"{param}_spin" in self.widgets:
                                self.widgets[f"{param}_spin"].setValue(int(value[i]))
                        show_toast(f"已导入变量: {var_name}", 'success')
                else:
                    show_toast(f"变量类型不匹配，需要 coordinate 或 region 类型", 'warning')
    
    def _pick_coords(self, params: list):
        self.window().hide()
        QTimer.singleShot(200, lambda: self._do_pick_coords(params))
    
    def _do_pick_coords(self, params: list):
        try:
            self._coord_selector = RegionSelector(mode='center')
            
            def on_selected(x, y):
                for param in params:
                    spin = self.widgets.get(f"{param}_spin")
                    if spin:
                        value = x if 'x' in param else y
                        spin.setValue(value)
                self.window().show()
                show_toast(f"已选择位置: ({x}, {y})", 'success')
            
            def on_cancelled():
                self.window().show()
            
            self._coord_selector.position_selected.connect(on_selected)
            self._coord_selector.cancelled.connect(on_cancelled)
        except Exception as e:
            self.window().show()
            show_error("错误", f"创建位置选择器失败: {str(e)}")
    
    def _browse_file(self, line_edit: QLineEdit, param: str):
        if 'save' in param.lower() or 'output' in param.lower():
            default_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'screenshots')
            try:
                os.makedirs(default_dir, exist_ok=True)
            except PermissionError:
                default_dir = os.path.dirname(os.path.dirname(__file__))
            
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            default_name = f'screenshot_{timestamp}.png'
            
            filepath, _ = QFileDialog.getSaveFileName(
                self, "保存文件", 
                os.path.join(default_dir, default_name),
                "PNG图片 (*.png);;JPEG图片 (*.jpg);;所有文件 (*.*)"
            )
        else:
            filepath, _ = QFileDialog.getOpenFileName(self, "选择文件")
        
        if filepath:
            line_edit.setText(filepath)
    
    def _browse_folder(self, line_edit: QLineEdit):
        default_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'screenshots')
        try:
            os.makedirs(default_dir, exist_ok=True)
        except PermissionError:
            default_dir = os.path.dirname(os.path.dirname(__file__))
        
        folder = QFileDialog.getExistingDirectory(self, "选择文件夹", default_dir)
        if folder:
            line_edit.setText(folder)
    
    def _browse_image(self, line_edit: QLineEdit):
        default_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'templates')
        try:
            os.makedirs(default_dir, exist_ok=True)
        except PermissionError:
            default_dir = os.path.dirname(os.path.dirname(__file__))
        
        filepath, _ = QFileDialog.getOpenFileName(
            self, "选择图片", 
            default_dir,
            "图片文件 (*.png *.jpg *.jpeg *.bmp *.gif);;PNG图片 (*.png);;JPEG图片 (*.jpg *.jpeg);;所有文件 (*.*)"
        )
        if filepath:
            line_edit.setText(filepath)
    
    def _create_param_widget(self, param: str, default_value: Any) -> QWidget:
        if param in ('x', 'y', 'start_x', 'start_y', 'end_x', 'end_y'):
            container = QWidget()
            h_layout = QHBoxLayout(container)
            h_layout.setContentsMargins(0, 0, 0, 0)
            
            spin = QSpinBox()
            spin.setRange(0, 9999)
            spin.setValue(int(default_value) if default_value else 0)
            spin.valueChanged.connect(self.config_changed.emit)
            
            h_layout.addWidget(spin)
            
            pick_btn = QPushButton("选取")
            pick_btn.setMaximumWidth(40)
            pick_btn.clicked.connect(lambda checked, p=param, s=spin: self._pick_position(p, s))
            
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
        
        if param in ('save_path', 'filepath', 'filename'):
            container = QWidget()
            h_layout = QHBoxLayout(container)
            h_layout.setContentsMargins(0, 0, 0, 0)
            
            line_edit = QLineEdit()
            if default_value:
                line_edit.setText(str(default_value))
            else:
                default_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'screenshots')
                line_edit.setPlaceholderText(f"默认: {default_dir}")
            line_edit.textChanged.connect(self.config_changed.emit)
            
            browse_btn = QPushButton("浏览")
            browse_btn.setMaximumWidth(50)
            browse_btn.clicked.connect(lambda: self._browse_file(line_edit, param))
            
            h_layout.addWidget(line_edit)
            h_layout.addWidget(browse_btn)
            
            self.widgets[f"{param}_edit"] = line_edit
            return container
        
        if param in ('template', 'image_path', 'image'):
            container = QWidget()
            h_layout = QHBoxLayout(container)
            h_layout.setContentsMargins(0, 0, 0, 0)
            
            line_edit = QLineEdit()
            if default_value:
                line_edit.setText(str(default_value))
            line_edit.setPlaceholderText("选择图片文件...")
            line_edit.textChanged.connect(self.config_changed.emit)
            
            browse_btn = QPushButton("选择图片")
            browse_btn.setMaximumWidth(70)
            browse_btn.clicked.connect(lambda: self._browse_image(line_edit))
            
            h_layout.addWidget(line_edit)
            h_layout.addWidget(browse_btn)
            
            self.widgets[f"{param}_edit"] = line_edit
            return container
        
        if param in ('folder', 'directory', 'output_dir'):
            container = QWidget()
            h_layout = QHBoxLayout(container)
            h_layout.setContentsMargins(0, 0, 0, 0)
            
            line_edit = QLineEdit()
            if default_value:
                line_edit.setText(str(default_value))
            else:
                default_dir = os.path.join(os.path.expanduser('~'), 'Pictures', 'rabai_screenshots')
                line_edit.setPlaceholderText(f"默认: {default_dir}")
            line_edit.textChanged.connect(self.config_changed.emit)
            
            browse_btn = QPushButton("浏览")
            browse_btn.setMaximumWidth(50)
            browse_btn.clicked.connect(lambda: self._browse_folder(line_edit))
            
            h_layout.addWidget(line_edit)
            h_layout.addWidget(browse_btn)
            
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
        elif param == 'click_count':
            widget = QComboBox()
            widget.addItem("单击", 1)
            widget.addItem("双击", 2)
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
    
    def _pick_region(self, line_edit: QLineEdit):
        self.window().hide()
        QTimer.singleShot(200, lambda: self._do_pick_region(line_edit))
    
    def _do_pick_region(self, line_edit: QLineEdit):
        try:
            self._region_selector = RegionSelector(mode='region')
            
            def on_selected(x, y, w, h):
                line_edit.setText(f"{x},{y},{w},{h}")
                self.window().show()
                show_toast(f"已选择区域: ({x}, {y}, {w}, {h})", 'success')
            
            def on_cancelled():
                self.window().show()
            
            self._region_selector.region_selected.connect(on_selected)
            self._region_selector.cancelled.connect(on_cancelled)
        except Exception as e:
            self.window().show()
            show_error("错误", f"创建区域选择器失败: {str(e)}")
    
    def _pick_position(self, param: str, spin: QSpinBox):
        self.window().hide()
        QTimer.singleShot(200, lambda: self._do_pick_position(param, spin))
    
    def _do_pick_position(self, param: str, spin: QSpinBox):
        try:
            from ui.region_selector import PositionSelector
            self._position_selector = PositionSelector()
            
            def on_selected(x, y):
                if param in ('x', 'start_x', 'end_x'):
                    spin.setValue(x)
                elif param in ('y', 'start_y', 'end_y'):
                    spin.setValue(y)
                self.window().show()
                self.window().activateWindow()
                show_toast(f"已选择位置: ({x}, {y})", 'success')
            
            def on_cancelled():
                self.window().show()
                self.window().activateWindow()
            
            self._position_selector.position_selected.connect(on_selected)
            self._position_selector.cancelled.connect(on_cancelled)
        except Exception as e:
            self.window().show()
            show_error("错误", f"创建位置选择器失败: {str(e)}")
    
    def get_config(self) -> Dict[str, Any]:
        config = {}
        
        coord_params = ['x', 'y', 'start_x', 'start_y', 'end_x', 'end_y']
        for param in coord_params:
            spin = self.widgets.get(f"{param}_spin")
            if spin:
                config[param] = spin.value()
        
        edit_params = ['template', 'image_path', 'image', 'save_path', 'filepath', 'filename', 'region', 'folder', 'directory', 'output_dir']
        for param in edit_params:
            edit = self.widgets.get(f"{param}_edit")
            if edit:
                text = edit.text().strip()
                if text:
                    config[param] = text
        
        for param, widget in self.widgets.items():
            if param.endswith('_spin') or param.endswith('_edit'):
                continue
            
            if param in coord_params or param in edit_params:
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
        
        output_var = self.output_var.currentText().strip()
        if output_var:
            config['output_var'] = output_var
        
        return config
    
    def set_config(self, config: Dict[str, Any]) -> None:
        coord_params = ['x', 'y', 'start_x', 'start_y', 'end_x', 'end_y']
        edit_params = ['template', 'image_path', 'image', 'save_path', 'filepath', 'filename', 'region', 'folder', 'directory', 'output_dir']
        
        for param in coord_params:
            if param in config and config[param] is not None:
                spin = self.widgets.get(f"{param}_spin")
                if spin:
                    spin.blockSignals(True)
                    spin.setValue(int(config[param]))
                    spin.blockSignals(False)
        
        for param in edit_params:
            if param in config:
                edit = self.widgets.get(f"{param}_edit")
                if edit:
                    edit.blockSignals(True)
                    edit.setText(str(config[param]) if config[param] else '')
                    edit.blockSignals(False)
        
        for param, widget in self.widgets.items():
            if param.endswith('_spin') or param.endswith('_edit'):
                continue
            
            if param in coord_params or param in edit_params:
                continue
            
            if param in config:
                value = config[param]
                
                widget.blockSignals(True)
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
                widget.blockSignals(False)
        
        if 'pre_delay' in config:
            self.pre_delay.blockSignals(True)
            self.pre_delay.setValue(config['pre_delay'])
            self.pre_delay.blockSignals(False)
        if 'post_delay' in config:
            self.post_delay.blockSignals(True)
            self.post_delay.setValue(config['post_delay'])
            self.post_delay.blockSignals(False)
        if 'output_var' in config:
            self.output_var.blockSignals(True)
            idx = self.output_var.findText(config['output_var'])
            if idx >= 0:
                self.output_var.setCurrentIndex(idx)
            else:
                self.output_var.setEditText(config['output_var'])
            self.output_var.blockSignals(False)


class StepListWidget(QWidget):
    step_selected = pyqtSignal(int)
    step_moved = pyqtSignal(int, int)
    steps_cleared = pyqtSignal()
    
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
        self.clear_btn = QPushButton("清空")
        colors = theme_manager.colors
        self.clear_btn.setStyleSheet(f"background-color: {colors['warning']}; color: white;")
        self.up_btn = QPushButton("↑")
        self.down_btn = QPushButton("↓")

        self.up_btn.setMaximumWidth(40)
        self.down_btn.setMaximumWidth(40)

        btn_layout.addWidget(self.add_btn)
        btn_layout.addWidget(self.remove_btn)
        btn_layout.addWidget(self.clear_btn)
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
    variables_changed = pyqtSignal()
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._variables: Dict[str, Dict] = {}
        self._init_ui()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["变量名", "类型", "默认值", "描述"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)
        layout.addWidget(self.table)
        
        btn_layout = QHBoxLayout()
        self.add_btn = QPushButton("添加变量")
        self.remove_btn = QPushButton("删除变量")
        self.import_btn = QPushButton("导入变量")
        btn_layout.addWidget(self.add_btn)
        btn_layout.addWidget(self.remove_btn)
        btn_layout.addWidget(self.import_btn)
        layout.addLayout(btn_layout)
        
        self.add_btn.clicked.connect(self._add_variable)
        self.remove_btn.clicked.connect(self._remove_variable)
        self.import_btn.clicked.connect(self._import_variable)
    
    def _add_variable(self):
        row = self.table.rowCount()
        self.table.insertRow(row)
        
        name_edit = QTableWidgetItem("")
        self.table.setItem(row, 0, name_edit)
        
        type_combo = QComboBox()
        type_combo.addItems(["string", "integer", "float", "boolean", "coordinate", "region", "list", "dict"])
        self.table.setCellWidget(row, 1, type_combo)
        
        value_edit = QTableWidgetItem("")
        self.table.setItem(row, 2, value_edit)
        
        desc_edit = QTableWidgetItem("")
        self.table.setItem(row, 3, desc_edit)
    
    def _remove_variable(self):
        current_row = self.table.currentRow()
        if current_row >= 0:
            self.table.removeRow(current_row)
            self.variables_changed.emit()
    
    def _import_variable(self):
        from PyQt5.QtWidgets import QInputDialog
        var_names = self.get_variable_names()
        if not var_names:
            show_toast("没有可用的变量", 'warning')
            return
        
        var_name, ok = QInputDialog.getItem(self, "导入变量", "选择变量:", var_names, 0, False)
        if ok and var_name:
            show_toast(f"已选择变量: ${{{var_name}}}", 'info')
    
    def get_variable_names(self) -> List[str]:
        names = []
        for row in range(self.table.rowCount()):
            name_item = self.table.item(row, 0)
            if name_item and name_item.text().strip():
                names.append(name_item.text().strip())
        return names
    
    def get_variables(self) -> Dict[str, Any]:
        variables = {}
        for row in range(self.table.rowCount()):
            name_item = self.table.item(row, 0)
            type_widget = self.table.cellWidget(row, 1)
            value_item = self.table.item(row, 2)
            desc_item = self.table.item(row, 3)
            
            if name_item:
                name = name_item.text().strip()
                if name:
                    var_type = type_widget.currentText() if type_widget else "string"
                    value_str = value_item.text() if value_item else ""
                    description = desc_item.text() if desc_item else ""
                    
                    try:
                        if var_type == "integer":
                            value = int(value_str) if value_str else 0
                        elif var_type == "float":
                            value = float(value_str) if value_str else 0.0
                        elif var_type == "boolean":
                            value = value_str.lower() in ('true', '1', 'yes')
                        elif var_type == "coordinate":
                            parts = value_str.replace('(', '').replace(')', '').split(',')
                            value = tuple(int(p.strip()) for p in parts[:2]) if len(parts) >= 2 else (0, 0)
                        elif var_type == "region":
                            parts = value_str.replace('(', '').replace(')', '').split(',')
                            value = tuple(int(p.strip()) for p in parts[:4]) if len(parts) >= 4 else (0, 0, 100, 100)
                        elif var_type in ("list", "dict"):
                            value = json.loads(value_str) if value_str else ([] if var_type == "list" else {})
                        else:
                            value = value_str
                    except Exception as e:
                        app_logger.warning(f"解析变量值失败: {e}")
                        value = value_str
                    
                    variables[name] = {
                        'type': var_type,
                        'default_value': value,
                        'description': description
                    }
        
        return variables
    
    def set_variables(self, variables: Dict[str, Any]) -> None:
        with batch_updates(self.table):
            self.table.setRowCount(0)
            for name, var_data in variables.items():
                if isinstance(var_data, dict):
                    row = self.table.rowCount()
                    self.table.insertRow(row)
                    self.table.setItem(row, 0, QTableWidgetItem(name))

                    type_combo = QComboBox()
                    type_combo.addItems(["string", "integer", "float", "boolean", "coordinate", "region", "list", "dict"])
                    idx = type_combo.findText(var_data.get('type', 'string'))
                    type_combo.setCurrentIndex(idx if idx >= 0 else 0)
                    self.table.setCellWidget(row, 1, type_combo)

                    default_val = var_data.get('default_value', '')
                    if isinstance(default_val, (tuple, list)):
                        default_val = str(default_val)
                    elif isinstance(default_val, (dict, list)):
                        default_val = json.dumps(default_val, ensure_ascii=False)
                    else:
                        default_val = str(default_val)
                    self.table.setItem(row, 2, QTableWidgetItem(default_val))
                    self.table.setItem(row, 3, QTableWidgetItem(var_data.get('description', '')))
                else:
                    row = self.table.rowCount()
                    self.table.insertRow(row)
                    self.table.setItem(row, 0, QTableWidgetItem(name))

                    type_combo = QComboBox()
                    type_combo.addItems(["string", "integer", "float", "boolean", "coordinate", "region", "list", "dict"])
                    self.table.setCellWidget(row, 1, type_combo)

                    self.table.setItem(row, 2, QTableWidgetItem(str(var_data)))
                    self.table.setItem(row, 3, QTableWidgetItem(""))


class LogWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._log_colors = theme_manager.get_log_colors()
        theme_manager.theme_changed.connect(self._on_theme_changed)
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)
        colors = theme_manager.colors
        self.text_edit.setStyleSheet(f"""
            QTextEdit {{
                background-color: {colors['status_bar']};
                color: {colors['status_text']};
                font-family: Consolas, 'Microsoft YaHei';
                font-size: 12px;
            }}
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

    def _on_theme_changed(self, theme):
        """Handle theme change to update log colors."""
        self._log_colors = theme_manager.get_log_colors()

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
    """录屏功能面板 - 已优化 Mac 系统稳定性
    
    Mac 专用优化：
    - 使用独立进程隔离 pynput，避免与 Qt 线程冲突
    - 自动恢复监听器
    - 更可靠的权限检查
    """
    
    action_added = pyqtSignal(str)
    list_cleared = pyqtSignal()
    workflow_added = pyqtSignal(list)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._use_mac_manager = IS_MACOS and PYNPUT_AVAILABLE
        
        if self._use_mac_manager:
            self._recording_manager = MacRecordingManager()
        else:
            self._recording_manager = RecordingManager()
        
        self._main_window = None
        self._init_ui()
        self._connect_signals()
        
        self.action_added.connect(self._do_add_item)
        self.list_cleared.connect(self._do_clear_list)
    
    def set_main_window(self, window):
        self._main_window = window
    
    def _do_add_item(self, text: str):
        self.action_list.addItem(text)
    
    def _do_clear_list(self):
        self.action_list.clear()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        colors = theme_manager.colors

        if self._use_mac_manager:
            info_label = QLabel("录屏功能：录制鼠标和键盘操作，自动生成工作流步骤\n✅ 已启用 Mac 进程隔离模式")
            info_label.setStyleSheet(f"color: {colors['primary']}; font-size: 11px;")
        else:
            info_label = QLabel("录屏功能：录制鼠标和键盘操作，自动生成工作流步骤")
            info_label.setStyleSheet("color: gray; font-size: 11px;")
        layout.addWidget(info_label)

        btn_layout = QHBoxLayout()

        self.record_btn = QPushButton("🔴 开始录制")
        self.record_btn.setStyleSheet(f"background-color: {colors['error']}; color: white; font-weight: bold;")
        self.stop_btn = QPushButton("⏹ 停止录制")
        self.stop_btn.setEnabled(False)
        self.clear_btn = QPushButton("清空")
        self.optimize_btn = QPushButton("🔧 优化")
        self.optimize_btn.setToolTip("优化录制的操作：\n1. 合并连续的文本输入\n2. 移除过短的延时\n3. 整理按键组合顺序")
        self.add_to_workflow_btn = QPushButton("添加到工作流")
        self.add_to_workflow_btn.setStyleSheet(f"background-color: {colors['success']}; color: white; font-weight: bold;")

        btn_layout.addWidget(self.record_btn)
        btn_layout.addWidget(self.stop_btn)
        btn_layout.addWidget(self.clear_btn)
        btn_layout.addWidget(self.optimize_btn)
        btn_layout.addWidget(self.add_to_workflow_btn)
        layout.addLayout(btn_layout)
        
        self.action_list = QListWidget()
        layout.addWidget(self.action_list)
        
        self.status_label = QLabel("状态: 就绪")
        layout.addWidget(self.status_label)
        
        help_text = (
            "📌 优化功能说明：\n"
            "• 合并连续的文本输入为一个完整字符串\n"
            "• 移除过短(<0.1秒)的延时\n"
            "• 整理快捷键顺序(如 cmd+c)\n\n"
            "💡 提示：录制时主窗口会自动最小化，方便操作"
        )
        help_label = QLabel(help_text)
        help_label.setStyleSheet(f"color: {colors['text_secondary']}; font-size: 10px;")
        layout.addWidget(help_label)
        
        if not PYNPUT_AVAILABLE:
            self.record_btn.setEnabled(False)
            self.status_label.setText("状态: pynput未安装，录屏功能不可用")
            self.status_label.setStyleSheet("color: red;")
    
    def _connect_signals(self):
        self.record_btn.clicked.connect(self._on_record)
        self.stop_btn.clicked.connect(self._on_stop)
        self.clear_btn.clicked.connect(self._on_clear)
        self.optimize_btn.clicked.connect(self._on_optimize)
        self.add_to_workflow_btn.clicked.connect(self._on_add_to_workflow)
        
        self._recording_manager.action_recorded.connect(self._on_action_recorded)
        self._recording_manager.recording_started.connect(self._on_recording_started)
        self._recording_manager.recording_stopped.connect(self._on_recording_stopped)
        
        if self._use_mac_manager:
            self._recording_manager.recording_error.connect(self._on_recording_error)
    
    def _on_record(self):
        try:
            if not PYNPUT_AVAILABLE:
                show_error("录制失败", "pynput模块未安装，请运行: pip install pynput")
                return
            
            if self._use_mac_manager:
                if not self._recording_manager.check_permission():
                    request_recording_permission(self)
                    return
            else:
                if not check_pynput_permission():
                    show_error("录制失败", "请先授权辅助功能权限：\n系统偏好设置 → 安全性与隐私 → 隐私 → 辅助功能\n添加终端或Python到列表")
                    return
            
            if self._recording_manager.start_recording():
                self.record_btn.setEnabled(False)
                self.stop_btn.setEnabled(True)
                self.status_label.setText("状态: 录制中...")
                
                if self._main_window:
                    self._main_window.showMinimized()
                
                show_toast("开始录制操作 - 主窗口已最小化", 'info')
            else:
                show_error("录制失败", "无法启动录制")
        except Exception as e:
            show_error("录制异常", f"启动录制时发生错误: {str(e)}")
    
    def _on_stop(self):
        actions = self._recording_manager.stop_recording()
        self.record_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.status_label.setText(f"状态: 已录制 {len(actions)} 个操作")
        
        if self._main_window:
            self._main_window.showNormal()
            self._main_window.activateWindow()
        
        show_toast(f"录制完成，共 {len(actions)} 个操作", 'success')
    
    def _on_clear(self):
        self._recording_manager.clear_actions()
        self.action_list.clear()
        self.status_label.setText("状态: 就绪")
    
    def _on_optimize(self):
        actions = self._recording_manager.get_actions()
        if not actions:
            show_warning("提示", "没有可优化的操作，请先录制")
            return
        
        if self._use_mac_manager:
            merged = self._merge_consecutive_actions(actions)
            optimized = self._optimize_delays(actions)
            self._fix_hotkey_order(actions)
            self._refresh_action_list()
            show_toast(f"优化完成：合并 {merged} 个文本，优化 {optimized} 个延时", 'success')
        else:
            editor = RecordingEditor(actions)
            merged = editor.merge_consecutive_types()
            optimized = editor.optimize_delays()
            self._recording_manager._actions = editor.get_actions()
            self._refresh_action_list()
            show_toast(f"优化完成：合并 {merged} 个，优化 {optimized} 个延时", 'success')
    
    def _fix_hotkey_order(self, actions: List) -> int:
        fixed = 0
        modifier_order = ['ctrl', 'cmd', 'alt', 'option', 'shift']
        
        for action in actions:
            if action.get('action_type') == 'hotkey':
                keys_str = action.get('params', {}).get('keys', '')
                if isinstance(keys_str, str) and '+' in keys_str:
                    keys_list = keys_str.split('+')
                    modifiers = [k for k in keys_list if k in modifier_order]
                    non_modifiers = [k for k in keys_list if k not in modifier_order]
                    
                    if modifiers and non_modifiers:
                        sorted_modifiers = sorted(modifiers, key=lambda x: modifier_order.index(x) if x in modifier_order else 999)
                        new_order = sorted_modifiers + non_modifiers
                        new_keys = '+'.join(new_order)
                        if new_keys != keys_str:
                            action['params']['keys'] = new_keys
                            fixed += 1
        
        return fixed
    
    def _merge_consecutive_actions(self, actions: List) -> int:
        if not actions:
            return 0
        merged = 0
        i = 0
        while i < len(actions) - 1:
            current = actions[i]
            next_action = actions[i + 1]
            if current.get('action_type') == 'type_text' and next_action.get('action_type') == 'type_text':
                current['params']['text'] += next_action['params'].get('text', '')
                del actions[i + 1]
                merged += 1
            else:
                i += 1
        return merged
    
    def _optimize_delays(self, actions: List, min_delay: float = 0.1) -> int:
        optimized = 0
        for action in actions:
            if 'pre_delay' in action.get('params', {}) and action['params']['pre_delay'] < min_delay:
                del action['params']['pre_delay']
                optimized += 1
        return optimized
    
    def _on_add_to_workflow(self):
        actions = self._recording_manager.get_actions()
        if not actions:
            show_warning("提示", "没有可添加的操作，请先录制")
            return
        
        workflow = self._recording_manager.to_workflow()
        steps = workflow.get('steps', [])
        
        if not steps:
            show_warning("提示", "无法生成工作流步骤")
            return
        
        self.workflow_added.emit(steps)
        show_toast(f"已添加 {len(steps)} 个步骤到工作流", 'success')
    
    def _on_action_recorded(self, action_type: str, params: dict):
        self.action_added.emit(f"{action_type}: {params}")
        app_logger.info(f"录屏动作: {action_type} - {params}", "Recording")
    
    def _on_recording_started(self):
        self.list_cleared.emit()
        app_logger.info("开始录制操作", "Recording")
    
    def _on_recording_stopped(self, actions):
        app_logger.info(f"录制完成，共 {len(actions)} 个动作", "Recording")
    
    def _on_recording_error(self, error_msg: str):
        self.record_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.status_label.setText(f"状态: 错误 - {error_msg}")
        self.status_label.setStyleSheet("color: red;")
        show_error("录制错误", error_msg)
    
    def _refresh_action_list(self):
        with batch_updates(self.action_list):
            self.action_list.clear()
            actions = self._recording_manager.get_actions()
            for action in actions:
                if isinstance(action, dict):
                    self.action_list.addItem(f"{action.get('action_type', 'unknown')}: {action.get('params', {})}")
                else:
                    self.action_list.addItem(f"{action.action_type}: {action.params}")
    
    def get_workflow(self) -> Dict[str, Any]:
        return self._recording_manager.to_workflow()


class PredictiveWidget(QWidget):
    """预测性自动化面板"""
    
    action_triggered = pyqtSignal(str, dict)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.engine = create_predictive_engine("./data")
        self._workflows = {}
        self._init_ui()
    
    def set_workflows(self, workflows: Dict):
        self._workflows = workflows
        self._refresh_predictions()
    
    def record_action(self, action_type: str, target: str, result: str = "success"):
        self.engine.record_action(action_type, target, result=result)
        self._update_stats()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        colors = theme_manager.colors

        info_label = QLabel("🧠 预测性自动化引擎 - 基于历史行为预测下一步操作")
        info_label.setStyleSheet(f"font-weight: bold; font-size: 13px; color: {colors['primary']};")
        layout.addWidget(info_label)

        self.prediction_label = QLabel("暂无足够数据进行预测，请先执行一些工作流")
        self.prediction_label.setWordWrap(True)
        self.prediction_label.setStyleSheet(f"padding: 10px; background-color: {colors['bg_hover']}; border-radius: 5px;")
        layout.addWidget(self.prediction_label)
        
        self.alternatives_list = QListWidget()
        self.alternatives_list.setMaximumHeight(100)
        self.alternatives_list.itemDoubleClicked.connect(self._on_alternative_clicked)
        layout.addWidget(QLabel("备选建议 (双击执行):"))
        layout.addWidget(self.alternatives_list)
        
        btn_layout = QHBoxLayout()
        refresh_btn = QPushButton("🔄 刷新预测")
        refresh_btn.clicked.connect(self._refresh_predictions)
        btn_layout.addWidget(refresh_btn)
        
        analyze_btn = QPushButton("📊 行为分析")
        analyze_btn.clicked.connect(self._show_analysis)
        btn_layout.addWidget(analyze_btn)
        
        layout.addLayout(btn_layout)
        
        self.stats_label = QLabel("历史动作: 0 | 成功率: 0%")
        self.stats_label.setStyleSheet(f"color: {colors['text_secondary']}; font-size: 11px;")
        layout.addWidget(self.stats_label)
        
        layout.addStretch()
        self._update_stats()
    
    def _update_stats(self):
        analysis = self.engine.analyze_user_behavior()
        self.stats_label.setText(f"历史动作: {analysis.get('total_actions', 0)} | 成功率: {analysis.get('success_rate', 0):.0%}")
    
    def _refresh_predictions(self):
        prediction = self.engine.predict_next_action()

        if prediction:
            self.prediction_label.setText(
                f"🎯 预测动作: {prediction.predicted_action}\n"
                f"📈 置信度: {prediction.confidence:.0%}\n"
                f"💡 推理: {prediction.reasoning}"
            )

            with batch_updates(self.alternatives_list):
                self.alternatives_list.clear()
                for alt in prediction.alternatives:
                    self.alternatives_list.addItem(f"• {alt}")
        else:
            self.prediction_label.setText("暂无足够数据进行预测，请先执行一些工作流")
            self.alternatives_list.clear()

        self._update_stats()
    
    def _on_alternative_clicked(self, item):
        text = item.text().replace("• ", "")
        self.action_triggered.emit("suggested", {"action": text})
    
    def _show_analysis(self):
        analysis = self.engine.analyze_user_behavior()
        
        dialog = QDialog(self)
        dialog.setWindowTitle("用户行为分析")
        dialog.setMinimumSize(400, 300)
        
        layout = QVBoxLayout(dialog)
        
        text = QTextEdit()
        text.setReadOnly(True)
        
        action_dist = analysis.get('action_type_distribution', {})
        top_targets = list(analysis.get('top_targets', {}).items())[:5]
        
        text.setHtml(f"""
        <h3>📊 用户行为分析报告</h3>
        <p><b>总动作数:</b> {analysis.get('total_actions', 0)}</p>
        <p><b>最近动作:</b> {analysis.get('recent_actions', 0)}</p>
        <p><b>平均耗时:</b> {analysis.get('avg_duration', 0):.2f}秒</p>
        <p><b>成功率:</b> {analysis.get('success_rate', 0):.0%}</p>
        <h4>动作类型分布:</h4>
        <ul>
        {''.join(f"<li>{k}: {v}</li>" for k, v in action_dist.items()) if action_dist else "<li>暂无数据</li>"}
        </ul>
        <h4>最常用目标:</h4>
        <ul>
        {''.join(f"<li>{k}: {v}次</li>" for k, v in top_targets) if top_targets else "<li>暂无数据</li>"}
        </ul>
        """)
        layout.addWidget(text)
        
        btn = QPushButton("关闭")
        btn.clicked.connect(dialog.accept)
        layout.addWidget(btn)
        
        dialog.exec_()


class DiagnosticsWidget(QWidget):
    """工作流诊断面板"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.diagnostics = create_diagnostics("./data")
        self._workflows = {}
        self._init_ui()
    
    def set_workflows(self, workflows: Dict):
        self._workflows = workflows
        self._refresh_workflows()
    
    def record_execution(self, workflow_id: str, workflow_name: str, 
                        step_results: list, duration: float, success: bool, error: str = None):
        self.diagnostics.record_execution(workflow_id, workflow_name, step_results, duration, success, error)
        self._refresh_workflows()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        colors = theme_manager.colors

        info_label = QLabel("🏥 工作流健康诊断 - 分析工作流执行状态和问题")
        info_label.setStyleSheet(f"font-weight: bold; font-size: 13px; color: {colors['success']};")
        layout.addWidget(info_label)

        self.workflow_list = QListWidget()
        self.workflow_list.setMaximumHeight(150)
        layout.addWidget(QLabel("选择工作流进行诊断:"))
        layout.addWidget(self.workflow_list)

        self.result_text = QTextEdit()
        self.result_text.setReadOnly(True)
        self.result_text.setStyleSheet(f"font-family: Consolas; background-color: {colors['bg_widget']};")
        layout.addWidget(self.result_text)
        
        btn_layout = QHBoxLayout()
        diagnose_btn = QPushButton("🔍 诊断")
        diagnose_btn.clicked.connect(self._diagnose)
        btn_layout.addWidget(diagnose_btn)
        
        summary_btn = QPushButton("📋 健康概览")
        summary_btn.clicked.connect(self._show_summary)
        btn_layout.addWidget(summary_btn)
        
        refresh_btn = QPushButton("🔄 刷新")
        refresh_btn.clicked.connect(self._refresh_workflows)
        btn_layout.addWidget(refresh_btn)
        
        layout.addLayout(btn_layout)
    
    def _refresh_workflows(self):
        with batch_updates(self.workflow_list):
            self.workflow_list.clear()

            if not self.diagnostics.execution_history:
                self.workflow_list.addItem("暂无执行历史，请先运行工作流")
                return

            for wf_id in self.diagnostics.execution_history.keys():
                try:
                    report = self.diagnostics.diagnose(wf_id)
                    emoji = "🟢" if report.overall_health == HealthLevel.EXCELLENT else \
                            "🟡" if report.overall_health == HealthLevel.GOOD else \
                            "🟠" if report.overall_health == HealthLevel.FAIR else \
                            "🔴" if report.overall_health == HealthLevel.POOR else "⛔"
                    self.workflow_list.addItem(f"{emoji} {report.workflow_name} (分数: {report.health_score:.0f})")
                except Exception as e:
                    self.workflow_list.addItem(f"⛔ {wf_id} (诊断失败)")
    
    def _diagnose(self):
        current = self.workflow_list.currentRow()
        if current < 0:
            show_warning("提示", "请先选择一个工作流")
            return
        
        wf_ids = list(self.diagnostics.execution_history.keys())
        if current < len(wf_ids):
            try:
                report = self.diagnostics.diagnose(wf_ids[current])
                self.result_text.setText(self.diagnostics.generate_report_text(report))
            except Exception as e:
                self.result_text.setText(f"诊断失败: {str(e)}")
    
    def _show_summary(self):
        try:
            summary = self.diagnostics.get_health_summary()
            
            text = f"""
📊 工作流健康概览
{'='*40}

总工作流数: {summary.get('total_workflows', 0)}
平均健康分: {summary.get('avg_health_score', 0):.1f}
平均成功率: {summary.get('avg_success_rate', 0):.0%}
平均耗时: {summary.get('avg_duration', 0):.1f}秒

健康分布:
"""
            for level, count in summary.get('health_distribution', {}).items():
                text += f"  • {level}: {count}个\n"
            
            if summary.get('needs_attention'):
                text += f"\n⚠️ 需要关注的工作流:\n"
                for wf in summary['needs_attention']:
                    text += f"  • {wf}\n"
            
            self.result_text.setText(text)
        except Exception as e:
            self.result_text.setText(f"获取健康概览失败: {str(e)}")


class ShareWidget(QWidget):
    """工作流分享面板"""
    
    workflow_imported = pyqtSignal(dict)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.share_system = create_share_system("./data")
        self._workflows = {}
        self._current_workflow = None
        self._init_ui()
    
    def set_workflows(self, workflows: Dict, current_workflow: Dict = None):
        self._workflows = workflows
        self._current_workflow = current_workflow
        self._refresh_workflow_list()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        colors = theme_manager.colors

        info_label = QLabel("🔗 工作流分享系统 - 导入导出和分享工作流")
        info_label.setStyleSheet(f"font-weight: bold; font-size: 13px; color: {colors['warning']};")
        layout.addWidget(info_label)
        
        export_group = QGroupBox("导出工作流")
        export_layout = QVBoxLayout()
        
        self.workflow_combo = QComboBox()
        export_layout.addWidget(QLabel("选择工作流:"))
        export_layout.addWidget(self.workflow_combo)
        
        export_btn = QPushButton("📤 导出为JSON")
        export_btn.clicked.connect(self._export_workflow)
        export_layout.addWidget(export_btn)
        
        export_group.setLayout(export_layout)
        layout.addWidget(export_group)
        
        import_group = QGroupBox("导入工作流")
        import_layout = QVBoxLayout()
        
        import_file_btn = QPushButton("📁 从文件导入")
        import_file_btn.clicked.connect(self._import_from_file)
        import_layout.addWidget(import_file_btn)
        
        import_group.setLayout(import_layout)
        layout.addWidget(import_group)
        
        self.links_list = QListWidget()
        layout.addWidget(QLabel("分享链接:"))
        layout.addWidget(self.links_list)
        
        layout.addStretch()
        self._refresh_links()
    
    def _refresh_workflow_list(self):
        self.workflow_combo.clear()
        if self._current_workflow and self._current_workflow.get('steps'):
            self.workflow_combo.addItem("当前工作流")
        self.workflow_combo.addItem("从文件选择...")
    
    def _export_workflow(self):
        if self.workflow_combo.currentText() == "当前工作流" and self._current_workflow:
            workflow_data = self._current_workflow
            workflow_name = "exported_workflow"
        else:
            filepath, _ = QFileDialog.getOpenFileName(self, "选择工作流文件", "", "JSON文件 (*.json)")
            if filepath:
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        workflow_data = json.load(f)
                    workflow_name = os.path.basename(filepath).replace('.json', '')
                except Exception as e:
                    show_error("导出失败", f"读取文件失败: {str(e)}")
                    return
            else:
                return
        
        save_path, _ = QFileDialog.getSaveFileName(self, "保存工作流", f"{workflow_name}.json", "JSON文件 (*.json)")
        if save_path:
            try:
                export_data = {
                    "version": "22.0.0",
                    "name": workflow_name,
                    "workflow": workflow_data,
                    "exported_at": time.time()
                }
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, ensure_ascii=False, indent=2)
                show_toast("工作流导出成功", 'success')
            except Exception as e:
                show_error("导出失败", f"保存文件失败: {str(e)}")
    
    def _import_from_file(self):
        filepath, _ = QFileDialog.getOpenFileName(self, "导入工作流", "", "JSON文件 (*.json)")
        if filepath:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, dict) and 'workflow' in data:
                    workflow = data['workflow']
                else:
                    workflow = data
                
                if isinstance(workflow, dict) and 'steps' in workflow:
                    self.workflow_imported.emit(workflow)
                    show_toast("工作流导入成功", 'success')
                else:
                    show_error("导入失败", "无效的工作流格式")
            except Exception as e:
                show_error("导入失败", f"读取文件失败: {str(e)}")
    
    def _refresh_links(self):
        with batch_updates(self.links_list):
            self.links_list.clear()
            try:
                links = self.share_system.list_shared_workflows()
                for link in links:
                    self.links_list.addItem(f"🔗 {link.link_id} - {link.workflow_name} (查看: {link.view_count})")
            except Exception:
                self.links_list.addItem("暂无分享链接")


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("RabAI AutoClick v22 - 桌面自动化工具 (增强版)")
        self.setGeometry(100, 100, 1400, 900)
        
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
        self._mini_mode = False
        self._mini_toolbar = None
        
        self.predictive_engine = create_predictive_engine("./data")
        self.healing_system = create_self_healing_system("./data")
        self.diagnostics = create_diagnostics("./data")
        
        message_manager.set_parent(self)
        
        self._init_ui()
        self._setup_hotkeys()
        self._connect_signals()
        self._update_button_texts()
        
        app_logger.info("RabAI AutoClick v22 增强版启动", "Main")
        
        QTimer.singleShot(500, self._check_accessibility_permission)
    
    def _init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(5)
        
        toolbar = QHBoxLayout()
        
        self.new_btn = QPushButton("📄 新建")
        self.open_btn = QPushButton("📂 打开")
        self.save_btn = QPushButton("💾 保存")
        self.history_btn = QPushButton("📚 记录")

        colors = theme_manager.colors
        self.run_btn = QPushButton("▶ 运行")
        self.run_btn.setStyleSheet(f"background-color: {colors['success']}; color: white; font-weight: bold; padding: 8px 16px;")
        self.stop_btn = QPushButton("⏹ 停止")
        self.stop_btn.setStyleSheet(f"background-color: {colors['error']}; color: white; padding: 8px 16px;")
        self.pause_btn = QPushButton("⏸ 暂停")

        self.on_top_btn = QPushButton("📌 置顶")
        self.on_top_btn.setCheckable(True)
        self.teaching_btn = QPushButton("🖥 显示")
        self.teaching_btn.setCheckable(True)
        self.hotkey_btn = QPushButton("⌨ 快捷键")
        self.window_btn = QPushButton("🪟 窗口")
        self.region_btn = QPushButton("📐 区域")
        self.loop_btn = QPushButton("🔄 循环")
        self.stats_btn = QPushButton("📊 统计")
        self.memory_btn = QPushButton("💾 内存")
        self.mini_btn = QPushButton("📱 迷你")
        self.theme_btn = QPushButton("🌙 深色")
        self.theme_btn.setCheckable(True)

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
        toolbar.addWidget(self.mini_btn)
        toolbar.addWidget(self.theme_btn)
        toolbar.addStretch()
        
        self.memory_label = QLabel()
        toolbar.addWidget(self.memory_label)
        
        main_layout.addLayout(toolbar)
        
        splitter = QSplitter(Qt.Horizontal)
        
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        self.step_list = StepListWidget()
        left_layout.addWidget(QLabel("步骤列表:"))
        left_layout.addWidget(self.step_list)
        
        splitter.addWidget(left_panel)
        
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
        self.recording_widget.set_main_window(self)
        self.recording_widget.workflow_added.connect(self._on_workflow_added_from_recording)
        right_panel.addTab(self.recording_widget, "录屏")
        
        self.predictive_widget = PredictiveWidget()
        self.predictive_widget.action_triggered.connect(self._on_predictive_action)
        right_panel.addTab(self.predictive_widget, "🧠 预测")
        
        self.diagnostics_widget = DiagnosticsWidget()
        right_panel.addTab(self.diagnostics_widget, "🏥 诊断")
        
        self.share_widget = ShareWidget()
        self.share_widget.workflow_imported.connect(self._on_workflow_imported)
        right_panel.addTab(self.share_widget, "🔗 分享")
        
        self.log_widget = LogWidget()
        right_panel.addTab(self.log_widget, "日志")
        
        splitter.addWidget(right_panel)
        splitter.setSizes([350, 1050])
        
        main_layout.addWidget(splitter)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        
        self.statusBar().showMessage("就绪 | F6运行 | F7停止 | v22增强版")
        
        self._load_actions()
        
        self._memory_timer = QTimer()
        self._memory_timer.timeout.connect(self._update_memory_display)
        self._memory_timer.start(5000)
    
    def _check_accessibility_permission(self):
        import platform
        if platform.system() != 'Darwin':
            return
        
        import pyautogui
        try:
            current_pos = pyautogui.position()
            test_x = current_pos.x
            test_y = current_pos.y
            
            pyautogui.moveTo(test_x + 1, test_y + 1, duration=0.05)
            new_pos = pyautogui.position()
            pyautogui.moveTo(test_x, test_y, duration=0.05)
            
            if new_pos.x == test_x + 1 and new_pos.y == test_y + 1:
                app_logger.success("辅助功能权限已授权", "Permission")
                return
        except Exception as e:
            app_logger.debug(f"辅助功能检查失败: {e}")
        
        reply = QMessageBox.question(
            self, 
            "需要辅助功能权限",
            "RabAI AutoClick 需要 macOS 辅助功能权限才能控制鼠标和键盘。\n\n"
            "点击「是」打开系统偏好设置进行授权。\n\n"
            "授权步骤：\n"
            "1. 点击左下角锁图标解锁\n"
            "2. 点击「+」添加应用程序\n"
            "3. 找到并添加终端或 Python\n"
            "4. 确保勾选已添加的项目\n"
            "5. 重启本程序",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes
        )
        
        if reply == QMessageBox.Yes:
            import subprocess
            subprocess.run(['open', 'x-apple.systempreferences:com.apple.preference.security?Privacy_Accessibility'])
    
    def _update_memory_display(self):
        try:
            mem = memory_manager.get_memory_usage()
            self.memory_label.setText(f"内存: {mem['rss']} MB")
        except Exception as e:
            app_logger.debug(f"内存显示更新失败: {e}")
            self.memory_label.setText("")
    
    def _on_workflow_added_from_recording(self, steps: list):
        TYPE_MAP = {
            'hotkey': 'key_press',
            'type_text': 'type_text',
            'key_press': 'key_press',
            'click': 'click',
            'double_click': 'double_click',
            'scroll': 'scroll',
        }
        
        app_logger.info(f"添加录屏步骤到工作流，共 {len(steps)} 个步骤", "Recording")
        
        for i, step in enumerate(steps):
            app_logger.debug(f"步骤 {i}: {step}", "Recording")
            
            step['id'] = self.next_step_id
            self.next_step_id += 1
            
            original_type = step.get('type')
            if not original_type:
                original_type = step.get('action_type', 'unknown')
            
            app_logger.debug(f"原始类型: {original_type}", "Recording")
            
            mapped_type = TYPE_MAP.get(original_type)
            if not mapped_type:
                mapped_type = original_type if original_type else 'unknown'
            
            app_logger.debug(f"映射类型: {mapped_type}", "Recording")
            
            step['type'] = mapped_type
            
            params = step.get('params', {})
            
            if original_type == 'hotkey' and 'keys' in params:
                keys_str = params['keys']
                if isinstance(keys_str, str):
                    params['keys'] = keys_str.split('+')
            
            if original_type in ('hotkey', 'key_press') and 'key' in params:
                if 'keys' not in params:
                    params['keys'] = [params['key']]
                del params['key']
            
            step['params'] = params
            
            self.current_workflow['steps'].append(step)
            
            action_info = self.engine.get_action_info().get(mapped_type, {})
            display_name = action_info.get('display_name', mapped_type)
            
            self.step_list.add_step(step['id'], mapped_type, display_name)
            self.step_configs[step['id']] = params
        
        self.step_list.set_current_index(self.step_list.get_step_count() - 1)
        show_toast(f"已添加 {len(steps)} 个步骤到工作流", 'success')
        
        self._update_all_panels()
    
    def _on_predictive_action(self, action_type: str, params: dict):
        app_logger.info(f"预测动作触发: {action_type} - {params}", "Predictive")
    
    def _on_workflow_imported(self, workflow: dict):
        self.current_workflow = workflow
        self.step_configs = {}
        self.next_step_id = 1

        with batch_updates(self.step_list):
            self.step_list.clear()

            for step in workflow.get('steps', []):
                step['id'] = self.next_step_id
                self.next_step_id += 1
                self.step_configs[step['id']] = step.get('params', {})

                action_type = step.get('type', 'unknown')
                action_info = self.engine.get_action_info().get(action_type, {})
                display_name = action_info.get('display_name', action_type)
                self.step_list.add_step(step['id'], action_type, display_name)
        
        if workflow.get('steps'):
            self.step_list.set_current_index(0)
        
        self._update_all_panels()
        show_toast("工作流导入成功", 'success')
    
    def _update_all_panels(self):
        """更新所有面板的工作流数据"""
        self.share_widget.set_workflows({}, self.current_workflow)
        self.predictive_widget.set_workflows({'current': self.current_workflow})
        self.diagnostics_widget.set_workflows({'current': self.current_workflow})
    
    def _update_share_widget(self):
        self._update_all_panels()
    
    def _load_actions(self):
        action_info = self.engine.get_action_info()

        with batch_updates(self.action_list):
            for action_type, info in action_info.items():
                item = QListWidgetItem(f"{info['display_name']}")
                item.setData(Qt.UserRole, action_type)
                item.setToolTip(f"{info['description']}\n类型: {action_type}")
                self.action_list.addItem(item)

        with batch_updates(self.config_stack):
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
                pause_key=self.current_hotkeys.get('pause', 'f8'),
                record_start_key=self.current_hotkeys.get('record_start', 'f9'),
                record_stop_key=self.current_hotkeys.get('record_stop', 'f10'),
                display_key=self.current_hotkeys.get('display', 'f11')
            )
            self.hotkey_manager.start_triggered.connect(self._on_run)
            self.hotkey_manager.stop_triggered.connect(self._on_stop)
            self.hotkey_manager.pause_triggered.connect(self._on_pause)
            self.hotkey_manager.record_start_triggered.connect(self._on_record_start_hotkey)
            self.hotkey_manager.record_stop_triggered.connect(self._on_record_stop_hotkey)
            self.hotkey_manager.display_triggered.connect(self._toggle_teaching_mode)
            
            def format_key(key_str):
                parts = key_str.replace(' ', '+').split('+')
                return '+'.join(p.strip().upper() for p in parts)
            
            start_key = format_key(self.current_hotkeys.get('start', 'f6'))
            stop_key = format_key(self.current_hotkeys.get('stop', 'f7'))
            record_start_key = format_key(self.current_hotkeys.get('record_start', 'f9'))
            record_stop_key = format_key(self.current_hotkeys.get('record_stop', 'f10'))
            display_key = format_key(self.current_hotkeys.get('display', 'f11'))
            
            app_logger.info(f"全局快捷键已启用: 运行={start_key}, "
                           f"停止={stop_key}, 录制={record_start_key}/{record_stop_key}, 显示={display_key}", "Hotkey")
        else:
            app_logger.warning("快捷键模块不可用", "Hotkey")
    
    def _on_record_start_hotkey(self):
        if not self.recording_widget._recording_manager.is_recording():
            self.recording_widget._on_record()
    
    def _on_record_stop_hotkey(self):
        if self.recording_widget._recording_manager.is_recording():
            self.recording_widget._on_stop()
            self.showNormal()
            self.activateWindow()
            self.raise_()
    
    def _update_button_texts(self):
        def format_key_display(key_str):
            parts = key_str.replace(' ', '+').split('+')
            return '+'.join(p.strip().upper() for p in parts)
        
        start_key = format_key_display(self.current_hotkeys.get('start', 'f6'))
        stop_key = format_key_display(self.current_hotkeys.get('stop', 'f7'))
        pause_key = format_key_display(self.current_hotkeys.get('pause', 'f8'))
        record_start_key = format_key_display(self.current_hotkeys.get('record_start', 'f9'))
        record_stop_key = format_key_display(self.current_hotkeys.get('record_stop', 'f10'))
        
        self.run_btn.setText(f"▶ 运行 ({start_key})")
        self.stop_btn.setText(f"⏹ 停止 ({stop_key})")
        self.pause_btn.setText(f"⏸ 暂停 ({pause_key})")
        
        self.recording_widget.record_btn.setText(f"🔴 开始录制 ({record_start_key})")
        self.recording_widget.stop_btn.setText(f"⏹ 停止录制 ({record_stop_key})")
    
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
        self.mini_btn.clicked.connect(self._toggle_mini_mode)
        self.theme_btn.clicked.connect(self._toggle_theme)

        self.action_list.currentRowChanged.connect(self._on_action_selected)
        
        self.step_list.add_btn.clicked.connect(self._on_add_step)
        self.step_list.remove_btn.clicked.connect(self._on_remove_step)
        self.step_list.clear_btn.clicked.connect(self._on_clear_steps)
        self.step_list.up_btn.clicked.connect(self._on_move_up)
        self.step_list.down_btn.clicked.connect(self._on_move_down)
        self.step_list.step_selected.connect(self._on_step_selected)
        
        for widget in self.config_widgets.values():
            widget.config_changed.connect(self._on_config_changed)

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
    
    def _on_new(self):
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
    
    def _on_open(self):
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
    
    def _on_save(self):
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
            step_type = data.get('type', 'unknown')
            
            if step_id in self.step_configs:
                step = self.step_configs[step_id].copy()
                step['id'] = step_id
                step['type'] = step_type
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
                step_type = step.get('type')
                if step_type in ('ocr', 'find_image', 'click_image', 'screenshot') and not step.get('region'):
                    step['region'] = self._target_region
            app_logger.info(f"使用识别区域: {self._target_region}", "Workflow")
        
        self._current_loop = 0
        self._is_looping = True
        
        workflow_name = "未命名工作流"
        execution_stats.start_session(workflow_name, self._loop_count)
        
        self.predictive_engine.record_action("workflow_trigger", workflow_name, {"loop_count": self._loop_count})
        
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
    
    def _toggle_mini_mode(self):
        from .mini_toolbar import MiniToolbar
        
        if self._mini_mode:
            self._mini_mode = False
            if self._mini_toolbar:
                self._mini_toolbar.close()
                self._mini_toolbar = None
            self.showNormal()
            self.activateWindow()
            self.mini_btn.setText("📱 迷你")
            self.mini_btn.setStyleSheet("")
        else:
            self._mini_mode = True
            self._mini_toolbar = MiniToolbar()
            self._mini_toolbar.run_clicked.connect(self._on_run)
            self._mini_toolbar.stop_clicked.connect(self._on_stop)
            self._mini_toolbar.region_clicked.connect(self._on_select_region)
            self._mini_toolbar.window_clicked.connect(self._on_select_window)
            self._mini_toolbar.settings_clicked.connect(self._on_hotkey_settings)
            self._mini_toolbar.switch_to_full.connect(self._toggle_mini_mode)
            
            if self._target_region:
                self._mini_toolbar.set_region(*self._target_region)
            
            self._mini_toolbar.move(100, 100)
            self._mini_toolbar.show()
            
            self.hide()
            self.mini_btn.setText("📱 完整")
            colors = theme_manager.colors
            self.mini_btn.setStyleSheet(f"background-color: {colors['warning']}; color: white;")

    def _toggle_theme(self):
        """Toggle between light and dark themes."""
        from ui.theme import ThemeType
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

    def _apply_theme(self):
        """Apply the current theme to all UI components."""
        self.setStyleSheet(theme_manager.get_stylesheet("main_window"))
        self.log_widget.text_edit.setStyleSheet(theme_manager.get_stylesheet("log"))

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
        
        if self._teaching_mode:
            self.teaching_btn.setChecked(True)
            self.teaching_btn.setStyleSheet(f"background-color: {colors['primary']}; color: white;")
            show_toast("按键显示已开启 - 屏幕显示鼠标位置，按 ESC 关闭", 'success')
        else:
            self.teaching_btn.setChecked(False)
            self.teaching_btn.setStyleSheet("")
            show_toast("按键显示已关闭", 'info')
        
        app_logger.info(f"按键显示: {self._teaching_mode}", "UI")
    
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
            config = copy.deepcopy(config_widget.get_config())
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
    
    def _on_clear_steps(self):
        if self.step_list.get_step_count() == 0:
            return
        
        reply = QMessageBox.question(
            self, "确认清空",
            "确定要清空所有步骤吗？此操作不可撤销。",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            self.step_list.clear()
            self.step_configs = {}
            self.current_workflow['steps'] = []
            self.next_step_id = 1
            app_logger.info("已清空所有步骤", "Editor")
            show_toast("已清空所有步骤", 'info')
    
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
                self._is_loading_config = True
                self.config_stack.setCurrentWidget(self.config_widgets[action_type])
                
                if step_id in self.step_configs:
                    saved_config = copy.deepcopy(self.step_configs[step_id])
                    self.config_widgets[action_type].set_config(saved_config)
                else:
                    action_info = self.engine.get_action_info().get(action_type, {})
                    self.config_widgets[action_type].set_config({
                        'id': step_id,
                        'type': action_type
                    })
                self._is_loading_config = False
    
    def _on_config_changed(self):
        if hasattr(self, '_is_loading_config') and self._is_loading_config:
            return
        
        index = self.step_list.get_current_index()
        if index >= 0:
            item = self.step_list.list_widget.item(index)
            data = item.data(Qt.UserRole)
            step_id = data['id']
            action_type = data['type']
            
            config_widget = self.config_widgets.get(action_type)
            if config_widget:
                config = copy.deepcopy(config_widget.get_config())
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
            
            self.predictive_engine.record_action(
                step.get('type', 'unknown'),
                step.get('type', 'unknown'),
                {"step_id": step.get('id')},
                "success" if result.success else "failed",
                step_duration
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
            
            self.showNormal()
            self.activateWindow()
            self.raise_()
            
            self.predictive_widget._refresh_predictions()
            
            loop_info = f" (共{self._loop_count}次循环)" if self._loop_count > 1 else ""
            time_info = f" | 总耗时: {total_duration:.1f}秒"
            if avg_duration > 0 and self._loop_count > 1:
                time_info += f" | 平均每循环: {avg_duration:.1f}秒"
            
            if success:
                app_logger.success(f"工作流执行完成{loop_info}{time_info}", "Workflow")
                show_toast(f"执行完成{loop_info} ({total_duration:.1f}秒)", 'success')
                QMessageBox.information(self, "执行完成", f"工作流执行完成！\n{loop_info}\n总耗时: {total_duration:.1f}秒")
            else:
                app_logger.warning("工作流已停止", "Workflow")
                QMessageBox.warning(self, "执行停止", "工作流已停止执行")
        except Exception as e:
            logger.error(f"Workflow end error: {e}")
    
    def _on_engine_error(self, step, message: str):
        try:
            app_logger.error(f"步骤 [{step.get('id')}] 错误: {message}", "Engine")
            execution_stats.record_error(step.get('type', 'unknown'), message)
        except Exception as e:
            logger.error(f"Engine error: {e}")
    
    def _on_hotkey_settings(self):
        try:
            dialog = HotkeySettingsDialog(self.current_hotkeys, self)
            if dialog.exec_() == QDialog.Accepted:
                new_hotkeys = dialog.get_hotkeys()
                self.current_hotkeys = new_hotkeys
                
                self.hotkey_manager.save_config(new_hotkeys)
                
                self.hotkey_manager.register_hotkeys(
                    start_key=new_hotkeys.get('start', 'f6'),
                    stop_key=new_hotkeys.get('stop', 'f7'),
                    pause_key=new_hotkeys.get('pause', 'f8'),
                    record_start_key=new_hotkeys.get('record_start', 'f9'),
                    record_stop_key=new_hotkeys.get('record_stop', 'f10'),
                    display_key=new_hotkeys.get('display', 'f11')
                )
                
                self._update_button_texts()
                app_logger.info(f"快捷键已更新", "Hotkey")
                show_toast("快捷键设置已保存", 'success')
        except Exception as e:
            import traceback
            traceback.print_exc()
    
    def _on_select_window(self):
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
        self._target_region = None
        self._window_mode = False
        self.window_btn.setText("🪟 窗口")
        self.window_btn.setStyleSheet("")
        show_toast("已清除窗口选择", 'info')
    
    def _on_select_region(self):
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
        self._region_selector = None
        self.showMinimized()
        QTimer.singleShot(500, self._create_region_selector)
    
    def _create_region_selector(self):
        try:
            self._region_selector = RegionSelector(mode='region')
            self._region_selector._parent_window = self
            self._region_selector.region_selected.connect(self._on_region_selected)
            self._region_selector.cancelled.connect(self._on_region_cancelled)
        except Exception as e:
            self.showNormal()
            self.activateWindow()
            show_error("错误", f"创建区域选择器失败: {str(e)}")
    
    def _on_region_selected(self, x, y, w, h):
        colors = theme_manager.colors
        self._target_region = (x, y, w, h)
        self._window_mode = False
        self.region_btn.setText(f"📐 ({x},{y},{w}x{h})")
        self.region_btn.setStyleSheet(f"background-color: {colors['success']}; color: white;")
        self.window_btn.setText("🪟 窗口")
        self.window_btn.setStyleSheet("")

        if self._region_selector:
            self._region_selector.close()
            self._region_selector = None

        self.showNormal()
        self.activateWindow()
        self.raise_()
        show_toast(f"已选择区域: ({x}, {y}, {w}x{h})", 'success')
        app_logger.info(f"设置OCR区域: ({x}, {y}, {w}x{h})", "UI")
    
    def _on_region_cancelled(self):
        if self._region_selector:
            self._region_selector.close()
            self._region_selector = None
        
        self.showNormal()
        self.activateWindow()
        self.raise_()
    
    def _clear_region(self):
        self._target_region = None
        self.region_btn.setText("📐 区域")
        self.region_btn.setStyleSheet("")
        show_toast("已清除识别区域", 'info')
    
    def _on_loop_settings(self):
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
    
    def keyPressEvent(self, event):
        from PyQt5.QtCore import Qt
        
        if event.key() == Qt.Key.Key_Escape:
            if key_display_window.is_enabled():
                key_display_window.disable()
                self.teaching_btn.setChecked(False)
                self.teaching_btn.setStyleSheet("")
                show_toast("按键显示已关闭", 'info')
                return

        super().keyPressEvent(event)

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
                app_logger.debug(f"清理engine_signals失败: {e}")
        
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
    app.setApplicationVersion('22.0.0')
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
