import sys
import os
import platform
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
from PyQt5.QtCore import Qt, pyqtSignal, QTimer, QThread
from PyQt5.QtGui import QIcon, QColor, QFont, QCursor

from core.engine import FlowEngine
from core.base_action import ActionResult
from utils.hotkey import HotkeyManager
from utils.app_logger import app_logger
from utils.memory import memory_manager
from utils.recording import RecordingManager, RecordingEditor, RecordedAction, PYNPUT_AVAILABLE
from utils.history import WorkflowHistoryManager, HistoryDialog, QuickSaveDialog
from utils.teaching_mode import teaching_mode_manager
from ui.hotkey_dialog import HotkeySettingsDialog
from ui.region_selector import RegionSelector, PositionSelector
from ui.message import message_manager, show_error, show_success, show_warning, show_toast

IS_MACOS = platform.system() == 'Darwin'
IS_WINDOWS = platform.system() == 'Windows'


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
                required_layout.addRow(f"{param}:", widget)
            
            required_group.setLayout(required_layout)
            layout.addWidget(required_group)
        
        if optional_params:
            optional_group = QGroupBox("可选参数")
            optional_layout = QFormLayout()
            optional_layout.setSpacing(5)
            
            for param, default_value in optional_params.items():
                widget = self._create_param_widget(param, default_value)
                self.widgets[param] = widget
                optional_layout.addRow(f"{param}:", widget)
            
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
            widget.setText(json.dumps(default_value))
            widget.setPlaceholderText("JSON数组格式")
            widget.textChanged.connect(self.config_changed.emit)
            return widget
        elif isinstance(default_value, tuple):
            widget = QLineEdit()
            widget.setText(json.dumps(list(default_value)))
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
                    except:
                        pass
                    variables[name] = value
        return variables
    
    def set_variables(self, variables: Dict[str, Any]) -> None:
        self.table.setRowCount(0)
        for name, value in variables.items():
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.table.setItem(row, 0, QTableWidgetItem(name))
            self.table.setItem(row, 1, QTableWidgetItem(json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value)))


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
        self.open_btn = QPushButton("📂 打开")
        self.save_btn = QPushButton("💾 保存")
        self.history_btn = QPushButton("📚 记录")
        
        self.run_btn = QPushButton("▶ 运行")
        self.run_btn.setStyleSheet("background-color: #4CAF50; color: white; font-weight: bold; padding: 8px 16px;")
        self.stop_btn = QPushButton("⏹ 停止")
        self.stop_btn.setStyleSheet("background-color: #f44336; color: white; padding: 8px 16px;")
        self.pause_btn = QPushButton("⏸ 暂停")
        
        self.on_top_btn = QPushButton("📌 置顶")
        self.on_top_btn.setCheckable(True)
        self.teaching_btn = QPushButton("🎓 教学")
        self.teaching_btn.setCheckable(True)
        self.hotkey_btn = QPushButton("⌨ 快捷键")
        
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
        right_panel.addTab(self.recording_widget, "录屏")
        
        self.log_widget = LogWidget()
        right_panel.addTab(self.log_widget, "日志")
        
        splitter.addWidget(right_panel)
        splitter.setSizes([350, 950])
        
        main_layout.addWidget(splitter)
        
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
        except:
            self.memory_label.setText("")
    
    def _load_actions(self):
        action_info = self.engine.get_action_info()
        
        for action_type, info in action_info.items():
            item = QListWidgetItem(f"{info['display_name']}")
            item.setData(Qt.UserRole, action_type)
            item.setToolTip(f"{info['description']}\n类型: {action_type}")
            self.action_list.addItem(item)
        
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
        
        self.action_list.currentRowChanged.connect(self._on_action_selected)
        
        self.step_list.add_btn.clicked.connect(self._on_add_step)
        self.step_list.remove_btn.clicked.connect(self._on_remove_step)
        self.step_list.up_btn.clicked.connect(self._on_move_up)
        self.step_list.down_btn.clicked.connect(self._on_move_down)
        self.step_list.step_selected.connect(self._on_step_selected)
        
        for widget in self.config_widgets.values():
            widget.config_changed.connect(self._on_config_changed)
        
        self.engine.set_callbacks(
            on_step_start=self._on_engine_step_start,
            on_step_end=self._on_engine_step_end,
            on_workflow_end=self._on_engine_workflow_end,
            on_error=self._on_engine_error
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
        
        app_logger.info("开始运行工作流", "Workflow")
        self.run_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.pause_btn.setEnabled(True)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, len(self.current_workflow['steps']))
        self.progress_bar.setValue(0)
        
        self.engine.load_workflow_from_dict(self.current_workflow)
        self.engine.run_async()
    
    def _on_stop(self):
        self.engine.stop()
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
        
        if self._always_on_top:
            self.setWindowFlags(self.windowFlags() | Qt.WindowStaysOnTopHint)
            self.on_top_btn.setChecked(True)
            self.on_top_btn.setStyleSheet("background-color: #2196F3; color: white;")
            show_toast("窗口已置顶", 'info')
        else:
            self.setWindowFlags(self.windowFlags() & ~Qt.WindowStaysOnTopHint)
            self.on_top_btn.setChecked(False)
            self.on_top_btn.setStyleSheet("")
            show_toast("窗口取消置顶", 'info')
        
        self.show()
        app_logger.info(f"窗口置顶: {self._always_on_top}", "UI")
    
    def _toggle_teaching_mode(self):
        self._teaching_mode = teaching_mode_manager.toggle()
        
        if self._teaching_mode:
            self.teaching_btn.setChecked(True)
            self.teaching_btn.setStyleSheet("background-color: #9C27B0; color: white;")
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
    
    def _on_engine_step_start(self, step):
        app_logger.info(f"执行步骤 [{step.get('id')}]: {step.get('type')}", "Engine")
        current = self.progress_bar.value()
        self.progress_bar.setValue(current + 1)
    
    def _on_engine_step_end(self, step, result: ActionResult):
        if result.success:
            app_logger.success(f"步骤 [{step.get('id')}] 完成: {result.message}", "Engine")
        else:
            app_logger.error(f"步骤 [{step.get('id')}] 失败: {result.message}", "Engine")
    
    def _on_engine_workflow_end(self, success: bool):
        self.run_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.pause_btn.setEnabled(False)
        self.pause_btn.setText("⏸ 暂停")
        self.progress_bar.setVisible(False)
        
        if success:
            app_logger.success("工作流执行完成", "Workflow")
            show_toast("工作流执行完成", 'success')
        else:
            app_logger.warning("工作流已停止", "Workflow")
    
    def _on_engine_error(self, step, message: str):
        app_logger.error(f"步骤 [{step.get('id')}] 错误: {message}", "Engine")
        show_error("执行错误", f"步骤 [{step.get('id')}]: {message}")
    
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
    
    def closeEvent(self, event):
        self._memory_timer.stop()
        self.hotkey_manager.unregister_hotkeys()
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
