import sys
import os
import platform
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QListWidget, QListWidgetItem, QDialog, QFileDialog,
    QMessageBox, QSplitter, QLineEdit, QTextEdit, QComboBox,
    QGroupBox, QFormLayout, QDialogButtonBox
)
from PyQt5.QtCore import Qt, pyqtSignal, QTimer, QPoint
from PyQt5.QtGui import QFont, QColor, QKeySequence


IS_MACOS = platform.system() == 'Darwin'
IS_WINDOWS = platform.system() == 'Windows'


class WorkflowHistoryManager:
    def __init__(self, history_dir: str = None):
        if history_dir is None:
            history_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'history')
        self.history_dir = history_dir
        os.makedirs(history_dir, exist_ok=True)
        self.index_file = os.path.join(history_dir, 'index.json')
        self._load_index()
    
    def _load_index(self):
        if os.path.exists(self.index_file):
            try:
                with open(self.index_file, 'r', encoding='utf-8') as f:
                    self.index = json.load(f)
            except Exception:
                self.index = {'workflows': []}
        else:
            self.index = {'workflows': []}
    
    def _save_index(self):
        with open(self.index_file, 'w', encoding='utf-8') as f:
            json.dump(self.index, f, ensure_ascii=False, indent=2)
    
    def save_workflow(self, name: str, workflow: Dict[str, Any], tags: List[str] = None) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{name}.json"
        filepath = os.path.join(self.history_dir, filename)
        
        data = {
            'name': name,
            'created_at': datetime.now().isoformat(),
            'tags': tags or [],
            'workflow': workflow
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        entry = {
            'filename': filename,
            'name': name,
            'created_at': datetime.now().isoformat(),
            'tags': tags or [],
            'step_count': len(workflow.get('steps', []))
        }
        
        for i, item in enumerate(self.index['workflows']):
            if item.get('name') == name:
                self.index['workflows'][i] = entry
                self._save_index()
                return filepath
        
        self.index['workflows'].insert(0, entry)
        self._save_index()
        return filepath
    
    def load_workflow(self, filename: str) -> Optional[Dict[str, Any]]:
        filepath = os.path.join(self.history_dir, filename)
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return data.get('workflow', data)
            except Exception:
                return None
        return None
    
    def delete_workflow(self, filename: str) -> bool:
        filepath = os.path.join(self.history_dir, filename)
        if os.path.exists(filepath):
            os.remove(filepath)
            self.index['workflows'] = [
                w for w in self.index['workflows'] 
                if w.get('filename') != filename
            ]
            self._save_index()
            return True
        return False
    
    def rename_workflow(self, filename: str, new_name: str) -> bool:
        filepath = os.path.join(self.history_dir, filename)
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                data['name'] = new_name
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                for item in self.index['workflows']:
                    if item.get('filename') == filename:
                        item['name'] = new_name
                        break
                self._save_index()
                return True
            except Exception:
                return False
        return False
    
    def get_all_workflows(self) -> List[Dict[str, Any]]:
        return self.index.get('workflows', [])
    
    def search_workflows(self, keyword: str) -> List[Dict[str, Any]]:
        keyword = keyword.lower()
        return [
            w for w in self.index.get('workflows', [])
            if keyword in w.get('name', '').lower() or
               keyword in ' '.join(w.get('tags', [])).lower()
        ]


class HistoryDialog(QDialog):
    workflow_selected = pyqtSignal(dict)
    
    def __init__(self, history_manager: WorkflowHistoryManager, parent=None):
        super().__init__(parent)
        self.history_manager = history_manager
        self.setWindowTitle("操作记录管理")
        self.setMinimumSize(800, 500)
        self._init_ui()
        self._load_workflows()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        
        search_layout = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("搜索工作流...")
        self.search_edit.textChanged.connect(self._on_search)
        search_layout.addWidget(self.search_edit)
        layout.addLayout(search_layout)
        
        splitter = QSplitter(Qt.Horizontal)
        
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        self.list_widget = QListWidget()
        self.list_widget.itemClicked.connect(self._on_item_clicked)
        self.list_widget.itemDoubleClicked.connect(self._on_item_double_clicked)
        left_layout.addWidget(self.list_widget)
        
        btn_layout = QHBoxLayout()
        self.load_btn = QPushButton("加载")
        self.delete_btn = QPushButton("删除")
        self.rename_btn = QPushButton("重命名")
        btn_layout.addWidget(self.load_btn)
        btn_layout.addWidget(self.rename_btn)
        btn_layout.addWidget(self.delete_btn)
        left_layout.addLayout(btn_layout)
        
        self.load_btn.clicked.connect(self._on_load)
        self.delete_btn.clicked.connect(self._on_delete)
        self.rename_btn.clicked.connect(self._on_rename)
        
        splitter.addWidget(left_widget)
        
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        
        info_group = QGroupBox("详细信息")
        info_layout = QFormLayout()
        
        self.name_label = QLabel()
        self.created_label = QLabel()
        self.steps_label = QLabel()
        self.tags_label = QLabel()
        
        info_layout.addRow("名称:", self.name_label)
        info_layout.addRow("创建时间:", self.created_label)
        info_layout.addRow("步骤数:", self.steps_label)
        info_layout.addRow("标签:", self.tags_label)
        
        info_group.setLayout(info_layout)
        right_layout.addWidget(info_group)
        
        preview_group = QGroupBox("步骤预览")
        preview_layout = QVBoxLayout()
        self.preview_text = QTextEdit()
        self.preview_text.setReadOnly(True)
        preview_layout.addWidget(self.preview_text)
        preview_group.setLayout(preview_layout)
        right_layout.addWidget(preview_group)
        
        splitter.addWidget(right_widget)
        splitter.setSizes([300, 500])
        
        layout.addWidget(splitter)
        
        button_box = QDialogButtonBox(QDialogButtonBox.Close)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    
    def _load_workflows(self, keyword: str = None):
        self.list_widget.clear()
        
        if keyword:
            workflows = self.history_manager.search_workflows(keyword)
        else:
            workflows = self.history_manager.get_all_workflows()
        
        for w in workflows:
            item = QListWidgetItem(f"{w.get('name', '未命名')} ({w.get('step_count', 0)} 步)")
            item.setData(Qt.UserRole, w)
            self.list_widget.addItem(item)
    
    def _on_search(self, text: str):
        self._load_workflows(text if text else None)
    
    def _on_item_clicked(self, item: QListWidgetItem):
        data = item.data(Qt.UserRole)
        if data:
            self.name_label.setText(data.get('name', ''))
            self.created_label.setText(data.get('created_at', ''))
            self.steps_label.setText(str(data.get('step_count', 0)))
            self.tags_label.setText(', '.join(data.get('tags', [])))
            
            workflow = self.history_manager.load_workflow(data.get('filename'))
            if workflow:
                self.preview_text.setPlainText(json.dumps(workflow, ensure_ascii=False, indent=2))
    
    def _on_item_double_clicked(self, item: QListWidgetItem):
        self._on_load()
    
    def _on_load(self):
        item = self.list_widget.currentItem()
        if item:
            data = item.data(Qt.UserRole)
            workflow = self.history_manager.load_workflow(data.get('filename'))
            if workflow:
                self.workflow_selected.emit(workflow)
                self.accept()
    
    def _on_delete(self):
        item = self.list_widget.currentItem()
        if item:
            data = item.data(Qt.UserRole)
            reply = QMessageBox.question(
                self, "确认删除",
                f"确定要删除 '{data.get('name')}' 吗？",
                QMessageBox.Yes | QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                self.history_manager.delete_workflow(data.get('filename'))
                self._load_workflows()
    
    def _on_rename(self):
        item = self.list_widget.currentItem()
        if item:
            data = item.data(Qt.UserRole)
            from PyQt5.QtWidgets import QInputDialog
            new_name, ok = QInputDialog.getText(
                self, "重命名", "新名称:", 
                QLineEdit.Normal, data.get('name', '')
            )
            if ok and new_name:
                self.history_manager.rename_workflow(data.get('filename'), new_name)
                self._load_workflows()


class QuickSaveDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("快速保存")
        self.setMinimumWidth(400)
        self._init_ui()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        
        form_layout = QFormLayout()
        
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("输入工作流名称")
        form_layout.addRow("名称:", self.name_edit)
        
        self.tags_edit = QLineEdit()
        self.tags_edit.setPlaceholderText("多个标签用逗号分隔")
        form_layout.addRow("标签:", self.tags_edit)
        
        layout.addLayout(form_layout)
        
        button_box = QDialogButtonBox(
            QDialogButtonBox.Save | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    
    def get_data(self) -> tuple:
        name = self.name_edit.text().strip() or f"工作流_{datetime.now().strftime('%H%M%S')}"
        tags = [t.strip() for t in self.tags_edit.text().split(',') if t.strip()]
        return name, tags
