"""Workflow history management for RabAI AutoClick.

Provides WorkflowHistoryManager for persisting and querying workflow history,
and HistoryDialog/QuickSaveDialog PyQt UI components for user interaction.
"""

import os
import platform
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QListWidget, QListWidgetItem, QDialog, QFileDialog,
    QMessageBox, QSplitter, QLineEdit, QTextEdit, QComboBox,
    QGroupBox, QFormLayout, QDialogButtonBox
)
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtGui import QFont, QColor


# Platform detection
IS_MACOS: bool = platform.system() == 'Darwin'
IS_WINDOWS: bool = platform.system() == 'Windows'


class WorkflowHistoryManager:
    """Manage workflow history with JSON persistence."""
    
    def __init__(self, history_dir: Optional[str] = None) -> None:
        """Initialize the history manager.
        
        Args:
            history_dir: Optional custom directory for history files.
        """
        if history_dir is None:
            history_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), 
                'history'
            )
        self.history_dir: str = history_dir
        os.makedirs(history_dir, exist_ok=True)
        self.index_file: str = os.path.join(history_dir, 'index.json')
        self.index: Dict[str, Any] = {'workflows': []}
        self._load_index()
    
    def _load_index(self) -> None:
        """Load the history index from file."""
        if os.path.exists(self.index_file):
            try:
                with open(self.index_file, 'r', encoding='utf-8') as f:
                    self.index = json.load(f)
            except Exception:
                self.index = {'workflows': []}
        else:
            self.index = {'workflows': []}
    
    def _save_index(self) -> None:
        """Save the history index to file."""
        with open(self.index_file, 'w', encoding='utf-8') as f:
            json.dump(self.index, f, ensure_ascii=False, indent=2)
    
    def save_workflow(
        self, 
        name: str, 
        workflow: Dict[str, Any], 
        tags: Optional[List[str]] = None
    ) -> str:
        """Save a workflow to history.
        
        Args:
            name: Workflow name.
            workflow: Workflow dictionary.
            tags: Optional list of tag strings.
            
        Returns:
            Path to the saved workflow file.
        """
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
        
        entry: Dict[str, Any] = {
            'filename': filename,
            'name': name,
            'created_at': datetime.now().isoformat(),
            'tags': tags or [],
            'step_count': len(workflow.get('steps', []))
        }
        
        # Update existing entry with same name, else insert
        workflows = self.index['workflows']
        for i, item in enumerate(workflows):
            if item.get('name') == name:
                self.index['workflows'][i] = entry
                self._save_index()
                return filepath
        
        self.index['workflows'].insert(0, entry)
        self._save_index()
        return filepath
    
    def load_workflow(self, filename: str) -> Optional[Dict[str, Any]]:
        """Load a workflow from history.
        
        Args:
            filename: Name of the workflow file.
            
        Returns:
            Workflow dictionary, or None if not found.
        """
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
        """Delete a workflow from history.
        
        Args:
            filename: Name of the workflow file to delete.
            
        Returns:
            True if deleted, False otherwise.
        """
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
        """Rename a workflow in history.
        
        Args:
            filename: Name of the workflow file to rename.
            new_name: New name for the workflow.
            
        Returns:
            True if renamed, False otherwise.
        """
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
        """Get all workflows from history.
        
        Returns:
            List of workflow index entries.
        """
        return self.index.get('workflows', [])
    
    def search_workflows(self, keyword: str) -> List[Dict[str, Any]]:
        """Search workflows by name or tags.
        
        Args:
            keyword: Search keyword.
            
        Returns:
            List of matching workflow entries.
        """
        keyword = keyword.lower()
        return [
            w for w in self.index.get('workflows', [])
            if keyword in w.get('name', '').lower() or
               keyword in ' '.join(w.get('tags', [])).lower()
        ]


class HistoryDialog(QDialog):
    """Dialog for browsing and managing workflow history."""
    
    workflow_selected = pyqtSignal(dict)
    
    def __init__(
        self, 
        history_manager: WorkflowHistoryManager, 
        parent: Optional[QWidget] = None
    ) -> None:
        """Initialize the history dialog.
        
        Args:
            history_manager: WorkflowHistoryManager instance.
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self.history_manager = history_manager
        self.setWindowTitle("操作记录管理")
        self.setMinimumSize(800, 500)
        self._init_ui()
        self._load_workflows()
    
    def _init_ui(self) -> None:
        """Initialize the dialog UI components."""
        layout = QVBoxLayout(self)
        
        # Search bar
        search_layout = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("搜索工作流...")
        self.search_edit.textChanged.connect(self._on_search)
        search_layout.addWidget(self.search_edit)
        layout.addLayout(search_layout)
        
        # Main splitter
        splitter = QSplitter(Qt.Horizontal)
        
        # Left panel: workflow list
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        self.list_widget = QListWidget()
        self.list_widget.itemClicked.connect(self._on_item_clicked)
        self.list_widget.itemDoubleClicked.connect(self._on_item_double_clicked)
        left_layout.addWidget(self.list_widget)
        
        # Action buttons
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
        
        # Right panel: details
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        
        # Info group
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
        
        # Preview group
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
        
        # Close button
        button_box = QDialogButtonBox(QDialogButtonBox.Close)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    
    def _load_workflows(self, keyword: Optional[str] = None) -> None:
        """Load workflows into the list widget.
        
        Args:
            keyword: Optional search keyword to filter workflows.
        """
        self.list_widget.clear()
        
        if keyword:
            workflows = self.history_manager.search_workflows(keyword)
        else:
            workflows = self.history_manager.get_all_workflows()
        
        for w in workflows:
            item = QListWidgetItem(
                f"{w.get('name', '未命名')} ({w.get('step_count', 0)} 步)"
            )
            item.setData(Qt.UserRole, w)
            self.list_widget.addItem(item)
    
    def _on_search(self, text: str) -> None:
        """Handle search text changed.
        
        Args:
            text: Current search text.
        """
        self._load_workflows(text if text else None)
    
    def _on_item_clicked(self, item: QListWidgetItem) -> None:
        """Handle item click - show details.
        
        Args:
            item: Clicked list item.
        """
        data = item.data(Qt.UserRole)
        if data:
            self.name_label.setText(data.get('name', ''))
            self.created_label.setText(data.get('created_at', ''))
            self.steps_label.setText(str(data.get('step_count', 0)))
            self.tags_label.setText(', '.join(data.get('tags', [])))
            
            workflow = self.history_manager.load_workflow(data.get('filename'))
            if workflow:
                self.preview_text.setPlainText(
                    json.dumps(workflow, ensure_ascii=False, indent=2)
                )
    
    def _on_item_double_clicked(self, item: QListWidgetItem) -> None:
        """Handle item double-click - load workflow.
        
        Args:
            item: Double-clicked list item.
        """
        self._on_load()
    
    def _on_load(self) -> None:
        """Load selected workflow and emit signal."""
        item = self.list_widget.currentItem()
        if item:
            data = item.data(Qt.UserRole)
            workflow = self.history_manager.load_workflow(data.get('filename'))
            if workflow:
                self.workflow_selected.emit(workflow)
                self.accept()
    
    def _on_delete(self) -> None:
        """Delete selected workflow after confirmation."""
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
    
    def _on_rename(self) -> None:
        """Rename selected workflow with input dialog."""
        item = self.list_widget.currentItem()
        if item:
            data = item.data(Qt.UserRole)
            from PyQt5.QtWidgets import QInputDialog
            new_name, ok = QInputDialog.getText(
                self, "重命名", "新名称:", 
                QLineEdit.Normal, data.get('name', '')
            )
            if ok and new_name:
                self.history_manager.rename_workflow(
                    data.get('filename'), new_name
                )
                self._load_workflows()


class QuickSaveDialog(QDialog):
    """Dialog for quickly saving a workflow with name and tags."""
    
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        """Initialize the quick save dialog.
        
        Args:
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self.setWindowTitle("快速保存")
        self.setMinimumWidth(400)
        self._init_ui()
    
    def _init_ui(self) -> None:
        """Initialize the dialog UI components."""
        layout = QVBoxLayout(self)
        
        # Form fields
        form_layout = QFormLayout()
        
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("输入工作流名称")
        form_layout.addRow("名称:", self.name_edit)
        
        self.tags_edit = QLineEdit()
        self.tags_edit.setPlaceholderText("多个标签用逗号分隔")
        form_layout.addRow("标签:", self.tags_edit)
        
        layout.addLayout(form_layout)
        
        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.Save | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    
    def get_data(self) -> Tuple[str, List[str]]:
        """Get the entered name and tags.
        
        Returns:
            Tuple of (workflow_name, tags_list).
        """
        name = self.name_edit.text().strip() or f"工作流_{datetime.now().strftime('%H%M%S')}"
        tags = [t.strip() for t in self.tags_edit.text().split(',') if t.strip()]
        return name, tags
