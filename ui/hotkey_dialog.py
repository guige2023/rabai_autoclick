"""Hotkey configuration dialog for RabAI AutoClick.

Provides a dialog for configuring global hotkeys for
workflow control and recording operations.
"""

from typing import Dict, List, Optional

from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtGui import QKeySequence
from PyQt5.QtWidgets import (
    QApplication, QDialog, QDialogButtonBox, QFormLayout,
    QGroupBox, QHBoxLayout, QLabel, QLineEdit, QPushButton,
    QVBoxLayout
)


class HotkeyEdit(QLineEdit):
    """Custom line edit for capturing hotkey input.
    
    Allows users to record keyboard shortcuts by pressing keys
    in the edit field. Supports modifier keys (Ctrl, Shift, Alt, Win)
    and regular keys, including function keys.
    """
    
    key_captured = pyqtSignal(str)
    
    # Mapping of special keys to their names
    SPECIAL_KEYS: Dict[int, str] = {
        Qt.Key_F1: 'f1', Qt.Key_F2: 'f2', Qt.Key_F3: 'f3', Qt.Key_F4: 'f4',
        Qt.Key_F5: 'f5', Qt.Key_F6: 'f6', Qt.Key_F7: 'f7', Qt.Key_F8: 'f8',
        Qt.Key_F9: 'f9', Qt.Key_F10: 'f10', Qt.Key_F11: 'f11', Qt.Key_F12: 'f12',
        Qt.Key_Space: 'space', Qt.Key_Tab: 'tab', Qt.Key_Return: 'enter',
        Qt.Key_Enter: 'enter', Qt.Key_Backspace: 'backspace',
        Qt.Key_Insert: 'insert', Qt.Key_Delete: 'delete',
        Qt.Key_Home: 'home', Qt.Key_End: 'end',
        Qt.Key_PageUp: 'pageup', Qt.Key_PageDown: 'pagedown',
        Qt.Key_Up: 'up', Qt.Key_Down: 'down', Qt.Key_Left: 'left',
        Qt.Key_Right: 'right', Qt.Key_Escape: 'esc',
    }
    
    def __init__(self, parent: Optional[QDialog] = None) -> None:
        """Initialize the hotkey edit widget.
        
        Args:
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self.setReadOnly(True)
        self.setPlaceholderText("按下快捷键...")
        self._captured_keys: List[str] = []
        self._recording: bool = False
    
    def mousePressEvent(self, event) -> None:
        """Handle mouse press to start recording.
        
        Args:
            event: Mouse press event.
        """
        self._recording = True
        self._captured_keys = []
        self.setText("...")
        self.setFocus()
        super().mousePressEvent(event)
    
    def keyPressEvent(self, event) -> None:
        """Handle key press for hotkey capture.
        
        Args:
            event: Key press event.
        """
        if not self._recording:
            super().keyPressEvent(event)
            return
        
        key = event.key()
        
        # Handle modifier-only keys
        if key in (Qt.Key_Control, Qt.Key_Shift, Qt.Key_Alt, Qt.Key_Meta):
            modifiers = []
            if event.modifiers() & Qt.ControlModifier:
                modifiers.append('ctrl')
            if event.modifiers() & Qt.ShiftModifier:
                modifiers.append('shift')
            if event.modifiers() & Qt.AltModifier:
                modifiers.append('alt')
            if event.modifiers() & Qt.MetaModifier:
                modifiers.append('win')
            
            self._captured_keys = modifiers
            self.setText(' + '.join(modifiers).upper() + ' + ...')
            return
        
        # Cancel on Escape
        if key == Qt.Key_Escape:
            self._recording = False
            self._captured_keys = []
            self.setText("")
            return
        
        # Clear on Backspace/Delete
        if key == Qt.Key_Backspace or key == Qt.Key_Delete:
            self._recording = False
            self._captured_keys = []
            self.setText("")
            return
        
        # Build modifier list
        modifiers: List[str] = []
        if event.modifiers() & Qt.ControlModifier:
            modifiers.append('ctrl')
        if event.modifiers() & Qt.ShiftModifier:
            modifiers.append('shift')
        if event.modifiers() & Qt.AltModifier:
            modifiers.append('alt')
        if event.modifiers() & Qt.MetaModifier:
            modifiers.append('win')
        
        # Get main key name
        key_name = self._get_key_name(key)
        if key_name:
            modifiers.append(key_name)
            hotkey = '+'.join(modifiers)
            self.setText(hotkey.upper().replace('+', ' + '))
            self._recording = False
            self.key_captured.emit(hotkey)
    
    def _get_key_name(self, key: int) -> Optional[str]:
        """Convert a Qt key code to string name.
        
        Args:
            key: Qt key code.
            
        Returns:
            String name of the key, or None if not representable.
        """
        if key in self.SPECIAL_KEYS:
            return self.SPECIAL_KEYS[key]
        
        # Number keys (0-9)
        if Qt.Key_0 <= key <= Qt.Key_9:
            return chr(key)
        
        # Letter keys (A-Z)
        if Qt.Key_A <= key <= Qt.Key_Z:
            return chr(key).lower()
        
        return None
    
    def set_hotkey(self, hotkey: str) -> None:
        """Set the displayed hotkey.
        
        Args:
            hotkey: Hotkey string to display.
        """
        if hotkey:
            self.setText(hotkey.upper().replace('+', ' + '))
        else:
            self.setText("")
    
    def get_hotkey(self) -> str:
        """Get the captured hotkey.
        
        Returns:
            Hotkey string in format like 'ctrl+shift+a'.
        """
        text = self.text().strip()
        if text and text != '...':
            return text.lower().replace(' ', '').replace('+', '+')
        return ""


class HotkeySettingsDialog(QDialog):
    """Dialog for configuring global hotkeys.
    
    Allows users to set hotkeys for workflow control (start/stop/pause),
    recording control, and display settings.
    """
    
    def __init__(
        self,
        current_hotkeys: Dict[str, str],
        parent: Optional[QDialog] = None
    ) -> None:
        """Initialize the hotkey settings dialog.
        
        Args:
            current_hotkeys: Dictionary of current hotkey assignments.
            parent: Optional parent widget.
        """
        super().__init__(parent)
        self.setWindowTitle("快捷键设置")
        self.setMinimumWidth(400)
        self.hotkeys: Dict[str, str] = current_hotkeys.copy()
        self._init_ui()
    
    def _init_ui(self) -> None:
        """Initialize the dialog UI components."""
        layout = QVBoxLayout(self)
        
        info_label = QLabel(
            "点击输入框后按下新的快捷键进行设置\n"
            "支持组合键，如: Ctrl+Shift+A"
        )
        info_label.setStyleSheet("color: gray; font-size: 11px;")
        layout.addWidget(info_label)
        
        # Workflow control group
        workflow_group = QGroupBox("工作流控制")
        form_layout = QFormLayout()
        
        self.start_edit = HotkeyEdit()
        self.start_edit.set_hotkey(self.hotkeys.get('start', 'f6'))
        self.start_edit.key_captured.connect(
            lambda k: self._on_key_captured('start', k)
        )
        form_layout.addRow("运行:", self.start_edit)
        
        self.stop_edit = HotkeyEdit()
        self.stop_edit.set_hotkey(self.hotkeys.get('stop', 'f7'))
        self.stop_edit.key_captured.connect(
            lambda k: self._on_key_captured('stop', k)
        )
        form_layout.addRow("停止:", self.stop_edit)
        
        self.pause_edit = HotkeyEdit()
        self.pause_edit.set_hotkey(self.hotkeys.get('pause', 'f8'))
        self.pause_edit.key_captured.connect(
            lambda k: self._on_key_captured('pause', k)
        )
        form_layout.addRow("暂停/继续:", self.pause_edit)
        
        workflow_group.setLayout(form_layout)
        layout.addWidget(workflow_group)
        
        # Recording control group
        record_group = QGroupBox("录屏控制")
        record_layout = QFormLayout()
        
        self.record_start_edit = HotkeyEdit()
        self.record_start_edit.set_hotkey(self.hotkeys.get('record_start', 'f9'))
        self.record_start_edit.key_captured.connect(
            lambda k: self._on_key_captured('record_start', k)
        )
        record_layout.addRow("开始录制:", self.record_start_edit)
        
        self.record_stop_edit = HotkeyEdit()
        self.record_stop_edit.set_hotkey(self.hotkeys.get('record_stop', 'f10'))
        self.record_stop_edit.key_captured.connect(
            lambda k: self._on_key_captured('record_stop', k)
        )
        record_layout.addRow("停止录制:", self.record_stop_edit)
        
        record_group.setLayout(record_layout)
        layout.addWidget(record_group)
        
        # Display control group
        display_group = QGroupBox("显示控制")
        display_layout = QFormLayout()
        
        self.display_edit = HotkeyEdit()
        self.display_edit.set_hotkey(self.hotkeys.get('display', 'f11'))
        self.display_edit.key_captured.connect(
            lambda k: self._on_key_captured('display', k)
        )
        display_layout.addRow("按键显示:", self.display_edit)
        
        display_group.setLayout(display_layout)
        layout.addWidget(display_group)
        
        # Reset button
        reset_btn = QPushButton("恢复默认设置")
        reset_btn.clicked.connect(self._reset_to_default)
        layout.addWidget(reset_btn)
        
        # Dialog buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self._on_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    
    def _on_key_captured(self, action: str, key: str) -> None:
        """Handle hotkey capture.
        
        Args:
            action: Action name being captured.
            key: Captured hotkey string.
        """
        self.hotkeys[action] = key
    
    def _reset_to_default(self) -> None:
        """Reset all hotkeys to default values."""
        self.start_edit.set_hotkey('f6')
        self.stop_edit.set_hotkey('f7')
        self.pause_edit.set_hotkey('f8')
        self.record_start_edit.set_hotkey('f9')
        self.record_stop_edit.set_hotkey('f10')
        self.display_edit.set_hotkey('f11')
        self.hotkeys = {
            'start': 'f6',
            'stop': 'f7',
            'pause': 'f8',
            'record_start': 'f9',
            'record_stop': 'f10',
            'display': 'f11'
        }
    
    def _on_accept(self) -> None:
        """Handle dialog acceptance, saving hotkey values."""
        self.hotkeys['start'] = self.start_edit.get_hotkey() or 'f6'
        self.hotkeys['stop'] = self.stop_edit.get_hotkey() or 'f7'
        self.hotkeys['pause'] = self.pause_edit.get_hotkey() or 'f8'
        self.hotkeys['record_start'] = self.record_start_edit.get_hotkey() or 'f9'
        self.hotkeys['record_stop'] = self.record_stop_edit.get_hotkey() or 'f10'
        self.hotkeys['display'] = self.display_edit.get_hotkey() or 'f11'
        self.accept()
    
    def get_hotkeys(self) -> Dict[str, str]:
        """Get the configured hotkeys.
        
        Returns:
            Dictionary of action name to hotkey string.
        """
        return self.hotkeys
