from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QLineEdit, QFormLayout, QDialogButtonBox, QMessageBox,
    QGroupBox, QApplication
)
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtGui import QKeySequence


class HotkeyEdit(QLineEdit):
    key_captured = pyqtSignal(str)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setReadOnly(True)
        self.setPlaceholderText("按下快捷键...")
        self._captured_keys = []
        self._recording = False
    
    def mousePressEvent(self, event):
        self._recording = True
        self._captured_keys = []
        self.setText("...")
        self.setFocus()
        super().mousePressEvent(event)
    
    def keyPressEvent(self, event):
        if not self._recording:
            super().keyPressEvent(event)
            return
        
        key = event.key()
        
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
        
        if key == Qt.Key_Escape:
            self._recording = False
            self._captured_keys = []
            self.setText("")
            return
        
        if key == Qt.Key_Backspace or key == Qt.Key_Delete:
            self._recording = False
            self._captured_keys = []
            self.setText("")
            return
        
        modifiers = []
        if event.modifiers() & Qt.ControlModifier:
            modifiers.append('ctrl')
        if event.modifiers() & Qt.ShiftModifier:
            modifiers.append('shift')
        if event.modifiers() & Qt.AltModifier:
            modifiers.append('alt')
        if event.modifiers() & Qt.MetaModifier:
            modifiers.append('win')
        
        key_name = self._get_key_name(key)
        if key_name:
            modifiers.append(key_name)
            hotkey = '+'.join(modifiers)
            self.setText(hotkey.upper().replace('+', ' + '))
            self._recording = False
            self.key_captured.emit(hotkey)
    
    def _get_key_name(self, key):
        special_keys = {
            Qt.Key_F1: 'f1', Qt.Key_F2: 'f2', Qt.Key_F3: 'f3', Qt.Key_F4: 'f4',
            Qt.Key_F5: 'f5', Qt.Key_F6: 'f6', Qt.Key_F7: 'f7', Qt.Key_F8: 'f8',
            Qt.Key_F9: 'f9', Qt.Key_F10: 'f10', Qt.Key_F11: 'f11', Qt.Key_F12: 'f12',
            Qt.Key_Space: 'space', Qt.Key_Tab: 'tab', Qt.Key_Return: 'enter',
            Qt.Key_Enter: 'enter', Qt.Key_Backspace: 'backspace',
            Qt.Key_Insert: 'insert', Qt.Key_Delete: 'delete',
            Qt.Key_Home: 'home', Qt.Key_End: 'end',
            Qt.Key_PageUp: 'pageup', Qt.Key_PageDown: 'pagedown',
            Qt.Key_Up: 'up', Qt.Key_Down: 'down', Qt.Key_Left: 'left', Qt.Key_Right: 'right',
            Qt.Key_Escape: 'esc',
        }
        
        if key in special_keys:
            return special_keys[key]
        
        if Qt.Key_0 <= key <= Qt.Key_9:
            return chr(key)
        if Qt.Key_A <= key <= Qt.Key_Z:
            return chr(key).lower()
        
        return None
    
    def set_hotkey(self, hotkey: str):
        if hotkey:
            self.setText(hotkey.upper().replace('+', ' + '))
        else:
            self.setText("")
    
    def get_hotkey(self) -> str:
        text = self.text().strip()
        if text and text != '...':
            return text.lower().replace(' ', '').replace('+', '+')
        return ""


class HotkeySettingsDialog(QDialog):
    def __init__(self, current_hotkeys: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("快捷键设置")
        self.setMinimumWidth(400)
        self.hotkeys = current_hotkeys.copy()
        self._init_ui()
    
    def _init_ui(self):
        layout = QVBoxLayout(self)
        
        info_label = QLabel("点击输入框后按下新的快捷键进行设置\n支持组合键，如: Ctrl+Shift+A")
        info_label.setStyleSheet("color: gray; font-size: 11px;")
        layout.addWidget(info_label)
        
        hotkey_group = QGroupBox("快捷键配置")
        form_layout = QFormLayout()
        
        self.start_edit = HotkeyEdit()
        self.start_edit.set_hotkey(self.hotkeys.get('start', 'f6'))
        self.start_edit.key_captured.connect(lambda k: self._on_key_captured('start', k))
        form_layout.addRow("运行:", self.start_edit)
        
        self.stop_edit = HotkeyEdit()
        self.stop_edit.set_hotkey(self.hotkeys.get('stop', 'f7'))
        self.stop_edit.key_captured.connect(lambda k: self._on_key_captured('stop', k))
        form_layout.addRow("停止:", self.stop_edit)
        
        self.pause_edit = HotkeyEdit()
        self.pause_edit.set_hotkey(self.hotkeys.get('pause', 'f8'))
        self.pause_edit.key_captured.connect(lambda k: self._on_key_captured('pause', k))
        form_layout.addRow("暂停/继续:", self.pause_edit)
        
        hotkey_group.setLayout(form_layout)
        layout.addWidget(hotkey_group)
        
        reset_btn = QPushButton("恢复默认设置")
        reset_btn.clicked.connect(self._reset_to_default)
        layout.addWidget(reset_btn)
        
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(self._on_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    
    def _on_key_captured(self, action: str, key: str):
        self.hotkeys[action] = key
    
    def _reset_to_default(self):
        self.start_edit.set_hotkey('f6')
        self.stop_edit.set_hotkey('f7')
        self.pause_edit.set_hotkey('f8')
        self.hotkeys = {'start': 'f6', 'stop': 'f7', 'pause': 'f8'}
    
    def _on_accept(self):
        self.hotkeys['start'] = self.start_edit.get_hotkey() or 'f6'
        self.hotkeys['stop'] = self.stop_edit.get_hotkey() or 'f7'
        self.hotkeys['pause'] = self.pause_edit.get_hotkey() or 'f8'
        self.accept()
    
    def get_hotkeys(self) -> dict:
        return self.hotkeys
