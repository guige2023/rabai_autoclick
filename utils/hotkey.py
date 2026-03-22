import json
import os
import platform
import threading
from typing import Dict, Optional, Callable
from PyQt5.QtCore import QObject, pyqtSignal

IS_MACOS = platform.system() == 'Darwin'

try:
    from pynput import keyboard as pynput_keyboard
    PYNPUT_AVAILABLE = True
except ImportError:
    PYNPUT_AVAILABLE = False


class HotkeyManager(QObject):
    start_triggered = pyqtSignal()
    stop_triggered = pyqtSignal()
    pause_triggered = pyqtSignal()
    custom_triggered = pyqtSignal(str)
    record_start_triggered = pyqtSignal()
    record_stop_triggered = pyqtSignal()
    display_triggered = pyqtSignal()
    
    DEFAULT_HOTKEYS = {
        'start': 'f6',
        'stop': 'f7',
        'pause': 'f8',
        'record_start': 'f9',
        'record_stop': 'f10',
        'display': 'f11'
    }
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._hotkeys_enabled = False
        self._keyboard_available = False
        self._registered_hotkeys: Dict[str, str] = {}
        self._custom_hotkeys: Dict[str, Callable] = {}
        self._config_path = None
        self._pynput_listener = None
        self._pressed_keys = set()
        self._listener_started = False
        self._check_keyboard()
    
    def _check_keyboard(self):
        if IS_MACOS:
            self._keyboard_available = PYNPUT_AVAILABLE
            return
        
        try:
            import keyboard
            self._keyboard_available = True
        except ImportError:
            self._keyboard_available = PYNPUT_AVAILABLE
        except Exception:
            self._keyboard_available = PYNPUT_AVAILABLE
    
    def set_config_path(self, path: str):
        self._config_path = path
    
    def load_config(self) -> Dict[str, str]:
        if self._config_path and os.path.exists(self._config_path):
            try:
                with open(self._config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    return config.get('hotkeys', self.DEFAULT_HOTKEYS.copy())
            except Exception as e:
                import logging
                logging.getLogger("RabAI").warning(f"加载热键配置失败: {e}")
        return self.DEFAULT_HOTKEYS.copy()
    
    def save_config(self, hotkeys: Dict[str, str]) -> bool:
        if not self._config_path:
            return False
        try:
            config = {}
            if os.path.exists(self._config_path):
                with open(self._config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            config['hotkeys'] = hotkeys
            with open(self._config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
                import logging
                logging.getLogger("RabAI").warning(f"保存热键配置失败: {e}")
            return False
    
    def _normalize_key(self, key_str: str) -> str:
        return key_str.lower().replace(' ', '+').replace('_', '+')
    
    def _get_key_name(self, key) -> str:
        try:
            if hasattr(key, 'char') and key.char:
                return key.char.lower()
            elif hasattr(key, 'name'):
                name = key.name.lower()
                name_map = {
                    'cmd_l': 'cmd', 'cmd_r': 'cmd',
                    'ctrl_l': 'ctrl', 'ctrl_r': 'ctrl',
                    'alt_l': 'alt', 'alt_r': 'alt',
                    'shift_l': 'shift', 'shift_r': 'shift',
                    'page_up': 'pageup', 'page_down': 'pagedown',
                }
                return name_map.get(name, name)
        except Exception:
            pass
        return ''
    
    def _check_hotkey_match(self, pressed_keys: set, target_key: str) -> bool:
        target_parts = set(self._normalize_key(target_key).split('+'))
        return target_parts == pressed_keys
    
    def _on_pynput_press(self, key):
        key_name = self._get_key_name(key)
        if key_name:
            self._pressed_keys.add(key_name)
            
            for action, hotkey in self._registered_hotkeys.items():
                if self._check_hotkey_match(self._pressed_keys, hotkey):
                    if action == 'start':
                        self.start_triggered.emit()
                    elif action == 'stop':
                        self.stop_triggered.emit()
                    elif action == 'pause':
                        self.pause_triggered.emit()
                    elif action == 'record_start':
                        self.record_start_triggered.emit()
                    elif action == 'record_stop':
                        self.record_stop_triggered.emit()
                    elif action == 'display':
                        self.display_triggered.emit()
                    break
    
    def _on_pynput_release(self, key):
        key_name = self._get_key_name(key)
        if key_name and key_name in self._pressed_keys:
            self._pressed_keys.discard(key_name)
    
    def register_hotkeys(self, 
                         start_key: str = 'f6',
                         stop_key: str = 'f7',
                         pause_key: str = 'f8',
                         record_start_key: str = 'f9',
                         record_stop_key: str = 'f10',
                         display_key: str = 'f11',
                         custom_hotkeys: Dict[str, str] = None) -> bool:
        if not self._keyboard_available:
            return False
        
        self._registered_hotkeys = {
            'start': start_key,
            'stop': stop_key,
            'pause': pause_key,
            'record_start': record_start_key,
            'record_stop': record_stop_key,
            'display': display_key,
        }
        
        if IS_MACOS and PYNPUT_AVAILABLE:
            if self._listener_started and self._pynput_listener:
                print("[DEBUG] Listener already running, just updating hotkeys")
                self._hotkeys_enabled = True
                return True
            
            try:
                print("[DEBUG] Starting new pynput listener")
                self._pynput_listener = pynput_keyboard.Listener(
                    on_press=self._on_pynput_press,
                    on_release=self._on_pynput_release
                )
                self._pynput_listener.start()
                self._listener_started = True
                self._hotkeys_enabled = True
                return True
            except Exception as e:
                print(f"pynput注册热键失败: {e}")
                return False
        
        try:
            import keyboard
            
            keyboard.unhook_all()
            keyboard.add_hotkey(start_key, self._on_start)
            keyboard.add_hotkey(stop_key, self._on_stop)
            keyboard.add_hotkey(pause_key, self._on_pause)
            keyboard.add_hotkey(record_start_key, self._on_record_start)
            keyboard.add_hotkey(record_stop_key, self._on_record_stop)
            keyboard.add_hotkey(display_key, self._on_display)
            
            if custom_hotkeys:
                for name, key in custom_hotkeys.items():
                    callback = self._custom_hotkeys.get(name)
                    if callback:
                        keyboard.add_hotkey(key, lambda n=name: self._on_custom(n))
                        self._registered_hotkeys[f'custom_{name}'] = key
            
            self._hotkeys_enabled = True
            return True
        except Exception as e:
            print(f"注册热键失败: {e}")
            return False
    
    def register_custom_hotkey(self, name: str, key: str, callback: Callable) -> bool:
        if not self._keyboard_available:
            return False
        
        self._custom_hotkeys[name] = callback
        self._registered_hotkeys[f'custom_{name}'] = key
        return True
    
    def unregister_hotkeys(self) -> None:
        print("[DEBUG] unregister_hotkeys called")
        
        self._registered_hotkeys.clear()
        self._pressed_keys.clear()
        self._hotkeys_enabled = False
        
        if not IS_MACOS:
            try:
                import keyboard
                keyboard.unhook_all()
            except Exception:
                pass
        
        print("[DEBUG] unregister_hotkeys done")
    
    def update_hotkey(self, action: str, new_key: str) -> bool:
        if not self._keyboard_available:
            return False
        
        self._registered_hotkeys[action] = new_key
        return True
    
    def _on_start(self):
        self.start_triggered.emit()
    
    def _on_stop(self):
        self.stop_triggered.emit()
    
    def _on_pause(self):
        self.pause_triggered.emit()
    
    def _on_record_start(self):
        self.record_start_triggered.emit()
    
    def _on_record_stop(self):
        self.record_stop_triggered.emit()
    
    def _on_display(self):
        self.display_triggered.emit()
    
    def _on_custom(self, name: str):
        callback = self._custom_hotkeys.get(name)
        if callback:
            callback()
        self.custom_triggered.emit(name)
    
    def is_available(self) -> bool:
        return self._keyboard_available
    
    def is_enabled(self) -> bool:
        return self._hotkeys_enabled
    
    def get_registered_hotkeys(self) -> Dict[str, str]:
        return self._registered_hotkeys.copy()
    
    @staticmethod
    def parse_hotkey(key_str: str) -> str:
        key_str = key_str.strip().lower()
        key_str = key_str.replace(' ', '+')
        key_str = key_str.replace('ctrl', 'ctrl')
        key_str = key_str.replace('alt', 'alt')
        key_str = key_str.replace('shift', 'shift')
        key_str = key_str.replace('win', 'win')
        return key_str
    
    @staticmethod
    def format_hotkey(key_str: str) -> str:
        parts = key_str.split('+')
        parts = [p.strip().upper() for p in parts]
        return ' + '.join(parts)
