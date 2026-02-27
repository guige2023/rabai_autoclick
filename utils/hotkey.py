import json
import os
from typing import Dict, Optional, Callable
from PyQt5.QtCore import QObject, pyqtSignal


class HotkeyManager(QObject):
    start_triggered = pyqtSignal()
    stop_triggered = pyqtSignal()
    pause_triggered = pyqtSignal()
    custom_triggered = pyqtSignal(str)
    
    DEFAULT_HOTKEYS = {
        'start': 'f6',
        'stop': 'f7',
        'pause': 'f8'
    }
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._hotkeys_enabled = False
        self._keyboard_available = False
        self._registered_hotkeys: Dict[str, str] = {}
        self._custom_hotkeys: Dict[str, Callable] = {}
        self._config_path = None
        self._check_keyboard()
    
    def _check_keyboard(self):
        try:
            import keyboard
            self._keyboard_available = True
        except ImportError:
            self._keyboard_available = False
    
    def set_config_path(self, path: str):
        self._config_path = path
    
    def load_config(self) -> Dict[str, str]:
        if self._config_path and os.path.exists(self._config_path):
            try:
                with open(self._config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    return config.get('hotkeys', self.DEFAULT_HOTKEYS.copy())
            except:
                pass
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
        except:
            return False
    
    def register_hotkeys(self, 
                         start_key: str = 'f6',
                         stop_key: str = 'f7',
                         pause_key: str = 'f8',
                         custom_hotkeys: Dict[str, str] = None) -> bool:
        if not self._keyboard_available:
            return False
        
        self.unregister_hotkeys()
        
        try:
            import keyboard
            
            keyboard.add_hotkey(start_key, self._on_start)
            self._registered_hotkeys['start'] = start_key
            
            keyboard.add_hotkey(stop_key, self._on_stop)
            self._registered_hotkeys['stop'] = stop_key
            
            keyboard.add_hotkey(pause_key, self._on_pause)
            self._registered_hotkeys['pause'] = pause_key
            
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
        
        try:
            import keyboard
            
            old_key = self._registered_hotkeys.get(f'custom_{name}')
            if old_key:
                try:
                    keyboard.remove_hotkey(old_key)
                except:
                    pass
            
            self._custom_hotkeys[name] = callback
            keyboard.add_hotkey(key, lambda n=name: self._on_custom(n))
            self._registered_hotkeys[f'custom_{name}'] = key
            return True
        except Exception as e:
            print(f"注册自定义热键失败: {e}")
            return False
    
    def unregister_hotkeys(self) -> None:
        if not self._keyboard_available:
            return
        
        try:
            import keyboard
            keyboard.unhook_all()
            self._registered_hotkeys.clear()
            self._hotkeys_enabled = False
        except:
            pass
    
    def update_hotkey(self, action: str, new_key: str) -> bool:
        if not self._keyboard_available:
            return False
        
        try:
            import keyboard
            
            old_key = self._registered_hotkeys.get(action)
            if old_key:
                try:
                    keyboard.remove_hotkey(old_key)
                except:
                    pass
            
            if action == 'start':
                keyboard.add_hotkey(new_key, self._on_start)
            elif action == 'stop':
                keyboard.add_hotkey(new_key, self._on_stop)
            elif action == 'pause':
                keyboard.add_hotkey(new_key, self._on_pause)
            else:
                return False
            
            self._registered_hotkeys[action] = new_key
            return True
        except Exception as e:
            print(f"更新热键失败: {e}")
            return False
    
    def _on_start(self):
        self.start_triggered.emit()
    
    def _on_stop(self):
        self.stop_triggered.emit()
    
    def _on_pause(self):
        self.pause_triggered.emit()
    
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
