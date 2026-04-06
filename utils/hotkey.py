"""Hotkey manager for RabAI AutoClick.

Provides global hotkey registration and management using pynput (macOS)
or keyboard library (Windows), with PyQt signal support.
"""

import json
import os
import platform
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from PyQt5.QtCore import QObject, pyqtSignal


# Platform detection
IS_MACOS: bool = platform.system() == 'Darwin'

# Check pynput availability
try:
    from pynput import keyboard as pynput_keyboard
    PYNPUT_AVAILABLE: bool = True
except ImportError:
    PYNPUT_AVAILABLE = False


class HotkeyManager(QObject):
    """Global hotkey manager with PyQt signal support.
    
    Manages global hotkey registration for workflow control
    (start, stop, pause) and custom hotkeys.
    """
    
    # PyQt signals for hotkey events
    start_triggered = pyqtSignal()
    stop_triggered = pyqtSignal()
    pause_triggered = pyqtSignal()
    custom_triggered = pyqtSignal(str)
    record_start_triggered = pyqtSignal()
    record_stop_triggered = pyqtSignal()
    display_triggered = pyqtSignal()
    
    # Default hotkey bindings
    DEFAULT_HOTKEYS: Dict[str, str] = {
        'start': 'f6',
        'stop': 'f7',
        'pause': 'f8',
        'record_start': 'f9',
        'record_stop': 'f10',
        'display': 'f11'
    }
    
    def __init__(self, parent: Optional[Any] = None) -> None:
        """Initialize the hotkey manager.
        
        Args:
            parent: Optional parent QObject.
        """
        super().__init__(parent)
        self._hotkeys_enabled: bool = False
        self._keyboard_available: bool = False
        self._registered_hotkeys: Dict[str, str] = {}
        self._custom_hotkeys: Dict[str, Callable[[], None]] = {}
        self._config_path: Optional[str] = None
        self._pynput_listener: Optional[Any] = None
        self._pressed_keys: Set[str] = set()
        self._listener_started: bool = False
        self._check_keyboard()
    
    def _check_keyboard(self) -> None:
        """Check keyboard library availability."""
        if IS_MACOS:
            self._keyboard_available = PYNPUT_AVAILABLE
            return
        
        try:
            import keyboard  # noqa: F401
            self._keyboard_available = True
        except ImportError:
            self._keyboard_available = PYNPUT_AVAILABLE
        except Exception:
            self._keyboard_available = PYNPUT_AVAILABLE
    
    def set_config_path(self, path: str) -> None:
        """Set the configuration file path for hotkey persistence.
        
        Args:
            path: Path to the config JSON file.
        """
        self._config_path = path
    
    def load_config(self) -> Dict[str, str]:
        """Load hotkey configuration from file.
        
        Returns:
            Dictionary of action -> hotkey string mappings.
        """
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
        """Save hotkey configuration to file.
        
        Args:
            hotkeys: Dictionary of action -> hotkey string mappings.
            
        Returns:
            True if saved successfully, False otherwise.
        """
        if not self._config_path:
            return False
        try:
            config: Dict[str, Any] = {}
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
    
    @staticmethod
    def _normalize_key(key_str: str) -> str:
        """Normalize a key string for comparison.
        
        Args:
            key_str: Raw key string (e.g., 'Ctrl+F9').
            
        Returns:
            Normalized key string (e.g., 'ctrl+f9').
        """
        return key_str.lower().replace(' ', '+').replace('_', '+')
    
    def _get_key_name(self, key: Any) -> str:
        """Get the normalized name from a pynput key object.
        
        Args:
            key: A pynput key object.
            
        Returns:
            Normalized key name string.
        """
        try:
            if hasattr(key, 'char') and key.char:
                return key.char.lower()
            elif hasattr(key, 'name'):
                name = key.name.lower()
                name_map: Dict[str, str] = {
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
    
    def _check_hotkey_match(
        self, 
        pressed_keys: Set[str], 
        target_key: str
    ) -> bool:
        """Check if pressed keys match a target hotkey combination.
        
        Args:
            pressed_keys: Set of currently pressed key names.
            target_key: Target hotkey string (e.g., 'ctrl+shift+a').
            
        Returns:
            True if the pressed keys exactly match the target.
        """
        target_parts = set(self._normalize_key(target_key).split('+'))
        return target_parts == pressed_keys
    
    def _on_pynput_press(self, key: Any) -> None:
        """Handle pynput key press event.
        
        Args:
            key: The pynput key object.
        """
        key_name = self._get_key_name(key)
        if key_name:
            self._pressed_keys.add(key_name)
            
            for action, hotkey in self._registered_hotkeys.items():
                if self._check_hotkey_match(self._pressed_keys, hotkey):
                    self._emit_action(action)
                    break
    
    def _on_pynput_release(self, key: Any) -> None:
        """Handle pynput key release event.
        
        Args:
            key: The pynput key object.
        """
        key_name = self._get_key_name(key)
        if key_name and key_name in self._pressed_keys:
            self._pressed_keys.discard(key_name)
    
    def _emit_action(self, action: str) -> None:
        """Emit the appropriate signal for an action.
        
        Args:
            action: Action name (e.g., 'start', 'stop', 'pause').
        """
        action_map: Dict[str, Callable[[], None]] = {
            'start': self.start_triggered.emit,
            'stop': self.stop_triggered.emit,
            'pause': self.pause_triggered.emit,
            'record_start': self.record_start_triggered.emit,
            'record_stop': self.record_stop_triggered.emit,
            'display': self.display_triggered.emit,
        }
        
        emit_fn = action_map.get(action)
        if emit_fn:
            emit_fn()
    
    def register_hotkeys(
        self,
        start_key: str = 'f6',
        stop_key: str = 'f7',
        pause_key: str = 'f8',
        record_start_key: str = 'f9',
        record_stop_key: str = 'f10',
        display_key: str = 'f11',
        custom_hotkeys: Optional[Dict[str, str]] = None
    ) -> bool:
        """Register global hotkeys.
        
        Args:
            start_key: Hotkey for start action.
            stop_key: Hotkey for stop action.
            pause_key: Hotkey for pause action.
            record_start_key: Hotkey for record start.
            record_stop_key: Hotkey for record stop.
            display_key: Hotkey for display toggle.
            custom_hotkeys: Additional custom hotkeys.
            
        Returns:
            True if registered successfully, False otherwise.
        """
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
                self._hotkeys_enabled = True
                return True
            
            try:
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
                        def make_callback(n: str) -> Callable[[], None]:
                            return lambda: self._on_custom(n)
                        keyboard.add_hotkey(key, make_callback(name))
                        self._registered_hotkeys[f'custom_{name}'] = key
            
            self._hotkeys_enabled = True
            return True
        except Exception as e:
            print(f"注册热键失败: {e}")
            return False
    
    def register_custom_hotkey(
        self, 
        name: str, 
        key: str, 
        callback: Callable[[], None]
    ) -> bool:
        """Register a custom hotkey with a callback.
        
        Args:
            name: Unique name for this hotkey.
            key: Hotkey string.
            callback: Function to call when hotkey is pressed.
            
        Returns:
            True if registered successfully, False otherwise.
        """
        if not self._keyboard_available:
            return False
        
        self._custom_hotkeys[name] = callback
        self._registered_hotkeys[f'custom_{name}'] = key
        return True
    
    def unregister_hotkeys(self) -> None:
        """Unregister all hotkeys and clean up listeners."""
        self._registered_hotkeys.clear()
        self._pressed_keys.clear()
        self._hotkeys_enabled = False
        
        if not IS_MACOS:
            try:
                import keyboard
                keyboard.unhook_all()
            except Exception:
                pass
    
    def update_hotkey(self, action: str, new_key: str) -> bool:
        """Update a single hotkey binding.
        
        Args:
            action: Action name to update.
            new_key: New hotkey string.
            
        Returns:
            True if updated successfully, False otherwise.
        """
        if not self._keyboard_available:
            return False
        
        self._registered_hotkeys[action] = new_key
        return True
    
    def _on_start(self) -> None:
        """Internal start action handler."""
        self.start_triggered.emit()
    
    def _on_stop(self) -> None:
        """Internal stop action handler."""
        self.stop_triggered.emit()
    
    def _on_pause(self) -> None:
        """Internal pause action handler."""
        self.pause_triggered.emit()
    
    def _on_record_start(self) -> None:
        """Internal record start action handler."""
        self.record_start_triggered.emit()
    
    def _on_record_stop(self) -> None:
        """Internal record stop action handler."""
        self.record_stop_triggered.emit()
    
    def _on_display(self) -> None:
        """Internal display toggle action handler."""
        self.display_triggered.emit()
    
    def _on_custom(self, name: str) -> None:
        """Internal custom action handler.
        
        Args:
            name: Name of the custom action.
        """
        callback = self._custom_hotkeys.get(name)
        if callback:
            callback()
        self.custom_triggered.emit(name)
    
    def is_available(self) -> bool:
        """Check if keyboard hotkeys are available.
        
        Returns:
            True if keyboard library is available, False otherwise.
        """
        return self._keyboard_available
    
    def is_enabled(self) -> bool:
        """Check if hotkeys are currently enabled.
        
        Returns:
            True if hotkeys are registered and enabled.
        """
        return self._hotkeys_enabled
    
    def get_registered_hotkeys(self) -> Dict[str, str]:
        """Get a copy of all registered hotkeys.
        
        Returns:
            Dictionary of action -> hotkey string mappings.
        """
        return self._registered_hotkeys.copy()
    
    @staticmethod
    def parse_hotkey(key_str: str) -> str:
        """Parse and normalize a hotkey string.
        
        Args:
            key_str: Raw hotkey string from user input.
            
        Returns:
            Normalized hotkey string.
        """
        key_str = key_str.strip().lower()
        key_str = key_str.replace(' ', '+')
        key_str = key_str.replace('ctrl', 'ctrl')
        key_str = key_str.replace('alt', 'alt')
        key_str = key_str.replace('shift', 'shift')
        key_str = key_str.replace('win', 'win')
        return key_str
    
    @staticmethod
    def format_hotkey(key_str: str) -> str:
        """Format a hotkey string for display.
        
        Args:
            key_str: Normalized hotkey string.
            
        Returns:
            Display-formatted hotkey string (e.g., 'Ctrl + Shift + A').
        """
        parts = key_str.split('+')
        parts = [p.strip().upper() for p in parts]
        return ' + '.join(parts)
