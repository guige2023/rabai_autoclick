"""Recording utilities for RabAI AutoClick.

Provides action recording functionality using pynput or pyautogui
for capturing mouse and keyboard events during workflow creation.
"""

import os
import sys
import time
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from PyQt5.QtCore import QObject, pyqtSignal, QTimer


# Add project root to path
sys.path.insert(0, os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
))


# Check library availability
try:
    from pynput import mouse, keyboard
    PYNPUT_AVAILABLE: bool = True
except ImportError:
    PYNPUT_AVAILABLE = False

try:
    import pyautogui
    PYAUTOGUI_AVAILABLE: bool = True
except ImportError:
    PYAUTOGUI_AVAILABLE = False


def check_pynput_permission() -> bool:
    """Check if pynput has permission to capture input.
    
    Returns:
        True if pynput can capture input, False otherwise.
    """
    if not PYNPUT_AVAILABLE:
        return False
    try:
        from pynput import mouse
        listener = mouse.Listener(lambda x: None)
        listener.start()
        listener.stop()
        return True
    except Exception:
        return False


class RecordedAction:
    """Represents a single recorded action.
    
    Attributes:
        action_type: Type of action ('click', 'scroll', 'key_press', etc.).
        timestamp: Time when action occurred (seconds since epoch).
        params: Dictionary of action parameters.
    """
    
    def __init__(
        self,
        action_type: str,
        timestamp: float,
        params: Dict[str, Any]
    ) -> None:
        """Initialize a recorded action.
        
        Args:
            action_type: Type of action.
            timestamp: Timestamp of action.
            params: Action parameters dictionary.
        """
        self.action_type: str = action_type
        self.timestamp: float = timestamp
        self.params: Dict[str, Any] = params
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation.
        
        Returns:
            Dictionary with action_type, timestamp, and params.
        """
        return {
            'action_type': self.action_type,
            'timestamp': self.timestamp,
            'params': self.params
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RecordedAction':
        """Create RecordedAction from dictionary.
        
        Args:
            data: Dictionary with action data.
            
        Returns:
            New RecordedAction instance.
        """
        return cls(
            action_type=data['action_type'],
            timestamp=data['timestamp'],
            params=data['params']
        )


class RecordingManager(QObject):
    """Manages action recording for workflow capture.
    
    Supports both pynput (preferred) and pyautogui (fallback)
    for capturing mouse and keyboard events.
    """
    
    action_recorded = pyqtSignal(str, dict)
    recording_started = pyqtSignal()
    recording_stopped = pyqtSignal(list)
    
    def __init__(self, parent: Optional[QObject] = None) -> None:
        """Initialize the recording manager.
        
        Args:
            parent: Optional parent QObject.
        """
        super().__init__(parent)
        self._is_recording: bool = False
        self._actions: List[RecordedAction] = []
        self._start_time: float = 0
        self._mouse_listener: Optional[Any] = None
        self._keyboard_listener: Optional[Any] = None
        self._last_action_time: float = 0
        self._min_interval: float = 0.1
        self._initialized: bool = False
        self._use_pyautogui: bool = False
        self._polling_timer: Optional[QTimer] = None
        self._last_mouse_pos: Optional[Any] = None
        self._last_mouse_buttons: Set[str] = set()
        self._pressed_keys: Set[str] = set()
        self._modifier_keys: Set[str] = {
            'shift', 'ctrl', 'alt', 'cmd', 'command', 'option', 'control'
        }
        
        if PYNPUT_AVAILABLE:
            try:
                from pynput import mouse as mouse_module
                from pynput import keyboard as keyboard_module
                self._mouse_module = mouse_module
                self._keyboard_module = keyboard_module
                self._initialized = True
                print("[Recording] pynput initialized successfully")
            except Exception as e:
                print(f"[Recording] Failed to initialize pynput: {e}")
                self._initialized = False
        
        if not self._initialized and PYAUTOGUI_AVAILABLE:
            self._use_pyautogui = True
            self._initialized = True
            print("[Recording] Using pyautogui fallback")
    
    def is_recording(self) -> bool:
        """Check if recording is in progress.
        
        Returns:
            True if recording, False otherwise.
        """
        return self._is_recording
    
    def start_recording(self) -> bool:
        """Start recording actions.
        
        Returns:
            True if recording started successfully.
        """
        if not self._initialized:
            print("[Recording] Not initialized")
            return False
        
        if self._is_recording:
            return False
        
        self._actions = []
        self._start_time = time.time()
        self._is_recording = True
        self._last_action_time = 0
        self._last_mouse_pos = pyautogui.position()
        self._last_mouse_buttons = set()
        
        if self._use_pyautogui:
            return self._start_pyautogui_recording()
        else:
            return self._start_pynput_recording()
    
    def _start_pynput_recording(self) -> bool:
        """Start recording using pynput.
        
        Returns:
            True if started successfully.
        """
        try:
            self._pressed_keys = set()
            
            self._mouse_listener = self._mouse_module.Listener(
                on_click=self._on_mouse_click,
                on_scroll=self._on_mouse_scroll
            )
            
            self._keyboard_listener = self._keyboard_module.Listener(
                on_press=self._on_key_press,
                on_release=self._on_key_release
            )
            
            self._mouse_listener.start()
            self._keyboard_listener.start()
            
            self.recording_started.emit()
            return True
        except Exception as e:
            print(f"[Recording] Failed to start pynput recording: {e}")
            self._is_recording = False
            self._use_pyautogui = True
            return self._start_pyautogui_recording()
    
    def _start_pyautogui_recording(self) -> bool:
        """Start recording using pyautogui polling.
        
        Returns:
            True if started successfully.
        """
        try:
            self._polling_timer = QTimer(self)
            self._polling_timer.timeout.connect(self._poll_input)
            self._polling_timer.start(50)
            
            self.recording_started.emit()
            return True
        except Exception as e:
            print(f"[Recording] Failed to start pyautogui recording: {e}")
            self._is_recording = False
            return False
    
    def _poll_input(self) -> None:
        """Poll for mouse position changes (pyautogui fallback)."""
        if not self._is_recording:
            return
        
        try:
            current_pos = pyautogui.position()
            if current_pos != self._last_mouse_pos:
                self._last_mouse_pos = current_pos
        except Exception:
            pass
    
    def stop_recording(self) -> List[RecordedAction]:
        """Stop recording and return captured actions.
        
        Returns:
            List of RecordedAction objects captured during recording.
        """
        if not self._is_recording:
            return []
        
        self._is_recording = False
        
        if self._use_pyautogui:
            if self._polling_timer:
                self._polling_timer.stop()
                self._polling_timer = None
        else:
            if self._mouse_listener:
                self._mouse_listener.stop()
                try:
                    self._mouse_listener.join(timeout=1.0)
                except Exception as e:
                    import logging
                    logging.getLogger("RabAI").debug(
                        f"停止鼠标监听器失败: {e}"
                    )
                self._mouse_listener = None
            
            if self._keyboard_listener:
                self._keyboard_listener.stop()
                try:
                    self._keyboard_listener.join(timeout=1.0)
                except Exception as e:
                    import logging
                    logging.getLogger("RabAI").debug(
                        f"停止键盘监听器失败: {e}"
                    )
                self._keyboard_listener = None
        
        self.recording_stopped.emit(self._actions)
        return self._actions.copy()
    
    def _on_mouse_click(
        self,
        x: int,
        y: int,
        button: Any,
        pressed: bool
    ) -> None:
        """Handle mouse click event from pynput.
        
        Args:
            x: Mouse X coordinate.
            y: Mouse Y coordinate.
            button: Mouse button that was clicked.
            pressed: True if button was pressed, False if released.
        """
        if not self._is_recording:
            return
        
        current_time = time.time()
        if current_time - self._last_action_time < self._min_interval:
            return
        
        self._last_action_time = current_time
        
        button_name = str(button).split('.')[-1]
        action_type = 'mouse_click'
        
        if pressed:
            action = RecordedAction(
                action_type=action_type,
                timestamp=current_time - self._start_time,
                params={
                    'x': x,
                    'y': y,
                    'button': button_name,
                    'pressed': True
                }
            )
            self._actions.append(action)
            self.action_recorded.emit(action_type, action.params)
    
    def _on_mouse_scroll(
        self,
        x: int,
        y: int,
        dx: int,
        dy: int
    ) -> None:
        """Handle mouse scroll event from pynput.
        
        Args:
            x: Mouse X coordinate.
            y: Mouse Y coordinate.
            dx: Horizontal scroll amount.
            dy: Vertical scroll amount.
        """
        if not self._is_recording:
            return
        
        current_time = time.time()
        if current_time - self._last_action_time < self._min_interval:
            return
        
        self._last_action_time = current_time
        
        action = RecordedAction(
            action_type='mouse_scroll',
            timestamp=current_time - self._start_time,
            params={
                'x': x,
                'y': y,
                'dx': dx,
                'dy': dy
            }
        )
        self._actions.append(action)
        self.action_recorded.emit('mouse_scroll', action.params)
    
    def _on_key_press(self, key: Any) -> None:
        """Handle key press event from pynput.
        
        Args:
            key: Key that was pressed.
        """
        if not self._is_recording:
            return
        
        try:
            key_name = self._get_key_name(key)
            
            if key_name in self._modifier_keys:
                self._pressed_keys.add(key_name)
            else:
                current_time = time.time()
                if current_time - self._last_action_time < self._min_interval:
                    return
                
                self._last_action_time = current_time
                
                modifiers = list(self._pressed_keys)
                action = RecordedAction(
                    action_type='key_press',
                    timestamp=current_time - self._start_time,
                    params={
                        'key': key_name,
                        'modifiers': modifiers
                    }
                )
                self._actions.append(action)
                self.action_recorded.emit('key_press', action.params)
        except Exception as e:
            import logging
            logging.getLogger("RabAI").debug(f"录制按键失败: {e}")
    
    def _on_key_release(self, key: Any) -> None:
        """Handle key release event from pynput.
        
        Args:
            key: Key that was released.
        """
        if not self._is_recording:
            return
        
        try:
            key_name = self._get_key_name(key)
            
            if key_name in self._modifier_keys:
                self._pressed_keys.discard(key_name)
        except Exception:
            pass
    
    def _get_key_name(self, key: Any) -> str:
        """Convert pynput key to string name.
        
        Args:
            key: Pynput key object.
            
        Returns:
            String name of the key.
        """
        try:
            if hasattr(key, 'char') and key.char:
                return key.char.lower()
            elif hasattr(key, 'name'):
                return key.name.lower()
            else:
                return str(key).split('.')[-1].lower()
        except Exception:
            return 'unknown'


# Import pyautogui at module level (already imported above)
# This is needed for the recording functions
