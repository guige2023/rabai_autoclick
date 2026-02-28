import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import threading
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from PyQt5.QtCore import QObject, pyqtSignal, QTimer
from PyQt5.QtWidgets import QMessageBox

try:
    from pynput import mouse, keyboard
    PYNPUT_AVAILABLE = True
except ImportError:
    PYNPUT_AVAILABLE = False


class RecordedAction:
    def __init__(self, action_type: str, timestamp: float, params: Dict[str, Any]):
        self.action_type = action_type
        self.timestamp = timestamp
        self.params = params
    
    def to_dict(self) -> dict:
        return {
            'action_type': self.action_type,
            'timestamp': self.timestamp,
            'params': self.params
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'RecordedAction':
        return cls(
            action_type=data['action_type'],
            timestamp=data['timestamp'],
            params=data['params']
        )


class RecordingManager(QObject):
    action_recorded = pyqtSignal(str, dict)
    recording_started = pyqtSignal()
    recording_stopped = pyqtSignal(list)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._is_recording = False
        self._actions: List[RecordedAction] = []
        self._start_time: float = 0
        self._mouse_listener = None
        self._keyboard_listener = None
        self._last_action_time = 0
        self._min_interval = 0.1
    
    def is_recording(self) -> bool:
        return self._is_recording
    
    def start_recording(self) -> bool:
        if not PYNPUT_AVAILABLE:
            return False
        
        if self._is_recording:
            return False
        
        self._actions = []
        self._start_time = time.time()
        self._is_recording = True
        self._last_action_time = 0
        
        self._mouse_listener = mouse.Listener(
            on_click=self._on_mouse_click,
            on_scroll=self._on_mouse_scroll
        )
        
        self._keyboard_listener = keyboard.Listener(
            on_press=self._on_key_press
        )
        
        self._mouse_listener.start()
        self._keyboard_listener.start()
        
        self.recording_started.emit()
        return True
    
    def stop_recording(self) -> List[RecordedAction]:
        if not self._is_recording:
            return []
        
        self._is_recording = False
        
        if self._mouse_listener:
            self._mouse_listener.stop()
            try:
                self._mouse_listener.join(timeout=1.0)
            except:
                pass
            self._mouse_listener = None
        
        if self._keyboard_listener:
            self._keyboard_listener.stop()
            try:
                self._keyboard_listener.join(timeout=1.0)
            except:
                pass
            self._keyboard_listener = None
        
        self.recording_stopped.emit(self._actions)
        return self._actions.copy()
    
    def _add_action(self, action_type: str, params: Dict[str, Any]):
        current_time = time.time() - self._start_time
        
        if current_time - self._last_action_time < self._min_interval:
            return
        
        self._last_action_time = current_time
        
        if self._actions:
            last_action = self._actions[-1]
            delay = current_time - last_action.timestamp
            if delay > 0.05:
                params['pre_delay'] = round(delay, 3)
        
        action = RecordedAction(action_type, current_time, params)
        self._actions.append(action)
        self.action_recorded.emit(action_type, params)
    
    def _on_mouse_click(self, x, y, button, pressed):
        if not self._is_recording or not pressed:
            return
        
        button_name = 'left' if button == mouse.Button.left else 'right' if button == mouse.Button.right else 'middle'
        
        self._add_action('click', {
            'x': int(x),
            'y': int(y),
            'button': button_name,
            'clicks': 1
        })
    
    def _on_mouse_scroll(self, x, y, dx, dy):
        if not self._is_recording:
            return
        
        self._add_action('scroll', {
            'x': int(x),
            'y': int(y),
            'clicks': abs(int(dy)),
            'direction': 'up' if dy > 0 else 'down'
        })
    
    def _on_key_press(self, key):
        if not self._is_recording:
            return
        
        try:
            if hasattr(key, 'char') and key.char:
                self._add_action('type_text', {
                    'text': key.char
                })
            elif hasattr(key, 'name'):
                self._add_action('key_press', {
                    'key': key.name
                })
        except Exception:
            pass
    
    def get_actions(self) -> List[RecordedAction]:
        return self._actions.copy()
    
    def get_action_count(self) -> int:
        return len(self._actions)
    
    def clear_actions(self) -> None:
        self._actions = []
    
    def to_workflow(self) -> Dict[str, Any]:
        steps = []
        step_id = 1
        
        for action in self._actions:
            step = {
                'id': step_id,
                'type': action.action_type,
                **action.params
            }
            steps.append(step)
            step_id += 1
        
        return {
            'variables': {},
            'steps': steps
        }
    
    def save_to_file(self, filepath: str) -> bool:
        try:
            data = {
                'recorded_at': datetime.now().isoformat(),
                'actions': [a.to_dict() for a in self._actions]
            }
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception:
            return False
    
    def load_from_file(self, filepath: str) -> bool:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self._actions = [RecordedAction.from_dict(a) for a in data.get('actions', [])]
            return True
        except Exception:
            return False


class RecordingEditor:
    def __init__(self, actions: List[RecordedAction]):
        self._actions = actions.copy()
    
    def get_actions(self) -> List[RecordedAction]:
        return self._actions
    
    def remove_action(self, index: int) -> bool:
        if 0 <= index < len(self._actions):
            del self._actions[index]
            return True
        return False
    
    def insert_action(self, index: int, action: RecordedAction) -> bool:
        if 0 <= index <= len(self._actions):
            self._actions.insert(index, action)
            return True
        return False
    
    def modify_action(self, index: int, params: Dict[str, Any]) -> bool:
        if 0 <= index < len(self._actions):
            self._actions[index].params.update(params)
            return True
        return False
    
    def merge_consecutive_types(self) -> int:
        merged = 0
        i = 0
        while i < len(self._actions) - 1:
            current = self._actions[i]
            next_action = self._actions[i + 1]
            
            if current.action_type == 'type_text' and next_action.action_type == 'type_text':
                current.params['text'] += next_action.params['text']
                del self._actions[i + 1]
                merged += 1
            else:
                i += 1
        
        return merged
    
    def add_delay_after(self, index: int, delay: float) -> bool:
        if 0 <= index < len(self._actions):
            delay_action = RecordedAction('delay', self._actions[index].timestamp + 0.001, {'seconds': delay})
            self._actions.insert(index + 1, delay_action)
            return True
        return False
    
    def optimize_delays(self, min_delay: float = 0.1) -> int:
        optimized = 0
        for action in self._actions:
            if 'pre_delay' in action.params and action.params['pre_delay'] < min_delay:
                del action.params['pre_delay']
                optimized += 1
        return optimized
