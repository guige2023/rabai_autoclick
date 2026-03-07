import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import threading
import platform
import subprocess
import multiprocessing as mp
from typing import Dict, Any, List, Optional
from datetime import datetime
from queue import Empty
from PyQt5.QtCore import QObject, pyqtSignal, QTimer, QThread
from PyQt5.QtWidgets import QMessageBox

import pyautogui

IS_MACOS = platform.system() == 'Darwin'

try:
    from pynput import mouse, keyboard
    PYNPUT_AVAILABLE = True
except ImportError:
    PYNPUT_AVAILABLE = False


class MacPermissionChecker:
    """macOS 权限检查器"""
    
    @staticmethod
    def check_accessibility_permission() -> bool:
        if not IS_MACOS:
            return True
        
        try:
            result = subprocess.run(
                ['osascript', '-e', 'tell application "System Events" to get name of processes'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                return True
        except Exception:
            pass
        
        try:
            test_pos = pyautogui.position()
            pyautogui.moveTo(test_pos.x + 1, test_pos.y + 1, duration=0.01)
            new_pos = pyautogui.position()
            pyautogui.moveTo(test_pos.x, test_pos.y, duration=0.01)
            if new_pos.x == test_pos.x + 1:
                return True
        except Exception:
            pass
        
        return False
    
    @staticmethod
    def request_accessibility_permission(parent=None) -> bool:
        if not IS_MACOS:
            return True
        
        if MacPermissionChecker.check_accessibility_permission():
            return True
        
        msg = QMessageBox(parent)
        msg.setIcon(QMessageBox.Warning)
        msg.setWindowTitle("需要辅助功能权限")
        msg.setText("RabAI AutoClick 需要 macOS 辅助功能权限才能录制操作。")
        msg.setInformativeText(
            "授权步骤：\n"
            "1. 点击「打开系统设置」\n"
            "2. 在左侧找到「辅助功能」\n"
            "3. 点击左下角锁图标解锁\n"
            "4. 点击「+」添加应用程序\n"
            "5. 找到并添加终端或 Python\n"
            "6. 确保勾选已添加的项目\n"
            "7. 重启本程序"
        )
        msg.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
        msg.button(QMessageBox.Yes).setText("打开系统设置")
        msg.button(QMessageBox.No).setText("稍后设置")
        
        result = msg.exec_()
        
        if result == QMessageBox.Yes:
            try:
                subprocess.run(['open', 'x-apple.systempreferences:com.apple.preference.security?Privacy_Accessibility'])
            except Exception:
                pass
        
        return False


def pynput_listener_process(event_queue: mp.Queue, stop_event: mp.Event, stop_hotkeys: list = None):
    """独立进程中的 pynput 监听器 - 彻底隔离 Qt 线程"""
    
    if stop_hotkeys is None:
        stop_hotkeys = ['f9', 'f10']
    
    last_click_time = 0
    last_click_pos = (0, 0)
    last_click_button = None
    double_click_threshold = 0.3
    
    def on_click(x, y, button, pressed):
        nonlocal last_click_time, last_click_pos, last_click_button
        
        if stop_event.is_set():
            return False
        
        if pressed:
            button_name = 'left'
            if button == mouse.Button.right:
                button_name = 'right'
            elif button == mouse.Button.middle:
                button_name = 'middle'
            
            current_time = time.time()
            current_pos = (int(x), int(y))
            
            is_double_click = (
                current_time - last_click_time < double_click_threshold and
                current_pos == last_click_pos and
                button_name == last_click_button
            )
            
            if is_double_click:
                action_type = 'double_click'
                last_click_time = 0
                last_click_pos = (0, 0)
                last_click_button = None
            else:
                action_type = 'click'
                last_click_time = current_time
                last_click_pos = current_pos
                last_click_button = button_name
            
            try:
                event_queue.put((action_type, {
                    'x': int(x),
                    'y': int(y),
                    'button': button_name,
                    'clicks': 2 if is_double_click else 1
                }))
            except Exception:
                pass
        return True
    
    def on_scroll(x, y, dx, dy):
        if stop_event.is_set():
            return False
        try:
            event_queue.put(('scroll', {
                'x': int(x),
                'y': int(y),
                'clicks': abs(int(dy)),
                'direction': 'up' if dy > 0 else 'down'
            }))
        except Exception:
            pass
        return True
    
    pressed_keys = set()
    modifier_keys = {'shift', 'ctrl', 'alt', 'cmd', 'command', 'option', 'control'}
    
    def get_key_name(key):
        try:
            if hasattr(key, 'char') and key.char:
                return key.char
            elif hasattr(key, 'name'):
                name = key.name.lower()
                name_map = {
                    'cmd_l': 'cmd', 'cmd_r': 'cmd',
                    'ctrl_l': 'ctrl', 'ctrl_r': 'ctrl',
                    'alt_l': 'alt', 'alt_r': 'alt',
                    'shift_l': 'shift', 'shift_r': 'shift',
                }
                return name_map.get(name, name)
        except Exception:
            pass
        return None
    
    def is_stop_hotkey(combo):
        combo_lower = combo.lower()
        for hk in stop_hotkeys:
            if hk.lower() in combo_lower or combo_lower in hk.lower():
                return True
        return False
    
    def on_press(key):
        if stop_event.is_set():
            return False
        
        key_name = get_key_name(key)
        if not key_name:
            return True
        
        pressed_keys.add(key_name)
        
        if key_name in modifier_keys:
            return True
        
        try:
            if len(pressed_keys) > 1:
                modifiers = [k for k in pressed_keys if k in modifier_keys]
                if modifiers:
                    modifier_order = ['ctrl', 'cmd', 'alt', 'option', 'shift']
                    sorted_modifiers = sorted(modifiers, key=lambda x: modifier_order.index(x) if x in modifier_order else 999)
                    combo = '+'.join(sorted_modifiers + [key_name])
                    
                    if is_stop_hotkey(combo):
                        return True
                    
                    event_queue.put(('hotkey', {'keys': combo}))
                    return True
            
            if key_name.lower() in [hk.lower() for hk in stop_hotkeys]:
                return True
            
            if len(key_name) == 1:
                event_queue.put(('type_text', {'text': key_name}))
            else:
                event_queue.put(('key_press', {'key': key_name}))
        except Exception:
            pass
        
        return True
    
    def on_release(key):
        if stop_event.is_set():
            return False
        key_name = get_key_name(key)
        if key_name and key_name in pressed_keys:
            pressed_keys.discard(key_name)
        return True
    
    try:
        mouse_listener = mouse.Listener(on_click=on_click, on_scroll=on_scroll)
        keyboard_listener = keyboard.Listener(on_press=on_press, on_release=on_release)
        
        mouse_listener.start()
        keyboard_listener.start()
        
        mouse_listener.join()
        keyboard_listener.join()
        
    except Exception as e:
        print(f"[ListenerProcess] Error: {e}")
    finally:
        try:
            event_queue.put(None)
        except Exception:
            pass


class EventReaderThread(QThread):
    """从多进程队列读取事件的线程"""
    
    action_ready = pyqtSignal(str, dict)
    listener_stopped = pyqtSignal()
    
    def __init__(self, event_queue, parent=None):
        super().__init__(parent)
        self._event_queue = event_queue
        self._running = True
    
    def run(self):
        while self._running:
            try:
                event = self._event_queue.get(timeout=0.1)
                if event is None:
                    self.listener_stopped.emit()
                    break
                action_type, params = event
                self.action_ready.emit(action_type, params)
            except Empty:
                continue
            except Exception as e:
                print(f"[EventReader] Error: {e}")
    
    def stop(self):
        self._running = False
        self.wait(2000)


class MacRecordingManager(QObject):
    """Mac 专用录屏管理器 - 使用独立进程隔离 pynput"""
    
    action_recorded = pyqtSignal(str, dict)
    recording_started = pyqtSignal()
    recording_stopped = pyqtSignal(list)
    recording_error = pyqtSignal(str)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._is_recording = False
        self._actions: List[Dict[str, Any]] = []
        self._start_time: float = 0
        self._last_action_time = 0
        self._min_interval = 0.05
        self._initialized = PYNPUT_AVAILABLE
        self._listener_process = None
        self._event_queue = None
        self._stop_event = None
        self._reader_thread = None
        self._lock = threading.RLock()
        
        if PYNPUT_AVAILABLE:
            print("[MacRecording] pynput available, using process isolation mode")
    
    def is_recording(self) -> bool:
        with self._lock:
            return self._is_recording
    
    def is_initialized(self) -> bool:
        return self._initialized
    
    def check_permission(self) -> bool:
        return MacPermissionChecker.check_accessibility_permission()
    
    def start_recording(self, stop_hotkeys: list = None) -> bool:
        if stop_hotkeys is None:
            stop_hotkeys = ['f9', 'f10', 'ctrl+f9', 'ctrl+f10', 'cmd+f9', 'cmd+f10']
        
        with self._lock:
            if not self._initialized:
                self.recording_error.emit("录屏模块未初始化")
                return False
            
            if self._is_recording:
                return False
            
            if not self.check_permission():
                self.recording_error.emit("请先授权辅助功能权限")
                return False
            
            self._actions = []
            self._start_time = time.time()
            self._is_recording = True
            self._last_action_time = 0
        
        self._event_queue = mp.Queue()
        self._stop_event = mp.Event()
        
        self._listener_process = mp.Process(
            target=pynput_listener_process,
            args=(self._event_queue, self._stop_event, stop_hotkeys),
            daemon=True
        )
        
        try:
            self._listener_process.start()
            time.sleep(0.2)
            
            if not self._listener_process.is_alive():
                self.recording_error.emit("监听进程启动失败")
                self._cleanup()
                return False
            
            self._reader_thread = EventReaderThread(self._event_queue, self)
            self._reader_thread.action_ready.connect(self._on_action_ready)
            self._reader_thread.listener_stopped.connect(self._on_listener_stopped)
            self._reader_thread.start()
            
            print(f"[MacRecording] Listener process started (PID: {self._listener_process.pid})")
            self.recording_started.emit()
            return True
            
        except Exception as e:
            print(f"[MacRecording] Failed to start: {e}")
            self.recording_error.emit(f"启动录制失败: {str(e)}")
            self._cleanup()
            return False
    
    def _on_listener_stopped(self):
        """监听进程意外停止"""
        with self._lock:
            if self._is_recording:
                self.recording_error.emit("监听进程意外停止")
                self._do_stop_recording()
    
    def stop_recording(self) -> List[Dict[str, Any]]:
        with self._lock:
            if not self._is_recording:
                return []
            self._do_stop_recording()
        return self._actions.copy()
    
    def _do_stop_recording(self):
        self._is_recording = False
        self._cleanup()
        self.recording_stopped.emit(self._actions.copy())
    
    def _cleanup(self):
        """清理资源"""
        if self._stop_event:
            self._stop_event.set()
        
        if self._reader_thread:
            self._reader_thread.stop()
            self._reader_thread = None
        
        if self._listener_process and self._listener_process.is_alive():
            self._listener_process.terminate()
            self._listener_process.join(timeout=2)
            if self._listener_process.is_alive():
                self._listener_process.kill()
        self._listener_process = None
        
        if self._event_queue:
            try:
                while not self._event_queue.empty():
                    self._event_queue.get_nowait()
            except Exception:
                pass
            self._event_queue = None
        
        self._stop_event = None
    
    def _on_action_ready(self, action_type: str, params: dict):
        with self._lock:
            if not self._is_recording:
                return
            
            current_time = time.time() - self._start_time
            
            if current_time - self._last_action_time < self._min_interval:
                return
            
            self._last_action_time = current_time
            
            if self._actions:
                last_action = self._actions[-1]
                delay = current_time - last_action['timestamp']
                if delay > 0.05:
                    params['pre_delay'] = round(delay, 3)
            
            action = {
                'action_type': action_type,
                'timestamp': current_time,
                'params': params.copy()
            }
            self._actions.append(action)
            
            self.action_recorded.emit(action_type, params)
    
    def get_actions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return self._actions.copy()
    
    def get_action_count(self) -> int:
        with self._lock:
            return len(self._actions)
    
    def clear_actions(self):
        with self._lock:
            self._actions = []
    
    def to_workflow(self) -> Dict[str, Any]:
        with self._lock:
            steps = []
            for i, action in enumerate(self._actions, 1):
                steps.append({
                    'id': i,
                    'type': action['action_type'],
                    'params': dict(action['params'])
                })
            return {'variables': {}, 'steps': steps}
    
    def save_to_file(self, filepath: str) -> bool:
        try:
            with self._lock:
                data = {
                    'recorded_at': datetime.now().isoformat(),
                    'actions': self._actions.copy()
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
            with self._lock:
                self._actions = data.get('actions', [])
            return True
        except Exception:
            return False


def create_recording_manager(parent=None):
    if IS_MACOS and PYNPUT_AVAILABLE:
        return MacRecordingManager(parent)
    else:
        from utils.recording import RecordingManager
        return RecordingManager(parent)


def check_recording_permission() -> bool:
    if IS_MACOS:
        return MacPermissionChecker.check_accessibility_permission()
    return True


def request_recording_permission(parent=None) -> bool:
    if IS_MACOS:
        return MacPermissionChecker.request_accessibility_permission(parent)
    return True
