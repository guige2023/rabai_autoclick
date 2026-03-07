import sys
import time
import platform
from collections import deque

IS_MACOS = platform.system() == 'Darwin'

try:
    from pynput import keyboard, mouse
    PYNPUT_AVAILABLE = True
except ImportError:
    PYNPUT_AVAILABLE = False
    print("pynput not available")
    sys.exit(1)

import tkinter as tk
from tkinter import font as tkfont


class KeyDisplayApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Key Display")
        self.root.overrideredirect(True)
        self.root.attributes('-topmost', True)
        self.root.configure(bg='#1a1a2e')
        
        if IS_MACOS:
            try:
                self.root.call('::tk::unsupported::MacWindowStyle', 
                               'style', self.root._w, 
                               'floating', 'closeBox collapseBox resizable')
            except:
                pass
        
        self.keys = deque(maxlen=5)
        self.pressed_keys = {}
        self.mouse_listener = None
        self.keyboard_listener = None
        self.running = True
        
        self._create_ui()
        self._position_window()
    
    def _create_ui(self):
        main_frame = tk.Frame(self.root, bg='#1a1a2e')
        main_frame.pack(padx=10, pady=10)
        
        mouse_frame = tk.Frame(main_frame, bg='#0078d7', padx=10, pady=5)
        mouse_frame.pack(fill=tk.X, pady=(0, 5))
        
        self.mouse_label = tk.Label(
            mouse_frame, 
            text="Mouse: (0, 0)",
            font=('Arial', 11, 'bold'),
            fg='white',
            bg='#0078d7'
        )
        self.mouse_label.pack()
        
        self.key_frame = tk.Frame(main_frame, bg='#1a1a2e')
        self.key_frame.pack(fill=tk.X)
        
        tip_label = tk.Label(
            main_frame,
            text="Press ESC or F11 to close",
            font=('Arial', 9),
            fg='#888888',
            bg='#1a1a2e'
        )
        tip_label.pack(pady=(5, 0))
    
    def _position_window(self):
        self.root.update_idletasks()
        screen_width = self.root.winfo_screenwidth()
        window_width = 300
        self.root.geometry(f"+{screen_width - window_width - 20}+50")
    
    def _start_mouse_update(self):
        if not self.running:
            return
        
        try:
            x = self.root.winfo_pointerx()
            y = self.root.winfo_pointery()
            self.mouse_label.config(text=f"Mouse: ({x}, {y})")
        except:
            pass
        
        self.root.after(50, self._start_mouse_update)
    
    def _start_listeners(self):
        self.mouse_listener = mouse.Listener(on_click=self._on_mouse_click)
        self.keyboard_listener = keyboard.Listener(
            on_press=self._on_key_press,
            on_release=self._on_key_release
        )
        
        self.mouse_listener.start()
        self.keyboard_listener.start()
    
    def _stop_listeners(self):
        self.running = False
        
        if self.mouse_listener:
            try:
                self.mouse_listener.stop()
            except:
                pass
        
        if self.keyboard_listener:
            try:
                self.keyboard_listener.stop()
            except:
                pass
    
    def _on_mouse_click(self, x, y, button, pressed):
        if not pressed:
            return
        
        button_name = 'L' if button == mouse.Button.left else 'R' if button == mouse.Button.right else 'M'
        self.root.after(0, lambda: self._add_key(f"🖱{button_name}"))
    
    def _on_key_press(self, key):
        try:
            if hasattr(key, 'char') and key.char:
                key_name = key.char.upper()
            elif hasattr(key, 'name'):
                key_name = self._format_key_name(key.name)
            else:
                return
            
            if key_name in ('ESC', 'F11'):
                self.root.after(0, self._quit)
                return
            
            if key_name not in self.pressed_keys:
                self.pressed_keys[key_name] = True
                self.root.after(0, lambda: self._add_key(key_name))
        except Exception:
            pass
    
    def _on_key_release(self, key):
        try:
            if hasattr(key, 'char') and key.char:
                key_name = key.char.upper()
            elif hasattr(key, 'name'):
                key_name = self._format_key_name(key.name)
            else:
                return
            
            if key_name in self.pressed_keys:
                del self.pressed_keys[key_name]
        except Exception:
            pass
    
    def _format_key_name(self, name: str) -> str:
        key_map = {
            'ctrl': 'Ctrl',
            'shift': 'Shift',
            'alt': 'Alt',
            'cmd': '⌘',
            'command': '⌘',
            'space': '␣',
            'enter': '↵',
            'return': '↵',
            'tab': '⇥',
            'backspace': '⌫',
            'delete': '⌦',
            'escape': 'Esc',
            'up': '↑',
            'down': '↓',
            'left': '←',
            'right': '→',
            'caps_lock': '⇪',
            'page_up': 'PgUp',
            'page_down': 'PgDn',
            'home': 'Home',
            'end': 'End',
        }
        return key_map.get(name.lower(), name.upper())
    
    def _add_key(self, key: str):
        self.keys.append(key)
        self._update_display()
    
    def _update_display(self):
        for widget in self.key_frame.winfo_children():
            widget.destroy()
        
        for key in self.keys:
            label = tk.Label(
                self.key_frame,
                text=key,
                font=('Arial', 14, 'bold'),
                fg='white',
                bg='#333344',
                padx=10,
                pady=5
            )
            label.pack(side=tk.LEFT, padx=2)
    
    def _quit(self):
        self._stop_listeners()
        self.root.quit()
    
    def run(self):
        self._start_listeners()
        self._start_mouse_update()
        
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            self._quit()
        finally:
            self._stop_listeners()


if __name__ == '__main__':
    app = KeyDisplayApp()
    app.run()
