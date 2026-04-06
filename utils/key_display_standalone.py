#!/usr/bin/env python3
"""Standalone key display application for RabAI AutoClick.

This module provides a floating window that displays pressed keys
and mouse position, useful for teaching and recording workflows.
"""

import sys
import platform
from collections import deque
from typing import Deque, Dict, Optional

# Platform detection
IS_MACOS: bool = platform.system() == 'Darwin'

# Check pynput availability
try:
    from pynput import keyboard, mouse
    PYNPUT_AVAILABLE: bool = True
except ImportError:
    PYNPUT_AVAILABLE = False
    print("pynput not available")
    sys.exit(1)

import tkinter as tk
from tkinter import font as tkfont


class KeyDisplayApp:
    """Floating key display application.
    
    Displays pressed keyboard keys and mouse position in a
    floating, always-on-top window. Useful for teaching mode
    and recording workflows.
    """
    
    def __init__(self) -> None:
        """Initialize the key display application."""
        self.root: tk.Tk = tk.Tk()
        self.root.title("Key Display")
        self.root.overrideredirect(True)
        self.root.attributes('-topmost', True)
        self.root.configure(bg='#1a1a2e')
        
        if IS_MACOS:
            try:
                self.root.call(
                    '::tk::unsupported::MacWindowStyle',
                    'style', self.root._w,
                    'floating', 'closeBox collapseBox resizable'
                )
            except Exception:
                pass
        
        self.keys: Deque[str] = deque(maxlen=5)
        self.pressed_keys: Dict[str, bool] = {}
        self.mouse_listener: Optional[mouse.Listener] = None
        self.keyboard_listener: Optional[keyboard.Listener] = None
        self.running: bool = True
        
        self._create_ui()
        self._position_window()
    
    def _create_ui(self) -> None:
        """Create the application UI components."""
        main_frame = tk.Frame(self.root, bg='#1a1a2e')
        main_frame.pack(padx=10, pady=10)
        
        # Mouse position frame
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
        
        # Key display frame
        self.key_frame = tk.Frame(main_frame, bg='#1a1a2e')
        self.key_frame.pack(fill=tk.X)
        
        # Help text
        tip_label = tk.Label(
            main_frame,
            text="Press ESC or F11 to close",
            font=('Arial', 9),
            fg='#888888',
            bg='#1a1a2e'
        )
        tip_label.pack(pady=(5, 0))
    
    def _position_window(self) -> None:
        """Position the window in the top-right corner."""
        self.root.update_idletasks()
        screen_width = self.root.winfo_screenwidth()
        window_width = 300
        self.root.geometry(f"+{screen_width - window_width - 20}+50")
    
    def _start_mouse_update(self) -> None:
        """Start the mouse position polling loop."""
        if not self.running:
            return
        
        try:
            x = self.root.winfo_pointerx()
            y = self.root.winfo_pointery()
            self.mouse_label.config(text=f"Mouse: ({x}, {y})")
        except Exception:
            pass
        
        self.root.after(50, self._start_mouse_update)
    
    def _start_listeners(self) -> None:
        """Start pynput input listeners."""
        self.mouse_listener = mouse.Listener(on_click=self._on_mouse_click)
        self.keyboard_listener = keyboard.Listener(
            on_press=self._on_key_press,
            on_release=self._on_key_release
        )
        
        self.mouse_listener.start()
        self.keyboard_listener.start()
    
    def _stop_listeners(self) -> None:
        """Stop pynput input listeners."""
        self.running = False
        
        if self.mouse_listener:
            try:
                self.mouse_listener.stop()
            except Exception:
                pass
        
        if self.keyboard_listener:
            try:
                self.keyboard_listener.stop()
            except Exception:
                pass
    
    def _on_mouse_click(
        self,
        x: int,
        y: int,
        button: mouse.Button,
        pressed: bool
    ) -> None:
        """Handle mouse click event.
        
        Args:
            x: Mouse X coordinate.
            y: Mouse Y coordinate.
            button: Mouse button that was clicked.
            pressed: True if button was pressed.
        """
        if not pressed:
            return
        
        button_name = (
            'L' if button == mouse.Button.left
            else 'R' if button == mouse.Button.right
            else 'M'
        )
        self.root.after(0, lambda: self._add_key(f"🖱{button_name}"))
    
    def _on_key_press(self, key: keyboard.Key) -> None:
        """Handle key press event.
        
        Args:
            key: Key that was pressed.
        """
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
    
    def _on_key_release(self, key: keyboard.Key) -> None:
        """Handle key release event.
        
        Args:
            key: Key that was released.
        """
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
        """Format a key name for display with Unicode symbols.
        
        Args:
            name: Raw key name.
            
        Returns:
            Formatted key name with Unicode symbols.
        """
        key_map: Dict[str, str] = {
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
    
    def _add_key(self, key: str) -> None:
        """Add a key to the display.
        
        Args:
            key: Key name to display.
        """
        self.keys.append(key)
        self._update_display()
    
    def _update_display(self) -> None:
        """Update the key display labels."""
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
    
    def _quit(self) -> None:
        """Quit the application."""
        self._stop_listeners()
        self.root.quit()
    
    def run(self) -> None:
        """Run the application main loop."""
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
