import sys
import subprocess
import os
import time
from typing import Optional


class KeyDisplayWindow:
    def __init__(self):
        self._process: Optional[subprocess.Popen] = None
        self._enabled = False
        self._script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'key_display_standalone.py'
        )
    
    def is_enabled(self) -> bool:
        if self._process is None:
            return False
        
        if self._process.poll() is not None:
            self._process = None
            self._enabled = False
            return False
        
        return self._enabled
    
    def enable(self) -> bool:
        if self._enabled and self._process is not None:
            if self._process.poll() is None:
                return True
            else:
                self._process = None
                self._enabled = False
        
        try:
            python_exe = sys.executable
            
            self._process = subprocess.Popen(
                [python_exe, self._script_path],
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
            )
            self._enabled = True
            print("[KeyDisplay] Enabled")
            return True
            
        except Exception as e:
            print(f"[KeyDisplay] Error enabling: {e}")
            import traceback
            traceback.print_exc()
            self._process = None
        
        self._enabled = False
        return False
    
    def disable(self):
        if self._process is not None:
            try:
                self._process.terminate()
            except:
                pass
            self._process = None
        
        self._enabled = False
        print("[KeyDisplay] Disabled")
    
    def toggle(self) -> bool:
        if self._enabled:
            self.disable()
            return False
        else:
            return self.enable()


key_display_window = KeyDisplayWindow()


if __name__ == '__main__':
    key_display_window.enable()
    while True:
        time.sleep(1)
