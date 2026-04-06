"""Key display window manager for RabAI AutoClick.

Manages a separate subprocess for displaying keystrokes on screen.
"""

import subprocess
import sys
import os
from typing import Optional


class KeyDisplayWindow:
    """Manages a subprocess that displays keystrokes on screen.
    
    Launches key_display_standalone.py as a separate process to avoid
    conflicts with the main application's event loop.
    """
    
    def __init__(self) -> None:
        """Initialize the key display window manager."""
        self._process: Optional[subprocess.Popen] = None
        self._enabled: bool = False
        self._script_path: str = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'key_display_standalone.py'
        )
    
    def is_enabled(self) -> bool:
        """Check if key display is currently enabled and running.
        
        Returns:
            True if enabled and process is alive, False otherwise.
        """
        if self._process is None:
            return False
        
        if self._process.poll() is not None:
            # Process has terminated
            self._process = None
            self._enabled = False
            return False
        
        return self._enabled
    
    def enable(self) -> bool:
        """Enable key display by starting the subprocess.
        
        Returns:
            True if started successfully, False otherwise.
        """
        # Already enabled and running
        if self._enabled and self._process is not None:
            if self._process.poll() is None:
                return True
            else:
                self._process = None
                self._enabled = False
        
        try:
            python_exe = sys.executable
            
            # Use CREATE_NO_WINDOW on Windows to hide console
            creation_flags = (
                subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0
            )
            
            self._process = subprocess.Popen(
                [python_exe, self._script_path],
                creationflags=creation_flags
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
    
    def disable(self) -> None:
        """Disable key display by terminating the subprocess."""
        if self._process is not None:
            try:
                self._process.terminate()
            except Exception:
                pass
            self._process = None
        
        self._enabled = False
        print("[KeyDisplay] Disabled")
    
    def toggle(self) -> bool:
        """Toggle key display on/off.
        
        Returns:
            True if enabled after toggle, False if disabled.
        """
        if self._enabled:
            self.disable()
            return False
        else:
            return self.enable()


# Global singleton instance
key_display_window: KeyDisplayWindow = KeyDisplayWindow()


if __name__ == '__main__':
    key_display_window.enable()
    import time
    while True:
        time.sleep(1)
