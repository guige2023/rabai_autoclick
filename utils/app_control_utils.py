"""
App control utilities for launching and managing applications.

Provides application launch, quit, and state management
for automation workflows.
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from enum import Enum


class AppState(Enum):
    """Application state."""
    RUNNING = "running"
    NOT_RUNNING = "not_running"
    FRONTGROUND = "frontground"
    BACKGROUND = "background"
    HIDDEN = "hidden"


@dataclass
class AppInfo:
    """Application information."""
    bundle_id: str
    name: str
    pid: int
    state: AppState
    window_count: int


@dataclass
class AppActionResult:
    """Result of app action."""
    success: bool
    message: str
    app_info: Optional[AppInfo] = None


def get_app_info(bundle_id: str) -> Optional[AppInfo]:
    """
    Get application information.
    
    Args:
        bundle_id: App bundle identifier.
        
    Returns:
        AppInfo or None.
    """
    try:
        script = f'''
        tell application "System Events"
            set targetApp to first process whose bundle identifier is "{bundle_id}"
            set appName to name of targetApp
            set appPID to process ID of targetApp
            set isRunning to running of targetApp
            set isFront to frontmost of targetApp
            set winCount to count of windows of targetApp
            return {{appName, appPID, isRunning, isFront, winCount}}
        end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.stdout.strip():
            return parse_app_info(bundle_id, result.stdout)
    except Exception:
        pass
    return None


def parse_app_info(bundle_id: str, output: str) -> Optional[AppInfo]:
    """Parse AppleScript app info output."""
    parts = output.strip().split(',')
    if len(parts) >= 5:
        name = parts[0].strip()
        pid = int(parts[1].strip())
        is_running = parts[2].strip() == 'true'
        is_front = parts[3].strip() == 'true'
        win_count = int(parts[4].strip())
        
        if not is_running:
            state = AppState.NOT_RUNNING
        elif is_front:
            state = AppState.FRONTGROUND
        else:
            state = AppState.RUNNING
        
        return AppInfo(
            bundle_id=bundle_id,
            name=name,
            pid=pid,
            state=state,
            window_count=win_count
        )
    return None


class AppController:
    """Controls application lifecycle."""
    
    def __init__(self, bundle_id: Optional[str] = None):
        """
        Initialize app controller.
        
        Args:
            bundle_id: Optional app bundle ID.
        """
        self.bundle_id = bundle_id
    
    def launch(self) -> AppActionResult:
        """
        Launch application.
        
        Returns:
            AppActionResult.
        """
        if not self.bundle_id:
            return AppActionResult(
                success=False,
                message="No bundle ID specified"
            )
        
        try:
            subprocess.run(
                ["open", "-b", self.bundle_id],
                capture_output=True,
                timeout=10
            )
            
            time.sleep(0.5)
            info = get_app_info(self.bundle_id)
            
            return AppActionResult(
                success=True,
                message=f"Launched: {self.bundle_id}",
                app_info=info
            )
        except Exception as e:
            return AppActionResult(
                success=False,
                message=f"Launch failed: {e}"
            )
    
    def quit(self, force: bool = False) -> AppActionResult:
        """
        Quit application.
        
        Args:
            force: Force quit.
            
        Returns:
            AppActionResult.
        """
        if not self.bundle_id:
            return AppActionResult(
                success=False,
                message="No bundle ID specified"
            )
        
        try:
            if force:
                script = f'''
                tell application "System Events"
                    set targetApp to first process whose bundle identifier is "{self.bundle_id}"
                    do shell script "kill -9 " & (process ID of targetApp as text)
                end tell
                '''
            else:
                script = f'''
                tell application "{self.get_app_name()}"
                    quit
                end tell
                '''
            
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            
            return AppActionResult(
                success=True,
                message="App quit"
            )
        except Exception as e:
            return AppActionResult(
                success=False,
                message=f"Quit failed: {e}"
            )
    
    def focus(self) -> AppActionResult:
        """
        Bring app to front.
        
        Returns:
            AppActionResult.
        """
        if not self.bundle_id:
            return AppActionResult(
                success=False,
                message="No bundle ID specified"
            )
        
        try:
            script = f'''
            tell application "System Events"
                set targetApp to first process whose bundle identifier is "{self.bundle_id}"
                set frontmost of targetApp to true
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            
            info = get_app_info(self.bundle_id)
            
            return AppActionResult(
                success=True,
                message="App focused",
                app_info=info
            )
        except Exception as e:
            return AppActionResult(
                success=False,
                message=f"Focus failed: {e}"
            )
    
    def hide(self) -> AppActionResult:
        """
        Hide application.
        
        Returns:
            AppActionResult.
        """
        if not self.bundle_id:
            return AppActionResult(
                success=False,
                message="No bundle ID specified"
            )
        
        try:
            script = f'''
            tell application "System Events"
                set targetApp to first process whose bundle identifier is "{self.bundle_id}"
                set visible of targetApp to false
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            
            return AppActionResult(
                success=True,
                message="App hidden"
            )
        except Exception as e:
            return AppActionResult(
                success=False,
                message=f"Hide failed: {e}"
            )
    
    def get_state(self) -> AppState:
        """
        Get app state.
        
        Returns:
            AppState.
        """
        info = get_app_info(self.bundle_id) if self.bundle_id else None
        if info:
            return info.state
        return AppState.NOT_RUNNING
    
    def is_running(self) -> bool:
        """Check if app is running."""
        return self.get_state() != AppState.NOT_RUNNING
    
    def get_app_name(self) -> str:
        """Get app name from bundle ID."""
        if not self.bundle_id:
            return ""
        try:
            script = f'''
            tell application "System Events"
                set targetApp to first process whose bundle identifier is "{self.bundle_id}"
                return name of targetApp
            end tell
            '''
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        return self.bundle_id.split('.')[-1]


def list_running_apps() -> List[AppInfo]:
    """
    List all running applications.
    
    Returns:
        List of AppInfo.
    """
    apps = []
    
    try:
        script = '''
        tell application "System Events"
            set appList to every process whose background only is false
            set resultList to {}
            repeat with proc in appList
                set procName to name of proc
                set procPID to process ID of proc
                set isFront to frontmost of proc
                set winCount to count of windows of proc
                set end of resultList to {procName, procPID, isFront, winCount}
            end repeat
            return resultList
        end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.stdout.strip():
            for line in result.stdout.strip().split('\n'):
                if ',' in line:
                    parts = line.split(',')
                    if len(parts) >= 4:
                        apps.append(AppInfo(
                            bundle_id="",
                            name=parts[0].strip(),
                            pid=int(parts[1].strip()),
                            state=AppState.FRONTGROUND if parts[2].strip() == 'true' else AppState.RUNNING,
                            window_count=int(parts[3].strip())
                        ))
    except Exception:
        pass
    
    return apps


def launch_app_by_name(name: str) -> AppActionResult:
    """
    Launch app by name.
    
    Args:
        name: App name.
        
    Returns:
        AppActionResult.
    """
    try:
        subprocess.run(
            ["open", "-a", name],
            capture_output=True,
            timeout=10
        )
        return AppActionResult(
            success=True,
            message=f"Launched: {name}"
        )
    except Exception as e:
        return AppActionResult(
            success=False,
            message=f"Launch failed: {e}"
        )
