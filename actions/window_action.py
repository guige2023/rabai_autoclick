"""Window action module for RabAI AutoClick.

Provides window management actions including minimize, maximize, move, and focus.
"""

import subprocess
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WindowMinimizeAction(BaseAction):
    """Minimize a window to dock.
    
    Minimizes the specified or frontmost window.
    """
    action_type = "window_minimize"
    display_name = "最小化窗口"
    description = "最小化窗口到Dock"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Minimize window.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: window_title, app_name.
        
        Returns:
            ActionResult with minimize status.
        """
        window_title = params.get('window_title', '')
        app_name = params.get('app_name', '')
        
        try:
            script = 'tell application "System Events" to tell process'
            
            if app_name:
                script += f' "{app_name}"'
                if window_title:
                    script += f' to set miniaturized of (first window whose name contains "{window_title}") to true'
                else:
                    script += ' to set miniaturized of first window to true'
            else:
                script = f'''
                tell application "System Events"
                    set frontApp to first application process whose frontmost is true
                    set miniaturized of first window of frontApp to true
                end tell
                '''
            
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message="Window minimized",
                    data={'window_title': window_title, 'app_name': app_name}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"osascript error: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Minimize timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Minimize error: {e}",
                data={'error': str(e)}
            )


class WindowMaximizeAction(BaseAction):
    """Maximize a window to full screen.
    
    Zooms window to fill the screen.
    """
    action_type = "window_maximize"
    display_name = "最大化窗口"
    description = "最大化窗口到全屏"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Maximize window.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: window_title, app_name.
        
        Returns:
            ActionResult with maximize status.
        """
        window_title = params.get('window_title', '')
        app_name = params.get('app_name', '')
        
        try:
            script = 'tell application "System Events" to tell process'
            
            if app_name:
                script += f' "{app_name}"'
                if window_title:
                    script += f' to setzoomed of (first window whose name contains "{window_title}") to true'
                else:
                    script += ' to setzoomed of first window to true'
            else:
                script = f'''
                tell application "System Events"
                    set frontApp to first application process whose frontmost is true
                    setzoomed of first window of frontApp to true
                end tell
                '''
            
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message="Window maximized",
                    data={'window_title': window_title, 'app_name': app_name}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"osascript error: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Maximize timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Maximize error: {e}",
                data={'error': str(e)}
            )


class WindowMoveAction(BaseAction):
    """Move window to specified position.
    
    Repositions window to x, y coordinates.
    """
    action_type = "window_move"
    display_name = "移动窗口"
    description = "移动窗口到指定位置"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Move window.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: x, y, window_title, app_name.
        
        Returns:
            ActionResult with move status.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)
        window_title = params.get('window_title', '')
        app_name = params.get('app_name', '')
        
        try:
            if app_name:
                if window_title:
                    script = f'''
                    tell application "System Events"
                        tell process "{app_name}"
                            set position of (first window whose name contains "{window_title}") to {{{x}, {y}}}
                        end tell
                    end tell
                    '''
                else:
                    script = f'''
                    tell application "System Events"
                        tell process "{app_name}"
                            set position of first window to {{{x}, {y}}}
                        end tell
                    end tell
                    '''
            else:
                script = f'''
                tell application "System Events"
                    set frontApp to first application process whose frontmost is true
                    set position of first window of frontApp to {{{x}, {y}}}
                end tell
                '''
            
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Window moved to ({x}, {y})",
                    data={'x': x, 'y': y, 'window_title': window_title, 'app_name': app_name}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"osascript error: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Move timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Move error: {e}",
                data={'error': str(e)}
            )


class WindowResizeAction(BaseAction):
    """Resize window to specified dimensions.
    
    Changes window size to width x height.
    """
    action_type = "window_resize"
    display_name = "调整窗口大小"
    description = "调整窗口尺寸"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Resize window.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: width, height, window_title, app_name.
        
        Returns:
            ActionResult with resize status.
        """
        width = params.get('width', 800)
        height = params.get('height', 600)
        window_title = params.get('window_title', '')
        app_name = params.get('app_name', '')
        
        try:
            if app_name:
                if window_title:
                    script = f'''
                    tell application "System Events"
                        tell process "{app_name}"
                            set size of (first window whose name contains "{window_title}") to {{{width}, {height}}}
                        end tell
                    end tell
                    '''
                else:
                    script = f'''
                    tell application "System Events"
                        tell process "{app_name}"
                            set size of first window to {{{width}, {height}}}
                        end tell
                    end tell
                    '''
            else:
                script = f'''
                tell application "System Events"
                    set frontApp to first application process whose frontmost is true
                    set size of first window of frontApp to {{{width}, {height}}}
                end tell
                '''
            
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Window resized to {width}x{height}",
                    data={'width': width, 'height': height, 'window_title': window_title, 'app_name': app_name}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"osascript error: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Resize timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Resize error: {e}",
                data={'error': str(e)}
            )


class WindowFocusAction(BaseAction):
    """Focus and bring window to front.
    
    Activates the specified application or window.
    """
    action_type = "window_focus"
    display_name = "聚焦窗口"
    description = "聚焦并激活窗口"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Focus window.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: app_name, window_title.
        
        Returns:
            ActionResult with focus status.
        """
        app_name = params.get('app_name', '')
        window_title = params.get('window_title', '')
        
        if not app_name:
            return ActionResult(success=False, message="app_name is required")
        
        try:
            script = f'tell application "{app_name}" to activate'
            
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Focused: {app_name}",
                    data={'app_name': app_name, 'window_title': window_title}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"osascript error: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Focus timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Focus error: {e}",
                data={'error': str(e)}
            )
