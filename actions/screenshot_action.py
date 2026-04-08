"""Screenshot action module for RabAI AutoClick.

Provides screenshot capture and image comparison actions.
"""

import subprocess
import os
import sys
from typing import Any, Dict, Optional
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ScreenshotCaptureAction(BaseAction):
    """Capture screenshot with various options.
    
    Full screen, region, or window capture.
    """
    action_type = "screenshot_capture"
    display_name = "屏幕截图"
    description = "捕获屏幕截图"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Capture screenshot.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: path, region, include_cursor, display.
        
        Returns:
            ActionResult with screenshot path.
        """
        path = params.get('path', f'/tmp/screenshot_{int(time.time())}.png')
        region = params.get('region', None)
        include_cursor = params.get('include_cursor', False)
        display = params.get('display', 0)
        
        try:
            cmd = ['screencapture']
            
            if region:
                x, y, w, h = region
                cmd.extend(['-R', f'{x},{y},{w},{h}'])
            
            if include_cursor:
                cmd.append('-C')
            
            cmd.extend(['-x', path])
            
            result = subprocess.run(cmd, capture_output=True, timeout=10)
            
            if result.returncode == 0 and os.path.exists(path):
                size = os.path.getsize(path)
                return ActionResult(
                    success=True,
                    message=f"Screenshot saved: {path}",
                    data={'path': path, 'size': size, 'region': region}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"screencapture failed"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Screenshot timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Screenshot error: {e}",
                data={'error': str(e)}
            )


class ScreenshotWindowAction(BaseAction):
    """Capture screenshot of specific window.
    
    Uses window ID or name to capture window content.
    """
    action_type = "screenshot_window"
    display_name = "窗口截图"
    description = "捕获特定窗口的截图"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Capture window screenshot.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: window_name, path, window_id.
        
        Returns:
            ActionResult with screenshot path.
        """
        window_name = params.get('window_name', '')
        window_id = params.get('window_id', None)
        path = params.get('path', f'/tmp/window_{int(time.time())}.png')
        
        try:
            cmd = ['screencapture', '-x', '-w']
            if window_id:
                cmd.extend(['-l', str(window_id)])
            cmd.append(path)
            
            result = subprocess.run(cmd, capture_output=True, timeout=15)
            
            if result.returncode == 0 and os.path.exists(path):
                size = os.path.getsize(path)
                return ActionResult(
                    success=True,
                    message=f"Window screenshot saved",
                    data={'path': path, 'size': size}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Window capture failed"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Window capture timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Window screenshot error: {e}",
                data={'error': str(e)}
            )


class ScreenRecordAction(BaseAction):
    """Record screen as video.
    
    Records screen for specified duration.
    """
    action_type = "screen_record"
    display_name = "屏幕录制"
    description = "录制屏幕为视频"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Record screen.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: path, duration, fps, region.
        
        Returns:
            ActionResult with recording status.
        """
        path = params.get('path', f'/tmp/screen_record_{int(time.time())}.mov')
        duration = params.get('duration', 10)
        fps = params.get('fps', 30)
        region = params.get('region', None)
        
        if duration > 300:
            return ActionResult(success=False, message="Duration exceeds 5 minutes")
        
        try:
            # Use QuickTime screen recording via AppleScript
            script = f'''
            tell application "QuickTime Player"
                set newRecording to new movie recording
                tell newRecording
                    start
                    delay {duration}
                    stop
                end tell
                export newRecording in "{path}"
                close newRecording
            end tell
            '''
            
            result = subprocess.run(
                ['osascript', '-e', script],
                capture_output=True,
                timeout=duration + 30
            )
            
            if result.returncode == 0 and os.path.exists(path):
                size = os.path.getsize(path)
                return ActionResult(
                    success=True,
                    message=f"Recording saved: {path}",
                    data={'path': path, 'size': size, 'duration': duration}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Recording failed: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Recording timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Recording error: {e}",
                data={'error': str(e)}
            )
