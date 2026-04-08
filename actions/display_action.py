"""Display action module for RabAI AutoClick.

Provides display and monitor management actions.
"""

import subprocess
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DisplayListAction(BaseAction):
    """List connected displays.
    
    Returns information about all connected monitors.
    """
    action_type = "display_list"
    display_name = "列出显示器"
    description = "列出所有连接的显示器"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List displays.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict (unused).
        
        Returns:
            ActionResult with display list.
        """
        try:
            script = '''
            tell application "System Events"
                get bounds of every window of every process whose name is "Window Server"
            end tell
            '''
            # Alternative: use system_profiler
            result = subprocess.run(
                ['system_profiler', 'SPDisplaysDataType', '-json'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            import json
            data = json.loads(result.stdout)
            
            displays = []
            if 'SPDisplaysDataType' in data:
                for disp in data['SPDisplaysDataType']:
                    displays.append({
                        'name': disp.get('sppfs_display_product_name', 'Unknown'),
                        'resolution': disp.get('sppfs_display_resolution', 'Unknown'),
                        'connected': True
                    })
            
            return ActionResult(
                success=True,
                message=f"Found {len(displays)} display(s)",
                data={'displays': displays, 'count': len(displays)}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Display list error: {e}",
                data={'error': str(e)}
            )


class DisplayBrightnessAction(BaseAction):
    """Control display brightness.
    
    Gets or sets display brightness level.
    """
    action_type = "display_brightness"
    display_name = "屏幕亮度"
    description = "控制显示器亮度"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Control brightness.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: brightness (0-100), direction (up/down/set/get).
        
        Returns:
            ActionResult with brightness status.
        """
        brightness = params.get('brightness', None)
        direction = params.get('direction', 'get')
        
        try:
            if direction == 'get':
                result = subprocess.run(
                    ['brightness', '-l'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                
                for line in result.stdout.split('\n'):
                    if 'display 0' in line.lower():
                        val = line.split(':')[1].strip()
                        current = float(val) * 100
                        return ActionResult(
                            success=True,
                            message=f"Current brightness: {current:.0f}%",
                            data={'brightness': current}
                        )
                
                return ActionResult(success=False, message="Could not get brightness")
            
            elif direction in ['up', 'down']:
                step = 10
                current = brightness or 50
                new_val = min(100, current + step) if direction == 'up' else max(0, current - step)
                subprocess.run(['brightness', str(new_val / 100)], timeout=5)
                return ActionResult(
                    success=True,
                    message=f"Brightness {'+' if direction == 'up' else '-'}: {new_val:.0f}%",
                    data={'brightness': new_val}
                )
            
            elif direction == 'set' and brightness is not None:
                if not 0 <= brightness <= 100:
                    return ActionResult(success=False, message="brightness must be 0-100")
                subprocess.run(['brightness', str(brightness / 100)], timeout=5)
                return ActionResult(
                    success=True,
                    message=f"Brightness set to {brightness}%",
                    data={'brightness': brightness}
                )
            
            return ActionResult(success=False, message=f"Unknown direction: {direction}")
            
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="brightness command not found"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Brightness error: {e}",
                data={'error': str(e)}
            )


class DisplayResolutionAction(BaseAction):
    """Get or set display resolution.
    
    Changes display resolution.
    """
    action_type = "display_resolution"
    display_name = "屏幕分辨率"
    description = "获取或设置屏幕分辨率"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Control resolution.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: width, height, display.
        
        Returns:
            ActionResult with resolution status.
        """
        width = params.get('width', None)
        height = params.get('height', None)
        display = params.get('display', 0)
        
        if width and height:
            try:
                # Use cscreen or CGSwitchResolution
                cmd = ['/usr/sbin/cscreen', '-x', str(width), '-y', str(height), '-d', str(display)]
                result = subprocess.run(cmd, capture_output=True, timeout=10)
                
                if result.returncode == 0:
                    return ActionResult(
                        success=True,
                        message=f"Resolution set to {width}x{height}",
                        data={'width': width, 'height': height}
                    )
                else:
                    return ActionResult(
                        success=False,
                        message=f"Resolution change failed"
                    )
                    
            except FileNotFoundError:
                return ActionResult(
                    success=False,
                    message="Resolution change not available"
                )
                return ActionResult(success=False, message="Resolution change not available")
        else:
            # Get current resolution
            try:
                result = subprocess.run(
                    ['system_profiler', 'SPDisplaysDataType', '-json'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                import json
                data = json.loads(result.stdout)
                
                if 'SPDisplaysDataType' in data:
                    for disp in data['SPDisplaysDataType']:
                        if 'sppfs_display_resolution' in disp:
                            res = disp['sppfs_display_resolution']
                            parts = res.split('x')
                            return ActionResult(
                                success=True,
                                message=f"Current resolution: {res}",
                                data={'resolution': res, 'width': int(parts[0]), 'height': int(parts[1])}
                            )
                            
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Resolution error: {e}",
                    data={'error': str(e)}
                )


class DisplaySleepAction(BaseAction):
    """Put display to sleep.
    
    Turns off display while system stays on.
    """
    action_type = "display_sleep"
    display_name = "显示器休眠"
    description = "关闭显示器（系统保持运行）"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Put display to sleep.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict (unused).
        
        Returns:
            ActionResult with status.
        """
        try:
            # Use pmset displaysleepnow
            result = subprocess.run(
                ['pmset', 'displaysleepnow'],
                capture_output=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message="Display going to sleep",
                    data={'action': 'display_sleep'}
                )
            else:
                return ActionResult(
                    success=False,
                    message="Display sleep failed"
                )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Display sleep error: {e}",
                data={'error': str(e)}
            )
