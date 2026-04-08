"""System action module for RabAI AutoClick.

Provides system-level actions including sleep, restart, and system info.
"""

import subprocess
import os
import sys
import platform
import uuid
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SystemSleepAction(BaseAction):
    """Put system to sleep.
    
    Triggers system sleep mode.
    """
    action_type = "system_sleep"
    display_name = "系统休眠"
    description = "让系统进入休眠模式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Put system to sleep.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict (unused).
        
        Returns:
            ActionResult with sleep status.
        """
        try:
            # pmset sleepnow only works on Mac
            result = subprocess.run(
                ['pmset', 'sleepnow'],
                capture_output=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message="System going to sleep",
                    data={'action': 'sleep'}
                )
            else:
                return ActionResult(
                    success=False,
                    message="Sleep not available"
                )
                
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="Sleep not supported on this platform"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Sleep error: {e}",
                data={'error': str(e)}
            )


class SystemRestartAction(BaseAction):
    """Restart the system.
    
    Initiates system restart.
    """
    action_type = "system_restart"
    display_name = "系统重启"
    description = "重启系统"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Restart system.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: delay (seconds before restart).
        
        Returns:
            ActionResult with restart status.
        """
        delay = params.get('delay', 60)
        
        if delay < 0:
            return ActionResult(success=False, message="delay must be positive")
        
        try:
            cmd = ['shutdown', '-r', f'+{delay // 60}']
            result = subprocess.run(cmd, capture_output=True, timeout=5)
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"System restarting in {delay}s",
                    data={'delay': delay, 'action': 'restart'}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Restart failed: {result.stderr.decode()}"
                )
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Restart error: {e}",
                data={'error': str(e)}
            )


class SystemInfoAction(BaseAction):
    """Get system information.
    
    Returns OS, hardware, and network info.
    """
    action_type = "system_info"
    display_name = "系统信息"
    description = "获取系统信息"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get system info.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: info_type (all/cpu/memory/disk/network).
        
        Returns:
            ActionResult with system information.
        """
        info_type = params.get('info_type', 'all')
        
        try:
            info = {
                'platform': platform.system(),
                'platform_release': platform.release(),
                'platform_version': platform.version(),
                'architecture': platform.machine(),
                'processor': platform.processor(),
                'hostname': platform.node(),
                'python_version': platform.python_version()
            }
            
            if info_type in ['all', 'cpu']:
                info['cpu_count'] = os.cpu_count()
                
            if info_type in ['all', 'memory']:
                try:
                    import psutil
                    mem = psutil.virtual_memory()
                    info['memory_total'] = mem.total
                    info['memory_available'] = mem.available
                    info['memory_percent'] = mem.percent
                except ImportError:
                    pass
                    
            if info_type in ['all', 'disk']:
                try:
                    import psutil
                    disk = psutil.disk_usage('/')
                    info['disk_total'] = disk.total
                    info['disk_free'] = disk.free
                    info['disk_percent'] = disk.percent
                except ImportError:
                    pass
                    
            if info_type in ['all', 'network']:
                try:
                    import socket
                    info['hostname'] = socket.gethostname()
                    info['fqdn'] = socket.getfqdn()
                except:
                    pass
            
            return ActionResult(
                success=True,
                message=f"System info retrieved",
                data=info
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"System info error: {e}",
                data={'error': str(e)}
            )


class SystemLockAction(BaseAction):
    """Lock the system screen.
    
    Triggers screen lock.
    """
    action_type = "system_lock"
    display_name = "锁定屏幕"
    description = "锁定系统屏幕"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Lock system.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict (unused).
        
        Returns:
            ActionResult with lock status.
        """
        try:
            # Use /System/Library/CoreServices/Menu\ Extras/User.menu/Contents/Resources/CGSession -lock
            cmd = ['/System/Library/CoreServices/Menu Extras/User.menu/Contents/Resources/CGSession', '-lock']
            result = subprocess.run(cmd, capture_output=True, timeout=5)
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message="System locked",
                    data={'action': 'lock'}
                )
            else:
                return ActionResult(
                    success=False,
                    message="Lock failed"
                )
                
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="Lock not available on this platform"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Lock error: {e}",
                data={'error': str(e)}
            )
