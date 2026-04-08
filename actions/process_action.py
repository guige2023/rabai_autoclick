"""Process action module for RabAI AutoClick.

Provides process management actions including launching, killing, and monitoring processes.
"""

import subprocess
import psutil
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ProcessLaunchAction(BaseAction):
    """Launch a new process or application.
    
    Opens applications or executes shell commands.
    """
    action_type = "process_launch"
    display_name = "启动进程"
    description = "启动新进程或应用程序"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Launch process.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: command, args, app, wait, timeout.
        
        Returns:
            ActionResult with launch status.
        """
        command = params.get('command', '')
        args = params.get('args', [])
        app = params.get('app', '')
        wait = params.get('wait', False)
        timeout = params.get('timeout', 30)
        
        if not command and not app:
            return ActionResult(success=False, message="command or app required")
        
        try:
            if app:
                # Open application
                cmd = ['open', '-a', app]
                if command:
                    cmd.extend(['--args'] + ([command] if isinstance(command, list) else [command]))
                result = subprocess.run(cmd, capture_output=True, timeout=timeout)
            else:
                # Execute command
                cmd = [command] + (args if isinstance(args, list) else [args])
                if wait:
                    result = subprocess.run(cmd, capture_output=True, timeout=timeout)
                else:
                    subprocess.Popen(cmd)
                    result = None
            
            if result and result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"Launch failed: {result.stderr.decode() if result.stderr else 'unknown error'}"
                )
            
            return ActionResult(
                success=True,
                message=f"{'App' if app else 'Command'} launched",
                data={'command': command or app, 'app': app}
            )
            
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Launch timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Launch error: {e}",
                data={'error': str(e)}
            )


class ProcessKillAction(BaseAction):
    """Terminate a running process.
    
    Kills process by name or PID.
    """
    action_type = "process_kill"
    display_name = "终止进程"
    description = "终止运行中的进程"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Kill process.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: pid, name, force.
        
        Returns:
            ActionResult with kill status.
        """
        pid = params.get('pid', None)
        name = params.get('name', '')
        force = params.get('force', False)
        
        killed = []
        
        try:
            if pid:
                try:
                    p = psutil.Process(pid)
                    p.terminate() if not force else p.kill()
                    killed.append(pid)
                except psutil.NoSuchProcess:
                    pass
            
            if name:
                for p in psutil.process_iter(['pid', 'name']):
                    try:
                        if name.lower() in p.info['name'].lower():
                            p.terminate() if not force else p.kill()
                            killed.append(p.info['pid'])
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            
            return ActionResult(
                success=True,
                message=f"Killed {len(killed)} process(es)",
                data={'killed_pids': killed, 'count': len(killed)}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Kill error: {e}",
                data={'error': str(e)}
            )


class ProcessListAction(BaseAction):
    """List running processes.
    
    Returns list of processes with optional filtering.
    """
    action_type = "process_list"
    display_name = "列出进程"
    description = "列出运行中的进程"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List processes.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: name_filter, limit, user_only.
        
        Returns:
            ActionResult with process list.
        """
        name_filter = params.get('name_filter', '')
        limit = params.get('limit', 50)
        user_only = params.get('user_only', False)
        
        try:
            processes = []
            for p in psutil.process_iter(['pid', 'name', 'username', 'cpu_percent', 'memory_percent']):
                try:
                    info = p.info
                    if name_filter and name_filter.lower() not in info['name'].lower():
                        continue
                    if user_only and info.get('username') != os.getenv('USER'):
                        continue
                    processes.append(info)
                    if len(processes) >= limit:
                        break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            
            return ActionResult(
                success=True,
                message=f"Found {len(processes)} process(es)",
                data={'processes': processes, 'count': len(processes)}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"List error: {e}",
                data={'error': str(e)}
            )
