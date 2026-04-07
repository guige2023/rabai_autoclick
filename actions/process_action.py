"""Process management action module for RabAI AutoClick.

Provides process operations:
- ProcessRunAction: Run shell command
- ProcessKillAction: Kill process
- ProcessStatusAction: Check process status
- ProcessListAction: List processes
- ProcessSignalAction: Send signal to process
"""

from __future__ import annotations

import subprocess
import sys
import os
import signal
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ProcessRunAction(BaseAction):
    """Run shell command."""
    action_type = "process_run"
    display_name = "执行命令"
    description = "执行Shell命令"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute process run."""
        command = params.get('command', '')
        shell = params.get('shell', True)
        timeout = params.get('timeout', 60)
        cwd = params.get('cwd', None)
        env = params.get('env', {})
        capture_output = params.get('capture_output', True)
        output_var = params.get('output_var', 'process_result')

        if not command:
            return ActionResult(success=False, message="command is required")

        try:
            resolved_command = context.resolve_value(command) if context else command
            resolved_timeout = context.resolve_value(timeout) if context else timeout
            resolved_cwd = context.resolve_value(cwd) if context else cwd
            resolved_env = context.resolve_value(env) if context else env

            env_copy = dict(os.environ)
            if resolved_env:
                env_copy.update(resolved_env)

            if shell and isinstance(resolved_command, list):
                resolved_command = ' '.join(resolved_command)

            result = subprocess.run(
                resolved_command,
                shell=shell,
                capture_output=capture_output,
                timeout=resolved_timeout,
                cwd=resolved_cwd,
                env=env_copy if resolved_env else None,
                text=True
            )

            output_data = {
                'returncode': result.returncode,
                'stdout': result.stdout if capture_output else '',
                'stderr': result.stderr if capture_output else '',
                'success': result.returncode == 0,
            }

            if context:
                context.set(output_var, output_data)
            return ActionResult(
                success=result.returncode == 0,
                message=f"Exit code: {result.returncode}",
                data=output_data
            )
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message=f"Command timed out after {resolved_timeout}s")
        except Exception as e:
            return ActionResult(success=False, message=f"Process run error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'shell': True, 'timeout': 60, 'cwd': None, 'env': {},
            'capture_output': True, 'output_var': 'process_result'
        }


class ProcessKillAction(BaseAction):
    """Kill process by PID."""
    action_type = "process_kill"
    display_name = "终止进程"
    description = "终止进程"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute process kill."""
        pid = params.get('pid', 0)
        force = params.get('force', False)
        output_var = params.get('output_var', 'kill_result')

        if not pid:
            return ActionResult(success=False, message="pid is required")

        try:
            resolved_pid = context.resolve_value(pid) if context else pid
            sig = signal.SIGKILL if force else signal.SIGTERM

            os.kill(int(resolved_pid), sig)

            result = {'pid': resolved_pid, 'killed': True, 'signal': sig}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Killed process {resolved_pid}", data=result)
        except ProcessLookupError:
            return ActionResult(success=False, message=f"Process {resolved_pid} not found")
        except PermissionError:
            return ActionResult(success=False, message=f"Permission denied to kill {resolved_pid}")
        except Exception as e:
            return ActionResult(success=False, message=f"Process kill error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'force': False, 'output_var': 'kill_result'}


class ProcessStatusAction(BaseAction):
    """Check process status."""
    action_type = "process_status"
    display_name = "进程状态"
    description = "检查进程状态"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute process status."""
        pid = params.get('pid', 0)
        output_var = params.get('output_var', 'process_status')

        if not pid:
            return ActionResult(success=False, message="pid is required")

        try:
            resolved_pid = context.resolve_value(pid) if context else pid

            result = subprocess.run(
                ['ps', '-p', str(resolved_pid), '-o', 'pid,ppid,state,etime,command'],
                capture_output=True,
                text=True,
                timeout=5
            )

            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if len(lines) >= 2:
                    parts = lines[1].split(None, 4)
                    status_data = {
                        'pid': int(parts[0]),
                        'ppid': int(parts[1]) if len(parts) > 1 else 0,
                        'state': parts[2] if len(parts) > 2 else '',
                        'etime': parts[3] if len(parts) > 3 else '',
                        'command': parts[4] if len(parts) > 4 else '',
                        'running': True,
                    }
                else:
                    status_data = {'running': False, 'pid': resolved_pid}
            else:
                status_data = {'running': False, 'pid': resolved_pid}

            if context:
                context.set(output_var, status_data)
            return ActionResult(success=status_data['running'], message=f"Process {resolved_pid}: {'running' if status_data['running'] else 'not found'}", data=status_data)
        except Exception as e:
            return ActionResult(success=False, message=f"Process status error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'process_status'}


class ProcessSignalAction(BaseAction):
    """Send signal to process."""
    action_type = "process_signal"
    display_name = "发送信号"
    description = "向进程发送信号"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute signal send."""
        pid = params.get('pid', 0)
        sig = params.get('signal', 'TERM')  # TERM, HUP, INT, KILL, USR1, etc.
        output_var = params.get('output_var', 'signal_result')

        if not pid:
            return ActionResult(success=False, message="pid is required")

        try:
            resolved_pid = context.resolve_value(pid) if context else pid
            resolved_sig = context.resolve_value(sig) if context else sig

            sig_map = {
                'TERM': signal.SIGTERM, 'HUP': signal.SIGHUP, 'INT': signal.SIGINT,
                'KILL': signal.SIGKILL, 'USR1': signal.SIGUSR1, 'USR2': signal.SIGUSR2,
                'STOP': signal.SIGSTOP, 'CONT': signal.SIGCONT,
            }
            sig_val = sig_map.get(resolved_sig.upper(), signal.SIGTERM)

            os.kill(int(resolved_pid), sig_val)

            result = {'pid': resolved_pid, 'signal': resolved_sig}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Sent {resolved_sig} to process {resolved_pid}", data=result)
        except ProcessLookupError:
            return ActionResult(success=False, message=f"Process {resolved_pid} not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Signal error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pid', 'signal']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'signal_result'}
