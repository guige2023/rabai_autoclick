"""Shell action module for RabAI AutoClick.

Provides shell command execution actions.
"""

import subprocess
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ShellExecAction(BaseAction):
    """Execute shell command.
    
    Runs shell commands and captures output.
    """
    action_type = "shell_exec"
    display_name = "执行Shell命令"
    description = "执行Shell命令并捕获输出"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute shell command.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: command, shell, timeout, cwd, env.
        
        Returns:
            ActionResult with command output.
        """
        command = params.get('command', '')
        shell = params.get('shell', '/bin/bash')
        timeout = params.get('timeout', 60)
        cwd = params.get('cwd', None)
        env = params.get('env', None)
        
        if not command:
            return ActionResult(success=False, message="command required")
        
        try:
            env_vars = os.environ.copy()
            if env:
                env_vars.update(env)
            
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=cwd,
                env=env_vars
            )
            
            return ActionResult(
                success=result.returncode == 0,
                message=f"Exit code: {result.returncode}",
                data={
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'returncode': result.returncode,
                    'command': command
                }
            )
            
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message=f"Command timed out after {timeout}s",
                data={'timeout': timeout}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Shell error: {e}",
                data={'error': str(e)}
            )


class ShellScriptAction(BaseAction):
    """Run shell script from file.
    
    Executes pre-written shell scripts.
    """
    action_type = "shell_script"
    display_name = "运行Shell脚本"
    description = "从文件运行Shell脚本"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Run shell script.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: script_path, args, interpreter, timeout.
        
        Returns:
            ActionResult with script output.
        """
        script_path = params.get('script_path', '')
        args = params.get('args', [])
        interpreter = params.get('interpreter', '/bin/bash')
        timeout = params.get('timeout', 60)
        
        if not script_path:
            return ActionResult(success=False, message="script_path required")
        
        if not os.path.exists(script_path):
            return ActionResult(success=False, message=f"Script not found: {script_path}")
        
        cmd_args = [interpreter, script_path] + (args if isinstance(args, list) else [args])
        
        try:
            result = subprocess.run(
                cmd_args,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            return ActionResult(
                success=result.returncode == 0,
                message=f"Exit code: {result.returncode}",
                data={
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                    'returncode': result.returncode
                }
            )
            
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message=f"Script timed out after {timeout}s",
                data={'timeout': timeout}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Script error: {e}",
                data={'error': str(e)}
            )
