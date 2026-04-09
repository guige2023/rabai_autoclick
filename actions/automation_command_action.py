"""Automation Command Action Module.

Executes shell/system commands as part of automation workflows,
with timeout, output capture, and error handling.
"""

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
import subprocess
import shlex
import logging

logger = logging.getLogger(__name__)


@dataclass
class CommandResult:
    """Result of a command execution."""
    command: str
    returncode: int
    stdout: str
    stderr: str
    duration_sec: float
    success: bool


class AutomationCommandAction:
    """Execute system commands in automation workflows.
    
    Provides safe command execution with configurable timeout,
    environment variables, working directory, and output capture.
    """

    def __init__(
        self,
        default_timeout: int = 30,
        default_cwd: Optional[str] = None,
    ) -> None:
        self.default_timeout = default_timeout
        self.default_cwd = default_cwd

    def execute(
        self,
        command: str,
        timeout: Optional[int] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: bool = False,
        capture: bool = True,
    ) -> CommandResult:
        """Execute a shell command.
        
        Args:
            command: Command string or list of args.
            timeout: Timeout in seconds (uses default if None).
            cwd: Working directory (uses default_cwd if None).
            env: Additional environment variables.
            shell: Run through shell if True.
            capture: Capture stdout/stderr if True.
        
        Returns:
            CommandResult with output and exit status.
        """
        timeout = timeout if timeout is not None else self.default_timeout
        cwd = cwd or self.default_cwd
        import time
        start = time.time()

        try:
            cmd_list = command if isinstance(command, list) else shlex.split(command)
            logger.debug("Executing command: %s", command)

            process = subprocess.Popen(
                cmd_list if not shell else command,
                stdout=subprocess.PIPE if capture else None,
                stderr=subprocess.PIPE if capture else None,
                cwd=cwd,
                env=env,
                shell=shell,
                text=True,
            )
            stdout, stderr = process.communicate(timeout=timeout)
            duration = time.time() - start

            result = CommandResult(
                command=command,
                returncode=process.returncode,
                stdout=stdout or "",
                stderr=stderr or "",
                duration_sec=duration,
                success=process.returncode == 0,
            )
            logger.info(
                "Command completed: rc=%d duration=%.3fs success=%s",
                result.returncode, result.duration_sec, result.success,
            )
            return result

        except subprocess.TimeoutExpired:
            duration = time.time() - start
            logger.error("Command timed out after %ds: %s", timeout, command)
            return CommandResult(
                command=command, returncode=-1,
                stdout="", stderr=f"Command timed out after {timeout}s",
                duration_sec=duration, success=False,
            )
        except Exception as exc:
            duration = time.time() - start
            logger.error("Command failed: %s -> %s", command, exc)
            return CommandResult(
                command=command, returncode=-2,
                stdout="", stderr=str(exc),
                duration_sec=duration, success=False,
            )

    def execute_batch(
        self,
        commands: List[str],
        stop_on_failure: bool = True,
    ) -> List[CommandResult]:
        """Execute a list of commands in sequence.
        
        Args:
            commands: List of command strings.
            stop_on_failure: Stop and return if a command fails.
        
        Returns:
            List of CommandResult in execution order.
        """
        results: List[CommandResult] = []
        for cmd in commands:
            result = self.execute(cmd)
            results.append(result)
            if stop_on_failure and not result.success:
                logger.warning("Batch execution stopped at failed command: %s", cmd)
                break
        return results
