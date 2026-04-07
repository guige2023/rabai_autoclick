"""Shell utilities for RabAI AutoClick.

Provides:
- Shell command execution helpers
- Environment variable helpers
- Path manipulation
"""

import os
import shlex
import subprocess
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Union,
)


def run_command(
    cmd: Union[str, List[str]],
    *,
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
    check: bool = False,
    capture_output: bool = True,
    shell: bool = False,
    text: bool = True,
) -> subprocess.CompletedProcess:
    """Run a shell command.

    Args:
        cmd: Command to run (string or list).
        cwd: Working directory.
        env: Environment variables.
        timeout: Timeout in seconds.
        check: If True, raise CalledProcessError on non-zero exit.
        capture_output: If True, capture stdout and stderr.
        shell: If True, run through shell.
        text: If True, return string output.

    Returns:
        CompletedProcess instance.
    """
    if isinstance(cmd, str) and not shell:
        cmd = shlex.split(cmd)

    merged_env = dict(os.environ)
    if env:
        merged_env.update(env)

    return subprocess.run(
        cmd,
        cwd=cwd,
        env=merged_env if env else None,
        timeout=timeout,
        check=check,
        capture_output=capture_output,
        shell=shell,
        text=text,
    )


def run_command_output(
    cmd: Union[str, List[str]],
    *,
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
    strip: bool = True,
) -> str:
    """Run a command and return stdout.

    Args:
        cmd: Command to run.
        cwd: Working directory.
        env: Environment variables.
        timeout: Timeout in seconds.
        strip: If True, strip whitespace from output.

    Returns:
        Command stdout as string.
    """
    result = run_command(
        cmd,
        cwd=cwd,
        env=env,
        timeout=timeout,
        capture_output=True,
        text=True,
    )
    output = result.stdout
    if strip:
        output = output.strip()
    return output


def run_command_lines(
    cmd: Union[str, List[str]],
    *,
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
) -> List[str]:
    """Run a command and return stdout as lines.

    Args:
        cmd: Command to run.
        cwd: Working directory.
        env: Environment variables.
        timeout: Timeout in seconds.

    Returns:
        List of output lines (non-empty only).
    """
    output = run_command_output(cmd, cwd=cwd, env=env, timeout=timeout)
    return [line for line in output.splitlines() if line.strip()]


def run_background(
    cmd: Union[str, List[str]],
    *,
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    name: Optional[str] = None,
) -> subprocess.Popen:
    """Run a command in the background.

    Args:
        cmd: Command to run.
        cwd: Working directory.
        env: Environment variables.
        name: Optional process name.

    Returns:
        Popen instance.
    """
    if isinstance(cmd, str) and not name:
        name = cmd.split()[0]

    merged_env = dict(os.environ)
    if env:
        merged_env.update(env)

    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=merged_env if env else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return process


def which(program: str) -> Optional[str]:
    """Find an executable in PATH.

    Args:
        program: Program name to find.

    Returns:
        Full path to executable or None.
    """
    if os.path.isabs(program) and os.access(program, os.X_OK):
        return program

    for path_dir in os.environ.get("PATH", "").split(os.pathsep):
        full_path = os.path.join(path_dir, program)
        if os.access(full_path, os.X_OK):
            return full_path

    return None


def has_command(program: str) -> bool:
    """Check if a command is available.

    Args:
        program: Program name.

    Returns:
        True if command exists in PATH.
    """
    return which(program) is not None


def get_env(
    key: str,
    default: Optional[str] = None,
) -> Optional[str]:
    """Get environment variable.

    Args:
        key: Variable name.
        default: Default value if not set.

    Returns:
        Environment variable value or default.
    """
    return os.environ.get(key, default)


def set_env(
    key: str,
    value: str,
    *,
    overwrite: bool = True,
) -> Optional[str]:
    """Set an environment variable.

    Args:
        key: Variable name.
        value: Variable value.
        overwrite: If False, don't overwrite existing.

    Returns:
        Previous value or None.
    """
    if not overwrite and key in os.environ:
        return os.environ[key]

    return os.environ.setdefault(key, value)


def unset_env(key: str) -> Optional[str]:
    """Unset an environment variable.

    Args:
        key: Variable name.

    Returns:
        Previous value or None.
    """
    return os.environ.pop(key, None)


def expand_env_vars(text: str) -> str:
    """Expand environment variables in text.

    Args:
        text: Text with $VAR or ${VAR} references.

    Returns:
        Expanded text.
    """
    return os.path.expandvars(text)


def get_home_dir() -> str:
    """Get user home directory.

    Returns:
        Home directory path.
    """
    return os.path.expanduser("~")


def get_temp_dir() -> str:
    """Get temporary directory path.

    Returns:
        Temp directory path.
    """
    import tempfile
    return tempfile.gettempdir()


def normalize_path(path: str) -> str:
    """Normalize a path (expand user, resolve symlinks).

    Args:
        path: Path to normalize.

    Returns:
        Normalized path.
    """
    return os.path.normpath(os.path.expanduser(os.path.expandvars(path)))


def split_path(path: str) -> Dict[str, str]:
    """Split a path into its components.

    Args:
        path: Path to split.

    Returns:
        Dict with 'dir', 'name', 'ext' keys.
    """
    return {
        "dir": os.path.dirname(path),
        "name": os.path.basename(path),
        "ext": os.path.splitext(path)[1],
        "stem": os.path.splitext(path)[0],
    }


def shell_quote(text: str) -> str:
    """Quote text for shell safety.

    Args:
        text: Text to quote.

    Returns:
        Shell-safe quoted string.
    """
    return shlex.quote(text)


def make_executable(filepath: str) -> None:
    """Make a file executable.

    Args:
        filepath: Path to file.
    """
    import stat
    os.chmod(filepath, os.stat(filepath).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def CommandResult = subprocess.CompletedProcess
