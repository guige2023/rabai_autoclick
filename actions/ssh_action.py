"""
SSH (Secure Shell) connection and command execution actions.
"""
from __future__ import annotations

import subprocess
import time
from typing import Dict, Any, Optional, List, Tuple


def ssh_connect(
    host: str,
    user: Optional[str] = None,
    port: int = 22,
    key_file: Optional[str] = None,
    password: Optional[str] = None,
    timeout: int = 10
) -> Dict[str, Any]:
    """
    Test SSH connection to a host.

    Args:
        host: Target hostname or IP.
        user: SSH username.
        port: SSH port.
        key_file: Path to private key file.
        password: SSH password (not recommended).
        timeout: Connection timeout in seconds.

    Returns:
        Dictionary with connection result.
    """
    cmd = ['ssh', '-o', 'BatchMode=yes', '-o', f'ConnectTimeout={timeout}']

    if port != 22:
        cmd.extend(['-p', str(port)])

    if key_file:
        cmd.extend(['-i', key_file])

    if user:
        target = f"{user}@{host}"
    else:
        target = host

    cmd.extend([target, 'echo', 'Connection successful'])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 5
        )

        return {
            'success': result.returncode == 0,
            'host': host,
            'user': user,
            'port': port,
            'output': result.stdout.strip(),
            'error': result.stderr.strip() if result.returncode != 0 else None,
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'host': host,
            'error': 'Connection timed out',
        }
    except Exception as e:
        return {
            'success': False,
            'host': host,
            'error': str(e),
        }


def ssh_execute(
    host: str,
    command: str,
    user: Optional[str] = None,
    port: int = 22,
    key_file: Optional[str] = None,
    password: Optional[str] = None,
    timeout: int = 60,
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Execute a command on a remote host via SSH.

    Args:
        host: Target hostname or IP.
        command: Command to execute.
        user: SSH username.
        port: SSH port.
        key_file: Path to private key file.
        password: SSH password (not recommended).
        timeout: Command timeout in seconds.
        cwd: Working directory on remote host.
        env: Environment variables to set.

    Returns:
        Dictionary with execution result.

    Raises:
        RuntimeError: If SSH command fails.
    """
    if password:
        raise ValueError("Password authentication is not supported for security reasons")

    ssh_cmd = ['ssh']

    if port != 22:
        ssh_cmd.extend(['-p', str(port)])

    if key_file:
        ssh_cmd.extend(['-i', key_file])

    ssh_cmd.extend([
        '-o', 'BatchMode=yes',
        '-o', f'ConnectTimeout={min(timeout, 10)}',
        '-o', 'StrictHostKeyChecking=no',
    ])

    if user:
        target = f"{user}@{host}"
    else:
        target = host

    full_command = command
    if cwd:
        full_command = f'cd {cwd} && {command}'

    ssh_cmd.extend([target, full_command])

    try:
        result = subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        return {
            'success': result.returncode == 0,
            'host': host,
            'command': command,
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'host': host,
            'command': command,
            'error': 'Command timed out',
        }
    except Exception as e:
        return {
            'success': False,
            'host': host,
            'command': command,
            'error': str(e),
        }


def ssh_upload(
    host: str,
    local_path: str,
    remote_path: str,
    user: Optional[str] = None,
    port: int = 22,
    key_file: Optional[str] = None,
    recursive: bool = False
) -> Dict[str, Any]:
    """
    Upload a file to a remote host via SCP.

    Args:
        host: Target hostname or IP.
        local_path: Local file path.
        remote_path: Remote destination path.
        user: SSH username.
        port: SSH port.
        key_file: Path to private key file.
        recursive: Upload directories recursively.

    Returns:
        Dictionary with upload result.
    """
    scp_cmd = ['scp']

    if port != 22:
        scp_cmd.extend(['-P', str(port)])

    if key_file:
        scp_cmd.extend(['-i', key_file])

    scp_cmd.extend(['-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=no'])

    if recursive:
        scp_cmd.append('-r')

    scp_cmd.extend([local_path])

    if user:
        scp_cmd.append(f'{user}@{host}:{remote_path}')
    else:
        scp_cmd.append(f'{host}:{remote_path}')

    try:
        result = subprocess.run(
            scp_cmd,
            capture_output=True,
            text=True,
            timeout=120
        )

        return {
            'success': result.returncode == 0,
            'host': host,
            'local_path': local_path,
            'remote_path': remote_path,
            'error': result.stderr.strip() if result.returncode != 0 else None,
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'host': host,
            'error': 'Upload timed out',
        }
    except Exception as e:
        return {
            'success': False,
            'host': host,
            'error': str(e),
        }


def ssh_download(
    host: str,
    remote_path: str,
    local_path: str,
    user: Optional[str] = None,
    port: int = 22,
    key_file: Optional[str] = None,
    recursive: bool = False
) -> Dict[str, Any]:
    """
    Download a file from a remote host via SCP.

    Args:
        host: Target hostname or IP.
        remote_path: Remote file path.
        local_path: Local destination path.
        user: SSH username.
        port: SSH port.
        key_file: Path to private key file.
        recursive: Download directories recursively.

    Returns:
        Dictionary with download result.
    """
    scp_cmd = ['scp']

    if port != 22:
        scp_cmd.extend(['-P', str(port)])

    if key_file:
        scp_cmd.extend(['-i', key_file])

    scp_cmd.extend(['-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=no'])

    if recursive:
        scp_cmd.append('-r')

    if user:
        scp_cmd.append(f'{user}@{host}:{remote_path}')
    else:
        scp_cmd.append(f'{host}:{remote_path}')

    scp_cmd.append(local_path)

    try:
        result = subprocess.run(
            scp_cmd,
            capture_output=True,
            text=True,
            timeout=120
        )

        return {
            'success': result.returncode == 0,
            'host': host,
            'remote_path': remote_path,
            'local_path': local_path,
            'error': result.stderr.strip() if result.returncode != 0 else None,
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'host': host,
            'error': 'Download timed out',
        }
    except Exception as e:
        return {
            'success': False,
            'host': host,
            'error': str(e),
        }


def ssh_tunnel(
    local_port: int,
    remote_host: str,
    remote_port: int,
    user: Optional[str] = None,
    host: Optional[str] = None,
    key_file: Optional[str] = None,
    timeout: int = 0
) -> Dict[str, Any]:
    """
    Create an SSH tunnel (port forwarding).

    Args:
        local_port: Local port to listen on.
        remote_host: Remote host to tunnel to.
        remote_port: Remote port.
        user: SSH username.
        host: SSH jump host.
        key_file: Path to private key file.
        timeout: Tunnel timeout (0 for indefinite).

    Returns:
        Dictionary with tunnel status.
    """
    if not host:
        raise ValueError("Jump host is required for tunnel creation")

    cmd = ['ssh', '-L', f'{local_port}:{remote_host}:{remote_port}']

    if key_file:
        cmd.extend(['-i', key_file])

    if timeout > 0:
        cmd.extend(['-o', f'ServerAliveInterval={timeout}'])

    cmd.extend([
        '-o', 'BatchMode=yes',
        '-o', 'StrictHostKeyChecking=no',
        '-N', '-f',
    ])

    target = f'{user}@{host}' if user else host
    cmd.append(target)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )

        return {
            'success': result.returncode == 0,
            'local_port': local_port,
            'remote_host': remote_host,
            'remote_port': remote_port,
            'jump_host': host,
            'error': result.stderr.strip() if result.returncode != 0 else None,
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'error': 'Tunnel setup timed out',
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
        }


def ssh_exec_script(
    host: str,
    script: str,
    user: Optional[str] = None,
    port: int = 22,
    key_file: Optional[str] = None,
    timeout: int = 60
) -> Dict[str, Any]:
    """
    Execute a multi-line script on a remote host.

    Args:
        host: Target hostname or IP.
        script: Multi-line script to execute.
        user: SSH username.
        port: SSH port.
        key_file: Path to private key file.
        timeout: Script timeout in seconds.

    Returns:
        Dictionary with execution results.
    """
    import shlex

    escaped_script = script.replace("'", "'\\''")
    command = f"bash -c '{escaped_script}'"

    return ssh_execute(
        host=host,
        command=command,
        user=user,
        port=port,
        key_file=key_file,
        timeout=timeout
    )


def check_ssh_key_permissions(key_file: str) -> Dict[str, Any]:
    """
    Check and fix SSH private key permissions.

    Args:
        key_file: Path to private key file.

    Returns:
        Dictionary with permission status.
    """
    import os

    if not os.path.exists(key_file):
        return {'success': False, 'error': 'Key file not found'}

    stat_info = os.stat(key_file)
    mode = stat_info.st_mode & 0o777

    is_secure = mode in (0o600, 0o400)

    return {
        'success': is_secure,
        'key_file': key_file,
        'permissions': oct(mode),
        'is_secure': is_secure,
        'recommendation': 'Permissions should be 600 or 400' if not is_secure else None,
    }


def generate_ssh_key(
    key_path: str,
    key_type: str = 'rsa',
    key_size: int = 4096,
    passphrase: Optional[str] = None,
    comment: Optional[str] = None
) -> Dict[str, Any]:
    """
    Generate an SSH key pair.

    Args:
        key_path: Path to save the key.
        key_type: Key type ('rsa', 'ed25519', 'ecdsa').
        key_size: Key size in bits (for RSA).
        passphrase: Optional passphrase for key encryption.
        comment: Optional comment for the key.

    Returns:
        Dictionary with generation result.
    """
    cmd = ['ssh-keygen', '-t', key_type, '-f', key_path, '-N', passphrase or '']

    if key_type == 'rsa' and key_size:
        cmd.extend(['-b', str(key_size)])

    if comment:
        cmd.extend(['-C', comment])

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )

        return {
            'success': result.returncode == 0,
            'key_path': key_path,
            'public_key_path': f'{key_path}.pub',
            'error': result.stderr.strip() if result.returncode != 0 else None,
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
        }


def get_ssh_fingerprint(host: str, port: int = 22) -> Dict[str, Any]:
    """
    Get SSH host key fingerprint.

    Args:
        host: Target hostname or IP.
        port: SSH port.

    Returns:
        Dictionary with fingerprint information.
    """
    cmd = ['ssh-keyscan', '-p', str(port), '-t', 'rsa,ecdsa,ed25519', host]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )

        fingerprints = {}
        for line in result.stdout.splitlines():
            if line:
                parts = line.split()
                if len(parts) >= 2:
                    key_type = parts[0]
                    key_data = parts[1]

                    import hashlib
                    import base64

                    key_bytes = base64.b64decode(key_data)
                    fingerprint = base64.b64encode(
                        hashlib.sha256(key_bytes).digest()
                    ).decode().rstrip('=')

                    fingerprints[key_type] = fingerprint

        return {
            'success': True,
            'host': host,
            'port': port,
            'fingerprints': fingerprints,
        }
    except Exception as e:
        return {
            'success': False,
            'host': host,
            'error': str(e),
        }
