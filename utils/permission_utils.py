"""Permission utilities for RabAI AutoClick.

Provides:
- File permission checking and modification
- User/group resolution
- Access control helpers
"""

import os
import stat
from typing import (
    Callable,
    List,
    Optional,
    Union,
)


def get_mode(filepath: str) -> int:
    """Get file mode/permissions.

    Args:
        filepath: Path to file.

    Returns:
        Permission bits (octal).
    """
    return stat.S_IMODE(os.stat(filepath).st_mode)


def get_mode_str(filepath: str) -> str:
    """Get file permissions as string (e.g., 'rwxr-xr-x').

    Args:
        filepath: Path to file.

    Returns:
        Permission string.
    """
    mode = get_mode(filepath)
    chars = []
    for shift in (6, 3, 0):
        bits = (mode >> shift) & 0o7
        chars.append("r" if bits & 0o4 else "-")
        chars.append("w" if bits & 0o2 else "-")
        chars.append("x" if bits & 0o1 else "-")
    return "".join(chars)


def get_mode_octal(filepath: str) -> str:
    """Get file permissions as octal string.

    Args:
        filepath: Path to file.

    Returns:
        Octal permission string (e.g., '755').
    """
    return oct(get_mode(filepath))


def chmod(filepath: str, mode: Union[int, str]) -> None:
    """Set file permissions.

    Args:
        filepath: Path to file.
        mode: Permission mode (octal int or string like '755').
    """
    if isinstance(mode, str):
        mode = int(mode, 8)
    os.chmod(filepath, mode)


def chmod_plus_x(filepath: str) -> None:
    """Add executable permission.

    Args:
        filepath: Path to file.
    """
    current = get_mode(filepath)
    os.chmod(filepath, current | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def chmod_minus_x(filepath: str) -> None:
    """Remove executable permission.

    Args:
        filepath: Path to file.
    """
    current = get_mode(filepath)
    os.chmod(filepath, current & ~(stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))


def chmod_plus_r(filepath: str) -> None:
    """Add read permission for all.

    Args:
        filepath: Path to file.
    """
    current = get_mode(filepath)
    os.chmod(filepath, current | stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


def chmod_minus_r(filepath: str) -> None:
    """Remove read permission for all.

    Args:
        filepath: Path to file.
    """
    current = get_mode(filepath)
    os.chmod(filepath, current & ~(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH))


def chmod_plus_w(filepath: str) -> None:
    """Add write permission for all.

    Args:
        filepath: Path to file.
    """
    current = get_mode(filepath)
    os.chmod(filepath, current | stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)


def chmod_minus_w(filepath: str) -> None:
    """Remove write permission for all.

    Args:
        filepath: Path to file.
    """
    current = get_mode(filepath)
    os.chmod(filepath, current & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))


def is_readable(filepath: str) -> bool:
    """Check if file is readable by current user.

    Args:
        filepath: Path to file.

    Returns:
        True if readable.
    """
    return os.access(filepath, os.R_OK)


def is_writable(filepath: str) -> bool:
    """Check if file is writable by current user.

    Args:
        filepath: Path to file.

    Returns:
        True if writable.
    """
    return os.access(filepath, os.W_OK)


def is_executable(filepath: str) -> bool:
    """Check if file is executable by current user.

    Args:
        filepath: Path to file.

    Returns:
        True if executable.
    """
    return os.access(filepath, os.X_OK)


def is_owned_by_current_user(filepath: str) -> bool:
    """Check if file is owned by current user.

    Args:
        filepath: Path to file.

    Returns:
        True if owned by current user.
    """
    import pwd
    try:
        stat_info = os.stat(filepath)
        return stat_info.st_uid == os.getuid()
    except OSError:
        return False


def get_owner(filepath: str) -> str:
    """Get file owner username.

    Args:
        filepath: Path to file.

    Returns:
        Owner username.
    """
    import pwd
    try:
        stat_info = os.stat(filepath)
        return pwd.getpwuid(stat_info.st_uid).pw_name
    except (OSError, KeyError):
        return str(os.stat(filepath).st_uid)


def get_group(filepath: str) -> str:
    """Get file group name.

    Args:
        filepath: Path to file.

    Returns:
        Group name.
    """
    import grp
    try:
        stat_info = os.stat(filepath)
        return grp.getgrgid(stat_info.st_gid).gr_name
    except (OSError, KeyError):
        return str(os.stat(filepath).st_gid)


def chmod_recursive(
    dirpath: str,
    mode: Union[int, str],
    *,
    files_only: bool = False,
    dirs_only: bool = False,
) -> int:
    """Recursively change permissions.

    Args:
        dirpath: Root directory path.
        mode: Permission mode.
        files_only: If True, only change files.
        dirs_only: If True, only change directories.

    Returns:
        Number of items changed.
    """
    if isinstance(mode, str):
        mode = int(mode, 8)

    count = 0
    for root, dirs, files in os.walk(dirpath):
        if not dirs_only:
            for filename in files:
                filepath = os.path.join(root, filename)
                try:
                    os.chmod(filepath, mode)
                    count += 1
                except OSError:
                    pass

        if not files_only:
            for dirname in dirs:
                dirpath_full = os.path.join(root, dirname)
                try:
                    os.chmod(dirpath_full, mode)
                    count += 1
                except OSError:
                    pass

    return count


def chmod_u+rwx(filepath: str) -> None:
    """Set u+rwx (owner read/write/execute).

    Args:
        filepath: Path to file.
    """
    current = get_mode(filepath)
    os.chmod(filepath, current | stat.S_IRWXU)


def chmod_a+rw(filepath: str) -> None:
    """Set a+rw (all read/write).

    Args:
        filepath: Path to file.
    """
    current = get_mode(filepath)
    os.chmod(filepath, current | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)


def parse_mode_str(mode_str: str) -> int:
    """Parse permission string like 'rwxr-xr-x' to octal int.

    Args:
        mode_str: Permission string.

    Returns:
        Octal mode integer.
    """
    if len(mode_str) != 9:
        raise ValueError(f"Invalid mode string: {mode_str}")

    mode = 0
    for i, char in enumerate(mode_str):
        shift = 8 - i
        if char == "r" and i % 3 == 0:
            mode |= 0o4 << shift
        elif char == "w" and i % 3 == 1:
            mode |= 0o2 << shift
        elif char == "x" and i % 3 == 2:
            mode |= 0o1 << shift

    return mode


def check_permission(
    filepath: str,
    permission: str,
) -> bool:
    """Check a specific permission.

    Args:
        filepath: Path to file.
        permission: Permission to check ('r', 'w', 'x' for user/group/other).

    Returns:
        True if permission is set.
    """
    mode = get_mode(filepath)
    perm_map = {
        "u_r": stat.S_IRUSR,
        "u_w": stat.S_IWUSR,
        "u_x": stat.S_IXUSR,
        "g_r": stat.S_IRGRP,
        "g_w": stat.S_IWGRP,
        "g_x": stat.S_IXGRP,
        "o_r": stat.S_IROTH,
        "o_w": stat.S_IWOTH,
        "o_x": stat.S_IXOTH,
    }
    bit = perm_map.get(permission)
    if bit is None:
        return False
    return bool(mode & bit)
