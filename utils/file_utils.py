"""File utilities for RabAI AutoClick.

Provides:
- File operations and helpers
- Path manipulation
- File reading and writing
"""

import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union


def ensure_dir(path: Union[str, Path]) -> Path:
    """Ensure directory exists.

    Args:
        path: Directory path.

    Returns:
        Path object.
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_parent_dir(file_path: Union[str, Path]) -> Path:
    """Ensure parent directory of file exists.

    Args:
        file_path: File path.

    Returns:
        Path object.
    """
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def read_text(file_path: Union[str, Path], encoding: str = "utf-8") -> str:
    """Read text from file.

    Args:
        file_path: File path.
        encoding: Text encoding.

    Returns:
        File contents as string.
    """
    with open(file_path, "r", encoding=encoding) as f:
        return f.read()


def write_text(
    file_path: Union[str, Path],
    content: str,
    encoding: str = "utf-8",
) -> None:
    """Write text to file.

    Args:
        file_path: File path.
        content: Content to write.
        encoding: Text encoding.
    """
    ensure_parent_dir(file_path)
    with open(file_path, "w", encoding=encoding) as f:
        f.write(content)


def read_bytes(file_path: Union[str, Path]) -> bytes:
    """Read bytes from file.

    Args:
        file_path: File path.

    Returns:
        File contents as bytes.
    """
    with open(file_path, "rb") as f:
        return f.read()


def write_bytes(file_path: Union[str, Path], content: bytes) -> None:
    """Write bytes to file.

    Args:
        file_path: File path.
        content: Content to write.
    """
    ensure_parent_dir(file_path)
    with open(file_path, "wb") as f:
        f.write(content)


def read_json(file_path: Union[str, Path]) -> Any:
    """Read JSON from file.

    Args:
        file_path: File path.

    Returns:
        Parsed JSON data.
    """
    import json
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(
    file_path: Union[str, Path],
    data: Any,
    indent: int = 2,
) -> None:
    """Write JSON to file.

    Args:
        file_path: File path.
        data: Data to write.
        indent: JSON indentation.
    """
    import json
    ensure_parent_dir(file_path)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)


def copy_file(src: Union[str, Path], dst: Union[str, Path]) -> None:
    """Copy file.

    Args:
        src: Source path.
        dst: Destination path.
    """
    ensure_parent_dir(dst)
    shutil.copy2(src, dst)


def move_file(src: Union[str, Path], dst: Union[str, Path]) -> None:
    """Move file.

    Args:
        src: Source path.
        dst: Destination path.
    """
    ensure_parent_dir(dst)
    shutil.move(src, dst)


def delete_file(file_path: Union[str, Path]) -> None:
    """Delete file if exists.

    Args:
        file_path: File path.
    """
    path = Path(file_path)
    if path.exists():
        path.unlink()


def delete_dir(dir_path: Union[str, Path], recursive: bool = False) -> None:
    """Delete directory.

    Args:
        dir_path: Directory path.
        recursive: If True, delete recursively.
    """
    path = Path(dir_path)
    if not path.exists():
        return
    if recursive:
        shutil.rmtree(path)
    else:
        path.rmdir()


def file_exists(file_path: Union[str, Path]) -> bool:
    """Check if file exists.

    Args:
        file_path: File path.

    Returns:
        True if file exists.
    """
    return Path(file_path).is_file()


def dir_exists(dir_path: Union[str, Path]) -> bool:
    """Check if directory exists.

    Args:
        dir_path: Directory path.

    Returns:
        True if directory exists.
    """
    return Path(dir_path).is_dir()


def get_size(file_path: Union[str, Path]) -> int:
    """Get file size in bytes.

    Args:
        file_path: File path.

    Returns:
        File size.
    """
    return Path(file_path).stat().st_size


def get_extension(file_path: Union[str, Path]) -> str:
    """Get file extension.

    Args:
        file_path: File path.

    Returns:
        Extension with dot (e.g., ".txt").
    """
    return Path(file_path).suffix


def get_name(file_path: Union[str, Path]) -> str:
    """Get file name without extension.

    Args:
        file_path: File path.

    Returns:
        File name without extension.
    """
    return Path(file_path).stem


def get_basename(file_path: Union[str, Path]) -> str:
    """Get file basename (filename).

    Args:
        file_path: File path.

    Returns:
        File basename.
    """
    return Path(file_path).name


def list_files(
    dir_path: Union[str, Path],
    pattern: str = "*",
    recursive: bool = False,
) -> List[Path]:
    """List files in directory.

    Args:
        dir_path: Directory path.
        pattern: Glob pattern.
        recursive: If True, search recursively.

    Returns:
        List of file paths.
    """
    path = Path(dir_path)
    if recursive:
        return list(path.rglob(pattern))
    return list(path.glob(pattern))


def list_dirs(dir_path: Union[str, Path], recursive: bool = False) -> List[Path]:
    """List subdirectories.

    Args:
        dir_path: Directory path.
        recursive: If True, search recursively.

    Returns:
        List of directory paths.
    """
    path = Path(dir_path)
    if recursive:
        return [p for p in path.rglob("*") if p.is_dir()]
    return [p for p in path.glob("*") if p.is_dir()]


def walk_dir(
    dir_path: Union[str, Path],
    callback: Callable[[Path], Any],
    recursive: bool = True,
) -> None:
    """Walk directory and call callback for each file.

    Args:
        dir_path: Directory path.
        callback: Function to call for each file.
        recursive: If True, walk recursively.
    """
    path = Path(dir_path)
    if recursive:
        for file_path in path.rglob("*"):
            if file_path.is_file():
                callback(file_path)
    else:
        for file_path in path.glob("*"):
            if file_path.is_file():
                callback(file_path)


def temp_file_name(suffix: str = "", prefix: str = "tmp") -> str:
    """Generate temporary file name.

    Args:
        suffix: File suffix.
        prefix: File prefix.

    Returns:
        Temporary file path.
    """
    fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix)
    os.close(fd)
    return path


def temp_dir_name(prefix: str = "tmp") -> str:
    """Generate temporary directory name.

    Args:
        prefix: Directory prefix.

    Returns:
        Temporary directory path.
    """
    return tempfile.mkdtemp(prefix=prefix)


def is_empty_dir(dir_path: Union[str, Path]) -> bool:
    """Check if directory is empty.

    Args:
        dir_path: Directory path.

    Returns:
        True if directory is empty.
    """
    path = Path(dir_path)
    if not path.is_dir():
        return False
    return not any(path.iterdir())


def get_relative_path(path: Union[str, Path], base: Union[str, Path]) -> Path:
    """Get relative path from base.

    Args:
        path: Target path.
        base: Base path.

    Returns:
        Relative path.
    """
    return Path(path).relative_to(base)


def join_paths(*paths: Union[str, Path]) -> Path:
    """Join path components.

    Args:
        *paths: Path components to join.

    Returns:
        Joined path.
    """
    return Path(*paths)


def normalize_path(path: Union[str, Path]) -> Path:
    """Normalize path.

    Args:
        path: Path to normalize.

    Returns:
        Normalized path.
    """
    return Path(path).resolve()


def is_absolute(path: Union[str, Path]) -> bool:
    """Check if path is absolute.

    Args:
        path: Path to check.

    Returns:
        True if absolute.
    """
    return Path(path).is_absolute()


def make_absolute(path: Union[str, Path], base: Union[str, Path] = None) -> Path:
    """Make path absolute.

    Args:
        path: Path to make absolute.
        base: Base path if relative.

    Returns:
        Absolute path.
    """
    path = Path(path)
    if path.is_absolute():
        return path
    if base:
        return (Path(base) / path).resolve()
    return path.resolve()
