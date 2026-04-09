"""Path and file system utilities.

Provides path manipulation, file operations,
and directory management utilities.
"""

import os
import shutil
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union


def ensure_dir(path: Union[str, Path]) -> Path:
    """Ensure directory exists, create if needed.

    Example:
        ensure_dir("output/results")
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_dirs(paths: List[Union[str, Path]]) -> List[Path]:
    """Ensure multiple directories exist.

    Example:
        ensure_dirs(["output/a", "output/b", "cache"])
    """
    return [ensure_dir(p) for p in paths]


def get_size(path: Union[str, Path]) -> int:
    """Get file or directory size in bytes.

    Example:
        get_size("large_file.zip")
    """
    path = Path(path)
    if path.is_file():
        return path.stat().st_size
    elif path.is_dir():
        total = 0
        for item in path.rglob("*"):
            if item.is_file():
                total += item.stat().st_size
        return total
    return 0


def copy_file(src: Union[str, Path], dst: Union[str, Path]) -> Path:
    """Copy file to destination.

    Example:
        copy_file("input.txt", "output/copy.txt")
    """
    src = Path(src)
    dst = Path(dst)
    ensure_dir(dst.parent)
    shutil.copy2(src, dst)
    return dst


def move_file(src: Union[str, Path], dst: Union[str, Path]) -> Path:
    """Move file to destination.

    Example:
        move_file("temp.txt", "archive/old.txt")
    """
    src = Path(src)
    dst = Path(dst)
    ensure_dir(dst.parent)
    shutil.move(str(src), str(dst))
    return dst


def delete_file(path: Union[str, Path]) -> bool:
    """Delete file if exists.

    Example:
        delete_file("temp.txt")
    """
    path = Path(path)
    if path.exists():
        path.unlink()
        return True
    return False


def delete_dir(path: Union[str, Path], recursive: bool = False) -> bool:
    """Delete directory.

    Args:
        path: Directory path.
        recursive: Delete contents recursively.

    Example:
        delete_dir("temp", recursive=True)
    """
    path = Path(path)
    if not path.exists():
        return False

    if recursive:
        shutil.rmtree(path)
    else:
        path.rmdir()
    return True


def list_files(
    directory: Union[str, Path],
    pattern: str = "*",
    recursive: bool = False,
) -> List[Path]:
    """List files in directory.

    Example:
        list_files("src", "*.py")
        list_files("logs", "*.log", recursive=True)
    """
    directory = Path(directory)
    if recursive:
        return list(directory.rglob(pattern))
    return list(directory.glob(pattern))


def list_dirs(
    directory: Union[str, Path],
    recursive: bool = False,
) -> List[Path]:
    """List subdirectories.

    Example:
        list_dirs(".")
    """
    directory = Path(directory)
    if recursive:
        return [p for p in directory.rglob("*") if p.is_dir()]
    return [p for p in directory.glob("*") if p.is_dir()]


def find_files(
    directory: Union[str, Path],
    name_contains: Optional[str] = None,
    extension: Optional[str] = None,
    max_depth: Optional[int] = None,
) -> List[Path]:
    """Find files with criteria.

    Example:
        find_files(".", extension=".py")
        find_files(".", name_contains="test")
    """
    directory = Path(directory)
    results = []

    def _search(path: Path, depth: int = 0) -> None:
        if max_depth is not None and depth > max_depth:
            return

        try:
            for item in path.iterdir():
                if item.is_file():
                    match = True
                    if name_contains and name_contains not in item.name:
                        match = False
                    if extension and not item.name.endswith(extension):
                        match = False
                    if match:
                        results.append(item)
                elif item.is_dir():
                    _search(item, depth + 1)
        except PermissionError:
            pass

    _search(directory)
    return results


def read_file(path: Union[str, Path], encoding: str = "utf-8") -> str:
    """Read file contents as string.

    Example:
        content = read_file("data.txt")
    """
    with open(path, "r", encoding=encoding) as f:
        return f.read()


def write_file(
    path: Union[str, Path],
    content: str,
    encoding: str = "utf-8",
) -> None:
    """Write string content to file.

    Example:
        write_file("output.txt", "Hello World")
    """
    path = Path(path)
    ensure_dir(path.parent)
    with open(path, "w", encoding=encoding) as f:
        f.write(content)


def read_lines(
    path: Union[str, Path],
    encoding: str = "utf-8",
    strip: bool = True,
) -> List[str]:
    """Read file as list of lines.

    Example:
        lines = read_lines("input.txt")
    """
    with open(path, "r", encoding=encoding) as f:
        lines = f.readlines()
    if strip:
        return [line.strip() for line in lines]
    return lines


def write_lines(
    path: Union[str, Path],
    lines: List[str],
    newline: str = "\n",
) -> None:
    """Write list of lines to file.

    Example:
        write_lines("output.txt", ["line1", "line2"])
    """
    path = Path(path)
    ensure_dir(path.parent)
    with open(path, "w") as f:
        for line in lines:
            f.write(line + newline)


def join_paths(*parts: Union[str, Path]) -> Path:
    """Join path components.

    Example:
        join_paths("folder", "subfolder", "file.txt")
    """
    return Path(*parts)


def normalize_path(path: Union[str, Path]) -> Path:
    """Normalize and resolve path.

    Example:
        normalize_path("folder/../folder/./file.txt")
    """
    return Path(path).resolve()


def relative_path(
    path: Union[str, Path],
    start: Union[str, Path] = ".",
) -> Path:
    """Get relative path from start.

    Example:
        relative_path("/home/user/file.txt", "/home")
    """
    path = Path(path).resolve()
    start = Path(start).resolve()
    return path.relative_to(start)


def is_subpath(path: Union[str, Path], parent: Union[str, Path]) -> bool:
    """Check if path is under parent.

    Example:
        is_subpath("/home/user/file.txt", "/home/user")
    """
    try:
        path = Path(path).resolve()
        parent = Path(parent).resolve()
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def walk_dir(
    directory: Union[str, Path],
    callback: Callable[[Path], Optional[bool]],
) -> None:
    """Walk directory calling callback for each item.

    Example:
        walk_dir(".", lambda p: print(p) if p.is_file() else None)
    """
    directory = Path(directory)
    for item in directory.rglob("*"):
        result = callback(item)
        if result is False:
            break


def file_age(path: Union[str, Path]) -> float:
    """Get file age in seconds since last modification.

    Example:
        if file_age("cache.json") > 3600:
            refresh_cache()
    """
    import time
    path = Path(path)
    return time.time() - path.stat().st_mtime


def is_older_than(path: Union[str, Path], seconds: float) -> bool:
    """Check if file is older than specified seconds.

    Example:
        if is_older_than("cache.json", 3600):
            print("Cache is stale")
    """
    return file_age(path) > seconds


def file_hash(path: Union[str, Path], algorithm: str = "md5") -> str:
    """Calculate file hash.

    Example:
        hash = file_hash("large_file.zip", "sha256")
    """
    import hashlib
    path = Path(path)
    h = hashlib.new(algorithm)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def create_temp_file(
    suffix: str = "",
    prefix: str = "tmp",
    dir: Optional[Union[str, Path]] = None,
) -> Path:
    """Create temporary file.

    Example:
        temp = create_temp_file(".txt", "myapp")
    """
    import tempfile
    fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=dir)
    os.close(fd)
    return Path(path)


def create_temp_dir(
    prefix: str = "tmp",
    dir: Optional[Union[str, Path]] = None,
) -> Path:
    """Create temporary directory.

    Example:
        temp = create_temp_dir("work")
    """
    import tempfile
    return Path(tempfile.mkdtemp(prefix=prefix, dir=dir))
