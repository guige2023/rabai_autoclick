"""Path utilities for RabAI AutoClick.

Provides:
- Path helpers
- Directory management
- File type detection
"""

import os
import shutil
import tempfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, List, Optional, Union


class FileType(Enum):
    """File type categories."""
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    DOCUMENT = "document"
    CONFIG = "config"
    CODE = "code"
    ARCHIVE = "archive"
    UNKNOWN = "unknown"


IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.bmp', '.webp', '.tiff', '.svg'}
VIDEO_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm'}
AUDIO_EXTENSIONS = {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.wma'}
DOCUMENT_EXTENSIONS = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.txt', '.md'}
CONFIG_EXTENSIONS = {'.json', '.yaml', '.yml', '.toml', '.ini', '.cfg', '.conf'}
CODE_EXTENSIONS = {'.py', '.js', '.ts', '.java', '.c', '.cpp', '.h', '.go', '.rs', '.rb', '.php'}
ARCHIVE_EXTENSIONS = {'.zip', '.tar', '.gz', '.bz2', '.7z', '.rar'}


def get_file_type(path: Union[str, Path]) -> FileType:
    """Determine file type from extension.

    Args:
        path: File path.

    Returns:
        FileType enum value.
    """
    path = Path(path)
    ext = path.suffix.lower()

    if ext in IMAGE_EXTENSIONS:
        return FileType.IMAGE
    if ext in VIDEO_EXTENSIONS:
        return FileType.VIDEO
    if ext in AUDIO_EXTENSIONS:
        return FileType.AUDIO
    if ext in DOCUMENT_EXTENSIONS:
        return FileType.DOCUMENT
    if ext in CONFIG_EXTENSIONS:
        return FileType.CONFIG
    if ext in CODE_EXTENSIONS:
        return FileType.CODE
    if ext in ARCHIVE_EXTENSIONS:
        return FileType.ARCHIVE

    return FileType.UNKNOWN


def ensure_dir(path: Union[str, Path], mode: int = 0o755) -> Path:
    """Ensure directory exists.

    Args:
        path: Directory path.
        mode: Directory permissions.

    Returns:
        Path object.
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True, mode=mode)
    return path


def ensure_parent_dir(path: Union[str, Path], mode: int = 0o755) -> Path:
    """Ensure parent directory of a file exists.

    Args:
        path: File path.
        mode: Directory permissions.

    Returns:
        Path object of parent directory.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=mode)
    return path


def safe_filename(filename: str, replacement: str = '_') -> str:
    """Make filename safe by removing invalid characters.

    Args:
        filename: Original filename.
        replacement: Character to replace invalid chars with.

    Returns:
        Safe filename.
    """
    import re
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', replacement, filename)
    safe = safe.strip('. ')
    if not safe:
        safe = 'unnamed'
    return safe[:255]  # Most filesystems limit to 255 chars


def copy_file(src: Union[str, Path], dst: Union[str, Path], overwrite: bool = False) -> bool:
    """Copy file with error handling.

    Args:
        src: Source file path.
        dst: Destination file path.
        overwrite: If True, overwrite existing file.

    Returns:
        True on success, False on error.
    """
    src = Path(src)
    dst = Path(dst)

    if not src.exists():
        return False

    if dst.exists() and not overwrite:
        return False

    try:
        ensure_parent_dir(dst)
        shutil.copy2(src, dst)
        return True
    except Exception:
        return False


def move_file(src: Union[str, Path], dst: Union[str, Path], overwrite: bool = False) -> bool:
    """Move file with error handling.

    Args:
        src: Source file path.
        dst: Destination file path.
        overwrite: If True, overwrite existing file.

    Returns:
        True on success, False on error.
    """
    src = Path(src)
    dst = Path(dst)

    if not src.exists():
        return False

    if dst.exists():
        if not overwrite:
            return False
        try:
            dst.unlink()
        except Exception:
            return False

    try:
        ensure_parent_dir(dst)
        shutil.move(str(src), str(dst))
        return True
    except Exception:
        return False


def delete_file(path: Union[str, Path]) -> bool:
    """Delete file with error handling.

    Args:
        path: File path to delete.

    Returns:
        True on success, False on error.
    """
    path = Path(path)
    try:
        if path.exists():
            path.unlink()
        return True
    except Exception:
        return False


def get_size(path: Union[str, Path]) -> int:
    """Get file or directory size in bytes.

    Args:
        path: File or directory path.

    Returns:
        Size in bytes, 0 on error.
    """
    path = Path(path)
    try:
        if path.is_file():
            return path.stat().st_size
        if path.is_dir():
            total = 0
            for item in path.rglob('*'):
                if item.is_file():
                    total += item.stat().st_size
            return total
        return 0
    except Exception:
        return 0


def format_size(size_bytes: int) -> str:
    """Format byte size to human readable string.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Formatted string (e.g., "1.5 MB").
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def walk_dir(
    path: Union[str, Path],
    pattern: str = "*",
    recursive: bool = True,
    include_hidden: bool = False,
) -> List[Path]:
    """Walk directory and return matching paths.

    Args:
        path: Directory path.
        pattern: Glob pattern to match.
        recursive: If True, search recursively.
        include_hidden: If True, include hidden files.

    Returns:
        List of matching Path objects.
    """
    path = Path(path)
    if not path.is_dir():
        return []

    results = []
    if recursive:
        for item in path.rglob(pattern):
            if item.is_file():
                if include_hidden or not _is_hidden(item):
                    results.append(item)
    else:
        for item in path.glob(pattern):
            if item.is_file():
                if include_hidden or not _is_hidden(item):
                    results.append(item)

    return sorted(results)


def _is_hidden(path: Path) -> bool:
    """Check if path is hidden."""
    return any(part.startswith('.') for part in path.parts)


def clean_dir(
    path: Union[str, Path],
    pattern: str = "*",
    recursive: bool = True,
    dry_run: bool = True,
) -> List[Path]:
    """Clean directory by deleting matching files.

    Args:
        path: Directory path.
        pattern: Glob pattern to match.
        recursive: If True, search recursively.
        dry_run: If True, don't actually delete.

    Returns:
        List of deleted (or would-be deleted) paths.
    """
    path = Path(path)
    files = walk_dir(path, pattern, recursive)

    deleted = []
    for file in files:
        if dry_run:
            deleted.append(file)
        elif delete_file(file):
            deleted.append(file)

    return deleted


@dataclass
class TempDir:
    """Temporary directory context manager."""
    path: Path
    _created: bool = False

    @classmethod
    def create(cls, prefix: str = "rabai_") -> 'TempDir':
        """Create a temporary directory.

        Args:
            prefix: Directory name prefix.

        Returns:
            TempDir instance.
        """
        path = Path(tempfile.mkdtemp(prefix=prefix))
        return cls(path, _created=True)

    def __enter__(self) -> Path:
        """Enter context manager."""
        return self.path

    def __exit__(self, *args: Any) -> None:
        """Exit context manager and cleanup."""
        if self._created and self.path.exists():
            shutil.rmtree(self.path, ignore_errors=True)

    def cleanup(self) -> None:
        """Manually cleanup directory."""
        if self._created and self.path.exists():
            shutil.rmtree(self.path, ignore_errors=True)


def find_files_by_type(
    directory: Union[str, Path],
    file_type: FileType,
    recursive: bool = True,
) -> List[Path]:
    """Find files of a specific type.

    Args:
        directory: Directory to search.
        file_type: Type of files to find.
        recursive: If True, search recursively.

    Returns:
        List of matching file paths.
    """
    extensions = {
        FileType.IMAGE: IMAGE_EXTENSIONS,
        FileType.VIDEO: VIDEO_EXTENSIONS,
        FileType.AUDIO: AUDIO_EXTENSIONS,
        FileType.DOCUMENT: DOCUMENT_EXTENSIONS,
        FileType.CONFIG: CONFIG_EXTENSIONS,
        FileType.CODE: CODE_EXTENSIONS,
        FileType.ARCHIVE: ARCHIVE_EXTENSIONS,
    }.get(file_type, set())

    if not extensions:
        return []

    directory = Path(directory)
    results = []

    for ext in extensions:
        pattern = f"*{ext}"
        results.extend(walk_dir(directory, pattern, recursive))

    return sorted(set(results))