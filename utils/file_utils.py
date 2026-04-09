"""
File operation utilities for UI automation workflows.

Provides functions for file watching, temporary files,
path operations, and file comparison.
"""

from __future__ import annotations

import os
import sys
import time
import hashlib
import tempfile
import shutil
import threading
from pathlib import Path
from typing import List, Optional, Callable, Dict, Any, Iterator
from dataclasses import dataclass
from enum import Enum, auto


class FileEvent(Enum):
    """File system event types."""
    CREATED = auto()
    MODIFIED = auto()
    DELETED = auto()
    MOVED = auto()
    ACCESSED = auto()


@dataclass
class FileChange:
    """Represents a file system change."""
    event: FileEvent
    path: str
    timestamp: float
    old_path: Optional[str] = None


class FileWatcher:
    """Watch files or directories for changes."""
    
    def __init__(
        self,
        paths: List[str],
        recursive: bool = True,
        poll_interval: float = 0.5,
    ) -> None:
        """Initialize file watcher.
        
        Args:
            paths: Paths to watch
            recursive: Watch subdirectories
            poll_interval: Seconds between checks
        """
        self.paths = [os.path.abspath(p) for p in paths]
        self.recursive = recursive
        self.poll_interval = poll_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: List[Callable[[FileChange], None]] = []
        self._last_state: Dict[str, float] = {}
        self._lock = threading.Lock()
    
    def start(self) -> None:
        """Start watching for file changes."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """Stop watching."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
    
    def on_change(self, callback: Callable[[FileChange], None]) -> None:
        """Register callback for file changes.
        
        Args:
            callback: Function to call on change
        """
        self._callbacks.append(callback)
    
    def _watch_loop(self) -> None:
        """Main watch loop."""
        self._scan_initial()
        
        while self._running:
            try:
                changes = self._check_changes()
                for change in changes:
                    for cb in self._callbacks:
                        cb(change)
            except Exception:
                pass
            time.sleep(self.poll_interval)
    
    def _scan_initial(self) -> None:
        """Initial scan to establish baseline."""
        for path in self.paths:
            self._update_state(path)
    
    def _update_state(self, root: str) -> None:
        """Update file state for path."""
        if not os.path.exists(root):
            return
        
        if os.path.isfile(root):
            try:
                self._last_state[root] = os.path.getmtime(root)
            except OSError:
                pass
        elif os.path.isdir(root):
            for dirpath, dirnames, filenames in os.walk(root):
                if not self.recursive:
                    dirnames.clear()
                for fname in filenames:
                    fpath = os.path.join(dirpath, fname)
                    try:
                        self._last_state[fpath] = os.path.getmtime(fpath)
                    except OSError:
                        pass
    
    def _check_changes(self) -> List[FileChange]:
        """Check for file changes since last scan."""
        changes: List[FileChange] = []
        current_state: Dict[str, float] = {}
        now = time.time()
        
        for path in self.paths:
            if not os.path.exists(path):
                continue
            
            if os.path.isfile(path):
                self._check_file(path, current_state, changes, now)
            elif os.path.isdir(path):
                self._check_directory(path, current_state, changes, now)
        
        with self._lock:
            self._last_state = current_state
        
        return changes
    
    def _check_file(
        self,
        fpath: str,
        state: Dict[str, float],
        changes: List[FileChange],
        now: float,
    ) -> None:
        """Check single file for changes."""
        try:
            mtime = os.path.getmtime(fpath)
        except OSError:
            if fpath in self._last_state:
                changes.append(FileChange(FileEvent.DELETED, fpath, now))
            return
        
        state[fpath] = mtime
        
        if fpath not in self._last_state:
            changes.append(FileChange(FileEvent.CREATED, fpath, now))
        elif self._last_state[fpath] != mtime:
            changes.append(FileChange(FileEvent.MODIFIED, fpath, now))
    
    def _check_directory(
        self,
        dpath: str,
        state: Dict[str, float],
        changes: List[FileChange],
        now: float,
    ) -> None:
        """Check directory for changes."""
        try:
            for dirpath, dirnames, filenames in os.walk(dpath):
                if not self.recursive:
                    dirnames.clear()
                
                for fname in filenames:
                    fpath = os.path.join(dirpath, fname)
                    self._check_file(fpath, state, changes, now)
        except PermissionError:
            pass


def get_file_hash(path: str, algorithm: str = 'sha256') -> str:
    """Compute hash of file contents.
    
    Args:
        path: Path to file
        algorithm: Hash algorithm (md5, sha1, sha256, sha512)
    
    Returns:
        Hex digest of file hash
    """
    hasher = hashlib.new(algorithm)
    
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            hasher.update(chunk)
    
    return hasher.hexdigest()


def compare_files(path1: str, path2: str) -> bool:
    """Compare two files for equality.
    
    Args:
        path1: First file path
        path2: Second file path
    
    Returns:
        True if files have identical content
    """
    if os.path.getsize(path1) != os.path.getsize(path2):
        return False
    
    with open(path1, 'rb') as f1, open(path2, 'rb') as f2:
        while True:
            chunk1 = f1.read(65536)
            chunk2 = f2.read(65536)
            
            if chunk1 != chunk2:
                return False
            
            if not chunk1:
                return True


def create_temp_file(
    suffix: str = '',
    prefix: str = 'tmp',
    dir: Optional[str] = None,
    content: Optional[bytes] = None,
) -> str:
    """Create a temporary file.
    
    Args:
        suffix: File suffix
        prefix: File prefix
        dir: Directory to create in
        content: Initial content to write
    
    Returns:
        Path to created file
    """
    fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=dir)
    
    if content is not None:
        os.write(fd, content)
    
    os.close(fd)
    return path


def create_temp_dir(
    prefix: str = 'tmp',
    dir: Optional[str] = None,
) -> str:
    """Create a temporary directory.
    
    Args:
        prefix: Directory prefix
        dir: Parent directory
    
    Returns:
        Path to created directory
    """
    return tempfile.mkdtemp(prefix=prefix, dir=dir)


def safe_remove(path: str) -> bool:
    """Safely remove file or directory.
    
    Args:
        path: Path to remove
    
    Returns:
        True if removed, False otherwise
    """
    try:
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
        else:
            return False
        return True
    except (OSError, shutil.Error):
        return False


def ensure_dir(path: str) -> str:
    """Ensure directory exists, creating if needed.
    
    Args:
        path: Directory path
    
    Returns:
        Absolute path to directory
    """
    os.makedirs(path, exist_ok=True)
    return os.path.abspath(path)


def copy_with_metadata(src: str, dst: str) -> bool:
    """Copy file preserving metadata.
    
    Args:
        src: Source path
        dst: Destination path
    
    Returns:
        True if successful
    """
    try:
        shutil.copy2(src, dst)
        return True
    except (OSError, shutil.Error):
        return False


def find_files(
    root: str,
    pattern: str = '*',
    recursive: bool = True,
    type: str = 'f',
) -> List[str]:
    """Find files matching pattern.
    
    Args:
        root: Root directory
        pattern: Glob pattern
        recursive: Search subdirectories
        type: 'f' for files, 'd' for directories, 'a' for all
    
    Returns:
        List of matching paths
    """
    path = Path(root)
    
    if recursive:
        if type == 'f':
            return [str(p) for p in path.rglob(pattern) if p.is_file()]
        elif type == 'd':
            return [str(p) for p in path.rglob(pattern) if p.is_dir()]
        else:
            return [str(p) for p in path.rglob(pattern)]
    else:
        if type == 'f':
            return [str(p) for p in path.glob(pattern) if p.is_file()]
        elif type == 'd':
            return [str(p) for p in path.glob(pattern) if p.is_dir()]
        else:
            return [str(p) for p in path.glob(pattern)]


def get_file_age(path: str) -> float:
    """Get file age in seconds.
    
    Args:
        path: File path
    
    Returns:
        Age in seconds since last modification
    """
    return time.time() - os.path.getmtime(path)


def is_file_locked(path: str) -> bool:
    """Check if file is locked by another process.
    
    Args:
        path: File path
    
    Returns:
        True if file appears locked
    """
    if not os.path.exists(path):
        return False
    
    try:
        with open(path, 'a'):
            return False
    except IOError:
        return True


def read_chunks(path: str, chunk_size: int = 65536) -> Iterator[bytes]:
    """Read file in chunks.
    
    Args:
        path: File path
        chunk_size: Size of each chunk
    
    Yields:
        Chunks of bytes
    """
    with open(path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk


def atomic_write(path: str, content: bytes, mode: int = 0o644) -> bool:
    """Atomically write content to file.
    
    Args:
        path: Target file path
        content: Bytes to write
        mode: File permissions
    
    Returns:
        True if successful
    """
    dir_path = os.path.dirname(path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    
    tmp_path = path + '.tmp'
    
    try:
        with open(tmp_path, 'wb') as f:
            f.write(content)
        os.chmod(tmp_path, mode)
        os.replace(tmp_path, path)
        return True
    except (OSError, IOError):
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False


class FileLock:
    """Context manager for file locking."""
    
    def __init__(self, path: str, timeout: float = 10.0) -> None:
        """Initialize file lock.
        
        Args:
            path: Lock file path
            timeout: Maximum seconds to wait
        """
        self.lock_path = path + '.lock'
        self.timeout = timeout
        self._acquired = False
    
    def acquire(self) -> bool:
        """Acquire the lock."""
        start = time.time()
        
        while time.time() - start < self.timeout:
            try:
                os.makedirs(self.lock_path, exist_ok=False)
                self._acquired = True
                return True
            except FileExistsError:
                time.sleep(0.01)
        
        return False
    
    def release(self) -> None:
        """Release the lock."""
        if self._acquired:
            try:
                os.rmdir(self.lock_path)
            except OSError:
                pass
            self._acquired = False
    
    def __enter__(self) -> 'FileLock':
        if not self.acquire():
            raise TimeoutError(f"Could not acquire lock: {self.lock_path}")
        return self
    
    def __exit__(self, *args: Any) -> None:
        self.release()
