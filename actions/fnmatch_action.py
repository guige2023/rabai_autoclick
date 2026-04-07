"""
Glob and fnmatch pattern operations for file/path matching.
"""

from __future__ import annotations

import fnmatch
import glob as glob_module
import os
import re
from pathlib import Path, PosixPath, WindowsPath
from typing import Callable, Iterator, List, Optional, Union


def fnmatch_filter(
    names: List[str],
    pattern: str,
    mode: str = "match",
) -> List[str]:
    """
    Filter names using fnmatch pattern.
    
    Args:
        names: List of names to filter
        pattern: fnmatch pattern (e.g., "*.txt", "test_*.py")
        mode: "match" (return matching) or "exclude" (return non-matching)
    
    Returns:
        Filtered list of names
    
    Raises:
        ValueError: If mode is not "match" or "exclude"
    
    Example:
        >>> fnmatch_filter(['file1.txt', 'file2.py', 'file3.txt'], '*.txt')
        ['file1.txt', 'file3.txt']
    """
    if mode not in ("match", "exclude"):
        raise ValueError(f"Mode must be 'match' or 'exclude', got {mode}")
    
    if mode == "match":
        return [n for n in names if fnmatch.fnmatch(n, pattern)]
    return [n for n in names if not fnmatch.fnmatch(n, pattern)]


def fnmatch_case_insensitive(
    name: str,
    pattern: str,
) -> bool:
    """
    Match pattern case-insensitively.
    
    Args:
        name: String to match
        pattern: fnmatch pattern
    
    Returns:
        True if name matches pattern (case-insensitive)
    
    Example:
        >>> fnmatch_case_insensitive('FILE.TXT', '*.txt')
        True
    """
    return fnmatch.fnmatch(name.lower(), pattern.lower())


def glob_recursive(
    pattern: str,
    root_dir: Optional[str] = None,
    include_hidden: bool = False,
) -> List[str]:
    """
    Recursive glob matching with ** pattern.
    
    Args:
        pattern: Glob pattern (use ** for recursive)
        root_dir: Root directory to search from
        include_hidden: Whether to include hidden files
    
    Returns:
        List of matching file paths
    
    Example:
        >>> glob_recursive('**/*.py', '/path/to/project')
        ['file1.py', 'subdir/file2.py']
    """
    if root_dir:
        full_pattern = os.path.join(root_dir, pattern)
    else:
        full_pattern = pattern
    
    results = glob_module.glob(full_pattern, recursive=True)
    
    if not include_hidden:
        results = [r for r in results if not os.path.basename(r).startswith('.')]
    
    return results


def glob_iterative(
    patterns: List[str],
    root_dir: Optional[str] = None,
) -> Iterator[str]:
    """
    Iterate over multiple glob patterns.
    
    Args:
        patterns: List of glob patterns
        root_dir: Root directory to search from
    
    Yields:
        Matching file paths from all patterns
    
    Example:
        >>> list(glob_iterative(['*.py', '*.txt']))
        ['file1.py', 'file2.txt']
    """
    for pattern in patterns:
        if root_dir:
            full_pattern = os.path.join(root_dir, pattern)
        else:
            full_pattern = pattern
        yield from glob_module.glob(full_pattern)


def filter_by_extension(
    paths: List[str],
    extensions: List[str],
    include: bool = True,
) -> List[str]:
    """
    Filter paths by file extensions.
    
    Args:
        paths: List of file paths
        extensions: List of extensions (e.g., ['.py', '.txt'])
        include: If True, keep matching; if False, exclude matching
    
    Returns:
        Filtered list of paths
    
    Example:
        >>> filter_by_extension(['a.py', 'b.txt', 'c.py'], ['.py'])
        ['a.py', 'c.py']
    """
    extensions = [ext.lower() if ext.startswith('.') else f'.{ext.lower()}' for ext in extensions]
    results = []
    for path in paths:
        ext = os.path.splitext(path)[1].lower()
        if include and ext in extensions:
            results.append(path)
        elif not include and ext not in extensions:
            results.append(path)
    return results


def match_pattern(
    string: str,
    pattern: str,
    regex: bool = False,
) -> bool:
    """
    Match a string against a pattern.
    
    Args:
        string: String to match
        pattern: Pattern to match against (fnmatch or regex)
        regex: If True, treat pattern as regex; otherwise fnmatch
    
    Returns:
        True if string matches pattern
    
    Example:
        >>> match_pattern('test_file.py', 'test_*.py')
        True
    """
    if regex:
        try:
            return bool(re.match(pattern, string))
        except re.error:
            return False
    return fnmatch.fnmatch(string, pattern)


def extract_pattern(
    string: str,
    pattern: str,
    group: int = 0,
    regex: bool = True,
) -> Optional[str]:
    """
    Extract a subgroup from a regex match.
    
    Args:
        string: String to search
        pattern: Regex pattern
        group: Group number to extract (0 = entire match)
        regex: If True, treat as regex; otherwise fnmatch
    
    Returns:
        Extracted match or None if no match
    
    Example:
        >>> extract_pattern('file_v1.0.txt', r'file_v(\d+\.\d+)', group=1)
        '1.0'
    """
    if not regex:
        return None
    
    match = re.search(pattern, string)
    if match:
        return match.group(group)
    return None


def list_directory_tree(
    root: str,
    pattern: str = "*",
    max_depth: Optional[int] = None,
    include_files: bool = True,
    include_dirs: bool = True,
) -> Iterator[tuple]:
    """
    Iterate over directory tree with glob filtering.
    
    Args:
        root: Root directory path
        pattern: Glob pattern to match files
        max_depth: Maximum directory depth (None for unlimited)
        include_files: Include files in output
        include_dirs: Include directories in output
    
    Yields:
        Tuples of (path, depth, is_dir)
    
    Example:
        >>> for path, depth, is_dir in list_directory_tree('/path'):
        ...     print('  ' * depth + os.path.basename(path))
    """
    root_depth = root.rstrip(os.sep).count(os.sep)
    
    for dirpath, dirnames, filenames in os.walk(root):
        current_depth = dirpath.count(os.sep) - root_depth
        
        if max_depth is not None and current_depth >= max_depth:
            dirnames[:] = []
            continue
        
        if include_dirs:
            yield (dirpath, current_depth, True)
        
        if include_files:
            matched = fnmatch.filter(filenames, pattern)
            for filename in matched:
                yield (os.path.join(dirpath, filename), current_depth + 1, False)


def wildcard_to_regex(pattern: str) -> str:
    """
    Convert fnmatch wildcard pattern to regex.
    
    Args:
        pattern: fnmatch pattern
    
    Returns:
        Equivalent regex pattern string
    
    Example:
        >>> wildcard_to_regex('*.py')
        '.*\\.py'
    """
    return fnmatch.translate(pattern)


def expand_braces(pattern: str) -> List[str]:
    """
    Expand brace expressions like {a,b,c}.
    
    Args:
        pattern: Pattern with optional brace expressions
    
    Returns:
        List of all expanded patterns
    
    Example:
        >>> expand_braces('file_{a,b,c}.txt')
        ['file_a.txt', 'file_b.txt', 'file_c.txt']
    """
    brace_pattern = re.compile(r'\{([^}]+)\}')
    match = brace_pattern.search(pattern)
    
    if not match:
        return [pattern]
    
    options = match.group(1).split(',')
    base = pattern[:match.start()]
    suffix = pattern[match.end():]
    
    results = []
    for opt in options:
        results.extend(expand_braces(base + opt + suffix))
    
    return results


def filter_paths(
    paths: List[str],
    include_patterns: Optional[List[str]] = None,
    exclude_patterns: Optional[List[str]] = None,
    case_sensitive: bool = False,
) -> List[str]:
    """
    Filter paths using include/exclude patterns.
    
    Args:
        paths: List of paths to filter
        include_patterns: List of patterns to include (None = all)
        exclude_patterns: List of patterns to exclude
        case_sensitive: Whether matching is case-sensitive
    
    Returns:
        Filtered list of paths
    
    Example:
        >>> filter_paths(['a.py', 'b.txt', 'c.py'], include=['*.py'], exclude=['test_*')
        ['a.py', 'c.py']
    """
    if include_patterns is None:
        include_patterns = ['*']
    
    matcher: Callable[[str, str], bool] = (
        lambda s, p: fnmatch.fnmatch(s, p) if case_sensitive else fnmatch_case_insensitive(s, p)
    )
    
    results = []
    for path in paths:
        basename = os.path.basename(path)
        
        if not any(matcher(basename, p) for p in include_patterns):
            continue
        
        if exclude_patterns and any(matcher(basename, p) for p in exclude_patterns):
            continue
        
        results.append(path)
    
    return results


def safe_glob(
    pattern: str,
    root_dir: Optional[str] = None,
    recursive: bool = False,
) -> List[str]:
    """
    Glob with error handling and validation.
    
    Args:
        pattern: Glob pattern
        root_dir: Root directory
        recursive: Enable recursive matching with **
    
    Returns:
        List of matching paths (empty list on error)
    
    Example:
        >>> safe_glob('*.txt', '/path/to/dir')
        ['file1.txt']
    """
    try:
        if root_dir:
            full_pattern = os.path.join(root_dir, pattern)
        else:
            full_pattern = pattern
        
        return glob_module.glob(full_pattern, recursive=recursive)
    except (OSError, ValueError):
        return []


def path_match(
    path: str,
    pattern: str,
    separator: str = os.sep,
) -> bool:
    """
    Match path against pattern, handling separators.
    
    Args:
        path: Path to match
        pattern: Pattern to match against
        separator: Path separator (os.sep or '/')
    
    Returns:
        True if path matches pattern
    
    Example:
        >>> path_match('dir/file.txt', 'dir/*.txt')
        True
    """
    if separator != '/':
        path = path.replace(separator, '/')
        pattern = pattern.replace(separator, '/')
    
    return fnmatch.fnmatch(path, pattern)


def batch_glob(
    patterns: List[str],
    root_dir: Optional[str] = None,
) -> dict:
    """
    Execute multiple glob patterns and return results grouped by pattern.
    
    Args:
        patterns: List of glob patterns
        root_dir: Root directory
    
    Returns:
        Dict mapping pattern to list of matching paths
    
    Example:
        >>> results = batch_glob(['*.py', '*.txt'])
        >>> results['*.py']
        ['file1.py', 'file2.py']
    """
    results = {}
    for pattern in patterns:
        if root_dir:
            full_pattern = os.path.join(root_dir, pattern)
        else:
            full_pattern = pattern
        results[pattern] = glob_module.glob(full_pattern)
    return results
