"""
File system utilities - path manipulation, file operations, directory management, glob patterns.
"""
from typing import Any, Dict, List, Optional, Tuple
import os
import shutil
import glob as glob_module
import hashlib
import logging
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _calculate_hash(file_path: str, algorithm: str = "sha256") -> str:
    h = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def _copy_file(src: str, dst: str, overwrite: bool = False) -> bool:
    if not os.path.exists(src):
        return False
    if os.path.exists(dst) and not overwrite:
        return False
    shutil.copy2(src, dst)
    return True


def _walk_dir(path: str, max_depth: int = 3, current_depth: int = 0) -> List[Dict[str, Any]]:
    results = []
    if current_depth > max_depth:
        return results
    try:
        entries = list(os.scandir(path))
        for entry in entries:
            info = {
                "name": entry.name,
                "path": entry.path,
                "is_dir": entry.is_dir(),
                "is_file": entry.is_file(),
            }
            if entry.is_file():
                try:
                    stat = entry.stat()
                    info["size"] = stat.st_size
                    info["mtime"] = stat.st_mtime
                except OSError:
                    pass
            results.append(info)
            if entry.is_dir():
                results.extend(_walk_dir(entry.path, max_depth, current_depth + 1))
    except PermissionError:
        pass
    return results


class FileSystemAction(BaseAction):
    """File system operations.

    Provides path manipulation, file/directory operations, glob patterns, hashing.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "exists")
        path = params.get("path", "")

        try:
            if operation == "exists":
                return {"success": True, "exists": os.path.exists(path), "path": path}

            elif operation == "is_file":
                return {"success": True, "is_file": os.path.isfile(path), "path": path}

            elif operation == "is_dir":
                return {"success": True, "is_dir": os.path.isdir(path), "path": path}

            elif operation == "read":
                if not os.path.isfile(path):
                    return {"success": False, "error": "Not a file"}
                encoding = params.get("encoding", "utf-8")
                with open(path, "r", encoding=encoding, errors="replace") as f:
                    content = f.read()
                return {"success": True, "content": content, "size": len(content)}

            elif operation == "read_lines":
                if not os.path.isfile(path):
                    return {"success": False, "error": "Not a file"}
                encoding = params.get("encoding", "utf-8")
                with open(path, "r", encoding=encoding, errors="replace") as f:
                    lines = f.readlines()
                offset = int(params.get("offset", 0))
                limit = int(params.get("limit", -1))
                if limit > 0:
                    lines = lines[offset:offset + limit]
                elif offset > 0:
                    lines = lines[offset:]
                return {"success": True, "lines": lines, "count": len(lines)}

            elif operation == "write":
                content = params.get("content", "")
                encoding = params.get("encoding", "utf-8")
                _ensure_dir(os.path.dirname(path) or ".")
                with open(path, "w", encoding=encoding) as f:
                    f.write(content)
                return {"success": True, "path": path, "bytes": len(content)}

            elif operation == "append":
                content = params.get("content", "")
                with open(path, "a", encoding="utf-8") as f:
                    f.write(content)
                return {"success": True, "path": path, "appended_bytes": len(content)}

            elif operation == "copy":
                src = path
                dst = params.get("dest", "")
                overwrite = params.get("overwrite", False)
                if not dst:
                    return {"success": False, "error": "dest required"}
                success = _copy_file(src, dst, overwrite)
                return {"success": success, "src": src, "dest": dst}

            elif operation == "move":
                src = path
                dst = params.get("dest", "")
                if not dst:
                    return {"success": False, "error": "dest required"}
                shutil.move(src, dst)
                return {"success": True, "src": src, "dest": dst}

            elif operation == "delete":
                if os.path.isdir(path):
                    shutil.rmtree(path)
                elif os.path.isfile(path):
                    os.remove(path)
                else:
                    return {"success": False, "error": "Path does not exist"}
                return {"success": True, "path": path}

            elif operation == "mkdir":
                recursive = params.get("recursive", True)
                if recursive:
                    _ensure_dir(path)
                else:
                    os.mkdir(path)
                return {"success": True, "path": path}

            elif operation == "glob":
                pattern = params.get("pattern", path + "/*")
                recursive = params.get("recursive", False)
                if recursive:
                    matches = glob_module.glob(pattern, recursive=True)
                else:
                    matches = glob_module.glob(pattern)
                return {"success": True, "matches": matches, "count": len(matches)}

            elif operation == "list_dir":
                if not os.path.isdir(path):
                    return {"success": False, "error": "Not a directory"}
                entries = os.listdir(path)
                return {"success": True, "entries": entries, "count": len(entries)}

            elif operation == "walk":
                if not os.path.isdir(path):
                    return {"success": False, "error": "Not a directory"}
                max_depth = int(params.get("max_depth", 3))
                tree = _walk_dir(path, max_depth)
                return {"success": True, "entries": tree, "count": len(tree)}

            elif operation == "file_hash":
                if not os.path.isfile(path):
                    return {"success": False, "error": "Not a file"}
                algorithm = params.get("algorithm", "sha256")
                hash_value = _calculate_hash(path, algorithm)
                return {"success": True, "hash": hash_value, "algorithm": algorithm, "path": path}

            elif operation == "file_size":
                if not os.path.isfile(path):
                    return {"success": False, "error": "Not a file"}
                size = os.path.getsize(path)
                return {"success": True, "size_bytes": size, "path": path}

            elif operation == "temp_file":
                suffix = params.get("suffix", "")
                prefix = params.get("prefix", "tmp")
                fd, temp_path = tempfile.mkstemp(suffix=suffix, prefix=prefix)
                os.close(fd)
                return {"success": True, "path": temp_path}

            elif operation == "temp_dir":
                prefix = params.get("prefix", "tmp")
                temp_path = tempfile.mkdtemp(prefix=prefix)
                return {"success": True, "path": temp_path}

            elif operation == "disk_usage":
                if not os.path.exists(path):
                    return {"success": False, "error": "Path does not exist"}
                total, used, free = shutil.disk_usage(path)
                return {"success": True, "total_bytes": total, "used_bytes": used, "free_bytes": free, "percent_used": round(used / total * 100, 2)}

            elif operation == "expand_path":
                expanded = os.path.expanduser(os.path.expandvars(path))
                return {"success": True, "path": expanded, "expanded": expanded}

            elif operation == "join_path":
                parts = params.get("parts", [path])
                joined = os.path.join(*parts) if parts else path
                return {"success": True, "path": joined}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"FileSystemAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for file system operations."""
    return FileSystemAction().execute(context, params)
