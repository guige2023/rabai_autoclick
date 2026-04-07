"""File system automation action module for RabAI AutoClick.

Provides file system operations:
- FileListAction: List directory contents
- FileSearchAction: Search for files
- FileCopyAction: Copy files
- FileMoveAction: Move files
- FileDeleteAction: Delete files
- FileCreateAction: Create files/directories
- FileMetadataAction: Get file metadata
- FileWatchAction: Watch for file changes
"""

import os
import shutil
import time
import fnmatch
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
import os as _os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FileListAction(BaseAction):
    """List directory contents."""
    action_type = "file_list"
    display_name = "列出文件"
    description = "列出目录内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            directory = params.get("directory", ".")
            pattern = params.get("pattern", "*")
            recursive = params.get("recursive", False)
            include_hidden = params.get("include_hidden", False)

            path = Path(directory).expanduser().resolve()

            if not path.exists():
                return ActionResult(success=False, message=f"Directory does not exist: {directory}")
            if not path.is_dir():
                return ActionResult(success=False, message=f"Not a directory: {directory}")

            results = []

            if recursive:
                for item in path.rglob("*"):
                    if not include_hidden and any(part.startswith(".") for part in item.parts):
                        continue
                    if fnmatch.fnmatch(item.name, pattern):
                        results.append({
                            "name": item.name,
                            "path": str(item),
                            "is_dir": item.is_dir(),
                            "is_file": item.is_file(),
                            "size": item.stat().st_size if item.is_file() else None
                        })
            else:
                for item in path.iterdir():
                    if not include_hidden and item.name.startswith("."):
                        continue
                    if fnmatch.fnmatch(item.name, pattern):
                        results.append({
                            "name": item.name,
                            "path": str(item),
                            "is_dir": item.is_dir(),
                            "is_file": item.is_file(),
                            "size": item.stat().st_size if item.is_file() else None
                        })

            results.sort(key=lambda x: (not x["is_dir"], x["name"]))

            return ActionResult(
                success=True,
                message=f"Listed {len(results)} items",
                data={"items": results, "count": len(results), "directory": str(path)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"List error: {str(e)}")


class FileSearchAction(BaseAction):
    """Search for files."""
    action_type = "file_search"
    display_name = "搜索文件"
    description = "搜索文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            root = params.get("root", ".")
            pattern = params.get("pattern", "*")
            name_contains = params.get("name_contains", "")
            file_type = params.get("type", "all")
            max_results = params.get("max_results", 100)

            path = Path(root).expanduser().resolve()

            if not path.exists():
                return ActionResult(success=False, message=f"Root directory does not exist")

            results = []
            count = 0

            for item in path.rglob("*"):
                if count >= max_results:
                    break

                if item.name.startswith("."):
                    continue

                if name_contains and name_contains.lower() not in item.name.lower():
                    continue

                if not fnmatch.fnmatch(item.name, pattern):
                    continue

                if file_type == "file" and not item.is_file():
                    continue
                elif file_type == "dir" and not item.is_dir():
                    continue

                results.append({
                    "name": item.name,
                    "path": str(item),
                    "is_dir": item.is_dir(),
                    "size": item.stat().st_size if item.is_file() else None,
                    "modified": item.stat().st_mtime if item.exists() else None
                })
                count += 1

            results.sort(key=lambda x: x["name"])

            return ActionResult(
                success=True,
                message=f"Found {len(results)} files matching '{pattern}'",
                data={"files": results, "count": len(results), "pattern": pattern}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Search error: {str(e)}")


class FileCopyAction(BaseAction):
    """Copy files."""
    action_type = "file_copy"
    display_name = "复制文件"
    description = "复制文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            destination = params.get("destination", "")
            overwrite = params.get("overwrite", False)
            preserve_metadata = params.get("preserve_metadata", True)

            if not source or not destination:
                return ActionResult(success=False, message="source and destination required")

            src_path = Path(source).expanduser().resolve()
            dst_path = Path(destination).expanduser().resolve()

            if not src_path.exists():
                return ActionResult(success=False, message=f"Source does not exist: {source}")

            if dst_path.exists() and not overwrite:
                return ActionResult(success=False, message=f"Destination exists: {destination}")

            if src_path.is_dir():
                if dst_path.exists() and dst_path.is_file():
                    return ActionResult(success=False, message=f"Cannot copy dir to file: {destination}")

                if preserve_metadata:
                    shutil.copytree(src_path, dst_path, dirs_exist_ok=overwrite)
                else:
                    shutil.copytree(src_path, dst_path, dirs_exist_ok=overwrite)

                return ActionResult(success=True, message=f"Copied directory to {destination}")

            else:
                dst_parent = dst_path.parent
                dst_parent.mkdir(parents=True, exist_ok=True)

                if preserve_metadata:
                    shutil.copy2(src_path, dst_path)
                else:
                    shutil.copy(src_path, dst_path)

                return ActionResult(success=True, message=f"Copied {source} to {destination}")

        except Exception as e:
            return ActionResult(success=False, message=f"Copy error: {str(e)}")


class FileMoveAction(BaseAction):
    """Move files."""
    action_type = "file_move"
    display_name = "移动文件"
    description = "移动文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            destination = params.get("destination", "")
            overwrite = params.get("overwrite", False)

            if not source or not destination:
                return ActionResult(success=False, message="source and destination required")

            src_path = Path(source).expanduser().resolve()
            dst_path = Path(destination).expanduser().resolve()

            if not src_path.exists():
                return ActionResult(success=False, message=f"Source does not exist: {source}")

            if dst_path.exists() and not overwrite:
                return ActionResult(success=False, message=f"Destination exists: {destination}")

            dst_parent = dst_path.parent
            dst_parent.mkdir(parents=True, exist_ok=True)

            shutil.move(str(src_path), str(dst_path))

            return ActionResult(success=True, message=f"Moved {source} to {destination}")

        except Exception as e:
            return ActionResult(success=False, message=f"Move error: {str(e)}")


class FileDeleteAction(BaseAction):
    """Delete files."""
    action_type = "file_delete"
    display_name = "删除文件"
    description = "删除文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            target = params.get("target", "")
            recursive = params.get("recursive", False)
            use_trash = params.get("use_trash", True)

            if not target:
                return ActionResult(success=False, message="target required")

            path = Path(target).expanduser().resolve()

            if not path.exists():
                return ActionResult(success=False, message=f"Target does not exist: {target}")

            if path.is_dir():
                if recursive:
                    if use_trash:
                        trash_dir = Path.home() / ".Trash"
                        dest = trash_dir / path.name
                        counter = 1
                        while dest.exists():
                            dest = trash_dir / f"{path.name}_{counter}"
                            counter += 1
                        shutil.move(str(path), str(dest))
                    else:
                        shutil.rmtree(path)
                else:
                    path.rmdir()

            else:
                if use_trash:
                    trash_dir = Path.home() / ".Trash"
                    trash_dir.mkdir(exist_ok=True)
                    dest = trash_dir / path.name
                    counter = 1
                    while dest.exists():
                        dest = trash_dir / f"{path.stem}_{counter}{path.suffix}"
                        counter += 1
                    shutil.move(str(path), str(dest))
                else:
                    path.unlink()

            return ActionResult(success=True, message=f"Deleted {target}")

        except Exception as e:
            return ActionResult(success=False, message=f"Delete error: {str(e)}")


class FileCreateAction(BaseAction):
    """Create files/directories."""
    action_type = "file_create"
    display_name = "创建文件"
    description = "创建文件或目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            target = params.get("target", "")
            content = params.get("content", "")
            is_directory = params.get("is_directory", False)
            parents = params.get("parents", True)

            if not target:
                return ActionResult(success=False, message="target required")

            path = Path(target).expanduser().resolve()

            if is_directory:
                path.mkdir(parents=parents, exist_ok=True)
                return ActionResult(success=True, message=f"Created directory: {target}")

            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                if content:
                    path.write_text(content, encoding="utf-8")
                else:
                    path.touch()
                return ActionResult(success=True, message=f"Created file: {target}")

        except Exception as e:
            return ActionResult(success=False, message=f"Create error: {str(e)}")


class FileMetadataAction(BaseAction):
    """Get file metadata."""
    action_type = "file_metadata"
    display_name = "文件元数据"
    description = "获取文件元数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            target = params.get("target", "")
            compute_hash = params.get("compute_hash", False)

            if not target:
                return ActionResult(success=False, message="target required")

            path = Path(target).expanduser().resolve()

            if not path.exists():
                return ActionResult(success=False, message=f"Target does not exist: {target}")

            stat = path.stat()

            metadata = {
                "name": path.name,
                "path": str(path),
                "is_file": path.is_file(),
                "is_dir": path.is_dir(),
                "size": stat.st_size,
                "created": stat.st_ctime,
                "modified": stat.st_mtime,
                "accessed": stat.st_atime,
            }

            if compute_hash and path.is_file():
                md5 = hashlib.md5()
                sha256 = hashlib.sha256()
                with open(path, "rb") as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        md5.update(chunk)
                        sha256.update(chunk)
                metadata["md5"] = md5.hexdigest()
                metadata["sha256"] = sha256.hexdigest()

            return ActionResult(
                success=True,
                message=f"Retrieved metadata for {target}",
                data={"metadata": metadata}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Metadata error: {str(e)}")


class FileWatchAction(BaseAction):
    """Watch for file changes."""
    action_type = "file_watch"
    display_name = "监控文件"
    description = "监控文件变化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            target = params.get("target", "")
            recursive = params.get("recursive", False)
            timeout = params.get("timeout", 10)
            poll_interval = params.get("poll_interval", 0.5)

            if not target:
                return ActionResult(success=False, message="target required")

            path = Path(target).expanduser().resolve()

            if not path.exists():
                return ActionResult(success=False, message=f"Target does not exist: {target}")

            initial_stat = path.stat() if path.is_file() else None
            initial_mtime = initial_stat.st_mtime if initial_stat else None
            initial_contents = {}

            if path.is_dir():
                for item in path.rglob("*") if recursive else path.iterdir():
                    if item.is_file():
                        try:
                            initial_contents[str(item)] = item.stat().st_mtime
                        except:
                            pass

            start_time = time.time()
            changes = []

            while time.time() - start_time < timeout:
                time.sleep(poll_interval)

                if path.is_file():
                    current_stat = path.stat()
                    if current_stat.st_mtime != initial_mtime:
                        changes.append({
                            "type": "modified",
                            "path": str(path),
                            "time": time.time()
                        })
                        break

                elif path.is_dir():
                    current_contents = {}
                    items = path.rglob("*") if recursive else path.iterdir()
                    for item in items:
                        if item.is_file():
                            try:
                                current_contents[str(item)] = item.stat().st_mtime
                            except:
                                pass

                    for p, mtime in current_contents.items():
                        if p not in initial_contents:
                            changes.append({"type": "created", "path": p, "time": time.time()})
                        elif current_contents[p] != initial_contents[p]:
                            changes.append({"type": "modified", "path": p, "time": time.time()})

                    for p in initial_contents:
                        if p not in current_contents:
                            changes.append({"type": "deleted", "path": p, "time": time.time()})

                    if changes:
                        break

            return ActionResult(
                success=True,
                message=f"Watch completed: {len(changes)} changes detected",
                data={"changes": changes, "change_count": len(changes), "watched": str(path)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Watch error: {str(e)}")
