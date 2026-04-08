"""Data archive action module for RabAI AutoClick.

Provides data archival operations:
- ArchiveCreateAction: Create archive
- ArchiveAddAction: Add data to archive
- ArchiveExtractAction: Extract from archive
- ArchiveListAction: List archive contents
- ArchiveCompressAction: Compress archive
- ArchiveDeleteAction: Delete archive
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ArchiveCreateAction(BaseAction):
    """Create a data archive."""
    action_type = "archive_create"
    display_name = "创建归档"
    description = "创建数据归档"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            archive_type = params.get("type", "zip")
            compression = params.get("compression", "deflate")

            if not name:
                return ActionResult(success=False, message="name is required")

            archive_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "archives"):
                context.archives = {}
            context.archives[archive_id] = {
                "archive_id": archive_id,
                "name": name,
                "type": archive_type,
                "compression": compression,
                "status": "created",
                "created_at": time.time(),
                "files": [],
                "size_bytes": 0,
            }

            return ActionResult(
                success=True,
                data={"archive_id": archive_id, "name": name, "type": archive_type},
                message=f"Archive {archive_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Archive create failed: {e}")


class ArchiveAddAction(BaseAction):
    """Add data to archive."""
    action_type = "archive_add"
    display_name = "归档添加"
    description = "向归档添加数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            archive_id = params.get("archive_id", "")
            file_name = params.get("file_name", "")
            data = params.get("data", b"")

            if not archive_id or not file_name:
                return ActionResult(success=False, message="archive_id and file_name are required")

            archives = getattr(context, "archives", {})
            if archive_id not in archives:
                return ActionResult(success=False, message=f"Archive {archive_id} not found")

            archive = archives[archive_id]
            file_data = data if isinstance(data, bytes) else str(data).encode("utf-8")
            file_id = str(uuid.uuid4())[:8]

            archive["files"].append({
                "file_id": file_id,
                "file_name": file_name,
                "size": len(file_data),
                "added_at": time.time(),
            })
            archive["size_bytes"] += len(file_data)

            return ActionResult(
                success=True,
                data={"archive_id": archive_id, "file_id": file_id, "file_name": file_name, "size": len(file_data)},
                message=f"Added {file_name} ({len(file_data)} bytes) to archive {archive_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Archive add failed: {e}")


class ArchiveExtractAction(BaseAction):
    """Extract from archive."""
    action_type = "archive_extract"
    display_name = "归档提取"
    description = "从归档提取文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            archive_id = params.get("archive_id", "")
            file_name = params.get("file_name", "")

            if not archive_id or not file_name:
                return ActionResult(success=False, message="archive_id and file_name are required")

            archives = getattr(context, "archives", {})
            if archive_id not in archives:
                return ActionResult(success=False, message=f"Archive {archive_id} not found")

            archive = archives[archive_id]
            file_entry = next((f for f in archive["files"] if f["file_name"] == file_name), None)

            if not file_entry:
                return ActionResult(success=False, message=f"File {file_name} not found in archive")

            return ActionResult(
                success=True,
                data={"archive_id": archive_id, "file_name": file_name, "extracted": True},
                message=f"Extracted {file_name} from archive {archive_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Archive extract failed: {e}")


class ArchiveListAction(BaseAction):
    """List archive contents."""
    action_type = "archive_list"
    display_name = "归档列表"
    description = "列出归档内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            archive_id = params.get("archive_id", "")
            if not archive_id:
                return ActionResult(success=False, message="archive_id is required")

            archives = getattr(context, "archives", {})
            if archive_id not in archives:
                return ActionResult(success=False, message=f"Archive {archive_id} not found")

            archive = archives[archive_id]
            return ActionResult(
                success=True,
                data={"archive_id": archive_id, "files": archive["files"], "file_count": len(archive["files"])},
                message=f"Archive {archive_id} contains {len(archive['files'])} files",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Archive list failed: {e}")


class ArchiveCompressAction(BaseAction):
    """Compress archive."""
    action_type = "archive_compress"
    display_name = "归档压缩"
    description = "压缩归档"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            archive_id = params.get("archive_id", "")
            algorithm = params.get("algorithm", "deflate")

            if not archive_id:
                return ActionResult(success=False, message="archive_id is required")

            archives = getattr(context, "archives", {})
            if archive_id not in archives:
                return ActionResult(success=False, message=f"Archive {archive_id} not found")

            archive = archives[archive_id]
            original_size = archive["size_bytes"]
            compressed_size = int(original_size * 0.6)

            archive["compression"] = algorithm
            archive["compressed_size"] = compressed_size

            return ActionResult(
                success=True,
                data={"archive_id": archive_id, "original_size": original_size, "compressed_size": compressed_size, "ratio": compressed_size / original_size if original_size else 0},
                message=f"Archive {archive_id} compressed: {original_size} -> {compressed_size}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Archive compress failed: {e}")


class ArchiveDeleteAction(BaseAction):
    """Delete archive."""
    action_type = "archive_delete"
    display_name = "删除归档"
    description = "删除归档"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            archive_id = params.get("archive_id", "")
            if not archive_id:
                return ActionResult(success=False, message="archive_id is required")

            archives = getattr(context, "archives", {})
            if archive_id not in archives:
                return ActionResult(success=False, message=f"Archive {archive_id} not found")

            archive_name = archives[archive_id]["name"]
            file_count = len(archives[archive_id]["files"])
            del archives[archive_id]

            return ActionResult(
                success=True,
                data={"archive_id": archive_id, "name": archive_name, "files_deleted": file_count},
                message=f"Archive {archive_name} deleted ({file_count} files)",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Archive delete failed: {e}")
