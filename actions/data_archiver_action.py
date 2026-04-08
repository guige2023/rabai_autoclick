"""Data Archiver Action Module. Archives files and directories."""
import sys, os, tarfile, zipfile, hashlib
from typing import Any
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class ArchiveEntry:
    path: str; size_bytes: int; compressed_size: int; checksum: str; modified_time: float

@dataclass
class ArchiveResult:
    archive_path: str; archive_format: str; total_files: int
    total_size_bytes: int; compressed_size_bytes: int; compression_ratio: float
    entries: list = field(default_factory=list)

class DataArchiverAction(BaseAction):
    action_type = "data_archiver"; display_name = "数据归档"
    description = "归档文件和目录"
    def __init__(self) -> None: super().__init__()
    def _checksum(self, path: str) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""): h.update(chunk)
        return h.hexdigest()
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode","create"); source = params.get("source"); output = params.get("output")
        archive_fmt = params.get("format","tar.gz").lower()
        compression = params.get("compression","gzip")
        exclude = params.get("exclude_patterns",[]); dest = params.get("destination",".")
        if mode == "extract":
            if not output: return ActionResult(success=False, message="Archive path required")
            try:
                if archive_fmt == "zip":
                    with zipfile.ZipFile(output,"r") as zf: zf.extractall(dest)
                else:
                    mode_map = {"tar": "r", "tar.gz": "r:gz", "tar.bz2": "r:bz2"}
                    with tarfile.open(output, mode_map.get(archive_fmt,"r")) as tar: tar.extractall(dest)
                return ActionResult(success=True, message=f"Extracted to {dest}")
            except Exception as e: return ActionResult(success=False, message=f"Error: {e}")
        if not source or not output: return ActionResult(success=False, message="Source and output required")
        if not os.path.exists(source): return ActionResult(success=False, message=f"Source not found: {source}")
        try:
            entries = []; total_size = 0
            if archive_fmt == "zip":
                compress_map = {"gzip": zipfile.ZIP_DEFLATED, "none": zipfile.ZIP_STORED}
                with zipfile.ZipFile(output, "w", compression=compress_map.get(compression, zipfile.ZIP_DEFLATED)) as zf:
                    for root, dirs, files in os.walk(source):
                        dirs[:] = [d for d in dirs if not any(p in d for p in exclude)]
                        for file in files:
                            if any(p in file for p in exclude): continue
                            filepath = os.path.join(root, file)
                            arcname = os.path.relpath(filepath, source)
                            zf.write(filepath, arcname)
                            size = os.path.getsize(filepath); total_size += size
                            entries.append(ArchiveEntry(path=arcname, size_bytes=size, compressed_size=0,
                                                      checksum=self._checksum(filepath), modified_time=os.path.getmtime(filepath)))
                compressed_size = os.path.getsize(output)
            else:
                mode_map = {"gzip": "w:gz", "bzip2": "w:bz2", "none": "w"}
                with tarfile.open(output, mode_map.get(compression,"w:gz")) as tar:
                    for root, dirs, files in os.walk(source):
                        dirs[:] = [d for d in dirs if not any(p in d for p in exclude)]
                        for file in files:
                            if any(p in file for p in exclude): continue
                            filepath = os.path.join(root, file)
                            arcname = os.path.relpath(filepath, source)
                            tar.add(filepath, arcname=arcname)
                            size = os.path.getsize(filepath); total_size += size
                            entries.append(ArchiveEntry(path=arcname, size_bytes=size, compressed_size=0,
                                                      checksum=self._checksum(filepath), modified_time=os.path.getmtime(filepath)))
                compressed_size = os.path.getsize(output)
            result = ArchiveResult(archive_path=output, archive_format=archive_fmt, total_files=len(entries),
                                  total_size_bytes=total_size, compressed_size_bytes=compressed_size,
                                  compression_ratio=(1-compressed_size/total_size) if total_size > 0 else 0,
                                  entries=entries)
            return ActionResult(success=True, message=f"Archived {len(entries)} files: {compressed_size} bytes",
                              data=vars(result))
        except Exception as e: return ActionResult(success=False, message=f"Error: {e}")
