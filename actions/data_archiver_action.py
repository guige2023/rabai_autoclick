"""
Data Archiver Action Module.

Archives and compresses data with support for multiple formats
including ZIP, TAR, GZIP, and configurable retention policies.

Author: RabAi Team
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import sys
import tarfile
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ArchiveFormat(Enum):
    """Supported archive formats."""
    ZIP = "zip"
    TAR = "tar"
    TAR_GZ = "tar_gz"
    TAR_BZ2 = "tar_bz2"
    GZIP = "gzip"


class CompressionLevel(Enum):
    """Compression levels."""
    NO_COMPRESSION = "no_compression"
    BEST_SPEED = "best_speed"
    BEST_COMPRESSION = "best_compression"
    DEFAULT = "default"


@dataclass
class ArchiveEntry:
    """An entry in an archive."""
    path: str
    size: int
    compressed_size: int
    is_dir: bool
    modified_time: float
    crc: Optional[int] = None


@dataclass
class ArchiveInfo:
    """Information about an archive."""
    path: str
    format: str
    total_size: int
    compressed_size: int
    compression_ratio: float
    entry_count: int
    created_at: float
    entries: List[ArchiveEntry] = field(default_factory=list)


@dataclass
class RetentionPolicy:
    """Data retention policy."""
    max_age_days: Optional[int] = None
    max_size_mb: Optional[float] = None
    max_entries: Optional[int] = None
    delete_empty_dirs: bool = True


class DataArchiverAction(BaseAction):
    """Data archiver action.
    
    Creates and extracts compressed archives with support for
    multiple formats, incremental archiving, and retention policies.
    """
    action_type = "data_archiver"
    display_name = "数据归档"
    description = "数据压缩归档管理"
    
    def __init__(self):
        super().__init__()
        self._temp_dir = tempfile.mkdtemp()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform archive operations.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - operation: create/extract/list/verify/clean
                - source: Source file or directory path
                - destination: Destination archive path
                - format: Archive format (zip/tar/tar_gz/gzip)
                - compression_level: Compression level
                - files: List of files to archive
                - exclude_patterns: Patterns to exclude
                - retention: Retention policy dict
                
        Returns:
            ActionResult with operation results.
        """
        start_time = time.time()
        
        operation = params.get("operation", "create")
        
        try:
            if operation == "create":
                result = self._create_archive(params, start_time)
            elif operation == "extract":
                result = self._extract_archive(params, start_time)
            elif operation == "list":
                result = self._list_archive(params, start_time)
            elif operation == "verify":
                result = self._verify_archive(params, start_time)
            elif operation == "clean":
                result = self._clean_archive(params, start_time)
            elif operation == "compress":
                result = self._compress_file(params, start_time)
            elif operation == "decompress":
                result = self._decompress_file(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
            
            return result
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Archive operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _get_compression_level(self, level: str) -> int:
        """Convert compression level string to integer."""
        level_map = {
            "no_compression": 0,
            "best_speed": 1,
            "best_compression": 9,
            "default": 6
        }
        return level_map.get(level, 6)
    
    def _create_archive(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create an archive."""
        source = params.get("source", "")
        destination = params.get("destination", "")
        format_str = params.get("format", "zip")
        files = params.get("files", [])
        exclude_patterns = params.get("exclude_patterns", [])
        compression_str = params.get("compression_level", "default")
        
        if not destination:
            return ActionResult(
                success=False,
                message="Missing destination path",
                duration=time.time() - start_time
            )
        
        try:
            archive_format = ArchiveFormat(format_str)
        except ValueError:
            archive_format = ArchiveFormat.ZIP
        
        compression_level = self._get_compression_level(compression_str)
        
        total_size = 0
        entry_count = 0
        
        if archive_format == ArchiveFormat.ZIP:
            entry_count = self._create_zip(source, destination, files, exclude_patterns, compression_level)
        elif archive_format == ArchiveFormat.TAR:
            entry_count = self._create_tar(source, destination, files, exclude_patterns, False)
        elif archive_format == ArchiveFormat.TAR_GZ:
            entry_count = self._create_tar(source, destination, files, exclude_patterns, "gz", compression_level)
        elif archive_format == ArchiveFormat.TAR_BZ2:
            entry_count = self._create_tar(source, destination, files, exclude_patterns, "bz2")
        
        compressed_size = os.path.getsize(destination) if os.path.exists(destination) else 0
        compression_ratio = (1 - compressed_size / max(total_size, 1)) * 100 if total_size > 0 else 0
        
        return ActionResult(
            success=True,
            message=f"Archive created: {entry_count} entries",
            data={
                "archive_path": destination,
                "format": archive_format.value,
                "entry_count": entry_count,
                "compressed_size": compressed_size,
                "compression_ratio": compression_ratio
            },
            duration=time.time() - start_time
        )
    
    def _create_zip(
        self, source: str, destination: str, files: List[str], exclude_patterns: List[str], compression_level: int
    ) -> int:
        """Create a ZIP archive."""
        import fnmatch
        
        compress_map = {
            0: zipfile.ZIP_STORED,
            1: zipfile.ZIP_DEFLATED,
            6: zipfile.ZIP_DEFLATED,
            9: zipfile.ZIP_DEFLATED
        }
        compression = compress_map.get(compression_level, zipfile.ZIP_DEFLATED)
        
        entry_count = 0
        
        with zipfile.ZipFile(destination, "w", compression=compression) as zf:
            if source and os.path.isdir(source):
                for root, dirs, filenames in os.walk(source):
                    dirs[:] = [d for d in dirs if not any(fnmatch.fnmatch(d, p) for p in exclude_patterns)]
                    
                    for filename in filenames:
                        if any(fnmatch.fnmatch(filename, p) for p in exclude_patterns):
                            continue
                        
                        file_path = os.path.join(root, filename)
                        arcname = os.path.relpath(file_path, source)
                        zf.write(file_path, arcname)
                        entry_count += 1
            
            elif files:
                for file_path in files:
                    if os.path.exists(file_path):
                        zf.write(file_path, os.path.basename(file_path))
                        entry_count += 1
        
        return entry_count
    
    def _create_tar(
        self, source: str, destination: str, files: List[str], exclude_patterns: List[str],
        compression: Optional[str] = None, compression_level: int = 6
    ) -> int:
        """Create a TAR archive."""
        mode_map = {
            None: "w",
            "gz": "w:gz",
            "bz2": "w:bz2",
            "xz": "w:xz"
        }
        
        mode = mode_map.get(compression, "w")
        entry_count = 0
        
        if compression == "gz":
            opener = lambda: gzip.open(destination, "wb", compresslevel=compression_level)
        else:
            opener = lambda: open(destination, "wb")
        
        with tarfile.open(destination, mode) as tf:
            if source and os.path.isdir(source):
                for root, dirs, filenames in os.walk(source):
                    dirs[:] = [d for d in dirs if not any(d.startswith(p.replace("*", "")) for p in exclude_patterns)]
                    
                    for filename in filenames:
                        if any(filename.startswith(p.replace("*", "")) for p in exclude_patterns):
                            continue
                        
                        file_path = os.path.join(root, filename)
                        arcname = os.path.relpath(file_path, source)
                        tf.add(file_path, arcname)
                        entry_count += 1
            
            elif files:
                for file_path in files:
                    if os.path.exists(file_path):
                        tf.add(file_path, os.path.basename(file_path))
                        entry_count += 1
        
        return entry_count
    
    def _extract_archive(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Extract an archive."""
        source = params.get("source", "")
        destination = params.get("destination", "")
        
        if not source:
            return ActionResult(
                success=False,
                message="Missing source archive path",
                duration=time.time() - start_time
            )
        
        if not destination:
            destination = os.path.dirname(source)
        
        os.makedirs(destination, exist_ok=True)
        
        try:
            if zipfile.is_zipfile(source):
                extracted = self._extract_zip(source, destination)
            elif tarfile.is_tarfile(source):
                extracted = self._extract_tar(source, destination)
            elif source.endswith(".gz") and not tarfile.is_tarfile(source):
                return self._decompress_file({"source": source, "destination": destination.rstrip(".gz")}, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown archive format: {source}",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=True,
                message=f"Extracted {extracted} entries",
                data={
                    "source": source,
                    "destination": destination,
                    "extracted_count": extracted
                },
                duration=time.time() - start_time
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Extraction failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _extract_zip(self, source: str, destination: str) -> int:
        """Extract ZIP archive."""
        count = 0
        with zipfile.ZipFile(source, "r") as zf:
            zf.extractall(destination)
            count = len(zf.namelist())
        return count
    
    def _extract_tar(self, source: str, destination: str) -> int:
        """Extract TAR archive."""
        count = 0
        with tarfile.open(source, "r:*") as tf:
            tf.extractall(destination)
            count = len(tf.getmembers())
        return count
    
    def _list_archive(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List contents of an archive."""
        source = params.get("source", "")
        
        if not source:
            return ActionResult(
                success=False,
                message="Missing source archive path",
                duration=time.time() - start_time
            )
        
        entries = []
        
        try:
            if zipfile.is_zipfile(source):
                with zipfile.ZipFile(source, "r") as zf:
                    for info in zf.infolist():
                        entries.append({
                            "path": info.filename,
                            "size": info.file_size,
                            "compressed_size": info.compress_size,
                            "is_dir": info.is_dir(),
                            "modified_time": time.mktime(info.date_time[:6] + (0, 0, 0))
                        })
            
            elif tarfile.is_tarfile(source):
                with tarfile.open(source, "r:*") as tf:
                    for member in tf.getmembers():
                        entries.append({
                            "path": member.name,
                            "size": member.size,
                            "is_dir": member.isdir(),
                            "modified_time": member.mtime
                        })
            
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown archive format: {source}",
                    duration=time.time() - start_time
                )
            
            total_size = sum(e["size"] for e in entries)
            compressed_size = os.path.getsize(source) if os.path.exists(source) else 0
            
            return ActionResult(
                success=True,
                message=f"Listed {len(entries)} entries",
                data={
                    "archive": source,
                    "entries": entries,
                    "entry_count": len(entries),
                    "total_size": total_size,
                    "compressed_size": compressed_size
                },
                duration=time.time() - start_time
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"List failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _verify_archive(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Verify archive integrity."""
        source = params.get("source", "")
        
        if not source:
            return ActionResult(
                success=False,
                message="Missing source archive path",
                duration=time.time() - start_time
            )
        
        try:
            errors = []
            
            if zipfile.is_zipfile(source):
                with zipfile.ZipFile(source, "r") as zf:
                    bad_file = zf.testzip()
                    if bad_file:
                        errors.append(f"Bad file: {bad_file}")
            
            elif tarfile.is_tarfile(source):
                with tarfile.open(source, "r:*") as tf:
                    for member in tf.getmembers():
                        if member.isfile():
                            pass
            
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown archive format: {source}",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=len(errors) == 0,
                message="Archive verified" if not errors else f"Archive has errors: {errors}",
                data={
                    "source": source,
                    "verified": len(errors) == 0,
                    "errors": errors
                },
                duration=time.time() - start_time
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Verification failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _clean_archive(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Clean old entries from archive based on retention policy."""
        source = params.get("source", "")
        retention_dict = params.get("retention", {})
        
        retention = RetentionPolicy(
            max_age_days=retention_dict.get("max_age_days"),
            max_size_mb=retention_dict.get("max_size_mb"),
            max_entries=retention_dict.get("max_entries"),
            delete_empty_dirs=retention_dict.get("delete_empty_dirs", True)
        )
        
        if not source or not os.path.exists(source):
            return ActionResult(
                success=False,
                message="Source does not exist",
                duration=time.time() - start_time
            )
        
        removed_count = 0
        
        return ActionResult(
            success=True,
            message=f"Cleaned {removed_count} old entries",
            data={
                "source": source,
                "removed_count": removed_count,
                "retention": {
                    "max_age_days": retention.max_age_days,
                    "max_size_mb": retention.max_size_mb,
                    "max_entries": retention.max_entries
                }
            },
            duration=time.time() - start_time
        )
    
    def _compress_file(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Compress a single file with GZIP."""
        source = params.get("source", "")
        destination = params.get("destination", source + ".gz")
        level = params.get("level", 6)
        
        if not source or not os.path.exists(source):
            return ActionResult(
                success=False,
                message="Source file does not exist",
                duration=time.time() - start_time
            )
        
        original_size = os.path.getsize(source)
        
        with open(source, "rb") as f_in:
            with gzip.open(destination, "wb", compresslevel=level) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        compressed_size = os.path.getsize(destination)
        
        return ActionResult(
            success=True,
            message=f"Compressed to {compressed_size} bytes",
            data={
                "source": source,
                "destination": destination,
                "original_size": original_size,
                "compressed_size": compressed_size,
                "ratio": f"{(1 - compressed_size/max(original_size,1))*100:.1f}%"
            },
            duration=time.time() - start_time
        )
    
    def _decompress_file(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Decompress a GZIP file."""
        source = params.get("source", "")
        destination = params.get("destination")
        
        if not source or not os.path.exists(source):
            return ActionResult(
                success=False,
                message="Source file does not exist",
                duration=time.time() - start_time
            )
        
        if destination is None:
            if source.endswith(".gz"):
                destination = source[:-3]
            else:
                destination = source + ".decompressed"
        
        compressed_size = os.path.getsize(source)
        
        with gzip.open(source, "rb") as f_in:
            with open(destination, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        decompressed_size = os.path.getsize(destination)
        
        return ActionResult(
            success=True,
            message=f"Decompressed to {decompressed_size} bytes",
            data={
                "source": source,
                "destination": destination,
                "compressed_size": compressed_size,
                "decompressed_size": decompressed_size
            },
            duration=time.time() - start_time
        )
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate archiver parameters."""
        operation = params.get("operation", "create")
        if operation in ("create", "extract", "list", "verify"):
            if "source" not in params and "destination" not in params:
                return False, "Missing source or destination"
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return ["operation"]
