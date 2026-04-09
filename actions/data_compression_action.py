"""Data compression action module for RabAI AutoClick.

Provides data compression operations:
- CompressAction: Compress data using various algorithms
- DecompressAction: Decompress data
- CompressionAnalyzerAction: Analyze compression efficiency
- ArchiveManagerAction: Manage compressed archives
"""

import sys
import os
import logging
import gzip
import zlib
import bz2
import lzma
import tarfile
import zipfile
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class CompressionResult:
    """Result of a compression operation."""
    original_size: int
    compressed_size: int
    ratio: float
    algorithm: str
    duration_ms: float


class CompressionAnalyzer:
    """Analyzes compression efficiency."""

    @staticmethod
    def analyze(data: bytes, algorithms: List[str]) -> List[CompressionResult]:
        results = []
        for algo in algorithms:
            try:
                import time
                start = time.time()

                if algo == "gzip":
                    compressed = gzip.compress(data)
                elif algo == "zlib":
                    compressed = zlib.compress(data)
                elif algo == "bz2":
                    compressed = bz2.compress(data)
                elif algo == "lzma":
                    compressed = lzma.compress(data)
                else:
                    continue

                duration = (time.time() - start) * 1000
                ratio = len(compressed) / len(data) if len(data) > 0 else 0

                results.append(CompressionResult(
                    original_size=len(data),
                    compressed_size=len(compressed),
                    ratio=round(ratio * 100, 2),
                    algorithm=algo,
                    duration_ms=round(duration, 4)
                ))
            except Exception as e:
                logger.warning(f"Compression {algo} failed: {e}")

        return sorted(results, key=lambda x: x.ratio)


class ArchiveManager:
    """Manages tar and zip archives."""

    @staticmethod
    def create_tar(files: Dict[str, bytes], output_path: str, compression: str = "") -> bool:
        mode = "w"
        if compression == "gz":
            mode = "w:gz"
        elif compression == "bz2":
            mode = "w:bz2"
        elif compression == "xz":
            mode = "w:xz"

        try:
            with tarfile.open(output_path, mode) as tar:
                for name, data in files.items():
                    import io
                    info = tarfile.TarInfo(name=name)
                    info.size = len(data)
                    tar.addfile(info, io.BytesIO(data))
            return True
        except Exception as e:
            logger.error(f"Failed to create tar: {e}")
            return False

    @staticmethod
    def extract_tar(input_path: str) -> Dict[str, bytes]:
        result = {}
        try:
            with tarfile.open(input_path, "r:*") as tar:
                for member in tar.getmembers():
                    if member.isfile():
                        f = tar.extractfile(member)
                        if f:
                            result[member.name] = f.read()
        except Exception as e:
            logger.error(f"Failed to extract tar: {e}")
        return result

    @staticmethod
    def create_zip(files: Dict[str, bytes], output_path: str) -> bool:
        try:
            with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for name, data in files.items():
                    zf.writestr(name, data)
            return True
        except Exception as e:
            logger.error(f"Failed to create zip: {e}")
            return False

    @staticmethod
    def extract_zip(input_path: str) -> Dict[str, bytes]:
        result = {}
        try:
            with zipfile.ZipFile(input_path, "r") as zf:
                for name in zf.namelist():
                    result[name] = zf.read(name)
        except Exception as e:
            logger.error(f"Failed to extract zip: {e}")
        return result


class CompressAction(BaseAction):
    """Compress data using various algorithms."""
    action_type = "data_compress"
    display_name = "数据压缩"
    description = "使用各种算法压缩数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", "")
        algorithm = params.get("algorithm", "gzip")
        level = params.get("level", 9)

        if isinstance(data, str):
            data = data.encode("utf-8")
        elif isinstance(data, dict):
            import json
            data = json.dumps(data).encode("utf-8")

        if not isinstance(data, bytes):
            return ActionResult(success=False, message="data必须是字符串或字节")

        original_size = len(data)

        try:
            import time
            start = time.time()

            if algorithm == "gzip":
                compressed = gzip.compress(data, compresslevel=level)
            elif algorithm == "zlib":
                compressed = zlib.compress(data, level=level)
            elif algorithm == "bz2":
                compressed = bz2.compress(data, compresslevel=level)
            elif algorithm == "lzma":
                compressed = lzma.compress(data, preset=level)
            else:
                return ActionResult(success=False, message=f"未知算法: {algorithm}")

            duration = (time.time() - start) * 1000
            ratio = len(compressed) / original_size if original_size > 0 else 0

            import base64
            compressed_b64 = base64.b64encode(compressed).decode("ascii")

            return ActionResult(
                success=True,
                message=f"压缩成功，压缩比: {ratio*100:.2f}%",
                data={
                    "algorithm": algorithm,
                    "original_size": original_size,
                    "compressed_size": len(compressed),
                    "ratio_percent": round(ratio * 100, 2),
                    "duration_ms": round(duration, 4),
                    "compressed_data": compressed_b64
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"压缩失败: {e}")


class DecompressAction(BaseAction):
    """Decompress data."""
    action_type = "data_decompress"
    display_name = "数据解压缩"
    description = "解压缩数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", "")
        algorithm = params.get("algorithm", "gzip")

        if isinstance(data, str):
            import base64
            try:
                data = base64.b64decode(data)
            except Exception:
                data = data.encode("utf-8")
        elif not isinstance(data, bytes):
            return ActionResult(success=False, message="data必须是字符串或字节")

        try:
            import time
            start = time.time()

            if algorithm == "gzip":
                decompressed = gzip.decompress(data)
            elif algorithm == "zlib":
                decompressed = zlib.decompress(data)
            elif algorithm == "bz2":
                decompressed = bz2.decompress(data)
            elif algorithm == "lzma":
                decompressed = lzma.decompress(data)
            else:
                return ActionResult(success=False, message=f"未知算法: {algorithm}")

            duration = (time.time() - start) * 1000

            try:
                result_str = decompressed.decode("utf-8")
                result_type = "string"
            except UnicodeDecodeError:
                result_str = decompressed.hex()
                result_type = "hex"

            return ActionResult(
                success=True,
                message=f"解压缩成功，耗时: {duration:.2f}ms",
                data={
                    "algorithm": algorithm,
                    "original_size": len(data),
                    "decompressed_size": len(decompressed),
                    "duration_ms": round(duration, 4),
                    "result": result_str,
                    "result_type": result_type
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"解压缩失败: {e}")


class CompressionAnalyzerAction(BaseAction):
    """Analyze compression efficiency."""
    action_type = "data_compression_analyzer"
    display_name = "压缩效率分析"
    description = "分析不同压缩算法的效率"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", "")
        algorithms = params.get("algorithms", ["gzip", "zlib", "bz2", "lzma"])

        if isinstance(data, str):
            data = data.encode("utf-8")
        elif not isinstance(data, bytes):
            return ActionResult(success=False, message="data必须是字符串或字节")

        results = CompressionAnalyzer.analyze(data, algorithms)

        if not results:
            return ActionResult(success=False, message="所有压缩算法均失败")

        best = results[0]

        return ActionResult(
            success=True,
            message=f"最佳算法: {best.algorithm} (压缩比: {best.ratio}%)",
            data={
                "results": [
                    {
                        "algorithm": r.algorithm,
                        "original_size": r.original_size,
                        "compressed_size": r.compressed_size,
                        "ratio_percent": r.ratio,
                        "duration_ms": r.duration_ms
                    }
                    for r in results
                ],
                "best_algorithm": best.algorithm
            }
        )


class ArchiveManagerAction(BaseAction):
    """Manage compressed archives."""
    action_type = "data_archive_manager"
    display_name = "归档管理器"
    description = "管理tar和zip压缩归档"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "create")
        format_type = params.get("format", "tar")
        files = params.get("files", {})
        archive_path = params.get("archive_path", "/tmp/archive")

        if operation == "create":
            if not files:
                return ActionResult(success=False, message="files是必需的")

            if format_type == "tar":
                success = ArchiveManager.create_tar(files, archive_path)
            elif format_type == "zip":
                success = ArchiveManager.create_zip(files, archive_path)
            else:
                return ActionResult(success=False, message=f"未知格式: {format_type}")

            if success:
                return ActionResult(success=True, message=f"归档已创建: {archive_path}")
            return ActionResult(success=False, message="归档创建失败")

        if operation == "extract":
            if format_type == "tar":
                extracted = ArchiveManager.extract_tar(archive_path)
            elif format_type == "zip":
                extracted = ArchiveManager.extract_zip(archive_path)
            else:
                return ActionResult(success=False, message=f"未知格式: {format_type}")

            return ActionResult(
                success=True,
                message=f"已提取 {len(extracted)} 个文件",
                data={"files": extracted, "count": len(extracted)}
            )

        if operation == "list":
            try:
                if format_type == "tar":
                    with tarfile.open(archive_path, "r:*") as tar:
                        members = tar.getnames()
                elif format_type == "zip":
                    with zipfile.ZipFile(archive_path, "r") as zf:
                        members = zf.namelist()
                else:
                    return ActionResult(success=False, message=f"未知格式: {format_type}")

                return ActionResult(
                    success=True,
                    message=f"归档包含 {len(members)} 个文件",
                    data={"members": members, "count": len(members)}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"列出归档失败: {e}")

        return ActionResult(success=False, message=f"未知操作: {operation}")
