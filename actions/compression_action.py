"""Compression and archive action module for RabAI AutoClick.

Provides compression/decompression operations:
- GzipCompressAction: Gzip compress data
- GzipDecompressAction: Gzip decompress data
- ZipCreateAction: Create ZIP archive
- ZipExtractAction: Extract ZIP archive
- TarCompressAction: Create tar archive
- TarExtractAction: Extract tar archive
- Bz2CompressAction: BZ2 compress
-Lz4CompressAction: LZ4 compress
"""

from __future__ import annotations

import gzip
import bz2
import zipfile
import tarfile
import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GzipCompressAction(BaseAction):
    """Gzip compress data."""
    action_type = "gzip_compress"
    display_name = "Gzip压缩"
    description = "Gzip压缩数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute gzip compress."""
        data = params.get('data', '')
        output_path = params.get('output_path', '')
        compression_level = params.get('compression_level', 9)

        if not data and not output_path:
            return ActionResult(success=False, message="data or output_path is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_output = context.resolve_value(output_path) if context else output_path
            resolved_level = context.resolve_value(compression_level) if context else compression_level

            if isinstance(resolved_data, str):
                resolved_data = resolved_data.encode('utf-8')

            if resolved_output:
                _os.makedirs(_os.path.dirname(resolved_output) or '.', exist_ok=True)
                with gzip.open(resolved_output, 'wb', compresslevel=resolved_level) as f:
                    f.write(resolved_data)
                return ActionResult(success=True, message=f"Compressed to {resolved_output}", data={'output_path': resolved_output})
            else:
                compressed = gzip.compress(resolved_data, compresslevel=resolved_level)
                import base64
                encoded = base64.b64encode(compressed).decode('ascii')
                return ActionResult(success=True, message=f"Compressed {len(resolved_data)} -> {len(compressed)} bytes", data={'data': encoded})
        except Exception as e:
            return ActionResult(success=False, message=f"Gzip compress error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'data': '', 'output_path': '', 'compression_level': 9}


class GzipDecompressAction(BaseAction):
    """Gzip decompress data."""
    action_type = "gzip_decompress"
    display_name = "Gzip解压"
    description = "Gzip解压数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute gzip decompress."""
        data = params.get('data', '')
        input_path = params.get('input_path', '')
        output_var = params.get('output_var', 'decompressed_data')

        if not data and not input_path:
            return ActionResult(success=False, message="data or input_path is required")

        try:
            if input_path:
                resolved_path = context.resolve_value(input_path) if context else input_path
                with gzip.open(resolved_path, 'rb') as f:
                    decompressed = f.read()
            else:
                resolved_data = context.resolve_value(data) if context else data
                if isinstance(resolved_data, str):
                    import base64
                    resolved_data = base64.b64decode(resolved_data)
                decompressed = gzip.decompress(resolved_data)

            try:
                text_result = decompressed.decode('utf-8')
            except UnicodeDecodeError:
                text_result = decompressed.hex()

            if context:
                context.set(output_var, text_result)
            return ActionResult(success=True, message=f"Decompressed {len(decompressed)} bytes", data={'data': text_result, 'bytes': len(decompressed)})
        except Exception as e:
            return ActionResult(success=False, message=f"Gzip decompress error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'data': '', 'input_path': '', 'output_var': 'decompressed_data'}


class ZipCreateAction(BaseAction):
    """Create ZIP archive."""
    action_type = "zip_create"
    display_name = "创建ZIP"
    description = "创建ZIP压缩包"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute ZIP create."""
        output_path = params.get('output_path', '')
        files = params.get('files', [])  # list of {path, arcname} or just paths
        compression = params.get('compression', 'deflated')

        if not output_path or not files:
            return ActionResult(success=False, message="output_path and files are required")

        try:
            resolved_output = context.resolve_value(output_path) if context else output_path
            resolved_files = context.resolve_value(files) if context else files

            _os.makedirs(_os.path.dirname(resolved_output) or '.', exist_ok=True)

            comp_map = {'stored': zipfile.ZIP_STORED, 'deflated': zipfile.ZIP_DEFLATED, 'bzip2': zipfile.ZIP_BZIP2}
            comp_type = comp_map.get(compression.lower(), zipfile.ZIP_DEFLATED)

            with zipfile.ZipFile(resolved_output, 'w', compression=comp_type) as zf:
                for item in resolved_files:
                    if isinstance(item, dict):
                        file_path = item.get('path', '')
                        arcname = item.get('arcname', _os.path.basename(file_path))
                    else:
                        file_path = item
                        arcname = _os.path.basename(file_path)
                    zf.write(file_path, arcname=arcname)

            return ActionResult(success=True, message=f"Created {resolved_output} with {len(resolved_files)} files", data={'output_path': resolved_output, 'files_count': len(resolved_files)})
        except Exception as e:
            return ActionResult(success=False, message=f"ZIP create error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['output_path', 'files']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'compression': 'deflated'}


class ZipExtractAction(BaseAction):
    """Extract ZIP archive."""
    action_type = "zip_extract"
    display_name = "解压ZIP"
    description = "解压ZIP压缩包"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute ZIP extract."""
        input_path = params.get('input_path', '')
        output_dir = params.get('output_dir', '.')
        filter_ext = params.get('filter_ext', None)  # e.g. '.txt' to extract only those

        if not input_path:
            return ActionResult(success=False, message="input_path is required")

        try:
            resolved_path = context.resolve_value(input_path) if context else input_path
            resolved_output = context.resolve_value(output_dir) if context else output_dir
            resolved_filter = context.resolve_value(filter_ext) if context else filter_ext

            _os.makedirs(resolved_output, exist_ok=True)
            extracted_count = 0

            with zipfile.ZipFile(resolved_path, 'r') as zf:
                for info in zf.infolist():
                    if resolved_filter and not info.filename.endswith(resolved_filter):
                        continue
                    zf.extract(info, path=resolved_output)
                    extracted_count += 1

            return ActionResult(success=True, message=f"Extracted {extracted_count} files to {resolved_output}", data={'extracted_count': extracted_count, 'output_dir': resolved_output})
        except Exception as e:
            return ActionResult(success=False, message=f"ZIP extract error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['input_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_dir': '.', 'filter_ext': None}


class TarCompressAction(BaseAction):
    """Create tar archive."""
    action_type = "tar_compress"
    display_name = "创建tar"
    description = "创建tar归档"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute tar create."""
        output_path = params.get('output_path', '')
        files = params.get('files', [])
        mode = params.get('mode', 'w:gz')  # w:gz, w:bz2, w:
        output_var = params.get('output_var', 'tar_result')

        if not output_path or not files:
            return ActionResult(success=False, message="output_path and files are required")

        try:
            resolved_output = context.resolve_value(output_path) if context else output_path
            resolved_files = context.resolve_value(files) if context else files

            _os.makedirs(_os.path.dirname(resolved_output) or '.', exist_ok=True)

            mode_map = {'w': 'w', 'w:gz': 'w:gz', 'w:bz2': 'w:bz2', 'w:xz': 'w:xz', 'w:zst': 'w:zst'}
            tar_mode = mode_map.get(mode, 'w:gz')

            with tarfile.open(resolved_output, tar_mode) as tf:
                for item in resolved_files:
                    if isinstance(item, dict):
                        file_path = item.get('path', '')
                        arcname = item.get('arcname', None)
                    else:
                        file_path = item
                        arcname = None
                    tf.add(file_path, arcname=arcname)

            result = {'output_path': resolved_output, 'files_count': len(resolved_files), 'mode': tar_mode}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Created {resolved_output}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"tar create error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['output_path', 'files']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'mode': 'w:gz', 'output_var': 'tar_result'}


class TarExtractAction(BaseAction):
    """Extract tar archive."""
    action_type = "tar_extract"
    display_name = "解压tar"
    description = "解压tar归档"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute tar extract."""
        input_path = params.get('input_path', '')
        output_dir = params.get('output_dir', '.')

        if not input_path:
            return ActionResult(success=False, message="input_path is required")

        try:
            resolved_path = context.resolve_value(input_path) if context else input_path
            resolved_output = context.resolve_value(output_dir) if context else output_dir

            _os.makedirs(resolved_output, exist_ok=True)
            with tarfile.open(resolved_path, 'r:*') as tf:
                members = tf.getmembers()
                tf.extractall(path=resolved_output)

            return ActionResult(success=True, message=f"Extracted {len(members)} files to {resolved_output}", data={'extracted_count': len(members), 'output_dir': resolved_output})
        except Exception as e:
            return ActionResult(success=False, message=f"tar extract error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['input_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_dir': '.'}


class Bz2CompressAction(BaseAction):
    """BZ2 compress/decompress."""
    action_type = "bz2_compress"
    display_name = "BZ2压缩"
    description = "BZ2压缩数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute BZ2 compress."""
        data = params.get('data', '')
        output_path = params.get('output_path', '')
        decompress = params.get('decompress', False)
        output_var = params.get('output_var', 'bz2_result')

        if not data and not output_path:
            return ActionResult(success=False, message="data or output_path is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_output = context.resolve_value(output_path) if context else output_path
            resolved_decompress = context.resolve_value(decompress) if context else decompress

            if isinstance(resolved_data, str):
                resolved_data = resolved_data.encode('utf-8')

            if resolved_decompress:
                if resolved_output:
                    with bz2.open(resolved_output, 'rb') as f:
                        result = f.read()
                    return ActionResult(success=True, message=f"Decompressed to {resolved_output}", data={'output_path': resolved_output})
                else:
                    decompressed = bz2.decompress(resolved_data)
                    try:
                        text = decompressed.decode('utf-8')
                    except UnicodeDecodeError:
                        text = decompressed.hex()
                    if context:
                        context.set(output_var, text)
                    return ActionResult(success=True, message=f"Decompressed {len(decompressed)} bytes", data={'data': text})
            else:
                compressed = bz2.compress(resolved_data)
                if resolved_output:
                    with bz2.open(resolved_output, 'wb') as f:
                        f.write(resolved_data)
                    return ActionResult(success=True, message=f"Compressed to {resolved_output}", data={'output_path': resolved_output})
                else:
                    import base64
                    encoded = base64.b64encode(compressed).decode('ascii')
                    return ActionResult(success=True, message=f"Compressed {len(resolved_data)} -> {len(compressed)} bytes", data={'data': encoded})
        except Exception as e:
            return ActionResult(success=False, message=f"BZ2 error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'data': '', 'output_path': '', 'decompress': False, 'output_var': 'bz2_result'}
