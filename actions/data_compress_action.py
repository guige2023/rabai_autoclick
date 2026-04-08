"""Data Compress Action.

Compresses and decompresses data using various algorithms (gzip, bz2, lzma, zstd)
with configurable compression levels and streaming support.
"""

import sys
import os
import gzip
import bz2
import lzma
import zlib
from typing import Any, Dict, List, Optional
from io import BytesIO

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataCompressAction(BaseAction):
    """Compress and decompress data using various algorithms.
    
    Supports gzip, bz2, lzma, zstd with configurable compression
    levels and both memory and file-based operations.
    """
    action_type = "data_compress"
    display_name = "数据压缩"
    description = "数据压缩与解压，支持gzip/bz2/lzma/zstd算法"

    SUPPORTED_ALGORITHMS = ['gzip', 'bz2', 'lzma', 'zlib', 'zstd']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compress or decompress data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'compress', 'decompress', 'test'.
                - data: Data to compress (string, bytes, or variable name).
                - algorithm: Compression algorithm (gzip, bz2, lzma, zlib, zstd).
                - level: Compression level 1-9 (default: 6).
                - source_file: Source file to compress/decompress.
                - destination_file: Output file path.
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with compression results.
        """
        try:
            action = params.get('action', 'compress')
            save_to_var = params.get('save_to_var', 'compress_result')

            if action == 'compress':
                return self._compress(context, params, save_to_var)
            elif action == 'decompress':
                return self._decompress(context, params, save_to_var)
            elif action == 'test':
                return self._test_compression(context, params, save_to_var)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Compression error: {e}")

    def _compress(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Compress data."""
        algorithm = params.get('algorithm', 'gzip').lower()
        level = params.get('level', 6)
        data = params.get('data')
        source_file = params.get('source_file')
        destination_file = params.get('destination_file')
        use_var = params.get('use_var', 'input_data')

        # Get data to compress
        if data is None and source_file:
            if not os.path.exists(source_file):
                return ActionResult(success=False, message=f"Source file not found: {source_file}")
            with open(source_file, 'rb') as f:
                data = f.read()
        elif data is None:
            data = context.get_variable(use_var)
            if data is None:
                return ActionResult(success=False, message="No data provided")

        if isinstance(data, str):
            data = data.encode('utf-8')

        # Compress
        if algorithm == 'gzip':
            compressed = self._gzip_compress(data, level)
        elif algorithm == 'bz2':
            compressed = self._bz2_compress(data, level)
        elif algorithm == 'lzma':
            compressed = self._lzma_compress(data, level)
        elif algorithm == 'zlib':
            compressed = self._zlib_compress(data, level)
        elif algorithm == 'zstd':
            compressed = self._zstd_compress(data, level)
        else:
            return ActionResult(success=False, message=f"Unknown algorithm: {algorithm}")

        # Save or return
        if destination_file:
            with open(destination_file, 'wb') as f:
                f.write(compressed)
            result = {
                'algorithm': algorithm,
                'original_size': len(data),
                'compressed_size': len(compressed),
                'ratio': len(data) / len(compressed) if len(compressed) > 0 else 0,
                'destination': destination_file
            }
        else:
            import base64
            result = {
                'algorithm': algorithm,
                'original_size': len(data),
                'compressed_size': len(compressed),
                'ratio': len(data) / len(compressed) if len(compressed) > 0 else 0,
                'data': compressed,
                'base64': base64.b64encode(compressed).decode('ascii')
            }

        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result,
                           message=f"Compressed {len(data)} -> {len(compressed)} bytes ({result['ratio']:.2f}x)")

    def _decompress(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Decompress data."""
        algorithm = params.get('algorithm', 'gzip').lower()
        data = params.get('data')
        source_file = params.get('source_file')
        destination_file = params.get('destination_file')
        use_var = params.get('use_var', 'compressed_data')

        # Get data to decompress
        if data is None and source_file:
            if not os.path.exists(source_file):
                return ActionResult(success=False, message=f"Source file not found: {source_file}")
            with open(source_file, 'rb') as f:
                data = f.read()
        elif data is None:
            data = context.get_variable(use_var)
            if data is None:
                return ActionResult(success=False, message="No data provided")

        if isinstance(data, str):
            import base64
            data = base64.b64decode(data)

        # Decompress
        try:
            if algorithm == 'gzip':
                decompressed = self._gzip_decompress(data)
            elif algorithm == 'bz2':
                decompressed = self._bz2_decompress(data)
            elif algorithm == 'lzma':
                decompressed = self._lzma_decompress(data)
            elif algorithm == 'zlib':
                decompressed = self._zlib_decompress(data)
            elif algorithm == 'zstd':
                decompressed = self._zstd_decompress(data)
            else:
                return ActionResult(success=False, message=f"Unknown algorithm: {algorithm}")
        except Exception as e:
            return ActionResult(success=False, message=f"Decompression failed: {e}")

        # Save or return
        if destination_file:
            with open(destination_file, 'wb') as f:
                f.write(decompressed)
            result = {
                'algorithm': algorithm,
                'compressed_size': len(data),
                'decompressed_size': len(decompressed),
                'destination': destination_file
            }
        else:
            try:
                text = decompressed.decode('utf-8')
                result = {
                    'algorithm': algorithm,
                    'compressed_size': len(data),
                    'decompressed_size': len(decompressed),
                    'data': text
                }
            except UnicodeDecodeError:
                import base64
                result = {
                    'algorithm': algorithm,
                    'compressed_size': len(data),
                    'decompressed_size': len(decompressed),
                    'data': decompressed,
                    'base64': base64.b64encode(decompressed).decode('ascii')
                }

        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result,
                           message=f"Decompressed {len(data)} -> {len(decompressed)} bytes")

    def _test_compression(self, context: Any, params: Dict, save_to_var: str) -> ActionResult:
        """Test compression with different algorithms."""
        data = params.get('data') or context.get_variable(params.get('use_var', 'test_data'))
        
        if not data:
            return ActionResult(success=False, message="No data provided")

        if isinstance(data, str):
            data = data.encode('utf-8')

        results = {}
        for algo in self.SUPPORTED_ALGORITHMS:
            try:
                if algo == 'gzip':
                    compressed = self._gzip_compress(data, 6)
                elif algo == 'bz2':
                    compressed = self._bz2_compress(data, 6)
                elif algo == 'lzma':
                    compressed = self._lzma_compress(data, 6)
                elif algo == 'zlib':
                    compressed = self._zlib_compress(data, 6)
                elif algo == 'zstd':
                    compressed = self._zstd_compress(data, 6)
                
                results[algo] = {
                    'compressed_size': len(compressed),
                    'ratio': len(data) / len(compressed) if len(compressed) > 0 else float('inf'),
                    'supported': True
                }
            except Exception as e:
                results[algo] = {'supported': False, 'error': str(e)}

        # Find best
        best = min((r for r in results.values() if r.get('supported')), 
                  key=lambda r: r.get('compressed_size', float('inf')))

        result = {'original_size': len(data), 'algorithms': results, 'best': best}
        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result)

    def _gzip_compress(self, data: bytes, level: int) -> bytes:
        """Compress with gzip."""
        level = max(1, min(level, 9))
        buf = BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=level) as f:
            f.write(data)
        return buf.getvalue()

    def _gzip_decompress(self, data: bytes) -> bytes:
        """Decompress gzip."""
        with gzip.GzipFile(fileobj=BytesIO(data), mode='rb') as f:
            return f.read()

    def _bz2_compress(self, data: bytes, level: int) -> bytes:
        """Compress with bz2."""
        level = max(1, min(level, 9))
        return bz2.compress(data, compressionlevel=level)

    def _bz2_decompress(self, data: bytes) -> bytes:
        """Decompress bz2."""
        return bz2.decompress(data)

    def _lzma_compress(self, data: bytes, level: int) -> bytes:
        """Compress with lzma."""
        preset = min(max(level, 0), 9)
        return lzma.compress(data, preset=preset)

    def _lzma_decompress(self, data: bytes) -> bytes:
        """Decompress lzma."""
        return lzma.decompress(data)

    def _zlib_compress(self, data: bytes, level: int) -> bytes:
        """Compress with zlib."""
        level = max(1, min(level, 9))
        return zlib.compress(data, level=level)

    def _zlib_decompress(self, data: bytes) -> bytes:
        """Decompress zlib."""
        return zlib.decompress(data)

    def _zstd_compress(self, data: bytes, level: int) -> bytes:
        """Compress with zstd."""
        try:
            import zstandard as zstd
            cctx = zstd.ZstdCompressor(level=level)
            return cctx.compress(data)
        except ImportError:
            # Fallback to zlib if zstd not available
            return self._zlib_compress(data, level)

    def _zstd_decompress(self, data: bytes) -> bytes:
        """Decompress zstd."""
        try:
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor()
            return dctx.decompress(data)
        except ImportError:
            return self._zlib_decompress(data)
