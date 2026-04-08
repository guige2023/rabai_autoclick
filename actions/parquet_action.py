"""Parquet action module for RabAI AutoClick.

Provides Apache Parquet file read/write operations with schema support.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ParquetAction(BaseAction):
    """Apache Parquet file operations.
    
    Supports reading and writing Parquet files with schema inference,
    column selection, filtering, and compression options.
    """
    action_type = "parquet"
    display_name = "Parquet文件操作"
    description = "读写Parquet文件，支持Schema和压缩"
    
    def __init__(self) -> None:
        super().__init__()
    
    def _get_parquet_lib(self):
        """Get pyarrow or pandas with parquet support."""
        try:
            import pyarrow
            return 'pyarrow'
        except ImportError:
            try:
                import pandas
                return 'pandas'
            except ImportError:
                return None
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Parquet operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'read', 'write', 'info', 'convert'
                - file_path: Path to Parquet file
                - columns: List of columns to read (optional)
                - filter: Filter expression (optional)
                - output_path: Output file path (for write/convert)
                - compression: Compression codec ('snappy', 'gzip', 'brotli', 'none')
                - data: Data to write (list of dicts or dict with lists)
        
        Returns:
            ActionResult with operation result.
        """
        lib = self._get_parquet_lib()
        if lib is None:
            return ActionResult(
                success=False,
                message="Requires pyarrow or pandas. Install: pip install pyarrow"
            )
        
        command = params.get('command', 'read')
        file_path = params.get('file_path')
        columns = params.get('columns')
        filter_expr = params.get('filter')
        output_path = params.get('output_path')
        compression = params.get('compression', 'snappy')
        data = params.get('data')
        
        if command == 'read':
            if not file_path:
                return ActionResult(success=False, message="file_path is required for read")
            return self._read_parquet(lib, file_path, columns, filter_expr)
        
        if command == 'write':
            if not output_path:
                return ActionResult(success=False, message="output_path is required for write")
            if data is None:
                return ActionResult(success=False, message="data is required for write")
            return self._write_parquet(lib, output_path, data, compression)
        
        if command == 'info':
            if not file_path:
                return ActionResult(success=False, message="file_path is required for info")
            return self._parquet_info(lib, file_path)
        
        if command == 'convert':
            if not file_path or not output_path:
                return ActionResult(success=False, message="file_path and output_path required for convert")
            return self._convert_parquet(lib, file_path, output_path, compression)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _read_parquet(self, lib: str, file_path: str, columns: Optional[List[str]], filter_expr: Optional[str]) -> ActionResult:
        """Read Parquet file."""
        try:
            if lib == 'pyarrow':
                import pyarrow.parquet as pq
                table = pq.read_table(file_path, columns=columns)
                df = table.to_pandas()
            else:
                import pandas as pd
                df = pd.read_parquet(file_path, columns=columns)
            
            if filter_expr:
                df = df.query(filter_expr)
            
            return ActionResult(
                success=True,
                message=f"Read {len(df)} rows, {len(df.columns)} columns from {file_path}",
                data={
                    'rows': len(df),
                    'columns': list(df.columns),
                    'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                    'data': df.to_dict('records')
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to read Parquet: {e}")
    
    def _write_parquet(self, lib: str, output_path: str, data: Any, compression: str) -> ActionResult:
        """Write Parquet file."""
        try:
            import pandas as pd
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                if all(isinstance(v, list) for v in data.values()):
                    df = pd.DataFrame(data)
                else:
                    df = pd.DataFrame([data])
            else:
                return ActionResult(success=False, message="data must be list of dicts or dict of lists")
            
            comp = None if compression == 'none' else compression
            df.to_parquet(output_path, compression=comp, index=False)
            return ActionResult(
                success=True,
                message=f"Wrote {len(df)} rows to {output_path}",
                data={'rows': len(df), 'columns': list(df.columns), 'file': output_path}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to write Parquet: {e}")
    
    def _parquet_info(self, lib: str, file_path: str) -> ActionResult:
        """Get Parquet file metadata."""
        try:
            if lib == 'pyarrow':
                import pyarrow.parquet as pq
                metadata = pq.read_metadata(file_path)
                return ActionResult(
                    success=True,
                    message=f"Parquet file: {file_path}",
                    data={
                        'num_rows': metadata.num_rows,
                        'num_columns': metadata.num_columns,
                        'num_row_groups': metadata.num_row_groups,
                        'format_version': metadata.format_version,
                        'created_by': metadata.created_by,
                        'schema': str(metadata.schema)
                    }
                )
            else:
                import pandas as pd
                df = pd.read_parquet(file_path)
                return ActionResult(
                    success=True,
                    message=f"Parquet file: {file_path}",
                    data={
                        'num_rows': len(df),
                        'num_columns': len(df.columns),
                        'columns': list(df.columns),
                        'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
                    }
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to get Parquet info: {e}")
    
    def _convert_parquet(self, lib: str, file_path: str, output_path: str, compression: str) -> ActionResult:
        """Convert Parquet file with new compression."""
        try:
            import pandas as pd
            df = pd.read_parquet(file_path)
            comp = None if compression == 'none' else compression
            df.to_parquet(output_path, compression=comp, index=False)
            return ActionResult(
                success=True,
                message=f"Converted {file_path} -> {output_path} (compression={compression})",
                data={'input': file_path, 'output': output_path, 'compression': compression}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to convert Parquet: {e}")
