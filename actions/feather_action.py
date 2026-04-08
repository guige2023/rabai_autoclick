"""Feather action module for RabAI AutoClick.

Provides Apache Feather/Arrow file operations for fast data interchange.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FeatherAction(BaseAction):
    """Apache Feather/Arrow file operations.
    
    Supports reading and writing Feather (Arrow) format files
    with zero-copy performance.
    """
    action_type = "feather"
    display_name = "Feather文件操作"
    description = "Apache Arrow/Feather高速数据格式读写"
    
    def __init__(self) -> None:
        super().__init__()
    
    def _get_feather_lib(self):
        """Get pyarrow.feather or pandas."""
        try:
            import pyarrow.feather
            return 'pyarrow'
        except ImportError:
            try:
                import pandas
                return 'pandas'
            except ImportError:
                return None
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Feather operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'read', 'write', 'info'
                - file_path: Path to Feather file
                - output_path: Output file path (for write)
                - data: Data to write (list of dicts)
                - compression: Compression ('zstd', 'lz4', 'uncompressed')
        
        Returns:
            ActionResult with operation result.
        """
        lib = self._get_feather_lib()
        if lib is None:
            return ActionResult(
                success=False,
                message="Requires pyarrow or pandas. Install: pip install pyarrow"
            )
        
        command = params.get('command', 'read')
        file_path = params.get('file_path')
        output_path = params.get('output_path')
        data = params.get('data')
        compression = params.get('compression', 'zstd')
        
        if command == 'read':
            if not file_path:
                return ActionResult(success=False, message="file_path required for read")
            return self._read_feather(lib, file_path)
        
        if command == 'write':
            if not output_path:
                return ActionResult(success=False, message="output_path required for write")
            if data is None:
                return ActionResult(success=False, message="data required for write")
            return self._write_feather(lib, output_path, data, compression)
        
        if command == 'info':
            if not file_path:
                return ActionResult(success=False, message="file_path required for info")
            return self._feather_info(lib, file_path)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _read_feather(self, lib: str, file_path: str) -> ActionResult:
        """Read Feather file."""
        try:
            if lib == 'pyarrow':
                import pyarrow.feather as feather
                table = feather.read_table(file_path)
                df = table.to_pandas()
            else:
                import pandas as pd
                df = pd.read_feather(file_path)
            
            return ActionResult(
                success=True,
                message=f"Read {len(df)} rows from {file_path}",
                data={
                    'rows': len(df),
                    'columns': list(df.columns),
                    'data': df.to_dict('records')
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to read Feather: {e}")
    
    def _write_feather(self, lib: str, output_path: str, data: Any, compression: str) -> ActionResult:
        """Write Feather file."""
        try:
            import pandas as pd
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data]) if not any(isinstance(v, list) for v in data.values()) else pd.DataFrame(data)
            else:
                return ActionResult(success=False, message="data must be list or dict")
            
            comp = compression if compression != 'uncompressed' else None
            if lib == 'pyarrow':
                import pyarrow.feather as feather
                table = pyarrow.Table.from_pandas(df)
                feather.write_table(table, output_path, compression=comp)
            else:
                df.to_feather(output_path, compression=comp)
            
            return ActionResult(
                success=True,
                message=f"Wrote {len(df)} rows to {output_path}",
                data={'rows': len(df), 'columns': list(df.columns), 'file': output_path}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to write Feather: {e}")
    
    def _feather_info(self, lib: str, file_path: str) -> ActionResult:
        """Get Feather file metadata."""
        try:
            import os
            size = os.path.getsize(file_path)
            if lib == 'pyarrow':
                import pyarrow.feather as feather
                table = feather.read_table(file_path)
                return ActionResult(
                    success=True,
                    message=f"Feather file: {file_path}",
                    data={
                        'rows': table.num_rows,
                        'columns': table.num_columns,
                        'column_names': table.column_names,
                        'file_size_bytes': size
                    }
                )
            else:
                import pandas as pd
                df = pd.read_feather(file_path)
                return ActionResult(
                    success=True,
                    message=f"Feather file: {file_path}",
                    data={
                        'rows': len(df),
                        'columns': len(df.columns),
                        'column_names': list(df.columns),
                        'file_size_bytes': size
                    }
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to get Feather info: {e}")
