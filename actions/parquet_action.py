"""Parquet action module for RabAI AutoClick.

Provides Apache Parquet file operations:
- ParquetReader: Read Parquet files with schema inference
- ParquetWriter: Write data to Parquet files
- ParquetSchema: Schema inspection and validation
- ParquetPartition: Handle partitioned Parquet datasets
- ParquetCompressor: Configure compression codecs
"""

from __future__ import annotations

import os
import sys
import json
import gzip
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


@dataclass
class ParquetSchema:
    """Parquet schema descriptor."""
    schema_name: str = ""
    fields: List[Dict[str, Any]] = field(default_factory=list)
    row_groups: int = 0
    num_rows: int = 0
    num_columns: int = 0
    created_at: str = ""


@dataclass
class ParquetOptions:
    """Options for Parquet operations."""
    compression: str = "snappy"
    use_dictionary: bool = True
    write_statistics: bool = True
    use_deprecated_int96_timestamps: bool = False
    flavor: str = "spark"


class ParquetReaderAction(BaseAction):
    """Read Parquet files and extract data."""
    action_type = "parquet_reader"
    display_name = "Parquet读取"
    description = "读取Parquet文件并提取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not PYARROW_AVAILABLE:
            return ActionResult(success=False, message="pyarrow not installed: pip install pyarrow")

        try:
            file_path = params.get("file_path", "")
            columns = params.get("columns", None)
            row_group = params.get("row_group", None)
            batch_size = params.get("batch_size", 10000)
            filters = params.get("filters", None)
            as_pandas = params.get("as_pandas", False)

            if not file_path:
                return ActionResult(success=False, message="file_path is required")

            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")

            try:
                if row_group is not None:
                    table = pq.read_table(file_path, row_group=row_group)
                else:
                    table = pq.read_table(file_path, columns=columns, filters=filters)

                if as_pandas:
                    df = table.to_pandas()
                    return ActionResult(
                        success=True,
                        message=f"Read {len(df)} rows",
                        data={"rows": len(df), "columns": list(df.columns), "data": df.to_dict(orient="records")}
                    )

                return ActionResult(
                    success=True,
                    message=f"Read {table.num_rows} rows, {table.num_columns} columns",
                    data={
                        "num_rows": table.num_rows,
                        "num_columns": table.num_columns,
                        "column_names": table.column_names,
                        "schema": str(table.schema),
                    }
                )
            except Exception as e:
                return ActionResult(success=False, message=f"Read error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ParquetWriterAction(BaseAction):
    """Write data to Parquet files."""
    action_type = "parquet_writer"
    display_name = "Parquet写入"
    description = "将数据写入Parquet文件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not PYARROW_AVAILABLE:
            return ActionResult(success=False, message="pyarrow not installed: pip install pyarrow")

        try:
            file_path = params.get("file_path", "")
            data = params.get("data", [])
            compression = params.get("compression", "snappy")
            use_dictionary = params.get("use_dictionary", True)
            write_statistics = params.get("write_statistics", True)
            overwrite = params.get("overwrite", True)

            if not file_path:
                return ActionResult(success=False, message="file_path is required")

            if not data:
                return ActionResult(success=False, message="data is required")

            os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)

            if overwrite and os.path.exists(file_path):
                os.remove(file_path)

            try:
                if isinstance(data, list) and len(data) > 0:
                    if isinstance(data[0], dict):
                        import pandas as pd
                        df = pd.DataFrame(data)
                        table = pa.Table.from_pandas(df)
                    else:
                        return ActionResult(success=False, message="Data must be list of dicts")
                elif hasattr(data, "to_parquet"):
                    table = data
                else:
                    return ActionResult(success=False, message="Unsupported data format")

                options = ParquetOptions(compression=compression)
                writer = pq.ParquetWriter(
                    file_path,
                    table.schema,
                    compression=compression,
                    use_dictionary=use_dictionary,
                    write_statistics=write_statistics,
                )
                writer.write_table(table)
                writer.close()

                file_size = os.path.getsize(file_path)
                return ActionResult(
                    success=True,
                    message=f"Wrote {table.num_rows} rows to {file_path}",
                    data={"file_path": file_path, "rows": table.num_rows, "file_size": file_size}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"Write error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ParquetSchemaAction(BaseAction):
    """Inspect Parquet file schema."""
    action_type = "parquet_schema"
    display_name = "ParquetSchema查询"
    description = "查询Parquet文件schema信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not PYARROW_AVAILABLE:
            return ActionResult(success=False, message="pyarrow not installed: pip install pyarrow")

        try:
            file_path = params.get("file_path", "")
            if not file_path:
                return ActionResult(success=False, message="file_path is required")

            if not os.path.exists(file_path):
                return ActionResult(success=False, message=f"File not found: {file_path}")

            pf = pq.ParquetFile(file_path)
            schema = pf.schema_arrow

            fields = []
            for field in schema:
                fields.append({
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": field.nullable,
                })

            return ActionResult(
                success=True,
                message=f"Schema: {len(fields)} fields",
                data={
                    "num_fields": len(fields),
                    "num_rows": pf.metadata.num_rows,
                    "num_row_groups": pf.metadata.num_row_groups,
                    "format_version": pf.format_version,
                    "fields": fields,
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ParquetPartitionAction(BaseAction):
    """Handle partitioned Parquet datasets."""
    action_type = "parquet_partition"
    display_name = "Parquet分区处理"
    description = "处理Parquet分区数据集"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not PYARROW_AVAILABLE:
            return ActionResult(success=False, message="pyarrow not installed: pip install pyarrow")

        try:
            base_path = params.get("base_path", "")
            partition_cols = params.get("partition_cols", [])
            filters = params.get("filters", None)

            if not base_path:
                return ActionResult(success=False, message="base_path is required")

            if not os.path.exists(base_path):
                return ActionResult(success=False, message=f"Path not found: {base_path}")

            dataset = pq.ParquetDataset(base_path, filters=filters)
            table = dataset.read()

            partitions = {}
            if dataset.partitioning:
                for field in dataset.partitioning.schema:
                    partitions[field.name] = str(field.type)

            return ActionResult(
                success=True,
                message=f"Read partitioned dataset: {table.num_rows} rows",
                data={
                    "num_rows": table.num_rows,
                    "num_columns": table.num_columns,
                    "partition_columns": partition_cols,
                    "partition_info": partitions,
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ParquetCompressAction(BaseAction):
    """Compress and decompress Parquet files."""
    action_type = "parquet_compress"
    display_name = "Parquet压缩"
    description = "Parquet文件压缩与解压缩"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not PYARROW_AVAILABLE:
            return ActionResult(success=False, message="pyarrow not installed: pip install pyarrow")

        try:
            input_path = params.get("input_path", "")
            output_path = params.get("output_path", "")
            codec = params.get("codec", "snappy")
            compression_level = params.get("compression_level", None)

            if not input_path:
                return ActionResult(success=False, message="input_path is required")
            if not os.path.exists(input_path):
                return ActionResult(success=False, message=f"File not found: {input_path}")

            if not output_path:
                output_path = input_path.replace(".parquet", f".{codec}.parquet")

            valid_codecs = ["snappy", "gzip", "brotli", "zstd", "lz4", "none"]
            if codec not in valid_codecs:
                return ActionResult(success=False, message=f"Invalid codec. Use: {valid_codecs}")

            table = pq.read_table(input_path)
            writer = pq.ParquetWriter(
                output_path,
                table.schema,
                compression=codec if codec != "none" else None,
            )
            writer.write_table(table)
            writer.close()

            orig_size = os.path.getsize(input_path)
            new_size = os.path.getsize(output_path)
            ratio = (1 - new_size / orig_size) * 100 if orig_size > 0 else 0

            return ActionResult(
                success=True,
                message=f"Compressed: {orig_size} -> {new_size} bytes ({ratio:.1f}% reduction)",
                data={"input_size": orig_size, "output_size": new_size, "ratio": ratio, "codec": codec}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
