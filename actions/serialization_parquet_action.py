"""
Parquet serialization action for efficient columnar data storage.

This module provides actions for reading and writing Apache Parquet files,
supporting compression, encoding options, and schema evolution.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

try:
    import pyarrow as pa
    import pyarrow.csv as pa_csv
    import pyarrow.feather as pa_feather
    import pyarrow.json as pa_json
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class CompressionType(Enum):
    """Supported compression codecs for Parquet files."""
    UNCOMPRESSED = "uncompressed"
    SNAPPY = "snappy"
    GZIP = "gzip"
    BROTLI = "brotli"
    ZSTD = "zstd"
    LZ4 = "lz4"


class EncodingType(Enum):
    """Supported encoding types for Parquet columns."""
    PLAIN = "plain"
    PLAIN_DICTIONARY = "plain_dictionary"
    RLE = "rle"
    RLE_DICTIONARY = "rle_dictionary"
    BIT_PACKED = "bit_packed"
    DELTA_BINARY_PACKED = "delta_binary_packed"
    DELTA_LENGTH_BYTE_ARRAY = "delta_length_byte_array"
    DELTA_BYTE_ARRAY = "delta_byte_array"
    DELTA_FP_SUM = "delta_fp_sum"


@dataclass
class ParquetColumnConfig:
    """Configuration for a single Parquet column."""
    name: str
    pyarrow_type: Optional[str] = None
    compression: Optional[CompressionType] = None
    encoding: Optional[EncodingType] = None
    dictionary_encoding: bool = False
    statistics: bool = True


@dataclass
class ParquetWriteOptions:
    """Options for writing Parquet files."""
    compression: CompressionType = CompressionType.SNAPPY
    use_dictionary: bool = True
    use_arrow_schema: bool = True
    compression_level: Optional[int] = None
    row_group_size: Optional[int] = None
    data_page_size: Optional[int] = None
    write_statistics: bool = True
    int96_timestamp: bool = False
    coerce_timestamps: Optional[str] = None
    allow_truncated_timestamps: bool = False
    columns: Optional[List[str]] = None
    exclude_columns: Optional[List[str]] = None
    metadata: Optional[Dict[str, str]] = None
    custom_metadata: Optional[Dict[str, bytes]] = None

    def to_pyarrow_options(self) -> Dict[str, Any]:
        """Convert to pyarrow parquet write options."""
        opts: Dict[str, Any] = {
            "compression": self.compression.value if self.compression else None,
            "use_dictionary": self.use_dictionary,
            "write_statistics": self.write_statistics,
            "int96_timestamp": self.int96_timestamp,
            "allow_truncated_timestamps": self.allow_truncated_timestamps,
        }
        if self.row_group_size is not None:
            opts["row_group_size"] = self.row_group_size
        if self.data_page_size is not None:
            opts["data_page_size"] = self.data_page_size
        if self.coerce_timestamps is not None:
            opts["coerce_timestamps"] = self.coerce_timestamps
        if self.metadata is not None:
            opts["metadata"] = self.metadata
        if self.custom_metadata is not None:
            opts["custom_metadata"] = self.custom_metadata
        return opts


@dataclass 
class ParquetReadOptions:
    """Options for reading Parquet files."""
    columns: Optional[List[str]] = None
    filter: Optional[str] = None
    use_threads: bool = True
    use_memory_map: bool = True
    lazy_index: bool = False
    metadata_key_mapper: Optional[Callable[[str], str]] = None
    schema: Optional[Any] = None
    batch_size: int = 64 * 1024

    def to_pyarrow_options(self) -> Dict[str, Any]:
        """Convert to pyarrow parquet read options."""
        return {
            "columns": self.columns,
            "use_threads": self.use_threads,
            "use_memory_map": self.use_memory_map,
            "lazy_index": self.lazy_index,
        }


class ParquetSerializer:
    """Serialize and deserialize data to/from Parquet format."""

    def __init__(self, default_options: Optional[ParquetWriteOptions] = None):
        """
        Initialize the Parquet serializer.

        Args:
            default_options: Default write options to use when not specified.
        """
        if not PYARROW_AVAILABLE:
            raise ImportError(
                "pyarrow is required for Parquet serialization. "
                "Install with: pip install pyarrow"
            )
        self.default_options = default_options or ParquetWriteOptions()

    def serialize_dict_list(
        self,
        data: List[Dict[str, Any]],
        output_path: Union[str, Path],
        options: Optional[ParquetWriteOptions] = None,
    ) -> Dict[str, Any]:
        """
        Serialize a list of dictionaries to a Parquet file.

        Args:
            data: List of dictionaries to serialize.
            output_path: Path to write the Parquet file.
            options: Write options (uses defaults if not specified).

        Returns:
            Dictionary with metadata about the written file.

        Raises:
            ValueError: If data is empty or options are invalid.
            IOError: If the file cannot be written.
        """
        if not data:
            raise ValueError("Cannot serialize empty data list")

        opts = options or self.default_options
        output_path = Path(output_path)

        try:
            table = pa.Table.from_pylist(data)
            writer_options = opts.to_pyarrow_options()
            
            with pq.ParquetWriter(
                output_path,
                table.schema,
                **writer_options
            ) as writer:
                writer.write_table(table)

            file_size = output_path.stat().st_size
            num_rows = len(data)
            num_columns = len(data[0]) if data else 0

            return {
                "output_path": str(output_path),
                "file_size_bytes": file_size,
                "num_rows": num_rows,
                "num_columns": num_columns,
                "compression": opts.compression.value,
                "schema": str(table.schema),
            }

        except Exception as e:
            raise IOError(f"Failed to write Parquet file: {e}") from e

    def deserialize_to_dict_list(
        self,
        input_path: Union[str, Path],
        options: Optional[ParquetReadOptions] = None,
    ) -> List[Dict[str, Any]]:
        """
        Deserialize a Parquet file to a list of dictionaries.

        Args:
            input_path: Path to the Parquet file.
            options: Read options (uses defaults if not specified).

        Returns:
            List of dictionaries representing the Parquet data.

        Raises:
            FileNotFoundError: If the file does not exist.
            IOError: If the file cannot be read.
        """
        input_path = Path(input_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {input_path}")

        opts = options or ParquetReadOptions()

        try:
            if opts.filter:
                pf = pq.ParquetFile(input_path, filters=opts.filter)
            else:
                pf = pq.ParquetFile(input_path)

            table = pf.read(
                columns=opts.columns,
                use_threads=opts.use_threads,
                use_memory_map=opts.use_memory_map,
            )

            return table.to_pylist()

        except Exception as e:
            raise IOError(f"Failed to read Parquet file: {e}") from e

    def serialize_pandas(
        self,
        df: Any,
        output_path: Union[str, Path],
        options: Optional[ParquetWriteOptions] = None,
    ) -> Dict[str, Any]:
        """
        Serialize a pandas DataFrame to a Parquet file.

        Args:
            df: pandas DataFrame to serialize.
            output_path: Path to write the Parquet file.
            options: Write options (uses defaults if not specified).

        Returns:
            Dictionary with metadata about the written file.

        Raises:
            ImportError: If pandas is not available.
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for DataFrame serialization")

        if df.empty:
            raise ValueError("Cannot serialize empty DataFrame")

        opts = options or self.default_options
        output_path = Path(output_path)

        try:
            writer_options = opts.to_pyarrow_options()
            df.to_parquet(output_path, **writer_options)

            file_size = output_path.stat().st_size

            return {
                "output_path": str(output_path),
                "file_size_bytes": file_size,
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "compression": opts.compression.value,
            }

        except Exception as e:
            raise IOError(f"Failed to write DataFrame to Parquet: {e}") from e

    def deserialize_pandas(
        self,
        input_path: Union[str, Path],
        options: Optional[ParquetReadOptions] = None,
    ) -> Any:
        """
        Deserialize a Parquet file to a pandas DataFrame.

        Args:
            input_path: Path to the Parquet file.
            options: Read options (uses defaults if not specified).

        Returns:
            pandas DataFrame containing the Parquet data.

        Raises:
            FileNotFoundError: If the file does not exist.
            ImportError: If pandas is not available.
        """
        if not PANDAS_AVAILABLE:
            raise ImportError("pandas is required for DataFrame deserialization")

        input_path = Path(input_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {input_path}")

        try:
            return pd.read_parquet(input_path, columns=options.columns if options else None)
        except Exception as e:
            raise IOError(f"Failed to read Parquet to DataFrame: {e}") from e

    def get_schema(
        self,
        input_path: Union[str, Path],
    ) -> Dict[str, Any]:
        """
        Get the schema of a Parquet file without reading the data.

        Args:
            input_path: Path to the Parquet file.

        Returns:
            Dictionary containing schema information.
        """
        input_path = Path(input_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {input_path}")

        try:
            pf = pq.ParquetFile(input_path)
            schema = pf.schema_arrow

            return {
                "num_columns": len(schema),
                "num_row_groups": pf.metadata.num_row_groups,
                "num_rows": pf.metadata.num_rows,
                "format_version": pf.metadata.format_version,
                "created_by": pf.metadata.created_by,
                "columns": [
                    {
                        "name": field.name,
                        "type": str(field.type),
                        "is_nullable": field.nullable,
                        "statistics": {
                            "min": field.statistics.min if field.statistics else None,
                            "max": field.statistics.max if field.statistics else None,
                            "null_count": field.statistics.null_count if field.statistics else None,
                            "distinct_count": field.statistics.distinct_count if field.statistics else None,
                        } if field.statistics else None,
                    }
                    for field in schema
                ],
            }

        except Exception as e:
            raise IOError(f"Failed to read Parquet schema: {e}") from e

    def merge_files(
        self,
        input_paths: List[Union[str, Path]],
        output_path: Union[str, Path],
    ) -> Dict[str, Any]:
        """
        Merge multiple Parquet files into a single file.

        Args:
            input_paths: List of Parquet file paths to merge.
            output_path: Path for the merged output file.

        Returns:
            Dictionary with metadata about the merged file.
        """
        if not input_paths:
            raise ValueError("No input files provided for merge")

        output_path = Path(output_path)
        tables = []

        try:
            for path in input_paths:
                pf = pq.ParquetFile(path)
                tables.append(pf.read())

            merged_table = pa.concat_tables(tables)

            with pq.ParquetWriter(
                output_path,
                merged_table.schema,
                compression="snappy"
            ) as writer:
                writer.write_table(merged_table)

            return {
                "output_path": str(output_path),
                "num_input_files": len(input_paths),
                "total_rows": merged_table.num_rows,
                "total_columns": merged_table.num_columns,
            }

        except Exception as e:
            raise IOError(f"Failed to merge Parquet files: {e}") from e


def parquet_serialize_action(
    input_data: Any,
    output_path: str,
    compression: str = "snappy",
    use_dictionary: bool = True,
) -> Dict[str, Any]:
    """
    Action function to serialize data to Parquet format.

    Args:
        input_data: Data to serialize (list of dicts or pandas DataFrame).
        output_path: Path for the output Parquet file.
        compression: Compression type (snappy, gzip, brotli, zstd, lz4).
        use_dictionary: Whether to use dictionary encoding.

    Returns:
        Dictionary with operation results and metadata.
    """
    compression_map = {
        "uncompressed": CompressionType.UNCOMPRESSED,
        "snappy": CompressionType.SNAPPY,
        "gzip": CompressionType.GZIP,
        "brotli": CompressionType.BROTLI,
        "zstd": CompressionType.ZSTD,
        "lz4": CompressionType.LZ4,
    }

    if compression.lower() not in compression_map:
        raise ValueError(f"Unsupported compression: {compression}")

    options = ParquetWriteOptions(
        compression=compression_map[compression.lower()],
        use_dictionary=use_dictionary,
    )

    serializer = ParquetSerializer(default_options=options)

    if PANDAS_AVAILABLE and isinstance(input_data, pd.DataFrame):
        return serializer.serialize_pandas(input_data, output_path, options)
    elif isinstance(input_data, list):
        return serializer.serialize_dict_list(input_data, output_path, options)
    else:
        raise ValueError(
            "input_data must be a list of dictionaries or pandas DataFrame"
        )


def parquet_deserialize_action(
    input_path: str,
    as_dataframe: bool = False,
) -> Any:
    """
    Action function to deserialize Parquet data.

    Args:
        input_path: Path to the Parquet file.
        as_dataframe: Return as pandas DataFrame if True.

    Returns:
        Deserialized data (list of dicts or DataFrame).
    """
    serializer = ParquetSerializer()

    if as_dataframe:
        return serializer.deserialize_pandas(input_path)
    else:
        return serializer.deserialize_to_dict_list(input_path)


# Convenience instances
serialize_parquet = ParquetSerializer()
deserialize_parquet = ParquetSerializer()
