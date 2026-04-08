"""
Data Chunk Action Module.

Splits data into chunks with configurable size, overlap,
and strategy for efficient batch processing.

Author: RabAi Team
"""

from __future__ import annotations

import sys
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ChunkStrategy(Enum):
    """Strategies for chunking data."""
    FIXED_SIZE = "fixed_size"
    FIXED_COUNT = "fixed_count"
    EQUAL_SIZE = "equal_size"
    OVERLAPPING = "overlapping"
    RECORD_BREAK = "record_break"
    KEY_GROUP = "key_group"


@dataclass
class Chunk:
    """A chunk of data."""
    index: int
    data: Any
    size: int
    start_idx: int
    end_idx: int
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataChunkAction(BaseAction):
    """Data chunk action.
    
    Splits data into chunks with various strategies for
    batch processing, parallelization, and memory management.
    """
    action_type = "data_chunk"
    display_name = "数据分块"
    description = "数据分块处理"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Split data into chunks.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - data: Data to chunk (list, string, dict)
                - operation: chunk/unchunk/process
                - strategy: Chunking strategy (fixed_size/fixed_count/equal_size/overlapping/record_break/key_group)
                - chunk_size: Size of each chunk
                - chunk_count: Number of chunks
                - overlap: Overlap between chunks for overlapping strategy
                - key_field: Field to group by for key_group strategy
                - batch_size: Batch size for processing
                
        Returns:
            ActionResult with chunked data or processing results.
        """
        start_time = time.time()
        
        operation = params.get("operation", "chunk")
        data = params.get("data", [])
        strategy_str = params.get("strategy", "fixed_size")
        chunk_size = params.get("chunk_size", 100)
        chunk_count = params.get("chunk_count", 10)
        overlap = params.get("overlap", 0)
        key_field = params.get("key_field")
        batch_size = params.get("batch_size")
        
        try:
            strategy = ChunkStrategy(strategy_str)
        except ValueError:
            strategy = ChunkStrategy.FIXED_SIZE
        
        try:
            if operation == "chunk":
                result = self._chunk_data(data, strategy, chunk_size, chunk_count, overlap, key_field, start_time)
            elif operation == "unchunk":
                result = self._unchunk_data(data, start_time)
            elif operation == "process":
                result = self._process_chunks(data, strategy, chunk_size, chunk_count, overlap, key_field, batch_size, params, context, start_time)
            elif operation == "window":
                result = self._create_windows(data, chunk_size, overlap, start_time)
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
                message=f"Chunk operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _chunk_data(
        self, data: Any, strategy: ChunkStrategy, chunk_size: int,
        chunk_count: int, overlap: int, key_field: Optional[str], start_time: float
    ) -> ActionResult:
        """Chunk data using specified strategy."""
        if not data:
            return ActionResult(
                success=True,
                message="No data to chunk",
                data={"chunks": [], "count": 0},
                duration=time.time() - start_time
            )
        
        chunks = []
        
        if isinstance(data, (list, tuple)):
            if strategy == ChunkStrategy.FIXED_SIZE:
                chunks = self._chunk_list_fixed_size(data, chunk_size, overlap)
            elif strategy == ChunkStrategy.FIXED_COUNT:
                chunks = self._chunk_list_fixed_count(data, chunk_count)
            elif strategy == ChunkStrategy.EQUAL_SIZE:
                chunks = self._chunk_list_equal_size(data, chunk_count)
            elif strategy == ChunkStrategy.OVERLAPPING:
                chunks = self._chunk_list_overlapping(data, chunk_size, overlap)
            elif strategy == ChunkStrategy.KEY_GROUP:
                chunks = self._chunk_by_key(data, key_field or "key")
            else:
                chunks = self._chunk_list_fixed_size(data, chunk_size, 0)
        
        elif isinstance(data, str):
            chunks = self._chunk_string(data, chunk_size, overlap)
        
        elif isinstance(data, dict):
            chunks = self._chunk_dict(data, chunk_size)
        
        else:
            return ActionResult(
                success=False,
                message=f"Unsupported data type: {type(data)}",
                duration=time.time() - start_time
            )
        
        chunked_output = []
        for i, chunk_data in enumerate(chunks):
            if isinstance(chunk_data, list):
                chunked_output.append({
                    "index": i,
                    "data": chunk_data,
                    "size": len(chunk_data),
                    "start_idx": getattr(chunk_data[0], 'index', 0) if hasattr(chunk_data[0], 'index') else i * chunk_size,
                    "end_idx": i * chunk_size + len(chunk_data)
                })
            elif isinstance(chunk_data, dict):
                chunked_output.append({
                    "index": i,
                    "data": chunk_data,
                    "size": len(chunk_data),
                    "start_idx": 0,
                    "end_idx": len(chunk_data)
                })
            else:
                chunked_output.append({
                    "index": i,
                    "data": chunk_data,
                    "size": len(str(chunk_data)) if hasattr(chunk_data, '__len__') else 1,
                    "start_idx": 0,
                    "end_idx": i * chunk_size + (len(chunk_data) if hasattr(chunk_data, '__len__') else 1)
                })
        
        return ActionResult(
            success=True,
            message=f"Created {len(chunks)} chunks",
            data={
                "chunks": chunked_output,
                "count": len(chunks),
                "strategy": strategy.value
            },
            duration=time.time() - start_time
        )
    
    def _chunk_list_fixed_size(self, data: List, chunk_size: int, overlap: int) -> List[List]:
        """Chunk list with fixed size and optional overlap."""
        chunks = []
        step = chunk_size - overlap if overlap > 0 else chunk_size
        
        for i in range(0, len(data), step):
            chunk = data[i:i + chunk_size]
            if chunk:
                chunks.append(chunk)
        
        return chunks
    
    def _chunk_list_fixed_count(self, data: List, chunk_count: int) -> List[List]:
        """Chunk list into fixed number of chunks."""
        if not data or chunk_count <= 0:
            return []
        
        chunk_size = (len(data) + chunk_count - 1) // chunk_count
        chunks = []
        
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            if chunk:
                chunks.append(chunk)
        
        return chunks
    
    def _chunk_list_equal_size(self, data: List, target_size: int) -> List[List]:
        """Chunk list into approximately equal-sized chunks."""
        return self._chunk_list_fixed_count(data, target_size)
    
    def _chunk_list_overlapping(self, data: List, chunk_size: int, overlap: int) -> List[List]:
        """Chunk list with overlapping windows."""
        if overlap >= chunk_size:
            overlap = chunk_size - 1
        return self._chunk_list_fixed_size(data, chunk_size, overlap)
    
    def _chunk_by_key(self, data: List[Dict], key_field: str) -> List[List[Dict]]:
        """Chunk list by grouping on a key field."""
        groups: Dict[str, List[Dict]] = {}
        
        for record in data:
            if isinstance(record, dict):
                key = record.get(key_field, "__none__")
                if key not in groups:
                    groups[key] = []
                groups[key].append(record)
        
        return list(groups.values())
    
    def _chunk_string(self, data: str, chunk_size: int, overlap: int) -> List[str]:
        """Chunk string data."""
        chunks = []
        step = chunk_size - overlap if overlap > 0 else chunk_size
        
        for i in range(0, len(data), step):
            chunk = data[i:i + chunk_size]
            if chunk:
                chunks.append(chunk)
        
        return chunks
    
    def _chunk_dict(self, data: Dict, chunk_size: int) -> List[Dict]:
        """Chunk dictionary into smaller dicts."""
        items = list(data.items())
        chunks = []
        
        for i in range(0, len(items), chunk_size):
            chunk_items = items[i:i + chunk_size]
            chunks.append(dict(chunk_items))
        
        return chunks
    
    def _create_windows(self, data: List, window_size: int, step: int, start_time: float) -> ActionResult:
        """Create sliding windows over data."""
        if step <= 0:
            step = window_size
        
        windows = []
        for i in range(0, len(data), step):
            window = data[i:i + window_size]
            if len(window) == window_size:
                windows.append({
                    "index": len(windows),
                    "data": window,
                    "size": len(window),
                    "start_idx": i,
                    "end_idx": i + len(window)
                })
        
        return ActionResult(
            success=True,
            message=f"Created {len(windows)} windows",
            data={"windows": windows, "count": len(windows), "window_size": window_size, "step": step},
            duration=time.time() - start_time
        )
    
    def _unchunk_data(self, data: List[Dict], start_time: float) -> ActionResult:
        """Unchunk previously chunked data."""
        if not data:
            return ActionResult(
                success=True,
                message="No chunks to unchunk",
                data={"unchunked": [], "count": 0},
                duration=time.time() - start_time
            )
        
        unchunked = []
        for chunk_info in data:
            chunk_data = chunk_info.get("data", chunk_info)
            if isinstance(chunk_data, list):
                unchunked.extend(chunk_data)
            else:
                unchunked.append(chunk_data)
        
        return ActionResult(
            success=True,
            message=f"Unchunked to {len(unchunked)} items",
            data={"unchunked": unchunked, "count": len(unchunked), "chunks_processed": len(data)},
            duration=time.time() - start_time
        )
    
    def _process_chunks(
        self, data: Any, strategy: ChunkStrategy, chunk_size: int,
        chunk_count: int, overlap: int, key_field: Optional[str],
        batch_size: Optional[int], params: Dict, context: Any, start_time: float
    ) -> ActionResult:
        """Process data in chunks with a processor function."""
        chunk_result = self._chunk_data(data, strategy, chunk_size, chunk_count, overlap, key_field, start_time)
        
        if not chunk_result.success:
            return chunk_result
        
        chunks = chunk_result.data["chunks"]
        processor = params.get("processor")
        aggregate = params.get("aggregate", True)
        
        results = []
        for chunk_info in chunks:
            if processor and callable(processor):
                try:
                    processed = processor(chunk_info["data"])
                    results.append(processed)
                except Exception as e:
                    results.append({"error": str(e), "chunk_index": chunk_info["index"]})
            else:
                results.append(chunk_info)
        
        if aggregate and results:
            aggregated = self._aggregate_results(results)
            return ActionResult(
                success=True,
                message=f"Processed {len(chunks)} chunks",
                data={
                    "results": results,
                    "aggregated": aggregated,
                    "chunks_processed": len(chunks)
                },
                duration=time.time() - start_time
            )
        
        return ActionResult(
            success=True,
            message=f"Processed {len(chunks)} chunks",
            data={"results": results, "chunks_processed": len(chunks)},
            duration=time.time() - start_time
        )
    
    def _aggregate_results(self, results: List) -> Dict[str, Any]:
        """Aggregate processing results."""
        if not results:
            return {}
        
        success_count = sum(1 for r in results if not isinstance(r, dict) or "error" not in r)
        error_count = len(results) - success_count
        
        all_numbers = []
        for r in results:
            if isinstance(r, (int, float)):
                all_numbers.append(r)
            elif isinstance(r, dict) and "result" in r:
                val = r["result"]
                if isinstance(val, (int, float)):
                    all_numbers.append(val)
        
        agg = {
            "total_chunks": len(results),
            "success_count": success_count,
            "error_count": error_count
        }
        
        if all_numbers:
            agg["sum"] = sum(all_numbers)
            agg["mean"] = sum(all_numbers) / len(all_numbers)
            agg["min"] = min(all_numbers)
            agg["max"] = max(all_numbers)
        
        return agg
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate chunk parameters."""
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []
