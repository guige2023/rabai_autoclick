"""Slice utilities action module for RabAI AutoClick.

Provides slice and range operations for sequences,
including pagination and chunking utilities.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union, Sequence

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SliceAction(BaseAction):
    """Slice a sequence by index range.
    
    Supports start:stop:step slicing with negative indices.
    """
    action_type = "slice"
    display_name = "切片"
    description = "按索引范围切片序列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Slice sequence.
        
        Args:
            context: Execution context.
            params: Dict with keys: sequence, start, stop, step,
                   save_to_var.
        
        Returns:
            ActionResult with sliced sequence.
        """
        sequence = params.get('sequence', [])
        start = params.get('start', 0)
        stop = params.get('stop', None)
        step = params.get('step', 1)
        save_to_var = params.get('save_to_var', None)

        if not isinstance(sequence, (list, tuple, str)):
            return ActionResult(
                success=False,
                message=f"Sequence must be list/tuple/str, got {type(sequence).__name__}"
            )

        try:
            # Handle None for stop
            actual_stop = stop if stop is not None else len(sequence)

            # Handle negative indices
            actual_start = start if start >= 0 else len(sequence) + start
            actual_stop = actual_stop if actual_stop >= 0 else len(sequence) + actual_stop

            result = sequence[actual_start:actual_stop:step]

            result_data = {
                'result': result,
                'original_length': len(sequence),
                'result_length': len(result),
                'slice': f'{actual_start}:{actual_stop}:{step}'
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"切片成功: [{actual_start}:{actual_stop}:{step}] -> {len(result)} 项",
                data=result_data
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"切片失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['sequence']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'start': 0,
            'stop': None,
            'step': 1,
            'save_to_var': None
        }


class ChunkAction(BaseAction):
    """Split sequence into chunks.
    
    Creates evenly-sized chunks from a sequence.
    """
    action_type = "chunk"
    display_name = "分块"
    description = "将序列分割为均匀块"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Chunk sequence.
        
        Args:
            context: Execution context.
            params: Dict with keys: sequence, chunk_size,
                   save_to_var.
        
        Returns:
            ActionResult with chunks.
        """
        sequence = params.get('sequence', [])
        chunk_size = params.get('chunk_size', 10)
        save_to_var = params.get('save_to_var', None)

        if not isinstance(sequence, (list, tuple, str)):
            return ActionResult(
                success=False,
                message=f"Sequence must be list/tuple/str, got {type(sequence).__name__}"
            )

        if chunk_size <= 0:
            return ActionResult(
                success=False,
                message=f"chunk_size must be > 0, got {chunk_size}"
            )

        chunks = []
        for i in range(0, len(sequence), chunk_size):
            chunks.append(sequence[i:i + chunk_size])

        result_data = {
            'chunks': chunks,
            'count': len(chunks),
            'chunk_size': chunk_size,
            'original_length': len(sequence)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"分块完成: {len(sequence)} -> {len(chunks)} 块",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['sequence', 'chunk_size']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'save_to_var': None}


class FlattenAction(BaseAction):
    """Flatten nested sequences.
    
    Recursively flattens nested lists/tuples.
    """
    action_type = "flatten"
    display_name = "扁平化"
    description = "递归扁平化嵌套序列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Flatten sequence.
        
        Args:
            context: Execution context.
            params: Dict with keys: sequence, max_depth,
                   save_to_var.
        
        Returns:
            ActionResult with flattened list.
        """
        sequence = params.get('sequence', [])
        max_depth = params.get('max_depth', None)
        save_to_var = params.get('save_to_var', None)

        def _flatten_recursive(seq, depth=0):
            if max_depth is not None and depth >= max_depth:
                return [seq]

            result = []
            for item in seq:
                if isinstance(item, (list, tuple)):
                    result.extend(_flatten_recursive(item, depth + 1))
                else:
                    result.append(item)
            return result

        flattened = _flatten_recursive(sequence)

        result_data = {
            'flattened': flattened,
            'original_length': len(sequence),
            'result_length': len(flattened)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"扁平化完成: {len(sequence)} -> {len(flattened)} 项",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['sequence']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'max_depth': None,
            'save_to_var': None
        }
