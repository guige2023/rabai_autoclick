"""Batch process action module for RabAI AutoClick.

Provides batch processing with chunking, progress tracking,
error handling, and checkpoint/resume support.
"""

import sys
import os
import time
import json
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class BatchResult:
    """Result of a batch processing run."""
    total: int
    processed: int
    succeeded: int
    failed: int
    skipped: int
    duration: float
    errors: List[Dict] = field(default_factory=list)


class BatchProcessAction(BaseAction):
    """Process data in batches with configurable chunking.
    
    Supports chunk-based processing, progress tracking,
    error handling, and checkpoint-based resume.
    """
    action_type = "batch_process"
    display_name = "批量处理"
    description = "批量数据处理：分块/进度跟踪/错误处理"

    _checkpoints: Dict[str, int] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch processing.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (process/transform/filter/map/reduce)
                - data: list, data to process
                - chunk_size: int, items per batch
                - action_name: str, action to execute per chunk
                - action_params: dict, base params for action
                - data_field: str, field name in action_params containing chunk data
                - skip_errors: bool, continue on error
                - max_errors: int, max errors before stopping
                - checkpoint_id: str, for resume support
                - save_checkpoint: bool, save progress
                - resume_from_checkpoint: bool
                - save_to_var: str
        
        Returns:
            ActionResult with batch processing results.
        """
        operation = params.get('operation', 'process')
        data = params.get('data', [])
        chunk_size = params.get('chunk_size', 10)
        action_name = params.get('action_name', '')
        action_params = params.get('action_params', {})
        data_field = params.get('data_field', 'items')
        skip_errors = params.get('skip_errors', True)
        max_errors = params.get('max_errors', 0)
        checkpoint_id = params.get('checkpoint_id', '')
        save_checkpoint = params.get('save_checkpoint', False)
        resume = params.get('resume_from_checkpoint', False)
        save_to_var = params.get('save_to_var', None)

        if not data:
            return ActionResult(success=False, message="No data provided")

        if operation in ('transform', 'filter', 'map', 'reduce'):
            return self._process_with_operation(
                context, data, operation, chunk_size, action_params,
                skip_errors, max_errors, checkpoint_id, save_checkpoint, resume,
                save_to_var
            )
        elif operation == 'process':
            return self._process_chunks(
                context, data, chunk_size, action_name, action_params,
                data_field, skip_errors, max_errors, checkpoint_id,
                save_checkpoint, resume, save_to_var
            )
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _process_chunks(
        self, context: Any, data: List, chunk_size: int,
        action_name: str, action_params: Dict, data_field: str,
        skip_errors: bool, max_errors: int,
        checkpoint_id: str, save_checkpoint: bool,
        resume: bool, save_to_var: Optional[str]
    ) -> ActionResult:
        """Process data in chunks through an action."""
        if not action_name:
            return ActionResult(success=False, message="action_name is required")

        action = self._find_action(action_name)
        if action is None:
            return ActionResult(success=False, message=f"Action not found: {action_name}")

        start_offset = 0
        if resume and checkpoint_id:
            start_offset = self._checkpoints.get(checkpoint_id, 0)

        start_time = time.time()
        succeeded = 0
        failed = 0
        skipped = 0
        errors = []
        results = []

        total = len(data)
        for i in range(start_offset, total, chunk_size):
            chunk = data[i:i + chunk_size]
            chunk_params = dict(action_params)
            chunk_params[data_field] = chunk

            try:
                result = action.execute(context, chunk_params)
                if result.success:
                    succeeded += 1
                    results.append(result.data)
                else:
                    failed += 1
                    errors.append({'chunk_index': i // chunk_size, 'error': result.message})
                    if not skip_errors and (max_errors > 0 and failed >= max_errors):
                        skipped = total - i - chunk_size
                        break
            except Exception as e:
                failed += 1
                errors.append({'chunk_index': i // chunk_size, 'error': str(e)})
                if not skip_errors and (max_errors > 0 and failed >= max_errors):
                    skipped = total - i - chunk_size
                    break

            # Save checkpoint
            if save_checkpoint and checkpoint_id:
                self._checkpoints[checkpoint_id] = i + chunk_size

        batch_result = BatchResult(
            total=total,
            processed=succeeded + failed,
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            duration=time.time() - start_time,
            errors=errors
        )

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = {
                'results': results,
                'summary': {
                    'total': batch_result.total,
                    'processed': batch_result.processed,
                    'succeeded': batch_result.succeeded,
                    'failed': batch_result.failed,
                    'skipped': batch_result.skipped,
                    'duration': batch_result.duration
                }
            }

        return ActionResult(
            success=failed == 0,
            message=f"Batch: {succeeded} succeeded, {failed} failed, {skipped} skipped",
            data={
                'results': results,
                'summary': {
                    'total': batch_result.total,
                    'processed': batch_result.processed,
                    'succeeded': batch_result.succeeded,
                    'failed': batch_result.failed,
                    'skipped': batch_result.skipped,
                    'duration': batch_result.duration
                }
            }
        )

    def _process_with_operation(
        self, context: Any, data: List, operation: str,
        chunk_size: int, action_params: Dict,
        skip_errors: bool, max_errors: int,
        checkpoint_id: str, save_checkpoint: bool,
        resume: bool, save_to_var: Optional[str]
    ) -> ActionResult:
        """Process data using built-in operations."""
        start_offset = 0
        if resume and checkpoint_id:
            start_offset = self._checkpoints.get(checkpoint_id, 0)

        start_time = time.time()
        results = []
        failed = 0
        errors = []

        total = len(data)

        for i in range(start_offset, total):
            item = data[i]
            try:
                processed = self._apply_operation(item, operation, action_params)
                results.append(processed)
            except Exception as e:
                failed += 1
                errors.append({'index': i, 'error': str(e)})
                if not skip_errors and (max_errors > 0 and failed >= max_errors):
                    break

            if save_checkpoint and checkpoint_id and i % chunk_size == 0:
                self._checkpoints[checkpoint_id] = i

        succeeded = len(results)
        duration = time.time() - start_time

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = results

        return ActionResult(
            success=failed == 0,
            message=f"{operation}: {succeeded} succeeded, {failed} failed",
            data={
                'results': results,
                'succeeded': succeeded,
                'failed': failed,
                'duration': duration,
                'errors': errors
            }
        )

    def _apply_operation(
        self, item: Any, operation: str, params: Dict
    ) -> Any:
        """Apply built-in operation to item."""
        if operation == 'transform':
            # Transform each item using field mappings
            field_map = params.get('field_map', {})
            if isinstance(item, dict):
                result = {}
                for k, v in item.items():
                    if k in field_map:
                        result[field_map[k]] = v
                    else:
                        result[k] = v
                return result
            return item

        elif operation == 'filter':
            # Filter item based on conditions
            conditions = params.get('conditions', {})
            if isinstance(item, dict):
                for key, expected in conditions.items():
                    if key not in item or item[key] != expected:
                        raise ValueError(f"Condition failed for {key}")
            return item

        elif operation == 'map':
            # Map item to new value
            field = params.get('map_field', None)
            if field and isinstance(item, dict):
                return item.get(field)
            return item

        elif operation == 'reduce':
            # Reduce is applied to entire dataset
            return item

        return item

    def _find_action(self, action_name: str) -> Optional[BaseAction]:
        """Find an action by name."""
        try:
            from actions import (
                ClickAction, TypeAction, KeyPressAction, ImageMatchAction,
                FindImageAction, OCRAction, ScrollAction, MouseMoveAction,
                DragAction, ScriptAction, DelayAction, ConditionAction,
                LoopAction, SetVariableAction, ScreenshotAction,
                GetMousePosAction, AlertAction
            )
            action_map = {
                'click': ClickAction, 'type': TypeAction,
                'key_press': KeyPressAction, 'image_match': ImageMatchAction,
                'find_image': FindImageAction, 'ocr': OCRAction,
                'scroll': ScrollAction, 'mouse_move': MouseMoveAction,
                'drag': DragAction, 'script': ScriptAction,
                'delay': DelayAction, 'condition': ConditionAction,
                'loop': LoopAction, 'set_variable': SetVariableAction,
                'screenshot': ScreenshotAction, 'get_mouse_pos': GetMousePosAction,
                'alert': AlertAction,
            }
            action_cls = action_map.get(action_name.lower())
            return action_cls() if action_cls else None
        except Exception:
            return None

    def get_required_params(self) -> List[str]:
        return ['operation', 'data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'chunk_size': 10,
            'action_name': '',
            'action_params': {},
            'data_field': 'items',
            'skip_errors': True,
            'max_errors': 0,
            'checkpoint_id': '',
            'save_checkpoint': False,
            'resume_from_checkpoint': False,
            'field_map': {},
            'conditions': {},
            'map_field': None,
            'save_to_var': None,
        }
