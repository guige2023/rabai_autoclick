"""Data Checkpoint action module for RabAI AutoClick.

Manages data processing checkpoints for resumable
pipelines and crash recovery.
"""

import json
import time
import sys
import os
import hashlib
from typing import Any, Dict, List, Optional
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataCheckpointAction(BaseAction):
    """Manage checkpoints for resumable data processing.

    Saves progress, validates checkpoints, and enables
    recovery from failures.
    """
    action_type = "data_checkpoint"
    display_name = "数据检查点"
    description = "管理数据处理检查点，支持断点恢复"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage checkpoint.

        Args:
            context: Execution context.
            params: Dict with keys: action (save/load/list/delete),
                   checkpoint_id, pipeline_id, state, checkpoint_dir.

        Returns:
            ActionResult with checkpoint data.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'save')
            checkpoint_id = params.get('checkpoint_id', '')
            pipeline_id = params.get('pipeline_id', 'default')
            state = params.get('state', {})
            checkpoint_dir = params.get('checkpoint_dir', '/tmp/checkpoints')

            if not checkpoint_id and action != 'list':
                return ActionResult(
                    success=False,
                    message="checkpoint_id is required",
                    duration=time.time() - start_time,
                )

            os.makedirs(checkpoint_dir, exist_ok=True)
            checkpoint_path = os.path.join(checkpoint_dir, f"{pipeline_id}_{checkpoint_id}.json")

            if action == 'save':
                checkpoint_data = {
                    'checkpoint_id': checkpoint_id,
                    'pipeline_id': pipeline_id,
                    'state': state,
                    'timestamp': datetime.now().isoformat(),
                    'version': '1.0',
                }
                with open(checkpoint_path, 'w') as f:
                    json.dump(checkpoint_data, f, indent=2)
                duration = time.time() - start_time
                return ActionResult(
                    success=True,
                    message=f"Saved checkpoint {checkpoint_id}",
                    data={'path': checkpoint_path, 'checkpoint': checkpoint_data},
                    duration=duration,
                )

            elif action == 'load':
                if not os.path.exists(checkpoint_path):
                    return ActionResult(
                        success=False,
                        message=f"Checkpoint {checkpoint_id} not found",
                        duration=time.time() - start_time,
                    )
                with open(checkpoint_path, 'r') as f:
                    checkpoint_data = json.load(f)
                duration = time.time() - start_time
                return ActionResult(
                    success=True,
                    message=f"Loaded checkpoint {checkpoint_id}",
                    data=checkpoint_data,
                    duration=duration,
                )

            elif action == 'list':
                checkpoints = []
                prefix = f"{pipeline_id}_"
                if os.path.exists(checkpoint_dir):
                    for fname in os.listdir(checkpoint_dir):
                        if fname.startswith(prefix) and fname.endswith('.json'):
                            fpath = os.path.join(checkpoint_dir, fname)
                            try:
                                with open(fpath, 'r') as f:
                                    cp = json.load(f)
                                    checkpoints.append({
                                        'id': cp.get('checkpoint_id'),
                                        'timestamp': cp.get('timestamp'),
                                        'path': fpath,
                                    })
                            except Exception:
                                pass
                duration = time.time() - start_time
                return ActionResult(
                    success=True,
                    message=f"Found {len(checkpoints)} checkpoints",
                    data={'checkpoints': checkpoints},
                    duration=duration,
                )

            elif action == 'delete':
                if os.path.exists(checkpoint_path):
                    os.remove(checkpoint_path)
                duration = time.time() - start_time
                return ActionResult(
                    success=True,
                    message=f"Deleted checkpoint {checkpoint_id}",
                    duration=duration,
                )

            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown action: {action}",
                    duration=time.time() - start_time,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Checkpoint error: {str(e)}",
                duration=duration,
            )


class DataWatermarkAction(BaseAction):
    """Manage data watermarks for incremental processing.

    Tracks high-water marks for streaming data and
    incremental ETL.
    """
    action_type = "data_watermark"
    display_name = "数据水位线"
    description = "管理数据水位线，支持增量处理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage watermark.

        Args:
            context: Execution context.
            params: Dict with keys: action (get/update/check),
                   stream_id, watermark_value, watermark_dir.

        Returns:
            ActionResult with watermark status.
        """
        start_time = time.time()
        try:
            action = params.get('action', 'get')
            stream_id = params.get('stream_id', 'default')
            watermark_value = params.get('watermark_value')
            watermark_dir = params.get('watermark_dir', '/tmp/watermarks')

            os.makedirs(watermark_dir, exist_ok=True)
            wm_path = os.path.join(watermark_dir, f"watermark_{stream_id}.json")

            if action == 'get':
                if os.path.exists(wm_path):
                    with open(wm_path, 'r') as f:
                        wm_data = json.load(f)
                    return ActionResult(
                        success=True,
                        message=f"Watermark for {stream_id}: {wm_data.get('value')}",
                        data=wm_data,
                        duration=time.time() - start_time,
                    )
                return ActionResult(
                    success=True,
                    message=f"No watermark for {stream_id}",
                    data={'stream_id': stream_id, 'value': None},
                    duration=time.time() - start_time,
                )

            elif action == 'update':
                if watermark_value is None:
                    return ActionResult(
                        success=False,
                        message="watermark_value is required for update",
                        duration=time.time() - start_time,
                    )
                wm_data = {
                    'stream_id': stream_id,
                    'value': watermark_value,
                    'updated_at': datetime.now().isoformat(),
                }
                with open(wm_path, 'w') as f:
                    json.dump(wm_data, f)
                return ActionResult(
                    success=True,
                    message=f"Updated watermark for {stream_id} to {watermark_value}",
                    data=wm_data,
                    duration=time.time() - start_time,
                )

            elif action == 'check':
                if not os.path.exists(wm_path):
                    return ActionResult(
                        success=True,
                        message=f"No watermark for {stream_id} (all data is new)",
                        data={'is_new': True, 'watermark': None},
                        duration=time.time() - start_time,
                    )
                with open(wm_path, 'r') as f:
                    wm_data = json.load(f)
                current_wm = wm_data.get('value')
                is_new = (watermark_value is None) or (watermark_value > current_wm if current_wm else True)
                return ActionResult(
                    success=True,
                    message=f"Check: is_new={is_new}",
                    data={'is_new': is_new, 'watermark': current_wm},
                    duration=time.time() - start_time,
                )

            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown action: {action}",
                    duration=time.time() - start_time,
                )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Watermark error: {str(e)}",
                duration=duration,
            )


class DataFingerprintAction(BaseAction):
    """Generate and compare data fingerprints for change detection.

    Creates hashes of data to detect modifications
    without comparing full contents.
    """
    action_type = "data_fingerprint"
    display_name = "数据指纹"
    description = "生成数据指纹用于变更检测"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate fingerprint.

        Args:
            context: Execution context.
            params: Dict with keys: data, algorithm, compare_with.

        Returns:
            ActionResult with fingerprint.
        """
        start_time = time.time()
        try:
            data = params.get('data')
            algorithm = params.get('algorithm', 'sha256')
            compare_with = params.get('compare_with', None)

            if data is None:
                return ActionResult(
                    success=False,
                    message="Data is required",
                    duration=time.time() - start_time,
                )

            # Normalize data to string
            if isinstance(data, (dict, list)):
                normalized = json.dumps(data, sort_keys=True, ensure_ascii=False)
            else:
                normalized = str(data)

            # Compute hash
            if algorithm == 'md5':
                hash_obj = hashlib.md5()
            elif algorithm == 'sha1':
                hash_obj = hashlib.sha1()
            elif algorithm == 'sha256':
                hash_obj = hashlib.sha256()
            elif algorithm == 'sha512':
                hash_obj = hashlib.sha512()
            else:
                hash_obj = hashlib.sha256()

            hash_obj.update(normalized.encode('utf-8'))
            fingerprint = hash_obj.hexdigest()

            result = {
                'fingerprint': fingerprint,
                'algorithm': algorithm,
                'length': len(normalized),
            }

            if compare_with:
                result['matches'] = (fingerprint == compare_with)

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Generated {algorithm} fingerprint",
                data=result,
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Fingerprint error: {str(e)}",
                duration=duration,
            )
