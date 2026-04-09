"""Data Watermark Action Module.

Provides watermark embedding and detection for data tracking.
"""

import time
import hashlib
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataWatermarkAction(BaseAction):
    """Embed watermarks in data for tracking.
    
    Supports invisible and visible watermarking strategies.
    """
    action_type = "data_watermark"
    display_name = "数据水印"
    description = "在数据中嵌入水印用于追踪"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute watermarking operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, data, watermark, options.
        
        Returns:
            ActionResult with watermarking result.
        """
        action = params.get('action', 'embed')
        data = params.get('data', [])
        watermark = params.get('watermark', '')
        options = params.get('options', {})
        
        if action == 'embed':
            return self._embed_watermark(data, watermark, options)
        elif action == 'detect':
            return self._detect_watermark(data, options)
        elif action == 'verify':
            return self._verify_watermark(data, watermark, options)
        elif action == 'remove':
            return self._remove_watermark(data, options)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _embed_watermark(
        self,
        data: List,
        watermark: str,
        options: Dict
    ) -> ActionResult:
        """Embed watermark in data."""
        strategy = options.get('strategy', 'metadata')
        
        if strategy == 'metadata':
            # Add watermark as metadata
            watermarked = []
            for item in data:
                if isinstance(item, dict):
                    item_copy = item.copy()
                    item_copy['_watermark'] = watermark
                    item_copy['_watermarked_at'] = time.time()
                    watermarked.append(item_copy)
                else:
                    watermarked.append(item)
        
        elif strategy == 'pattern':
            # Embed as pattern in data
            watermarked = self._embed_pattern(data, watermark)
        
        elif strategy == 'hash':
            # Embed as hash signature
            watermarked = []
            for item in data:
                if isinstance(item, dict):
                    item_copy = item.copy()
                    content = json.dumps(item, sort_keys=True)
                    item_copy['_signature'] = hashlib.sha256(
                        f"{content}{watermark}".encode()
                    ).hexdigest()
                    watermarked.append(item_copy)
                else:
                    watermarked.append(item)
        
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown strategy: {strategy}"
            )
        
        return ActionResult(
            success=True,
            data={
                'watermarked': watermarked,
                'watermark': watermark,
                'strategy': strategy
            },
            error=None
        )
    
    def _embed_pattern(self, data: List, watermark: str) -> List:
        """Embed watermark as pattern."""
        # Simple pattern embedding: append chars at intervals
        watermarked = []
        pattern_interval = 10
        
        for i, item in enumerate(data):
            if isinstance(item, dict):
                item_copy = item.copy()
                # Embed watermark character at interval positions
                watermark_chars = []
                for j, c in enumerate(watermark):
                    if (i + j) % pattern_interval == 0:
                        watermark_chars.append(c)
                if watermark_chars:
                    item_copy['_pattern'] = ''.join(watermark_chars)
                watermarked.append(item_copy)
            else:
                watermarked.append(item)
        
        return watermarked
    
    def _detect_watermark(
        self,
        data: List,
        options: Dict
    ) -> ActionResult:
        """Detect watermark in data."""
        detected_watermarks = []
        
        for item in data:
            if isinstance(item, dict):
                if '_watermark' in item:
                    detected_watermarks.append({
                        'type': 'metadata',
                        'watermark': item['_watermark'],
                        'timestamp': item.get('_watermarked_at')
                    })
                if '_signature' in item:
                    detected_watermarks.append({
                        'type': 'hash',
                        'signature': item['_signature']
                    })
                if '_pattern' in item:
                    detected_watermarks.append({
                        'type': 'pattern',
                        'pattern': item['_pattern']
                    })
        
        return ActionResult(
            success=True,
            data={
                'detected': detected_watermarks,
                'count': len(detected_watermarks)
            },
            error=None
        )
    
    def _verify_watermark(
        self,
        data: List,
        expected_watermark: str,
        options: Dict
    ) -> ActionResult:
        """Verify watermark matches expected."""
        detected = self._detect_watermark(data, {})
        
        matches = False
        for dw in detected.data.get('detected', []):
            if dw.get('watermark') == expected_watermark:
                matches = True
                break
        
        return ActionResult(
            success=True,
            data={
                'verified': matches,
                'expected': expected_watermark,
                'detected_count': detected.data.get('count', 0)
            },
            error=None
        )
    
    def _remove_watermark(
        self,
        data: List,
        options: Dict
    ) -> ActionResult:
        """Remove watermark from data."""
        watermark_fields = ['_watermark', '_watermarked_at', '_signature', '_pattern']
        
        cleaned = []
        for item in data:
            if isinstance(item, dict):
                item_copy = {
                    k: v for k, v in item.items()
                    if k not in watermark_fields
                }
                cleaned.append(item_copy)
            else:
                cleaned.append(item)
        
        return ActionResult(
            success=True,
            data={
                'cleaned': cleaned,
                'removed_count': len(data) - len(cleaned)
            },
            error=None
        )


class DataTimestampAction(BaseAction):
    """Add timestamps to data records.
    
    Tracks when data was created, modified, and accessed.
    """
    action_type = "data_timestamp"
    display_name = "数据时间戳"
    description = "为数据记录添加时间戳追踪"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute timestamp operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, data, timestamp_fields.
        
        Returns:
            ActionResult with timestamped data.
        """
        action = params.get('action', 'add')
        data = params.get('data', [])
        timestamp_fields = params.get('timestamp_fields', {
            'created': 'created_at',
            'modified': 'modified_at',
            'accessed': 'accessed_at'
        })
        
        if action == 'add':
            return self._add_timestamps(data, timestamp_fields)
        elif action == 'update_accessed':
            return self._update_accessed(data, timestamp_fields)
        elif action == 'get':
            return self._get_timestamps(data, timestamp_fields)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _add_timestamps(
        self,
        data: List,
        fields: Dict
    ) -> ActionResult:
        """Add timestamps to data."""
        now = time.time()
        timestamped = []
        
        for item in data:
            if isinstance(item, dict):
                item_copy = item.copy()
                if 'created' in fields:
                    item_copy[fields['created']] = now
                if 'modified' in fields:
                    item_copy[fields['modified']] = now
                if 'accessed' in fields:
                    item_copy[fields['accessed']] = now
                timestamped.append(item_copy)
            else:
                timestamped.append(item)
        
        return ActionResult(
            success=True,
            data={
                'timestamped': timestamped,
                'count': len(timestamped)
            },
            error=None
        )
    
    def _update_accessed(
        self,
        data: List,
        fields: Dict
    ) -> ActionResult:
        """Update accessed timestamp."""
        now = time.time()
        updated = 0
        
        for item in data:
            if isinstance(item, dict) and 'accessed' in fields:
                item[fields['accessed']] = now
                updated += 1
        
        return ActionResult(
            success=True,
            data={'updated_count': updated},
            error=None
        )
    
    def _get_timestamps(
        self,
        data: List,
        fields: Dict
    ) -> ActionResult:
        """Get timestamps from data."""
        timestamps = []
        
        for item in data:
            if isinstance(item, dict):
                ts = {}
                for key, field_name in fields.items():
                    if field_name in item:
                        ts[key] = item[field_name]
                timestamps.append(ts)
        
        return ActionResult(
            success=True,
            data={
                'timestamps': timestamps,
                'count': len(timestamps)
            },
            error=None
        )


def register_actions():
    """Register all Data Watermark actions."""
    return [
        DataWatermarkAction,
        DataTimestampAction,
    ]
