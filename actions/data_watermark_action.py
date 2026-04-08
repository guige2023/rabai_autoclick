"""Data watermark action module for RabAI AutoClick.

Provides data watermarking for traceability including
invisible watermarks, fingerprinting, and audit trail.
"""

import sys
import os
import hashlib
import time
import json
import base64
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Watermark:
    """A data watermark."""
    watermark_id: str
    source: str
    timestamp: float
    owner: str
    fingerprint: str
    metadata: Dict[str, Any]


class DataWatermarkAction(BaseAction):
    """Embed and verify data watermarks.
    
    Supports invisible watermarks, fingerprinting,
    ownership tracking, and tamper detection.
    """
    action_type = "data_watermark"
    display_name = "数据水印"
    description = "数据水印嵌入和验证，支持指纹和溯源"

    WATERMARK_PREFIX = "__wm_"
    WATERMARK_FIELDS = ["id", "source", "timestamp", "owner", "fingerprint", "metadata"]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Embed or verify watermarks in data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (embed/verify/extract/remove)
                - data: any, data to watermark
                - source: str, data source identifier
                - owner: str, owner identifier
                - metadata: dict, additional metadata
                - strength: str (weak/strong), watermark strength
                - save_to_var: str
        
        Returns:
            ActionResult with watermark operation result.
        """
        operation = params.get('operation', 'embed')
        data = params.get('data', None)
        source = params.get('source', 'unknown')
        owner = params.get('owner', '')
        metadata = params.get('metadata', {})
        strength = params.get('strength', 'strong')
        save_to_var = params.get('save_to_var', None)

        if operation == 'embed':
            return self._embed_watermark(data, source, owner, metadata, strength, save_to_var)
        elif operation == 'verify':
            return self._verify_watermark(data, save_to_var)
        elif operation == 'extract':
            return self._extract_watermark(data, save_to_var)
        elif operation == 'remove':
            return self._remove_watermark(data, save_to_var)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _embed_watermark(
        self, data: Any, source: str, owner: str,
        metadata: Dict, strength: str, save_to_var: Optional[str]
    ) -> ActionResult:
        """Embed a watermark into data."""
        if data is None:
            return ActionResult(success=False, message="No data to watermark")

        watermark_id = self._generate_id(data, owner)
        fingerprint = self._compute_fingerprint(data, watermark_id)
        timestamp = time.time()

        watermark = Watermark(
            watermark_id=watermark_id,
            source=source,
            timestamp=timestamp,
            owner=owner,
            fingerprint=fingerprint,
            metadata=metadata
        )

        wm_dict = {
            f'{self.WATERMARK_PREFIX}{k}': v for k, v in {
                'id': watermark_id,
                'source': source,
                'timestamp': timestamp,
                'owner': owner,
                'fingerprint': fingerprint,
                'metadata': json.dumps(metadata),
            }.items()
        }

        # Embed watermark
        if isinstance(data, dict):
            result = {**data, **wm_dict}
        elif isinstance(data, list):
            # Embed in each item
            result = []
            for item in data:
                if isinstance(item, dict):
                    result.append({**item, **wm_dict})
                else:
                    result.append(item)
        else:
            result = {'data': data, **wm_dict}

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = result

        return ActionResult(
            success=True,
            message=f"Watermark embedded: {watermark_id}",
            data={
                'watermarked_data': result,
                'watermark_id': watermark_id,
                'fingerprint': fingerprint
            }
        )

    def _verify_watermark(self, data: Any, save_to_var: Optional[str]) -> ActionResult:
        """Verify watermark integrity."""
        extracted = self._do_extract(data)
        if extracted is None:
            return ActionResult(
                success=False,
                message="No watermark found",
                data={'verified': False, 'reason': 'no_watermark'}
            )

        fingerprint = extracted.fingerprint
        stored_fp = extracted.metadata  # stored as string

        # Recompute fingerprint from data
        wm_id = extracted.watermark_id
        computed_fp = self._compute_fingerprint(data, wm_id)

        is_valid = computed_fp == fingerprint

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = {
                'verified': is_valid,
                'watermark_id': extracted.watermark_id,
                'owner': extracted.owner,
                'source': extracted.source,
                'timestamp': extracted.timestamp
            }

        return ActionResult(
            success=is_valid,
            message=f"Watermark verification: {'PASSED' if is_valid else 'FAILED'}",
            data={
                'verified': is_valid,
                'watermark_id': extracted.watermark_id,
                'owner': extracted.owner
            }
        )

    def _extract_watermark(self, data: Any, save_to_var: Optional[str]) -> ActionResult:
        """Extract watermark from data."""
        extracted = self._do_extract(data)
        if extracted is None:
            return ActionResult(
                success=False,
                message="No watermark found",
                data=None
            )

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = {
                'watermark_id': extracted.watermark_id,
                'source': extracted.source,
                'timestamp': extracted.timestamp,
                'owner': extracted.owner,
                'fingerprint': extracted.fingerprint,
                'metadata': extracted.metadata
            }

        return ActionResult(
            success=True,
            message=f"Extracted watermark: {extracted.watermark_id}",
            data={
                'watermark_id': extracted.watermark_id,
                'source': extracted.source,
                'owner': extracted.owner,
                'timestamp': extracted.timestamp
            }
        )

    def _remove_watermark(self, data: Any, save_to_var: Optional[str]) -> ActionResult:
        """Remove watermark from data."""
        if isinstance(data, dict):
            result = {k: v for k, v in data.items() if not k.startswith(self.WATERMARK_PREFIX)}
        elif isinstance(data, list):
            result = []
            for item in data:
                if isinstance(item, dict):
                    result.append({k: v for k, v in item.items() if not k.startswith(self.WATERMARK_PREFIX)})
                else:
                    result.append(item)
        else:
            result = data

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = result

        return ActionResult(
            success=True,
            message="Watermark removed",
            data=result
        )

    def _do_extract(self, data: Any) -> Optional[Watermark]:
        """Internal extract method."""
        wm_data = {}

        if isinstance(data, dict):
            wm_data = {k[len(self.WATERMARK_PREFIX):]: v
                      for k, v in data.items()
                      if k.startswith(self.WATERMARK_PREFIX)}
        elif isinstance(data, list) and data and isinstance(data[0], dict):
            wm_data = {k[len(self.WATERMARK_PREFIX):]: v
                      for k, v in data[0].items()
                      if k.startswith(self.WATERMARK_PREFIX)}

        if not wm_data:
            return None

        try:
            metadata = json.loads(wm_data.get('metadata', '{}'))
        except Exception:
            metadata = {}

        return Watermark(
            watermark_id=wm_data.get('id', ''),
            source=wm_data.get('source', ''),
            timestamp=wm_data.get('timestamp', 0),
            owner=wm_data.get('owner', ''),
            fingerprint=wm_data.get('fingerprint', ''),
            metadata=metadata
        )

    def _generate_id(self, data: Any, owner: str) -> str:
        """Generate unique watermark ID."""
        raw = f"{str(data)}-{owner}-{time.time()}-{os.getpid()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _compute_fingerprint(self, data: Any, watermark_id: str) -> str:
        """Compute data fingerprint."""
        # Normalize data for consistent fingerprinting
        if isinstance(data, dict):
            # Remove watermark fields before fingerprinting
            clean = {k: v for k, v in data.items() if not k.startswith(self.WATERMARK_PREFIX)}
            content = json.dumps(clean, sort_keys=True, default=str)
        elif isinstance(data, list):
            content = json.dumps(data, sort_keys=True, default=str)
        else:
            content = str(data)

        raw = f"{content}-{watermark_id}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def get_required_params(self) -> List[str]:
        return ['operation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'data': None,
            'source': 'unknown',
            'owner': '',
            'metadata': {},
            'strength': 'strong',
            'save_to_var': None,
        }
