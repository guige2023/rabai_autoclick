"""
Data Watermark Action Module.

Watermarking for data provenance tracking,
embeds invisible markers for data origin verification.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass
import logging
import hashlib
import json
import time
import random

logger = logging.getLogger(__name__)


@dataclass
class WatermarkInfo:
    """Watermark metadata."""
    watermark_id: str
    embedded_at: float
    source: str
    version: str
    payload: dict[str, Any]


class DataWatermarkAction:
    """
    Data watermarking for provenance tracking.

    Embeds invisible markers in data structures
    for verification of origin and integrity.

    Example:
        wm = DataWatermarkAction(secret_key="my_secret")
        watermarked = wm.embed(original_data, source="upstream_service")
        is_valid = wm.verify(watermarked)
    """

    def __init__(
        self,
        secret_key: str = "",
        watermark_version: str = "1.0",
    ) -> None:
        self.secret_key = secret_key
        self.watermark_version = watermark_version
        self._watermark_key = "_wm"

    def embed(
        self,
        data: dict,
        source: str = "unknown",
        payload: Optional[dict[str, Any]] = None,
        watermark_id: Optional[str] = None,
    ) -> dict:
        """Embed watermark into data."""
        import copy
        result = copy.deepcopy(data)

        watermark_id = watermark_id or self._generate_id(source)

        watermark = WatermarkInfo(
            watermark_id=watermark_id,
            embedded_at=time.time(),
            source=source,
            version=self.watermark_version,
            payload=payload or {},
        )

        watermark_dict = {
            "id": watermark.watermark_id,
            "at": watermark.embedded_at,
            "src": watermark.source,
            "ver": watermark.version,
            "pl": watermark.payload,
            "sig": self._sign(watermark),
        }

        result[self._watermark_key] = watermark_dict

        return result

    def verify(
        self,
        data: dict,
        strict: bool = True,
    ) -> bool:
        """Verify watermark in data."""
        if self._watermark_key not in data:
            return False

        wm_data = data[self._watermark_key]

        required_fields = ["id", "at", "src", "ver", "sig"]
        for field in required_fields:
            if field not in wm_data:
                return False

        expected_sig = self._sign_from_dict(wm_data)

        if strict and wm_data["sig"] != expected_sig:
            logger.warning("Watermark signature mismatch")
            return False

        return True

    def extract(self, data: dict) -> Optional[WatermarkInfo]:
        """Extract watermark information from data."""
        if self._watermark_key not in data:
            return None

        wm_data = data[self._watermark_key]

        return WatermarkInfo(
            watermark_id=wm_data.get("id", ""),
            embedded_at=wm_data.get("at", 0.0),
            source=wm_data.get("src", "unknown"),
            version=wm_data.get("ver", ""),
            payload=wm_data.get("pl", {}),
        )

    def strip(self, data: dict) -> dict:
        """Remove watermark from data."""
        import copy
        result = copy.deepcopy(data)

        if self._watermark_key in result:
            del result[self._watermark_key]

        return result

    def rewatermark(
        self,
        data: dict,
        new_source: Optional[str] = None,
    ) -> dict:
        """Replace existing watermark with new one."""
        info = self.extract(data)

        if info is None:
            raise ValueError("No watermark found to rewatermark")

        return self.embed(
            data=self.strip(data),
            source=new_source or info.source,
            payload=info.payload,
        )

    def _generate_id(self, source: str) -> str:
        """Generate unique watermark ID."""
        data = f"{source}:{time.time()}:{random.random()}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def _sign(self, info: WatermarkInfo) -> str:
        """Generate signature for watermark info."""
        data = f"{info.watermark_id}:{info.embedded_at}:{info.source}:{self.secret_key}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def _sign_from_dict(self, wm_data: dict) -> str:
        """Generate signature from dict representation."""
        data = f"{wm_data['id']}:{wm_data['at']}:{wm_data['src']}:{self.secret_key}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
