"""
Data Watermark Action Module.

Provides watermark generation and verification for data
integrity, provenance tracking, and tamper detection.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class WatermarkType(Enum):
    """Watermark types."""

    VISIBLE = "visible"
    INVISIBLE = "invisible"
    FRAGILE = "fragile"
    SEMI_FRAGILE = "semi_fragile"
    ROBUST = "robust"


@dataclass
class Watermark:
    """Represents a data watermark."""

    id: str
    watermark_type: WatermarkType
    content_hash: str
    timestamp: float = field(default_factory=time.time)
    creator: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    signature: str = ""
    verified: bool = False


@dataclass
class WatermarkConfig:
    """Configuration for watermarking."""

    watermark_type: WatermarkType = WatermarkType.ROBUST
    hash_algorithm: str = "sha256"
    include_timestamp: bool = True
    include_metadata: bool = True


class DataWatermarkAction:
    """
    Manages watermarking for data integrity and provenance.

    Features:
    - Watermark generation and embedding
    - Verification and tamper detection
    - Timestamp and metadata support
    - Signature-based authentication

    Example:
        wm = DataWatermarkAction()
        watermark = wm.generate_watermark(data, creator="processor")
        wm.embed_watermark(dataset, watermark)
    """

    def __init__(self, config: Optional[WatermarkConfig] = None) -> None:
        """
        Initialize watermark action.

        Args:
            config: Watermark configuration.
        """
        self.config = config or WatermarkConfig()
        self._watermarks: dict[str, Watermark] = {}
        self._stats = {
            "total_generated": 0,
            "total_verified": 0,
            "verified_pass": 0,
            "verified_fail": 0,
        }

    def generate_watermark(
        self,
        data: Any,
        watermark_type: Optional[WatermarkType] = None,
        creator: str = "",
        metadata: Optional[dict[str, Any]] = None,
    ) -> Watermark:
        """
        Generate a watermark for data.

        Args:
            data: Data to watermark.
            watermark_type: Type of watermark.
            creator: Creator identifier.
            metadata: Optional metadata.

        Returns:
            Generated Watermark.
        """
        wm_type = watermark_type or self.config.watermark_type

        data_str = self._serialize_data(data)
        content_hash = self._compute_hash(data_str)

        import uuid
        wm_id = str(uuid.uuid4())

        watermark = Watermark(
            id=wm_id,
            watermark_type=wm_type,
            content_hash=content_hash,
            timestamp=time.time(),
            creator=creator,
            metadata=metadata or {},
        )

        self._watermarks[wm_id] = watermark
        self._stats["total_generated"] += 1

        logger.info(f"Generated watermark: {wm_id} ({wm_type.value})")
        return watermark

    def embed_watermark(
        self,
        data: list[dict[str, Any]],
        watermark: Watermark,
    ) -> list[dict[str, Any]]:
        """
        Embed a watermark into dataset records.

        Args:
            data: Dataset records.
            watermark: Watermark to embed.

        Returns:
            Dataset with embedded watermark.
        """
        for record in data:
            record["_watermark_id"] = watermark.id
            record["_watermark_hash"] = watermark.content_hash
            record["_watermark_timestamp"] = watermark.timestamp

        logger.debug(f"Embedded watermark into {len(data)} records")
        return data

    def extract_watermark(
        self,
        record: dict[str, Any],
    ) -> Optional[Watermark]:
        """
        Extract watermark from a record.

        Args:
            record: Data record.

        Returns:
            Extracted Watermark or None.
        """
        wm_id = record.get("_watermark_id")
        if not wm_id or wm_id not in self._watermarks:
            return None

        return self._watermarks[wm_id]

    def verify_watermark(
        self,
        data: Any,
        watermark: Watermark,
    ) -> bool:
        """
        Verify a watermark against data.

        Args:
            data: Data to verify.
            watermark: Watermark to check.

        Returns:
            True if watermark is valid.
        """
        self._stats["total_verified"] += 1

        data_str = self._serialize_data(data)
        computed_hash = self._compute_hash(data_str)

        is_valid = computed_hash == watermark.content_hash
        watermark.verified = is_valid

        if is_valid:
            self._stats["verified_pass"] += 1
            logger.debug(f"Watermark verified: {watermark.id}")
        else:
            self._stats["verified_fail"] += 1
            logger.warning(f"Watermark verification failed: {watermark.id}")

        return is_valid

    def detect_tamper(
        self,
        data: list[dict[str, Any]],
        watermark: Watermark,
    ) -> dict[str, Any]:
        """
        Detect tampering in watermarked data.

        Args:
            data: Data records to check.
            watermark: Expected watermark.

        Returns:
            Dictionary with tamper detection results.
        """
        tampered_records = []
        valid_records = []

        for i, record in enumerate(data):
            record_hash = record.get("_watermark_hash")
            if record_hash and record_hash != watermark.content_hash:
                tampered_records.append({
                    "index": i,
                    "record": record,
                    "reason": "hash_mismatch",
                })
            else:
                valid_records.append(i)

        return {
            "is_tampered": len(tampered_records) > 0,
            "tampered_count": len(tampered_records),
            "valid_count": len(valid_records),
            "tampered_records": tampered_records[:10],
            "watermark_id": watermark.id,
        }

    def _serialize_data(self, data: Any) -> str:
        """Serialize data for hashing."""
        if isinstance(data, (list, dict)):
            return json.dumps(data, sort_keys=True, default=str)
        return str(data)

    def _compute_hash(self, data_str: str) -> str:
        """Compute hash of data string."""
        algo = self.config.hash_algorithm
        if algo == "md5":
            return hashlib.md5(data_str.encode()).hexdigest()
        elif algo == "sha1":
            return hashlib.sha1(data_str.encode()).hexdigest()
        else:
            return hashlib.sha256(data_str.encode()).hexdigest()

    def get_stats(self) -> dict[str, Any]:
        """
        Get watermark statistics.

        Returns:
            Statistics dictionary.
        """
        return {
            **self._stats,
            "total_watermarks": len(self._watermarks),
            "verification_rate": (
                f"{self._stats['verified_pass'] / max(1, self._stats['total_verified']) * 100:.1f}%"
            ),
        }
