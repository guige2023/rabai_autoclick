"""Data Watermark Action Module.

Provides watermarking capabilities for data provenance including
invisible watermarks, visible stamps, cryptographic signatures,
and reversible data watermarking for data lineage tracking.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class WatermarkType(Enum):
    """Types of watermarks."""
    INVISIBLE = "invisible"
    VISIBLE = "visible"
    METADATA = "metadata"
    CRYPTOGRAPHIC = "cryptographic"
    ROBUST = "robust"
    FRAGILE = "fragile"


class WatermarkStatus(Enum):
    """Watermark status."""
    VALID = "valid"
    INVALID = "invalid"
    TAMPERED = "tampered"
    EXPIRED = "expired"
    MISSING = "missing"


@dataclass
class Watermark:
    """A data watermark."""
    watermark_id: str
    watermark_type: WatermarkType
    content: str
    signature: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    payload: Optional[Dict[str, Any]] = None


@dataclass
class WatermarkVerification:
    """Result of watermark verification."""
    status: WatermarkStatus
    watermark_id: Optional[str] = None
    extracted_payload: Optional[Dict[str, Any]] = None
    verification_time_ms: float = 0.0
    integrity_score: float = 0.0
    message: Optional[str] = None


@dataclass
class WatermarkConfig:
    """Configuration for watermarking."""
    watermark_type: WatermarkType = WatermarkType.INVISIBLE
    secret_key: Optional[str] = None
    hash_algorithm: str = "sha256"
    include_timestamp: bool = True
    include_checksum: bool = True
    embed_in_json: bool = True
    embed_in_binary: bool = False
    expiration_hours: int = 0


class WatermarkEncoder:
    """Encode watermarks into data."""

    @staticmethod
    def encode_metadata(
        data: Dict[str, Any],
        watermark: Watermark,
        config: WatermarkConfig
    ) -> Dict[str, Any]:
        """Embed watermark as metadata in JSON data."""
        marked_data = data.copy()

        watermark_info = {
            "watermark_id": watermark.watermark_id,
            "watermark_type": watermark.watermark_type.value,
            "content": watermark.content,
            "created_at": watermark.created_at.isoformat() if config.include_timestamp else None,
        }

        if config.include_checksum:
            content_str = json.dumps(data, sort_keys=True, default=str)
            watermark_info["data_checksum"] = hashlib.sha256(
                content_str.encode()
            ).hexdigest()

        if watermark.expires_at:
            watermark_info["expires_at"] = watermark.expires_at.isoformat()

        watermark_info["metadata"] = watermark.metadata

        marked_data["_watermark"] = watermark_info

        return marked_data

    @staticmethod
    def encode_binary(
        data: bytes,
        watermark: Watermark,
        config: WatermarkConfig
    ) -> bytes:
        """Embed watermark in binary data using LSB steganography."""
        if len(data) < 100:
            raise ValueError("Data too small for watermarking")

        header = b"WATERMARK:"
        header_len = len(header)

        watermark_json = json.dumps({
            "id": watermark.watermark_id,
            "type": watermark.watermark_type.value,
            "content": watermark.content,
            "timestamp": watermark.created_at.isoformat()
        }, separators=(",", ":"))

        watermark_bytes = watermark_json.encode("utf-8")
        watermark_len = len(watermark_bytes)

        total_header = header + watermark_len.to_bytes(4, "big") + watermark_bytes

        if len(total_header) > len(data):
            raise ValueError("Data too small for watermark")

        result = bytearray(data)
        for i, byte in enumerate(total_header):
            result[i] = (data[i] & 0xFE) | (byte & 0x01)

        return bytes(result)

    @staticmethod
    def sign_watermark(watermark: Watermark, secret_key: str) -> str:
        """Create HMAC signature for watermark."""
        content = f"{watermark.watermark_id}:{watermark.content}:{watermark.created_at.isoformat()}"
        signature = hmac.new(
            secret_key.encode(),
            content.encode(),
            hashlib.sha256
        ).hexdigest()
        return signature


class WatermarkExtractor:
    """Extract and verify watermarks from data."""

    @staticmethod
    def extract_from_metadata(data: Dict[str, Any]) -> Tuple[Optional[Watermark], WatermarkVerification]:
        """Extract watermark from JSON metadata."""
        if "_watermark" not in data:
            return None, WatermarkVerification(
                status=WatermarkStatus.MISSING,
                message="No watermark found in data"
            )

        wm_info = data["_watermark"]
        start_time = time.time()

        watermark = Watermark(
            watermark_id=wm_info.get("watermark_id", ""),
            watermark_type=WatermarkType(wm_info.get("watermark_type", "invisible")),
            content=wm_info.get("content", ""),
            created_at=datetime.fromisoformat(wm_info["created_at"]) if "created_at" in wm_info else datetime.now()
        )

        if "expires_at" in wm_info and wm_info["expires_at"]:
            expires_at = datetime.fromisoformat(wm_info["expires_at"])
            if datetime.now() > expires_at:
                return watermark, WatermarkVerification(
                    status=WatermarkStatus.EXPIRED,
                    watermark_id=watermark.watermark_id,
                    verification_time_ms=(time.time() - start_time) * 1000,
                    message="Watermark has expired"
                )

        integrity_score = 1.0

        verification = WatermarkVerification(
            status=WatermarkStatus.VALID,
            watermark_id=watermark.watermark_id,
            verification_time_ms=(time.time() - start_time) * 1000,
            integrity_score=integrity_score,
            extracted_payload=wm_info.get("metadata", {})
        )

        return watermark, verification

    @staticmethod
    def extract_from_binary(data: bytes, config: WatermarkConfig) -> Tuple[Optional[Watermark], WatermarkVerification]:
        """Extract watermark from binary data."""
        header = b"WATERMARK:"
        header_len = len(header)

        if len(data) < header_len + 4:
            return None, WatermarkVerification(
                status=WatermarkStatus.MISSING,
                message="Data too small for watermark"
            )

        if data[:len(header)] != header:
            return None, WatermarkVerification(
                status=WatermarkStatus.INVALID,
                message="Invalid watermark header"
            )

        try:
            watermark_len = int.from_bytes(data[header_len:header_len + 4], "big")
            watermark_bytes = data[header_len + 4:header_len + 4 + watermark_len]
            watermark_json = watermark_bytes.decode("utf-8")
            wm_info = json.loads(watermark_json)

            watermark = Watermark(
                watermark_id=wm_info.get("id", ""),
                watermark_type=WatermarkType(wm_info.get("type", "invisible")),
                content=wm_info.get("content", ""),
                created_at=datetime.fromisoformat(wm_info["timestamp"]) if "timestamp" in wm_info else datetime.now()
            )

            return watermark, WatermarkVerification(
                status=WatermarkStatus.VALID,
                watermark_id=watermark.watermark_id,
                verification_time_ms=0.0
            )

        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            return None, WatermarkVerification(
                status=WatermarkStatus.INVALID,
                message="Failed to parse watermark"
            )


class DataWatermarkAction(BaseAction):
    """Action for data watermarking."""

    def __init__(self):
        super().__init__(name="data_watermark")
        self._config = WatermarkConfig()
        self._encoder = WatermarkEncoder()
        self._extractor = WatermarkExtractor()
        self._watermarks: Dict[str, Watermark] = {}
        self._lock = threading.Lock()
        self._verification_history: List[WatermarkVerification] = []

    def configure(self, config: WatermarkConfig):
        """Configure watermarking settings."""
        self._config = config

    def embed(
        self,
        data: Any,
        content: str,
        watermark_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None
    ) -> ActionResult:
        """Embed a watermark in data."""
        try:
            watermark_id = watermark_id or f"wm_{int(time.time() * 1000)}"

            if watermark_id in self._watermarks:
                return ActionResult(success=False, error=f"Watermark {watermark_id} already exists")

            expires_at = None
            if self._config.expiration_hours > 0:
                expires_at = datetime.now() + timedelta(hours=self._config.expiration_hours)

            watermark = Watermark(
                watermark_id=watermark_id,
                watermark_type=self._config.watermark_type,
                content=content,
                created_at=datetime.now(),
                expires_at=expires_at,
                metadata=metadata or {},
                payload=payload
            )

            if self._config.secret_key:
                watermark.signature = self._encoder.sign_watermark(watermark, self._config.secret_key)

            if isinstance(data, dict):
                marked_data = self._encoder.encode_metadata(data, watermark, self._config)
            elif isinstance(data, bytes):
                marked_data = self._encoder.encode_binary(data, watermark, self._config)
            else:
                marked_data = data

            with self._lock:
                self._watermarks[watermark_id] = watermark

            return ActionResult(
                success=True,
                data={
                    "watermark_id": watermark_id,
                    "marked_data": marked_data,
                    "created_at": watermark.created_at.isoformat()
                }
            )
        except Exception as e:
            logger.exception("Watermark embedding failed")
            return ActionResult(success=False, error=str(e))

    def verify(self, data: Any) -> ActionResult:
        """Verify watermark in data."""
        start_time = time.time()

        try:
            if isinstance(data, dict):
                watermark, verification = self._extractor.extract_from_metadata(data)
            elif isinstance(data, bytes):
                watermark, verification = self._extractor.extract_from_binary(data, self._config)
            else:
                return ActionResult(success=False, error="Unsupported data type")

            verification.verification_time_ms = (time.time() - start_time) * 1000

            with self._lock:
                self._verification_history.append(verification)

            return ActionResult(
                success=verification.status == WatermarkStatus.VALID,
                data={
                    "status": verification.status.value,
                    "watermark_id": verification.watermark_id,
                    "verification_time_ms": verification.verification_time_ms,
                    "integrity_score": verification.integrity_score,
                    "payload": verification.extracted_payload,
                    "message": verification.message
                }
            )
        except Exception as e:
            logger.exception("Watermark verification failed")
            return ActionResult(success=False, error=str(e))

    def extract(self, data: Any) -> ActionResult:
        """Extract watermark from data without verification."""
        try:
            if isinstance(data, dict):
                watermark, _ = self._extractor.extract_from_metadata(data)
            elif isinstance(data, bytes):
                watermark, _ = self._extractor.extract_from_binary(data, self._config)
            else:
                return ActionResult(success=False, error="Unsupported data type")

            if not watermark:
                return ActionResult(success=False, error="No watermark found")

            return ActionResult(
                success=True,
                data={
                    "watermark_id": watermark.watermark_id,
                    "type": watermark.watermark_type.value,
                    "content": watermark.content,
                    "created_at": watermark.created_at.isoformat(),
                    "metadata": watermark.metadata
                }
            )
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def get_watermarks(self) -> List[Watermark]:
        """Get all registered watermarks."""
        with self._lock:
            return list(self._watermarks.values())

    def get_history(self) -> List[WatermarkVerification]:
        """Get watermark verification history."""
        with self._lock:
            return self._verification_history.copy()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute watermarking action."""
        try:
            action = params.get("action", "embed")

            if action == "embed":
                return self.embed(
                    params["data"],
                    params["content"],
                    params.get("watermark_id"),
                    params.get("metadata"),
                    params.get("payload")
                )
            elif action == "verify":
                return self.verify(params["data"])
            elif action == "extract":
                return self.extract(params["data"])
            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
