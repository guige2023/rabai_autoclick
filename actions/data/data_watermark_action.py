"""Data watermarking for automation workflows.

Provides invisible and visible watermarking for data tracking,
ownership verification, and tamper detection.
"""

from __future__ import annotations

import hashlib
import json
import random
import struct
import threading
import time
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import copy


class WatermarkType(Enum):
    """Type of watermark."""
    INVISIBLE = "invisible"
    VISIBLE = "visible"
    ROBUST = "robust"
    FRAGILE = "fragile"


class WatermarkEncoding(Enum):
    """Encoding method for watermarks."""
    LSB = "lsb"
    HASH_BASED = "hash_based"
    PARITY = "parity"
    BINARY = "binary"


@dataclass
class Watermark:
    """A data watermark."""
    watermark_id: str
    content: str
    wtype: WatermarkType
    encoding: WatermarkEncoding
    created_at: float = field(default_factory=time.time)
    owner: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    strength: float = 1.0
    seed: Optional[int] = None


@dataclass
class WatermarkExtraction:
    """Result of watermark extraction."""
    watermark_id: Optional[str]
    content: Optional[str]
    found: bool
    confidence: float
    timestamp: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class WatermarkEncoder:
    """Encodes watermarks into data."""

    def __init__(self, default_encoding: WatermarkEncoding = WatermarkEncoding.LSB):
        self._default_encoding = default_encoding
        self._registry: Dict[str, Watermark] = {}

    def _generate_watermark_id(self) -> str:
        """Generate a unique watermark ID."""
        return f"wm_{uuid.uuid4().hex[:12]}"

    def _string_to_bits(self, s: str) -> List[int]:
        """Convert string to bit array."""
        bits = []
        for char in s:
            bits.extend([int(b) for b in format(ord(char), '08b')])
        return bits

    def _bits_to_string(self, bits: List[int]) -> str:
        """Convert bit array back to string."""
        chars = []
        for i in range(0, len(bits), 8):
            byte = bits[i:i+8]
            if len(byte) == 8:
                chars.append(chr(int(''.join(str(b) for b in byte), 2)))
        return ''.join(chars)

    def _lsb_encode(
        self,
        data: str,
        watermark_bits: List[int],
        seed: Optional[int] = None,
    ) -> str:
        """Encode watermark using LSB (Least Significant Bit) steganography."""
        if seed is not None:
            random.seed(seed)

        data_bytes = data.encode('utf-8') if isinstance(data, str) else data
        data_bits = [int(b) for b in format(len(data_bytes), '032b')]

        total_bits_needed = len(data_bits) + len(watermark_bits)
        if len(data_bits) < total_bits_needed:
            padding = [0] * (total_bits_needed - len(data_bits))
            data_bits.extend(padding)

        result = bytearray(data_bits[:len(data_bits)])
        for i, bit in enumerate(watermark_bits):
            if i < len(result):
                result[i] = (result[i] & 0xFE) | bit

        return bytes(result)

    def _lsb_decode(self, data: bytes, length_hint: Optional[int] = None) -> str:
        """Decode watermark from LSB encoded data."""
        data_bits = list(data)

        if length_hint:
            length = length_hint
        else:
            length_bits = data_bits[:32]
            length = int(''.join(str(b) for b in length_bits), 2)

        actual_length = min(length, len(data_bits) - 32)
        watermark_bits = data_bits[32:32 + actual_length]

        return self._bits_to_string(watermark_bits)

    def _hash_encode(
        self,
        data: str,
        watermark_content: str,
        seed: Optional[int] = None,
    ) -> str:
        """Create hash-based watermark marker."""
        if seed is not None:
            random.seed(seed)

        combined = f"{data}:{watermark_content}:{seed or time.time()}"
        marker = hashlib.sha256(combined.encode()).hexdigest()[:16]
        return f"{data}::{marker}::{watermark_content}"

    def _hash_decode(self, data: str) -> Optional[Tuple[str, str]]:
        """Extract hash-based watermark."""
        if "::" not in data:
            return None

        parts = data.split("::")
        if len(parts) != 3:
            return None

        _, marker, content = parts
        return (marker, content)

    def encode(
        self,
        data: str,
        content: str,
        wtype: WatermarkType = WatermarkType.INVISIBLE,
        encoding: Optional[WatermarkEncoding] = None,
        owner: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        strength: float = 1.0,
    ) -> Tuple[str, Watermark]:
        """Encode a watermark into data."""
        encoding = encoding or self._default_encoding
        watermark_id = self._generate_watermark_id()
        seed = random.randint(0, 2**31)

        watermark = Watermark(
            watermark_id=watermark_id,
            content=content,
            wtype=wtype,
            encoding=encoding,
            owner=owner,
            metadata=metadata or {},
            strength=strength,
            seed=seed,
        )

        self._registry[watermark_id] = watermark

        if encoding == WatermarkEncoding.LSB:
            if not isinstance(data, bytes):
                data_bytes = data.encode('utf-8')
            else:
                data_bytes = data
            watermark_bits = self._string_to_bits(content)
            encoded = self._lsb_encode(data_bytes, watermark_bits, seed)
            return encoded, watermark

        elif encoding == WatermarkEncoding.HASH_BASED:
            encoded = self._hash_encode(data, content, seed)
            return encoded, watermark

        elif encoding == WatermarkEncoding.PARITY:
            words = data.split()
            watermark_bits = self._string_to_bits(content)
            result_words = []
            for i, word in enumerate(words):
                if i < len(watermark_bits):
                    bit = watermark_bits[i]
                    parity = sum(ord(c) for c in word) % 2
                    if parity != bit:
                        word = word + ('!' if bit else '.')
                result_words.append(word)
            return ' '.join(result_words), watermark

        else:
            return data, watermark

    def decode(
        self,
        data: Union[str, bytes],
        watermark_id: Optional[str] = None,
        encoding: Optional[WatermarkEncoding] = None,
    ) -> WatermarkExtraction:
        """Attempt to extract a watermark from data."""
        encoding = encoding or self._default_encoding

        if encoding == WatermarkEncoding.LSB:
            if not isinstance(data, bytes):
                return WatermarkExtraction(
                    watermark_id=None,
                    content=None,
                    found=False,
                    confidence=0.0,
                    metadata={"error": "LSB requires bytes data"},
                )
            try:
                content = self._lsb_decode(data)
                if content:
                    return WatermarkExtraction(
                        watermark_id=None,
                        content=content,
                        found=True,
                        confidence=0.9,
                    )
            except Exception:
                pass

        elif encoding == WatermarkEncoding.HASH_BASED:
            if isinstance(data, bytes):
                data = data.decode('utf-8', errors='ignore')
            result = self._hash_decode(data)
            if result:
                marker, content = result
                return WatermarkExtraction(
                    watermark_id=None,
                    content=content,
                    found=True,
                    confidence=1.0,
                    metadata={"marker": marker},
                )

        return WatermarkExtraction(
            watermark_id=None,
            content=None,
            found=False,
            confidence=0.0,
        )

    def verify(
        self,
        data: Union[str, bytes],
        watermark_id: str,
    ) -> Tuple[bool, float]:
        """Verify if a specific watermark is present in data."""
        watermark = self._registry.get(watermark_id)
        if not watermark:
            return False, 0.0

        extraction = self.decode(
            data,
            watermark_id=watermark_id,
            encoding=watermark.encoding,
        )

        if extraction.found and extraction.content == watermark.content:
            return True, extraction.confidence * watermark.strength

        return False, 0.0


class AutomationWatermarkAction:
    """Action providing data watermarking for automation workflows."""

    def __init__(self, encoder: Optional[WatermarkEncoder] = None):
        self._encoder = encoder or WatermarkEncoder()
        self._embedded_tracking: Dict[str, List[str]] = {}

    def embed(
        self,
        data: str,
        content: str,
        wtype: str = "invisible",
        encoding: str = "lsb",
        owner: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        strength: float = 1.0,
    ) -> Dict[str, Any]:
        """Embed a watermark into data."""
        try:
            wtype_enum = WatermarkType(wtype.lower())
        except ValueError:
            wtype_enum = WatermarkType.INVISIBLE

        try:
            encoding_enum = WatermarkEncoding(encoding.lower())
        except ValueError:
            encoding_enum = WatermarkEncoding.LSB

        encoded_data, watermark = self._encoder.encode(
            data=data,
            content=content,
            wtype=wtype_enum,
            encoding=encoding_enum,
            owner=owner,
            metadata=metadata,
            strength=strength,
        )

        self._embedded_tracking.setdefault(watermark.watermark_id, []).append(
            str(time.time())
        )

        return {
            "watermark_id": watermark.watermark_id,
            "encoded_data": encoded_data,
            "type": watermark.wtype.value,
            "encoding": watermark.encoding.value,
            "owner": watermark.owner,
            "created_at": datetime.fromtimestamp(watermark.created_at).isoformat(),
        }

    def extract(
        self,
        data: Union[str, bytes],
        encoding: str = "lsb",
    ) -> Dict[str, Any]:
        """Extract a watermark from data."""
        try:
            encoding_enum = WatermarkEncoding(encoding.lower())
        except ValueError:
            encoding_enum = WatermarkEncoding.LSB

        extraction = self._encoder.decode(data, encoding=encoding_enum)

        return {
            "found": extraction.found,
            "watermark_id": extraction.watermark_id,
            "content": extraction.content,
            "confidence": extraction.confidence,
            "timestamp": (
                datetime.fromtimestamp(extraction.timestamp).isoformat()
                if extraction.timestamp else None
            ),
            "metadata": extraction.metadata,
        }

    def verify(
        self,
        data: Union[str, bytes],
        watermark_id: str,
    ) -> Dict[str, Any]:
        """Verify a watermark is present."""
        verified, confidence = self._encoder.verify(data, watermark_id)
        return {
            "verified": verified,
            "confidence": confidence,
            "watermark_id": watermark_id,
        }

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a watermarking operation.

        Required params:
            operation: str - 'embed', 'extract', or 'verify'
            data: str - Data to watermark or extract from

        For embed operation:
            content: str - Watermark content to embed
            type: str - Watermark type
            encoding: str - Encoding method

        For verify operation:
            watermark_id: str - ID of watermark to verify
        """
        operation = params.get("operation")
        data = params.get("data")

        if not data:
            raise ValueError("data is required")

        if operation == "embed":
            content = params.get("content")
            if not content:
                raise ValueError("content is required for embed operation")
            wtype = params.get("type", "invisible")
            encoding = params.get("encoding", "lsb")
            owner = params.get("owner")
            metadata = params.get("metadata")
            strength = params.get("strength", 1.0)
            return self.embed(
                data=data,
                content=content,
                wtype=wtype,
                encoding=encoding,
                owner=owner,
                metadata=metadata,
                strength=strength,
            )

        elif operation == "extract":
            encoding = params.get("encoding", "lsb")
            return self.extract(data=data, encoding=encoding)

        elif operation == "verify":
            watermark_id = params.get("watermark_id")
            if not watermark_id:
                raise ValueError("watermark_id is required for verify operation")
            return self.verify(data=data, watermark_id=watermark_id)

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def get_registry(self) -> List[Dict[str, Any]]:
        """Get all registered watermarks."""
        return [
            {
                "watermark_id": wm.watermark_id,
                "content": wm.content,
                "type": wm.wtype.value,
                "encoding": wm.encoding.value,
                "owner": wm.owner,
                "created_at": datetime.fromtimestamp(wm.created_at).isoformat(),
                "embed_count": len(self._embedded_tracking.get(wm.watermark_id, [])),
            }
            for wm in self._encoder._registry.values()
        ]
