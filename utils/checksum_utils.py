"""Checksum and data integrity utilities: MD5, SHA, CRC, parity, and HMAC."""

from __future__ import annotations

import hashlib
import hmac
import struct
from typing import Any

__all__ = [
    "ChecksumConfig",
    "compute_checksum",
    "verify_checksum",
    "compute_file_checksum",
    "ParityChecker",
]


def compute_checksum(
    data: bytes,
    algorithm: str = "sha256",
    encode: bool = True,
) -> str | bytes:
    """Compute checksum of data using specified algorithm."""
    algorithms = {
        "md5": hashlib.md5,
        "sha1": hashlib.sha1,
        "sha256": hashlib.sha256,
        "sha512": hashlib.sha512,
        "blake2b": hashlib.blake2b,
        "blake2s": hashlib.blake2s,
    }
    if algorithm not in algorithms:
        raise ValueError(f"Unknown algorithm: {algorithm}")

    hasher = algorithms[algorithm]()
    hasher.update(data)
    result = hasher.digest()
    return result if not encode else result.hex()


def verify_checksum(
    data: bytes,
    expected: str,
    algorithm: str = "sha256",
) -> bool:
    """Verify data checksum against expected value."""
    computed = compute_checksum(data, algorithm, encode=True)
    return hmac.compare_digest(computed.lower(), expected.lower())


def compute_file_checksum(
    filepath: str,
    algorithm: str = "sha256",
    chunk_size: int = 8192,
) -> str:
    """Compute checksum of a file by streaming chunks."""
    algorithms = {
        "md5": hashlib.md5,
        "sha1": hashlib.sha1,
        "sha256": hashlib.sha256,
        "sha512": hashlib.sha512,
    }
    if algorithm not in algorithms:
        raise ValueError(f"Unknown algorithm: {algorithm}")

    hasher = algorithms[algorithm]()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


class ParityChecker:
    """Parity checking for data integrity."""

    @staticmethod
    def compute_parity(data: bytes) -> int:
        """Compute odd parity of data."""
        return sum(bin(b).count("1") for b in data) % 2

    @staticmethod
    def compute_crc16(data: bytes, polynomial: int = 0x8005) -> int:
        """Compute CRC-16 checksum."""
        crc = 0xFFFF
        for byte in data:
            crc ^= byte << 8
            for _ in range(8):
                if crc & 0x8000:
                    crc = (crc << 1) ^ polynomial
                else:
                    crc = crc << 1
                crc &= 0xFFFF
        return crc

    @staticmethod
    def compute_crc32(data: bytes) -> int:
        """Compute CRC-32 checksum."""
        return hashlib.crc32(data) & 0xFFFFFFFF


class HMACChecksum:
    """HMAC-based checksums for authentication."""

    @staticmethod
    def compute(
        data: bytes,
        key: bytes,
        algorithm: str = "sha256",
    ) -> str:
        return hmac.new(key, data, getattr(hashlib, algorithm)).hexdigest()

    @staticmethod
    def verify(
        data: bytes,
        key: bytes,
        expected: str,
        algorithm: str = "sha256",
    ) -> bool:
        computed = HMACChecksum.compute(data, key, algorithm)
        return hmac.compare_digest(computed.lower(), expected.lower())
