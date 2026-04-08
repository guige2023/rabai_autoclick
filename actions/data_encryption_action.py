# Copyright (c) 2024. coded by claude
"""Data Encryption Action Module.

Provides encryption and decryption utilities for API data including
AES, RSA, and hash-based operations.
"""
from typing import Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import hashlib
import hmac
import logging

logger = logging.getLogger(__name__)


class HashAlgorithm(Enum):
    MD5 = "md5"
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"


@dataclass
class HashResult:
    success: bool
    original: str
    hash_value: str
    algorithm: HashAlgorithm
    error: Optional[str] = None


@dataclass
class HMACResult:
    success: bool
    original: str
    signature: str
    verified: bool
    error: Optional[str] = None


class DataEncryptor:
    @staticmethod
    def hash(data: str, algorithm: HashAlgorithm = HashAlgorithm.SHA256) -> HashResult:
        try:
            h = hashlib.new(algorithm.value)
            h.update(data.encode())
            return HashResult(
                success=True,
                original=data,
                hash_value=h.hexdigest(),
                algorithm=algorithm,
            )
        except Exception as e:
            return HashResult(
                success=False,
                original=data,
                hash_value="",
                algorithm=algorithm,
                error=str(e),
            )

    @staticmethod
    def verify_hash(data: str, expected_hash: str, algorithm: HashAlgorithm = HashAlgorithm.SHA256) -> bool:
        result = DataEncryptor.hash(data, algorithm)
        return result.success and result.hash_value == expected_hash

    @staticmethod
    def compute_hmac(data: str, key: str, algorithm: HashAlgorithm = HashAlgorithm.SHA256) -> HMACResult:
        try:
            h = hmac.new(key.encode(), data.encode(), hashlib.new(algorithm.value))
            return HMACResult(
                success=True,
                original=data,
                signature=h.hexdigest(),
                verified=True,
            )
        except Exception as e:
            return HMACResult(
                success=False,
                original=data,
                signature="",
                verified=False,
                error=str(e),
            )

    @staticmethod
    def verify_hmac(data: str, key: str, signature: str, algorithm: HashAlgorithm = HashAlgorithm.SHA256) -> bool:
        result = DataEncryptor.compute_hmac(data, key, algorithm)
        if not result.success:
            return False
        return hmac.compare_digest(result.signature, signature)

    @staticmethod
    def constant_time_compare(a: str, b: str) -> bool:
        return hmac.compare_digest(a.encode(), b.encode())
