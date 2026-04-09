"""Data hashing action module for RabAI AutoClick.

Provides data hashing operations:
- HashGeneratorAction: Generate various hashes of data
- HashVerifierAction: Verify data integrity using hashes
- HMACGeneratorAction: Generate HMAC codes
- ChecksumCalculatorAction: Calculate checksums
"""

import sys
import os
import hashlib
import hmac
import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


class HashGenerator:
    """Generate various cryptographic hashes."""

    ALGORITHMS = ["md5", "sha1", "sha256", "sha384", "sha512", "sha3_256", "sha3_512", "blake2b", "blake2s"]

    @staticmethod
    def hash(data: bytes, algorithm: str) -> str:
        if algorithm == "md5":
            return hashlib.md5(data).hexdigest()
        elif algorithm == "sha1":
            return hashlib.sha1(data).hexdigest()
        elif algorithm == "sha256":
            return hashlib.sha256(data).hexdigest()
        elif algorithm == "sha384":
            return hashlib.sha384(data).hexdigest()
        elif algorithm == "sha512":
            return hashlib.sha512(data).hexdigest()
        elif algorithm == "sha3_256":
            return hashlib.sha3_256(data).hexdigest()
        elif algorithm == "sha3_512":
            return hashlib.sha3_512(data).hexdigest()
        elif algorithm == "blake2b":
            return hashlib.blake2b(data).hexdigest()
        elif algorithm == "blake2s":
            return hashlib.blake2s(data).hexdigest()
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

    @staticmethod
    def hash_file(file_path: str, algorithm: str, chunk_size: int = 8192) -> str:
        hasher = hashlib.new(algorithm)
        with open(file_path, "rb") as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def list_algorithms() -> List[str]:
        return HashGenerator.ALGORITHMS.copy()


class HashVerifier:
    """Verify data integrity using hashes."""

    @staticmethod
    def verify(data: bytes, expected_hash: str, algorithm: str) -> bool:
        actual = HashGenerator.hash(data, algorithm)
        return hmac.compare_digest(actual.lower(), expected_hash.lower())

    @staticmethod
    def verify_file(file_path: str, expected_hash: str, algorithm: str) -> bool:
        actual = HashGenerator.hash_file(file_path, algorithm)
        return hmac.compare_digest(actual.lower(), expected_hash.lower())


class HMACGenerator:
    """Generate HMAC codes."""

    ALGORITHMS = ["sha256", "sha512", "sha3_256", "blake2b"]

    @staticmethod
    def generate(data: bytes, key: bytes, algorithm: str = "sha256") -> str:
        if algorithm not in HMACGenerator.ALGORITHMS:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        h = hmac.new(key, data, digestmod=algorithm)
        return h.hexdigest()

    @staticmethod
    def verify(data: bytes, key: bytes, expected_hmac: str, algorithm: str = "sha256") -> bool:
        actual = HMACGenerator.generate(data, key, algorithm)
        return hmac.compare_digest(actual.lower(), expected_hmac.lower())


class ChecksumCalculator:
    """Calculate various checksums."""

    @staticmethod
    def crc32(data: bytes) -> str:
        import zlib
        return format(zlib.crc32(data) & 0xFFFFFFFF, "08x")

    @staticmethod
    def adler32(data: bytes) -> str:
        import zlib
        return format(zlib.adler32(data) & 0xFFFFFFFF, "08x")


class HashGeneratorAction(BaseAction):
    """Generate various hashes of data."""
    action_type = "data_hash_generator"
    display_name = "哈希生成器"
    description = "生成数据的各种哈希值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", "")
        algorithm = params.get("algorithm", "sha256")
        file_path = params.get("file_path", "")

        if file_path:
            try:
                hash_value = HashGenerator.hash_file(file_path, algorithm)
                return ActionResult(
                    success=True,
                    message=f"文件哈希已生成: {algorithm}",
                    data={"algorithm": algorithm, "hash": hash_value, "source": "file"}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"哈希生成失败: {e}")

        if isinstance(data, str):
            data = data.encode("utf-8")
        elif not isinstance(data, bytes):
            return ActionResult(success=False, message="data必须是字符串或字节")

        try:
            hash_value = HashGenerator.hash(data, algorithm)
            return ActionResult(
                success=True,
                message=f"哈希已生成: {algorithm}",
                data={"algorithm": algorithm, "hash": hash_value, "length": len(data)}
            )
        except ValueError as e:
            return ActionResult(success=False, message=str(e))


class HashVerifierAction(BaseAction):
    """Verify data integrity using hashes."""
    action_type = "data_hash_verifier"
    display_name = "哈希验证器"
    description = "使用哈希验证数据完整性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", "")
        expected_hash = params.get("expected_hash", "")
        algorithm = params.get("algorithm", "sha256")
        file_path = params.get("file_path", "")

        if not expected_hash:
            return ActionResult(success=False, message="expected_hash是必需的")

        if file_path:
            try:
                valid = HashVerifier.verify_file(file_path, expected_hash, algorithm)
                return ActionResult(
                    success=valid,
                    message=f"文件哈希验证{'通过' if valid else '失败'}",
                    data={"valid": valid, "expected": expected_hash, "algorithm": algorithm}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"验证失败: {e}")

        if isinstance(data, str):
            data = data.encode("utf-8")
        elif not isinstance(data, bytes):
            return ActionResult(success=False, message="data必须是字符串或字节")

        try:
            valid = HashVerifier.verify(data, expected_hash, algorithm)
            return ActionResult(
                success=True,
                message=f"哈希验证{'通过' if valid else '失败'}",
                data={"valid": valid, "expected": expected_hash, "algorithm": algorithm}
            )
        except ValueError as e:
            return ActionResult(success=False, message=str(e))


class HMACGeneratorAction(BaseAction):
    """Generate HMAC codes."""
    action_type = "data_hmac_generator"
    display_name = "HMAC生成器"
    description = "生成HMAC认证码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", "")
        key = params.get("key", "")
        algorithm = params.get("algorithm", "sha256")
        operation = params.get("operation", "generate")

        if not key:
            return ActionResult(success=False, message="key是必需的")

        if isinstance(data, str):
            data = data.encode("utf-8")
        if isinstance(key, str):
            key = key.encode("utf-8")

        if operation == "generate":
            if isinstance(data, str):
                data = data.encode("utf-8")

            try:
                hmac_value = HMACGenerator.generate(data, key, algorithm)
                return ActionResult(
                    success=True,
                    message="HMAC已生成",
                    data={"hmac": hmac_value, "algorithm": algorithm}
                )
            except ValueError as e:
                return ActionResult(success=False, message=str(e))

        if operation == "verify":
            expected_hmac = params.get("expected_hmac", "")
            if not expected_hmac:
                return ActionResult(success=False, message="expected_hmac是必需的")

            if isinstance(data, str):
                data = data.encode("utf-8")

            try:
                valid = HMACGenerator.verify(data, key, expected_hmac, algorithm)
                return ActionResult(
                    success=True,
                    message=f"HMAC验证{'通过' if valid else '失败'}",
                    data={"valid": valid, "algorithm": algorithm}
                )
            except ValueError as e:
                return ActionResult(success=False, message=str(e))

        return ActionResult(success=False, message=f"未知操作: {operation}")


class ChecksumCalculatorAction(BaseAction):
    """Calculate checksums."""
    action_type = "data_checksum_calculator"
    display_name = "校验和计算器"
    description = "计算数据校验和"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", "")
        checksum_type = params.get("type", "crc32")

        if isinstance(data, str):
            data = data.encode("utf-8")
        elif not isinstance(data, bytes):
            return ActionResult(success=False, message="data必须是字符串或字节")

        if checksum_type == "crc32":
            checksum = ChecksumCalculator.crc32(data)
        elif checksum_type == "adler32":
            checksum = ChecksumCalculator.adler32(data)
        else:
            return ActionResult(success=False, message=f"未知类型: {checksum_type}")

        return ActionResult(
            success=True,
            message=f"{checksum_type.upper()} 校验和已计算",
            data={"type": checksum_type, "checksum": checksum, "length": len(data)}
        )
