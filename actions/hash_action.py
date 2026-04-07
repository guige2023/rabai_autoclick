"""Hash action for computing cryptographic hashes.

This module provides hash computation for files and strings
with support for multiple hash algorithms.

Example:
    >>> action = HashAction()
    >>> result = action.execute(algorithm="md5", text="Hello World")
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class HashResult:
    """Hash computation result."""
    algorithm: str
    hash: str
    bytes: int


class HashAction:
    """Hash computation action.

    Computes cryptographic hashes of strings and files
    using various algorithms.

    Example:
        >>> action = HashAction()
        >>> result = action.execute(
        ...     algorithm="sha256",
        ...     text="Hello World"
        ... )
    """

    SUPPORTED_ALGORITHMS = [
        "md5", "sha1", "sha224", "sha256", "sha384", "sha512",
        "blake2b", "blake2s", "sha3_224", "sha3_256", "sha3_384", "sha3_512",
    ]

    def __init__(self) -> None:
        """Initialize hash action."""
        pass

    def execute(
        self,
        algorithm: str,
        text: Optional[str] = None,
        path: Optional[str] = None,
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute hash operation.

        Args:
            algorithm: Hash algorithm (md5, sha256, etc.).
            text: Text to hash.
            path: File path to hash.
            encoding: Text encoding.
            **kwargs: Additional parameters.

        Returns:
            Hash result dictionary.

        Raises:
            ValueError: If algorithm is invalid or input is missing.
        """
        algorithm = algorithm.lower()
        if algorithm not in self.SUPPORTED_ALGORITHMS:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

        result: dict[str, Any] = {"algorithm": algorithm, "success": True}

        if text is not None:
            result.update(self._hash_text(text, algorithm, encoding))
        elif path:
            result.update(self._hash_file(path, algorithm))
        else:
            raise ValueError("text or path required")

        return result

    def _hash_text(
        self,
        text: str,
        algorithm: str,
        encoding: str,
    ) -> dict[str, Any]:
        """Hash text content.

        Args:
            text: Text to hash.
            algorithm: Hash algorithm.
            encoding: Text encoding.

        Returns:
            Result dictionary.
        """
        try:
            hasher = hashlib.new(algorithm)
            hasher.update(text.encode(encoding))
            return {
                "hash": hasher.hexdigest(),
                "digest_size": hasher.digest_size,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _hash_file(self, path: str, algorithm: str) -> dict[str, Any]:
        """Hash file content.

        Args:
            path: File path.
            algorithm: Hash algorithm.

        Returns:
            Result dictionary.
        """
        try:
            hasher = hashlib.new(algorithm)
            bytes_hashed = 0

            with open(path, "rb") as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)
                    bytes_hashed += len(chunk)

            return {
                "hash": hasher.hexdigest(),
                "bytes": bytes_hashed,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def verify(
        self,
        algorithm: str,
        text: Optional[str] = None,
        path: Optional[str] = None,
        expected_hash: str = "",
    ) -> dict[str, Any]:
        """Verify hash matches expected value.

        Args:
            algorithm: Hash algorithm.
            text: Text to hash.
            path: File path to hash.
            expected_hash: Expected hash value.

        Returns:
            Verification result.
        """
        result = self.execute(algorithm=algorithm, text=text, path=path)
        if result.get("success"):
            result["verified"] = result["hash"].lower() == expected_hash.lower()
            result["expected"] = expected_hash
            result["actual"] = result["hash"]
        return result

    def list_algorithms(self) -> list[str]:
        """Get list of supported algorithms.

        Returns:
            List of algorithm names.
        """
        return self.SUPPORTED_ALGORITHMS.copy()
