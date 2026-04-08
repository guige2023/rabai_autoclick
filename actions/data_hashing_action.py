"""Data Hashing Action.

Provides cryptographic and non-cryptographic hashing for data integrity.
"""
from typing import Any, Dict, List, Union
from dataclasses import dataclass
import hashlib
import json


@dataclass
class HashResult:
    value: str
    algorithm: str
    input_size: int
    salt: Optional[str] = None


class DataHashingAction:
    """Hashes data using various algorithms."""

    SUPPORTED_ALGORITHMS = [
        "md5", "sha1", "sha256", "sha384", "sha512",
        "blake2b", "blake2s", "sha3_256", "sha3_512",
    ]

    def __init__(self, default_algorithm: str = "sha256") -> None:
        if default_algorithm not in self.SUPPORTED_ALGORITHMS:
            raise ValueError(f"Unsupported algorithm: {default_algorithm}")
        self.default_algorithm = default_algorithm

    def hash(
        self,
        data: Union[str, bytes],
        algorithm: Optional[str] = None,
        salt: Optional[str] = None,
        encoding: str = "utf-8",
    ) -> HashResult:
        algo = algorithm or self.default_algorithm
        if algo not in self.SUPPORTED_ALGORITHMS:
            raise ValueError(f"Unsupported algorithm: {algo}")
        if isinstance(data, str):
            data_bytes = data.encode(encoding)
        else:
            data_bytes = data
        if salt:
            data_bytes = salt.encode(encoding) + data_bytes
        h = hashlib.new(algo)
        h.update(data_bytes)
        return HashResult(
            value=h.hexdigest(),
            algorithm=algo,
            input_size=len(data_bytes),
            salt=salt,
        )

    def hash_dict(
        self,
        data: Dict[str, Any],
        algorithm: Optional[str] = None,
        sort_keys: bool = True,
    ) -> HashResult:
        json_str = json.dumps(data, sort_keys=sort_keys, default=str)
        return self.hash(json_str, algorithm=algorithm)

    def verify(
        self,
        data: Union[str, bytes],
        expected_hash: str,
        algorithm: Optional[str] = None,
    ) -> bool:
        result = self.hash(data, algorithm=algorithm)
        return result.value == expected_hash
