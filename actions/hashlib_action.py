"""hashlib action extensions for rabai_autoclick.

Provides cryptographic hash utilities including MD5, SHA,
Blake2, and keyed HMAC operations.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from typing import Any, Callable

__all__ = [
    "hash_md5",
    "hash_sha1",
    "hash_sha256",
    "hash_sha512",
    "hash_sha3_256",
    "hash_sha3_512",
    "hash_blake2b",
    "hash_blake2s",
    "hash_bytes",
    "hash_string",
    "hash_file",
    "hmac_md5",
    "hmac_sha256",
    "hmac_sha512",
    "hmac_verify",
    "pbkdf2_hmac",
    "scrypt",
    "generate_salt",
    "generate_token",
    "generate_random_string",
    "verify_hash",
    "make_hash_func",
    "HashBuilder",
    "MerkleTree",
    "HashRing",
]


def hash_md5(data: bytes | str, encoding: str = "utf-8") -> str:
    """Compute MD5 hash.

    Args:
        data: Data to hash.
        encoding: Text encoding if string.

    Returns:
        Hexadecimal hash string.
    """
    if isinstance(data, str):
        data = data.encode(encoding)
    return hashlib.md5(data).hexdigest()


def hash_sha1(data: bytes | str, encoding: str = "utf-8") -> str:
    """Compute SHA1 hash.

    Args:
        data: Data to hash.
        encoding: Text encoding if string.

    Returns:
        Hexadecimal hash string.
    """
    if isinstance(data, str):
        data = data.encode(encoding)
    return hashlib.sha1(data).hexdigest()


def hash_sha256(data: bytes | str, encoding: str = "utf-8") -> str:
    """Compute SHA256 hash.

    Args:
        data: Data to hash.
        encoding: Text encoding if string.

    Returns:
        Hexadecimal hash string.
    """
    if isinstance(data, str):
        data = data.encode(encoding)
    return hashlib.sha256(data).hexdigest()


def hash_sha512(data: bytes | str, encoding: str = "utf-8") -> str:
    """Compute SHA512 hash.

    Args:
        data: Data to hash.
        encoding: Text encoding if string.

    Returns:
        Hexadecimal hash string.
    """
    if isinstance(data, str):
        data = data.encode(encoding)
    return hashlib.sha512(data).hexdigest()


def hash_sha3_256(data: bytes | str, encoding: str = "utf-8") -> str:
    """Compute SHA3-256 hash.

    Args:
        data: Data to hash.
        encoding: Text encoding if string.

    Returns:
        Hexadecimal hash string.
    """
    if isinstance(data, str):
        data = data.encode(encoding)
    return hashlib.sha3_256(data).hexdigest()


def hash_sha3_512(data: bytes | str, encoding: str = "utf-8") -> str:
    """Compute SHA3-512 hash.

    Args:
        data: Data to hash.
        encoding: Text encoding if string.

    Returns:
        Hexadecimal hash string.
    """
    if isinstance(data, str):
        data = data.encode(encoding)
    return hashlib.sha3_512(data).hexdigest()


def hash_blake2b(
    data: bytes | str,
    key: bytes | None = None,
    encoding: str = "utf-8",
) -> str:
    """Compute BLAKE2b hash.

    Args:
        data: Data to hash.
        key: Optional key for keyed hashing.
        encoding: Text encoding if string.

    Returns:
        Hexadecimal hash string.
    """
    if isinstance(data, str):
        data = data.encode(encoding)
    if key:
        return hashlib.blake2b(data, key=key).hexdigest()
    return hashlib.blake2b(data).hexdigest()


def hash_blake2s(
    data: bytes | str,
    key: bytes | None = None,
    encoding: str = "utf-8",
) -> str:
    """Compute BLAKE2s hash.

    Args:
        data: Data to hash.
        key: Optional key for keyed hashing.
        encoding: Text encoding if string.

    Returns:
        Hexadecimal hash string.
    """
    if isinstance(data, str):
        data = data.encode(encoding)
    if key:
        return hashlib.blake2s(data, key=key).hexdigest()
    return hashlib.blake2s(data).hexdigest()


def hash_bytes(
    data: bytes,
    algorithm: str = "sha256",
) -> str:
    """Compute hash of bytes.

    Args:
        data: Bytes to hash.
        algorithm: Hash algorithm name.

    Returns:
        Hexadecimal hash string.
    """
    hasher = hashlib.new(algorithm)
    hasher.update(data)
    return hasher.hexdigest()


def hash_string(
    data: str,
    algorithm: str = "sha256",
    encoding: str = "utf-8",
) -> str:
    """Compute hash of string.

    Args:
        data: String to hash.
        algorithm: Hash algorithm name.
        encoding: Text encoding.

    Returns:
        Hexadecimal hash string.
    """
    return hash_bytes(data.encode(encoding), algorithm)


def hash_file(
    path: str,
    algorithm: str = "sha256",
    chunk_size: int = 8192,
) -> str:
    """Compute hash of file contents.

    Args:
        path: File path.
        algorithm: Hash algorithm name.
        chunk_size: Read chunk size.

    Returns:
        Hexadecimal hash string.
    """
    hasher = hashlib.new(algorithm)
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def hmac_md5(key: bytes | str, data: bytes | str) -> str:
    """Compute HMAC-MD5.

    Args:
        key: Secret key.
        data: Data to authenticate.

    Returns:
        Hexadecimal HMAC.
    """
    if isinstance(key, str):
        key = key.encode()
    if isinstance(data, str):
        data = data.encode()
    return hmac.new(key, data, hashlib.md5).hexdigest()


def hmac_sha256(key: bytes | str, data: bytes | str) -> str:
    """Compute HMAC-SHA256.

    Args:
        key: Secret key.
        data: Data to authenticate.

    Returns:
        Hexadecimal HMAC.
    """
    if isinstance(key, str):
        key = key.encode()
    if isinstance(data, str):
        data = data.encode()
    return hmac.new(key, data, hashlib.sha256).hexdigest()


def hmac_sha512(key: bytes | str, data: bytes | str) -> str:
    """Compute HMAC-SHA512.

    Args:
        key: Secret key.
        data: Data to authenticate.

    Returns:
        Hexadecimal HMAC.
    """
    if isinstance(key, str):
        key = key.encode()
    if isinstance(data, str):
        data = data.encode()
    return hmac.new(key, data, hashlib.sha512).hexdigest()


def hmac_verify(
    key: bytes | str,
    data: bytes | str,
    signature: str,
    algorithm: str = "sha256",
) -> bool:
    """Verify HMAC signature.

    Args:
        key: Secret key.
        data: Data that was authenticated.
        signature: Expected signature.
        algorithm: Hash algorithm.

    Returns:
        True if signature is valid.
    """
    if isinstance(key, str):
        key = key.encode()
    if isinstance(data, str):
        data = data.encode()

    compute_func = getattr(hashlib, f"hmac_{algorithm}", None)
    if not compute_func:
        compute_func = lambda k, d: hmac.new(k, d, hashlib.new(algorithm)).hexdigest()

    expected = hmac.new(key, data, hashlib.new(algorithm)).hexdigest()
    return hmac.compare_digest(signature, expected)


def pbkdf2_hmac(
    password: str,
    salt: bytes,
    iterations: int = 100000,
    keylen: int = 32,
    hash_name: str = "sha256",
) -> bytes:
    """Compute PBKDF2-HMAC.

    Args:
        password: Password to derive key from.
        salt: Salt value.
        iterations: Number of iterations.
        keylen: Desired key length.
        hash_name: Hash algorithm name.

    Returns:
        Derived key bytes.
    """
    if isinstance(password, str):
        password = password.encode()
    return hashlib.pbkdf2_hmac(hash_name, password, salt, iterations, keylen)


def scrypt(
    password: str,
    salt: bytes,
    n: int = 16384,
    r: int = 8,
    p: int = 1,
    maxmem: int = 32 * 1024 * 1024,
    keylen: int = 32,
) -> bytes:
    """Compute scrypt.

    Args:
        password: Password.
        salt: Salt value.
        n: CPU/memory cost parameter.
        r: Block size.
        p: Parallelization parameter.
        maxmem: Maximum memory to use.
        keylen: Desired key length.

    Returns:
        Derived key bytes.
    """
    if isinstance(password, str):
        password = password.encode()
    return hashlib.scrypt(password, salt=salt, n=n, r=r, p=p, maxmem=maxmem, dklen=keylen)


def generate_salt(length: int = 16) -> bytes:
    """Generate a random salt.

    Args:
        length: Salt length in bytes.

    Returns:
        Random salt bytes.
    """
    return secrets.token_bytes(length)


def generate_token(length: int = 32) -> str:
    """Generate a random token.

    Args:
        length: Token length in bytes.

    Returns:
        Hexadecimal token string.
    """
    return secrets.token_hex(length)


def generate_random_string(
    length: int = 16,
    characters: str | None = None,
) -> str:
    """Generate a random string.

    Args:
        length: String length.
        characters: Character set to use.

    Returns:
        Random string.
    """
    if characters is None:
        characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(secrets.choice(characters) for _ in range(length))


def verify_hash(
    data: str,
    hash_value: str,
    algorithm: str = "sha256",
) -> bool:
    """Verify data matches hash.

    Args:
        data: Data to verify.
        hash_value: Expected hash.
        algorithm: Hash algorithm.

    Returns:
        True if data matches hash.
    """
    computed = hash_string(data, algorithm)
    return hmac.compare_digest(computed, hash_value)


def make_hash_func(
    algorithm: str = "sha256",
) -> Callable[[str], str]:
    """Create a hash function for a specific algorithm.

    Args:
        algorithm: Hash algorithm name.

    Returns:
        Hash function.
    """
    def hash_func(data: str) -> str:
        return hash_string(data, algorithm)
    return hash_func


class HashBuilder:
    """Builder for incremental hash computation."""

    def __init__(self, algorithm: str = "sha256") -> None:
        self._hasher = hashlib.new(algorithm)

    def update(self, data: bytes | str, encoding: str = "utf-8") -> HashBuilder:
        """Add data to hash.

        Args:
            data: Data to add.
            encoding: Text encoding if string.

        Returns:
            Self for chaining.
        """
        if isinstance(data, str):
            data = data.encode(encoding)
        self._hasher.update(data)
        return self

    def digest(self) -> str:
        """Get hash as hexadecimal string.

        Returns:
            Hash hex string.
        """
        return self._hasher.hexdigest()

    def digest_bytes(self) -> bytes:
        """Get hash as bytes.

        Returns:
            Hash bytes.
        """
        return self._hasher.digest()

    def copy(self) -> HashBuilder:
        """Get a copy of this builder.

        Returns:
            New HashBuilder with same state.
        """
        new_builder = HashBuilder.__new__(HashBuilder)
        new_builder._hasher = self._hasher.copy()
        return new_builder


class MerkleTree:
    """Merkle tree for efficient content verification."""

    def __init__(self, data: list[bytes]) -> None:
        self._data = data
        self._tree = self._build_tree()

    def _build_tree(self) -> list[list[str]]:
        """Build the merkle tree."""
        if not self._data:
            return []

        hashes = [hash_bytes(d) for d in self._data]
        tree = [hashes]

        while len(tree[-1]) > 1:
            level = tree[-1]
            if len(level) % 2 == 1:
                level.append(level[-1])
            new_level = []
            for i in range(0, len(level), 2):
                combined = level[i] + level[i + 1]
                new_level.append(hash_string(combined))
            tree.append(new_level)

        return tree

    def root(self) -> str:
        """Get the merkle root.

        Returns:
            Root hash.
        """
        if not self._tree:
            return ""
        return self._tree[-1][0]

    def proof(self, index: int) -> list[dict[str, Any]]:
        """Get merkle proof for a leaf.

        Args:
            index: Leaf index.

        Returns:
            List of proof nodes.
        """
        if index >= len(self._data):
            return []

        proof = []
        idx = index
        for level in self._tree[:-1]:
            if idx % 2 == 0:
                sibling = idx + 1 if idx + 1 < len(level) else idx
                proof.append({"position": "right", "hash": level[sibling]})
            else:
                proof.append({"position": "left", "hash": level[idx - 1]})
            idx = idx // 2

        return proof

    def verify(self, index: int, data: bytes, proof: list[dict[str, Any]]) -> bool:
        """Verify merkle proof.

        Args:
            index: Leaf index.
            data: Leaf data.
            proof: Merkle proof.

        Returns:
            True if proof is valid.
        """
        current = hash_bytes(data)
        idx = index

        for node in proof:
            if node["position"] == "right":
                current = hash_string(current + node["hash"])
            else:
                current = hash_string(node["hash"] + current)

        return current == self.root()


class HashRing:
    """Consistent hashing ring for distributed hashing."""

    def __init__(self, nodes: list[str] | None = None, replicas: int = 100) -> None:
        self._nodes: dict[str, str] = {}
        self._ring: list[tuple[int, str]] = []
        self._replicas = replicas
        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node: str) -> None:
        """Add a node to the ring.

        Args:
            node: Node identifier.
        """
        for i in range(self._replicas):
            key = hash_string(f"{node}:{i}")
            self._ring.append((int(key, 16), node))
        self._ring.sort(key=lambda x: x[0])
        self._nodes[node] = node

    def remove_node(self, node: str) -> None:
        """Remove a node from the ring.

        Args:
            node: Node identifier.
        """
        self._ring = [(pos, n) for pos, n in self._ring if n != node]
        if node in self._nodes:
            del self._nodes[node]

    def get_node(self, key: str) -> str:
        """Get node for a key.

        Args:
            key: Key to hash.

        Returns:
            Node identifier.
        """
        if not self._ring:
            return ""

        key_hash = int(hash_string(key), 16)
        for pos, node in self._ring:
            if pos >= key_hash:
                return node
        return self._ring[0][1]
