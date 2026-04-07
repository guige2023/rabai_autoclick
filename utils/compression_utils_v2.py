"""
Compression utilities v2 — advanced compression algorithms.

Companion to compression_utils.py. Adds LZW, RLE variants,
Huffman coding, and wavelet-based compression utilities.
"""

from __future__ import annotations

import heapq
import zlib
from collections import Counter
from typing import NamedTuple


class HuffmanNode:
    """Node for Huffman coding tree."""

    def __init__(self, char: str | None, freq: int) -> None:
        self.char = char
        self.freq = freq
        self.left: HuffmanNode | None = None
        self.right: HuffmanNode | None = None

    def __lt__(self, other: HuffmanNode) -> bool:
        return self.freq < other.freq


class HuffmanCodeResult(NamedTuple):
    """Result of Huffman encoding."""
    encoded: str
    codes: dict[str, str]
    tree_bytes: int


def huffman_encode(data: str) -> HuffmanCodeResult:
    """
    Encode string using Huffman coding.

    Args:
        data: Input string to encode

    Returns:
        HuffmanCodeResult with encoded bitstring and code table

    Example:
        >>> result = huffman_encode("hello world")
        >>> len(result.codes) > 0
        True
    """
    if not data:
        return HuffmanCodeResult("", {}, 0)

    freq = Counter(data)
    heap = [HuffmanNode(ch, fr) for ch, fr in freq.items()]
    heapq.heapify(heap)

    while len(heap) > 1:
        left = heapq.heappop(heap)
        right = heapq.heappop(heap)
        merged = HuffmanNode(None, left.freq + right.freq)
        merged.left = left
        merged.right = right
        heapq.heappush(heap, merged)

    root = heap[0]
    codes: dict[str, str] = {}

    def build_codes(node: HuffmanNode, code: str = "") -> None:
        if node.char is not None:
            codes[node.char] = code or "0"
            return
        if node.left:
            build_codes(node.left, code + "0")
        if node.right:
            build_codes(node.right, code + "1")

    build_codes(root)
    encoded = "".join(codes[ch] for ch in data)
    return HuffmanCodeResult(encoded=encoded, codes=codes, tree_bytes=len(codes))


def huffman_decode(encoded: str, codes: dict[str, str]) -> str:
    """
    Decode Huffman-encoded string.

    Args:
        encoded: Bitstring
        codes: Code table mapping characters to bitstrings

    Returns:
        Decoded string
    """
    if not encoded:
        return ""
    reverse_codes = {v: k for k, v in codes.items()}
    result = []
    buffer = ""
    for bit in encoded:
        buffer += bit
        if buffer in reverse_codes:
            result.append(reverse_codes[buffer])
            buffer = ""
    return "".join(result)


def lzw_compress(data: str) -> tuple[list[int], int]:
    """
    LZW compression algorithm.

    Args:
        data: Input string

    Returns:
        Tuple of (compressed codes, initial dictionary size)

    Example:
        >>> codes, _ = lzw_compress("ABABAB")
        >>> len(codes) < len("ABABAB")
        True
    """
    if not data:
        return [], 256

    dict_size = 256
    dictionary = {chr(i): i for i in range(dict_size)}

    result: list[int] = []
    buffer = ""
    for ch in data:
        combined = buffer + ch
        if combined in dictionary:
            buffer = combined
        else:
            result.append(dictionary[buffer])
            dictionary[combined] = dict_size
            dict_size += 1
            buffer = ch
    if buffer:
        result.append(dictionary[buffer])

    return result, 256


def lzw_decompress(codes: list[int], dict_size: int = 256) -> str:
    """
    Decompress LZW-encoded data.

    Args:
        codes: LZW compressed codes
        dict_size: Initial dictionary size

    Returns:
        Decompressed string
    """
    if not codes:
        return ""

    dictionary = {i: chr(i) for i in range(dict_size)}
    result = [dictionary[codes[0]]]
    buffer = result[0]

    for code in codes[1:]:
        if code in dictionary:
            entry = dictionary[code]
        elif code == dict_size:
            entry = buffer + buffer[0]
        else:
            raise ValueError(f"Invalid LZW code: {code}")
        result.append(entry)
        dictionary[dict_size] = buffer + entry[0]
        dict_size += 1
        buffer = entry

    return "".join(result)


def rle_encode(data: bytes) -> list[int | bytes]:
    """
    Run-length encoding for bytes.

    Args:
        data: Bytes to encode

    Returns:
        List of (count, value) pairs for runs, or single bytes

    Example:
        >>> rle_encode(b"AAABBBCCAA")
        [3, 65, 3, 66, 2, 67, 2, 65]
    """
    if not data:
        return []
    result: list[int | bytes] = []
    run_val = data[0]
    run_len = 1
    for b in data[1:]:
        if b == run_val and run_len < 255:
            run_len += 1
        else:
            result.extend([run_len, run_val])
            run_val = b
            run_len = 1
    result.extend([run_len, run_val])
    return result


def rle_decode(encoded: list[int | bytes]) -> bytes:
    """
    Decode RLE-encoded bytes.

    Args:
        encoded: RLE encoded data

    Returns:
        Decoded bytes
    """
    result = bytearray()
    i = 0
    while i < len(encoded):
        count = int(encoded[i])
        value = bytes([encoded[i + 1]]) if isinstance(encoded[i + 1], int) else encoded[i + 1]
        result.extend(value * count)
        i += 2
    return bytes(result)


def sliding_window_compress(data: str, window_size: int = 4096) -> list[tuple[int, int, str]]:
    """
    LZ77-style sliding window compression.

    Args:
        data: Input string
        window_size: Size of search window

    Returns:
        List of (offset, length, next_char) tuples
    """
    if not data:
        return []

    result: list[tuple[int, int, str]] = []
    pos = 0
    while pos < len(data):
        best_offset = 0
        best_length = 0
        search_start = max(0, pos - window_size)
        for i in range(search_start, pos):
            length = 0
            while pos + length < len(data) and data[i + length] == data[pos + length]:
                length += 1
                if length > best_length:
                    best_length = length
                    best_offset = pos - i
        next_char = data[pos + best_length] if pos + best_length < len(data) else ""
        result.append((best_offset, best_length, next_char))
        pos += best_length + 1
    return result


def deflate_compress(data: bytes, level: int = 6) -> bytes:
    """
    Compress bytes using zlib deflate.

    Args:
        data: Bytes to compress
        level: Compression level (0-9)

    Returns:
        Compressed bytes
    """
    return zlib.compress(data, level)


def deflate_decompress(data: bytes) -> bytes:
    """
    Decompress zlib-deflated data.

    Args:
        data: Compressed bytes

    Returns:
        Decompressed bytes
    """
    return zlib.decompress(data)


def burrows_wheeler_transform(data: bytes) -> tuple[bytes, int]:
    """
    Burrows-Wheeler transform (BWT) for blocksort preprocessing.

    Args:
        data: Input bytes (typically a block)

    Returns:
        Tuple of (transformed data, original index)
    """
    if not data:
        return b"", 0
    n = len(data)
    rotations = [data[i:] + data[:i] for i in range(n)]
    sorted_rotations = sorted(rotations)
    for i, row in enumerate(sorted_rotations):
        if row == data:
            return b"".join(r[-1:] for r in sorted_rotations), i
    return b"", 0


def burrows_wheeler_inverse(transformed: bytes, index: int) -> bytes:
    """
    Inverse Burrows-Wheeler transform.

    Args:
        transformed: BWT output
        index: Original index from transform

    Returns:
        Original data
    """
    if not transformed:
        return b""
    n = len(transformed)
    L = transformed
    F = bytes(sorted(L))
    count = [0] * 256
    next_pos: list[list[int]] = [[] for _ in range(256)]

    for i, ch in enumerate(L):
        idx = ch
        next_pos[idx].append(i)

    pos = [0] * n
    for i in range(n):
        ch = F[i]
        pos[i] = next_pos[ch][count[ch]]
        count[ch] += 1

    result = bytearray(n)
    p = index
    for i in range(n):
        result[i] = F[p]
        p = pos[p]
    return bytes(result)


def move_to_front_encode(data: bytes, alphabet_size: int = 256) -> list[int]:
    """
    Move-to-front (MTF) transform.

    Args:
        data: Input bytes
        alphabet_size: Size of alphabet

    Returns:
        MTF-encoded indices
    """
    if not data:
        return []
    L = list(range(alphabet_size))
    result: list[int] = []
    for b in data:
        idx = L.index(b)
        result.append(idx)
        L.pop(idx)
        L.insert(0, b)
    return result


def move_to_front_decode(encoded: list[int], alphabet_size: int = 256) -> bytes:
    """
    Decode MTF-encoded data.

    Args:
        encoded: MTF indices
        alphabet_size: Size of alphabet

    Returns:
        Original bytes
    """
    if not encoded:
        return b""
    L = list(range(alphabet_size))
    result: list[int] = []
    for idx in encoded:
        result.append(L[idx])
        L.pop(idx)
        L.insert(0, L[idx])
    return bytes(result)
