"""
Screen content hashing utilities.

This module provides content-addressable hashing for screen regions,
enabling quick comparison of UI states without pixel-by-pixel comparison.
"""

from __future__ import annotations

import hashlib
import math
import struct
from typing import List, Tuple, Optional
from dataclasses import dataclass


# Type aliases
PixelRGB = Tuple[int, int, int]
ImageRow = List[PixelRGB]
ImageMatrix = List[ImageRow]


@dataclass
class ContentHash:
    """Content hash result for a screen region."""
    perceptual_hash: str
    average_hash: str
    difference_hash: str
    block_hashes: List[str]
    width: int
    height: int


@dataclass
class HashMatch:
    """Result of comparing two content hashes."""
    identical: bool
    similarity: float
    perceptual_diff: int


def rgb_to_grayscale(r: int, g: int, b: int) -> int:
    """Convert RGB to 8-bit grayscale using luminosity method."""
    return int(0.299 * r + 0.587 * g + 0.114 * b)


def downsample_image(
    pixels: ImageMatrix,
    target_width: int,
    target_height: int,
) -> ImageMatrix:
    """
    Downsample an image matrix to target dimensions using area averaging.

    Args:
        pixels: 2D list of RGB tuples.
        target_width: Desired output width.
        target_height: Desired output height.

    Returns:
        Downsampled image matrix.
    """
    if not pixels or not pixels[0]:
        return [[]]
    src_h = len(pixels)
    src_w = len(pixels[0])
    result: ImageMatrix = []
    block_h = src_h / target_height
    block_w = src_w / target_width

    for py in range(target_height):
        row: ImageRow = []
        y_start = int(py * block_h)
        y_end = int((py + 1) * block_h)
        for px in range(target_width):
            x_start = int(px * block_w)
            x_end = int((px + 1) * block_w)
            total_r, total_g, total_b = 0, 0, 0
            count = 0
            for y in range(y_start, min(y_end, src_h)):
                for x in range(x_start, min(x_end, src_w)):
                    r, g, b = pixels[y][x]
                    total_r += r
                    total_g += g
                    total_b += b
                    count += 1
            if count > 0:
                row.append((total_r // count, total_g // count, total_b // count))
            else:
                row.append((0, 0, 0))
        result.append(row)

    return result


def compute_average_hash(
    pixels: ImageMatrix,
    hash_size: int = 8,
) -> str:
    """
    Compute average hash (aHash) for an image.

    Args:
        pixels: 2D list of RGB tuples.
        hash_size: Size of the hash (default 8x8).

    Returns:
        Hex string of the hash.
    """
    if not pixels:
        return "0" * (hash_size * hash_size // 4)

    small = downsample_image(pixels, hash_size, hash_size)
    total = 0
    gray_values: List[int] = []
    for row in small:
        for r, g, b in row:
            gray = rgb_to_grayscale(r, g, b)
            gray_values.append(gray)
            total += gray

    avg = total // len(gray_values) if gray_values else 0
    bits = [(1 << i) if gray_values[i] >= avg else 0 for i in range(len(gray_values))]
    hash_bytes = [sum(bits[j] << (7 - (j % 8)) for j in range(i * 8, min(i * 8 + 8, len(bits)))) for i in range(hash_size * hash_size // 8)]
    return "".join(f"{byte:02x}" for byte in hash_bytes)


def compute_perceptual_hash(
    pixels: ImageMatrix,
    hash_size: int = 8,
) -> str:
    """
    Compute perceptual hash (pHash) for an image using DCT.

    Args:
        pixels: 2D list of RGB tuples.
        hash_size: Size of the hash.

    Returns:
        Hex string of the hash.
    """
    if not pixels:
        return "0" * (hash_size * hash_size // 4)

    small = downsample_image(pixels, hash_size + 1, hash_size + 1)
    gray: List[List[float]] = []
    for row in small:
        gray_row = [rgb_to_grayscale(r, g, b) / 255.0 for r, g, b in row]
        gray.append(gray_row)

    # Simplified DCT (only low frequencies)
    dct: List[List[float]] = [[0.0] * (hash_size + 1) for _ in range(hash_size + 1)]
    for u in range(hash_size + 1):
        for v in range(hash_size + 1):
            total = 0.0
            for x in range(hash_size + 1):
                for y in range(hash_size + 1):
                    cos_x = math.cos(math.pi * x * u / hash_size)
                    cos_y = math.cos(math.pi * y * v / hash_size)
                    total += gray[x][y] * cos_x * cos_y
            cu = 1.0 if u == 0 else 0.707
            cv = 1.0 if v == 0 else 0.707
            dct[u][v] = cu * cv * total

    # Use top-left hash_size x hash_size (excluding DC)
    dct_vals = [dct[u][v] for u in range(1, hash_size + 1) for v in range(1, hash_size + 1)]
    median = sorted(dct_vals)[len(dct_vals) // 2] if dct_vals else 0.0
    bits = [(1 << i) if dct_vals[i] >= median else 0 for i in range(len(dct_vals))]
    hash_bytes = [sum(bits[j] << (7 - (j % 8)) for j in range(i * 8, min(i * 8 + 8, len(bits)))) for i in range(len(bits) // 8)]
    return "".join(f"{byte:02x}" for byte in hash_bytes)


def compute_difference_hash(
    pixels: ImageMatrix,
    hash_size: int = 8,
) -> str:
    """
    Compute difference hash (dHash) for an image.

    Args:
        pixels: 2D list of RGB tuples.
        hash_size: Size of the hash.

    Returns:
        Hex string of the hash.
    """
    if not pixels:
        return "0" * (hash_size * hash_size // 4)

    small = downsample_image(pixels, hash_size + 1, hash_size, )
    gray: List[List[int]] = []
    for row in small:
        gray_row = [rgb_to_grayscale(r, g, b) for r, g, b in row]
        gray.append(gray_row)

    bits: List[int] = []
    for row in gray:
        for i in range(hash_size):
            bits.append(1 if row[i] < row[i + 1] else 0)

    hash_bytes = [sum(bits[j] << (7 - (j % 8)) for j in range(i * 8, min(i * 8 + 8, len(bits)))) for i in range(len(bits) // 8)]
    return "".join(f"{byte:02x}" for byte in hash_bytes)


def compute_block_hashes(
    pixels: ImageMatrix,
    block_size: int = 16,
) -> List[str]:
    """
    Compute hashes for blocks within an image.

    Args:
        pixels: 2D list of RGB tuples.
        block_size: Size of each block in pixels.

    Returns:
        List of hex hashes, one per block.
    """
    if not pixels:
        return []
    h = len(pixels)
    w = len(pixels[0]) if pixels else 0
    blocks: List[str] = []
    for by in range(0, h, block_size):
        for bx in range(0, w, block_size):
            block: ImageMatrix = []
            for y in range(by, min(by + block_size, h)):
                row: ImageRow = []
                for x in range(bx, min(bx + block_size, w)):
                    row.append(pixels[y][x])
                block.append(row)
            blocks.append(compute_average_hash(block, 4))
    return blocks


def compute_content_hash(pixels: ImageMatrix, width: int, height: int) -> ContentHash:
    """
    Compute all content hashes for a screen region.

    Args:
        pixels: 2D list of RGB tuples.
        width: Original width of the region.
        height: Original height of the region.

    Returns:
        ContentHash containing all hash variants.
    """
    return ContentHash(
        perceptual_hash=compute_perceptual_hash(pixels),
        average_hash=compute_average_hash(pixels),
        difference_hash=compute_difference_hash(pixels),
        block_hashes=compute_block_hashes(pixels),
        width=width,
        height=height,
    )


def hamming_distance(hash1: str, hash2: str) -> int:
    """Compute Hamming distance between two hex hashes."""
    if len(hash1) != len(hash2):
        return abs(len(hash1) - len(hash2)) * 4
    h1 = int(hash1, 16)
    h2 = int(hash2, 16)
    diff = h1 ^ h2
    return bin(diff).count("1")


def compare_hashes(hash1: ContentHash, hash2: ContentHash) -> HashMatch:
    """
    Compare two content hashes and return similarity.

    Args:
        hash1: First content hash.
        hash2: Second content hash.

    Returns:
        HashMatch with similarity score.
    """
    max_bits = 64
    perf_dist = hamming_distance(hash1.perceptual_hash, hash2.perceptual_hash)
    avg_dist = hamming_distance(hash1.average_hash, hash2.average_hash)
    diff_dist = hamming_distance(hash1.difference_hash, hash2.difference_hash)

    avg_hamming = (perf_dist + avg_dist + diff_dist) / 3.0
    similarity = 1.0 - (avg_hamming / max_bits)

    return HashMatch(
        identical=avg_hamming == 0,
        similarity=max(0.0, similarity),
        perceptual_diff=perf_dist,
    )



