"""
Input fingerprint utilities.

This module provides utilities for generating fingerprints of input sequences,
enabling quick similarity detection and deduplication.
"""

from __future__ import annotations

import hashlib
from typing import List, Tuple, Optional, Any
from dataclasses import dataclass, field


# Type aliases
Point2D = Tuple[float, float]
Fingerprint = str


@dataclass
class InputFingerprint:
    """Fingerprint of an input sequence."""
    fingerprint: str
    hash_type: str = "sha256"
    length: int = 0
    metadata: dict = field(default_factory=dict)


def fingerprint_trajectory(points: List[Point2D], hash_type: str = "sha256") -> InputFingerprint:
    """
    Generate a fingerprint for a trajectory.

    Args:
        points: List of (x, y) coordinate tuples.
        hash_type: Hash algorithm to use (md5, sha1, sha256).

    Returns:
        InputFingerprint with hash and metadata.
    """
    if not points:
        return InputFingerprint(fingerprint="", hash_type=hash_type, length=0)

    # Create a canonical string representation
    canonical = ";".join(f"{p[0]:.2f},{p[1]:.2f}" for p in points)
    return compute_hash(canonical, hash_type, {"point_count": len(points)})


def fingerprint_displacement_sequence(
    displacements: List[Tuple[float, float]],
    quantization: float = 1.0,
    hash_type: str = "sha256",
) -> InputFingerprint:
    """
    Generate a fingerprint from a displacement sequence.

    Args:
        displacements: List of (dx, dy) displacement tuples.
        quantization: Quantization step for reducing noise.
        hash_type: Hash algorithm.

    Returns:
        InputFingerprint.
    """
    if not displacements:
        return InputFingerprint(fingerprint="", hash_type=hash_type, length=0)

    quantized = [
        (round(dx / quantization) * quantization, round(dy / quantization) * quantization)
        for dx, dy in displacements
    ]
    canonical = ";".join(f"{p[0]:.2f},{p[1]:.2f}" for p in quantized)
    return compute_hash(canonical, hash_type, {"displacement_count": len(displacements), "quantization": quantization})


def fingerprint_timing_sequence(times_ms: List[float], hash_type: str = "sha256") -> InputFingerprint:
    """
    Generate a fingerprint from inter-arrival timing sequence.

    Args:
        times_ms: List of inter-arrival times in milliseconds.
        hash_type: Hash algorithm.

    Returns:
        InputFingerprint.
    """
    if not times_ms:
        return InputFingerprint(fingerprint="", hash_type=hash_type, length=0)

    canonical = ";".join(f"{t:.1f}" for t in times_ms)
    return compute_hash(canonical, hash_type, {"timing_count": len(times_ms)})


def fingerprint_velocity_profile(
    velocities: List[float],
    buckets: int = 8,
    hash_type: str = "sha256",
) -> InputFingerprint:
    """
    Generate a fingerprint from a velocity profile using histogram.

    Args:
        velocities: List of velocity magnitudes.
        buckets: Number of histogram buckets.
        hash_type: Hash algorithm.

    Returns:
        InputFingerprint.
    """
    if not velocities or buckets < 1:
        return InputFingerprint(fingerprint="", hash_type=hash_type, length=0)

    min_v = min(velocities)
    max_v = max(velocities)
    bucket_width = (max_v - min_v) / buckets if max_v > min_v else 1.0

    histogram = [0] * buckets
    for v in velocities:
        idx = min(int((v - min_v) / bucket_width), buckets - 1) if bucket_width > 0 else 0
        histogram[idx] += 1

    canonical = ";".join(str(count) for count in histogram)
    return compute_hash(canonical, hash_type, {"bucket_count": buckets, "total_samples": len(velocities)})


def compute_hash(
    data: str,
    hash_type: str = "sha256",
    metadata: Optional[dict] = None,
) -> InputFingerprint:
    """
    Compute a hash of input data.

    Args:
        data: String data to hash.
        hash_type: Hash algorithm (md5, sha1, sha256).
        metadata: Additional metadata to include.

    Returns:
        InputFingerprint.
    """
    if hash_type == "md5":
        h = hashlib.md5(data.encode("utf-8")).hexdigest()
    elif hash_type == "sha1":
        h = hashlib.sha1(data.encode("utf-8")).hexdigest()
    else:
        h = hashlib.sha256(data.encode("utf-8")).hexdigest()

    return InputFingerprint(
        fingerprint=h,
        hash_type=hash_type,
        length=len(data),
        metadata=metadata or {},
    )


def hamming_similarity(f1: str, f2: str) -> float:
    """
    Compute fingerprint similarity using Hamming distance.

    Args:
        f1: First fingerprint hex string.
        f2: Second fingerprint hex string.

    Returns:
        Similarity score 0-1 (1 = identical).
    """
    if len(f1) != len(f2):
        return 0.0

    h1 = int(f1, 16)
    h2 = int(f2, 16)
    diff = h1 ^ h2
    bits_diff = bin(diff).count("1")
    max_bits = len(f1) * 4  # Each hex char = 4 bits
    return 1.0 - bits_diff / max_bits


def are_fingerprints_similar(
    fp1: InputFingerprint,
    fp2: InputFingerprint,
    threshold: float = 0.9,
) -> bool:
    """
    Check if two fingerprints are similar.

    Args:
        fp1: First fingerprint.
        fp2: Second fingerprint.
        threshold: Similarity threshold.

    Returns:
        True if similarity >= threshold.
    """
    if fp1.fingerprint == fp2.fingerprint:
        return True
    if fp1.hash_type != fp2.hash_type:
        return False
    return hamming_similarity(fp1.fingerprint, fp2.fingerprint) >= threshold
