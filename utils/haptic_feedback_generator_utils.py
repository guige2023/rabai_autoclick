"""
Haptic Feedback Generator Utilities

Generate haptic feedback patterns for touch events in automation
contexts that support haptic output.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class HapticPattern:
    """A haptic feedback pattern."""
    name: str
    duration_ms: int
    intensity: float  # 0.0 to 1.0
    waveform: str = "sine"  # 'sine', 'square', 'triangle', 'tap'


@dataclass
class HapticSequence:
    """A sequence of haptic patterns."""
    name: str
    patterns: List[HapticPattern]
    total_duration_ms: int

    def __post_init__(self):
        if not self.total_duration_ms:
            self.total_duration_ms = sum(p.duration_ms for p in self.patterns)


BUILTIN_PATTERNS = {
    "tap": HapticPattern(name="tap", duration_ms=10, intensity=0.7, waveform="tap"),
    "short_buzz": HapticPattern(name="short_buzz", duration_ms=50, intensity=0.5, waveform="square"),
    "long_buzz": HapticPattern(name="long_buzz", duration_ms=200, intensity=0.8, waveform="square"),
    "tick": HapticPattern(name="tick", duration_ms=5, intensity=0.9, waveform="tap"),
    "double_tap": HapticPattern(name="double_tap", duration_ms=20, intensity=0.6, waveform="tap"),
}


def create_sequence(
    name: str,
    pattern_names: List[str],
    inter_pattern_delay_ms: int = 10,
) -> HapticSequence:
    """Create a haptic sequence from built-in pattern names."""
    patterns = []
    for pname in pattern_names:
        if pname in BUILTIN_PATTERNS:
            patterns.append(BUILTIN_PATTERNS[pname])
    return HapticSequence(name=name, patterns=patterns, total_duration_ms=0)


def scale_intensity(pattern: HapticPattern, factor: float) -> HapticPattern:
    """Scale the intensity of a haptic pattern."""
    return HapticPattern(
        name=pattern.name,
        duration_ms=pattern.duration_ms,
        intensity=max(0.0, min(1.0, pattern.intensity * factor)),
        waveform=pattern.waveform,
    )


def encode_haptic_signal(pattern: HapticPattern) -> bytes:
    """Encode a haptic pattern to a binary signal for transmission."""
    header = b"HAPT1"
    name_bytes = pattern.name.encode("utf-8")[:32].ljust(32, b"\x00")
    duration_bytes = pattern.duration_ms.to_bytes(4, "little")
    intensity_byte = int(pattern.intensity * 255).to_bytes(1, "little")
    waveform_byte = ord(pattern.waveform[0]).to_bytes(1, "little")
    return header + name_bytes + duration_bytes + intensity_byte + waveform_byte
