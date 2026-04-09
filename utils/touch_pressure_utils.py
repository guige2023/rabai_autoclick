"""
Touch Pressure Utilities

Process and analyze touch pressure data from touch input events,
including normalization, calibration, and pressure pattern analysis.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class PressureSample:
    """A single touch pressure sample."""
    pressure: float  # normalized 0.0 to 1.0
    timestamp_ms: float


@dataclass
class PressureProfile:
    """Statistical profile of touch pressure samples."""
    mean_pressure: float
    peak_pressure: float
    pressure_range: float
    is_consistent: bool


def normalize_pressure(raw_pressure: float, max_raw: float = 1.0) -> float:
    """Normalize raw pressure to [0.0, 1.0] range."""
    return max(0.0, min(1.0, raw_pressure / max_raw))


def is_palm_contact(mean_pressure: float, threshold: float = 0.9) -> bool:
    """Detect potential palm contact based on high average pressure."""
    return mean_pressure > threshold


def is_light_tap(mean_pressure: float, threshold: float = 0.3) -> bool:
    """Detect a light tap based on low average pressure."""
    return mean_pressure < threshold


def build_pressure_profile(samples: List[PressureSample]) -> PressureProfile:
    """Build a statistical profile from pressure samples."""
    if not samples:
        return PressureProfile(
            mean_pressure=0.0,
            peak_pressure=0.0,
            pressure_range=0.0,
            is_consistent=False,
        )

    pressures = [s.pressure for s in samples]
    mean_pressure = sum(pressures) / len(pressures)
    peak_pressure = max(pressures)
    pressure_range = peak_pressure - min(pressures)

    # Check consistency: samples shouldn't vary too much
    variance = sum((p - mean_pressure) ** 2 for p in pressures) / len(pressures)
    is_consistent = variance < 0.05

    return PressureProfile(
        mean_pressure=mean_pressure,
        peak_pressure=peak_pressure,
        pressure_range=pressure_range,
        is_consistent=is_consistent,
    )


def apply_calibration(
    raw_pressure: float,
    calibration_min: float,
    calibration_max: float,
) -> float:
    """Apply calibration to a raw pressure reading."""
    if calibration_max <= calibration_min:
        return raw_pressure
    calibrated = (raw_pressure - calibration_min) / (calibration_max - calibration_min)
    return max(0.0, min(1.0, calibrated))
