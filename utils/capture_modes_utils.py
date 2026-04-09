"""
Capture Modes Utilities for Screen Recording.

This module provides utilities for different screen capture modes
and recording configurations for UI automation testing.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List, Tuple, Dict, Any
from enum import Enum
import time


class CaptureMode(Enum):
    """Screen capture modes."""
    FULL_SCREEN = "full_screen"
    REGION = "region"
    WINDOW = "window"
    DISPLAY = "display"
    CURSOR = "cursor"
    ANIMATION = "animation"


class ColorDepth(Enum):
    """Color depth options."""
    BIT_8 = 8
    BIT_16 = 16
    BIT_24 = 24
    BIT_32 = 32


class FrameRate(Enum):
    """Frame rate presets."""
    FPS_15 = 15
    FPS_24 = 24
    FPS_30 = 30
    FPS_60 = 60
    FPS_120 = 120


@dataclass
class CaptureRegion:
    """Region of screen to capture."""
    x: int
    y: int
    width: int
    height: int

    @property
    def area(self) -> int:
        return self.width * self.height

    @property
    def center(self) -> Tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)

    def contains_point(self, x: int, y: int) -> bool:
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height


@dataclass
class CaptureConfig:
    """Configuration for screen capture."""
    mode: CaptureMode = CaptureMode.FULL_SCREEN
    frame_rate: FrameRate = FrameRate.FPS_30
    color_depth: ColorDepth = ColorDepth.BIT_32
    quality: float = 0.9
    format: str = "png"
    region: Optional[CaptureRegion] = None
    include_cursor: bool = True
    include_timestamp: bool = False
    compression_level: int = 6
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CaptureFrame:
    """Single captured frame."""
    timestamp: float
    sequence: int
    image_data: Any
    config: CaptureConfig
    region: Optional[CaptureRegion] = None


class CaptureModeManager:
    """
    Manage different screen capture modes.
    """

    def __init__(self, config: Optional[CaptureConfig] = None):
        """
        Initialize capture mode manager.

        Args:
            config: Default capture configuration
        """
        self.config = config or CaptureConfig()
        self._sequence: int = 0

    def create_config(
        self,
        mode: CaptureMode,
        **overrides
    ) -> CaptureConfig:
        """
        Create capture configuration for a mode.

        Args:
            mode: Capture mode
            **overrides: Configuration overrides

        Returns:
            Configured CaptureConfig
        """
        config = CaptureConfig(mode=mode)

        if mode == CaptureMode.ANIMATION:
            config.frame_rate = FrameRate.FPS_30
            config.quality = 0.95
            config.include_timestamp = True

        if mode == CaptureMode.REGION and "region" not in overrides:
            config.region = CaptureRegion(0, 0, 1920, 1080)

        for key, value in overrides.items():
            if hasattr(config, key):
                setattr(config, key, value)

        return config

    def next_sequence(self) -> int:
        """Get next frame sequence number."""
        self._sequence += 1
        return self._sequence

    def capture_frame(
        self,
        image_data: Any,
        region: Optional[CaptureRegion] = None
    ) -> CaptureFrame:
        """
        Create a CaptureFrame with current config.

        Args:
            image_data: Raw image data
            region: Capture region (if applicable)

        Returns:
            CaptureFrame instance
        """
        return CaptureFrame(
            timestamp=time.time(),
            sequence=self.next_sequence(),
            image_data=image_data,
            config=self.config,
            region=region or self.config.region
        )


def calculate_frame_interval(frame_rate: FrameRate) -> float:
    """
    Calculate frame interval in seconds.

    Args:
        frame_rate: Frame rate

    Returns:
        Interval in seconds
    """
    return 1.0 / frame_rate.value


def estimate_recording_size(
    width: int,
    height: int,
    frame_rate: FrameRate,
    duration_seconds: float,
    bytes_per_pixel: int = 4,
    compression_ratio: float = 0.1
) -> int:
    """
    Estimate recording file size.

    Args:
        width: Frame width
        height: Frame height
        frame_rate: Frame rate
        duration_seconds: Recording duration
        bytes_per_pixel: Bytes per pixel
        compression_ratio: Expected compression ratio

    Returns:
        Estimated size in bytes
    """
    frame_size = width * height * bytes_per_pixel
    total_frames = int(frame_rate.value * duration_seconds)
    uncompressed = frame_size * total_frames
    return int(uncompressed * compression_ratio)


def select_optimal_capture_config(
    display_count: int,
    primary_resolution: Tuple[int, int],
    target_fps: int = 30,
    memory_limit_mb: float = 500.0
) -> CaptureConfig:
    """
    Select optimal capture configuration.

    Args:
        display_count: Number of displays
        primary_resolution: Primary display resolution
        target_fps: Target frames per second
        memory_limit_mb: Memory limit in MB

    Returns:
        Optimized CaptureConfig
    """
    width, height = primary_resolution

    frame_size = width * height * 4
    max_frames_in_memory = int((memory_limit_mb * 1024 * 1024) / frame_size)

    if frame_size * target_fps > memory_limit_mb * 1024 * 1024 / 10:
        color_depth = ColorDepth.BIT_16
    else:
        color_depth = ColorDepth.BIT_32

    mode = CaptureMode.FULL_SCREEN
    if display_count > 1:
        mode = CaptureMode.DISPLAY

    return CaptureConfig(
        mode=mode,
        frame_rate=FrameRate.FPS_30 if target_fps >= 30 else FrameRate.FPS_15,
        color_depth=color_depth,
        region=CaptureRegion(0, 0, width, height)
    )
