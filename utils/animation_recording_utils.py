"""Animation recording utilities.

This module provides utilities for recording UI animations,
capturing frame sequences, and analyzing animation properties.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto


class AnimationType(Enum):
    """Types of UI animations."""
    TRANSITION = auto()
    TRANSFORM = auto()
    OPACITY = auto()
    COLOR = auto()
    LAYOUT = auto()
    CUSTOM = auto()


@dataclass
class KeyFrame:
    """A keyframe in an animation."""
    timestamp: float
    x: float = 0.0
    y: float = 0.0
    width: float = 0.0
    height: float = 0.0
    opacity: float = 1.0
    rotation: float = 0.0
    scale: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AnimationClip:
    """A recorded animation clip."""
    name: str
    animation_type: AnimationType
    keyframes: List[KeyFrame] = field(default_factory=list)
    duration_ms: float = 0.0
    loop_count: int = 1
    easing: str = "linear"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_keyframe(self, keyframe: KeyFrame) -> None:
        self.keyframes.append(keyframe)
        self._recompute_duration()

    def _recompute_duration(self) -> None:
        if self.keyframes:
            self.duration_ms = (self.keyframes[-1].timestamp - self.keyframes[0].timestamp) * 1000

    def get_frame_at(self, timestamp: float) -> Optional[KeyFrame]:
        if not self.keyframes:
            return None
        if timestamp <= self.keyframes[0].timestamp:
            return self.keyframes[0]
        if timestamp >= self.keyframes[-1].timestamp:
            return self.keyframes[-1]
        for i in range(len(self.keyframes) - 1):
            kf1 = self.keyframes[i]
            kf2 = self.keyframes[i + 1]
            if kf1.timestamp <= timestamp <= kf2.timestamp:
                t = (timestamp - kf1.timestamp) / (kf2.timestamp - kf1.timestamp)
                return self._interpolate(kf1, kf2, t)
        return None

    def _interpolate(self, kf1: KeyFrame, kf2: KeyFrame, t: float) -> KeyFrame:
        return KeyFrame(
            timestamp=kf1.timestamp + (kf2.timestamp - kf1.timestamp) * t,
            x=kf1.x + (kf2.x - kf1.x) * t,
            y=kf1.y + (kf2.y - kf1.y) * t,
            width=kf1.width + (kf2.width - kf1.width) * t,
            height=kf1.height + (kf2.height - kf1.height) * t,
            opacity=kf1.opacity + (kf2.opacity - kf1.opacity) * t,
            rotation=kf1.rotation + (kf2.rotation - kf1.rotation) * t,
            scale=kf1.scale + (kf2.scale - kf1.scale) * t,
        )


class AnimationRecorder:
    """Records UI animations as clips."""

    def __init__(self) -> None:
        self._clips: Dict[str, AnimationClip] = {}
        self._recording: Optional[AnimationClip] = None
        self._recording_start: Optional[float] = None

    def start_recording(
        self,
        name: str,
        animation_type: AnimationType,
    ) -> None:
        self._recording = AnimationClip(
            name=name,
            animation_type=animation_type,
        )
        self._recording_start = time.time()

    def add_keyframe(
        self,
        x: float = 0.0,
        y: float = 0.0,
        width: float = 0.0,
        height: float = 0.0,
        opacity: float = 1.0,
        rotation: float = 0.0,
        scale: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if self._recording and self._recording_start is not None:
            timestamp = time.time() - self._recording_start
            kf = KeyFrame(
                timestamp=timestamp,
                x=x,
                y=y,
                width=width,
                height=height,
                opacity=opacity,
                rotation=rotation,
                scale=scale,
                metadata=metadata or {},
            )
            self._recording.add_keyframe(kf)

    def stop_recording(self) -> Optional[AnimationClip]:
        clip = self._recording
        if clip:
            self._clips[clip.name] = clip
        self._recording = None
        self._recording_start = None
        return clip

    @property
    def clips(self) -> Dict[str, AnimationClip]:
        return self._clips.copy()

    def get_clip(self, name: str) -> Optional[AnimationClip]:
        return self._clips.get(name)

    def remove_clip(self, name: str) -> bool:
        if name in self._clips:
            del self._clips[name]
            return True
        return False


__all__ = [
    "AnimationType",
    "KeyFrame",
    "AnimationClip",
    "AnimationRecorder",
]
