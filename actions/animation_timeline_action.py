"""
Animation Timeline Action Module

Provides timeline-based animation control with
keyframes, tracks, and playback management.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __default__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class KeyframeInterpolation(Enum):
    """Keyframe interpolation types."""

    LINEAR = "linear"
    HOLD = "hold"
    BEZIER = "bezier"
    EASING = "easing"


@dataclass
class Keyframe:
    """Animation keyframe."""

    time: float
    property: str
    value: Any
    interpolation: KeyframeInterpolation = KeyframeInterpolation.LINEAR
    easing_type: str = "ease_out"


@dataclass
class AnimationTrack:
    """Animation track containing keyframes."""

    name: str
    target: str
    keyframes: List[Keyframe] = field(default_factory=list)


@dataclass
class Timeline:
    """Animation timeline."""

    name: str
    tracks: List[AnimationTrack] = field(default_factory=list)
    duration: float = 1.0
    fps: int = 30


class AnimationTimeline:
    """
    Manages animation timelines with multiple tracks.

    Supports keyframe editing, playback control,
    and track management.
    """

    def __init__(self):
        self._timelines: Dict[str, Timeline] = {}
        self._current_time: float = 0
        self._is_playing: bool = False
        self._playback_callback: Optional[Callable[[float, Dict], None]] = None

    def create_timeline(
        self,
        name: str,
        duration: float = 1.0,
        fps: int = 30,
    ) -> Timeline:
        """Create a new timeline."""
        timeline = Timeline(name=name, duration=duration, fps=fps)
        self._timelines[name] = timeline
        return timeline

    def add_track(
        self,
        timeline_name: str,
        track_name: str,
        target: str,
    ) -> Optional[AnimationTrack]:
        """Add a track to timeline."""
        timeline = self._timelines.get(timeline_name)
        if not timeline:
            return None

        track = AnimationTrack(name=track_name, target=target)
        timeline.tracks.append(track)
        return track

    def add_keyframe(
        self,
        timeline_name: str,
        track_name: str,
        time: float,
        property: str,
        value: Any,
        interpolation: KeyframeInterpolation = KeyframeInterpolation.LINEAR,
    ) -> bool:
        """Add a keyframe to track."""
        timeline = self._timelines.get(timeline_name)
        if not timeline:
            return False

        track = next((t for t in timeline.tracks if t.name == track_name), None)
        if not track:
            return False

        keyframe = Keyframe(
            time=time,
            property=property,
            value=value,
            interpolation=interpolation,
        )
        track.keyframes.append(keyframe)
        track.keyframes.sort(key=lambda k: k.time)
        return True

    def get_value_at(
        self,
        timeline_name: str,
        track_name: str,
        time: float,
    ) -> Any:
        """Get interpolated value at time."""
        timeline = self._timelines.get(timeline_name)
        if not timeline:
            return None

        track = next((t for t in timeline.tracks if t.name == track_name), None)
        if not track or len(track.keyframes) == 0:
            return None

        if time <= track.keyframes[0].time:
            return track.keyframes[0].value

        if time >= track.keyframes[-1].time:
            return track.keyframes[-1].value

        for i in range(len(track.keyframes) - 1):
            kf1 = track.keyframes[i]
            kf2 = track.keyframes[i + 1]

            if kf1.time <= time <= kf2.time:
                t = (time - kf1.time) / (kf2.time - kf1.time)
                return self._interpolate(kf1.value, kf2.value, t)

        return None

    def _interpolate(self, start: Any, end: Any, t: float) -> Any:
        """Interpolate between values."""
        if isinstance(start, (int, float)) and isinstance(end, (int, float)):
            return start + (end - start) * t
        return end if t > 0.5 else start

    def play(self, timeline_name: str) -> bool:
        """Start timeline playback."""
        if timeline_name not in self._timelines:
            return False

        self._is_playing = True
        logger.info(f"Playing timeline: {timeline_name}")
        return True

    def stop(self) -> None:
        """Stop playback."""
        self._is_playing = False

    def seek(self, time: float) -> None:
        """Seek to time position."""
        self._current_time = max(0, time)

    def get_timeline(self, name: str) -> Optional[Timeline]:
        """Get timeline by name."""
        return self._timelines.get(name)


def create_animation_timeline() -> AnimationTimeline:
    """Factory function."""
    return AnimationTimeline()
