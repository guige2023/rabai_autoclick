"""Timeline and keyframe animation utilities for RabAI AutoClick.

Provides:
- Keyframe interpolation
- Timeline sequencing
- Easing functions for animations
- Animation clip management
"""

from typing import Callable, Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
import math


@dataclass
class Keyframe:
    """A single keyframe in an animation."""
    time: float
    value: float
    easing: str = "linear"  # linear, ease_in, ease_out, ease_in_out, cubic
    tangent: Optional[float] = None  # For cubic Hermite


def lerp(a: float, b: float, t: float) -> float:
    """Linear interpolation."""
    return a + (b - a) * t


def ease_in(t: float) -> float:
    """Ease in: quadratic."""
    return t * t


def ease_out(t: float) -> float:
    """Ease out: quadratic."""
    return t * (2 - t)


def ease_in_out(t: float) -> float:
    """Ease in-out: cubic."""
    return t * t * (3 - 2 * t)


def cubic_ease(t: float) -> float:
    """Cubic ease."""
    return t * t * t


def elastic_out(t: float) -> float:
    """Elastic ease out."""
    if t == 0 or t == 1:
        return t
    p = 0.3
    return math.pow(2, -10 * t) * math.sin((t - p / 4) * (2 * math.pi) / p) + 1


def bounce_out(t: float) -> float:
    """Bounce ease out."""
    if t < 1 / 2.75:
        return 7.5625 * t * t
    elif t < 2 / 2.75:
        t -= 1.5 / 2.75
        return 7.5625 * t * t + 0.75
    elif t < 2.5 / 2.75:
        t -= 2.25 / 2.75
        return 7.5625 * t * t + 0.9375
    else:
        t -= 2.625 / 2.75
        return 7.5625 * t * t + 0.984375


EASING_MAP: Dict[str, Callable[[float], float]] = {
    "linear": lambda t: t,
    "ease_in": ease_in,
    "ease_out": ease_out,
    "ease_in_out": ease_in_out,
    "cubic": cubic_ease,
    "elastic_out": elastic_out,
    "bounce_out": bounce_out,
}


def interpolate_keyframes(
    keyframes: List[Keyframe],
    time: float,
) -> float:
    """Interpolate value at given time from keyframes.

    Args:
        keyframes: Sorted list of keyframes.
        time: Query time.

    Returns:
        Interpolated value.
    """
    if not keyframes:
        return 0.0
    if len(keyframes) == 1:
        return keyframes[0].value

    # Before first keyframe
    if time <= keyframes[0].time:
        return keyframes[0].value
    # After last keyframe
    if time >= keyframes[-1].time:
        return keyframes[-1].value

    # Find surrounding keyframes
    for i in range(len(keyframes) - 1):
        k0, k1 = keyframes[i], keyframes[i + 1]
        if k0.time <= time <= k1.time:
            t = (time - k0.time) / (k1.time - k0.time)
            easing_fn = EASING_MAP.get(k0.easing, lerp)
            et = easing_fn(t)
            return lerp(k0.value, k1.value, et)

    return keyframes[-1].value


@dataclass
class Track:
    """A single animation track (e.g., X position, Y position)."""
    name: str
    keyframes: List[Keyframe] = field(default_factory=list)

    def get_value(self, time: float) -> float:
        """Get interpolated value at time."""
        return interpolate_keyframes(
            sorted(self.keyframes, key=lambda k: k.time),
            time
        )

    def add_keyframe(self, time: float, value: float, easing: str = "linear") -> None:
        """Add a keyframe to this track."""
        self.keyframes.append(Keyframe(time=time, value=value, easing=easing))


@dataclass
class AnimationClip:
    """An animation clip containing multiple tracks."""
    name: str
    duration: float
    tracks: Dict[str, Track] = field(default_factory=dict)
    loop: bool = False

    def add_track(self, name: str) -> Track:
        """Add a new track."""
        track = Track(name=name)
        self.tracks[name] = track
        return track

    def get_track(self, name: str) -> Optional[Track]:
        """Get track by name."""
        return self.tracks.get(name)

    def get_value(self, time: float, track_name: str) -> Optional[float]:
        """Get value at time for a track."""
        if track_name not in self.tracks:
            return None
        t = time
        if self.loop:
            t = time % self.duration
        elif t > self.duration:
            return None
        return self.tracks[track_name].get_value(t)

    def get_all_values(self, time: float) -> Dict[str, float]:
        """Get all track values at given time."""
        result: Dict[str, float] = {}
        for name, track in self.tracks.items():
            val = self.get_value(time, name)
            if val is not None:
                result[name] = val
        return result


@dataclass
class Timeline:
    """A timeline that manages multiple animation clips."""
    clips: Dict[str, AnimationClip] = field(default_factory=dict)
    current_clip: Optional[str] = None
    time: float = 0.0
    playing: bool = False

    def add_clip(self, clip: AnimationClip) -> None:
        """Add a clip to timeline."""
        self.clips[clip.name] = clip

    def play(self, clip_name: str, from_time: float = 0.0) -> None:
        """Start playing a clip."""
        if clip_name in self.clips:
            self.current_clip = clip_name
            self.time = from_time
            self.playing = True

    def pause(self) -> None:
        """Pause playback."""
        self.playing = False

    def stop(self) -> None:
        """Stop and reset playback."""
        self.playing = False
        self.time = 0.0

    def update(self, delta_time: float) -> None:
        """Update timeline by delta time."""
        if not self.playing or not self.current_clip:
            return
        clip = self.clips[self.current_clip]
        self.time += delta_time
        if self.time >= clip.duration:
            if clip.loop:
                self.time = self.time % clip.duration
            else:
                self.time = clip.duration
                self.playing = False

    def get_current_values(self) -> Dict[str, float]:
        """Get all values at current time."""
        if not self.current_clip:
            return {}
        return self.clips[self.current_clip].get_all_values(self.time)


def lerp_keyframes(
    kf1: Keyframe,
    kf2: Keyframe,
    time: float,
) -> float:
    """Lerp between two keyframes with easing."""
    t = max(0.0, min(1.0, (time - kf1.time) / (kf2.time - kf1.time)))
    easing = EASING_MAP.get(kf1.easing, lerp)
    return lerp(kf1.value, kf2.value, easing(t))


def create_position_tween(
    start: Tuple[float, float],
    end: Tuple[float, float],
    duration: float,
    easing: str = "ease_in_out",
) -> AnimationClip:
    """Create a simple position tween animation.

    Args:
        start: (x, y) start position.
        end: (x, y) end position.
        duration: Animation duration in seconds.
        easing: Easing name.

    Returns:
        Animation clip.
    """
    clip = AnimationClip(name="position_tween", duration=duration)
    tx = clip.add_track("x")
    ty = clip.add_track("y")
    tx.add_keyframe(0.0, start[0], easing=easing)
    tx.add_keyframe(duration, end[0], easing=easing)
    ty.add_keyframe(0.0, start[1], easing=easing)
    ty.add_keyframe(duration, end[1], easing=easing)
    return clip
