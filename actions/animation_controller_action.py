"""
Animation Controller Action Module

Controls UI animations, transitions, and visual effects
for smooth automation playback and feedback.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class EasingFunction(Enum):
    """Standard easing functions for animations."""

    LINEAR = "linear"
    EASE_IN = "ease_in"
    EASE_OUT = "ease_out"
    EASE_IN_OUT = "ease_in_out"
    BOUNCE = "bounce"
    ELASTIC = "elastic"
    SPRING = "spring"


class AnimationType(Enum):
    """Types of animations."""

    MOVE = "move"
    RESIZE = "resize"
    FADE = "fade"
    ROTATE = "rotate"
    SCALE = "scale"
    COLOR = "color"
    SEQUENCE = "sequence"
    PARALLEL = "parallel"


@dataclass
class Keyframe:
    """Represents an animation keyframe."""

    time: float  # 0.0 to 1.0 (normalized)
    value: Any
    easing: Optional[EasingFunction] = None


@dataclass
class AnimationConfig:
    """Configuration for animation playback."""

    duration: float = 0.3
    easing: EasingFunction = EasingFunction.EASE_OUT
    fps: int = 60
    loop: bool = False
    reverse: bool = False
    delay: float = 0.0


@dataclass
class AnimationFrame:
    """A single frame of animation output."""

    timestamp: float
    progress: float  # 0.0 to 1.0
    value: Any
    is_final: bool = False


@dataclass
class AnimationSequence:
    """A sequence of animations to play."""

    name: str
    animations: List[Dict[str, Any]] = field(default_factory=list)


class EasingCalculator:
    """Calculates easing function values."""

    @staticmethod
    def calculate(t: float, easing: EasingFunction) -> float:
        """
        Calculate eased progress value.

        Args:
            t: Normalized time (0.0 to 1.0)
            easing: Easing function to apply

        Returns:
            Eased value (0.0 to 1.0)
        """
        if easing == EasingFunction.LINEAR:
            return t

        elif easing == EasingFunction.EASE_IN:
            return t * t

        elif easing == EasingFunction.EASE_OUT:
            return 1 - (1 - t) * (1 - t)

        elif easing == EasingFunction.EASE_IN_OUT:
            if t < 0.5:
                return 2 * t * t
            return 1 - ((-2 * t + 2) ** 2) / 2

        elif easing == EasingFunction.BOUNCE:
            return EasingCalculator._bounce(t)

        elif easing == EasingFunction.ELASTIC:
            return EasingCalculator._elastic(t)

        elif easing == EasingFunction.SPRING:
            return EasingCalculator._spring(t)

        return t

    @staticmethod
    def _bounce(t: float) -> float:
        """Bounce easing function."""
        if t < 0.5:
            return 8 * t * t * t * t
        t = 2 * t - 2
        return 1 - (-t * t * t * t) / 2 + 1

    @staticmethod
    def _elastic(t: float) -> float:
        """Elastic easing function."""
        if t == 0 or t == 1:
            return t
        p = 0.3
        s = p / 4
        t = t - 1
        return -(2 ** (10 * t)) * math.sin((t - s) * (2 * math.pi) / p) + 1

    @staticmethod
    def _spring(t: float) -> float:
        """Spring easing function."""
        k = 0.3
        return 1 - math.cos(t * math.pi * 2) * math.exp(-t / k)


class AnimationController:
    """
    Controls animations and visual transitions.

    Supports keyframe-based animations, easing functions,
    sequences, and parallel playback with callbacks.
    """

    def __init__(
        self,
        frame_callback: Optional[Callable[[AnimationFrame], None]] = None,
    ):
        self.frame_callback = frame_callback
        self._is_playing: bool = False
        self._is_paused: bool = False
        self._current_time: float = 0
        self._sequences: Dict[str, AnimationSequence] = {}
        self._active_animations: Dict[str, Dict[str, Any]] = {}

    def register_sequence(self, sequence: AnimationSequence) -> None:
        """Register an animation sequence."""
        self._sequences[sequence.name] = sequence

    def play(
        self,
        name: str,
        config: Optional[AnimationConfig] = None,
    ) -> bool:
        """
        Play an animation sequence.

        Args:
            name: Name of sequence to play
            config: Animation configuration

        Returns:
            True if playback started successfully
        """
        if name not in self._sequences:
            logger.warning(f"Animation sequence not found: {name}")
            return False

        config = config or AnimationConfig()
        self._is_playing = True
        self._is_paused = False
        self._current_time = 0

        sequence = self._sequences[name]
        self._active_animations[name] = {
            "config": config,
            "start_time": time.time(),
            "sequence": sequence,
        }

        return True

    def update(self) -> None:
        """Update all active animations. Call this in a loop."""
        if not self._is_playing or self._is_paused:
            return

        completed: List[str] = []

        for name, anim in self._active_animations.items():
            config = anim["config"]
            elapsed = time.time() - anim["start_time"] - config.delay

            if elapsed < 0:
                continue

            progress = min(elapsed / config.duration, 1.0)

            if config.reverse:
                progress = 1.0 - progress

            eased_progress = EasingCalculator.calculate(progress, config.easing)

            frame = AnimationFrame(
                timestamp=time.time(),
                progress=eased_progress,
                value=self._calculate_frame_value(anim["sequence"], eased_progress),
                is_final=progress >= 1.0,
            )

            if self.frame_callback:
                self.frame_callback(frame)

            if progress >= 1.0:
                completed.append(name)

        for name in completed:
            if self._active_animations[name]["config"].loop:
                self._active_animations[name]["start_time"] = time.time()
            else:
                del self._active_animations[name]

        if not self._active_animations:
            self._is_playing = False

    def _calculate_frame_value(
        self,
        sequence: AnimationSequence,
        progress: float,
    ) -> Dict[str, Any]:
        """Calculate animation values for current frame."""
        result = {}

        for anim in sequence.animations:
            anim_type = anim.get("type")
            start = anim.get("start")
            end = anim.get("end")

            if anim_type == AnimationType.MOVE.value:
                result["x"] = self._lerp(start[0], end[0], progress)
                result["y"] = self._lerp(start[1], end[1], progress)

            elif anim_type == AnimationType.RESIZE.value:
                result["width"] = self._lerp(start[0], end[0], progress)
                result["height"] = self._lerp(start[1], end[1], progress)

            elif anim_type == AnimationType.FADE.value:
                result["opacity"] = self._lerp(start, end, progress)

            elif anim_type == AnimationType.SCALE.value:
                result["scale"] = self._lerp(start, end, progress)

            elif anim_type == AnimationType.ROTATE.value:
                result["angle"] = self._lerp(start, end, progress)

        return result

    def _lerp(self, start: float, end: float, t: float) -> float:
        """Linear interpolation between two values."""
        return start + (end - start) * t

    def pause(self) -> None:
        """Pause all active animations."""
        self._is_paused = True

    def resume(self) -> None:
        """Resume paused animations."""
        self._is_paused = False
        for anim in self._active_animations.values():
            remaining = anim["config"].duration - (time.time() - anim["start_time"])
            anim["start_time"] = time.time() - (anim["config"].duration - remaining)

    def stop(self, name: Optional[str] = None) -> None:
        """
        Stop animation(s).

        Args:
            name: Specific animation to stop (all if None)
        """
        if name:
            if name in self._active_animations:
                del self._active_animations[name]
        else:
            self._active_animations.clear()
            self._is_playing = False

    def seek(self, name: str, progress: float) -> None:
        """
        Seek to a specific progress point.

        Args:
            name: Animation name
            progress: Target progress (0.0 to 1.0)
        """
        if name in self._active_animations:
            anim = self._active_animations[name]
            anim["start_time"] = time.time() - (progress * anim["config"].duration)

    @property
    def is_playing(self) -> bool:
        """Check if any animation is playing."""
        return self._is_playing

    def create_move_animation(
        self,
        name: str,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        config: Optional[AnimationConfig] = None,
    ) -> None:
        """Helper to create a move animation."""
        sequence = AnimationSequence(
            name=name,
            animations=[
                {
                    "type": AnimationType.MOVE.value,
                    "start": (start_x, start_y),
                    "end": (end_x, end_y),
                }
            ],
        )
        self.register_sequence(sequence)

    def create_fade_animation(
        self,
        name: str,
        start_opacity: float,
        end_opacity: float,
        config: Optional[AnimationConfig] = None,
    ) -> None:
        """Helper to create a fade animation."""
        sequence = AnimationSequence(
            name=name,
            animations=[
                {
                    "type": AnimationType.FADE.value,
                    "start": start_opacity,
                    "end": end_opacity,
                }
            ],
        )
        self.register_sequence(sequence)

    def create_scale_animation(
        self,
        name: str,
        start_scale: float,
        end_scale: float,
        config: Optional[AnimationConfig] = None,
    ) -> None:
        """Helper to create a scale animation."""
        sequence = AnimationSequence(
            name=name,
            animations=[
                {
                    "type": AnimationType.SCALE.value,
                    "start": start_scale,
                    "end": end_scale,
                }
            ],
        )
        self.register_sequence(sequence)


def create_animation_controller(
    frame_callback: Optional[Callable[[AnimationFrame], None]] = None,
) -> AnimationController:
    """Factory function to create an AnimationController."""
    return AnimationController(frame_callback=frame_callback)
