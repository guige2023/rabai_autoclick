"""Animation physics engine for realistic motion simulation."""
from typing import Dict, List, Optional, Tuple, Any, Callable
from dataclasses import dataclass
from enum import Enum, auto
import math


class EasingType(Enum):
    """Types of easing functions."""
    LINEAR = auto()
    EASE_IN = auto()
    EASE_OUT = auto()
    EASE_IN_OUT = auto()
    BOUNCE = auto()
    ELASTIC = auto()


@dataclass
class AnimationFrame:
    """A single frame of animation."""
    timestamp: float
    x: float
    y: float
    scale: float
    rotation: float
    alpha: float


class AnimationPhysics:
    """Physics engine for computing realistic animation motion.
    
    Calculates intermediate animation frames using physics-based
    easing, spring dynamics, and motion curves.
    
    Example:
        physics = AnimationPhysics()
        frames = physics.compute_frames(
            start=(0, 0),
            end=(100, 100),
            duration=1.0,
            easing=EasingType.EASE_OUT,
        )
    """

    def __init__(self, fps: int = 60) -> None:
        self._fps = fps
        self._dt = 1.0 / fps

    def compute_frames(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
        duration: float,
        easing: EasingType = EasingType.EASE_OUT,
        scale_start: float = 1.0,
        scale_end: float = 1.0,
        rotation_start: float = 0.0,
        rotation_end: float = 0.0,
        alpha_start: float = 1.0,
        alpha_end: float = 1.0,
    ) -> List[AnimationFrame]:
        """Compute animation frames from start to end state."""
        frames: List[AnimationFrame] = []
        num_frames = int(duration * self._fps)
        
        for i in range(num_frames + 1):
            t = i / num_frames
            eased_t = self._apply_easing(t, easing)
            
            x = start[0] + (end[0] - start[0]) * eased_t
            y = start[1] + (end[1] - start[1]) * eased_t
            scale = scale_start + (scale_end - scale_start) * eased_t
            rotation = rotation_start + (rotation_end - rotation_start) * eased_t
            alpha = alpha_start + (alpha_end - alpha_start) * eased_t
            
            frames.append(AnimationFrame(
                timestamp=t * duration,
                x=x,
                y=y,
                scale=scale,
                rotation=rotation,
                alpha=alpha,
            ))
        
        return frames

    def _apply_easing(self, t: float, easing: EasingType) -> float:
        """Apply easing function to normalized time (0-1)."""
        if easing == EasingType.LINEAR:
            return t
        elif easing == EasingType.EASE_IN:
            return t * t
        elif easing == EasingType.EASE_OUT:
            return 1 - (1 - t) * (1 - t)
        elif easing == EasingType.EASE_IN_OUT:
            return t * t * (3 - 2 * t) if t < 0.5 else 1 - ((-2 * t + 2) ** 2) / 2
        elif easing == EasingType.BOUNCE:
            return self._bounce_ease(t)
        elif easing == EasingType.ELASTIC:
            return self._elastic_ease(t)
        return t

    def _bounce_ease(self, t: float) -> float:
        """Bounce easing function."""
        if t < 0.5:
            return 0.5 * self._bounce_ease(t * 2)
        return 0.5 * self._bounce_ease(t * 2 - 1) + 0.5

    def _bounce_ease_inner(self, t: float) -> float:
        """Inner bounce calculation."""
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

    def _elastic_ease(self, t: float) -> float:
        """Elastic easing function."""
        if t == 0 or t == 1:
            return t
        return math.sin(-13 * math.pi / 2 * (t + 1)) * ((2 ** (-10 * t)) + 1)

    def spring_animate(
        self,
        start: float,
        end: float,
        velocity: float,
        stiffness: float = 180.0,
        damping: float = 12.0,
        mass: float = 1.0,
    ) -> List[Tuple[float, float]]:
        """Simulate spring physics animation.
        
        Returns list of (time, value) tuples.
        """
        result: List[Tuple[float, float]] = []
        t = 0.0
        dt = self._dt
        current = start
        v = velocity
        
        omega = math.sqrt(stiffness / mass)
        zeta = damping / (2 * math.sqrt(stiffness * mass))
        
        for _ in range(600):
            result.append((t, current))
            
            if abs(end - current) < 0.001 and abs(v) < 0.001:
                break
            
            if zeta < 1.0:
                decay = math.exp(-zeta * omega * t)
                current = end - decay * (
                    (end - start) * math.cos(omega * math.sqrt(1 - zeta**2) * t) +
                    (v + zeta * omega * (end - start)) / (omega * math.sqrt(1 - zeta**2)) * math.sin(omega * math.sqrt(1 - zeta**2) * t)
                )
            else:
                decay = math.exp(-omega * t)
                current = end - decay * ((end - start) + (v + omega * (end - start)) * t)
            
            t += dt
        
        return result
