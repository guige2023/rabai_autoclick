"""
Window effects utilities.

Visual effects for window transitions and feedback.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class EffectType(Enum):
    """Types of window effects."""
    FADE = auto()
    SLIDE = auto()
    SCALE = auto()
    BLUR = auto()
    DIM = auto()
    HIGHLIGHT = auto()


@dataclass
class EffectConfig:
    """Configuration for an effect."""
    effect_type: EffectType
    duration_ms: int = 300
    easing: str = "ease_in_out"
    intensity: float = 1.0


class WindowEffect:
    """Base class for window effects."""
    
    def __init__(self, config: EffectConfig):
        self.config = config
    
    def apply(self, window_id: str) -> None:
        """Apply the effect to a window."""
        raise NotImplementedError
    
    def reverse(self, window_id: str) -> None:
        """Reverse the effect on a window."""
        raise NotImplementedError


class FadeEffect(WindowEffect):
    """Fade effect for windows."""
    
    def apply(self, window_id: str) -> None:
        """Apply fade in."""
        pass
    
    def reverse(self, window_id: str) -> None:
        """Apply fade out."""
        pass


class SlideEffect(WindowEffect):
    """Slide effect for windows."""
    
    def __init__(self, config: EffectConfig, direction: str = "left"):
        super().__init__(config)
        self.direction = direction


class ScaleEffect(WindowEffect):
    """Scale effect for windows."""
    
    def apply(self, window_id: str) -> None:
        """Apply scale up."""
        pass


class EffectComposer:
    """Compose multiple effects together."""
    
    def __init__(self):
        self._effects: list[WindowEffect] = []
    
    def add_effect(self, effect: WindowEffect) -> "EffectComposer":
        """Add an effect to the composition."""
        self._effects.append(effect)
        return self
    
    def apply_all(self, window_id: str) -> None:
        """Apply all effects in sequence."""
        for effect in self._effects:
            effect.apply(window_id)
    
    def reverse_all(self, window_id: str) -> None:
        """Reverse all effects in sequence."""
        for effect in reversed(self._effects):
            effect.reverse(window_id)
    
    def clear(self) -> None:
        """Clear all effects."""
        self._effects.clear()
