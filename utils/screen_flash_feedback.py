"""Screen flash feedback utility for providing visual confirmation."""
from typing import Optional, Tuple
from dataclasses import dataclass
import time


@dataclass
class FlashConfig:
    """Configuration for flash feedback."""
    duration: float = 0.15
    color: Tuple[int, int, int, int] = (255, 255, 255, 180)
    radius: Optional[int] = None
    fade_out: bool = True


class ScreenFlashFeedback:
    """Provides screen flash feedback for touch and gesture confirmation.
    
    Shows visual flashes at touch/click locations to provide feedback,
    especially useful for accessibility and touch confirmation.
    
    Example:
        feedback = ScreenFlashFeedback()
        feedback.flash(point=(100, 200), config=FlashConfig(duration=0.2))
    """

    def __init__(
        self,
        default_duration: float = 0.15,
        default_color: Tuple[int, int, int, int] = (255, 255, 255, 180),
    ) -> None:
        self._default_duration = default_duration
        self._default_color = default_color
        self._last_flash_time: float = 0
        self._enabled = True

    def flash(
        self,
        point: Tuple[int, int],
        config: Optional[FlashConfig] = None,
        duration: Optional[float] = None,
        color: Optional[Tuple[int, int, int, int]] = None,
    ) -> bool:
        """Trigger a flash at the specified point."""
        if not self._enabled:
            return False
        
        cfg = config or FlashConfig()
        dur = duration if duration is not None else cfg.duration
        col = color if color is not None else cfg.color
        
        if time.time() - self._last_flash_time < 0.05:
            return False
        
        self._last_flash_time = time.time()
        return self._render_flash(point, dur, col, cfg.radius, cfg.fade_out)

    def flash_success(self, point: Tuple[int, int]) -> bool:
        """Flash green success indicator."""
        return self.flash(point, color=(0, 255, 0, 200))

    def flash_error(self, point: Tuple[int, int]) -> bool:
        """Flash red error indicator."""
        return self.flash(point, color=(255, 0, 0, 200))

    def flash_warning(self, point: Tuple[int, int]) -> bool:
        """Flash yellow warning indicator."""
        return self.flash(point, color=(255, 200, 0, 200))

    def enable(self) -> None:
        """Enable flash feedback."""
        self._enabled = True

    def disable(self) -> None:
        """Disable flash feedback."""
        self._enabled = False

    def is_enabled(self) -> bool:
        """Check if feedback is enabled."""
        return self._enabled

    def _render_flash(
        self,
        point: Tuple[int, int],
        duration: float,
        color: Tuple[int, int, int, int],
        radius: Optional[int],
        fade_out: bool,
    ) -> bool:
        """Render the actual flash (stub - implement with platform-specific code)."""
        return True
