"""
UI Feedback Action Module

Provides visual and audio feedback for automation actions,
including highlights, overlays, notifications, and sounds.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class FeedbackType(Enum):
    """Types of feedback."""

    HIGHLIGHT = "highlight"
    OVERLAY = "overlay"
    CURSOR_CHANGE = "cursor_change"
    SOUND = "sound"
    NOTIFICATION = "notification"
    ANIMATION = "animation"
    HAPTIC = "haptic"


class HighlightStyle(Enum):
    """Highlight styles."""

    RECTANGLE = "rectangle"
    ROUNDED_RECT = "rounded_rect"
    CIRCLE = "circle"
    UNDERLINE = "underline"
    GLOW = "glow"


@dataclass
class FeedbackConfig:
    """Configuration for UI feedback."""

    highlight_duration: float = 0.5
    highlight_style: HighlightStyle = HighlightStyle.RECTANGLE
    highlight_color: str = "#00FF00"
    highlight_border_width: int = 2
    sound_enabled: bool = True
    animation_enabled: bool = True
    default_volume: float = 0.5


@dataclass
class HighlightRegion:
    """Region to highlight."""

    x: int
    y: int
    width: int
    height: int
    style: HighlightStyle = HighlightStyle.RECTANGLE
    color: str = "#00FF00"
    border_width: int = 2
    corner_radius: int = 5


@dataclass
class FeedbackEvent:
    """Represents a feedback event."""

    type: FeedbackType
    timestamp: float
    data: Dict[str, Any] = field(default_factory=dict)


class UIFeedback:
    """
    Provides visual and audio feedback for automation.

    Supports highlights, overlays, sound effects,
    notifications, and haptic feedback.
    """

    def __init__(
        self,
        config: Optional[FeedbackConfig] = None,
        renderer: Optional[Callable[[FeedbackEvent], None]] = None,
        sound_player: Optional[Callable[[str, float], None]] = None,
    ):
        self.config = config or FeedbackConfig()
        self.renderer = renderer
        self.sound_player = sound_player or self._default_sound_player
        self._feedback_history: List[FeedbackEvent] = []
        self._active_highlights: Dict[str, HighlightRegion] = {}
        self._enabled: bool = True

    def _default_sound_player(self, sound: str, volume: float) -> None:
        """Default sound player (logs the sound)."""
        logger.debug(f"Playing sound: {sound} at volume {volume}")

    def highlight(
        self,
        x: int,
        y: int,
        width: int,
        height: int,
        style: Optional[HighlightStyle] = None,
        color: Optional[str] = None,
        duration: Optional[float] = None,
        label: Optional[str] = "default",
    ) -> None:
        """
        Show a highlight around a region.

        Args:
            x: X coordinate
            y: Y coordinate
            width: Width of highlight region
            height: Height of highlight region
            style: Highlight style
            color: Highlight color
            duration: Duration in seconds (auto-hide if None)
            label: Identifier for this highlight
        """
        if not self._enabled:
            return

        style = style or self.config.highlight_style
        color = color or self.config.highlight_color
        duration = duration or self.config.highlight_duration

        region = HighlightRegion(
            x=x,
            y=y,
            width=width,
            height=height,
            style=style,
            color=color,
            border_width=self.config.highlight_border_width,
        )

        self._active_highlights[label] = region

        event = FeedbackEvent(
            type=FeedbackType.HIGHLIGHT,
            timestamp=time.time(),
            data={
                "region": region,
                "action": "show",
                "label": label,
            },
        )
        self._feedback_history.append(event)

        if self.renderer:
            self.renderer(event)

        if duration > 0:
            def hide_later():
                time.sleep(duration)
                self.hide_highlight(label)

            import threading
            t = threading.Thread(target=hide_later, daemon=True)
            t.start()

    def hide_highlight(self, label: str = "default") -> None:
        """
        Hide a highlight.

        Args:
            label: Highlight identifier
        """
        if label in self._active_highlights:
            del self._active_highlights[label]

        event = FeedbackEvent(
            type=FeedbackType.HIGHLIGHT,
            timestamp=time.time(),
            data={"action": "hide", "label": label},
        )

        if self.renderer:
            self.renderer(event)

    def hide_all_highlights(self) -> None:
        """Hide all active highlights."""
        labels = list(self._active_highlights.keys())
        for label in labels:
            self.hide_highlight(label)

    def show_overlay(
        self,
        x: int,
        y: int,
        width: int,
        height: int,
        opacity: float = 0.3,
        color: str = "#000000",
        duration: Optional[float] = None,
    ) -> str:
        """
        Show an overlay on screen region.

        Args:
            x: X coordinate
            y: Y coordinate
            width: Overlay width
            height: Overlay height
            opacity: Overlay opacity (0.0 to 1.0)
            color: Overlay color
            duration: Duration in seconds

        Returns:
            Overlay identifier
        """
        if not self._enabled:
            return ""

        overlay_id = f"overlay_{time.time()}"

        event = FeedbackEvent(
            type=FeedbackType.OVERLAY,
            timestamp=time.time(),
            data={
                "id": overlay_id,
                "x": x,
                "y": y,
                "width": width,
                "height": height,
                "opacity": opacity,
                "color": color,
                "action": "show",
            },
        )
        self._feedback_history.append(event)

        if self.renderer:
            self.renderer(event)

        if duration and duration > 0:
            def hide_later():
                time.sleep(duration)
                self.hide_overlay(overlay_id)

            import threading
            t = threading.Thread(target=hide_later, daemon=True)
            t.start()

        return overlay_id

    def hide_overlay(self, overlay_id: str) -> None:
        """Hide an overlay by ID."""
        event = FeedbackEvent(
            type=FeedbackType.OVERLAY,
            timestamp=time.time(),
            data={"id": overlay_id, "action": "hide"},
        )

        if self.renderer:
            self.renderer(event)

    def play_sound(
        self,
        sound_name: str,
        volume: Optional[float] = None,
    ) -> None:
        """
        Play a sound effect.

        Args:
            sound_name: Name of sound to play
            volume: Volume level (0.0 to 1.0)
        """
        if not self._enabled or not self.config.sound_enabled:
            return

        volume = volume or self.config.default_volume

        event = FeedbackEvent(
            type=FeedbackType.SOUND,
            timestamp=time.time(),
            data={"name": sound_name, "volume": volume},
        )
        self._feedback_history.append(event)

        self.sound_player(sound_name, volume)

    def show_notification(
        self,
        title: str,
        message: str,
        icon: Optional[str] = None,
        duration: float = 3.0,
    ) -> None:
        """
        Show a notification.

        Args:
            title: Notification title
            message: Notification message
            icon: Optional icon identifier
            duration: Duration in seconds
        """
        if not self._enabled:
            return

        event = FeedbackEvent(
            type=FeedbackType.NOTIFICATION,
            timestamp=time.time(),
            data={
                "title": title,
                "message": message,
                "icon": icon,
                "duration": duration,
            },
        )
        self._feedback_history.append(event)

        if self.renderer:
            self.renderer(event)

    def animate_element(
        self,
        element_id: str,
        animation_type: str,
        params: Dict[str, Any],
    ) -> None:
        """
        Trigger an animation on an element.

        Args:
            element_id: Element identifier
            animation_type: Type of animation
            params: Animation parameters
        """
        if not self._enabled or not self.config.animation_enabled:
            return

        event = FeedbackEvent(
            type=FeedbackType.ANIMATION,
            timestamp=time.time(),
            data={
                "element_id": element_id,
                "animation_type": animation_type,
                "params": params,
            },
        )
        self._feedback_history.append(event)

        if self.renderer:
            self.renderer(event)

    def flash_screen(
        self,
        color: str = "#FFFFFF",
        duration: float = 0.1,
        opacity: float = 0.5,
    ) -> None:
        """
        Flash the screen.

        Args:
            color: Flash color
            duration: Duration in seconds
            opacity: Flash opacity
        """
        event = FeedbackEvent(
            type=FeedbackType.OVERLAY,
            timestamp=time.time(),
            data={
                "action": "flash",
                "color": color,
                "duration": duration,
                "opacity": opacity,
                "fullscreen": True,
            },
        )

        if self.renderer:
            self.renderer(event)

    def pulse_cursor(self) -> None:
        """Pulse the cursor to indicate action."""
        event = FeedbackEvent(
            type=FeedbackType.CURSOR_CHANGE,
            timestamp=time.time(),
            data={"cursor_type": "pulse"},
        )

        if self.renderer:
            self.renderer(event)

    def enable(self) -> None:
        """Enable all feedback."""
        self._enabled = True

    def disable(self) -> None:
        """Disable all feedback."""
        self._enabled = False

    def get_active_highlights(self) -> Dict[str, HighlightRegion]:
        """Get all active highlights."""
        return self._active_highlights.copy()

    def get_feedback_history(
        self,
        limit: Optional[int] = None,
    ) -> List[FeedbackEvent]:
        """
        Get feedback history.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of recent feedback events
        """
        if limit:
            return self._feedback_history[-limit:]
        return self._feedback_history.copy()

    def clear_history(self) -> None:
        """Clear feedback history."""
        self._feedback_history.clear()


def create_ui_feedback(
    config: Optional[FeedbackConfig] = None,
) -> UIFeedback:
    """Factory function to create a UIFeedback instance."""
    return UIFeedback(config=config)
