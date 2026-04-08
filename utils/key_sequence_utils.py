"""Key sequence recording and playback utilities.

This module provides utilities for recording and playing back
sequences of key presses, useful for macro recording and playback.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class KeyPress:
    """Represents a single key press event."""
    key: str
    modifiers: list[str] = field(default_factory=list)
    action: str = "press"  # 'press' or 'release'
    timestamp: float = 0.0
    
    def to_tuple(self) -> tuple:
        return (self.key, tuple(self.modifiers), self.action, self.timestamp)
    
    def __hash__(self) -> int:
        return hash(self.to_tuple())


class KeySequence:
    """Records and plays back sequences of key presses."""
    
    def __init__(self, name: str = "unnamed"):
        self.name = name
        self._presses: list[KeyPress] = []
        self._start_time: Optional[float] = None
    
    def start(self) -> None:
        """Start recording a new sequence."""
        self._presses = []
        self._start_time = time.monotonic()
    
    def record(self, key: str, modifiers: Optional[list[str]] = None) -> None:
        """Record a key press.
        
        Args:
            key: The key that was pressed.
            modifiers: List of active modifiers (ctrl, alt, shift, cmd).
        """
        if self._start_time is None:
            self.start()
        
        timestamp = time.monotonic() - self._start_time
        press = KeyPress(
            key=key.lower(),
            modifiers=[m.lower() for m in (modifiers or [])],
            action="press",
            timestamp=timestamp,
        )
        self._presses.append(press)
    
    def stop(self) -> int:
        """Stop recording.
        
        Returns:
            Number of key presses recorded.
        """
        count = len(self._presses)
        self._start_time = None
        return count
    
    def get_presses(self) -> list[KeyPress]:
        """Get all recorded key presses."""
        return list(self._presses)
    
    def get_duration(self) -> float:
        """Get the total duration of the sequence."""
        if not self._presses:
            return 0.0
        return self._presses[-1].timestamp
    
    def replay(
        self,
        speed: float = 1.0,
        loop: bool = False,
    ) -> None:
        """Replay the recorded key sequence.
        
        Args:
            speed: Playback speed multiplier.
            loop: Whether to loop the playback.
        """
        try:
            import pyautogui
        except ImportError:
            return
        
        while True:
            last_timestamp = 0.0
            
            for press in self._presses:
                wait_time = (press.timestamp - last_timestamp) / speed
                if wait_time > 0:
                    time.sleep(wait_time)
                
                # Build the key string
                key_parts = press.modifiers + [press.key]
                key_str = "+".join(key_parts)
                
                pyautogui.keyDown(key_str)
                time.sleep(0.01)
                pyautogui.keyUp(key_str)
                
                last_timestamp = press.timestamp
            
            if not loop:
                break
    
    def append(self, other: "KeySequence") -> None:
        """Append another sequence to this one.
        
        Args:
            other: KeySequence to append.
        """
        offset = self.get_duration()
        for press in other._presses:
            new_press = KeyPress(
                key=press.key,
                modifiers=list(press.modifiers),
                action=press.action,
                timestamp=press.timestamp + offset,
            )
            self._presses.append(new_press)
    
    def __len__(self) -> int:
        return len(self._presses)


# Global sequence for tracking current recording
_current_sequence: Optional[KeySequence] = None


def start_recording(name: str = "recording") -> KeySequence:
    """Start recording a new key sequence.
    
    Args:
        name: Name for the sequence.
    
    Returns:
        The new KeySequence instance.
    """
    global _current_sequence
    _current_sequence = KeySequence(name)
    _current_sequence.start()
    return _current_sequence


def stop_recording() -> KeySequence:
    """Stop the current recording.
    
    Returns:
        The recorded KeySequence.
    """
    global _current_sequence
    if _current_sequence is None:
        return KeySequence("empty")
    _current_sequence.stop()
    return _current_sequence


def get_current_sequence() -> Optional[KeySequence]:
    """Get the currently recording sequence."""
    return _current_sequence
