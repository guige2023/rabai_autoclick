"""
Key Hold Simulation Utilities

Provides utilities for simulating keyboard key hold duration,
repeat rate configuration, and chord hold patterns.
"""

from typing import Optional, Callable, List, Tuple, Dict
from dataclasses import dataclass
from enum import Enum
import time


class HoldPattern(Enum):
    """Key hold pattern types."""
    SINGLE = "single"
    REPEAT = "repeat"
    CHORD = "chord"
    STAGGERED = "staggered"
    BURST = "burst"


@dataclass
class KeyHoldEvent:
    """Represents a key hold action."""
    key: str
    start_time: float
    end_time: Optional[float] = None
    hold_duration: float = 0.0
    repeat_count: int = 0
    
    @property
    def duration(self) -> float:
        """Calculate actual hold duration."""
        if self.end_time is not None:
            return self.end_time - self.start_time
        return self.hold_duration
    
    @property
    def is_active(self) -> bool:
        """Check if key hold is still active."""
        return self.end_time is None


@dataclass
class KeyHoldConfig:
    """Configuration for key hold simulation."""
    initial_delay: float = 0.25
    repeat_delay: float = 0.05
    release_delay: float = 0.05
    min_hold_duration: float = 0.1
    max_hold_duration: float = 5.0


class KeyHoldSimulator:
    """
    Simulates keyboard key hold operations with configurable timing.
    
    Supports single key holds, key repeats, and chord holds for
    keyboard automation tasks.
    
    Example:
        >>> simulator = KeyHoldSimulator(KeyHoldConfig(
        ...     initial_delay=0.2,
        ...     repeat_delay=0.03
        ... ))
        >>> simulator.start_hold("a")
        >>> time.sleep(1.0)
        >>> simulator.end_hold("a")
    """
    
    def __init__(self, config: Optional[KeyHoldConfig] = None) -> None:
        """
        Initialize key hold simulator.
        
        Args:
            config: Hold configuration parameters.
        """
        self._config = config or KeyHoldConfig()
        self._active_holds: Dict[str, KeyHoldEvent] = {}
        self._repeat_callback: Optional[Callable[[str], None]] = None
    
    def start_hold(
        self,
        key: str,
        timestamp: Optional[float] = None
    ) -> KeyHoldEvent:
        """
        Start holding a key.
        
        Args:
            key: The key to hold.
            timestamp: Optional timestamp (defaults to current time).
            
        Returns:
            KeyHoldEvent for the started hold.
        """
        timestamp = timestamp or time.time()
        
        if key in self._active_holds:
            self.end_hold(key)
        
        event = KeyHoldEvent(
            key=key,
            start_time=timestamp,
            hold_duration=self._config.initial_delay
        )
        self._active_holds[key] = event
        return event
    
    def end_hold(
        self,
        key: str,
        timestamp: Optional[float] = None
    ) -> Optional[KeyHoldEvent]:
        """
        End holding a key.
        
        Args:
            key: The key to release.
            timestamp: Optional timestamp.
            
        Returns:
            The completed KeyHoldEvent or None if not held.
        """
        if key not in self._active_holds:
            return None
        
        timestamp = timestamp or time.time()
        event = self._active_holds.pop(key)
        event.end_time = timestamp
        event.hold_duration = event.duration
        return event
    
    def get_active_holds(self) -> List[KeyHoldEvent]:
        """Get list of currently active key holds."""
        return list(self._active_holds.values())
    
    def is_key_held(self, key: str) -> bool:
        """Check if a specific key is currently being held."""
        return key in self._active_holds
    
    def set_repeat_callback(
        self,
        callback: Callable[[str], None]
    ) -> None:
        """Set callback for key repeat events."""
        self._repeat_callback = callback


class ChordHoldManager:
    """
    Manages simultaneous key chord holds with staggered release timing.
    
    Supports keyboard shortcuts where multiple keys must be held
    together with configurable release ordering.
    """
    
    def __init__(self, config: Optional[KeyHoldConfig] = None) -> None:
        """
        Initialize chord hold manager.
        
        Args:
            config: Key hold configuration.
        """
        self._config = config or KeyHoldConfig()
        self._active_chords: Dict[str, Dict[str, KeyHoldEvent]] = {}
        self._chord_releases: Dict[str, List[Tuple[str, float]]] = {}
    
    def start_chord(
        self,
        chord_id: str,
        keys: List[str],
        timestamp: Optional[float] = None
    ) -> Dict[str, KeyHoldEvent]:
        """
        Start holding a chord (multiple keys simultaneously).
        
        Args:
            chord_id: Unique identifier for this chord instance.
            keys: List of keys to hold together.
            timestamp: Optional timestamp.
            
        Returns:
            Dictionary mapping keys to their hold events.
        """
        timestamp = timestamp or time.time()
        
        chord: Dict[str, KeyHoldEvent] = {}
        for key in keys:
            event = KeyHoldEvent(
                key=key,
                start_time=timestamp,
                hold_duration=self._config.initial_delay
            )
            chord[key] = event
        
        self._active_chords[chord_id] = chord
        return chord
    
    def end_chord(
        self,
        chord_id: str,
        release_order: Optional[List[str]] = None,
        timestamp: Optional[float] = None
    ) -> List[KeyHoldEvent]:
        """
        End a chord hold with optional staggered release.
        
        Args:
            chord_id: Identifier of the chord to end.
            release_order: Optional ordered list of keys for staggered release.
            timestamp: Optional timestamp.
            
        Returns:
            List of completed KeyHoldEvents in release order.
        """
        if chord_id not in self._active_chords:
            return []
        
        timestamp = timestamp or time.time()
        chord = self._active_chords.pop(chord_id)
        releases: List[KeyHoldEvent] = []
        
        keys_to_release = release_order or list(chord.keys())
        
        for i, key in enumerate(keys_to_release):
            if key in chord:
                event = chord[key]
                release_delay = i * self._config.release_delay
                event.end_time = timestamp + release_delay
                event.hold_duration = event.duration
                releases.append(event)
        
        self._chord_releases[chord_id] = [
            (e.key, e.end_time) for e in releases if e.end_time
        ]
        return releases
    
    def get_active_chords(self) -> Dict[str, List[str]]:
        """Get currently active chords as {chord_id: [keys]}."""
        return {
            cid: list(chord.keys())
            for cid, chord in self._active_chords.items()
        }


class KeyHoldPatternExecutor:
    """
    Executes predefined key hold patterns for complex input sequences.
    
    Supports patterns like key bursts (rapid taps), staggered holds,
    and timed chord transitions.
    """
    
    def __init__(self, config: Optional[KeyHoldConfig] = None) -> None:
        """
        Initialize pattern executor.
        
        Args:
            config: Key hold configuration.
        """
        self._config = config or KeyHoldConfig()
        self._simulator = KeyHoldSimulator(config)
        self._chord_manager = ChordHoldManager(config)
    
    def execute_burst(
        self,
        key: str,
        count: int,
        interval: float,
        initial_delay: Optional[float] = None
    ) -> List[KeyHoldEvent]:
        """
        Execute a burst of key taps.
        
        Args:
            key: Key to tap.
            count: Number of taps.
            interval: Time between taps in seconds.
            initial_delay: Optional initial delay before first tap.
            
        Returns:
            List of completed KeyHoldEvents.
        """
        events: List[KeyHoldEvent] = []
        delay = initial_delay or self._config.initial_delay
        
        for i in range(count):
            event = self._simulator.start_hold(key)
            event.hold_duration = delay
            events.append(event)
            time.sleep(delay)
            self._simulator.end_hold(key)
            if i < count - 1:
                time.sleep(interval)
        
        return events
    
    def execute_staggered_hold(
        self,
        keys: List[str],
        hold_duration: float,
        stagger_interval: float = 0.05
    ) -> List[KeyHoldEvent]:
        """
        Execute staggered key holds (keys press at different times).
        
        Args:
            keys: Keys to hold in order.
            hold_duration: Duration to hold each key.
            stagger_interval: Time between each key press.
            
        Returns:
            List of completed KeyHoldEvents.
        """
        events: List[KeyHoldEvent] = []
        for key in keys:
            event = self._simulator.start_hold(key)
            event.hold_duration = hold_duration
            events.append(event)
            time.sleep(stagger_interval)
        
        time.sleep(hold_duration)
        
        for event in events:
            self._simulator.end_hold(event.key)
        
        return events


def calculate_hold_timing(
    pattern: HoldPattern,
    duration: float,
    config: Optional[KeyHoldConfig] = None
) -> List[Tuple[str, float, float]]:
    """
    Calculate key hold timing for a given pattern.
    
    Args:
        pattern: Hold pattern type.
        duration: Total desired hold duration.
        config: Optional hold configuration.
        
    Returns:
        List of (key, press_time, release_time) tuples.
    """
    config = config or KeyHoldConfig()
    
    if pattern == HoldPattern.SINGLE:
        return [("key", 0, duration)]
    elif pattern == HoldPattern.BURST:
        burst_count = int(duration / 0.1)
        interval = duration / burst_count
        return [(f"key_{i}", i * interval, i * interval + 0.05)
                for i in range(burst_count)]
    else:
        return [("key", 0, duration)]


def validate_hold_sequence(
    sequence: List[Tuple[str, float, float]],
    min_duration: float = 0.05,
    max_overlap: float = 0.1
) -> bool:
    """
    Validate a key hold sequence for conflicts and overlaps.
    
    Args:
        sequence: List of (key, press_time, release_time) tuples.
        min_duration: Minimum valid hold duration.
        max_overlap: Maximum allowed overlap for same key.
        
    Returns:
        True if the sequence is valid.
    """
    key_releases: Dict[str, float] = {}
    
    for key, press, release in sequence:
        if release - press < min_duration:
            return False
        
        if key in key_releases:
            if press - key_releases[key] < max_overlap:
                return False
        
        key_releases[key] = release
    
    return True
