"""System sound utilities for audio feedback during automation.

This module provides utilities for playing system sounds, beeps,
and custom audio cues during automation tasks.
"""

from __future__ import annotations

import platform
import subprocess
from typing import Optional


IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"


# Standard system sound names
SOUND_BEEP = "beep"
SOUND_NOTIFICATION = "notification"
SOUND_SUCCESS = "success"
SOUND_ERROR = "error"
SOUND_WARNING = "warning"
SOUND_CLICK = "click"
SOUND_TICK = "tick"
SOUND_COMPLETE = "complete"


def _run_command(cmd: list[str]) -> bool:
    """Run a shell command and return success status.
    
    Args:
        cmd: Command and arguments as list.
    
    Returns:
        True if command succeeded, False otherwise.
    """
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=5)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def play_beep(
    frequency: int = 800,
    duration: float = 0.2,
    volume: float = 0.5,
) -> bool:
    """Play a beep tone through the system speaker.
    
    Args:
        frequency: Frequency in Hz (20-20000).
        duration: Duration in seconds.
        volume: Volume from 0.0 to 1.0.
    
    Returns:
        True if beep was played successfully.
    """
    if IS_MACOS:
        try:
            import subprocess
            # Use AppleScript to play beep
            script = f'set volume output volume {int(volume * 100)}'
            subprocess.run(["osascript", "-e", script], capture_output=True)
            # Use say as a fallback beep
            return subprocess.run(
                ["say", "-v", "Boing", "beep"],
                capture_output=True,
                timeout=2
            ).returncode == 0
        except Exception:
            return False
    
    elif IS_LINUX:
        try:
            subprocess.run(
                ["beep", "-f", str(frequency), "-l", str(int(duration * 1000))],
                capture_output=True,
                timeout=1
            )
            return True
        except FileNotFoundError:
            return False
    
    elif IS_WINDOWS:
        try:
            import winsound
            winsound.Beep(frequency, int(duration * 1000))
            return True
        except Exception:
            return False
    
    return False


def play_system_sound(sound_name: str) -> bool:
    """Play a named system sound.
    
    Args:
        sound_name: Name of the system sound to play.
    
    Returns:
        True if sound was played successfully.
    """
    if IS_MACOS:
        # Map sound names to macOS sound files
        sound_map = {
            SOUND_BEEP: "Basso",
            SOUND_NOTIFICATION: "Pop",
            SOUND_SUCCESS: "Glass",
            SOUND_ERROR: "Basso",
            SOUND_WARNING: "Funk",
            SOUND_CLICK: "Tink",
            SOUND_TICK: "Pop",
            SOUND_COMPLETE: "Glass",
        }
        sound_file = sound_map.get(sound_name, "Pop")
        script = f'if running of application "Finder" then beep "{sound_file}"'
        return _run_command(["osascript", "-e", script])
    
    elif IS_LINUX:
        # Try paplay for PulseAudio or aplay for ALSA
        sounds_dir = "/usr/share/sounds"
        if sound_name == SOUND_SUCCESS:
            return _run_command(["paplay", f"{sounds_dir}/freedesktop/stereo/complete.oga"])
        elif sound_name == SOUND_ERROR:
            return _run_command(["paplay", f"{sounds_dir}/freedesktop/stereo/dialog-error.oga"])
        return _run_command(["paplay", f"{sounds_dir}/freedesktop/stereo/message.oga"])
    
    elif IS_WINDOWS:
        try:
            import winsound
            if sound_name == SOUND_SUCCESS:
                winsound.MessageBeep(winsound.MB_ICONASTERISK)
            elif sound_name == SOUND_ERROR:
                winsound.MessageBeep(winsound.MB_ICONHAND)
            else:
                winsound.MessageBeep(winsound.MB_OK)
            return True
        except Exception:
            return False
    
    return False


def play_tone(
    frequency: int = 440,
    duration: float = 0.3,
    volume: float = 0.3,
    waveform: str = "sine",
) -> bool:
    """Play a tone using the system audio output.
    
    Args:
        frequency: Frequency in Hz.
        duration: Duration in seconds.
        volume: Volume from 0.0 to 1.0.
        waveform: Waveform type ('sine', 'square', 'sawtooth', 'triangle').
    
    Returns:
        True if tone was played successfully.
    """
    try:
        import numpy as np
        sample_rate = 44100
        
        # Generate waveform samples
        t = np.linspace(0, duration, int(sample_rate * duration), False)
        
        if waveform == "sine":
            samples = np.sin(2 * np.pi * frequency * t)
        elif waveform == "square":
            samples = np.sign(np.sin(2 * np.pi * frequency * t))
        elif waveform == "sawtooth":
            samples = 2 * (t * frequency % 1) - 1
        elif waveform == "triangle":
            samples = np.abs(2 * (t * frequency % 1) - 1) * 2 - 1
        else:
            samples = np.sin(2 * np.pi * frequency * t)
        
        # Apply volume
        samples = (samples * volume * 32767).astype(np.int16)
        
        # Create stereo
        stereo = np.column_stack((samples, samples))
        
        # Write WAV
        import wave
        with wave.open("/tmp/_tone.wav", "w") as f:
            f.setnchannels(2)
            f.setsampwidth(2)
            f.setframerate(sample_rate)
            f.writeframes(stereo.tobytes())
        
        return _play_audio_file("/tmp/_tone.wav")
    
    except ImportError:
        # Fallback to simple beep
        return play_beep(frequency, duration, volume)


def _play_audio_file(filepath: str) -> bool:
    """Play an audio file through the system audio output.
    
    Args:
        filepath: Path to the audio file.
    
    Returns:
        True if file was played successfully.
    """
    if IS_MACOS:
        return _run_command(["afplay", filepath])
    elif IS_LINUX:
        return _run_command(["paplay", filepath]) or _run_command(["aplay", filepath])
    elif IS_WINDOWS:
        try:
            import winsound
            winsound.PlaySound(filepath, winsound.SND_FILENAME)
            return True
        except Exception:
            return False
    return False


def play_file(filepath: str) -> bool:
    """Play an audio file.
    
    Args:
        filepath: Path to the audio file.
    
    Returns:
        True if file was played successfully.
    """
    return _play_audio_file(filepath)


def set_volume(level: float) -> bool:
    """Set the system volume level.
    
    Args:
        level: Volume level from 0.0 to 1.0.
    
    Returns:
        True if volume was set successfully.
    """
    level = max(0.0, min(1.0, level))
    
    if IS_MACOS:
        script = f"set volume output volume {int(level * 100)}"
        return _run_command(["osascript", "-e", script])
    elif IS_LINUX:
        try:
            subprocess.run(
                ["pactl", "set-sink-volume", "@DEFAULT_SINK@", f"{int(level * 100)}%"],
                capture_output=True,
                timeout=2
            )
            return True
        except Exception:
            return False
    elif IS_WINDOWS:
        try:
            from ctypes import cast, POINTER, c_float
            from comtypes import CLSCTX_ALL
            from pycaw.pycaw import AudioUtilities, IAudioEndpointVolume
            devices = AudioUtilities.GetSpeakers()
            interface = devices.Activate(IAudioEndpointVolume._iid_, CLSCTX_ALL, None)
            volume = cast(interface, POINTER(IAudioEndpointVolume))
            volume.SetMasterScalarVolume(level, None)
            return True
        except Exception:
            return False
    return False
