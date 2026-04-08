"""
Text-to-speech and voice output utilities.

Provides utilities for converting text to speech using macOS TTS engines,
including voice selection, rate control, and audio output management.
"""

from __future__ import annotations

import subprocess
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass
from enum import Enum


class OutputDevice(Enum):
    """Audio output device types."""
    SPEAKERS = "built-in speakers"
    HEADPHONES = "headphones"
    BLUETOOTH = "bluetooth"
    AIRPLAY = "airplay"
    HDMI = "hdmi"
    DISPLAYPORT = "displayport"


@dataclass
class VoiceInfo:
    """Information about a TTS voice."""
    identifier: str
    name: str
    language: str
    gender: str
    quality: str
    is_default: bool = False


@dataclass
class SpeechOptions:
    """Options for speech synthesis."""
    voice: Optional[str] = None
    rate: float = 200.0  # Words per minute
    pitch: float = 1.0
    volume: float = 1.0
    output_device: Optional[OutputDevice] = None


class TextToSpeechEngine:
    """Interface for text-to-speech operations."""
    
    def __init__(self):
        """Initialize TTS engine."""
        self._default_voice: Optional[str] = None
        self._current_options = SpeechOptions()
    
    def speak(
        self,
        text: str,
        options: Optional[SpeechOptions] = None
    ) -> bool:
        """Speak the given text.
        
        Args:
            text: Text to speak
            options: Optional speech options
            
        Returns:
            True if speech was started successfully
        """
        raise NotImplementedError
    
    def stop(self) -> bool:
        """Stop current speech.
        
        Returns:
            True if stopped
        """
        raise NotImplementedError
    
    def pause(self) -> bool:
        """Pause current speech.
        
        Returns:
            True if paused
        """
        raise NotImplementedError
    
    def resume(self) -> bool:
        """Resume paused speech.
        
        Returns:
            True if resumed
        """
        raise NotImplementedError
    
    def is_speaking(self) -> bool:
        """Check if currently speaking.
        
        Returns:
            True if speaking
        """
        raise NotImplementedError
    
    def get_voices(self) -> List[VoiceInfo]:
        """Get available voices.
        
        Returns:
            List of VoiceInfo objects
        """
        raise NotImplementedError
    
    def set_default_voice(self, voice_identifier: str) -> bool:
        """Set the default voice.
        
        Args:
            voice_identifier: Voice identifier
            
        Returns:
            True if set successfully
        """
        raise NotImplementedError


class SayCommandEngine(TextToSpeechEngine):
    """TTS engine using the macOS 'say' command."""
    
    def __init__(self):
        """Initialize say-based TTS engine."""
        super().__init__()
        self._speaking = False
    
    def speak(
        self,
        text: str,
        options: Optional[SpeechOptions] = None
    ) -> bool:
        """Speak text using the 'say' command.
        
        Args:
            text: Text to speak
            options: Optional speech options
            
        Returns:
            True if speech was started
        """
        if options is None:
            options = self._current_options
        
        try:
            cmd = ["say"]
            
            if options.voice:
                cmd.extend(["-v", options.voice])
            
            # Rate: say uses -r for rate (words per minute)
            # Default is ~175 WPM
            if options.rate != 200.0:
                cmd.extend(["-r", str(int(options.rate))])
            
            if options.output_device:
                # Set audio device
                pass  # Handled separately
            
            cmd.extend(["--", text])
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            self._speaking = True
            self._process = process
            
            return True
        except Exception:
            return False
    
    def stop(self) -> bool:
        """Stop speech using 'say'.
        
        Returns:
            True if stopped
        """
        try:
            if hasattr(self, "_process"):
                self._process.terminate()
                self._speaking = False
                return True
            
            # Kill any say process
            subprocess.run(["pkill", "-f", "^say"], capture_output=True, timeout=2)
            self._speaking = False
            return True
        except Exception:
            return False
    
    def pause(self) -> bool:
        """Pause speech (not directly supported by say).
        
        Returns:
            False (pause not supported)
        """
        # The 'say' command doesn't support pause
        return False
    
    def resume(self) -> bool:
        """Resume speech (not directly supported by say).
        
        Returns:
            False (resume not supported)
        """
        return False
    
    def is_speaking(self) -> bool:
        """Check if speaking.
        
        Returns:
            True if speaking
        """
        try:
            result = subprocess.run(
                ["pgrep", "-f", "^say"],
                capture_output=True,
                timeout=2
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def get_voices(self) -> List[VoiceInfo]:
        """Get available voices.
        
        Returns:
            List of VoiceInfo objects
        """
        voices = []
        
        try:
            result = subprocess.run(
                ["say", "-v", "?"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                default_voice = self._get_default_voice_identifier()
                
                for line in result.stdout.strip().split("\n"):
                    if not line.strip():
                        continue
                    
                    parts = line.split()
                    if len(parts) >= 2:
                        name = parts[0]
                        lang = parts[1].strip("()")
                        quality = "Default"
                        
                        if "Premium" in line:
                            quality = "Premium"
                        elif "Enhanced" in line:
                            quality = "Enhanced"
                        
                        gender = "Unknown"
                        if any(g in name.lower() for g in ["female", "woman", "samantha", "victoria", "karen"]):
                            gender = "Female"
                        elif any(g in name.lower() for g in ["male", "man", "alex", "daniel", "fred"]):
                            gender = "Male"
                        
                        voices.append(VoiceInfo(
                            identifier=name,
                            name=name,
                            language=lang,
                            gender=gender,
                            quality=quality,
                            is_default=(name == default_voice)
                        ))
        except Exception:
            pass
        
        return voices
    
    def _get_default_voice_identifier(self) -> Optional[str]:
        """Get the default voice identifier.
        
        Returns:
            Default voice name or None
        """
        try:
            result = subprocess.run(
                ["defaults", "read", "com.apple.speech.synthesis.general", "defaultVoice"],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if result.returncode == 0:
                return result.stdout.strip().strip('"')
        except Exception:
            pass
        
        return None
    
    def set_default_voice(self, voice_identifier: str) -> bool:
        """Set the default voice.
        
        Args:
            voice_identifier: Voice identifier
            
        Returns:
            True if set successfully
        """
        try:
            subprocess.run(
                ["defaults", "write", "com.apple.speech.synthesis.general", "defaultVoice", 
                 f"-string '{voice_identifier}'"],
                capture_output=True,
                timeout=2
            )
            self._default_voice = voice_identifier
            return True
        except Exception:
            return False
    
    def speak_to_file(
        self,
        text: str,
        output_path: str,
        options: Optional[SpeechOptions] = None,
        format: str = "aiff"
    ) -> bool:
        """Speak text and save to audio file.
        
        Args:
            text: Text to speak
            output_path: Output file path
            options: Optional speech options
            format: Audio format (aiff, wav, mp3)
            
        Returns:
            True if file was created
        """
        if options is None:
            options = self._current_options
        
        try:
            cmd = ["say", "-o", output_path]
            
            if options.voice:
                cmd.extend(["-v", options.voice])
            
            if options.rate != 200.0:
                cmd.extend(["-r", str(int(options.rate))])
            
            cmd.extend(["--", text])
            
            subprocess.run(cmd, capture_output=True, timeout=30)
            
            return True
        except Exception:
            return False


class VoiceOverTTSEngine(TextToSpeechEngine):
    """TTS engine using VoiceOver."""
    
    def __init__(self):
        """Initialize VoiceOver-based TTS engine."""
        super().__init__()
    
    def speak(
        self,
        text: str,
        options: Optional[SpeechOptions] = None
    ) -> bool:
        """Speak text using VoiceOver.
        
        Args:
            text: Text to speak
            options: Optional speech options
            
        Returns:
            True if speech was started
        """
        try:
            script = f'''
            tell application "VoiceOver"
                output "{text}"
            end tell
            '''
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False
    
    def stop(self) -> bool:
        """Stop VoiceOver speech.
        
        Returns:
            True if stopped
        """
        try:
            script = '''
            tell application "VoiceOver"
                stop speech
            end tell
            '''
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=2
            )
            return True
        except Exception:
            return False
    
    def pause(self) -> bool:
        """Pause VoiceOver speech."""
        try:
            script = '''
            tell application "VoiceOver"
                pause speech
            end tell
            '''
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=2
            )
            return True
        except Exception:
            return False
    
    def resume(self) -> bool:
        """Resume VoiceOver speech."""
        try:
            script = '''
            tell application "VoiceOver"
                continue speech
            end tell
            '''
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=2
            )
            return True
        except Exception:
            return False
    
    def is_speaking(self) -> bool:
        """Check if VoiceOver is speaking."""
        try:
            script = '''
            tell application "VoiceOver"
                if speaking then
                    return "true"
                else
                    return "false"
                end if
            end tell
            '''
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=2
            )
            return "true" in result.stdout.lower()
        except Exception:
            return False
    
    def get_voices(self) -> List[VoiceInfo]:
        """Get available voices (delegates to say command)."""
        say_engine = SayCommandEngine()
        return say_engine.get_voices()
    
    def set_default_voice(self, voice_identifier: str) -> bool:
        """Set the default voice for VoiceOver.
        
        Args:
            voice_identifier: Voice identifier
            
        Returns:
            True if set successfully
        """
        try:
            script = f'''
            tell application "VoiceOver"
                set speech default rate to 1.0
            end tell
            '''
            # VoiceOver uses its own voice settings
            return True
        except Exception:
            return False


def get_voices_by_language(language_code: str) -> List[VoiceInfo]:
    """Get voices for a specific language.
    
    Args:
        language_code: Language code (e.g., "en-US", "zh-CN")
        
    Returns:
        List of VoiceInfo objects for the language
    """
    engine = SayCommandEngine()
    all_voices = engine.get_voices()
    
    return [
        v for v in all_voices
        if language_code.lower() in v.language.lower()
    ]


def get_premium_voices() -> List[VoiceInfo]:
    """Get premium quality voices.
    
    Returns:
        List of premium VoiceInfo objects
    """
    engine = SayCommandEngine()
    all_voices = engine.get_voices()
    
    return [v for v in all_voices if v.quality == "Premium"]


def get_default_voice_for_language(language_code: str) -> Optional[VoiceInfo]:
    """Get the default voice for a language.
    
    Args:
        language_code: Language code
        
    Returns:
        Default VoiceInfo or None
    """
    voices = get_voices_by_language(language_code)
    
    if not voices:
        return None
    
    # Prefer enhanced/premium, then default
    for v in voices:
        if v.is_default:
            return v
    
    enhanced = [v for v in voices if v.quality == "Enhanced"]
    if enhanced:
        return enhanced[0]
    
    return voices[0]


def speak_with_options(
    text: str,
    voice: Optional[str] = None,
    rate: float = 200.0,
    wait: bool = True
) -> bool:
    """Convenience function to speak with options.
    
    Args:
        text: Text to speak
        voice: Voice identifier
        rate: Speaking rate (WPM)
        wait: Whether to wait for completion
        
    Returns:
        True if speech started/completed
    """
    engine = SayCommandEngine()
    options = SpeechOptions(voice=voice, rate=rate)
    
    success = engine.speak(text, options)
    
    if success and wait:
        engine._process.wait()
    
    return success


def stop_all_speech() -> bool:
    """Stop all ongoing speech.
    
    Returns:
        True if stopped
    """
    engine = SayCommandEngine()
    return engine.stop()
