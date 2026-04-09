"""
Audio Synthesis Utilities for UI Automation.

This module provides utilities for generating audio feedback,
sound effects, and audio signals for UI testing and automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Tuple, Callable
from enum import Enum
import struct


class WaveformType(Enum):
    """Audio waveform types."""
    SINE = "sine"
    SQUARE = "square"
    SAWTOOTH = "sawtooth"
    TRIANGLE = "triangle"
    NOISE = "noise"


@dataclass
class AudioFrame:
    """Single audio frame."""
    left: float
    right: float

    def to_mono(self) -> float:
        return (self.left + self.right) / 2.0


@dataclass
class AudioBuffer:
    """Buffer containing audio frames."""
    frames: List[AudioFrame]
    sample_rate: int
    channels: int = 2

    def __len__(self) -> int:
        return len(self.frames)

    @property
    def duration(self) -> float:
        return len(self.frames) / self.sample_rate


@dataclass
class SoundConfig:
    """Configuration for sound synthesis."""
    frequency: float = 440.0
    duration: float = 1.0
    sample_rate: int = 44100
    amplitude: float = 0.5
    waveform: WaveformType = WaveformType.SINE
    fade_in: float = 0.01
    fade_out: float = 0.01


class WaveformGenerator:
    """
    Generate waveforms for audio synthesis.
    """

    def __init__(self, waveform: WaveformType = WaveformType.SINE):
        """
        Initialize waveform generator.

        Args:
            waveform: Type of waveform to generate
        """
        self.waveform = waveform

    def generate(
        self,
        frequency: float,
        duration: float,
        sample_rate: int = 44100,
        amplitude: float = 0.5
    ) -> List[float]:
        """
        Generate a single cycle of waveform.

        Args:
            frequency: Frequency in Hz
            duration: Duration in seconds
            sample_rate: Sample rate in Hz
            amplitude: Amplitude (0.0-1.0)

        Returns:
            List of sample values
        """
        num_samples = int(duration * sample_rate)
        angular_freq = 2.0 * math.pi * frequency
        samples = []

        for i in range(num_samples):
            t = i / sample_rate
            phase = angular_freq * t

            if self.waveform == WaveformType.SINE:
                value = math.sin(phase)
            elif self.waveform == WaveformType.SQUARE:
                value = 1.0 if math.sin(phase) >= 0 else -1.0
            elif self.waveform == WaveformType.SAWTOOTH:
                value = 2.0 * (frequency * t % 1.0) - 1.0
            elif self.waveform == WaveformType.TRIANGLE:
                value = 2.0 * abs(2.0 * (frequency * t % 1.0)) - 1.0
            elif self.waveform == WaveformType.NOISE:
                import random
                value = random.uniform(-1.0, 1.0)
            else:
                value = math.sin(phase)

            samples.append(value * amplitude)

        return samples


class ADSREnvelope:
    """
    ADSR (Attack, Decay, Sustain, Release) envelope for sound shaping.
    """

    def __init__(
        self,
        attack: float = 0.01,
        decay: float = 0.1,
        sustain: float = 0.7,
        release: float = 0.2
    ):
        """
        Initialize ADSR envelope.

        Args:
            attack: Attack time in seconds
            decay: Decay time in seconds
            sustain: Sustain level (0.0-1.0)
            release: Release time in seconds
        """
        self.attack = attack
        self.decay = decay
        self.sustain = sustain
        self.release = release

    def apply(self, samples: List[float], sample_rate: int = 44100) -> List[float]:
        """
        Apply envelope to samples.

        Args:
            samples: Input samples
            sample_rate: Sample rate in Hz

        Returns:
            Envelope-modified samples
        """
        num_samples = len(samples)
        attack_samples = int(self.attack * sample_rate)
        decay_samples = int(self.decay * sample_rate)
        release_samples = int(self.release * sample_rate)
        sustain_samples = num_samples - attack_samples - decay_samples - release_samples

        result = []

        for i in range(num_samples):
            if i < attack_samples:
                envelope = i / attack_samples
            elif i < attack_samples + decay_samples:
                t = (i - attack_samples) / decay_samples
                envelope = 1.0 - (1.0 - self.sustain) * t
            elif i < attack_samples + decay_samples + sustain_samples:
                envelope = self.sustain
            else:
                t = (i - attack_samples - decay_samples - sustain_samples) / release_samples
                envelope = self.sustain * (1.0 - t)
                if envelope < 0:
                    envelope = 0

            result.append(samples[i - attack_samples - decay_samples - sustain_samples] * envelope)

        return result


class AudioSynthesizer:
    """
    High-level audio synthesizer for UI feedback sounds.
    """

    def __init__(self, sample_rate: int = 44100):
        """
        Initialize audio synthesizer.

        Args:
            sample_rate: Audio sample rate in Hz
        """
        self.sample_rate = sample_rate
        self.waveform_gen = WaveformGenerator()
        self.envelope = ADSREnvelope()

    def generate_tone(
        self,
        config: SoundConfig
    ) -> AudioBuffer:
        """
        Generate a tone with given configuration.

        Args:
            config: Sound configuration

        Returns:
            AudioBuffer containing the tone
        """
        self.waveform_gen.waveform = config.waveform
        samples = self.waveform_gen.generate(
            frequency=config.frequency,
            duration=config.duration,
            sample_rate=config.sample_rate,
            amplitude=config.amplitude
        )

        samples = self.envelope.apply(samples, config.sample_rate)

        frames = [AudioFrame(s, s) for s in samples]

        return AudioBuffer(
            frames=frames,
            sample_rate=config.sample_rate,
            channels=2
        )

    def generate_beep(
        self,
        frequency: float = 440.0,
        duration: float = 0.2,
        amplitude: float = 0.5
    ) -> AudioBuffer:
        """
        Generate a simple beep sound.

        Args:
            frequency: Beep frequency in Hz
            duration: Beep duration in seconds
            amplitude: Volume level (0.0-1.0)

        Returns:
            AudioBuffer containing the beep
        """
        config = SoundConfig(
            frequency=frequency,
            duration=duration,
            amplitude=amplitude,
            waveform=WaveformType.SINE
        )
        return self.generate_tone(config)

    def generate_success_sound(self) -> AudioBuffer:
        """Generate a success notification sound."""
        config1 = SoundConfig(frequency=523.25, duration=0.1, amplitude=0.5)
        config2 = SoundConfig(frequency=659.25, duration=0.1, amplitude=0.5)
        config3 = SoundConfig(frequency=783.99, duration=0.15, amplitude=0.5)

        buf1 = self.generate_tone(config1)
        buf2 = self.generate_tone(config2)
        buf3 = self.generate_tone(config3)

        combined_frames = buf1.frames + buf2.frames + buf3.frames
        return AudioBuffer(frames=combined_frames, sample_rate=self.sample_rate)

    def generate_error_sound(self) -> AudioBuffer:
        """Generate an error notification sound."""
        config1 = SoundConfig(frequency=200.0, duration=0.15, amplitude=0.5)
        config2 = SoundConfig(frequency=150.0, duration=0.2, amplitude=0.5)

        buf1 = self.generate_tone(config1)
        buf2 = self.generate_tone(config2)

        combined_frames = buf1.frames + buf2.frames
        return AudioBuffer(frames=combined_frames, sample_rate=self.sample_rate)

    def generate_chirp(
        self,
        start_freq: float = 200.0,
        end_freq: float = 2000.0,
        duration: float = 0.5,
        amplitude: float = 0.3
    ) -> AudioBuffer:
        """
        Generate a frequency sweep (chirp) sound.

        Args:
            start_freq: Starting frequency in Hz
            end_freq: Ending frequency in Hz
            duration: Duration in seconds
            amplitude: Volume level

        Returns:
            AudioBuffer containing the chirp
        """
        num_samples = int(duration * self.sample_rate)
        frames = []

        for i in range(num_samples):
            t = i / self.sample_rate
            freq = start_freq + (end_freq - start_freq) * (t / duration)
            phase = 2.0 * math.pi * freq * t
            sample = math.sin(phase) * amplitude
            frames.append(AudioFrame(sample, sample))

        return AudioBuffer(frames=frames, sample_rate=self.sample_rate)


def mix_buffers(buffer1: AudioBuffer, buffer2: AudioBuffer, ratio: float = 0.5) -> AudioBuffer:
    """
    Mix two audio buffers.

    Args:
        buffer1: First audio buffer
        buffer2: Second audio buffer
        ratio: Mix ratio (0.0 = all buffer1, 1.0 = all buffer2)

    Returns:
        Mixed audio buffer
    """
    max_len = max(len(buffer1), len(buffer2))
    frames = []

    for i in range(max_len):
        f1 = buffer1.frames[i] if i < len(buffer1) else AudioFrame(0.0, 0.0)
        f2 = buffer2.frames[i] if i < len(buffer2) else AudioFrame(0.0, 0.0)

        left = f1.left * (1.0 - ratio) + f2.left * ratio
        right = f1.right * (1.0 - ratio) + f2.right * ratio
        frames.append(AudioFrame(left, right))

    return AudioBuffer(frames=frames, sample_rate=buffer1.sample_rate)


def generate_click_sound(sample_rate: int = 44100) -> AudioBuffer:
    """
    Generate a click sound effect.

    Args:
        sample_rate: Sample rate in Hz

    Returns:
        AudioBuffer containing the click
    """
    duration = 0.01
    num_samples = int(duration * sample_rate)
    frames = []

    for i in range(num_samples):
        t = i / num_samples
        amplitude = (1.0 - t) * 0.8
        import random
        sample = random.uniform(-1.0, 1.0) * amplitude
        frames.append(AudioFrame(sample, sample))

    return AudioBuffer(frames=frames, sample_rate=sample_rate)
