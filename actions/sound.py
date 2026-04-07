"""Sound action module for RabAI AutoClick.

Provides sound/audio actions:
- PlaySoundAction: Play a sound file
- BeepAction: Play a beep sound
- SystemSoundAction: Play macOS system sounds
"""

import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


# Valid system sounds on macOS
SYSTEM_SOUNDS: Dict[str, str] = {
    'basso': '/System/Library/Sounds/Basso.aiff',
    'blow': '/System/Library/Sounds/Blow.aiff',
    'boo': '/System/Library/Sounds/Boo.aiff',
    'frog': '/System/Library/Sounds/Frog.aiff',
    'glass': '/System/Library/Sounds/Glass.aiff',
    'morse': '/System/Library/Sounds/Morse.aiff',
    'pop': '/System/Library/Sounds/Pop.aiff',
    'sos': '/System/Library/Sounds/Sos.aiff',
    'submarine': '/System/Library/Sounds/Submarine.aiff',
    'tink': '/System/Library/Sounds/Tink.aiff',
}


class PlaySoundAction(BaseAction):
    """Play a sound file."""
    action_type = "play_sound"
    display_name = "播放声音"
    description = "播放指定的声音文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute playing a sound file.

        Args:
            context: Execution context.
            params: Dict with path, volume (0.0-1.0).

        Returns:
            ActionResult indicating success.
        """
        path = params.get('path', '')
        volume = params.get('volume', 1.0)

        # Validate path
        if not path:
            return ActionResult(
                success=False,
                message="未指定声音文件路径"
            )
        valid, msg = self.validate_type(path, str, 'path')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate volume
        valid, msg = self.validate_type(volume, (int, float), 'volume')
        if not valid:
            return ActionResult(success=False, message=msg)
        valid, msg = self.validate_range(volume, 0.0, 1.0, 'volume')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            # Check if file exists
            if not Path(path).exists():
                return ActionResult(
                    success=False,
                    message=f"声音文件不存在: {path}"
                )

            # Play using afplay on macOS
            cmd = ['afplay', '-volume', str(volume), path]
            subprocess.run(cmd, capture_output=True, timeout=30, check=True)

            return ActionResult(
                success=True,
                message=f"播放声音: {path}",
                data={'path': path, 'volume': volume}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="播放声音超时"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"播放声音失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'volume': 1.0}


class BeepAction(BaseAction):
    """Play a beep sound."""
    action_type = "beep"
    display_name = "蜂鸣声"
    description = "播放蜂鸣提示音"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute playing a beep.

        Args:
            context: Execution context.
            params: Dict with frequency (Hz), duration (ms), count.

        Returns:
            ActionResult indicating success.
        """
        frequency = params.get('frequency', 440)  # A4 note by default
        duration = params.get('duration', 200)  # 200ms
        count = params.get('count', 1)

        # Validate frequency
        valid, msg = self.validate_type(frequency, (int, float), 'frequency')
        if not valid:
            return ActionResult(success=False, message=msg)
        if frequency <= 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'frequency' must be > 0, got {frequency}"
            )

        # Validate duration
        valid, msg = self.validate_type(duration, (int, float), 'duration')
        if not valid:
            return ActionResult(success=False, message=msg)
        if duration <= 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'duration' must be > 0, got {duration}"
            )

        # Validate count
        valid, msg = self.validate_type(count, int, 'count')
        if not valid:
            return ActionResult(success=False, message=msg)
        if count < 1:
            return ActionResult(
                success=False,
                message=f"Parameter 'count' must be >= 1, got {count}"
            )

        try:
            import math

            # Generate beep using macOS say command as fallback
            for i in range(count):
                # Use a simple beep using system sounds
                subprocess.run(
                    ['osascript', '-e', 'beep'],
                    capture_output=True,
                    timeout=5
                )
                if i < count - 1:
                    import time
                    time.sleep(0.1)

            return ActionResult(
                success=True,
                message=f"蜂鸣完成: {count}次",
                data={'frequency': frequency, 'duration': duration, 'count': count}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"蜂鸣失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'frequency': 440,
            'duration': 200,
            'count': 1
        }


class SystemSoundAction(BaseAction):
    """Play a macOS system sound."""
    action_type = "system_sound"
    display_name = "系统声音"
    description = "播放macOS系统声音"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute playing a system sound.

        Args:
            context: Execution context.
            params: Dict with sound_name.

        Returns:
            ActionResult indicating success.
        """
        sound_name = params.get('sound_name', 'glass')

        # Validate sound_name
        valid, msg = self.validate_in(
            sound_name, list(SYSTEM_SOUNDS.keys()), 'sound_name'
        )
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            sound_path = SYSTEM_SOUNDS.get(sound_name)

            if sound_path and Path(sound_path).exists():
                subprocess.run(
                    ['afplay', sound_path],
                    capture_output=True,
                    timeout=10
                )
            else:
                # Fallback to osascript
                subprocess.run(
                    ['osascript', '-e', f'play sound "{sound_name}"'],
                    capture_output=True,
                    timeout=10
                )

            return ActionResult(
                success=True,
                message=f"系统声音: {sound_name}",
                data={'sound_name': sound_name}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"播放系统声音失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'sound_name': 'glass'
        }

    def get_available_sounds(self) -> List[str]:
        """Get list of available system sound names.

        Returns:
            List of available system sound names.
        """
        return list(SYSTEM_SOUNDS.keys())