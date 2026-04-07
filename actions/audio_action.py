"""Audio action module for RabAI AutoClick.

Provides audio operations:
- AudioPlayAction: Play audio file
- AudioStopAction: Stop audio playback
- AudioVolumeAction: Adjust volume
"""

import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AudioPlayAction(BaseAction):
    """Play audio file."""
    action_type = "audio_play"
    display_name = "播放音频"
    description = "播放音频文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute play.

        Args:
            context: Execution context.
            params: Dict with file_path.

        Returns:
            ActionResult indicating success.
        """
        file_path = params.get('file_path', '')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import os
            resolved_path = context.resolve_value(file_path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"音频文件不存在: {resolved_path}"
                )

            # Use afplay on macOS
            subprocess.Popen(['afplay', resolved_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            return ActionResult(
                success=True,
                message=f"正在播放音频: {resolved_path}",
                data={'file_path': resolved_path}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"播放音频失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class AudioStopAction(BaseAction):
    """Stop audio playback."""
    action_type = "audio_stop"
    display_name = "停止音频"
    description = "停止音频播放"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute stop.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating success.
        """
        try:
            # Kill afplay process
            subprocess.run(['pkill', '-f', 'afplay'], capture_output=True)

            return ActionResult(
                success=True,
                message="音频已停止"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"停止音频失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class AudioVolumeAction(BaseAction):
    """Adjust volume."""
    action_type = "audio_volume"
    display_name = "调整音量"
    description = "调整系统音量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute volume adjust.

        Args:
            context: Execution context.
            params: Dict with volume.

        Returns:
            ActionResult indicating success.
        """
        volume = params.get('volume', 50)

        try:
            resolved_volume = context.resolve_value(volume)

            # Set volume using osascript (0-100 to 0-10)
            vol = min(100, max(0, int(resolved_volume)))
            vol = vol / 10.0

            script = f'''osascript -e 'set volume output volume {int(vol * 10)}' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"音量已调整为: {vol * 10}%",
                data={'volume': vol * 10}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"调整音量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['volume']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}