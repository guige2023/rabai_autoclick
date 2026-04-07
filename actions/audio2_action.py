"""Audio2 action module for RabAI AutoClick.

Provides additional audio operations:
- AudioPlayAction: Play audio file
- AudioStopAction: Stop audio playback
- AudioVolumeAction: Get/Set volume
- AudioDurationAction: Get audio duration
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AudioPlayAction(BaseAction):
    """Play audio file."""
    action_type = "audio2_play"
    display_name = "播放音频"
    description = "播放音频文件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute play audio.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with play result.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'audio_result')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import pygame

            resolved_path = context.resolve_value(file_path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            pygame.mixer.init()
            pygame.mixer.music.load(resolved_path)
            pygame.mixer.music.play()

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"播放音频: {resolved_path}",
                data={
                    'file_path': resolved_path,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 pygame 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"播放音频失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'audio_result'}


class AudioStopAction(BaseAction):
    """Stop audio playback."""
    action_type = "audio2_stop"
    display_name = "停止音频"
    description = "停止音频播放"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute stop audio.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with stop result.
        """
        output_var = params.get('output_var', 'audio_result')

        try:
            import pygame

            pygame.mixer.music.stop()
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message="音频已停止",
                data={
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 pygame 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"停止音频失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'audio_result'}


class AudioVolumeAction(BaseAction):
    """Get/Set volume."""
    action_type = "audio2_volume"
    display_name = "音频音量"
    description = "获取或设置音量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute volume.

        Args:
            context: Execution context.
            params: Dict with volume, output_var.

        Returns:
            ActionResult with volume result.
        """
        volume = params.get('volume', None)
        output_var = params.get('output_var', 'volume_result')

        try:
            import pygame

            if volume is None:
                current = pygame.mixer.music.get_volume()
                context.set(output_var, current)

                return ActionResult(
                    success=True,
                    message=f"当前音量: {int(current * 100)}%",
                    data={
                        'volume': current,
                        'output_var': output_var
                    }
                )
            else:
                resolved_volume = float(context.resolve_value(volume))
                if not (0 <= resolved_volume <= 1):
                    return ActionResult(
                        success=False,
                        message="音量必须在0-1之间"
                    )

                pygame.mixer.music.set_volume(resolved_volume)
                context.set(output_var, resolved_volume)

                return ActionResult(
                    success=True,
                    message=f"音量已设置为: {int(resolved_volume * 100)}%",
                    data={
                        'volume': resolved_volume,
                        'output_var': output_var
                    }
                )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 pygame 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"音频音量失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'volume': None, 'output_var': 'volume_result'}


class AudioDurationAction(BaseAction):
    """Get audio duration."""
    action_type = "audio2_duration"
    display_name = "音频时长"
    description = "获取音频文件时长"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute duration.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with duration.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'audio_duration')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from pydub import AudioSegment

            resolved_path = context.resolve_value(file_path)

            if not os.path.exists(resolved_path):
                return ActionResult(
                    success=False,
                    message=f"文件不存在: {resolved_path}"
                )

            audio = AudioSegment.from_file(resolved_path)
            duration_seconds = len(audio) / 1000.0

            context.set(output_var, duration_seconds)

            return ActionResult(
                success=True,
                message=f"音频时长: {duration_seconds:.2f} 秒",
                data={
                    'file_path': resolved_path,
                    'duration': duration_seconds,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 pydub 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取音频时长失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'audio_duration'}