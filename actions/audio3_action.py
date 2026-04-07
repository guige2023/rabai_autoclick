"""Audio3 action module for RabAI AutoClick.

Provides additional audio operations:
- AudioTrimAction: Trim audio
- AudioVolumeAction: Adjust volume
- AudioMergeAction: Merge audio files
- AudioSplitAction: Split audio
- AudioReverseAction: Reverse audio
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AudioTrimAction(BaseAction):
    """Trim audio."""
    action_type = "audio3_trim"
    display_name = "音频裁剪"
    description = "裁剪音频片段"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute audio trim.

        Args:
            context: Execution context.
            params: Dict with file_path, start_time, end_time, output_var.

        Returns:
            ActionResult with trimmed audio path.
        """
        file_path = params.get('file_path', '')
        start_time = params.get('start_time', 0)
        end_time = params.get('end_time', 10)
        output_var = params.get('output_var', 'trimmed_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_start = float(context.resolve_value(start_time))
            resolved_end = float(context.resolve_value(end_time))

            try:
                from pydub import AudioSegment
                audio = AudioSegment.from_file(resolved_path)
                trimmed = audio[resolved_start * 1000:resolved_end * 1000]

                output_path = resolved_path.replace('.', '_trimmed.')
                trimmed.export(output_path, format=output_path.split('.')[-1])

            except ImportError:
                return ActionResult(
                    success=False,
                    message="音频裁剪失败: 未安装pydub库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"音频裁剪完成: {output_path}",
                data={
                    'original': resolved_path,
                    'trimmed': output_path,
                    'start_time': resolved_start,
                    'end_time': resolved_end,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"音频裁剪失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'start_time', 'end_time']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'trimmed_path'}


class AudioVolumeAction(BaseAction):
    """Adjust volume."""
    action_type = "audio3_volume"
    display_name = "音频音量"
    description = "调整音频音量"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute volume adjust.

        Args:
            context: Execution context.
            params: Dict with file_path, db_change, output_var.

        Returns:
            ActionResult with adjusted audio path.
        """
        file_path = params.get('file_path', '')
        db_change = params.get('db_change', 0)
        output_var = params.get('output_var', 'adjusted_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_db = float(context.resolve_value(db_change))

            try:
                from pydub import AudioSegment
                audio = AudioSegment.from_file(resolved_path)
                adjusted = audio + resolved_db

                output_path = resolved_path.replace('.', '_adjusted.')
                adjusted.export(output_path, format=output_path.split('.')[-1])

            except ImportError:
                return ActionResult(
                    success=False,
                    message="音频音量调整失败: 未安装pydub库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"音频音量调整完成: {output_path}",
                data={
                    'original': resolved_path,
                    'adjusted': output_path,
                    'db_change': resolved_db,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"音频音量调整失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'db_change': 0, 'output_var': 'adjusted_path'}


class AudioMergeAction(BaseAction):
    """Merge audio files."""
    action_type = "audio3_merge"
    display_name = "音频合并"
    description = "合并多个音频文件"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute audio merge.

        Args:
            context: Execution context.
            params: Dict with file_paths, output_var.

        Returns:
            ActionResult with merged audio path.
        """
        file_paths = params.get('file_paths', [])
        output_var = params.get('output_var', 'merged_path')

        valid, msg = self.validate_type(file_paths, (list, tuple), 'file_paths')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_paths = context.resolve_value(file_paths)

            if not resolved_paths or len(resolved_paths) < 2:
                return ActionResult(
                    success=False,
                    message="音频合并失败: 需要至少2个文件"
                )

            try:
                from pydub import AudioSegment
                merged = AudioSegment.empty()

                for path in resolved_paths:
                    audio = AudioSegment.from_file(path)
                    merged += audio

                output_path = resolved_paths[0].replace('.', '_merged.')
                merged.export(output_path, format=output_path.split('.')[-1])

            except ImportError:
                return ActionResult(
                    success=False,
                    message="音频合并失败: 未安装pydub库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"音频合并完成: {output_path}",
                data={
                    'files': resolved_paths,
                    'merged': output_path,
                    'duration': len(merged) / 1000,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"音频合并失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_paths']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'merged_path'}


class AudioSplitAction(BaseAction):
    """Split audio."""
    action_type = "audio3_split"
    display_name = "音频分割"
    description = "分割音频文件"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute audio split.

        Args:
            context: Execution context.
            params: Dict with file_path, split_points, output_var.

        Returns:
            ActionResult with split audio paths.
        """
        file_path = params.get('file_path', '')
        split_points = params.get('split_points', [])
        output_var = params.get('output_var', 'split_paths')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)
            resolved_points = context.resolve_value(split_points)

            if not resolved_points:
                return ActionResult(
                    success=False,
                    message="音频分割失败: 需要指定分割点"
                )

            try:
                from pydub import AudioSegment
                audio = AudioSegment.from_file(resolved_path)

                output_paths = []
                start = 0

                for i, point in enumerate(resolved_points):
                    end = point * 1000
                    segment = audio[start:end]
                    output_path = resolved_path.replace('.', f'_part{i}.')
                    segment.export(output_path, format=output_path.split('.')[-1])
                    output_paths.append(output_path)
                    start = end

                if start < len(audio):
                    segment = audio[start:]
                    output_path = resolved_path.replace('.', f'_part{len(resolved_points)}.')
                    segment.export(output_path, format=output_path.split('.')[-1])
                    output_paths.append(output_path)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="音频分割失败: 未安装pydub库"
                )

            context.set(output_var, output_paths)

            return ActionResult(
                success=True,
                message=f"音频分割完成: {len(output_paths)} 个文件",
                data={
                    'original': resolved_path,
                    'split_paths': output_paths,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"音频分割失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path', 'split_points']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'split_paths'}


class AudioReverseAction(BaseAction):
    """Reverse audio."""
    action_type = "audio3_reverse"
    display_name = "音频反转"
    description = "反转音频"
    version = "3.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute audio reverse.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with reversed audio path.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'reversed_path')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            try:
                from pydub import AudioSegment
                audio = AudioSegment.from_file(resolved_path)
                reversed_audio = audio.reverse()

                output_path = resolved_path.replace('.', '_reversed.')
                reversed_audio.export(output_path, format=output_path.split('.')[-1])

            except ImportError:
                return ActionResult(
                    success=False,
                    message="音频反转失败: 未安装pydub库"
                )

            context.set(output_var, output_path)

            return ActionResult(
                success=True,
                message=f"音频反转完成: {output_path}",
                data={
                    'original': resolved_path,
                    'reversed': output_path,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"音频反转失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reversed_path'}