"""Video action module for RabAI AutoClick.

Provides video operations:
- VideoThumbnailAction: Extract thumbnail from video
- VideoDurationAction: Get video duration
- VideoInfoAction: Get video metadata
- VideoConvertAction: Convert video format
- VideoTrimAction: Trim video
- VideoExtractAudioAction: Extract audio from video
"""

import subprocess
import os
import json
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class VideoThumbnailAction(BaseAction):
    """Extract thumbnail from video."""
    action_type = "video_thumbnail"
    display_name = "提取视频缩略图"
    description = "从视频中提取缩略图"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute thumbnail extraction.

        Args:
            context: Execution context.
            params: Dict with video_path, output_path, timestamp.

        Returns:
            ActionResult with thumbnail path.
        """
        video_path = params.get('video_path', '')
        output_path = params.get('output_path', '')
        timestamp = params.get('timestamp', '00:00:01')

        valid, msg = self.validate_type(video_path, str, 'video_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_video = context.resolve_value(video_path)
            resolved_output = context.resolve_value(output_path) if output_path else ''
            resolved_ts = context.resolve_value(timestamp)

            if not os.path.exists(resolved_video):
                return ActionResult(
                    success=False,
                    message=f"视频文件不存在: {resolved_video}"
                )

            if not resolved_output:
                base = os.path.splitext(resolved_video)[0]
                resolved_output = f"{base}_thumb.jpg"

            # Use ffmpeg to extract thumbnail
            cmd = [
                'ffmpeg', '-y', '-ss', resolved_ts,
                '-i', resolved_video,
                '-vframes', '1',
                '-q:v', '2',
                resolved_output
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"提取缩略图失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"缩略图已保存: {resolved_output}",
                data={'thumbnail_path': resolved_output}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="提取缩略图超时"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="ffmpeg未安装，请先安装: brew install ffmpeg"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"提取缩略图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['video_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': '', 'timestamp': '00:00:01'}


class VideoDurationAction(BaseAction):
    """Get video duration."""
    action_type = "video_duration"
    display_name = "获取视频时长"
    description = "获取视频文件的时长"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute duration get.

        Args:
            context: Execution context.
            params: Dict with video_path, output_var.

        Returns:
            ActionResult with duration in seconds.
        """
        video_path = params.get('video_path', '')
        output_var = params.get('output_var', 'video_duration')

        valid, msg = self.validate_type(video_path, str, 'video_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_video = context.resolve_value(video_path)

            if not os.path.exists(resolved_video):
                return ActionResult(
                    success=False,
                    message=f"视频文件不存在: {resolved_video}"
                )

            # Use ffprobe to get duration
            cmd = [
                'ffprobe', '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'json',
                resolved_video
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"获取视频时长失败: {result.stderr}"
                )

            data = json.loads(result.stdout)
            duration = float(data['format']['duration'])

            context.set(output_var, duration)

            return ActionResult(
                success=True,
                message=f"视频时长: {duration:.2f}秒",
                data={'duration': duration, 'output_var': output_var}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="获取视频时长超时"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="ffprobe未安装，请先安装: brew install ffmpeg"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取视频时长失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['video_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'video_duration'}


class VideoInfoAction(BaseAction):
    """Get video metadata."""
    action_type = "video_info"
    display_name = "获取视频信息"
    description = "获取视频文件的详细信息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute info get.

        Args:
            context: Execution context.
            params: Dict with video_path, output_var.

        Returns:
            ActionResult with video metadata.
        """
        video_path = params.get('video_path', '')
        output_var = params.get('output_var', 'video_info')

        valid, msg = self.validate_type(video_path, str, 'video_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_video = context.resolve_value(video_path)

            if not os.path.exists(resolved_video):
                return ActionResult(
                    success=False,
                    message=f"视频文件不存在: {resolved_video}"
                )

            # Use ffprobe to get full info
            cmd = [
                'ffprobe', '-v', 'quiet',
                '-print_format', 'json',
                '-show_format', '-show_streams',
                resolved_video
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"获取视频信息失败: {result.stderr}"
                )

            data = json.loads(result.stdout)
            context.set(output_var, data)

            # Extract key info
            duration = float(data.get('format', {}).get('duration', 0))
            size = int(data.get('format', {}).get('size', 0))
            bitrate = int(data.get('format', {}).get('bit_rate', 0))

            video_stream = next(
                (s for s in data.get('streams', []) if s.get('codec_type') == 'video'),
                {}
            )
            width = video_stream.get('width', 0)
            height = video_stream.get('height', 0)
            codec = video_stream.get('codec_name', '')

            summary = f"{width}x{height}, {codec}, {duration:.1f}s, {size/1024/1024:.1f}MB"

            return ActionResult(
                success=True,
                message=f"视频信息: {summary}",
                data={
                    'duration': duration,
                    'size': size,
                    'bitrate': bitrate,
                    'width': width,
                    'height': height,
                    'codec': codec,
                    'output_var': output_var
                }
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="获取视频信息超时"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="ffprobe未安装，请先安装: brew install ffmpeg"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取视频信息失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['video_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'video_info'}


class VideoConvertAction(BaseAction):
    """Convert video format."""
    action_type = "video_convert"
    display_name = "转换视频格式"
    description = "转换视频文件格式"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute video conversion.

        Args:
            context: Execution context.
            params: Dict with video_path, output_path, codec.

        Returns:
            ActionResult with output path.
        """
        video_path = params.get('video_path', '')
        output_path = params.get('output_path', '')
        codec = params.get('codec', '')

        valid, msg = self.validate_type(video_path, str, 'video_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_path, str, 'output_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_video = context.resolve_value(video_path)
            resolved_output = context.resolve_value(output_path)

            if not os.path.exists(resolved_video):
                return ActionResult(
                    success=False,
                    message=f"视频文件不存在: {resolved_video}"
                )

            cmd = ['ffmpeg', '-y', '-i', resolved_video]

            if codec:
                resolved_codec = context.resolve_value(codec)
                cmd.extend(['-c:v', resolved_codec])

            cmd.append(resolved_output)

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"视频转换失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"视频已转换: {resolved_output}",
                data={'output_path': resolved_output}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="视频转换超时"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="ffmpeg未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"视频转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['video_path', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'codec': ''}


class VideoTrimAction(BaseAction):
    """Trim video."""
    action_type = "video_trim"
    display_name = "裁剪视频"
    description = "裁剪视频片段"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute video trim.

        Args:
            context: Execution context.
            params: Dict with video_path, output_path, start, end.

        Returns:
            ActionResult with output path.
        """
        video_path = params.get('video_path', '')
        output_path = params.get('output_path', '')
        start = params.get('start', '00:00:00')
        end = params.get('end', '00:00:10')

        valid, msg = self.validate_type(video_path, str, 'video_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_path, str, 'output_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_video = context.resolve_value(video_path)
            resolved_output = context.resolve_value(output_path)
            resolved_start = context.resolve_value(start)
            resolved_end = context.resolve_value(end)

            if not os.path.exists(resolved_video):
                return ActionResult(
                    success=False,
                    message=f"视频文件不存在: {resolved_video}"
                )

            cmd = [
                'ffmpeg', '-y',
                '-i', resolved_video,
                '-ss', resolved_start,
                '-to', resolved_end,
                '-c', 'copy',
                resolved_output
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"裁剪视频失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"视频已裁剪: {resolved_output}",
                data={'output_path': resolved_output}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="裁剪视频超时"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="ffmpeg未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"裁剪视频失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['video_path', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': '00:00:00', 'end': '00:00:10'}


class VideoExtractAudioAction(BaseAction):
    """Extract audio from video."""
    action_type = "video_extract_audio"
    display_name = "提取视频音频"
    description = "从视频中提取音频"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute audio extraction.

        Args:
            context: Execution context.
            params: Dict with video_path, output_path, audio_format.

        Returns:
            ActionResult with output path.
        """
        video_path = params.get('video_path', '')
        output_path = params.get('output_path', '')
        audio_format = params.get('audio_format', 'mp3')

        valid, msg = self.validate_type(video_path, str, 'video_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_path, str, 'output_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_video = context.resolve_value(video_path)
            resolved_output = context.resolve_value(output_path)
            resolved_fmt = context.resolve_value(audio_format)

            if not os.path.exists(resolved_video):
                return ActionResult(
                    success=False,
                    message=f"视频文件不存在: {resolved_video}"
                )

            cmd = [
                'ffmpeg', '-y',
                '-i', resolved_video,
                '-vn',
                '-acodec', 'libmp3lame' if resolved_fmt == 'mp3' else resolved_fmt,
                resolved_output
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300
            )

            if result.returncode != 0:
                return ActionResult(
                    success=False,
                    message=f"提取音频失败: {result.stderr}"
                )

            return ActionResult(
                success=True,
                message=f"音频已提取: {resolved_output}",
                data={'output_path': resolved_output}
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="提取音频超时"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="ffmpeg未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"提取音频失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['video_path', 'output_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'audio_format': 'mp3'}
