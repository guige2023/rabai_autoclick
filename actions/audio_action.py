"""Audio action module for RabAI AutoClick.

Provides audio playback and recording actions.
"""

import subprocess
import sys
import os
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AudioPlayAction(BaseAction):
    """Play audio file.
    
    Uses afplay for audio playback.
    """
    action_type = "audio_play"
    display_name = "播放音频"
    description = "播放音频文件"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Play audio.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: file_path, volume, wait.
        
        Returns:
            ActionResult with play status.
        """
        file_path = params.get('file_path', '')
        volume = params.get('volume', 1.0)
        wait = params.get('wait', True)
        
        if not file_path:
            return ActionResult(success=False, message="file_path required")
        
        if not os.path.exists(file_path):
            return ActionResult(success=False, message=f"File not found: {file_path}")
        
        if not 0 <= volume <= 1:
            return ActionResult(success=False, message="volume must be 0-1")
        
        try:
            cmd = ['afplay', '-v', str(volume)]
            if not wait:
                cmd.append('&')
            cmd.append(file_path)
            
            result = subprocess.run(cmd, capture_output=True, timeout=300 if wait else 5)
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"Played: {file_path}",
                    data={'file_path': file_path, 'volume': volume}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Play failed: {result.stderr.decode()}"
                )
                
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Play timed out")
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Play error: {e}",
                data={'error': str(e)}
            )


class AudioVolumeAction(BaseAction):
    """Control system audio volume.
    
    Gets or sets system output volume.
    """
    action_type = "audio_volume"
    display_name = "音量控制"
    description = "控制系统音量"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Control volume.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: volume (0-100), direction (up/down/set).
        
        Returns:
            ActionResult with volume status.
        """
        volume = params.get('volume', None)
        direction = params.get('direction', 'get')
        
        try:
            if direction == 'get':
                # Get current volume
                script = 'output volume of (get volume settings)'
                result = subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)
                if result.returncode == 0:
                    current = int(result.stdout.strip())
                    return ActionResult(
                        success=True,
                        message=f"Current volume: {current}",
                        data={'volume': current}
                    )
                return ActionResult(success=False, message="Failed to get volume")
            
            elif direction in ['up', 'down']:
                # Adjust volume
                script = f'''
                set vol to output volume of (get volume settings)
                set newVol to vol + (if "{direction}" is "up" then 5 else -5)
                set volume output volume newVol
                newVol
                '''
                result = subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)
                if result.returncode == 0:
                    return ActionResult(
                        success=True,
                        message=f"Volume {direction}: {result.stdout.strip()}",
                        data={'direction': direction}
                    )
                    
            elif direction == 'set' and volume is not None:
                if not 0 <= volume <= 100:
                    return ActionResult(success=False, message="volume must be 0-100")
                script = f'set volume output volume {volume}'
                subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)
                return ActionResult(
                    success=True,
                    message=f"Volume set to {volume}",
                    data={'volume': volume}
                )
            
            return ActionResult(success=False, message=f"Unknown direction: {direction}")
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Volume error: {e}",
                data={'error': str(e)}
            )
