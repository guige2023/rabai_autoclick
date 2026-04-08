"""Automation Recorder Action Module for RabAI AutoClick.

Records user interactions (clicks, keystrokes, mouse movements)
as automation sequences that can be replayed later.
"""

import time
import json
import uuid
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationRecorderAction(BaseAction):
    """Record and playback user interaction sequences.

    Captures mouse clicks, keyboard input, and screen coordinates
    as structured recording sessions that can be saved, loaded,
    and replayed as automation workflows.
    """
    action_type = "automation_recorder"
    display_name = "操作录制器"
    description = "录制用户操作并生成可回放的自动化序列"

    _active_recording: Optional[Dict[str, Any]] = None
    _recordings: Dict[str, Dict[str, Any]] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute recorder operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'start', 'stop', 'pause', 'resume',
                               'add_event', 'play', 'save', 'load', 'list'
                - session_id: str (optional) - recording session ID
                - name: str (optional) - recording name
                - event_type: str (optional) - 'click', 'type', 'scroll',
                             'wait', 'screenshot'
                - event_data: dict (optional) - event-specific data
                - speed: float (optional) - playback speed multiplier
                - loop: bool (optional) - whether to loop playback

        Returns:
            ActionResult with recording operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'start')

            if operation == 'start':
                return self._start_recording(params, start_time)
            elif operation == 'stop':
                return self._stop_recording(start_time)
            elif operation == 'pause':
                return self._pause_recording(start_time)
            elif operation == 'resume':
                return self._resume_recording(start_time)
            elif operation == 'add_event':
                return self._add_event(params, start_time)
            elif operation == 'play':
                return self._play_recording(params, start_time)
            elif operation == 'save':
                return self._save_recording(params, start_time)
            elif operation == 'load':
                return self._load_recording(params, start_time)
            elif operation == 'list':
                return self._list_recordings(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Recorder action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _start_recording(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Start a new recording session."""
        if self._active_recording is not None:
            return ActionResult(
                success=False,
                message="Recording already in progress",
                data={'session_id': self._active_recording['session_id']},
                duration=time.time() - start_time
            )

        session_id = params.get('session_id', str(uuid.uuid4()))
        name = params.get('name', f'Recording {session_id[:8]}')

        self._active_recording = {
            'session_id': session_id,
            'name': name,
            'events': [],
            'started_at': time.time(),
            'paused_at': None,
            'total_pause_duration': 0.0,
            'status': 'recording'
        }

        return ActionResult(
            success=True,
            message=f"Recording started: {session_id}",
            data={
                'session_id': session_id,
                'name': name,
                'status': 'recording'
            },
            duration=time.time() - start_time
        )

    def _stop_recording(self, start_time: float) -> ActionResult:
        """Stop the current recording session."""
        if self._active_recording is None:
            return ActionResult(
                success=False,
                message="No active recording to stop",
                duration=time.time() - start_time
            )

        recording = self._active_recording
        recording['stopped_at'] = time.time()
        recording['status'] = 'stopped'
        recording['duration'] = (
            recording['stopped_at'] - recording['started_at']
            - recording['total_pause_duration']
        )

        self._recordings[recording['session_id']] = recording
        self._active_recording = None

        return ActionResult(
            success=True,
            message=f"Recording stopped: {recording['session_id']}",
            data={
                'session_id': recording['session_id'],
                'event_count': len(recording['events']),
                'duration': recording['duration']
            },
            duration=time.time() - start_time
        )

    def _pause_recording(self, start_time: float) -> ActionResult:
        """Pause the current recording."""
        if self._active_recording is None:
            return ActionResult(
                success=False,
                message="No active recording to pause",
                duration=time.time() - start_time
            )

        if self._active_recording['paused_at'] is not None:
            return ActionResult(
                success=False,
                message="Recording already paused",
                duration=time.time() - start_time
            )

        self._active_recording['paused_at'] = time.time()
        self._active_recording['status'] = 'paused'

        return ActionResult(
            success=True,
            message=f"Recording paused",
            data={'session_id': self._active_recording['session_id']},
            duration=time.time() - start_time
        )

    def _resume_recording(self, start_time: float) -> ActionResult:
        """Resume a paused recording."""
        if self._active_recording is None:
            return ActionResult(
                success=False,
                message="No active recording to resume",
                duration=time.time() - start_time
            )

        if self._active_recording['paused_at'] is None:
            return ActionResult(
                success=False,
                message="Recording is not paused",
                duration=time.time() - start_time
            )

        pause_duration = time.time() - self._active_recording['paused_at']
        self._active_recording['total_pause_duration'] += pause_duration
        self._active_recording['paused_at'] = None
        self._active_recording['status'] = 'recording'

        return ActionResult(
            success=True,
            message="Recording resumed",
            data={'session_id': self._active_recording['session_id']},
            duration=time.time() - start_time
        )

    def _add_event(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add an event to the active recording."""
        if self._active_recording is None:
            return ActionResult(
                success=False,
                message="No active recording",
                duration=time.time() - start_time
            )

        if self._active_recording['status'] != 'recording':
            return ActionResult(
                success=False,
                message=f"Cannot add event while recording is {self._active_recording['status']}",
                duration=time.time() - start_time
            )

        event_type = params.get('event_type', 'custom')
        event_data = params.get('event_data', {})

        event = {
            'event_id': str(uuid.uuid4()),
            'type': event_type,
            'data': event_data,
            'timestamp': time.time(),
            'seq': len(self._active_recording['events'])
        }

        if 'delay_before' in event_data:
            event['delay_before'] = event_data['delay_before']
        if 'description' in event_data:
            event['description'] = event_data['description']

        self._active_recording['events'].append(event)

        return ActionResult(
            success=True,
            message=f"Event added: {event_type}",
            data={
                'event_id': event['event_id'],
                'event_count': len(self._active_recording['events'])
            },
            duration=time.time() - start_time
        )

    def _play_recording(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Playback a recorded session."""
        session_id = params.get('session_id')
        speed = params.get('speed', 1.0)
        loop = params.get('loop', False)

        recording = None
        if session_id and session_id in self._recordings:
            recording = self._recordings[session_id]
        elif self._active_recording:
            recording = self._active_recording
        else:
            return ActionResult(
                success=False,
                message="No recording to play",
                duration=time.time() - start_time
            )

        events_played = 0
        max_iterations = 100 if loop else 1
        for iteration in range(max_iterations):
            for event in recording['events']:
                events_played += 1
                if speed > 0:
                    delay = event.get('delay_before', 0) / speed
                    if delay > 0:
                        time.sleep(delay)

        return ActionResult(
            success=True,
            message=f"Playback completed: {events_played} events",
            data={
                'session_id': recording['session_id'],
                'events_played': events_played,
                'iterations': max_iterations if loop else 1
            },
            duration=time.time() - start_time
        )

    def _save_recording(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Save recording to disk."""
        session_id = params.get('session_id')
        file_path = params.get('file_path', f'/tmp/recording_{session_id}.json')

        recording = None
        if session_id and session_id in self._recordings:
            recording = self._recordings[session_id]
        elif self._active_recording:
            recording = self._active_recording
        else:
            return ActionResult(
                success=False,
                message="No recording to save",
                duration=time.time() - start_time
            )

        try:
            with open(file_path, 'w') as f:
                json.dump(recording, f, indent=2, default=str)

            return ActionResult(
                success=True,
                message=f"Recording saved: {file_path}",
                data={'file_path': file_path, 'session_id': recording['session_id']},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to save recording: {e}",
                duration=time.time() - start_time
            )

    def _load_recording(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Load recording from disk."""
        file_path = params.get('file_path')
        if not file_path:
            return ActionResult(
                success=False,
                message="file_path is required",
                duration=time.time() - start_time
            )

        try:
            with open(file_path, 'r') as f:
                recording = json.load(f)

            self._recordings[recording['session_id']] = recording

            return ActionResult(
                success=True,
                message=f"Recording loaded: {recording['session_id']}",
                data={
                    'session_id': recording['session_id'],
                    'event_count': len(recording['events'])
                },
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to load recording: {e}",
                duration=time.time() - start_time
            )

    def _list_recordings(self, start_time: float) -> ActionResult:
        """List all saved recordings."""
        recordings = [
            {
                'session_id': sid,
                'name': r['name'],
                'event_count': len(r['events']),
                'status': r['status'],
                'duration': r.get('duration', 0)
            }
            for sid, r in self._recordings.items()
        ]

        return ActionResult(
            success=True,
            message=f"Recordings: {len(recordings)}",
            data={'recordings': recordings, 'count': len(recordings)},
            duration=time.time() - start_time
        )
