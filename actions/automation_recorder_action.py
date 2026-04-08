# Copyright (c) 2024. coded by claude
"""Automation Recorder Action Module.

Records user interactions and automation sequences for later playback
with support for metadata capture, action grouping, and export.
"""
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json
import logging

logger = logging.getLogger(__name__)


class RecordingStatus(Enum):
    IDLE = "idle"
    RECORDING = "recording"
    PAUSED = "paused"


@dataclass
class RecordingSession:
    session_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    actions: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class AutomationRecorder:
    def __init__(self):
        self._status = RecordingStatus.IDLE
        self._current_session: Optional[RecordingSession] = None
        self._action_listeners: List[Callable] = []

    def start_recording(self, session_id: Optional[str] = None) -> RecordingSession:
        if self._status == RecordingStatus.RECORDING:
            raise RuntimeError("Recording already in progress")
        session_id = session_id or f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self._current_session = RecordingSession(
            session_id=session_id,
            start_time=datetime.now(),
        )
        self._status = RecordingStatus.RECORDING
        logger.info(f"Started recording session: {session_id}")
        return self._current_session

    def stop_recording(self) -> Optional[RecordingSession]:
        if self._status != RecordingStatus.RECORDING:
            return None
        if self._current_session:
            self._current_session.end_time = datetime.now()
        self._status = RecordingStatus.IDLE
        session = self._current_session
        logger.info(f"Stopped recording session: {session.session_id if session else 'None'}")
        return session

    def pause_recording(self) -> None:
        if self._status == RecordingStatus.RECORDING:
            self._status = RecordingStatus.PAUSED

    def resume_recording(self) -> None:
        if self._status == RecordingStatus.PAUSED:
            self._status = RecordingStatus.RECORDING

    def record_action(self, action_type: str, target: Optional[str] = None, value: Optional[Any] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        if self._status != RecordingStatus.RECORDING or not self._current_session:
            return
        action = {
            "type": action_type,
            "target": target,
            "value": value,
            "timestamp": datetime.now().isoformat(),
            "metadata": metadata or {},
        }
        self._current_session.actions.append(action)
        for listener in self._action_listeners:
            try:
                listener(action)
            except Exception as e:
                logger.error(f"Action listener failed: {e}")

    def add_action_listener(self, listener: Callable) -> None:
        self._action_listeners.append(listener)

    def get_session(self) -> Optional[RecordingSession]:
        return self._current_session

    def export_session(self, session: RecordingSession, format: str = "json") -> str:
        if format == "json":
            return json.dumps({
                "session_id": session.session_id,
                "start_time": session.start_time.isoformat(),
                "end_time": session.end_time.isoformat() if session.end_time else None,
                "actions": session.actions,
                "metadata": session.metadata,
            }, indent=2)
        raise ValueError(f"Unsupported format: {format}")

    def get_status(self) -> RecordingStatus:
        return self._status
