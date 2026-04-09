"""Automation Recorder Action Module.

Records and manages automation session recordings with metadata,
search, categorization, and export capabilities.
"""

import time
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class Recording:
    recording_id: str
    name: str
    created_at: float
    duration_sec: float
    action_count: int
    category: str
    tags: List[str]
    file_path: str
    metadata: Dict[str, Any]


@dataclass
class RecordedAction:
    index: int
    timestamp: float
    relative_time: float
    action_type: str
    params: Dict[str, Any]
    screen_region: Optional[str] = None
    result: Optional[Any] = None


class AutomationRecorderAction:
    """Records and manages automation session recordings."""

    def __init__(
        self,
        storage_dir: str = "/tmp/automation_recordings",
    ) -> None:
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self._recordings: Dict[str, Recording] = {}
        self._current_recording: Optional[str] = None
        self._current_actions: List[RecordedAction] = []
        self._recording_start: float = 0.0
        self._listeners: Dict[str, List[Callable]] = {
            "recording_start": [],
            "recording_stop": [],
            "action_recorded": [],
        }
        self._load_index()

    def start_recording(
        self,
        name: str,
        category: str = "general",
        tags: Optional[List[str]] = None,
    ) -> str:
        recording_id = f"rec_{int(time.time() * 1000)}"
        self._current_recording = recording_id
        self._current_actions = []
        self._recording_start = time.time()
        self._notify("recording_start", {"recording_id": recording_id, "name": name})
        logger.info(f"Started recording {recording_id}: {name}")
        return recording_id

    def record_action(
        self,
        action_type: str,
        params: Dict[str, Any],
        result: Optional[Any] = None,
        screen_region: Optional[str] = None,
    ) -> None:
        if not self._current_recording:
            return
        relative = time.time() - self._recording_start
        action = RecordedAction(
            index=len(self._current_actions),
            timestamp=time.time(),
            relative_time=relative,
            action_type=action_type,
            params=params,
            screen_region=screen_region,
            result=result,
        )
        self._current_actions.append(action)
        self._notify("action_recorded", action)

    def stop_recording(
        self,
        name: str,
        category: str = "general",
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        if not self._current_recording:
            return None
        recording_id = self._current_recording
        duration = time.time() - self._recording_start
        file_path = self._save_recording(recording_id, self._current_actions)
        recording = Recording(
            recording_id=recording_id,
            name=name,
            created_at=self._recording_start,
            duration_sec=duration,
            action_count=len(self._current_actions),
            category=category,
            tags=tags or [],
            file_path=str(file_path),
            metadata=metadata or {},
        )
        self._recordings[recording_id] = recording
        self._current_recording = None
        self._current_actions = []
        self._save_index()
        self._notify("recording_stop", recording)
        logger.info(f"Stopped recording {recording_id}: {name}")
        return recording_id

    def _save_recording(
        self,
        recording_id: str,
        actions: List[RecordedAction],
    ) -> Path:
        file_path = self.storage_dir / f"{recording_id}.json"
        data = {
            "recording_id": recording_id,
            "actions": [
                {
                    "index": a.index,
                    "timestamp": a.timestamp,
                    "relative_time": a.relative_time,
                    "action_type": a.action_type,
                    "params": a.params,
                    "screen_region": a.screen_region,
                    "result": str(a.result) if a.result else None,
                }
                for a in actions
            ],
        }
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        return file_path

    def load_recording(self, recording_id: str) -> Optional[List[RecordedAction]]:
        recording = self._recordings.get(recording_id)
        if not recording:
            return None
        file_path = Path(recording.file_path)
        if not file_path.exists():
            return None
        with open(file_path) as f:
            data = json.load(f)
        return [
            RecordedAction(
                index=a["index"],
                timestamp=a["timestamp"],
                relative_time=a["relative_time"],
                action_type=a["action_type"],
                params=a["params"],
                screen_region=a.get("screen_region"),
                result=a.get("result"),
            )
            for a in data.get("actions", [])
        ]

    def delete_recording(self, recording_id: str) -> bool:
        recording = self._recordings.pop(recording_id, None)
        if not recording:
            return False
        path = Path(recording.file_path)
        if path.exists():
            path.unlink()
        self._save_index()
        return True

    def list_recordings(
        self,
        category: Optional[str] = None,
        tag: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        results = []
        for rec in self._recordings.values():
            if category and rec.category != category:
                continue
            if tag and tag not in rec.tags:
                continue
            results.append(
                {
                    "recording_id": rec.recording_id,
                    "name": rec.name,
                    "created_at": rec.created_at,
                    "duration_sec": rec.duration_sec,
                    "action_count": rec.action_count,
                    "category": rec.category,
                    "tags": rec.tags,
                    "metadata": rec.metadata,
                }
            )
        results.sort(key=lambda x: x["created_at"], reverse=True)
        return results[:limit]

    def export_recording(
        self,
        recording_id: str,
        format: str = "json",
    ) -> Optional[str]:
        actions = self.load_recording(recording_id)
        if not actions:
            return None
        if format == "json":
            return json.dumps(
                [{"action_type": a.action_type, "params": a.params, "time": a.relative_time} for a in actions],
                indent=2,
            )
        return None

    def add_listener(self, event: str, callback: Callable) -> None:
        if event in self._listeners:
            self._listeners[event].append(callback)

    def _notify(self, event: str, data: Any) -> None:
        for cb in self._listeners.get(event, []):
            try:
                cb(data)
            except Exception as e:
                logger.error(f"Recorder listener error for {event}: {e}")

    def _load_index(self) -> None:
        index_path = self.storage_dir / "recordings_index.json"
        if index_path.exists():
            try:
                with open(index_path) as f:
                    raw = json.load(f)
                    for item in raw.get("recordings", []):
                        self._recordings[item["recording_id"]] = Recording(**item)
            except Exception as e:
                logger.warning(f"Failed to load recordings index: {e}")

    def _save_index(self) -> None:
        index_path = self.storage_dir / "recordings_index.json"
        data = {
            "recordings": [
                {
                    "recording_id": r.recording_id,
                    "name": r.name,
                    "created_at": r.created_at,
                    "duration_sec": r.duration_sec,
                    "action_count": r.action_count,
                    "category": r.category,
                    "tags": r.tags,
                    "file_path": r.file_path,
                    "metadata": r.metadata,
                }
                for r in self._recordings.values()
            ]
        }
        with open(index_path, "w") as f:
            json.dump(data, f, indent=2)
