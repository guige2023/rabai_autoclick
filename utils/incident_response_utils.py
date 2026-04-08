"""
Incident Response and SRE Utilities.

Provides utilities for managing SRE incidents, on-call schedules,
runbooks, post-mortems, and SLA tracking.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


class IncidentSeverity(Enum):
    """Incident severity levels."""
    SEV1 = "sev1"
    SEV2 = "sev2"
    SEV3 = "sev3"
    SEV4 = "sev4"


class IncidentStatus(Enum):
    """Incident status values."""
    TRIGGERED = "triggered"
    ACKNOWLEDGED = "acknowledged"
    INVESTIGATING = "investigating"
    IDENTIFIED = "identified"
    MITIGATING = "mitigating"
    RESOLVED = "resolved"
    POSTMORTEM = "postmortem"


class OnCallStatus(Enum):
    """On-call rotation status."""
    ACTIVE = "active"
    ON_LEAVE = "on_leave"
    OFF_DUTY = "off_duty"


@dataclass
class Incident:
    """An SRE incident."""
    incident_id: str
    title: str
    severity: IncidentSeverity
    status: IncidentStatus
    created_at: datetime
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    commander: Optional[str] = None
    assignees: list[str] = field(default_factory=list)
    affected_services: list[str] = field(default_factory=list)
    timeline: list[dict[str, Any]] = field(default_factory=list)
    runbook_url: Optional[str] = None
    slack_channel: Optional[str] = None
    annotations: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class OnCallEngineer:
    """On-call engineer information."""
    user_id: str
    name: str
    email: str
    phone: Optional[str] = None
    status: OnCallStatus = OnCallStatus.ACTIVE
    escalation_level: int = 1


@dataclass
class OnCallSchedule:
    """On-call schedule definition."""
    schedule_id: str
    team_name: str
    rotation_period_hours: int = 24
    handoff_time: str = "09:00"
    primary_engineer: Optional[OnCallEngineer] = None
    secondary_engineer: Optional[OnCallEngineer] = None
    backup_engineer: Optional[OnCallEngineer] = None


@dataclass
class SLATarget:
    """SLA target definition."""
    sla_id: str
    name: str
    service: str
    availability_target: float
    latency_p99_target_ms: int
    error_rate_target: float
    current_availability: float = 0.0
    current_latency_p99_ms: int = 0
    current_error_rate: float = 0.0
    last_measured: Optional[datetime] = None


@dataclass
class PostMortem:
    """Post-mortem document."""
    postmortem_id: str
    incident_id: str
    title: str
    severity: IncidentSeverity
    status: str
    created_at: datetime
    author: str
    summary: str = ""
    impact: str = ""
    root_cause: str = ""
    timeline: list[dict[str, Any]] = field(default_factory=list)
    action_items: list[dict[str, str]] = field(default_factory=list)
    lessons_learned: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


class IncidentManager:
    """Manages SRE incidents."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or Path("incidents.db")
        self._init_db()
        self._active_incidents: dict[str, Incident] = {}

    def _init_db(self) -> None:
        """Initialize the incidents database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS incidents (
                incident_id TEXT PRIMARY KEY,
                incident_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_incidents_status
            ON incidents(incident_json)
        """)
        conn.commit()
        conn.close()

    def create_incident(
        self,
        title: str,
        severity: IncidentSeverity,
        affected_services: Optional[list[str]] = None,
        commander: Optional[str] = None,
        runbook_url: Optional[str] = None,
    ) -> Incident:
        """Create a new incident."""
        incident_id = f"inc_{int(time.time())}_{hashlib.md5(title.encode()).hexdigest()[:8]}"

        incident = Incident(
            incident_id=incident_id,
            title=title,
            severity=severity,
            status=IncidentStatus.TRIGGERED,
            created_at=datetime.now(),
            commander=commander,
            affected_services=affected_services or [],
            runbook_url=runbook_url,
        )

        self._add_timeline_event(incident, "created", "Incident created")

        self._active_incidents[incident_id] = incident
        self._save_incident(incident)

        return incident

    def acknowledge_incident(
        self,
        incident_id: str,
        acknowledger: str,
    ) -> bool:
        """Acknowledge an incident."""
        incident = self._active_incidents.get(incident_id)
        if not incident:
            return False

        incident.status = IncidentStatus.ACKNOWLEDGED
        incident.acknowledged_at = datetime.now()

        self._add_timeline_event(
            incident,
            "acknowledged",
            f"Acknowledged by {acknowledger}",
        )

        self._save_incident(incident)
        return True

    def assign_incident(
        self,
        incident_id: str,
        assignees: list[str],
    ) -> bool:
        """Assign an incident to engineers."""
        incident = self._active_incidents.get(incident_id)
        if not incident:
            return False

        incident.assignees.extend(assignees)
        self._add_timeline_event(
            incident,
            "assigned",
            f"Assigned to {', '.join(assignees)}",
        )

        self._save_incident(incident)
        return True

    def update_status(
        self,
        incident_id: str,
        status: IncidentStatus,
        updater: str,
        message: str = "",
    ) -> bool:
        """Update incident status."""
        incident = self._active_incidents.get(incident_id)
        if not incident:
            return False

        incident.status = status

        if message:
            self._add_timeline_event(incident, status.value, message)
        else:
            self._add_timeline_event(incident, status.value, f"Status changed to {status.value}")

        if status == IncidentStatus.RESOLVED:
            incident.resolved_at = datetime.now()
            self._add_timeline_event(incident, "resolved", f"Resolved by {updater}")

        self._save_incident(incident)
        return True

    def resolve_incident(
        self,
        incident_id: str,
        resolver: str,
        resolution_notes: str = "",
    ) -> bool:
        """Resolve an incident."""
        incident = self._active_incidents.get(incident_id)
        if not incident:
            return False

        incident.status = IncidentStatus.RESOLVED
        incident.resolved_at = datetime.now()

        self._add_timeline_event(incident, "resolved", f"Resolved by {resolver}")

        if resolution_notes:
            self._add_timeline_event(incident, "notes", resolution_notes)

        self._save_incident(incident)

        if incident_id in self._active_incidents:
            del self._active_incidents[incident_id]

        return True

    def get_incident(self, incident_id: str) -> Optional[Incident]:
        """Get an incident by ID."""
        if incident_id in self._active_incidents:
            return self._active_incidents[incident_id]

        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM incidents WHERE incident_id = ?", (incident_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return self._json_to_incident(json.loads(row["incident_json"]))
        return None

    def get_active_incidents(self) -> list[Incident]:
        """Get all active incidents."""
        return list(self._active_incidents.values())

    def get_incidents_by_severity(
        self,
        severity: IncidentSeverity,
    ) -> list[Incident]:
        """Get incidents by severity level."""
        active = [i for i in self._active_incidents.values() if i.severity == severity]
        return active

    def add_timeline_event(
        self,
        incident_id: str,
        event_type: str,
        message: str,
        author: Optional[str] = None,
    ) -> bool:
        """Add an event to the incident timeline."""
        incident = self.get_incident(incident_id)
        if not incident:
            return False

        self._add_timeline_event(incident, event_type, message, author)
        self._save_incident(incident)
        return True

    def _add_timeline_event(
        self,
        incident: Incident,
        event_type: str,
        message: str,
        author: Optional[str] = None,
    ) -> None:
        """Add an event to the incident timeline."""
        incident.timeline.append({
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "message": message,
            "author": author,
        })

    def _save_incident(self, incident: Incident) -> None:
        """Save an incident to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO incidents (incident_id, incident_json, created_at)
            VALUES (?, ?, ?)
        """, (
            incident.incident_id,
            json.dumps({
                "title": incident.title,
                "severity": incident.severity.value,
                "status": incident.status.value,
                "acknowledged_at": incident.acknowledged_at.isoformat() if incident.acknowledged_at else None,
                "resolved_at": incident.resolved_at.isoformat() if incident.resolved_at else None,
                "commander": incident.commander,
                "assignees": incident.assignees,
                "affected_services": incident.affected_services,
                "timeline": incident.timeline,
                "runbook_url": incident.runbook_url,
                "slack_channel": incident.slack_channel,
                "annotations": incident.annotations,
                "metadata": incident.metadata,
            }),
            incident.created_at.isoformat(),
        ))
        conn.commit()
        conn.close()

    def _json_to_incident(self, data: dict[str, Any]) -> Incident:
        """Convert JSON to Incident object."""
        return Incident(
            incident_id="",
            title=data["title"],
            severity=IncidentSeverity(data["severity"]),
            status=IncidentStatus(data["status"]),
            created_at=datetime.now(),
            acknowledged_at=datetime.fromisoformat(data["acknowledged_at"]) if data.get("acknowledged_at") else None,
            resolved_at=datetime.fromisoformat(data["resolved_at"]) if data.get("resolved_at") else None,
            commander=data.get("commander"),
            assignees=data.get("assignees", []),
            affected_services=data.get("affected_services", []),
            timeline=data.get("timeline", []),
            runbook_url=data.get("runbook_url"),
            slack_channel=data.get("slack_channel"),
            annotations=data.get("annotations", []),
            metadata=data.get("metadata", {}),
        )


class OnCallManager:
    """Manages on-call schedules and rotations."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or Path("oncall.db")
        self._schedules: dict[str, OnCallSchedule] = {}
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the on-call database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                schedule_id TEXT PRIMARY KEY,
                schedule_json TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS handoffs (
                handoff_id TEXT PRIMARY KEY,
                schedule_id TEXT NOT NULL,
                from_engineer TEXT NOT NULL,
                to_engineer TEXT NOT NULL,
                handed_off_at TEXT NOT NULL
            )
        """)
        conn.commit()
        conn.close()

    def create_schedule(
        self,
        team_name: str,
        rotation_period_hours: int = 24,
        primary: Optional[OnCallEngineer] = None,
        secondary: Optional[OnCallEngineer] = None,
    ) -> OnCallSchedule:
        """Create a new on-call schedule."""
        schedule_id = f"schedule_{int(time.time())}"

        schedule = OnCallSchedule(
            schedule_id=schedule_id,
            team_name=team_name,
            rotation_period_hours=rotation_period_hours,
            primary_engineer=primary,
            secondary_engineer=secondary,
        )

        self._schedules[schedule_id] = schedule
        self._save_schedule(schedule)
        return schedule

    def get_current_oncall(self, schedule_id: str) -> Optional[OnCallEngineer]:
        """Get the current on-call engineer."""
        schedule = self._schedules.get(schedule_id)
        if schedule and schedule.primary_engineer:
            return schedule.primary_engineer
        return None

    def get_escalation_path(
        self,
        schedule_id: str,
    ) -> list[OnCallEngineer]:
        """Get the escalation path for a schedule."""
        schedule = self._schedules.get(schedule_id)
        if not schedule:
            return []

        path = []
        if schedule.primary_engineer:
            path.append(schedule.primary_engineer)
        if schedule.secondary_engineer:
            path.append(schedule.secondary_engineer)
        if schedule.backup_engineer:
            path.append(schedule.backup_engineer)

        return path

    def rotate_oncall(
        self,
        schedule_id: str,
    ) -> bool:
        """Rotate on-call to the next engineer."""
        schedule = self._schedules.get(schedule_id)
        if not schedule:
            return False

        if schedule.secondary_engineer and schedule.primary_engineer:
            old_primary = schedule.primary_engineer
            schedule.primary_engineer = schedule.secondary_engineer
            schedule.secondary_engineer = old_primary

            self._save_schedule(schedule)
            return True

        return False

    def _save_schedule(self, schedule: OnCallSchedule) -> None:
        """Save a schedule to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO schedules (schedule_id, schedule_json)
            VALUES (?, ?)
        """, (
            schedule.schedule_id,
            json.dumps({
                "team_name": schedule.team_name,
                "rotation_period_hours": schedule.rotation_period_hours,
                "handoff_time": schedule.handoff_time,
            }),
        ))
        conn.commit()
        conn.close()


class SLATracker:
    """Tracks SLA compliance."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or Path("sla.db")
        self._slas: dict[str, SLATarget] = {}
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the SLA database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS slas (
                sla_id TEXT PRIMARY KEY,
                sla_json TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sla_breaches (
                breach_id TEXT PRIMARY KEY,
                sla_id TEXT NOT NULL,
                breach_type TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                details TEXT
            )
        """)
        conn.commit()
        conn.close()

    def create_sla(
        self,
        name: str,
        service: str,
        availability_target: float,
        latency_p99_target_ms: int,
        error_rate_target: float,
    ) -> SLATarget:
        """Create a new SLA target."""
        sla_id = f"sla_{int(time.time())}"

        sla = SLATarget(
            sla_id=sla_id,
            name=name,
            service=service,
            availability_target=availability_target,
            latency_p99_target_ms=latency_p99_target_ms,
            error_rate_target=error_rate_target,
        )

        self._slas[sla_id] = sla
        self._save_sla(sla)
        return sla

    def update_sla_metrics(
        self,
        sla_id: str,
        availability: float,
        latency_p99_ms: int,
        error_rate: float,
    ) -> None:
        """Update current SLA metrics."""
        sla = self._slas.get(sla_id)
        if not sla:
            return

        sla.current_availability = availability
        sla.current_latency_p99_ms = latency_p99_ms
        sla.current_error_rate = error_rate
        sla.last_measured = datetime.now()

        if availability < sla.availability_target:
            self._record_breach(sla_id, "availability")

        if latency_p99_ms > sla.latency_p99_target_ms:
            self._record_breach(sla_id, "latency")

        if error_rate > sla.error_rate_target:
            self._record_breach(sla_id, "error_rate")

        self._save_sla(sla)

    def get_sla_status(self, sla_id: str) -> dict[str, Any]:
        """Get current SLA status."""
        sla = self._slas.get(sla_id)
        if not sla:
            return {}

        return {
            "sla_id": sla_id,
            "name": sla.name,
            "service": sla.service,
            "availability": {
                "current": sla.current_availability,
                "target": sla.availability_target,
                "compliant": sla.current_availability >= sla.availability_target,
            },
            "latency": {
                "current_ms": sla.current_latency_p99_ms,
                "target_ms": sla.latency_p99_target_ms,
                "compliant": sla.current_latency_p99_ms <= sla.latency_p99_target_ms,
            },
            "error_rate": {
                "current": sla.current_error_rate,
                "target": sla.error_rate_target,
                "compliant": sla.current_error_rate <= sla.error_rate_target,
            },
            "last_measured": sla.last_measured.isoformat() if sla.last_measured else None,
        }

    def _record_breach(
        self,
        sla_id: str,
        breach_type: str,
    ) -> None:
        """Record an SLA breach."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sla_breaches (breach_id, sla_id, breach_type, occurred_at)
            VALUES (?, ?, ?, ?)
        """, (
            f"breach_{int(time.time())}",
            sla_id,
            breach_type,
            datetime.now().isoformat(),
        ))
        conn.commit()
        conn.close()

    def _save_sla(self, sla: SLATarget) -> None:
        """Save an SLA to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO slas (sla_id, sla_json)
            VALUES (?, ?)
        """, (
            sla.sla_id,
            json.dumps({
                "name": sla.name,
                "service": sla.service,
                "availability_target": sla.availability_target,
                "latency_p99_target_ms": sla.latency_p99_target_ms,
                "error_rate_target": sla.error_rate_target,
                "current_availability": sla.current_availability,
                "current_latency_p99_ms": sla.current_latency_p99_ms,
                "current_error_rate": sla.current_error_rate,
                "last_measured": sla.last_measured.isoformat() if sla.last_measured else None,
            }),
        ))
        conn.commit()
        conn.close()
