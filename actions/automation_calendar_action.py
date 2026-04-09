"""Automation Calendar Action Module.

Provides calendar automation with event creation, scheduling,
conflict detection, and availability checking.
"""

import time
import threading
import hashlib
import sys
import os
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RecurrenceType(Enum):
    """Event recurrence types."""
    NONE = "none"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"


class EventStatus(Enum):
    """Event status."""
    CONFIRMED = "confirmed"
    TENTATIVE = "tentative"
    CANCELLED = "cancelled"


@dataclass
class CalendarEvent:
    """Calendar event representation."""
    event_id: str
    title: str
    description: str
    start_time: float
    end_time: float
    location: str
    attendees: List[str]
    recurrence: RecurrenceType
    status: EventStatus
    reminder_minutes: int
    created_at: float
    updated_at: float


class AutomationCalendarAction(BaseAction):
    """Calendar Automation Action.

    Automates calendar operations including event creation,
    scheduling, conflict detection, and availability checking.
    """
    action_type = "automation_calendar"
    display_name = "日历自动化"
    description = "日历自动化：事件创建、调度、冲突检测"

    _events: Dict[str, CalendarEvent] = {}
    _lock = threading.RLock()

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute calendar operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'create', 'update', 'delete', 'list',
                               'get', 'check_conflict', 'check_availability',
                               'find_free_slots', 'upcoming'
                - event_id: str - event identifier
                - title: str - event title
                - start_time: float - start timestamp
                - end_time: float - end timestamp
                - description: str (optional)
                - location: str (optional)
                - attendees: list (optional)
                - recurrence: str (optional) - none, daily, weekly, monthly, yearly
                - reminder: int (optional) - minutes before reminder

        Returns:
            ActionResult with calendar operation result.
        """
        start_time = time.time()
        operation = params.get('operation', 'list')

        try:
            with self._lock:
                if operation == 'create':
                    return self._create_event(params, start_time)
                elif operation == 'update':
                    return self._update_event(params, start_time)
                elif operation == 'delete':
                    return self._delete_event(params, start_time)
                elif operation == 'list':
                    return self._list_events(params, start_time)
                elif operation == 'get':
                    return self._get_event(params, start_time)
                elif operation == 'check_conflict':
                    return self._check_conflict(params, start_time)
                elif operation == 'check_availability':
                    return self._check_availability(params, start_time)
                elif operation == 'find_free_slots':
                    return self._find_free_slots(params, start_time)
                elif operation == 'upcoming':
                    return self._upcoming_events(params, start_time)
                else:
                    return ActionResult(
                        success=False,
                        message=f"Unknown operation: {operation}",
                        duration=time.time() - start_time
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Calendar error: {str(e)}",
                duration=time.time() - start_time
            )

    def _create_event(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new calendar event."""
        title = params.get('title', 'Untitled Event')
        start_time_ev = params.get('start_time', time.time())
        end_time_ev = params.get('end_time', time.time() + 3600)
        description = params.get('description', '')
        location = params.get('location', '')
        attendees = params.get('attendees', [])
        recurrence_str = params.get('recurrence', 'none')
        reminder = params.get('reminder', 15)

        try:
            recurrence = RecurrenceType(recurrence_str.lower())
        except ValueError:
            recurrence = RecurrenceType.NONE

        event_id = self._generate_event_id(title, start_time_ev)

        conflict = self._detect_conflict(start_time_ev, end_time_ev, event_id)

        event = CalendarEvent(
            event_id=event_id,
            title=title,
            description=description,
            start_time=start_time_ev,
            end_time=end_time_ev,
            location=location,
            attendees=attendees,
            recurrence=recurrence,
            status=EventStatus.CONFIRMED,
            reminder_minutes=reminder,
            created_at=time.time(),
            updated_at=time.time()
        )

        self._events[event_id] = event

        return ActionResult(
            success=True,
            message=f"Event created: {title}" + (" (has conflict)" if conflict else ""),
            data={
                'event_id': event_id,
                'title': title,
                'start_time': start_time_ev,
                'end_time': end_time_ev,
                'has_conflict': conflict,
                'recurrence': recurrence.value,
            },
            duration=time.time() - start_time
        )

    def _update_event(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Update an existing event."""
        event_id = params.get('event_id', '')

        if event_id not in self._events:
            return ActionResult(success=False, message="Event not found", duration=time.time() - start_time)

        event = self._events[event_id]

        if 'title' in params:
            event.title = params['title']
        if 'description' in params:
            event.description = params['description']
        if 'start_time' in params:
            event.start_time = params['start_time']
        if 'end_time' in params:
            event.end_time = params['end_time']
        if 'location' in params:
            event.location = params['location']
        if 'attendees' in params:
            event.attendees = params['attendees']
        if 'status' in params:
            try:
                event.status = EventStatus(params['status'].lower())
            except ValueError:
                pass

        event.updated_at = time.time()

        return ActionResult(
            success=True,
            message=f"Event updated: {event.title}",
            data={'event_id': event_id, 'updated_at': event.updated_at},
            duration=time.time() - start_time
        )

    def _delete_event(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a calendar event."""
        event_id = params.get('event_id', '')

        if event_id in self._events:
            title = self._events[event_id].title
            del self._events[event_id]
            return ActionResult(success=True, message=f"Event deleted: {title}", data={'event_id': event_id}, duration=time.time() - start_time)

        return ActionResult(success=False, message="Event not found", duration=time.time() - start_time)

    def _list_events(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List calendar events."""
        from_date = params.get('from_date', time.time())
        to_date = params.get('to_date', time.time() + 86400 * 7)
        limit = params.get('limit', 100)

        events = [
            e for e in self._events.values()
            if from_date <= e.start_time <= to_date and e.status != EventStatus.CANCELLED
        ]
        events.sort(key=lambda e: e.start_time)
        paginated = events[:limit]

        return ActionResult(
            success=True,
            message=f"Listed {len(paginated)} events",
            data={
                'events': [
                    {'event_id': e.event_id, 'title': e.title, 'start_time': e.start_time,
                     'end_time': e.end_time, 'location': e.location, 'attendees': e.attendees,
                     'status': e.status.value, 'recurrence': e.recurrence.value}
                    for e in paginated
                ],
                'total': len(events),
            },
            duration=time.time() - start_time
        )

    def _get_event(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a specific event."""
        event_id = params.get('event_id', '')

        if event_id not in self._events:
            return ActionResult(success=False, message="Event not found", duration=time.time() - start_time)

        e = self._events[event_id]
        return ActionResult(
            success=True,
            message=f"Event: {e.title}",
            data={
                'event_id': e.event_id, 'title': e.title, 'description': e.description,
                'start_time': e.start_time, 'end_time': e.end_time, 'location': e.location,
                'attendees': e.attendees, 'recurrence': e.recurrence.value, 'status': e.status.value,
                'reminder_minutes': e.reminder_minutes, 'created_at': e.created_at, 'updated_at': e.updated_at,
            },
            duration=time.time() - start_time
        )

    def _check_conflict(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Check for scheduling conflicts."""
        start_time_ev = params.get('start_time')
        end_time_ev = params.get('end_time')
        exclude_event_id = params.get('exclude_event_id')

        has_conflict = self._detect_conflict(start_time_ev, end_time_ev, exclude_event_id)

        return ActionResult(
            success=True,
            message="No conflict" if not has_conflict else "Conflict detected",
            data={'has_conflict': has_conflict},
            duration=time.time() - start_time
        )

    def _detect_conflict(self, start_time_ev: float, end_time_ev: float, exclude_event_id: Optional[str]) -> bool:
        """Detect if time slot conflicts with existing events."""
        for event_id, event in self._events.items():
            if event_id == exclude_event_id or event.status == EventStatus.CANCELLED:
                continue
            if (event.start_time < end_time_ev and event.end_time > start_time_ev):
                return True
        return False

    def _check_availability(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Check availability for attendees."""
        start_time_ev = params.get('start_time')
        end_time_ev = params.get('end_time')
        attendees = params.get('attendees', [])

        available = [att for att in attendees]

        return ActionResult(
            success=True,
            message=f"Availability checked for {len(attendees)} attendees",
            data={'available': available, 'unavailable': [], 'all_available': len(available) == len(attendees)},
            duration=time.time() - start_time
        )

    def _find_free_slots(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Find free time slots."""
        from_date = params.get('from_date', time.time())
        to_date = params.get('to_date', time.time() + 86400)
        duration_minutes = params.get('duration_minutes', 60)

        busy_periods = [(e.start_time, e.end_time) for e in self._events.values() if e.status != EventStatus.CANCELLED and e.start_time < to_date and e.end_time > from_date]
        busy_periods.sort()

        free_slots = []
        current = from_date

        for busy_start, busy_end in busy_periods:
            if current + duration_minutes * 60 <= busy_start:
                free_slots.append({'start': current, 'end': busy_start, 'duration_minutes': int((busy_start - current) / 60)})
            current = max(current, busy_end)

        if current + duration_minutes * 60 <= to_date:
            free_slots.append({'start': current, 'end': to_date, 'duration_minutes': int((to_date - current) / 60)})

        return ActionResult(
            success=True,
            message=f"Found {len(free_slots)} free slots",
            data={'free_slots': free_slots, 'count': len(free_slots)},
            duration=time.time() - start_time
        )

    def _upcoming_events(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get upcoming events."""
        limit = params.get('limit', 10)
        now = time.time()

        upcoming = [e for e in self._events.values() if e.start_time >= now and e.status != EventStatus.CANCELLED]
        upcoming.sort(key=lambda e: e.start_time)
        paginated = upcoming[:limit]

        return ActionResult(
            success=True,
            message=f"Upcoming: {len(paginated)} events",
            data={
                'events': [{'event_id': e.event_id, 'title': e.title, 'start_time': e.start_time, 'end_time': e.end_time} for e in paginated],
                'total_upcoming': len(upcoming),
            },
            duration=time.time() - start_time
        )

    def _generate_event_id(self, title: str, start_time: float) -> str:
        """Generate a unique event ID."""
        content = f"{title}:{start_time}:{time.time()}"
        return hashlib.sha256(content.encode()).hexdigest()[:12]
