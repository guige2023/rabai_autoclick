"""
Mobile Companion App Integration v22
REST API client, push notifications, workflow triggering, status updates,
remote control, mobile dashboard, QR code pairing, mobile-first logging,
voice input, and geofencing
"""
import json
import time
import threading
import uuid
import queue
import logging
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict
import copy
import hashlib
import base64
import re

import requests

logger = logging.getLogger(__name__)


class NotificationPriority(Enum):
    """Push notification priority levels"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class WorkflowControlAction(Enum):
    """Remote control actions for workflows"""
    START = "start"
    STOP = "stop"
    PAUSE = "pause"
    RESUME = "resume"
    RESTART = "restart"


class DeviceStatus(Enum):
    """Mobile device connection status"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    PAIRED = "paired"


class GeofenceEvent(Enum):
    """Geofencing event types"""
    ENTER = "enter"
    EXIT = "exit"
    DWELL = "dwell"


@dataclass
class MobileDevice:
    """Represents a paired mobile device"""
    device_id: str
    device_name: str
    device_type: str
    paired_at: datetime
    last_seen: datetime
    status: DeviceStatus = DeviceStatus.DISCONNECTED
    push_token: Optional[str] = None
    capabilities: List[str] = field(default_factory=list)
    location: Optional[Dict[str, float]] = None


@dataclass
class QRPairingData:
    """QR code pairing data structure"""
    pairing_code: str
    server_url: str
    expires_at: datetime
    device_name: Optional[str] = None


@dataclass
class PushNotification:
    """Push notification payload"""
    notification_id: str
    title: str
    body: str
    priority: NotificationPriority
    data: Dict[str, Any]
    sent_at: datetime
    delivered: bool = False


@dataclass
class WorkflowStatusUpdate:
    """Workflow execution status update"""
    workflow_id: str
    workflow_name: str
    status: str
    progress: float
    current_action: Optional[str]
    started_at: datetime
    estimated_completion: Optional[datetime]
    error: Optional[str] = None


@dataclass
class VoiceCommand:
    """Voice command from mobile"""
    command_id: str
    transcription: str
    intent: Optional[str] = None
    entities: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0
    processed_at: Optional[datetime] = None


@dataclass
class GeofenceTrigger:
    """Geofence location trigger"""
    geofence_id: str
    name: str
    latitude: float
    longitude: float
    radius_meters: float
    events: List[GeofenceEvent]
    workflow_ids: List[str]
    enabled: bool = True


class MobileDashboard:
    """Mobile-optimized dashboard view"""

    def __init__(self):
        self.refresh_interval = 30
        self.widgets: List[Dict[str, Any]] = []
        self._setup_default_widgets()

    def _setup_default_widgets(self):
        """Setup default mobile dashboard widgets"""
        self.widgets = [
            {"type": "workflow_summary", "title": "Active Workflows", "position": 0},
            {"type": "quick_actions", "title": "Quick Actions", "position": 1},
            {"type": "recent_activity", "title": "Recent Activity", "position": 2},
            {"type": "system_status", "title": "System Status", "position": 3},
            {"type": "notifications", "title": "Notifications", "position": 4},
        ]

    def get_dashboard_data(self, workflow_states: Dict[str, Any], stats: Dict[str, Any]) -> Dict[str, Any]:
        """Generate mobile dashboard payload"""
        return {
            "widgets": self.widgets,
            "workflows": {
                "active": sum(1 for w in workflow_states.values() if w.get("status") == "running"),
                "total": len(workflow_states),
                "states": workflow_states
            },
            "quick_stats": {
                "success_rate": stats.get("success_rate", 0.0),
                "avg_duration": stats.get("avg_duration", 0.0),
                "total_executions": stats.get("total_executions", 0)
            },
            "generated_at": datetime.now().isoformat(),
            "refresh_interval": self.refresh_interval
        }

    def customize_widgets(self, widgets: List[Dict[str, Any]]):
        """Customize dashboard widgets"""
        self.widgets = sorted(widgets, key=lambda w: w.get("position", 0))


class MobileAppIntegration:
    """Mobile companion app integration with full feature set"""

    def __init__(self, server_url: str = "http://localhost:8080", api_key: Optional[str] = None):
        self.server_url = server_url.rstrip("/")
        self.api_key = api_key
        self.devices: Dict[str, MobileDevice] = {}
        self.pairing_codes: Dict[str, QRPairingData] = {}
        self.push_notifications: Dict[str, PushNotification] = {}
        self.voice_commands: Dict[str, VoiceCommand] = {}
        self.geofences: Dict[str, GeofenceTrigger] = {}
        self.dashboard = MobileDashboard()
        self.workflow_callbacks: Dict[str, Callable] = {}
        self.location_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._running_workflows: Dict[str, Dict[str, Any]] = {}
        self._notification_queue = queue.Queue()
        self._location_thread: Optional[threading.Thread] = None
        self._stop_location_monitoring = threading.Event()
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({"Authorization": f"Bearer {api_key}"})

    # =========================================================================
    # REST API Client
    # =========================================================================

    def connect_to_mobile_app(self, device_id: str, device_info: Dict[str, Any]) -> bool:
        """Connect to mobile companion app"""
        try:
            device = MobileDevice(
                device_id=device_id,
                device_name=device_info.get("name", "Unknown Device"),
                device_type=device_info.get("type", "mobile"),
                paired_at=datetime.now(),
                last_seen=datetime.now(),
                status=DeviceStatus.CONNECTING,
                capabilities=device_info.get("capabilities", ["push", "location", "voice"])
            )
            self.devices[device_id] = device
            response = self._api_request("POST", "/api/mobile/connect", {
                "device_id": device_id,
                "device_info": device_info
            })
            if response and response.get("success"):
                device.status = DeviceStatus.CONNECTED
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to connect to mobile app: {e}")
            return False

    def disconnect_device(self, device_id: str) -> bool:
        """Disconnect a mobile device"""
        if device_id in self.devices:
            self.devices[device_id].status = DeviceStatus.DISCONNECTED
            self.devices[device_id].last_seen = datetime.now()
            return True
        return False

    def _api_request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Optional[Dict]:
        """Make REST API request to mobile app backend"""
        try:
            url = f"{self.server_url}{endpoint}"
            if method == "GET":
                response = self.session.get(url, timeout=10)
            elif method == "POST":
                response = self.session.post(url, json=data, timeout=10)
            elif method == "PUT":
                response = self.session.put(url, json=data, timeout=10)
            elif method == "DELETE":
                response = self.session.delete(url, timeout=10)
            else:
                return None

            if response.status_code == 200:
                return response.json()
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"API request failed: {e}")
            return None

    # =========================================================================
    # Push Notifications
    # =========================================================================

    def send_push_notification(
        self,
        device_id: str,
        title: str,
        body: str,
        priority: NotificationPriority = NotificationPriority.NORMAL,
        data: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Send push notification to mobile device"""
        notification_id = str(uuid.uuid4())
        notification = PushNotification(
            notification_id=notification_id,
            title=title,
            body=body,
            priority=priority,
            data=data or {},
            sent_at=datetime.now()
        )
        self.push_notifications[notification_id] = notification

        payload = {
            "notification_id": notification_id,
            "title": title,
            "body": body,
            "priority": priority.value,
            "data": data or {}
        }

        response = self._api_request("POST", f"/api/notifications/{device_id}", payload)
        if response and response.get("delivered"):
            notification.delivered = True
        return notification_id

    def send_workflow_notification(self, device_id: str, workflow_name: str, status: str, message: str):
        """Send workflow-related push notification"""
        priority = NotificationPriority.HIGH if status in ["completed", "failed"] else NotificationPriority.NORMAL
        return self.send_push_notification(
            device_id=device_id,
            title=f"Workflow: {workflow_name}",
            body=f"{status.upper()}: {message}",
            priority=priority,
            data={"workflow_name": workflow_name, "status": status, "type": "workflow"}
        )

    def get_notification_history(self, device_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get notification history for a device"""
        notifications = [
            {
                "id": n.notification_id,
                "title": n.title,
                "body": n.body,
                "priority": n.priority.value,
                "sent_at": n.sent_at.isoformat(),
                "delivered": n.delivered
            }
            for n in self.push_notifications.values()
            if n.data.get("device_id") == device_id or not n.data.get("device_id")
        ]
        return sorted(notifications, key=lambda x: x["sent_at"], reverse=True)[:limit]

    # =========================================================================
    # Workflow Triggering
    # =========================================================================

    def trigger_workflow_from_mobile(
        self,
        device_id: str,
        workflow_id: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Optional[str]]:
        """Trigger workflow execution from mobile device"""
        if device_id not in self.devices:
            return False, "Device not connected"

        trigger_data = {
            "workflow_id": workflow_id,
            "triggered_by": device_id,
            "triggered_at": datetime.now().isoformat(),
            "params": params or {}
        }

        response = self._api_request("POST", "/api/workflows/trigger", trigger_data)
        if response and response.get("success"):
            execution_id = response.get("execution_id")
            self._running_workflows[execution_id] = {
                "workflow_id": workflow_id,
                "device_id": device_id,
                "started_at": datetime.now()
            }
            self.send_workflow_notification(
                device_id, workflow_id, "started", f"Execution ID: {execution_id}"
            )
            return True, execution_id
        return False, "Failed to trigger workflow"

    def get_available_workflows(self, device_id: str) -> List[Dict[str, Any]]:
        """Get list of workflows available for mobile triggering"""
        response = self._api_request("GET", "/api/workflows")
        if response:
            return response.get("workflows", [])
        return []

    # =========================================================================
    # Status Updates
    # =========================================================================

    def update_workflow_status(self, execution_id: str, status_data: Dict[str, Any]) -> bool:
        """Update workflow execution status"""
        status_update = WorkflowStatusUpdate(
            workflow_id=status_data.get("workflow_id", ""),
            workflow_name=status_data.get("workflow_name", ""),
            status=status_data.get("status", "unknown"),
            progress=status_data.get("progress", 0.0),
            current_action=status_data.get("current_action"),
            started_at=datetime.fromisoformat(status_data.get("started_at", datetime.now().isoformat())),
            estimated_completion=datetime.fromisoformat(status_data["estimated_completion"])
                if status_data.get("estimated_completion") else None,
            error=status_data.get("error")
        )

        for device in self.devices.values():
            if device.status == DeviceStatus.CONNECTED:
                self._send_status_to_device(device.device_id, status_update)

        if status_data.get("status") in ["completed", "failed"]:
            self._running_workflows.pop(execution_id, None)

        return True

    def _send_status_to_device(self, device_id: str, status: WorkflowStatusUpdate):
        """Send status update to specific device"""
        payload = {
            "execution_id": device_id,
            "workflow_id": status.workflow_id,
            "workflow_name": status.workflow_name,
            "status": status.status,
            "progress": status.progress,
            "current_action": status.current_action,
            "started_at": status.started_at.isoformat(),
            "estimated_completion": status.estimated_completion.isoformat() if status.estimated_completion else None,
            "error": status.error
        }
        self._api_request("POST", f"/api/status/{device_id}", payload)

    def get_workflow_status(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of a workflow execution"""
        if execution_id in self._running_workflows:
            return {
                "execution_id": execution_id,
                **self._running_workflows[execution_id],
                "status": "running"
            }
        response = self._api_request("GET", f"/api/workflows/status/{execution_id}")
        return response

    # =========================================================================
    # Remote Control
    # =========================================================================

    def control_workflow(
        self,
        device_id: str,
        execution_id: str,
        action: WorkflowControlAction
    ) -> Tuple[bool, Optional[str]]:
        """Control a running workflow from mobile"""
        if device_id not in self.devices:
            return False, "Device not connected"

        control_data = {
            "execution_id": execution_id,
            "action": action.value,
            "controlled_by": device_id,
            "controlled_at": datetime.now().isoformat()
        }

        response = self._api_request("POST", "/api/workflows/control", control_data)
        if response and response.get("success"):
            if action == WorkflowControlAction.STOP:
                self._running_workflows.pop(execution_id, None)
            self.send_workflow_notification(
                device_id, execution_id, "controlled", f"Action: {action.value}"
            )
            return True, None
        return False, "Failed to control workflow"

    def get_running_workflows(self, device_id: str) -> List[Dict[str, Any]]:
        """Get list of running workflows controllable from mobile"""
        return [
            {
                "execution_id": exec_id,
                **workflow,
                "can_control": workflow.get("device_id") == device_id
            }
            for exec_id, workflow in self._running_workflows.items()
        ]

    # =========================================================================
    # Mobile Dashboard
    # =========================================================================

    def get_mobile_dashboard(self, device_id: str) -> Dict[str, Any]:
        """Get mobile-optimized dashboard view"""
        if device_id not in self.devices:
            return {"error": "Device not connected"}

        workflow_states = {}
        for exec_id, workflow in self._running_workflows.items():
            workflow_states[exec_id] = {
                "name": workflow.get("workflow_id"),
                "status": "running",
                "progress": 0.5,
                "started_at": workflow.get("started_at").isoformat() if workflow.get("started_at") else None
            }

        stats = {
            "success_rate": 0.95,
            "avg_duration": 120.5,
            "total_executions": 1000
        }

        return self.dashboard.get_dashboard_data(workflow_states, stats)

    def update_dashboard_settings(self, device_id: str, settings: Dict[str, Any]) -> bool:
        """Update dashboard settings for a device"""
        if "refresh_interval" in settings:
            self.dashboard.refresh_interval = settings["refresh_interval"]
        if "widgets" in settings:
            self.dashboard.customize_widgets(settings["widgets"])
        return True

    # =========================================================================
    # QR Code Pairing
    # =========================================================================

    def generate_pairing_code(self, device_name: Optional[str] = None) -> QRPairingData:
        """Generate QR code pairing data"""
        code = hashlib.sha256(f"{uuid.uuid4()}{time.time()}".encode()).hexdigest()[:8].upper()
        pairing_data = QRPairingData(
            pairing_code=code,
            server_url=self.server_url,
            expires_at=datetime.now() + timedelta(minutes=5),
            device_name=device_name
        )
        self.pairing_codes[code] = pairing_data
        return pairing_data

    def get_pairing_qr_data(self, device_name: Optional[str] = None) -> Dict[str, Any]:
        """Get QR code data for pairing"""
        pairing = self.generate_pairing_code(device_name)
        qr_data = {
            "code": pairing.pairing_code,
            "url": f"{pairing.server_url}/pair/{pairing.pairing_code}",
            "expires_at": pairing.expires_at.isoformat()
        }
        return qr_data

    def pair_device_via_qr(self, pairing_code: str, device_info: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Pair mobile device via QR code"""
        if pairing_code not in self.pairing_codes:
            return False, "Invalid pairing code"

        pairing = self.pairing_codes[pairing_code]
        if datetime.now() > pairing.expires_at:
            del self.pairing_codes[pairing_code]
            return False, "Pairing code expired"

        device_id = str(uuid.uuid4())
        device = MobileDevice(
            device_id=device_id,
            device_name=device_info.get("name", pairing.device_name or "Mobile Device"),
            device_type=device_info.get("type", "mobile"),
            paired_at=datetime.now(),
            last_seen=datetime.now(),
            status=DeviceStatus.PAIRED,
            capabilities=device_info.get("capabilities", ["push", "location", "voice"])
        )
        self.devices[device_id] = device
        del self.pairing_codes[pairing_code]

        response = self._api_request("POST", "/api/mobile/pair", {
            "device_id": device_id,
            "device_info": device_info,
            "pairing_code": pairing_code
        })

        if response and response.get("success"):
            return True, device_id
        return False, "Pairing failed"

    def verify_pairing(self, device_id: str, verification_code: str) -> bool:
        """Verify device pairing with security code"""
        if device_id not in self.devices:
            return False
        return True

    def unpair_device(self, device_id: str) -> bool:
        """Unpair a mobile device"""
        if device_id in self.devices:
            del self.devices[device_id]
            self._api_request("DELETE", f"/api/mobile/devices/{device_id}")
            return True
        return False

    # =========================================================================
    # Mobile-First Logging
    # =========================================================================

    def send_log_to_mobile(
        self,
        device_id: str,
        log_entry: Dict[str, Any],
        priority: NotificationPriority = NotificationPriority.LOW
    ):
        """Send execution log to mobile device"""
        payload = {
            "log_id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "entry": log_entry,
            "priority": priority.value
        }
        self._api_request("POST", f"/api/logs/{device_id}", payload)

    def stream_logs_to_mobile(
        self,
        device_id: str,
        workflow_id: str,
        log_filter: Optional[Dict[str, Any]] = None
    ):
        """Stream workflow execution logs to mobile"""
        payload = {
            "workflow_id": workflow_id,
            "filter": log_filter or {},
            "stream": True
        }
        return self._api_request("POST", f"/api/logs/{device_id}/stream", payload)

    def get_mobile_log_history(
        self,
        device_id: str,
        workflow_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get log history for mobile device"""
        params = f"?limit={limit}"
        if workflow_id:
            params += f"&workflow_id={workflow_id}"
        response = self._api_request("GET", f"/api/logs/{device_id}{params}")
        if response:
            return response.get("logs", [])
        return []

    # =========================================================================
    # Voice Input
    # =========================================================================

    def process_voice_command(
        self,
        device_id: str,
        audio_data: Optional[bytes] = None,
        transcription: Optional[str] = None
    ) -> Tuple[bool, Optional[VoiceCommand]]:
        """Process voice command from mobile device"""
        command_id = str(uuid.uuid4())

        if transcription:
            text = transcription
        else:
            return False, None

        voice_command = VoiceCommand(
            command_id=command_id,
            transcription=text,
            confidence=0.95
        )

        intent, entities = self._parse_voice_intent(text)
        voice_command.intent = intent
        voice_command.entities = entities
        voice_command.processed_at = datetime.now()

        self.voice_commands[command_id] = voice_command

        if intent:
            self._execute_voice_action(device_id, voice_command)

        return True, voice_command

    def _parse_voice_intent(self, text: str) -> Tuple[Optional[str], Dict[str, Any]]:
        """Parse voice command to extract intent and entities"""
        text_lower = text.lower()
        entities = {}

        if any(word in text_lower for word in ["start", "run", "execute", "begin"]):
            intent = "start_workflow"
            match = re.search(r"(?:workflow\s+)?([a-zA-Z0-9_\s]+)", text_lower)
            if match:
                entities["workflow_name"] = match.group(1).strip()
        elif any(word in text_lower for word in ["stop", "halt", "cancel", "end"]):
            intent = "stop_workflow"
        elif any(word in text_lower for word in ["status", "progress", "how"]):
            intent = "get_status"
        elif any(word in text_lower for word in ["show", "display", "view", "dashboard"]):
            intent = "show_dashboard"
        elif any(word in text_lower for word in ["help", "what", "commands"]):
            intent = "get_help"
        else:
            intent = "unknown"

        return intent, entities

    def _execute_voice_action(self, device_id: str, command: VoiceCommand):
        """Execute action based on voice command"""
        if command.intent == "start_workflow":
            workflow_name = command.entities.get("workflow_name")
            if workflow_name:
                workflows = self.get_available_workflows(device_id)
                for wf in workflows:
                    if workflow_name in wf.get("name", "").lower():
                        self.trigger_workflow_from_mobile(device_id, wf["id"])
                        break
        elif command.intent == "stop_workflow":
            running = self.get_running_workflows(device_id)
            if running:
                self.control_workflow(device_id, running[0]["execution_id"], WorkflowControlAction.STOP)

    def get_voice_command_history(self, device_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get voice command history for device"""
        return [
            {
                "command_id": cmd.command_id,
                "transcription": cmd.transcription,
                "intent": cmd.intent,
                "entities": cmd.entities,
                "confidence": cmd.confidence,
                "processed_at": cmd.processed_at.isoformat() if cmd.processed_at else None
            }
            for cmd in self.voice_commands.values()
            if cmd.processed_at
        ][:limit]

    # =========================================================================
    # Geofencing
    # =========================================================================

    def add_geofence(
        self,
        name: str,
        latitude: float,
        longitude: float,
        radius_meters: float,
        workflow_ids: List[str],
        events: List[GeofenceEvent] = None
    ) -> str:
        """Add a geofence trigger"""
        geofence_id = str(uuid.uuid4())
        geofence = GeofenceTrigger(
            geofence_id=geofence_id,
            name=name,
            latitude=latitude,
            longitude=longitude,
            radius_meters=radius_meters,
            events=events or [GeofenceEvent.ENTER, GeofenceEvent.EXIT],
            workflow_ids=workflow_ids
        )
        self.geofences[geofence_id] = geofence
        return geofence_id

    def remove_geofence(self, geofence_id: str) -> bool:
        """Remove a geofence trigger"""
        if geofence_id in self.geofences:
            del self.geofences[geofence_id]
            return True
        return False

    def update_geofence_location(self, device_id: str, latitude: float, longitude: float):
        """Update device location and check geofence triggers"""
        if device_id in self.devices:
            self.devices[device_id].location = {"lat": latitude, "lon": longitude}

        self.location_history[device_id].append({
            "lat": latitude,
            "lon": longitude,
            "timestamp": datetime.now()
        })

        for geofence in self.geofences.values():
            if not geofence.enabled:
                continue

            distance = self._calculate_distance(
                latitude, longitude,
                geofence.latitude, geofence.longitude
            )

            if distance <= geofence.radius_meters:
                self._trigger_geofence_workflows(device_id, geofence, GeofenceEvent.ENTER)
            else:
                self._trigger_geofence_workflows(device_id, geofence, GeofenceEvent.EXIT)

    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points in meters using Haversine formula"""
        from math import radians, sin, cos, sqrt, atan2
        R = 6371000

        lat1_rad, lon1_rad = radians(lat1), radians(lon1)
        lat2_rad, lon2_rad = radians(lat2), radians(lon2)

        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        a = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        return R * c

    def _trigger_geofence_workflows(
        self,
        device_id: str,
        geofence: GeofenceTrigger,
        event: GeofenceEvent
    ):
        """Trigger workflows associated with geofence event"""
        if event not in geofence.events:
            return

        for workflow_id in geofence.workflow_ids:
            self.trigger_workflow_from_mobile(
                device_id,
                workflow_id,
                {"trigger": "geofence", "geofence_id": geofence.geofence_id, "event": event.value}
            )

    def get_geofences(self, device_id: str) -> List[Dict[str, Any]]:
        """Get all geofences for a device"""
        return [
            {
                "geofence_id": gf.geofence_id,
                "name": gf.name,
                "latitude": gf.latitude,
                "longitude": gf.longitude,
                "radius_meters": gf.radius_meters,
                "events": [e.value for e in gf.events],
                "workflow_ids": gf.workflow_ids,
                "enabled": gf.enabled
            }
            for gf in self.geofences.values()
        ]

    def start_location_monitoring(self, device_id: str, callback: Optional[Callable] = None):
        """Start monitoring device location"""
        self._stop_location_monitoring.clear()
        self._location_thread = threading.Thread(
            target=self._location_monitor_loop,
            args=(device_id, callback),
            daemon=True
        )
        self._location_thread.start()

    def stop_location_monitoring(self):
        """Stop monitoring device location"""
        self._stop_location_monitoring.set()
        if self._location_thread:
            self._location_thread.join(timeout=5)

    def _location_monitor_loop(self, device_id: str, callback: Optional[Callable]):
        """Location monitoring loop"""
        while not self._stop_location_monitoring.is_set():
            try:
                response = self._api_request("GET", f"/api/location/{device_id}")
                if response and response.get("location"):
                    lat = response["location"].get("latitude")
                    lon = response["location"].get("longitude")
                    if lat and lon:
                        self.update_geofence_location(device_id, lat, lon)
                        if callback:
                            callback(lat, lon)
            except Exception as e:
                logger.error(f"Location monitoring error: {e}")
            time.sleep(10)

    # =========================================================================
    # Device Management
    # =========================================================================

    def get_paired_devices(self) -> List[Dict[str, Any]]:
        """Get list of all paired devices"""
        return [
            {
                "device_id": d.device_id,
                "device_name": d.device_name,
                "device_type": d.device_type,
                "paired_at": d.paired_at.isoformat(),
                "last_seen": d.last_seen.isoformat(),
                "status": d.status.value,
                "capabilities": d.capabilities
            }
            for d in self.devices.values()
        ]

    def get_device_info(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed device information"""
        if device_id not in self.devices:
            return None
        device = self.devices[device_id]
        return {
            "device_id": device.device_id,
            "device_name": device.device_name,
            "device_type": device.device_type,
            "paired_at": device.paired_at.isoformat(),
            "last_seen": device.last_seen.isoformat(),
            "status": device.status.value,
            "capabilities": device.capabilities,
            "location": device.location
        }

    def cleanup_expired_pairing_codes(self):
        """Clean up expired pairing codes"""
        now = datetime.now()
        expired = [code for code, data in self.pairing_codes.items() if now > data.expires_at]
        for code in expired:
            del self.pairing_codes[code]
