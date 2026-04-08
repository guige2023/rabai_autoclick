"""Auto-trigger automation action module.

Provides event-driven task triggering based on file changes,
HTTP webhooks, message queues, and custom event sources.
"""

from __future__ import annotations

import time
import hashlib
import logging
import threading
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import os

logger = logging.getLogger(__name__)


class TriggerType(Enum):
    """Type of trigger."""
    FILE_CHANGE = "file_change"
    WEBHOOK = "webhook"
    MESSAGE_QUEUE = "message_queue"
    SCHEDULE = "schedule"
    CUSTOM = "custom"
    KEYWORD = "keyword"


@dataclass
class TriggerEvent:
    """An event that fires a trigger."""
    trigger_type: TriggerType
    source: str
    payload: Any
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Trigger:
    """A trigger that fires on specific events."""
    name: str
    trigger_type: TriggerType
    condition: Callable[[TriggerEvent], bool]
    action: Callable[[TriggerEvent], Any]
    enabled: bool = True
    cooldown_seconds: float = 0
    last_fired_at: float = 0
    fire_count: int = 0
    description: str = ""


class AutoTriggerAction:
    """Event-driven automation trigger engine.

    Monitors file changes, webhooks, message queues, and custom events
    to fire automated actions.

    Example:
        trigger = AutoTriggerAction()

        trigger.on_file_change("/tmp/*.json", handle_json_change)
        trigger.on_keyword("error", alert_team)
        trigger.on_webhook("/webhooks/deploy", deploy_service)

        trigger.start()
    """

    def __init__(self) -> None:
        """Initialize auto-trigger engine."""
        self._triggers: Dict[str, Trigger] = {}
        self._file_watchers: Dict[str, float] = {}
        self._running = threading.Event()
        self._lock = threading.Lock()
        self._webhook_server: Optional[threading.Thread] = None
        self._webhook_port: int = 8080

    def on_file_change(
        self,
        pattern: str,
        action: Callable[[TriggerEvent], Any],
        description: str = "",
    ) -> "AutoTriggerAction":
        """Register a file change trigger.

        Args:
            pattern: Glob pattern for files to watch (e.g., '/tmp/*.json').
            action: Callable to execute on change.
            description: Trigger description.

        Returns:
            Self for chaining.
        """
        import fnmatch
        trigger = Trigger(
            name=f"file_change:{pattern}",
            trigger_type=TriggerType.FILE_CHANGE,
            condition=lambda e: fnmatch.fnmatch(e.source, pattern),
            action=action,
            description=description,
        )
        self._triggers[trigger.name] = trigger
        return self

    def on_keyword(
        self,
        keyword: str,
        action: Callable[[TriggerEvent], Any],
        case_sensitive: bool = False,
        description: str = "",
    ) -> "AutoTriggerAction":
        """Register a keyword/pattern trigger.

        Args:
            keyword: Keyword or regex pattern to match.
            action: Callable to execute on match.
            case_sensitive: Whether match is case-sensitive.
            description: Trigger description.

        Returns:
            Self for chaining.
        """
        import re
        regex = re.compile(keyword, 0 if case_sensitive else re.IGNORECASE)

        def keyword_condition(event: TriggerEvent) -> bool:
            payload_str = str(event.payload)
            return bool(regex.search(payload_str))

        trigger = Trigger(
            name=f"keyword:{keyword}",
            trigger_type=TriggerType.KEYWORD,
            condition=keyword_condition,
            action=action,
            description=description,
        )
        self._triggers[trigger.name] = trigger
        return self

    def on_webhook(
        self,
        path: str,
        action: Callable[[TriggerEvent], Any],
        methods: tuple = ("POST",),
        description: str = "",
    ) -> "AutoTriggerAction":
        """Register a webhook trigger.

        Args:
            path: URL path to listen on (e.g., '/webhooks/deploy').
            action: Callable to execute on webhook.
            methods: Allowed HTTP methods.
            description: Trigger description.

        Returns:
            Self for chaining.
        """
        trigger = Trigger(
            name=f"webhook:{path}",
            trigger_type=TriggerType.WEBHOOK,
            condition=lambda e: e.source == path,
            action=action,
            description=description,
        )
        self._triggers[trigger.name] = trigger
        return self

    def on_custom_event(
        self,
        event_name: str,
        action: Callable[[TriggerEvent], Any],
        condition: Optional[Callable[[TriggerEvent], bool]] = None,
        cooldown_seconds: float = 0,
        description: str = "",
    ) -> "AutoTriggerAction":
        """Register a custom event trigger.

        Args:
            event_name: Name of the custom event.
            action: Callable to execute.
            condition: Optional additional condition.
            cooldown_seconds: Minimum time between fires.
            description: Trigger description.

        Returns:
            Self for chaining.
        """
        def custom_condition(event: TriggerEvent) -> bool:
            return event.source == event_name and (condition is None or condition(event))

        trigger = Trigger(
            name=f"custom:{event_name}",
            trigger_type=TriggerType.CUSTOM,
            condition=custom_condition,
            action=action,
            cooldown_seconds=cooldown_seconds,
            description=description,
        )
        self._triggers[trigger.name] = trigger
        return self

    def emit_event(
        self,
        trigger_type: TriggerType,
        source: str,
        payload: Any,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Emit an event to trigger matching handlers.

        Args:
            trigger_type: Type of event.
            source: Event source (path, name, etc.).
            payload: Event payload data.
            metadata: Optional metadata.

        Returns:
            Number of triggers fired.
        """
        event = TriggerEvent(
            trigger_type=trigger_type,
            source=source,
            payload=payload,
            metadata=metadata or {},
        )

        fired = 0
        for trigger in self._triggers.values():
            if not trigger.enabled:
                continue

            if not trigger.condition(event):
                continue

            if trigger.cooldown_seconds > 0:
                if time.time() - trigger.last_fired_at < trigger.cooldown_seconds:
                    logger.debug("Trigger '%s' in cooldown", trigger.name)
                    continue

            try:
                trigger.action(event)
                trigger.last_fired_at = time.time()
                trigger.fire_count += 1
                fired += 1
                logger.info("Trigger '%s' fired (count: %d)", trigger.name, trigger.fire_count)
            except Exception as e:
                logger.error("Trigger '%s' action failed: %s", trigger.name, e)

        return fired

    def check_file_changes(self) -> int:
        """Check for file changes on watched paths.

        Returns:
            Number of triggers fired.
        """
        fired = 0
        for trigger in self._triggers.values():
            if trigger.trigger_type != TriggerType.FILE_CHANGE:
                continue

            pattern = trigger.name.split(":", 1)[1]
            import fnmatch
            parts = pattern.rsplit("/", 1)
            dir_path = parts[0] if len(parts) > 1 else "."
            file_pattern = parts[-1]

            try:
                if not os.path.isdir(dir_path):
                    continue

                for filename in os.listdir(dir_path):
                    if not fnmatch.fnmatch(filename, file_pattern):
                        continue

                    filepath = os.path.join(dir_path, filename)
                    mtime = os.path.getmtime(filepath)

                    if filepath not in self._file_watchers:
                        self._file_watchers[filepath] = mtime
                        continue

                    if mtime > self._file_watchers[filepath]:
                        self._file_watchers[filepath] = mtime
                        content = ""
                        try:
                            with open(filepath, "r") as f:
                                content = f.read()
                        except:
                            pass

                        event = TriggerEvent(
                            trigger_type=TriggerType.FILE_CHANGE,
                            source=filepath,
                            payload=content,
                            metadata={"mtime": mtime, "size": os.path.getsize(filepath)},
                        )

                        try:
                            trigger.action(event)
                            trigger.last_fired_at = time.time()
                            trigger.fire_count += 1
                            fired += 1
                        except Exception as e:
                            logger.error("Trigger '%s' action failed: %s", trigger.name, e)

            except Exception as e:
                logger.error("File watch error for '%s': %s", trigger.name, e)

        return fired

    def start_webhook_server(self, port: int = 8080) -> None:
        """Start a simple webhook HTTP server.

        Args:
            port: Port to listen on.
        """
        import http.server
        import json
        import urllib.parse

        self._webhook_port = port

        class WebhookHandler(http.server.BaseHTTPRequestHandler):
            triggers_ref = self._triggers

            def do_GET(self):
                self._handle_request("GET")

            def do_POST(self):
                self._handle_request("POST")

            def do_PUT(self):
                self._handle_request("PUT")

            def do_DELETE(self):
                self._handle_request("DELETE")

            def _handle_request(self, method: str):
                parsed = urllib.parse.urlparse(self.path)
                path = parsed.path
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length).decode() if content_length > 0 else ""

                try:
                    payload = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    payload = body

                event = TriggerEvent(
                    trigger_type=TriggerType.WEBHOOK,
                    source=path,
                    payload=payload,
                    metadata={"method": method, "query": dict(urllib.parse.parse_qsl(parsed.query))},
                )

                for trigger in self.triggers_ref.values():
                    if trigger.trigger_type == TriggerType.WEBHOOK and trigger.enabled:
                        if trigger.condition(event):
                            try:
                                trigger.action(event)
                                trigger.fire_count += 1
                            except Exception as e:
                                logger.error("Webhook trigger '%s' failed: %s", trigger.name, e)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"status": "ok"}).encode())

            def log_message(self, format, *args):
                logger.debug(format, *args)

        server = http.server.HTTPServer(("0.0.0.0", port), WebhookHandler)
        self._webhook_server = threading.Thread(target=server.serve_forever, daemon=True)
        self._webhook_server.start()
        logger.info("Webhook server started on port %d", port)

    def enable(self, name: str) -> bool:
        """Enable a trigger by name."""
        trigger = self._triggers.get(name)
        if trigger:
            trigger.enabled = True
            return True
        return False

    def disable(self, name: str) -> bool:
        """Disable a trigger by name."""
        trigger = self._triggers.get(name)
        if trigger:
            trigger.enabled = False
            return True
        return False

    def remove(self, name: str) -> bool:
        """Remove a trigger by name."""
        if name in self._triggers:
            del self._triggers[name]
            return True
        return False

    def list_triggers(self) -> List[Dict[str, Any]]:
        """List all registered triggers."""
        return [
            {
                "name": t.name,
                "type": t.trigger_type.value,
                "enabled": t.enabled,
                "fire_count": t.fire_count,
                "last_fired_at": t.last_fired_at,
                "description": t.description,
            }
            for t in self._triggers.values()
        ]
