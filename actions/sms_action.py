"""
SMS and notification utilities - sending, templating, rate limiting, delivery tracking.
"""
from typing import Any, Dict, List, Optional
import re
import logging
import hashlib
import time

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class SMSGateway:
    """Mock SMS gateway for development/testing."""

    def __init__(self) -> None:
        self.sent_messages: List[Dict[str, Any]] = []
        self._rate_limiter: Dict[str, List[float]] = {}

    def send(self, to: str, body: str, from_: Optional[str] = None) -> Dict[str, Any]:
        msg_id = hashlib.md5(f"{to}{body}{time.time()}".encode()).hexdigest()[:12]
        msg = {
            "id": msg_id,
            "to": to,
            "from": from_ or "rabai",
            "body": body,
            "status": "sent",
            "sent_at": time.time(),
        }
        self.sent_messages.append(msg)
        return msg

    def validate_phone(self, phone: str) -> bool:
        pattern = r"^\+?[1-9]\d{6,14}$"
        cleaned = re.sub(r"[\s\-\(\)]", "", phone)
        return bool(re.match(pattern, cleaned))


class NotificationChannel:
    """Unified notification channel (email, SMS, push)."""

    def __init__(self) -> None:
        self._templates: Dict[str, str] = {}
        self._history: List[Dict[str, Any]] = []

    def add_template(self, name: str, template: str) -> None:
        self._templates[name] = template

    def render_template(self, name: str, variables: Dict[str, Any]) -> Optional[str]:
        template = self._templates.get(name)
        if not template:
            return None
        result = template
        for key, value in variables.items():
            result = result.replace(f"{{{{{key}}}}}", str(value))
        return result

    def notify(
        self, channel: str, to: str, body: str,
        template_name: Optional[str] = None,
        variables: Optional[Dict[str, Any]] = None,
        priority: str = "normal"
    ) -> Dict[str, Any]:
        if template_name and variables:
            body = self.render_template(template_name, variables) or body
        notification = {
            "id": hashlib.md5(f"{to}{body}{time.time()}".encode()).hexdigest()[:12],
            "channel": channel,
            "to": to,
            "body": body,
            "priority": priority,
            "status": "delivered",
            "timestamp": time.time(),
        }
        self._history.append(notification)
        return notification

    def history(self, limit: int = 50) -> List[Dict[str, Any]]:
        return self._history[-limit:]


class SMSAction(BaseAction):
    """SMS and notification operations.

    Provides phone validation, templating, rate limiting, delivery simulation.
    Note: Requires Twilio/ClickSend/etc. credentials for actual SMS sending.
    """

    def __init__(self) -> None:
        self._gateway = SMSGateway()
        self._channel = NotificationChannel()
        self._rate_limits: Dict[str, List[float]] = {}

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "send")
        to = params.get("to", "")
        body = params.get("body", "")
        template_name = params.get("template")
        variables = params.get("variables", {})

        try:
            if operation == "validate_phone":
                valid = self._gateway.validate_phone(to)
                return {"success": True, "valid": valid, "phone": to}

            elif operation == "send":
                if not to or not body:
                    return {"success": False, "error": "to and body required"}
                result = self._gateway.send(to, body)
                return {"success": True, "id": result["id"], "status": result["status"], "to": to}

            elif operation == "send_templated":
                if not template_name:
                    return {"success": False, "error": "template name required"}
                rendered = self._channel.render_template(template_name, variables)
                if rendered is None:
                    return {"success": False, "error": f"Template not found: {template_name}"}
                result = self._gateway.send(to, rendered)
                return {"success": True, "id": result["id"], "body": rendered, "to": to}

            elif operation == "add_template":
                if not template_name:
                    return {"success": False, "error": "template name required"}
                content = params.get("content", body)
                self._channel.add_template(template_name, content)
                return {"success": True, "template": template_name}

            elif operation == "render_template":
                if not template_name:
                    return {"success": False, "error": "template name required"}
                rendered = self._channel.render_template(template_name, variables)
                if rendered is None:
                    return {"success": False, "error": f"Template not found: {template_name}"}
                return {"success": True, "rendered": rendered}

            elif operation == "notify":
                channel = params.get("channel", "email")
                result = self._channel.notify(channel, to, body, template_name, variables)
                return {"success": True, "id": result["id"], "channel": channel, "to": to}

            elif operation == "history":
                limit = int(params.get("limit", 50))
                msgs = self._gateway.sent_messages[-limit:]
                return {"success": True, "messages": msgs, "count": len(msgs)}

            elif operation == "rate_limit_check":
                window = int(params.get("window", 60))
                max_msgs = int(params.get("max_messages", 3))
                now = time.time()
                if to not in self._rate_limits:
                    self._rate_limits[to] = []
                self._rate_limits[to] = [t for t in self._rate_limits[to] if now - t < window]
                if len(self._rate_limits[to]) >= max_msgs:
                    retry_after = window - (now - self._rate_limits[to][0])
                    return {"success": True, "allowed": False, "retry_after": retry_after}
                self._rate_limits[to].append(now)
                return {"success": True, "allowed": True, "remaining": max_msgs - len(self._rate_limits[to])}

            elif operation == "batch_send":
                recipients = params.get("recipients", [])
                if not recipients:
                    return {"success": False, "error": "recipients list required"}
                results = []
                for r in recipients:
                    phone = r.get("to", "")
                    msg_body = r.get("body", body)
                    if phone and self._gateway.validate_phone(phone):
                        result = self._gateway.send(phone, msg_body)
                        results.append({"to": phone, "id": result["id"], "success": True})
                    else:
                        results.append({"to": phone, "success": False, "error": "Invalid phone"})
                return {"success": True, "results": results, "sent": sum(1 for r in results if r["success"])}

            elif operation == "format_phone":
                digits = re.sub(r"\D", "", to)
                country = params.get("country", "CN")
                if country == "CN" and len(digits) == 11:
                    return {"success": True, "formatted": f"+86{digits}"}
                elif country == "US" and len(digits) == 10:
                    return {"success": True, "formatted": f"+1{digits}"}
                return {"success": True, "formatted": f"+{digits}"}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"SMSAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for SMS operations."""
    return SMSAction().execute(context, params)
