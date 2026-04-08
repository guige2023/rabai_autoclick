"""
Email utilities - parsing, validation, templating, MIME construction, encoding.
"""
from typing import Any, Dict, List, Optional, Tuple
import re
import logging
import base64
import quopri
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import policy
import hashlib

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _validate_email(email: str) -> Tuple[bool, str]:
    pattern = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    if re.match(pattern, email):
        return True, ""
    return False, "Invalid email format"


def _parse_email_address(raw: str) -> Dict[str, str]:
    match = re.match(r"^(.+?)\s*<(.+)>$", raw.strip())
    if match:
        return {"name": match.group(1).strip('" '), "address": match.group(2).strip()}
    return {"name": "", "address": raw.strip()}


def _encode_header(value: str, charset: str = "utf-8") -> str:
    try:
        h = Header(value, charset)
        return h.encode()
    except Exception:
        return value


def _build_email(
    from_addr: str, to_addr: str, subject: str, body: str,
    body_type: str = "plain",
    cc: Optional[List[str]] = None,
    bcc: Optional[List[str]] = None,
    attachments: Optional[List[Dict[str, str]]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> MIMEMultipart:
    msg = MIMEMultipart("mixed")
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    if cc:
        msg["Cc"] = ", ".join(cc)
    if headers:
        for k, v in headers.items():
            msg[k] = v
    body_part = MIMEText(body, body_type, "utf-8")
    msg.attach(body_part)
    if attachments:
        for att in attachments:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(att["data"])
            if att.get("encoding") == "base64":
                base64.encode(att["data"].encode(), att["data"])
            email.encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={att['filename']}")
            msg.attach(part)
    return msg


def _extract_domain(email_addr: str) -> str:
    if "@" in email_addr:
        return email_addr.split("@")[1]
    return ""


class EmailAction(BaseAction):
    """Email operations.

    Provides validation, parsing, MIME construction, templating, encoding.
    Note: Requires smtplib credentials for actual sending.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "validate")
        email = params.get("email", "")
        emails = params.get("emails", [])
        raw_email = params.get("raw_email", "")
        template = params.get("template", "")

        try:
            if operation == "validate":
                if not email:
                    return {"success": False, "error": "email required"}
                valid, error = _validate_email(email)
                return {"success": True, "valid": valid, "email": email, "error": error}

            elif operation == "validate_batch":
                if not emails:
                    return {"success": False, "error": "emails list required"}
                results = [{"email": e, "valid": _validate_email(e)[0]} for e in emails]
                valid_count = sum(1 for r in results if r["valid"])
                return {"success": True, "results": results, "valid": valid_count, "invalid": len(results) - valid_count}

            elif operation == "parse_address":
                parsed = _parse_email_address(raw_email or email)
                return {"success": True, **parsed}

            elif operation == "extract_domain":
                domain = _extract_domain(email)
                return {"success": True, "domain": domain, "email": email}

            elif operation == "build":
                from_addr = params.get("from", "")
                to_addr = params.get("to", "")
                subject = params.get("subject", "")
                body = params.get("body", "")
                body_type = params.get("body_type", "plain")
                if not all([from_addr, to_addr, subject, body]):
                    return {"success": False, "error": "from, to, subject, body required"}
                cc_list = params.get("cc", [])
                bcc_list = params.get("bcc", [])
                atts = params.get("attachments", [])
                msg = _build_email(from_addr, to_addr, subject, body, body_type, cc_list, bcc_list, atts)
                return {"success": True, "message": msg.as_string()}

            elif operation == "build_html":
                from_addr = params.get("from", "")
                to_addr = params.get("to", "")
                subject = params.get("subject", "")
                body = params.get("body", "")
                if not all([from_addr, to_addr, subject, body]):
                    return {"success": False, "error": "from, to, subject, body required"}
                msg = _build_email(from_addr, to_addr, subject, body, "html")
                return {"success": True, "message": msg.as_string()}

            elif operation == "render_template":
                variables = params.get("variables", {})
                if not template:
                    return {"success": False, "error": "template required"}
                result = template
                for key, value in variables.items():
                    result = result.replace(f"{{{{{key}}}}}", str(value))
                return {"success": True, "rendered": result}

            elif operation == "encode_header":
                value = params.get("value", "")
                charset = params.get("charset", "utf-8")
                encoded = _encode_header(value, charset)
                return {"success": True, "encoded": encoded}

            elif operation == "detect_bulk":
                pattern = params.get("pattern", "")
                if not emails:
                    return {"success": False, "error": "emails list required"}
                by_domain: Dict[str, List[str]] = {}
                for e in emails:
                    domain = _extract_domain(e)
                    if domain not in by_domain:
                        by_domain[domain] = []
                    by_domain[domain].append(e)
                bulk = {d: emails for d, emails in by_domain.items() if len(emails) > 1}
                return {"success": True, "bulk_domains": bulk, "total_unique_domains": len(by_domain)}

            elif operation == "anonymize":
                if not email:
                    return {"success": False, "error": "email required"}
                local, domain = email.split("@") if "@" in email else (email, "")
                masked_local = local[0] + "***" + local[-1] if len(local) > 2 else "***"
                return {"success": True, "anonymized": f"{masked_local}@{domain}"}

            elif operation == "generate unsubscribe link":
                list_id = params.get("list_id", "")
                email_addr = params.get("email", "")
                if not list_id or not email_addr:
                    return {"success": False, "error": "list_id and email required"}
                token = hashlib.sha1(f"{list_id}{email_addr}".encode()).hexdigest()[:16]
                base_url = params.get("base_url", "https://example.com/unsubscribe")
                return {"success": True, "link": f"{base_url}?list={list_id}&token={token}&email={email_addr}"}

            elif operation == "parse_headers":
                if not raw_email:
                    return {"success": False, "error": "raw_email required"}
                import email.policy
                msg = email.message_from_string(raw_email, policy=email.policy.default)
                headers = {k: v for k, v in msg.items()}
                return {"success": True, "headers": headers, "from": msg.get("From"), "to": msg.get("To"), "subject": msg.get("Subject")}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"EmailAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for email operations."""
    return EmailAction().execute(context, params)
