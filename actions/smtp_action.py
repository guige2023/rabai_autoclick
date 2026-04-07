"""SMTP email action module for RabAI AutoClick.

Provides email operations:
- SmtpSendAction: Send email via SMTP
- SmtpSendHtmlAction: Send HTML email
- SmtpSendAttachmentAction: Send email with attachment
- SmtpTestConnectionAction: Test SMTP connection
"""

from __future__ import annotations

import smtplib
import sys
import os
import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SmtpSendAction(BaseAction):
    """Send email via SMTP."""
    action_type = "smtp_send"
    display_name = "发送邮件"
    description = "通过SMTP发送邮件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SMTP send."""
        host = params.get('host', 'localhost')
        port = params.get('port', 25)
        username = params.get('username', '')
        password = params.get('password', '')
        use_tls = params.get('use_tls', True)
        from_addr = params.get('from_addr', '')
        to_addrs = params.get('to_addrs', '')
        subject = params.get('subject', '')
        body = params.get('body', '')
        output_var = params.get('output_var', 'smtp_result')

        if not to_addrs or not subject:
            return ActionResult(success=False, message="to_addrs and subject are required")

        try:
            resolved_host = context.resolve_value(host) if context else host
            resolved_port = context.resolve_value(port) if context else port
            resolved_username = context.resolve_value(username) if context else username
            resolved_password = context.resolve_value(password) if context else password
            resolved_from = context.resolve_value(from_addr) if context else from_addr
            resolved_to = context.resolve_value(to_addrs) if context else to_addrs
            resolved_subject = context.resolve_value(subject) if context else subject
            resolved_body = context.resolve_value(body) if context else body

            msg = MIMEMultipart()
            msg['From'] = resolved_from
            msg['To'] = resolved_to
            msg['Subject'] = resolved_subject
            msg.attach(MIMEText(resolved_body, 'plain'))

            with smtplib.SMTP(resolved_host, int(resolved_port), timeout=30) as server:
                if use_tls:
                    server.starttls()
                if resolved_username and resolved_password:
                    server.login(resolved_username, resolved_password)
                server.send_message(msg)

            result = {'sent': True, 'to': resolved_to, 'subject': resolved_subject}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Email sent to {resolved_to}", data=result)
        except smtplib.SMTPAuthenticationError:
            return ActionResult(success=False, message="SMTP authentication failed")
        except smtplib.SMTPException as e:
            return ActionResult(success=False, message=f"SMTP error: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"SMTP send error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['to_addrs', 'subject']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'host': 'localhost', 'port': 25, 'username': '', 'password': '',
            'use_tls': True, 'from_addr': '', 'body': '', 'output_var': 'smtp_result'
        }


class SmtpSendHtmlAction(BaseAction):
    """Send HTML email via SMTP."""
    action_type = "smtp_send_html"
    display_name = "发送HTML邮件"
    description = "通过SMTP发送HTML邮件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HTML email send."""
        host = params.get('host', 'localhost')
        port = params.get('port', 25)
        username = params.get('username', '')
        password = params.get('password', '')
        use_tls = params.get('use_tls', True)
        from_addr = params.get('from_addr', '')
        to_addrs = params.get('to_addrs', '')
        subject = params.get('subject', '')
        html_body = params.get('html_body', '')
        text_body = params.get('text_body', '')  # plain text alternative
        output_var = params.get('output_var', 'smtp_html_result')

        if not to_addrs or not subject:
            return ActionResult(success=False, message="to_addrs and subject are required")

        try:
            resolved_host = context.resolve_value(host) if context else host
            resolved_port = context.resolve_value(port) if context else port
            resolved_username = context.resolve_value(username) if context else username
            resolved_password = context.resolve_value(password) if context else password
            resolved_from = context.resolve_value(from_addr) if context else from_addr
            resolved_to = context.resolve_value(to_addrs) if context else to_addrs
            resolved_subject = context.resolve_value(subject) if context else subject
            resolved_html = context.resolve_value(html_body) if context else html_body
            resolved_text = context.resolve_value(text_body) if context else text_body

            msg = MIMEMultipart('alternative')
            msg['From'] = resolved_from
            msg['To'] = resolved_to
            msg['Subject'] = resolved_subject
            if resolved_text:
                msg.attach(MIMEText(resolved_text, 'plain'))
            msg.attach(MIMEText(resolved_html, 'html'))

            with smtplib.SMTP(resolved_host, int(resolved_port), timeout=30) as server:
                if use_tls:
                    server.starttls()
                if resolved_username and resolved_password:
                    server.login(resolved_username, resolved_password)
                server.send_message(msg)

            result = {'sent': True, 'to': resolved_to, 'subject': resolved_subject}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"HTML email sent to {resolved_to}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"HTML email error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['to_addrs', 'subject', 'html_body']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'host': 'localhost', 'port': 25, 'username': '', 'password': '',
            'use_tls': True, 'from_addr': '', 'text_body': '', 'output_var': 'smtp_html_result'
        }


class SmtpSendAttachmentAction(BaseAction):
    """Send email with attachment via SMTP."""
    action_type = "smtp_send_attachment"
    display_name = "发送带附件邮件"
    description = "发送带附件的邮件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute attachment email send."""
        host = params.get('host', 'localhost')
        port = params.get('port', 25)
        username = params.get('username', '')
        password = params.get('password', '')
        use_tls = params.get('use_tls', True)
        from_addr = params.get('from_addr', '')
        to_addrs = params.get('to_addrs', '')
        subject = params.get('subject', '')
        body = params.get('body', '')
        attachments = params.get('attachments', [])  # list of file paths
        output_var = params.get('output_var', 'smtp_attachment_result')

        if not to_addrs or not subject:
            return ActionResult(success=False, message="to_addrs and subject are required")

        try:
            resolved_host = context.resolve_value(host) if context else host
            resolved_port = context.resolve_value(port) if context else port
            resolved_username = context.resolve_value(username) if context else username
            resolved_password = context.resolve_value(password) if context else password
            resolved_from = context.resolve_value(from_addr) if context else from_addr
            resolved_to = context.resolve_value(to_addrs) if context else to_addrs
            resolved_subject = context.resolve_value(subject) if context else subject
            resolved_body = context.resolve_value(body) if context else body
            resolved_attachments = context.resolve_value(attachments) if context else attachments

            msg = MIMEMultipart()
            msg['From'] = resolved_from
            msg['To'] = resolved_to
            msg['Subject'] = resolved_subject
            msg.attach(MIMEText(resolved_body, 'plain'))

            for file_path in resolved_attachments:
                with open(file_path, 'rb') as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                filename = _os.path.basename(file_path)
                part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                msg.attach(part)

            with smtplib.SMTP(resolved_host, int(resolved_port), timeout=30) as server:
                if use_tls:
                    server.starttls()
                if resolved_username and resolved_password:
                    server.login(resolved_username, resolved_password)
                server.send_message(msg)

            result = {'sent': True, 'to': resolved_to, 'attachments': len(resolved_attachments)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Email with {len(resolved_attachments)} attachments sent", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Attachment email error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['to_addrs', 'subject']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'host': 'localhost', 'port': 25, 'username': '', 'password': '',
            'use_tls': True, 'from_addr': '', 'body': '', 'attachments': [], 'output_var': 'smtp_attachment_result'
        }


class SmtpTestConnectionAction(BaseAction):
    """Test SMTP connection."""
    action_type = "smtp_test_connection"
    display_name = "测试SMTP连接"
    description = "测试SMTP服务器连接"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SMTP test."""
        host = params.get('host', 'localhost')
        port = params.get('port', 25)
        username = params.get('username', '')
        password = params.get('password', '')
        use_tls = params.get('use_tls', True)
        output_var = params.get('output_var', 'smtp_test_result')

        try:
            resolved_host = context.resolve_value(host) if context else host
            resolved_port = context.resolve_value(port) if context else port
            resolved_username = context.resolve_value(username) if context else username
            resolved_password = context.resolve_value(password) if context else password

            with smtplib.SMTP(resolved_host, int(resolved_port), timeout=10) as server:
                if use_tls:
                    server.starttls()
                if resolved_username and resolved_password:
                    server.login(resolved_username, resolved_password)
                result = {'connected': True, 'host': resolved_host, 'port': resolved_port}
                if context:
                    context.set(output_var, result)
                return ActionResult(success=True, message=f"Connected to {resolved_host}:{resolved_port}", data=result)
        except smtplib.SMTPConnectError:
            return ActionResult(success=False, message=f"Could not connect to {resolved_host}:{resolved_port}")
        except smtplib.SMTPAuthenticationError:
            return ActionResult(success=False, message="Authentication failed")
        except Exception as e:
            return ActionResult(success=False, message=f"SMTP test error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 25, 'username': '', 'password': '', 'use_tls': True, 'output_var': 'smtp_test_result'}
