"""Email action module for RabAI AutoClick.

Provides email sending capabilities via SMTP with
HTML support, attachments, templates, and batch sending.
"""

import sys
import os
import smtplib
import json
import time
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Any, Dict, List, Optional, Union
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EmailAction(BaseAction):
    """Send emails via SMTP.
    
    Supports plain text, HTML, attachments,
    CC/BCC, templates, and batch sending.
    """
    action_type = "email"
    display_name = "发送邮件"
    description = "通过SMTP发送邮件，支持HTML和附件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Send an email.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - smtp_host: str, SMTP server hostname
                - smtp_port: int, SMTP port (default 587)
                - smtp_user: str, username
                - smtp_password: str, password
                - use_tls: bool, use TLS (default True)
                - from_addr: str, sender email
                - to_addrs: str or list, recipient(s)
                - cc_addrs: str or list, CC recipients
                - bcc_addrs: str or list, BCC recipients
                - subject: str, email subject
                - body: str, plain text body
                - html_body: str, HTML body
                - attachments: list of file paths
                - save_to_var: str
        
        Returns:
            ActionResult with send status.
        """
        smtp_host = params.get('smtp_host', '')
        smtp_port = params.get('smtp_port', 587)
        smtp_user = params.get('smtp_user', '')
        smtp_password = params.get('smtp_password', '')
        use_tls = params.get('use_tls', True)
        from_addr = params.get('from_addr', smtp_user)
        to_addrs = params.get('to_addrs', '')
        cc_addrs = params.get('cc_addrs', [])
        bcc_addrs = params.get('bcc_addrs', [])
        subject = params.get('subject', '')
        body = params.get('body', '')
        html_body = params.get('html_body', None)
        attachments = params.get('attachments', [])
        save_to_var = params.get('save_to_var', None)

        if not smtp_host:
            return ActionResult(success=False, message="smtp_host is required")
        if not to_addrs:
            return ActionResult(success=False, message="to_addrs is required")

        # Normalize addresses
        if isinstance(to_addrs, str):
            to_addrs = [a.strip() for a in to_addrs.split(',')]
        if isinstance(cc_addrs, str):
            cc_addrs = [a.strip() for a in cc_addrs.split(',')]
        if isinstance(bcc_addds, str):  # typo in original, keep for compat
            bcc_addrs = [a.strip() for a in bcc_addrs.split(',')]

        start_time = time.time()

        # Build message
        msg = MIMEMultipart('alternative')
        msg['From'] = from_addr
        msg['To'] = ', '.join(to_addrs)
        msg['Subject'] = subject
        if cc_addrs:
            msg['Cc'] = ', '.join(cc_addrs)
        if bcc_addrs:
            msg['Bcc'] = ', '.join(bcc_addrs)

        # Add body
        if body:
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
        if html_body:
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))

        # Add attachments
        for filepath in attachments:
            try:
                with open(filepath, 'rb') as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    filename = os.path.basename(filepath)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename="{filename}"'
                    )
                    msg.attach(part)
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Failed to attach {filepath}: {e}",
                    duration=time.time() - start_time
                )

        # Send
        all_recipients = to_addrs + cc_addrs + bcc_addrs
        try:
            server = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
            if use_tls:
                server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.sendmail(from_addr, all_recipients, msg.as_string())
            server.quit()
        except smtplib.SMTPAuthenticationError:
            return ActionResult(
                success=False,
                message="SMTP authentication failed",
                duration=time.time() - start_time
            )
        except smtplib.SMTPException as e:
            return ActionResult(
                success=False,
                message=f"SMTP error: {e}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Email send failed: {e}",
                duration=time.time() - start_time
            )

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = {
                'sent': len(all_recipients),
                'recipients': all_recipients,
                'subject': subject
            }

        return ActionResult(
            success=True,
            message=f"Email sent to {len(all_recipients)} recipient(s)",
            data={'sent': len(all_recipients), 'recipients': all_recipients},
            duration=time.time() - start_time
        )

    def get_required_params(self) -> List[str]:
        return ['smtp_host', 'to_addrs']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'smtp_port': 587,
            'smtp_user': '',
            'smtp_password': '',
            'use_tls': True,
            'from_addr': '',
            'cc_addrs': [],
            'bcc_addrs': [],
            'subject': '',
            'body': '',
            'html_body': None,
            'attachments': [],
            'save_to_var': None,
        }


class EmailBatchAction(BaseAction):
    """Send batch emails from a template.
    
    Renders a template for each recipient with
    personalized data and sends in batch mode.
    """
    action_type = "email_batch"
    display_name = "批量发送邮件"
    description = "批量发送邮件，支持模板渲染"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Send batch emails.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - smtp_host, smtp_port, smtp_user, smtp_password, use_tls
                - from_addr: str, sender
                - recipients: list of {name, email, data} dicts
                - subject_template: str, with {name} placeholders
                - body_template: str, template string
                - html_template: str, HTML template
                - delay_between: float, seconds between sends
                - stop_on_error: bool
                - save_to_var: str
        
        Returns:
            ActionResult with batch results.
        """
        smtp_host = params.get('smtp_host', '')
        smtp_port = params.get('smtp_port', 587)
        smtp_user = params.get('smtp_user', '')
        smtp_password = params.get('smtp_password', '')
        use_tls = params.get('use_tls', True)
        from_addr = params.get('from_addr', smtp_user)
        recipients = params.get('recipients', [])
        subject_template = params.get('subject_template', '')
        body_template = params.get('body_template', '')
        html_template = params.get('html_template', None)
        delay_between = params.get('delay_between', 1.0)
        stop_on_error = params.get('stop_on_error', False)
        save_to_var = params.get('save_to_var', None)

        if not smtp_host:
            return ActionResult(success=False, message="smtp_host is required")
        if not recipients:
            return ActionResult(success=False, message="No recipients specified")

        start_time = time.time()
        results = []
        sent_count = 0
        failed_count = 0

        email_action = EmailAction()

        for recipient in recipients:
            name = recipient.get('name', '')
            email = recipient.get('email', '')
            data = recipient.get('data', {})

            # Render templates
            subject = self._render(subject_template, name=name, **data)
            body = self._render(body_template, name=name, **data)
            html = None
            if html_template:
                html = self._render(html_template, name=name, **data)

            send_params = {
                'smtp_host': smtp_host,
                'smtp_port': smtp_port,
                'smtp_user': smtp_user,
                'smtp_password': smtp_password,
                'use_tls': use_tls,
                'from_addr': from_addr,
                'to_addrs': email,
                'subject': subject,
                'body': body,
                'html_body': html,
            }

            result = email_action.execute(context, send_params)
            results.append({'email': email, 'success': result.success, 'message': result.message})

            if result.success:
                sent_count += 1
            else:
                failed_count += 1
                if stop_on_error:
                    break

            if delay_between > 0 and result.success:
                time.sleep(delay_between)

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = {
                'sent': sent_count,
                'failed': failed_count,
                'total': len(recipients),
                'results': results
            }

        return ActionResult(
            success=failed_count == 0,
            message=f"Batch: {sent_count} sent, {failed_count} failed",
            data={'sent': sent_count, 'failed': failed_count, 'results': results},
            duration=time.time() - start_time
        )

    def _render(self, template: str, **kwargs) -> str:
        """Simple template rendering with {placeholder} syntax."""
        result = template
        for key, value in kwargs.items():
            result = result.replace(f'{{{key}}}', str(value))
        return result

    def get_required_params(self) -> List[str]:
        return ['smtp_host', 'recipients']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'smtp_port': 587,
            'smtp_user': '',
            'smtp_password': '',
            'use_tls': True,
            'from_addr': '',
            'subject_template': '',
            'body_template': '',
            'html_template': None,
            'delay_between': 1.0,
            'stop_on_error': False,
            'save_to_var': None,
        }
