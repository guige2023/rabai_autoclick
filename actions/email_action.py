"""Email action module for RabAI AutoClick.

Provides email operations:
- SendEmailAction: Send an email
- CheckEmailAction: Check for new emails (mock for testing)
"""

from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SendEmailAction(BaseAction):
    """Send an email."""
    action_type = "send_email"
    display_name = "发送邮件"
    description = "发送电子邮件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sending an email.

        Args:
            context: Execution context.
            params: Dict with to, subject, body, from_addr, smtp_server.

        Returns:
            ActionResult indicating success.
        """
        to_addr = params.get('to', '')
        subject = params.get('subject', '')
        body = params.get('body', '')
        from_addr = params.get('from', '')
        smtp_server = params.get('smtp_server', 'localhost')
        smtp_port = params.get('smtp_port', 25)

        # Validate to
        if not to_addr:
            return ActionResult(
                success=False,
                message="未指定收件人"
            )
        valid, msg = self.validate_type(to_addr, str, 'to')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate subject
        valid, msg = self.validate_type(subject, str, 'subject')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(body, str, 'body')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_to = context.resolve_value(to_addr)
            resolved_subject = context.resolve_value(subject)
            resolved_body = context.resolve_value(body)

            # For macOS, we can use the mail command
            import subprocess

            # Format the email content
            email_content = f"""To: {resolved_to}
Subject: {resolved_subject}

{resolved_body}"""

            # Use sendmail if available, otherwise use mail command
            try:
                # Try using mail command
                cmd = ['mail', '-s', resolved_subject, resolved_to]
                subprocess.run(
                    cmd,
                    input=resolved_body,
                    text=True,
                    capture_output=True,
                    timeout=30
                )
                return ActionResult(
                    success=True,
                    message=f"邮件已发送至: {resolved_to}",
                    data={
                        'to': resolved_to,
                        'subject': resolved_subject,
                        'sent': True
                    }
                )
            except FileNotFoundError:
                # mail command not available
                return ActionResult(
                    success=False,
                    message="邮件发送失败: mail命令不可用 (需要安装mailutils)"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"发送邮件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['to', 'subject', 'body']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'from': '',
            'smtp_server': 'localhost',
            'smtp_port': 25
        }


class CheckEmailAction(BaseAction):
    """Check for new emails (simplified mock for testing)."""
    action_type = "check_email"
    display_name = "检查邮件"
    description = "检查邮箱中的新邮件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute checking emails.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with email count.
        """
        output_var = params.get('output_var', 'email_count')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            # This is a mock implementation
            # In real usage, you would connect to an IMAP/POP3 server
            email_count = 0

            context.set(output_var, email_count)

            return ActionResult(
                success=True,
                message=f"检查完成: {email_count} 封新邮件",
                data={
                    'count': email_count,
                    'output_var': output_var,
                    'note': 'Mock implementation - configure IMAP for real usage'
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查邮件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'email_count'}