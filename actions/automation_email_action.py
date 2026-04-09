"""Automation Email Action Module.

Provides email automation with sending, receiving, filtering,
thread management, and attachment handling.
"""

import time
import threading
import hashlib
import sys
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EmailPriority(Enum):
    """Email priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


@dataclass
class Email:
    """Email message representation."""
    email_id: str
    from_address: str
    to_addresses: List[str]
    subject: str
    body: str
    priority: EmailPriority
    is_read: bool
    is_starred: bool
    received_at: float
    attachments: List[str]


class AutomationEmailAction(BaseAction):
    """Email Automation Action.

    Automates email operations including send, receive,
    filter, thread management, and attachment handling.
    """
    action_type = "automation_email"
    display_name = "邮件自动化"
    description = "邮件自动化：发送、接收、过滤、线程管理"

    _emails: Dict[str, Email] = {}
    _sent_count: int = 0
    _lock = threading.RLock()
    _default_sender: str = "auto@rabai.local"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute email operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'send', 'read', 'list', 'filter', 'mark_read',
                               'mark_starred', 'delete', 'search', 'get_threads'
                - to: str/list - recipient(s)
                - subject: str - email subject
                - body: str - email body
                - email_id: str (optional) - specific email ID
                - priority: str (optional) - low, normal, high, urgent
                - filters: dict (optional) - filter criteria
                - folder: str (optional) - inbox, sent, draft, trash

        Returns:
            ActionResult with email operation result.
        """
        start_time = time.time()
        operation = params.get('operation', 'list')

        try:
            with self._lock:
                if operation == 'send':
                    return self._send_email(params, start_time)
                elif operation == 'read':
                    return self._read_email(params, start_time)
                elif operation == 'list':
                    return self._list_emails(params, start_time)
                elif operation == 'filter':
                    return self._filter_emails(params, start_time)
                elif operation == 'mark_read':
                    return self._mark_read(params, start_time)
                elif operation == 'mark_starred':
                    return self._mark_starred(params, start_time)
                elif operation == 'delete':
                    return self._delete_email(params, start_time)
                elif operation == 'search':
                    return self._search_emails(params, start_time)
                elif operation == 'get_threads':
                    return self._get_threads(params, start_time)
                else:
                    return ActionResult(
                        success=False,
                        message=f"Unknown operation: {operation}",
                        duration=time.time() - start_time
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Email error: {str(e)}",
                duration=time.time() - start_time
            )

    def _send_email(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send an email."""
        to_addresses = params.get('to', [])
        if isinstance(to_addresses, str):
            to_addresses = [to_addresses]
        subject = params.get('subject', '')
        body = params.get('body', '')
        priority_str = params.get('priority', 'normal')
        attachments = params.get('attachments', [])

        try:
            priority = EmailPriority(priority_str.lower())
        except ValueError:
            priority = EmailPriority.NORMAL

        email_id = self._generate_email_id(subject, body)

        email = Email(
            email_id=email_id,
            from_address=self._default_sender,
            to_addresses=to_addresses,
            subject=subject,
            body=body,
            priority=priority,
            is_read=True,
            is_starred=False,
            received_at=time.time(),
            attachments=attachments
        )

        self._emails[email_id] = email
        self._sent_count += 1

        return ActionResult(
            success=True,
            message=f"Email sent to {len(to_addresses)} recipient(s)",
            data={
                'email_id': email_id,
                'to': to_addresses,
                'subject': subject,
                'priority': priority.value,
                'sent_at': email.received_at,
            },
            duration=time.time() - start_time
        )

    def _read_email(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Read a specific email."""
        email_id = params.get('email_id', '')

        if email_id not in self._emails:
            return ActionResult(success=False, message=f"Email {email_id} not found", duration=time.time() - start_time)

        email = self._emails[email_id]
        email.is_read = True

        return ActionResult(
            success=True,
            message=f"Read email: {email.subject}",
            data={
                'email_id': email.email_id,
                'from': email.from_address,
                'to': email.to_addresses,
                'subject': email.subject,
                'body': email.body,
                'priority': email.priority.value,
                'is_read': email.is_read,
                'is_starred': email.is_starred,
                'received_at': email.received_at,
                'attachments': email.attachments,
            },
            duration=time.time() - start_time
        )

    def _list_emails(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List emails with optional filters."""
        folder = params.get('folder', 'inbox')
        limit = params.get('limit', 50)
        offset = params.get('offset', 0)
        unread_only = params.get('unread_only', False)

        emails = sorted(self._emails.values(), key=lambda e: e.received_at, reverse=True)

        if unread_only:
            emails = [e for e in emails if not e.is_read]

        paginated = emails[offset:offset + limit]

        return ActionResult(
            success=True,
            message=f"Listed {len(paginated)} emails",
            data={
                'emails': [
                    {'email_id': e.email_id, 'from': e.from_address, 'subject': e.subject,
                     'is_read': e.is_read, 'is_starred': e.is_starred, 'priority': e.priority.value,
                     'received_at': e.received_at, 'has_attachments': len(e.attachments) > 0}
                    for e in paginated
                ],
                'total': len(emails),
                'returned': len(paginated),
            },
            duration=time.time() - start_time
        )

    def _filter_emails(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Filter emails by criteria."""
        filters = params.get('filters', {})
        limit = params.get('limit', 50)

        results = []
        for email in self._emails.values():
            match = True
            if 'from' in filters and filters['from'] not in email.from_address:
                match = False
            if 'subject_contains' in filters and filters['subject_contains'].lower() not in email.subject.lower():
                match = False
            if 'priority' in filters and email.priority.value != filters['priority']:
                match = False
            if 'is_starred' in filters and email.is_starred != filters['is_starred']:
                match = False
            if 'has_attachments' in filters:
                has_attachments = len(email.attachments) > 0
                if has_attachments != filters['has_attachments']:
                    match = False
            if match:
                results.append(email)

        results.sort(key=lambda e: e.received_at, reverse=True)
        paginated = results[:limit]

        return ActionResult(
            success=True,
            message=f"Found {len(results)} matching emails",
            data={'count': len(results), 'emails': [{'email_id': e.email_id, 'subject': e.subject, 'from': e.from_address} for e in paginated]},
            duration=time.time() - start_time
        )

    def _mark_read(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Mark email(s) as read."""
        email_id = params.get('email_id')

        if email_id:
            if email_id in self._emails:
                self._emails[email_id].is_read = True
                return ActionResult(success=True, message="Marked as read", data={'email_id': email_id}, duration=time.time() - start_time)
            return ActionResult(success=False, message="Email not found", duration=time.time() - start_time)

        unread = sum(1 for e in self._emails.values() if not e.is_read)
        for email in self._emails.values():
            email.is_read = True

        return ActionResult(success=True, message=f"Marked {unread} emails as read", data={'count': unread}, duration=time.time() - start_time)

    def _mark_starred(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Mark email(s) as starred."""
        email_id = params.get('email_id', '')
        starred = params.get('starred', True)

        if email_id in self._emails:
            self._emails[email_id].is_starred = starred
            return ActionResult(success=True, message="Starred status updated", data={'email_id': email_id, 'is_starred': starred}, duration=time.time() - start_time)

        return ActionResult(success=False, message="Email not found", duration=time.time() - start_time)

    def _delete_email(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete an email."""
        email_id = params.get('email_id', '')

        if email_id in self._emails:
            del self._emails[email_id]
            return ActionResult(success=True, message="Email deleted", data={'email_id': email_id}, duration=time.time() - start_time)

        return ActionResult(success=False, message="Email not found", duration=time.time() - start_time)

    def _search_emails(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Search emails by query."""
        query = params.get('query', '').lower()
        limit = params.get('limit', 50)

        results = []
        for email in self._emails.values():
            if (query in email.subject.lower() or query in email.body.lower() or
                query in email.from_address.lower() or any(query in to.lower() for to in email.to_addresses)):
                results.append(email)

        results.sort(key=lambda e: e.received_at, reverse=True)
        paginated = results[:limit]

        return ActionResult(
            success=True,
            message=f"Search found {len(results)} emails",
            data={'count': len(results), 'emails': [{'email_id': e.email_id, 'subject': e.subject, 'from': e.from_address} for e in paginated]},
            duration=time.time() - start_time
        )

    def _get_threads(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Group emails into threads."""
        subject_keywords = {}
        threads: Dict[str, List[Email]] = {}

        for email in self._emails.values():
            base_subject = email.subject
            if base_subject.startswith('Re:'):
                base_subject = base_subject[3:].strip()
            elif base_subject.startswith('Fwd:'):
                base_subject = base_subject[4:].strip()

            key = hashlib.md5(base_subject.lower().encode()).hexdigest()[:8]

            if key not in threads:
                threads[key] = []
            threads[key].append(email)

        thread_summaries = []
        for thread_id, emails in threads.items():
            emails.sort(key=lambda e: e.received_at)
            thread_summaries.append({
                'thread_id': thread_id,
                'subject': emails[0].subject,
                'message_count': len(emails),
                'participants': list(set(e.from_address for e in emails)),
                'last_message_at': emails[-1].received_at,
            })

        thread_summaries.sort(key=lambda t: t['last_message_at'], reverse=True)

        return ActionResult(
            success=True,
            message=f"Found {len(threads)} threads",
            data={'threads': thread_summaries, 'thread_count': len(threads)},
            duration=time.time() - start_time
        )

    def _generate_email_id(self, subject: str, body: str) -> str:
        """Generate a unique email ID."""
        content = f"{subject}:{body}:{time.time()}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
