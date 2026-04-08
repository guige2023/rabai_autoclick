"""Email action module for RabAI AutoClick.

Provides email sending and retrieval actions via SMTP/IMAP protocols.
"""

import smtplib
import imaplib
import email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EmailSendAction(BaseAction):
    """Send email via SMTP.
    
    Supports plain text and HTML emails with attachments.
    """
    action_type = "email_send"
    display_name = "发送邮件"
    description = "通过SMTP发送电子邮件"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send email.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: smtp_host, smtp_port, username, password,
                   from_addr, to_addr, subject, body, html_body,
                   attachments, use_tls.
        
        Returns:
            ActionResult with send status.
        """
        smtp_host = params.get('smtp_host', 'smtp.gmail.com')
        smtp_port = params.get('smtp_port', 587)
        username = params.get('username', '')
        password = params.get('password', '')
        from_addr = params.get('from_addr', '')
        to_addr = params.get('to_addr', '')
        subject = params.get('subject', '')
        body = params.get('body', '')
        html_body = params.get('html_body', None)
        attachments = params.get('attachments', [])
        use_tls = params.get('use_tls', True)
        
        if not to_addr:
            return ActionResult(success=False, message="to_addr is required")
        
        if not body and not html_body:
            return ActionResult(success=False, message="body or html_body is required")
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = from_addr or username
            msg['To'] = to_addr
            msg['Subject'] = subject
            
            # Add body
            if body:
                msg.attach(MIMEText(body, 'plain'))
            if html_body:
                msg.attach(MIMEText(html_body, 'html'))
            
            # Add attachments
            for attachment in attachments:
                if not os.path.exists(attachment):
                    continue
                with open(attachment, 'rb') as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename={os.path.basename(attachment)}'
                )
                msg.attach(part)
            
            # Connect and send
            server = smtplib.SMTP(smtp_host, smtp_port)
            if use_tls:
                server.starttls()
            
            if username and password:
                server.login(username, password)
            
            server.send_message(msg)
            server.quit()
            
            return ActionResult(
                success=True,
                message=f"Email sent to {to_addr}",
                data={'to': to_addr, 'subject': subject}
            )
            
        except smtplib.SMTPAuthenticationError:
            return ActionResult(
                success=False,
                message="SMTP authentication failed"
            )
        except smtplib.SMTPException as e:
            return ActionResult(
                success=False,
                message=f"SMTP error: {e}",
                data={'error': str(e)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Email send error: {e}",
                data={'error': str(e)}
            )


class EmailCheckAction(BaseAction):
    """Check email inbox for new messages via IMAP.
    
    Retrieves email headers and basic information.
    """
    action_type = "email_check"
    display_name = "检查邮箱"
    description = "通过IMAP检查收件箱"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check email inbox.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: imap_host, imap_port, username, password,
                   mailbox, limit, unread_only.
        
        Returns:
            ActionResult with email list.
        """
        imap_host = params.get('imap_host', 'imap.gmail.com')
        imap_port = params.get('imap_port', 993)
        username = params.get('username', '')
        password = params.get('password', '')
        mailbox = params.get('mailbox', 'INBOX')
        limit = params.get('limit', 10)
        unread_only = params.get('unread_only', False)
        
        if not username or not password:
            return ActionResult(success=False, message="username and password required")
        
        try:
            # Connect to IMAP
            mail = imaplib.IMAP4_SSL(imap_host, imap_port)
            mail.login(username, password)
            mail.select(mailbox)
            
            # Search emails
            search_criteria = 'UNSEEN' if unread_only else 'ALL'
            status, message_ids = mail.search(None, search_criteria)
            
            if status != 'OK':
                return ActionResult(
                    success=False,
                    message=f"IMAP search failed: {status}"
                )
            
            ids_list = message_ids[0].split()
            emails = []
            
            # Get limited most recent
            for msg_id in ids_list[-limit:]:
                status, msg_data = mail.fetch(msg_id, '(RFC822)')
                
                if status != 'OK':
                    continue
                
                raw_email = msg_data[0][1]
                msg = email.message_from_bytes(raw_email)
                
                # Extract headers
                subject = msg['Subject'] or ''
                sender = msg['From'] or ''
                date = msg['Date'] or ''
                
                emails.append({
                    'id': msg_id.decode(),
                    'subject': subject,
                    'from': sender,
                    'date': date,
                    'has_attachments': len(msg._payload) > 0 if msg.is_multipart() else False
                })
            
            mail.logout()
            
            return ActionResult(
                success=True,
                message=f"Found {len(emails)} email(s)",
                data={'emails': emails, 'count': len(emails)}
            )
            
        except imaplib.IMAP4.error as e:
            return ActionResult(
                success=False,
                message=f"IMAP error: {e}",
                data={'error': str(e)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Email check error: {e}",
                data={'error': str(e)}
            )


class EmailReadAction(BaseAction):
    """Read full email content via IMAP.
    
    Retrieves email body, attachments metadata, and headers.
    """
    action_type = "email_read"
    display_name = "读取邮件"
    description = "读取完整邮件内容"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Read email.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: imap_host, imap_port, username, password,
                   mailbox, message_id.
        
        Returns:
            ActionResult with email content.
        """
        imap_host = params.get('imap_host', 'imap.gmail.com')
        imap_port = params.get('imap_port', 993)
        username = params.get('username', '')
        password = params.get('password', '')
        mailbox = params.get('mailbox', 'INBOX')
        message_id = params.get('message_id', '')
        
        if not username or not password:
            return ActionResult(success=False, message="username and password required")
        
        if not message_id:
            return ActionResult(success=False, message="message_id is required")
        
        try:
            mail = imaplib.IMAP4_SSL(imap_host, imap_port)
            mail.login(username, password)
            mail.select(mailbox)
            
            status, msg_data = mail.fetch(message_id.encode(), '(RFC822)')
            
            if status != 'OK':
                return ActionResult(
                    success=False,
                    message=f"IMAP fetch failed: {status}"
                )
            
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            
            # Extract body
            body_text = ''
            body_html = ''
            attachments = []
            
            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    if content_type == 'text/plain' and not body_text:
                        body_text = part.get_payload(decode=True).decode('utf-8', errors='replace')
                    elif content_type == 'text/html' and not body_html:
                        body_html = part.get_payload(decode=True).decode('utf-8', errors='replace')
                    elif part.get_filename():
                        attachments.append({
                            'filename': part.get_filename(),
                            'content_type': content_type
                        })
            else:
                body_text = msg.get_payload(decode=True).decode('utf-8', errors='replace')
            
            mail.logout()
            
            return ActionResult(
                success=True,
                message=f"Read email: {msg['Subject']}",
                data={
                    'subject': msg['Subject'],
                    'from': msg['From'],
                    'to': msg['To'],
                    'date': msg['Date'],
                    'body_text': body_text,
                    'body_html': body_html,
                    'attachments': attachments
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Email read error: {e}",
                data={'error': str(e)}
            )
