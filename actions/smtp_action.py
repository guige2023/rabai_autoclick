"""SMTP action module for RabAI AutoClick.

Provides email operations via SMTP including sending plain text and HTML emails,
attachments, and SMTP authentication.
"""

import os
import sys
import time
import smtplib
import email.mime.text
import email.mime.multipart
import email.mime.base
import email.encoders
import email.header
from email.mime.image import MIMEImage
from email.mime.audio import MIMEAudio
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SMTPClient:
    """SMTP client wrapper for sending emails.
    
    Provides methods for composing and sending emails with
    plain text, HTML, attachments, and authentication support.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 587,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_tls: bool = True,
        use_ssl: bool = False,
        timeout: int = 30
    ) -> None:
        """Initialize SMTP client.
        
        Args:
            host: SMTP server hostname.
            port: SMTP port (587 for TLS, 465 for SSL, 25 for plain).
            username: Optional authentication username.
            password: Optional authentication password.
            use_tls: Whether to use STARTTLS.
            use_ssl: Whether to use implicit SSL.
            timeout: Connection timeout in seconds.
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.use_ssl = use_ssl
        self.timeout = timeout
        self._conn: Optional[smtplib.SMTP] = None
        self._connected = False
    
    def connect(self) -> bool:
        """Establish connection to SMTP server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            if self.use_ssl:
                self._conn = smtplib.SMTP_SSL(
                    host=self.host,
                    port=self.port,
                    timeout=self.timeout
                )
            else:
                self._conn = smtplib.SMTP(
                    host=self.host,
                    port=self.port,
                    timeout=self.timeout
                )
            
            if self.use_tls and not self.use_ssl:
                self._conn.starttls()
            
            if self.username and self.password:
                self._conn.login(self.username, self.password)
            
            self._connected = True
            return True
        
        except Exception:
            self._connected = False
            return False
    
    def disconnect(self) -> bool:
        """Close the SMTP connection.
        
        Returns:
            True if disconnection successful.
        """
        if self._conn:
            try:
                self._conn.quit()
            except smtplib.SMTPServerDisconnected:
                pass
            except Exception:
                pass
            self._conn = None
        self._connected = False
        return True
    
    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        return self._connected
    
    def _build_message(
        self,
        subject: str,
        body: str,
        from_addr: str,
        to_addrs: Union[str, List[str]],
        cc_addrs: Optional[Union[str, List[str]]] = None,
        bcc_addrs: Optional[Union[str, List[str]]] = None,
        html_body: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> email.mime.multipart.MIMEMultipart:
        """Build a MIME multipart message.
        
        Args:
            subject: Email subject.
            body: Plain text body.
            from_addr: Sender email address.
            to_addrs: Recipient email address(es).
            cc_addrs: CC recipient email address(es).
            bcc_addrs: BCC recipient email address(es).
            html_body: Optional HTML body.
            headers: Optional custom headers.
            
        Returns:
            MIMEMultipart message object.
        """
        if isinstance(to_addrs, str):
            to_addrs = [to_addrs]
        if isinstance(cc_addrs, str):
            cc_addrs = [cc_addrs]
        if isinstance(bcc_addrs, str):
            bcc_addrs = [bcc_addrs]
        
        msg = email.mime.multipart.MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)
        
        if cc_addrs:
            msg["Cc"] = ", ".join(cc_addrs)
        
        if headers:
            for key, value in headers.items():
                msg[key] = value
        
        msg.attach(email.mime.text.MIMEText(body, "plain"))
        
        if html_body:
            msg.attach(email.mime.text.MIMEText(html_body, "html"))
        
        return msg
    
    def _add_attachments(
        self,
        msg: email.mime.multipart.MIMEMultipart,
        attachments: List[Dict[str, Any]]
    ) -> None:
        """Add attachments to a message.
        
        Args:
            msg: The message to attach to.
            attachments: List of attachment dictionaries.
        """
        for attachment in attachments:
            filepath = attachment.get("path")
            content = attachment.get("content")
            filename = attachment.get("filename")
            
            if filepath and os.path.exists(filepath):
                filename = filename or os.path.basename(filepath)
                path = filepath
            elif content and filename:
                path = None
            else:
                continue
            
            if path:
                _, ext = os.path.splitext(filename.lower())
                
                if ext in [".jpg", ".jpeg", ".png", ".gif", ".bmp"]:
                    with open(path, "rb") as f:
                        img_data = f.read()
                    part = MIMEImage(img_data, name=filename)
                elif ext in [".mp3", ".wav", ".ogg"]:
                    with open(path, "rb") as f:
                        audio_data = f.read()
                    part = MIMEAudio(audio_data, name=filename)
                else:
                    with open(path, "rb") as f:
                        part = email.mime.base.MIMEBase("application", "octet-stream")
                        part.set_payload(f.read())
                    email.encoders.encode_base64(part)
            else:
                part = email.mime.base.MIMEBase("application", "octet-stream")
                if isinstance(content, str):
                    content = content.encode("utf-8")
                part.set_payload(content)
                email.encoders.encode_base64(part)
            
            part.add_header(
                "Content-Disposition",
                "attachment",
                filename=filename
            )
            msg.attach(part)
    
    def send_email(
        self,
        subject: str,
        body: str,
        from_addr: str,
        to_addrs: Union[str, List[str]],
        cc_addrs: Optional[Union[str, List[str]]] = None,
        bcc_addrs: Optional[Union[str, List[str]]] = None,
        html_body: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Send an email.
        
        Args:
            subject: Email subject.
            body: Plain text body.
            from_addr: Sender email address.
            to_addrs: Recipient email address(es).
            cc_addrs: CC recipient email address(es).
            bcc_addrs: BCC recipient email address(es).
            html_body: Optional HTML body.
            attachments: Optional list of attachment dictionaries.
            headers: Optional custom headers.
            
        Returns:
            Send result dictionary.
        """
        if not self._conn or not self._connected:
            raise RuntimeError("Not connected to SMTP server")
        
        if isinstance(to_addrs, str):
            to_addrs = [to_addrs]
        if isinstance(cc_addrs, str):
            cc_addrs = [cc_addrs]
        if isinstance(bcc_addrs, str):
            bcc_addrs = [bcc_addrs]
        
        msg = self._build_message(
            subject=subject,
            body=body,
            from_addr=from_addr,
            to_addrs=to_addrs,
            cc_addrs=cc_addrs,
            bcc_addrs=bcc_addrs,
            html_body=html_body,
            headers=headers
        )
        
        if attachments:
            self._add_attachments(msg, attachments)
        
        all_recipients = list(to_addrs)
        if cc_addrs:
            all_recipients.extend(cc_addrs)
        if bcc_addrs:
            all_recipients.extend(bcc_addrs)
        
        try:
            self._conn.sendmail(from_addr, all_recipients, msg.as_string())
            return {
                "success": True,
                "recipients": len(all_recipients),
                "subject": subject
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }


class SMTPAction(BaseAction):
    """SMTP action for sending emails.
    
    Supports SMTP with TLS/SSL, authentication, HTML, and attachments.
    """
    action_type: str = "smtp"
    display_name: str = "SMTP邮件动作"
    description: str = "通过SMTP发送电子邮件，支持TLS/SSL认证、HTML和附件"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[SMTPClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SMTP operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                return self._disconnect(start_time)
            elif operation == "send":
                return self._send_email(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SMTP operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to SMTP server."""
        host = params.get("host", "localhost")
        port = params.get("port", 587)
        username = params.get("username")
        password = params.get("password")
        use_tls = params.get("use_tls", True)
        use_ssl = params.get("use_ssl", False)
        
        self._client = SMTPClient(
            host=host,
            port=port,
            username=username,
            password=password,
            use_tls=use_tls,
            use_ssl=use_ssl
        )
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to {host}:{port}" if success else f"Failed to connect to {host}:{port}",
            data={"host": host, "port": port},
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from SMTP server."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from SMTP server",
            duration=time.time() - start_time
        )
    
    def _send_email(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send an email."""
        if not self._client or not self._client.is_connected:
            return ActionResult(
                success=False,
                message="Not connected to SMTP server",
                duration=time.time() - start_time
            )
        
        subject = params.get("subject", "")
        body = params.get("body", "")
        from_addr = params.get("from", "")
        to_addrs = params.get("to", "")
        cc_addrs = params.get("cc")
        bcc_addrs = params.get("bcc")
        html_body = params.get("html_body")
        attachments = params.get("attachments")
        headers = params.get("headers")
        
        if not subject or not body or not from_addr or not to_addrs:
            return ActionResult(
                success=False,
                message="subject, body, from, and to are required",
                duration=time.time() - start_time
            )
        
        result = self._client.send_email(
            subject=subject,
            body=body,
            from_addr=from_addr,
            to_addrs=to_addrs,
            cc_addrs=cc_addrs,
            bcc_addrs=bcc_addrs,
            html_body=html_body,
            attachments=attachments,
            headers=headers
        )
        
        return ActionResult(
            success=result.get("success", False),
            message=f"Sent email: {subject}" if result.get("success") else f"Failed: {result.get('error')}",
            data=result,
            duration=time.time() - start_time
        )
