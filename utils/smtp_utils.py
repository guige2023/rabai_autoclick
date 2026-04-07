"""
SMTP utilities for email sending and management.

Provides SMTP client, templated emails, batch sending,
attachment handling, and email queue management.
"""

from __future__ import annotations

import asyncio
import logging
import smtplib
from dataclasses import dataclass, field
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class EmailPriority(Enum):
    LOW = 5
    NORMAL = 3
    HIGH = 1


@dataclass
class EmailAddress:
    """Email address with optional name."""
    email: str
    name: Optional[str] = None

    def __str__(self) -> str:
        if self.name:
            return f"{self.name} <{self.email}>"
        return self.email


@dataclass
class Attachment:
    """Email attachment."""
    filename: str
    content: bytes
    content_type: str = "application/octet-stream"


@dataclass
class EmailMessage:
    """Email message definition."""
    subject: str
    body: str
    body_html: Optional[str] = None
    from_address: Optional[EmailAddress] = None
    to: list[EmailAddress] = field(default_factory=list)
    cc: list[EmailAddress] = field(default_factory=list)
    bcc: list[EmailAddress] = field(default_factory=list)
    attachments: list[Attachment] = field(default_factory=list)
    priority: EmailPriority = EmailPriority.NORMAL
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class SMTPConfig:
    """SMTP server configuration."""
    host: str = "localhost"
    port: int = 587
    username: str = ""
    password: str = ""
    use_tls: bool = True
    use_ssl: bool = False
    timeout: int = 30


class SMTPClient:
    """SMTP client for sending emails."""

    def __init__(self, config: Optional[SMTPConfig] = None) -> None:
        self.config = config or SMTPConfig()
        self._connection: Optional[smtplib.SMTP] = None

    def connect(self) -> bool:
        """Establish SMTP connection."""
        try:
            if self.config.use_ssl:
                self._connection = smtplib.SMTP_SSL(
                    self.config.host,
                    self.config.port,
                    timeout=self.config.timeout,
                )
            else:
                self._connection = smtplib.SMTP(
                    self.config.host,
                    self.config.port,
                    timeout=self.config.timeout,
                )

            self._connection.ehlo()
            if self.config.use_tls and not self.config.use_ssl:
                self._connection.starttls()
                self._connection.ehlo()

            if self.config.username and self.config.password:
                self._connection.login(self.config.username, self.config.password)

            logger.info("SMTP connected to %s:%d", self.config.host, self.config.port)
            return True
        except Exception as e:
            logger.error("SMTP connection failed: %s", e)
            return False

    def disconnect(self) -> None:
        """Disconnect from SMTP server."""
        if self._connection:
            try:
                self._connection.quit()
            except Exception:
                pass
            self._connection = None

    def send(self, message: EmailMessage) -> bool:
        """Send an email message."""
        if not self._connection:
            if not self.connect():
                return False

        try:
            msg = self._build_message(message)
            recipients = [str(addr) for addr in message.to]
            self._connection.send_message(msg, str(message.from_address) if message.from_address else None, recipients)
            logger.info("Email sent: %s", message.subject)
            return True
        except Exception as e:
            logger.error("Failed to send email: %s", e)
            return False

    def _build_message(self, message: EmailMessage) -> MIMEMultipart:
        """Build MIME message from EmailMessage."""
        msg = MIMEMultipart("alternative")
        msg["Subject"] = message.subject

        if message.from_address:
            msg["From"] = str(message.from_address)

        msg["To"] = ", ".join(str(addr) for addr in message.to)

        if message.cc:
            msg["Cc"] = ", ".join(str(addr) for addr in message.cc)

        if message.priority != EmailPriority.NORMAL:
            priority_map = {EmailPriority.HIGH: "1", EmailPriority.LOW: "5"}
            msg["X-Priority"] = priority_map.get(message.priority, "3")

        for key, value in message.headers.items():
            msg[key] = value

        msg.attach(MIMEText(message.body, "plain"))
        if message.body_html:
            msg.attach(MIMEText(message.body_html, "html"))

        for attachment in message.attachments:
            part = MIMEApplication(attachment.content, Name=attachment.filename)
            part["Content-Disposition"] = f'attachment; filename="{attachment.filename}"'
            part["Content-Type"] = attachment.content_type
            msg.attach(part)

        return msg

    def __enter__(self) -> "SMTPClient":
        self.connect()
        return self

    def __exit__(self, *args: Any) -> None:
        self.disconnect()


class AsyncSMTPClient:
    """Async SMTP client for sending emails."""

    def __init__(self, config: Optional[SMTPConfig] = None) -> None:
        self.config = config or SMTPConfig()
        self._connection: Optional[smtplib.SMTP] = None

    async def connect(self) -> bool:
        """Establish async SMTP connection."""
        loop = asyncio.get_event_loop()
        try:
            if self.config.use_ssl:
                self._connection = await loop.run_in_executor(
                    None,
                    lambda: smtplib.SMTP_SSL(self.config.host, self.config.port, timeout=self.config.timeout),
                )
            else:
                self._connection = await loop.run_in_executor(
                    None,
                    lambda: smtplib.SMTP(self.config.host, self.config.port, timeout=self.config.timeout),
                )

            await loop.run_in_executor(None, self._connection.ehlo)
            if self.config.use_tls and not self.config.use_ssl:
                await loop.run_in_executor(None, self._connection.starttls)
                await loop.run_in_executor(None, self._connection.ehlo)

            if self.config.username and self.config.password:
                await loop.run_in_executor(
                    None,
                    lambda: self._connection.login(self.config.username, self.config.password),
                )
            return True
        except Exception as e:
            logger.error("Async SMTP connection failed: %s", e)
            return False

    async def send(self, message: EmailMessage) -> bool:
        """Send an email asynchronously."""
        if not self._connection:
            if not await self.connect():
                return False

        loop = asyncio.get_event_loop()
        msg = self._build_message(message)
        recipients = [str(addr) for addr in message.to]
        try:
            await loop.run_in_executor(
                None,
                lambda: self._connection.send_message(msg, str(message.from_address) if message.from_address else None, recipients),
            )
            return True
        except Exception as e:
            logger.error("Async email send failed: %s", e)
            return False

    def _build_message(self, message: EmailMessage) -> MIMEMultipart:
        from email.mime.application import MIMEApplication
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        msg = MIMEMultipart("alternative")
        msg["Subject"] = message.subject
        if message.from_address:
            msg["From"] = str(message.from_address)
        msg["To"] = ", ".join(str(addr) for addr in message.to)
        if message.cc:
            msg["Cc"] = ", ".join(str(addr) for addr in message.cc)

        msg.attach(MIMEText(message.body, "plain"))
        if message.body_html:
            msg.attach(MIMEText(message.body_html, "html"))
        for attachment in message.attachments:
            part = MIMEApplication(attachment.content, Name=attachment.filename)
            part["Content-Disposition"] = f'attachment; filename="{attachment.filename}"'
            msg.attach(part)
        return msg

    async def disconnect(self) -> None:
        """Disconnect from SMTP server."""
        if self._connection:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._connection.quit)
            self._connection = None
