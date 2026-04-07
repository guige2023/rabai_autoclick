"""SMTP utilities: email sending, templating, batch delivery, and attachment handling."""

from __future__ import annotations

import smtplib
from dataclasses import dataclass, field
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
import email.encoders
from typing import Any

__all__ = [
    "SMTPConfig",
    "SMTPClient",
    "send_email",
]


@dataclass
class SMTPConfig:
    """SMTP client configuration."""

    host: str = "localhost"
    port: int = 587
    username: str = ""
    password: str = ""
    from_addr: str = ""
    use_tls: bool = True
    use_ssl: bool = False


class SMTPClient:
    """SMTP client for sending emails."""

    def __init__(self, config: SMTPConfig) -> None:
        self.config = config

    def connect(self) -> smtplib.SMTP:
        """Connect to SMTP server."""
        if self.config.use_ssl:
            server = smtplib.SMTP_SSL(self.config.host, self.config.port)
        else:
            server = smtplib.SMTP(self.config.host, self.config.port)
            if self.config.use_tls:
                server.starttls()
        if self.config.username and self.config.password:
            server.login(self.config.username, self.config.password)
        return server

    def send(
        self,
        to: list[str],
        subject: str,
        body: str = "",
        html_body: str = "",
        cc: list[str] | None = None,
        bcc: list[str] | None = None,
        attachments: list[tuple[str, bytes]] | None = None,
    ) -> bool:
        """Send an email."""
        msg = MIMEMultipart("mixed")
        msg["From"] = self.config.from_addr
        msg["To"] = ", ".join(to)
        msg["Subject"] = subject

        if cc:
            msg["Cc"] = ", ".join(cc)
        if bcc:
            msg["Bcc"] = ", ".join(bcc)

        body_part = MIMEText(body, "plain") if body else MIMEText(html_body, "html")
        msg.attach(body_part)

        if html_body and body:
            html_part = MIMEText(html_body, "html")
            msg.attach(html_part)

        for filename, data in (attachments or []):
            content_type = self._guess_content_type(filename)
            main_type, sub_type = content_type.split("/")

            if main_type == "text":
                part = MIMEText(data.decode(), sub_type)
            elif main_type == "image":
                part = MIMEImage(data, _subtype=sub_type)
            elif main_type == "audio":
                part = MIMEAudio(data, _subtype=sub_type)
            else:
                part = MIMEBase(main_type, sub_type)
                part.set_payload(data)
                email.encoders.encode_base64(part)

            part.add_header("Content-Disposition", f"attachment; filename={filename}")
            msg.attach(part)

        recipients = to + (cc or []) + (bcc or [])

        try:
            server = self.connect()
            server.sendmail(self.config.from_addr, recipients, msg.as_string())
            server.quit()
            return True
        except Exception:
            return False

    @staticmethod
    def _guess_content_type(filename: str) -> str:
        import mimetypes
        return mimetypes.guess_type(filename)[0] or "application/octet-stream"


def send_email(
    config: SMTPConfig,
    to: list[str],
    subject: str,
    body: str = "",
    **kwargs,
) -> bool:
    """Convenience function to send an email."""
    client = SMTPClient(config)
    return client.send(to, subject, body, **kwargs)
