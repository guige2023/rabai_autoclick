"""Email utilities: composing, sending, templating, and batch email operations."""

from __future__ import annotations

import smtplib
from dataclasses import dataclass, field
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

__all__ = [
    "EmailConfig",
    "Email",
    "EmailClient",
    "EmailTemplate",
    "render_template",
]


@dataclass
class EmailConfig:
    """Email client configuration."""

    smtp_host: str = "localhost"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    from_address: str = ""
    use_tls: bool = True
    use_ssl: bool = False


@dataclass
class Email:
    """Represents an email message."""

    to: list[str]
    subject: str = ""
    body: str = ""
    html_body: str = ""
    cc: list[str] = field(default_factory=list)
    bcc: list[str] = field(default_factory=list)
    attachments: list[tuple[str, bytes]] = field(default_factory=list)
    headers: dict[str, str] = field(default_factory=dict)


class EmailTemplate:
    """Email template with variable substitution."""

    def __init__(self, template: str) -> None:
        self.template = template

    def render(self, **kwargs: str) -> str:
        """Render template with variables."""
        result = self.template
        for key, value in kwargs.items():
            placeholder = "{{" + key + "}}"
            result = result.replace(placeholder, str(value))
        return result


class EmailClient:
    """Email client for sending messages."""

    def __init__(self, config: EmailConfig) -> None:
        self.config = config

    def send(self, email: Email) -> bool:
        """Send an email message."""
        msg = MIMEMultipart("alternative")
        msg["Subject"] = email.subject
        msg["From"] = self.config.from_address
        msg["To"] = ", ".join(email.to)
        if email.cc:
            msg["Cc"] = ", ".join(email.cc)

        for key, value in email.headers.items():
            msg[key] = value

        if email.body:
            msg.attach(MIMEText(email.body, "plain"))
        if email.html_body:
            msg.attach(MIMEText(email.html_body, "html"))

        for filename, data in email.attachments:
            from email.mime.base import MIMEBase
            import email.encoders
            part = MIMEBase("application", "octet-stream")
            part.set_payload(data)
            email.encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={filename}")
            msg.attach(part)

        recipients = email.to + email.cc + email.bcc

        try:
            if self.config.use_ssl:
                server = smtplib.SMTP_SSL(self.config.smtp_host, self.config.smtp_port)
            else:
                server = smtplib.SMTP(self.config.smtp_host, self.config.smtp_port)
                if self.config.use_tls:
                    server.starttls()
            if self.config.smtp_user and self.config.smtp_password:
                server.login(self.config.smtp_user, self.config.smtp_password)
            server.sendmail(self.config.from_address, recipients, msg.as_string())
            server.quit()
            return True
        except Exception:
            return False


def render_template(template: str, **kwargs: str) -> str:
    """Convenience function to render an email template."""
    return EmailTemplate(template).render(**kwargs)
