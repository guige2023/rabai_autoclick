"""
SMTP email sending and management actions.
"""
from __future__ import annotations

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import Optional, List, Dict, Any


def send_email(
    to_address: str,
    subject: str,
    body: str,
    from_address: Optional[str] = None,
    smtp_host: str = 'localhost',
    smtp_port: int = 587,
    username: Optional[str] = None,
    password: Optional[str] = None,
    use_tls: bool = True,
    use_ssl: bool = False,
    html: bool = False,
    attachments: Optional[List[str]] = None,
    cc: Optional[List[str]] = None,
    bcc: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Send an email via SMTP.

    Args:
        to_address: Recipient email address.
        subject: Email subject.
        body: Email body.
        from_address: Sender email address.
        smtp_host: SMTP server hostname.
        smtp_port: SMTP port.
        username: SMTP username.
        password: SMTP password.
        use_tls: Use STARTTLS.
        use_ssl: Use SSL wrapper.
        html: Body is HTML.
        attachments: List of file paths to attach.
        cc: CC recipients.
        bcc: BCC recipients.

    Returns:
        Send result.
    """
    if not from_address:
        from_address = username

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_address
    msg['To'] = to_address

    if cc:
        msg['Cc'] = ', '.join(cc)

    if bcc:
        msg['Bcc'] = ', '.join(bcc)

    content_type = 'html' if html else 'plain'
    msg.attach(MIMEText(body, content_type))

    if attachments:
        for filepath in attachments:
            with open(filepath, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())

            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename="{filepath.split("/")[-1]}"'
            )
            msg.attach(part)

    recipients = [to_address]
    if cc:
        recipients.extend(cc)
    if bcc:
        recipients.extend(bcc)

    try:
        if use_ssl:
            context = ssl.create_default_context()
            server = smtplib.SMTP_SSL(smtp_host, smtp_port, context=context)
        else:
            server = smtplib.SMTP(smtp_host, smtp_port)

            if use_tls:
                server.starttls()

        if username and password:
            server.login(username, password)

        server.sendmail(from_address, recipients, msg.as_string())
        server.quit()

        return {
            'success': True,
            'to': to_address,
            'subject': subject,
        }
    except smtplib.SMTPException as e:
        return {
            'success': False,
            'error': str(e),
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
        }


def send_html_email(
    to_address: str,
    subject: str,
    html_body: str,
    from_address: Optional[str] = None,
    smtp_host: str = 'localhost',
    smtp_port: int = 587,
    username: Optional[str] = None,
    password: Optional[str] = None,
    plain_body: Optional[str] = None
) -> Dict[str, Any]:
    """
    Send an HTML email with plain text alternative.

    Args:
        to_address: Recipient.
        subject: Subject.
        html_body: HTML body.
        from_address: Sender.
        smtp_host: SMTP host.
        smtp_port: SMTP port.
        username: SMTP username.
        password: SMTP password.
        plain_body: Plain text alternative.

    Returns:
        Send result.
    """
    if not from_address:
        from_address = username

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_address
    msg['To'] = to_address

    if plain_body is None:
        import re
        plain_body = re.sub(r'<[^>]+>', '', html_body)

    msg.attach(MIMEText(plain_body, 'plain'))
    msg.attach(MIMEText(html_body, 'html'))

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(username, password)
        server.sendmail(from_address, [to_address], msg.as_string())
        server.quit()

        return {'success': True, 'to': to_address}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def create_email_html(
    title: str,
    heading: str,
    body: str,
    footer: Optional[str] = None,
    accent_color: str = '#0078D4'
) -> str:
    """
    Create a styled HTML email template.

    Args:
        title: Page title.
        heading: Main heading.
        body: Body content.
        footer: Footer text.
        accent_color: Accent color hex.

    Returns:
        HTML email template.
    """
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title}</title>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background-color: {accent_color}; color: white; padding: 20px; text-align: center; }}
            .content {{ padding: 20px; background-color: #f9f9f9; }}
            .footer {{ padding: 10px; text-align: center; font-size: 12px; color: #666; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>{heading}</h1>
        </div>
        <div class="content">
            {body}
        </div>
        <div class="footer">
            {footer or ''}
        </div>
    </body>
    </html>
    """
    return html


def batch_send_email(
    recipients: List[str],
    subject: str,
    body: str,
    smtp_host: str = 'localhost',
    smtp_port: int = 587,
    username: Optional[str] = None,
    password: Optional[str] = None,
    from_address: Optional[str] = None
) -> Dict[str, Any]:
    """
    Send the same email to multiple recipients.

    Args:
        recipients: List of email addresses.
        subject: Email subject.
        body: Email body.
        smtp_host: SMTP host.
        smtp_port: SMTP port.
        username: SMTP username.
        password: SMTP password.
        from_address: Sender address.

    Returns:
        Summary of results.
    """
    results = []

    for recipient in recipients:
        result = send_email(
            to_address=recipient,
            subject=subject,
            body=body,
            from_address=from_address,
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            username=username,
            password=password
        )
        results.append({
            'recipient': recipient,
            'success': result['success'],
        })

    return {
        'total': len(recipients),
        'successful': sum(1 for r in results if r['success']),
        'failed': sum(1 for r in results if not r['success']),
        'results': results,
    }


def validate_email(email: str) -> bool:
    """
    Validate an email address format.

    Args:
        email: Email address to validate.

    Returns:
        True if valid format.
    """
    import re

    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def parse_email_address(email: str) -> Dict[str, str]:
    """
    Parse an email address into name and address.

    Args:
        email: Email address (possibly with display name).

    Returns:
        Dictionary with 'name' and 'address'.
    """
    import re

    match = re.match(r'^(.+?) <(.+)>$', email)
    if match:
        return {
            'name': match.group(1).strip(),
            'address': match.group(2).strip(),
        }

    return {
        'name': '',
        'address': email.strip(),
    }


def create_mime_message(
    subject: str,
    from_address: str,
    to_address: str,
    body: str,
    body_type: str = 'plain',
    headers: Optional[Dict[str, str]] = None
) -> MIMEMultipart:
    """
    Create a MIMEMultipart message.

    Args:
        subject: Email subject.
        from_address: From address.
        to_address: To address.
        body: Message body.
        body_type: Content type ('plain' or 'html').
        headers: Additional headers.

    Returns:
        MIMEMultipart message.
    """
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_address
    msg['To'] = to_address

    if headers:
        for key, value in headers.items():
            msg[key] = value

    msg.attach(MIMEText(body, body_type))

    return msg


def test_smtp_connection(
    smtp_host: str,
    smtp_port: int = 587,
    username: Optional[str] = None,
    password: Optional[str] = None,
    use_tls: bool = True
) -> Dict[str, Any]:
    """
    Test SMTP server connectivity.

    Args:
        smtp_host: SMTP server hostname.
        smtp_port: SMTP port.
        username: SMTP username.
        password: SMTP password.
        use_tls: Use STARTTLS.

    Returns:
        Connection test result.
    """
    try:
        server = smtplib.SMTP(smtp_host, smtp_port, timeout=10)

        if use_tls:
            server.starttls()

        if username and password:
            server.login(username, password)

        server.quit()

        return {
            'success': True,
            'host': smtp_host,
            'port': smtp_port,
        }
    except smtplib.SMTPException as e:
        return {
            'success': False,
            'error': str(e),
            'host': smtp_host,
            'port': smtp_port,
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'host': smtp_host,
            'port': smtp_port,
        }
