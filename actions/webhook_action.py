"""
Webhook sending and management actions.
"""
from __future__ import annotations

import requests
import hmac
import hashlib
import time
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse


def send_webhook(
    url: str,
    method: str = 'POST',
    data: Optional[Dict[str, Any]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
    verify_ssl: bool = True
) -> Dict[str, Any]:
    """
    Send a webhook request.

    Args:
        url: Webhook URL.
        method: HTTP method.
        data: Form data to send.
        json_data: JSON data to send.
        headers: Custom headers.
        timeout: Request timeout in seconds.
        verify_ssl: Verify SSL certificates.

    Returns:
        Response information.
    """
    merged_headers = headers or {}

    if json_data and 'Content-Type' not in merged_headers:
        merged_headers['Content-Type'] = 'application/json'

    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            data=data,
            json=json_data,
            headers=merged_headers,
            timeout=timeout,
            verify=verify_ssl
        )

        return {
            'success': response.ok,
            'status_code': response.status_code,
            'response': response.text[:1000] if response.text else None,
            'headers': dict(response.headers),
        }
    except requests.Timeout:
        return {
            'success': False,
            'error': 'Request timed out',
            'status_code': None,
        }
    except requests.RequestException as e:
        return {
            'success': False,
            'error': str(e),
            'status_code': None,
        }


def send_github_webhook(
    url: str,
    event: str,
    payload: Dict[str, Any],
    secret: Optional[str] = None,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Send a GitHub webhook.

    Args:
        url: GitHub webhook URL.
        event: Event type (e.g., 'push', 'pull_request').
        payload: Webhook payload.
        secret: Webhook secret for signature.
        timeout: Request timeout.

    Returns:
        Result information.
    """
    import json

    headers = {
        'Content-Type': 'application/json',
        'X-GitHub-Event': event,
        'X-GitHub-Delivery': f'{int(time.time() * 1000)}',
    }

    body = json.dumps(payload)

    if secret:
        signature = hmac.new(
            secret.encode('utf-8'),
            body.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        headers['X-Hub-Signature-256'] = f'sha256={signature}'

    try:
        response = requests.post(
            url,
            data=body,
            headers=headers,
            timeout=timeout
        )

        return {
            'success': response.ok,
            'status_code': response.status_code,
            'event': event,
        }
    except requests.RequestException as e:
        return {
            'success': False,
            'error': str(e),
        }


def send_slack_webhook(
    url: str,
    message: str,
    username: Optional[str] = None,
    icon_emoji: Optional[str] = None,
    channel: Optional[str] = None,
    blocks: Optional[List[Dict[str, Any]]] = None,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Send a Slack webhook.

    Args:
        url: Slack webhook URL.
        message: Message text.
        username: Bot username.
        icon_emoji: Bot icon emoji.
        channel: Channel override.
        blocks: Slack block kit blocks.
        timeout: Request timeout.

    Returns:
        Result information.
    """
    payload: Dict[str, Any] = {'text': message}

    if username:
        payload['username'] = username

    if icon_emoji:
        payload['icon_emoji'] = icon_emoji

    if channel:
        payload['channel'] = channel

    if blocks:
        payload['blocks'] = blocks

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=timeout
        )

        return {
            'success': response.ok,
            'status_code': response.status_code,
        }
    except requests.RequestException as e:
        return {
            'success': False,
            'error': str(e),
        }


def send_discord_webhook(
    url: str,
    content: str,
    username: Optional[str] = None,
    avatar_url: Optional[str] = None,
    embeds: Optional[List[Dict[str, Any]]] = None,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Send a Discord webhook.

    Args:
        url: Discord webhook URL.
        content: Message content.
        username: Override username.
        avatar_url: Override avatar URL.
        embeds: Discord embeds.
        timeout: Request timeout.

    Returns:
        Result information.
    """
    payload: Dict[str, Any] = {'content': content}

    if username:
        payload['username'] = username

    if avatar_url:
        payload['avatar_url'] = avatar_url

    if embeds:
        payload['embeds'] = embeds

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=timeout
        )

        return {
            'success': response.ok,
            'status_code': response.status_code,
        }
    except requests.RequestException as e:
        return {
            'success': False,
            'error': str(e),
        }


def send_teams_webhook(
    url: str,
    title: str,
    message: str,
    color: Optional[str] = None,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Send a Microsoft Teams webhook.

    Args:
        url: Teams webhook URL.
        title: Message title.
        message: Message content.
        color: Accent color (hex).
        timeout: Request timeout.

    Returns:
        Result information.
    """
    payload = {
        '@type': 'MessageCard',
        '@context': 'http://schema.org/extensions',
        'themeColor': color or '0078D4',
        'summary': title,
        'sections': [{
            'activityTitle': title,
            'activitySubtitle': '',
            'text': message,
        }]
    }

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=timeout,
            headers={'Content-Type': 'application/json'}
        )

        return {
            'success': response.ok,
            'status_code': response.status_code,
        }
    except requests.RequestException as e:
        return {
            'success': False,
            'error': str(e),
        }


def verify_webhook_signature(
    payload: bytes,
    signature: str,
    secret: str,
    algorithm: str = 'sha256'
) -> bool:
    """
    Verify webhook signature.

    Args:
        payload: Raw request payload.
        signature: Signature header value.
        secret: Webhook secret.
        algorithm: Hash algorithm.

    Returns:
        True if signature is valid.
    """
    if algorithm == 'sha256':
        expected = 'sha256=' + hmac.new(
            secret.encode('utf-8'),
            payload,
            hashlib.sha256
        ).hexdigest()
    elif algorithm == 'sha1':
        expected = 'sha1=' + hmac.new(
            secret.encode('utf-8'),
            payload,
            hashlib.sha1
        ).hexdigest()
    else:
        return False

    return hmac.compare_digest(expected, signature)


def create_webhook_signature(
    payload: bytes,
    secret: str,
    algorithm: str = 'sha256'
) -> str:
    """
    Create a webhook signature.

    Args:
        payload: Request payload.
        secret: Webhook secret.
        algorithm: Hash algorithm.

    Returns:
        Signature header value.
    """
    if algorithm == 'sha256':
        return 'sha256=' + hmac.new(
            secret.encode('utf-8'),
            payload,
            hashlib.sha256
        ).hexdigest()
    elif algorithm == 'sha1':
        return 'sha1=' + hmac.new(
            secret.encode('utf-8'),
            payload,
            hashlib.sha1
        ).hexdigest()
    else:
        raise ValueError(f"Unsupported algorithm: {algorithm}")


def parse_webhook_url(url: str) -> Dict[str, Any]:
    """
    Parse a webhook URL to extract components.

    Args:
        url: Webhook URL.

    Returns:
        URL components.
    """
    parsed = urlparse(url)

    return {
        'scheme': parsed.scheme,
        'host': parsed.netloc,
        'path': parsed.path,
        'query': dict(p.split('=') for p in parsed.query.split('&') if '=' in p) if parsed.query else {},
    }


def batch_webhook(urls: List[str], payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Send the same payload to multiple webhooks.

    Args:
        urls: List of webhook URLs.
        payload: Data to send.

    Returns:
        Summary of results.
    """
    results = []

    for url in urls:
        result = send_webhook(url, json_data=payload)
        results.append({
            'url': url,
            'success': result['success'],
            'status_code': result.get('status_code'),
        })

    return {
        'total': len(urls),
        'successful': sum(1 for r in results if r['success']),
        'failed': sum(1 for r in results if not r['success']),
        'results': results,
    }


def create_slack_block_message(
    header: str,
    body: str,
    footer: Optional[str] = None,
    image_url: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create a structured Slack message with blocks.

    Args:
        header: Header text.
        body: Body text.
        footer: Optional footer text.
        image_url: Optional image URL.

    Returns:
        Slack message payload.
    """
    blocks: List[Dict[str, Any]] = [
        {
            'type': 'header',
            'text': {
                'type': 'plain_text',
                'text': header,
                'emoji': True,
            }
        },
        {
            'type': 'section',
            'text': {
                'type': 'mrkdwn',
                'text': body,
            }
        },
    ]

    if image_url:
        blocks.append({
            'type': 'image',
            'image_url': image_url,
            'alt_text': header,
        })

    if footer:
        blocks.append({
            'type': 'context',
            'elements': [
                {
                    'type': 'mrkdwn',
                    'text': footer,
                }
            ]
        })

    return {
        'blocks': blocks,
        'text': header,
    }


def create_discord_embed(
    title: str,
    description: str,
    color: Optional[int] = None,
    url: Optional[str] = None,
    author_name: Optional[str] = None,
    author_url: Optional[str] = None,
    thumbnail_url: Optional[str] = None,
    image_url: Optional[str] = None,
    footer_text: Optional[str] = None,
    fields: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Create a Discord embed.

    Args:
        title: Embed title.
        description: Embed description.
        color: Color (decimal).
        url: Link title URL.
        author_name: Author name.
        author_url: Author URL.
        thumbnail_url: Thumbnail image URL.
        image_url: Main image URL.
        footer_text: Footer text.
        fields: List of field objects.

    Returns:
        Discord embed payload.
    """
    embed: Dict[str, Any] = {
        'title': title,
        'description': description,
    }

    if color:
        embed['color'] = color

    if url:
        embed['url'] = url

    if author_name:
        author = {'name': author_name}
        if author_url:
            author['url'] = author_url
        embed['author'] = author

    if thumbnail_url:
        embed['thumbnail'] = {'url': thumbnail_url}

    if image_url:
        embed['image'] = {'url': image_url}

    if footer_text:
        embed['footer'] = {'text': footer_text}

    if fields:
        embed['fields'] = [
            {
                'name': f.get('name', ''),
                'value': f.get('value', ''),
                'inline': f.get('inline', False),
            }
            for f in fields
        ]

    return embed
