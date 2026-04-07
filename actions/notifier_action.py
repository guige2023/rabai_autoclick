"""Notifier action module for RabAI AutoClick.

Provides notification operations:
- NotifyAction: Send system notification
- NotifyEmailAction: Send email notification
- NotifySlackAction: Send Slack notification
- NotifyDingtalkAction: Send DingTalk notification
- NotifyWecomAction: Send WeCom notification
- NotifyPushoverAction: Send Pushover notification
- NotifyIFTTTAction: Trigger IFTTT webhook
"""

import os
import subprocess
import json
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class NotifyAction(BaseAction):
    """Send system notification."""
    action_type = "notify"
    display_name = "发送通知"
    description = "发送系统通知"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute notification.

        Args:
            context: Execution context.
            params: Dict with title, message, sound.

        Returns:
            ActionResult indicating success.
        """
        title = params.get('title', 'RabAI AutoClick')
        message = params.get('message', '')
        sound = params.get('sound', 'default')

        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_title = context.resolve_value(title)
            resolved_message = context.resolve_value(message)
            resolved_sound = context.resolve_value(sound)

            # Use macOS notification
            if os.path.exists('/usr/bin/osascript'):
                cmd = [
                    'osascript', '-e',
                    f'display notification "{resolved_message}" with title "{resolved_title}"'
                ]
                if resolved_sound != 'none':
                    cmd[-1] += f' sound name "{resolved_sound}"'

                result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
                return ActionResult(
                    success=result.returncode == 0,
                    message=f"通知已发送: {resolved_title}",
                    data={'title': resolved_title, 'message': resolved_message}
                )
            else:
                return ActionResult(
                    success=False,
                    message="系统通知不可用"
                )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message="通知发送超时"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"发送通知失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'title': 'RabAI AutoClick', 'sound': 'default'}


class NotifySlackAction(BaseAction):
    """Send Slack notification."""
    action_type = "notify_slack"
    display_name = "Slack通知"
    description = "发送Slack消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Slack notification.

        Args:
            context: Execution context.
            params: Dict with webhook_url, text, channel, username, icon_emoji.

        Returns:
            ActionResult indicating success.
        """
        webhook_url = params.get('webhook_url', '')
        text = params.get('text', '')
        channel = params.get('channel', '')
        username = params.get('username', 'RabAI Bot')
        icon_emoji = params.get('icon_emoji', ':robot_face:')

        valid, msg = self.validate_type(webhook_url, str, 'webhook_url')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_url = context.resolve_value(webhook_url)
            resolved_text = context.resolve_value(text)
            resolved_channel = context.resolve_value(channel) if channel else None
            resolved_username = context.resolve_value(username)
            resolved_icon = context.resolve_value(icon_emoji)

            payload = {'text': resolved_text, 'username': resolved_username, 'icon_emoji': resolved_icon}
            if resolved_channel:
                payload['channel'] = resolved_channel

            encoded_body = json.dumps(payload).encode('utf-8')

            request = urllib.request.Request(
                resolved_url,
                data=encoded_body,
                method='POST'
            )
            request.add_header('Content-Type', 'application/json')

            with urllib.request.urlopen(request, timeout=10) as resp:
                status = resp.status

            return ActionResult(
                success=status in (200, 204),
                message=f"Slack消息已发送: {resolved_text[:50]}",
                data={'text': resolved_text, 'status': status}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Slack通知失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['webhook_url', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'channel': '', 'username': 'RabAI Bot', 'icon_emoji': ':robot_face:'}


class NotifyDingtalkAction(BaseAction):
    """Send DingTalk notification."""
    action_type = "notify_dingtalk"
    display_name = "钉钉通知"
    description = "发送钉钉消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute DingTalk notification.

        Args:
            context: Execution context.
            params: Dict with webhook_url, content, at_mobiles.

        Returns:
            ActionResult indicating success.
        """
        webhook_url = params.get('webhook_url', '')
        content = params.get('content', '')
        at_mobiles = params.get('at_mobiles', [])

        valid, msg = self.validate_type(webhook_url, str, 'webhook_url')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(content, str, 'content')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_url = context.resolve_value(webhook_url)
            resolved_content = context.resolve_value(content)
            resolved_phones = context.resolve_value(at_mobiles) if at_mobiles else []

            payload = {
                'msgtype': 'text',
                'text': {'content': resolved_content}
            }

            if resolved_phones:
                payload['at'] = {'atMobiles': resolved_phones}

            encoded_body = json.dumps(payload).encode('utf-8')

            request = urllib.request.Request(
                resolved_url,
                data=encoded_body,
                method='POST'
            )
            request.add_header('Content-Type', 'application/json')

            with urllib.request.urlopen(request, timeout=10) as resp:
                response_body = json.loads(resp.read().decode('utf-8'))

            if response_body.get('errcode') == 0:
                return ActionResult(
                    success=True,
                    message=f"钉钉消息已发送",
                    data={'content': resolved_content}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"钉钉错误: {response_body.get('errmsg', 'unknown')}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"钉钉通知失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['webhook_url', 'content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'at_mobiles': []}


class NotifyWecomAction(BaseAction):
    """Send WeCom notification."""
    action_type = "notify_wecom"
    display_name = "企业微信通知"
    description = "发送企业微信消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute WeCom notification.

        Args:
            context: Execution context.
            params: Dict with webhook_url, content.

        Returns:
            ActionResult indicating success.
        """
        webhook_url = params.get('webhook_url', '')
        content = params.get('content', '')

        valid, msg = self.validate_type(webhook_url, str, 'webhook_url')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(content, str, 'content')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_url = context.resolve_value(webhook_url)
            resolved_content = context.resolve_value(content)

            payload = {
                'msgtype': 'text',
                'text': {'content': resolved_content}
            }

            encoded_body = json.dumps(payload).encode('utf-8')

            request = urllib.request.Request(
                resolved_url,
                data=encoded_body,
                method='POST'
            )
            request.add_header('Content-Type', 'application/json')

            with urllib.request.urlopen(request, timeout=10) as resp:
                response_body = json.loads(resp.read().decode('utf-8'))

            if response_body.get('errcode') == 0:
                return ActionResult(
                    success=True,
                    message=f"企业微信消息已发送",
                    data={'content': resolved_content}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"企业微信错误: {response_body.get('errmsg', 'unknown')}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"企业微信通知失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['webhook_url', 'content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class NotifyPushoverAction(BaseAction):
    """Send Pushover notification."""
    action_type = "notify_pushover"
    display_name = "Pushover通知"
    description = "发送Pushover消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Pushover notification.

        Args:
            context: Execution context.
            params: Dict with user_key, token, message, title, priority, device.

        Returns:
            ActionResult indicating success.
        """
        user_key = params.get('user_key', '')
        token = params.get('token', '')
        message = params.get('message', '')
        title = params.get('title', 'RabAI')
        priority = params.get('priority', 0)
        device = params.get('device', '')

        valid, msg = self.validate_type(user_key, str, 'user_key')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(token, str, 'token')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request
            import urllib.parse

            resolved_user = context.resolve_value(user_key)
            resolved_token = context.resolve_value(token)
            resolved_message = context.resolve_value(message)
            resolved_title = context.resolve_value(title)
            resolved_priority = context.resolve_value(priority)

            data = {
                'user': resolved_user,
                'token': resolved_token,
                'message': resolved_message,
                'title': resolved_title,
                'priority': resolved_priority
            }

            if device:
                resolved_device = context.resolve_value(device)
                data['device'] = resolved_device

            encoded_data = urllib.parse.urlencode(data).encode('utf-8')

            request = urllib.request.Request(
                'https://api.pushover.net/1/messages.json',
                data=encoded_data,
                method='POST'
            )

            with urllib.request.urlopen(request, timeout=10) as resp:
                response_body = json.loads(resp.read().decode('utf-8'))

            if response_body.get('status') == 1:
                return ActionResult(
                    success=True,
                    message=f"Pushover消息已发送: {resolved_message[:50]}",
                    data={'message': resolved_message, 'receipt': response_body.get('receipt')}
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"Pushover错误: {response_body.get('errors', 'unknown')}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Pushover通知失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['user_key', 'token', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'title': 'RabAI', 'priority': 0, 'device': ''}


class NotifyIFTTTAction(BaseAction):
    """Trigger IFTTT webhook."""
    action_type = "notify_ifttt"
    display_name = "IFTTT通知"
    description = "触发IFTTT webhook"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute IFTTT trigger.

        Args:
            context: Execution context.
            params: Dict with event_name, key, value1, value2, value3.

        Returns:
            ActionResult indicating success.
        """
        event_name = params.get('event_name', '')
        key = params.get('key', '')
        value1 = params.get('value1', '')
        value2 = params.get('value2', '')
        value3 = params.get('value3', '')

        valid, msg = self.validate_type(event_name, str, 'event_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_event = context.resolve_value(event_name)
            resolved_key = context.resolve_value(key)

            url = f"https://maker.ifttt.com/trigger/{resolved_event}/with/key/{resolved_key}"

            data = {}
            if value1:
                data['value1'] = context.resolve_value(value1)
            if value2:
                data['value2'] = context.resolve_value(value2)
            if value3:
                data['value3'] = context.resolve_value(value3)

            if data:
                import urllib.parse
                encoded_data = urllib.parse.urlencode(data).encode('utf-8')
                request = urllib.request.Request(url, data=encoded_data, method='POST')
            else:
                request = urllib.request.Request(url, method='POST')

            with urllib.request.urlopen(request, timeout=10) as resp:
                status = resp.status

            return ActionResult(
                success=status in (200, 204),
                message=f"IFTTT已触发: {resolved_event}",
                data={'event': resolved_event, 'status': status}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"IFTTT触发失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['event_name', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value1': '', 'value2': '', 'value3': ''}
