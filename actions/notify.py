"""Notification action module for RabAI AutoClick.

Provides notification actions:
- NotifyAction: Desktop notification via macOS osascript
- EmailNotifyAction: Send email notification
- WebhookNotifyAction: HTTP POST to webhook URL
- SlackNotifyAction: Send to Slack webhook
"""

import subprocess
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Any, Dict, List, Optional
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

from rabai_autoclick.core.base_action import BaseAction, ActionResult


class NotifyAction(BaseAction):
    """Send desktop notification using macOS osascript."""
    action_type = "notify"
    display_name = "桌面通知"
    description = "发送桌面通知提醒"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a desktop notification.
        
        Args:
            context: Execution context.
            params: Dict with title, message, sound.
            
        Returns:
            ActionResult indicating success or failure.
        """
        title = params.get('title', 'RabAI AutoClick')
        message = params.get('message', '')
        sound = params.get('sound', True)
        
        valid, msg = self.validate_type(title, str, 'title')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            sound_arg = 'true' if sound else 'false'
            script = f'display notification "{message}" with title "{title}" sound name (do shell script "if [ {sound_arg} = true ]; then echo "Blow"; else echo " "; fi")'
            
            result = subprocess.run(
                ['osascript', '-e', f'display notification "{message}" with title "{title}"'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message=f"通知已发送: {title}"
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"通知发送失败: {result.stderr}"
                )
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="通知发送超时")
        except Exception as e:
            return ActionResult(success=False, message=f"通知发送失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['message']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'title': 'RabAI AutoClick',
            'sound': True
        }


class EmailNotifyAction(BaseAction):
    """Send email notification."""
    action_type = "email_notify"
    display_name = "邮件通知"
    description = "发送邮件通知"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute an email notification.
        
        Args:
            context: Execution context.
            params: Dict with to, subject, body, from_addr, smtp_server, smtp_port, username, password.
            
        Returns:
            ActionResult indicating success or failure.
        """
        to_addr = params.get('to')
        subject = params.get('subject', 'RabAI AutoClick Notification')
        body = params.get('body', '')
        from_addr = params.get('from_addr')
        smtp_server = params.get('smtp_server')
        smtp_port = params.get('smtp_port', 587)
        username = params.get('username')
        password = params.get('password')
        
        valid, msg = self.validate_type(to_addr, str, 'to')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(subject, str, 'subject')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(body, str, 'body')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            msg_obj = MIMEMultipart()
            msg_obj['From'] = from_addr or username
            msg_obj['To'] = to_addr
            msg_obj['Subject'] = subject
            msg_obj.attach(MIMEText(body, 'plain'))
            
            if smtp_server:
                server = smtplib.SMTP(smtp_server, smtp_port)
                server.starttls()
                if username and password:
                    server.login(username, password)
                server.send_message(msg_obj)
                server.quit()
            else:
                return ActionResult(success=False, message="SMTP服务器未配置")
            
            return ActionResult(
                success=True,
                message=f"邮件已发送至: {to_addr}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"邮件发送失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['to']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'subject': 'RabAI AutoClick Notification',
            'body': '',
            'smtp_server': None,
            'smtp_port': 587,
            'username': None,
            'password': None
        }


class WebhookNotifyAction(BaseAction):
    """Send HTTP POST to webhook URL."""
    action_type = "webhook_notify"
    display_name = "Webhook通知"
    description = "发送HTTP POST请求到Webhook URL"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a webhook notification.
        
        Args:
            context: Execution context.
            params: Dict with url, data, headers, content_type.
            
        Returns:
            ActionResult indicating success or failure.
        """
        url = params.get('url')
        data = params.get('data', {})
        headers = params.get('headers', {})
        content_type = params.get('content_type', 'application/json')
        
        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(data, dict, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            json_data = json.dumps(data).encode('utf-8')
            headers['Content-Type'] = content_type
            
            request = Request(url, data=json_data, headers=headers, method='POST')
            
            with urlopen(request, timeout=30) as response:
                response_body = response.read().decode('utf-8')
                response_code = response.getcode()
            
            return ActionResult(
                success=True,
                message=f"Webhook请求成功 (状态码: {response_code})",
                data={'response': response_body, 'status_code': response_code}
            )
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"Webhook请求失败: HTTP {e.code} - {e.reason}"
            )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"Webhook请求失败: {e.reason}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Webhook请求失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['url']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'data': {},
            'headers': {},
            'content_type': 'application/json'
        }


class SlackNotifyAction(BaseAction):
    """Send notification to Slack webhook."""
    action_type = "slack_notify"
    display_name = "Slack通知"
    description = "发送Slack消息通知"
    
    def execute(
        self, 
        context: Any, 
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a Slack notification.
        
        Args:
            context: Execution context.
            params: Dict with webhook_url, text, channel, username, icon_emoji.
            
        Returns:
            ActionResult indicating success or failure.
        """
        webhook_url = params.get('webhook_url')
        text = params.get('text', '')
        channel = params.get('channel')
        username = params.get('username', 'RabAI Bot')
        icon_emoji = params.get('icon_emoji', ':robot_face:')
        
        valid, msg = self.validate_type(webhook_url, str, 'webhook_url')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            payload = {
                'text': text,
                'username': username,
                'icon_emoji': icon_emoji
            }
            
            if channel:
                payload['channel'] = channel
            
            json_data = json.dumps(payload).encode('utf-8')
            headers = {'Content-Type': 'application/json'}
            
            request = Request(webhook_url, data=json_data, headers=headers, method='POST')
            
            with urlopen(request, timeout=30) as response:
                response_body = response.read().decode('utf-8')
                response_code = response.getcode()
            
            return ActionResult(
                success=True,
                message=f"Slack消息已发送",
                data={'status_code': response_code}
            )
        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"Slack发送失败: HTTP {e.code} - {e.reason}"
            )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"Slack发送失败: {e.reason}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Slack发送失败: {str(e)}")
    
    def get_required_params(self) -> List[str]:
        return ['webhook_url', 'text']
    
    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'channel': None,
            'username': 'RabAI Bot',
            'icon_emoji': ':robot_face:'
        }
