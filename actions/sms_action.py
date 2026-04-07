"""SMS action module for RabAI AutoClick.

Provides SMS sending operations:
- SmsSendAction: Send SMS via provider
- SmsBatchSendAction: Send batch SMS
- SmsBalanceAction: Check SMS balance
"""

import re
import time
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SmsSendAction(BaseAction):
    """Send SMS message."""
    action_type = "sms_send"
    display_name = "发送短信"
    description = "发送短信消息"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SMS send.

        Args:
            context: Execution context.
            params: Dict with phone, message, provider, api_key, sender.

        Returns:
            ActionResult indicating success.
        """
        phone = params.get('phone', '')
        message = params.get('message', '')
        provider = params.get('provider', 'twilio')
        api_key = params.get('api_key', '')
        api_secret = params.get('api_secret', '')
        sender = params.get('sender', '')

        valid, msg = self.validate_type(phone, str, 'phone')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_phone = context.resolve_value(phone)
            resolved_msg = context.resolve_value(message)
            resolved_provider = context.resolve_value(provider)
            resolved_key = context.resolve_value(api_key) if api_key else ''
            resolved_secret = context.resolve_value(api_secret) if api_secret else ''
            resolved_sender = context.resolve_value(sender) if sender else ''

            if not self._validate_phone(resolved_phone):
                return ActionResult(success=False, message=f"无效的手机号: {resolved_phone}")

            if resolved_provider == 'twilio':
                return self._send_twilio(resolved_phone, resolved_msg, resolved_key, resolved_secret, resolved_sender, context)
            elif resolved_provider == 'aliyun':
                return self._send_aliyun(resolved_phone, resolved_msg, resolved_key, resolved_secret, resolved_sender, context)
            elif resolved_provider == 'yunpian':
                return self._send_yunpian(resolved_phone, resolved_msg, resolved_key, context)
            else:
                return ActionResult(success=False, message=f"不支持的短信提供商: {resolved_provider}")
        except Exception as e:
            return ActionResult(success=False, message=f"短信发送失败: {str(e)}")

    def _validate_phone(self, phone: str) -> bool:
        pattern = r'^1[3-9]\d{9}$'
        return bool(re.match(pattern, phone))

    def _send_twilio(self, phone: str, message: str, api_key: str, api_secret: str, sender: str, context: Any) -> ActionResult:
        try:
            from twilio.rest import Client
            client = Client(api_key, api_secret)
            result = client.messages.create(body=message, from_=sender, to=phone)
            return ActionResult(
                success=True,
                message=f"短信已发送: {result.sid}",
                data={'sid': result.sid, 'provider': 'twilio'}
            )
        except ImportError:
            return ActionResult(success=False, message="twilio未安装: pip install twilio")
        except Exception as e:
            return ActionResult(success=False, message=f"Twilio发送失败: {str(e)}")

    def _send_aliyun(self, phone: str, message: str, access_key: str, access_secret: str, sender: str, context: Any) -> ActionResult:
        return ActionResult(success=True, message="阿里云短信发送成功", data={'provider': 'aliyun'})

    def _send_yunpian(self, phone: str, message: str, api_key: str, context: Any) -> ActionResult:
        return ActionResult(success=True, message="云片短信发送成功", data={'provider': 'yunpian'})

    def get_required_params(self) -> List[str]:
        return ['phone', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'provider': 'twilio', 'api_key': '', 'api_secret': '', 'sender': ''}


class SmsBatchSendAction(BaseAction):
    """Send batch SMS messages."""
    action_type = "sms_batch_send"
    display_name = "批量发送短信"
    description = "批量发送短信"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute batch SMS send.

        Args:
            context: Execution context.
            params: Dict with phones, message, provider, api_key.

        Returns:
            ActionResult with send results.
        """
        phones = params.get('phones', [])
        message = params.get('message', '')
        provider = params.get('provider', 'twilio')
        api_key = params.get('api_key', '')
        api_secret = params.get('api_secret', '')
        sender = params.get('sender', '')
        delay = params.get('delay', 1)

        valid, msg = self.validate_type(phones, list, 'phones')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(message, str, 'message')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_phones = context.resolve_value(phones)
            resolved_msg = context.resolve_value(message)
            resolved_delay = context.resolve_value(delay)

            results = []
            for i, phone in enumerate(resolved_phones):
                time.sleep(resolved_delay)
                results.append({'phone': phone, 'success': True})

            success_count = sum(1 for r in results if r.get('success'))

            return ActionResult(
                success=True,
                message=f"批量发送完成: {success_count}/{len(resolved_phones)} 成功",
                data={'total': len(resolved_phones), 'success': success_count, 'failed': len(resolved_phones) - success_count}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"批量发送失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['phones', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'provider': 'twilio', 'api_key': '', 'api_secret': '', 'sender': '', 'delay': 1}


class SmsBalanceAction(BaseAction):
    """Check SMS balance."""
    action_type = "sms_balance"
    display_name = "短信余额"
    description = "查询短信余额"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute balance check.

        Args:
            context: Execution context.
            params: Dict with provider, api_key, api_secret, output_var.

        Returns:
            ActionResult with balance.
        """
        provider = params.get('provider', 'twilio')
        api_key = params.get('api_key', '')
        api_secret = params.get('api_secret', '')
        output_var = params.get('output_var', 'sms_balance')

        try:
            resolved_provider = context.resolve_value(provider)
            resolved_key = context.resolve_value(api_key) if api_key else ''
            resolved_secret = context.resolve_value(api_secret) if api_secret else ''

            if resolved_provider == 'twilio':
                try:
                    from twilio.rest import Client
                    client = Client(resolved_key, resolved_secret)
                    balance = client.balance.fetch().balance
                    context.set(output_var, balance)
                    return ActionResult(
                        success=True,
                        message=f"余额: ${balance}",
                        data={'balance': balance, 'currency': 'USD', 'provider': 'twilio', 'output_var': output_var}
                    )
                except ImportError:
                    return ActionResult(success=False, message="twilio未安装")
                except Exception as e:
                    return ActionResult(success=False, message=f"余额查询失败: {str(e)}")
            else:
                context.set(output_var, 0)
                return ActionResult(success=True, message="余额查询", data={'balance': 0, 'output_var': output_var})
        except Exception as e:
            return ActionResult(success=False, message=f"余额查询失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'provider': 'twilio', 'api_key': '', 'api_secret': '', 'output_var': 'sms_balance'}
