"""Anthropic API action module for RabAI AutoClick.

Provides Claude API operations via Anthropic's messages API.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AnthropicChatAction(BaseAction):
    """Execute Anthropic Claude chat completions via messages API.

    Supports Claude 3.5 Sonnet, Opus, Haiku, and related models.
    Handles streaming responses and multi-modal inputs.
    """
    action_type = "anthropic_chat"
    display_name = "Anthropic聊天"
    description = "调用Claude API生成回复"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Anthropic chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Anthropic API key
                - model: Model name (e.g. claude-3-5-sonnet-20241022)
                - messages: List of message dicts with role/content
                - max_tokens: Maximum tokens to generate
                - temperature: Sampling temperature
                - stream: Whether to stream
                - system: Optional system prompt

        Returns:
            ActionResult with chat response.
        """
        api_key = params.get('api_key') or os.environ.get('ANTHROPIC_API_KEY')
        model = params.get('model', 'claude-3-5-sonnet-20241022')
        messages = params.get('messages', [])
        max_tokens = params.get('max_tokens', 4096)
        temperature = params.get('temperature', 1.0)
        stream = params.get('stream', False)
        system = params.get('system', None)

        if not api_key:
            return ActionResult(success=False, message="ANTHROPIC_API_KEY is required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            import anthropic
        except ImportError:
            return ActionResult(success=False, message="anthropic package not installed. Run: pip install anthropic")

        client = anthropic.Anthropic(api_key=api_key)
        kwargs: Dict[str, Any] = {
            'model': model,
            'messages': messages,
            'max_tokens': max_tokens,
            'temperature': temperature,
        }
        if system:
            kwargs['system'] = system

        start = time.time()
        try:
            if stream:
                with client.messages.stream(**kwargs) as stream_resp:
                    full_content = ""
                    for event in stream_resp:
                        if event.type == 'content_block_delta' and hasattr(event, 'delta') and hasattr(event.delta, 'text'):
                            full_content += event.delta.text
                    duration = time.time() - start
                    return ActionResult(
                        success=True, message="Stream completed",
                        data={'content': full_content, 'model': model}, duration=duration
                    )
            else:
                response = client.messages.create(**kwargs)
                content_blocks = []
                for block in response.content:
                    if hasattr(block, 'text'):
                        content_blocks.append(block.text)
                content = "\n".join(content_blocks)
                usage = {
                    'input_tokens': response.usage.input_tokens,
                    'output_tokens': response.usage.output_tokens,
                }
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Anthropic chat completed",
                    data={'content': content, 'usage': usage, 'model': model,
                          'stop_reason': response.stop_reason},
                    duration=duration
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Anthropic API error: {str(e)}")


class AnthropicVisionAction(BaseAction):
    """Understand images via Anthropic Claude Vision API."""
    action_type = "anthropic_vision"
    display_name = "Anthropic视觉理解"
    description = "Claude Vision图像理解"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Analyze image with Claude.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Anthropic API key
                - model: Model name
                - image_source: Path or URL to image
                - prompt: Question about the image

        Returns:
            ActionResult with image understanding response.
        """
        api_key = params.get('api_key') or os.environ.get('ANTHROPIC_API_KEY')
        model = params.get('model', 'claude-3-5-sonnet-20241022')
        image_source = params.get('image_source', '')
        prompt = params.get('prompt', '描述这张图片')

        if not api_key:
            return ActionResult(success=False, message="ANTHROPIC_API_KEY is required")
        if not image_source:
            return ActionResult(success=False, message="image_source is required")

        try:
            import anthropic
            from PIL import Image
            import base64
        except ImportError as e:
            missing = 'anthropic' if 'anthropic' in str(e) else 'PIL'
            return ActionResult(success=False, message=f"{missing} package not installed")

        client = anthropic.Anthropic(api_key=api_key)

        # Load image
        if image_source.startswith('http://') or image_source.startswith('https://'):
            import urllib.request
            with urllib.request.urlopen(image_source, timeout=30) as resp:
                image_data = resp.read()
        elif os.path.exists(image_source):
            with open(image_source, 'rb') as f:
                image_data = f.read()
        else:
            return ActionResult(success=False, message=f"Cannot load image: {image_source}")

        # Encode to base64
        media_type = 'image/jpeg'
        if image_source.lower().endswith('.png'):
            media_type = 'image/png'
        elif image_source.lower().endswith(('.gif', '.webp')):
            media_type = f'image/{image_source.lower().split(".")[-1]}'

        image_b64 = base64.b64encode(image_data).decode('utf-8')

        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": image_b64}},
                    {"type": "text", "text": prompt}
                ]
            }
        ]

        start = time.time()
        try:
            response = client.messages.create(
                model=model, messages=messages, max_tokens=1024
            )
            content = "\n".join([b.text for b in response.content if hasattr(b, 'text')])
            duration = time.time() - start
            return ActionResult(
                success=True, message="Image understood",
                data={'content': content, 'model': model}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Vision error: {str(e)}")
