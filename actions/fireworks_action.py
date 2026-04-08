"""Fireworks AI API action module for RabAI AutoClick.

Provides high-performance LLM inference via Fireworks AI.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FireworksChatAction(BaseAction):
    """Execute Fireworks AI chat completions.

    Supports Llama, Mistral, and other models on Fireworks AI.
    """
    action_type = "fireworks_chat"
    display_name = "Fireworks聊天"
    description = "Fireworks AI高速LLM推理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Fireworks AI chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Fireworks API key
                - model: Model name (e.g. fireworks-llama-v2-70b)
                - messages: List of message dicts
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens
                - stream: Whether to stream

        Returns:
            ActionResult with chat response.
        """
        api_key = params.get('api_key') or os.environ.get('FIREWORKS_API_KEY')
        model = params.get('model', 'fireworks-llama-v2-70b')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', 2048)
        stream = params.get('stream', False)

        if not api_key:
            return ActionResult(success=False, message="FIREWORKS_API_KEY is required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            from openai import OpenAI
        except ImportError:
            return ActionResult(success=False, message="openai package not installed")

        client = OpenAI(api_key=api_key, base_url="https://api.fireworks.ai/inference/v1")
        start = time.time()
        try:
            kwargs: Dict[str, Any] = {
                'model': model,
                'messages': messages,
                'temperature': temperature,
                'max_tokens': max_tokens,
                'stream': stream,
            }
            response = client.chat.completions.create(**kwargs)
            if stream:
                full_content = ""
                for chunk in response:
                    if chunk.choices and chunk.choices[0].delta.content:
                        full_content += chunk.choices[0].delta.content
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Stream completed",
                    data={'content': full_content, 'model': model}, duration=duration
                )
            else:
                content = response.choices[0].message.content or ""
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Fireworks chat completed",
                    data={'content': content, 'model': model}, duration=duration
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Fireworks API error: {str(e)}")
