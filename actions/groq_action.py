"""Groq API action module for RabAI AutoClick.

Provides fast LLM inference via Groq's GPU-accelerated API.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GroqChatAction(BaseAction):
    """Execute Groq API chat completions for ultra-fast inference.

    Supports Llama, Mixtral, and other models hosted on Groq.
    """
    action_type = "groq_chat"
    display_name = "Groq聊天"
    description = "Groq高速LLM推理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Groq chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Groq API key
                - model: Model name (e.g. llama-3.1-70b-versatile)
                - messages: List of message dicts
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens
                - stream: Whether to stream

        Returns:
            ActionResult with chat response.
        """
        api_key = params.get('api_key') or os.environ.get('GROQ_API_KEY')
        model = params.get('model', 'llama-3.1-70b-versatile')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', 2048)
        stream = params.get('stream', False)

        if not api_key:
            return ActionResult(success=False, message="GROQ_API_KEY is required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            from groq import Groq
        except ImportError:
            return ActionResult(success=False, message="groq package not installed. Run: pip install groq")

        client = Groq(api_key=api_key)
        kwargs: Dict[str, Any] = {
            'model': model,
            'messages': messages,
            'temperature': temperature,
            'max_tokens': max_tokens,
            'stream': stream,
        }

        start = time.time()
        try:
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
                usage = {
                    'prompt_tokens': response.usage.prompt_tokens,
                    'completion_tokens': response.usage.completion_tokens,
                    'total_tokens': response.usage.total_tokens,
                }
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Groq chat completed",
                    data={'content': content, 'usage': usage, 'model': model}, duration=duration
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Groq API error: {str(e)}")
