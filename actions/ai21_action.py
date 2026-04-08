"""AI21 Labs API action module for RabAI AutoClick.

Provides AI21 API operations for Jurassic models.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AI21ChatAction(BaseAction):
    """Execute AI21 Jurassic model chat completions."""
    action_type = "ai21_chat"
    display_name = "AI21聊天"
    description = "AI21 Jurassic模型对话"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute AI21 chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: AI21 API key
                - model: Model name (e.g. j2-ultra)
                - messages: List of message dicts
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens

        Returns:
            ActionResult with chat response.
        """
        api_key = params.get('api_key') or os.environ.get('AI21_API_KEY')
        model = params.get('model', 'j2-ultra')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', 2048)

        if not api_key:
            return ActionResult(success=False, message="AI21_API_KEY is required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            import openai
        except ImportError:
            return ActionResult(success=False, message="openai package not installed")

        # AI21 uses OpenAI-compatible API
        client = openai.OpenAI(api_key=api_key, base_url="https://api.ai21.com/studio/v1")
        start = time.time()
        try:
            response = client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            content = response.choices[0].message.content or ""
            duration = time.time() - start
            return ActionResult(
                success=True, message="AI21 chat completed",
                data={
                    'content': content,
                    'model': model,
                    'usage': {
                        'total_tokens': response.usage.total_tokens,
                    }
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"AI21 API error: {str(e)}")
