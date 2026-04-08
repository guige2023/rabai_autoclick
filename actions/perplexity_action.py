"""Perplexity AI API action module for RabAI AutoClick.

Provides Perplexity API operations for real-time web-grounded answers.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PerplexityChatAction(BaseAction):
    """Execute Perplexity AI chat completions with web search grounding.

    Supports sonar and sonar-pro models for real-time answers.
    """
    action_type = "perplexity_chat"
    display_name = "Perplexity聊天"
    description = "Perplexity AI实时搜索问答"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Perplexity chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Perplexity API key
                - model: Model (sonar or sonar-pro)
                - messages: List of message dicts
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens
                - search_recency_filter: Limit to recent results (day, week, month, year)

        Returns:
            ActionResult with chat response and citations.
        """
        api_key = params.get('api_key') or os.environ.get('PERPLEXITY_API_KEY')
        model = params.get('model', 'sonar')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', 2048)
        search_recency_filter = params.get('search_recency_filter', None)

        if not api_key:
            return ActionResult(success=False, message="PERPLEXITY_API_KEY is required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            from openai import OpenAI
        except ImportError:
            return ActionResult(success=False, message="openai package not installed")

        client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai")
        start = time.time()
        try:
            kwargs: Dict[str, Any] = {
                'model': model,
                'messages': messages,
                'temperature': temperature,
                'max_tokens': max_tokens,
            }
            response = client.chat.completions.create(**kwargs)
            content = response.choices[0].message.content or ""
            citations = getattr(response.choices[0].message, 'citations', None)
            duration = time.time() - start
            return ActionResult(
                success=True, message="Perplexity chat completed",
                data={
                    'content': content,
                    'model': model,
                    'citations': citations,
                    'usage': {
                        'total_tokens': response.usage.total_tokens,
                    }
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Perplexity API error: {str(e)}")
