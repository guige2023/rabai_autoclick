"""Mistral AI API action module for RabAI AutoClick.

Provides Mistral AI API operations for chat and embeddings.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MistralChatAction(BaseAction):
    """Execute Mistral AI chat completions.

    Supports Mistral Small, Medium, Large, and open-mistral models.
    """
    action_type = "mistral_chat"
    display_name = "Mistral聊天"
    description = "Mistral AI对话API"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Mistral chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Mistral API key
                - model: Model name (e.g. mistral-large-latest)
                - messages: List of message dicts
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens

        Returns:
            ActionResult with chat response.
        """
        api_key = params.get('api_key') or os.environ.get('MISTRAL_API_KEY')
        model = params.get('model', 'mistral-small-latest')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', 2048)

        if not api_key:
            return ActionResult(success=False, message="MISTRAL_API_KEY is required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            from mistralai import Mistral
        except ImportError:
            return ActionResult(success=False, message="mistralai not installed. Run: pip install mistralai")

        client = Mistral(api_key=api_key)
        start = time.time()
        try:
            response = client.chat.complete(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            content = response.choices[0].message.content or ""
            duration = time.time() - start
            return ActionResult(
                success=True, message="Mistral chat completed",
                data={
                    'content': content,
                    'model': model,
                    'finish_reason': response.choices[0].finish_reason,
                    'usage': {
                        'prompt_tokens': response.usage.prompt_tokens,
                        'completion_tokens': response.usage.completion_tokens,
                        'total_tokens': response.usage.total_tokens,
                    }
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Mistral API error: {str(e)}")


class MistralEmbeddingAction(BaseAction):
    """Generate embeddings via Mistral AI API."""
    action_type = "mistral_embedding"
    display_name = "Mistral嵌入"
    description = "Mistral文本向量嵌入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate embeddings.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Mistral API key
                - texts: List of strings
                - model: Embedding model

        Returns:
            ActionResult with embedding vectors.
        """
        api_key = params.get('api_key') or os.environ.get('MISTRAL_API_KEY')
        texts = params.get('texts', [])
        model = params.get('model', 'mistral-embed')

        if not api_key:
            return ActionResult(success=False, message="MISTRAL_API_KEY is required")
        if not texts:
            return ActionResult(success=False, message="texts list is required")

        try:
            from mistralai import Mistral
        except ImportError:
            return ActionResult(success=False, message="mistralai not installed")

        client = Mistral(api_key=api_key)
        start = time.time()
        try:
            response = client.embeddings.create(model=model, inputs=texts)
            embeddings = [item.embedding for item in response.data]
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Generated {len(embeddings)} embeddings",
                data={'embeddings': embeddings, 'model': model}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Embedding error: {str(e)}")
