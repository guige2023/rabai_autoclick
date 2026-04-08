"""Volcengine (火山引擎) API action module for RabAI AutoClick.

Provides Volcengine ARK API operations for various LLM models including
DeepSeek and other hosted models on Volcano Engine.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class VolcengineChatAction(BaseAction):
    """Execute Volcengine ARK API chat completions.

    Supports DeepSeek-R1, Doubao, and other Volcano Engine hosted models.
    Uses the ARK API endpoint format.
    """
    action_type = "volcengine_chat"
    display_name = "火山引擎聊天"
    description = "调用火山引擎ARK API生成回复"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Volcengine ARK chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Volcengine ARK API key
                - endpoint: ARK endpoint URL
                - model: Model name (e.g. deepseek-r1-250528)
                - messages: List of message dicts
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens
                - stream: Whether to stream

        Returns:
            ActionResult with chat response.
        """
        api_key = params.get('api_key') or os.environ.get('VOLCANO_API_KEY')
        endpoint = params.get('endpoint') or os.environ.get('VOLCANO_ENDPOINT')
        model = params.get('model', 'deepseek-r1-250528')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', None)
        stream = params.get('stream', False)

        if not api_key:
            return ActionResult(success=False, message="VOLCANO_API_KEY is required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        if not endpoint:
            endpoint = "https://ark.cn-beijing.volces.com/api/v3/"

        try:
            import openai
        except ImportError:
            return ActionResult(success=False, message="openai package not installed")

        client = openai.OpenAI(api_key=api_key, base_url=endpoint.rstrip('/'))
        kwargs: Dict[str, Any] = {
            'model': model,
            'messages': messages,
            'temperature': temperature,
            'stream': stream,
        }
        if max_tokens:
            kwargs['max_tokens'] = max_tokens

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
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Volcengine chat completed",
                    data={'content': content, 'model': model}, duration=duration
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Volcengine API error: {str(e)}")


class VolcengineEmbeddingAction(BaseAction):
    """Generate text embeddings via Volcengine API."""
    action_type = "volcengine_embedding"
    display_name = "火山引擎嵌入"
    description = "火山引擎文本向量嵌入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate embeddings for input text.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Volcengine API key
                - endpoint: API endpoint
                - model: Embedding model
                - input: String or list of strings

        Returns:
            ActionResult with embedding vectors.
        """
        api_key = params.get('api_key') or os.environ.get('VOLCANO_API_KEY')
        endpoint = params.get('endpoint') or os.environ.get('VOLCANO_ENDPOINT', 'https://ark.cn-beijing.volces.com/api/v3/')
        model = params.get('model', 'embedding-model')
        input_text = params.get('input', '')

        if not api_key:
            return ActionResult(success=False, message="VOLCANO_API_KEY is required")
        if not input_text:
            return ActionResult(success=False, message="input text is required")

        try:
            import openai
        except ImportError:
            return ActionResult(success=False, message="openai package not installed")

        client = openai.OpenAI(api_key=api_key, base_url=endpoint.rstrip('/'))
        start = time.time()
        try:
            response = client.embeddings.create(model=model, input=input_text)
            embedding = response.data[0].embedding
            duration = time.time() - start
            return ActionResult(
                success=True, message="Embedding generated",
                data={'embedding': embedding, 'model': model}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Embedding error: {str(e)}")
