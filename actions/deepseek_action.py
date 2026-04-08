"""DeepSeek API action module for RabAI AutoClick.

Provides DeepSeek API operations including chat completions and reasoning models.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DeepSeekChatAction(BaseAction):
    """Execute DeepSeek chat completions API calls.

    Supports DeepSeek Chat and DeepSeek Reasoner models.
    Handles streaming and non-streaming responses.
    """
    action_type = "deepseek_chat"
    display_name = "DeepSeek聊天"
    description = "调用DeepSeek API生成回复"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute DeepSeek chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: DeepSeek API key
                - model: Model name (default: deepseek-chat)
                - messages: List of message dicts
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens
                - stream: Whether to stream

        Returns:
            ActionResult with chat response.
        """
        api_key = params.get('api_key') or os.environ.get('DEEPSEEK_API_KEY')
        model = params.get('model', 'deepseek-chat')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', None)
        stream = params.get('stream', False)

        if not api_key:
            return ActionResult(success=False, message="DEEPSEEK_API_KEY is required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            import openai
        except ImportError:
            return ActionResult(success=False, message="openai package not installed. Run: pip install openai")

        client = openai.OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
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
                usage = {
                    'prompt_tokens': response.usage.prompt_tokens,
                    'completion_tokens': response.usage.completion_tokens,
                    'total_tokens': response.usage.total_tokens,
                }
                duration = time.time() - start
                return ActionResult(
                    success=True, message="DeepSeek chat completed",
                    data={'content': content, 'usage': usage, 'model': model,
                          'finish_reason': response.choices[0].finish_reason},
                    duration=duration
                )
        except Exception as e:
            return ActionResult(success=False, message=f"DeepSeek API error: {str(e)}")


class DeepSeekReasonerAction(BaseAction):
    """Execute DeepSeek Reasoner (R1) API for chain-of-thought reasoning.

    Returns reasoning trace alongside the final answer.
    """
    action_type = "deepseek_reasoner"
    display_name = "DeepSeek推理"
    description = "DeepSeek R1 推理模型"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute DeepSeek Reasoner.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: DeepSeek API key
                - prompt: User prompt
                - model: Model (default: deepseek-reasoner)
                - max_tokens: Maximum tokens

        Returns:
            ActionResult with reasoning and answer.
        """
        api_key = params.get('api_key') or os.environ.get('DEEPSEEK_API_KEY')
        prompt = params.get('prompt', '')
        model = params.get('model', 'deepseek-reasoner')
        max_tokens = params.get('max_tokens', 8192)

        if not api_key:
            return ActionResult(success=False, message="DEEPSEEK_API_KEY is required")
        if not prompt:
            return ActionResult(success=False, message="prompt is required")

        try:
            import openai
        except ImportError:
            return ActionResult(success=False, message="openai package not installed")

        client = openai.OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
        messages = [{"role": "user", "content": prompt}]
        start = time.time()
        try:
            response = client.chat.completions.create(
                model=model, messages=messages, max_tokens=max_tokens, stream=False
            )
            answer = response.choices[0].message.content or ""
            duration = time.time() - start
            return ActionResult(
                success=True, message="Reasoning completed",
                data={'answer': answer, 'model': model,
                      'usage': {
                          'total_tokens': response.usage.total_tokens,
                          'completion_tokens': response.usage.completion_tokens,
                      }},
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DeepSeek Reasoner error: {str(e)}")


class DeepSeekEmbeddingAction(BaseAction):
    """Generate text embeddings via DeepSeek API."""
    action_type = "deepseek_embedding"
    display_name = "DeepSeek嵌入"
    description = "DeepSeek文本向量嵌入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate embeddings for input text.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: DeepSeek API key
                - input: String or list of strings
                - model: Embedding model

        Returns:
            ActionResult with embedding vectors.
        """
        api_key = params.get('api_key') or os.environ.get('DEEPSEEK_API_KEY')
        input_text = params.get('input', '')
        model = params.get('model', 'deepseek-embed')

        if not api_key:
            return ActionResult(success=False, message="DEEPSEEK_API_KEY is required")
        if not input_text:
            return ActionResult(success=False, message="input text is required")

        try:
            import openai
        except ImportError:
            return ActionResult(success=False, message="openai package not installed")

        client = openai.OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
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
