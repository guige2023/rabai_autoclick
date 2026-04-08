"""Alibaba DashScope (阿里云通义) API action module for RabAI AutoClick.

Provides DashScope API operations for Qwen models and other Alibaba AI services.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DashScopeChatAction(BaseAction):
    """Execute Alibaba DashScope Qwen chat completions.

    Supports Qwen-Turbo, Qwen-Max, and other DashScope models.
    """
    action_type = "dashscope_chat"
    display_name = "通义千问聊天"
    description = "阿里云通义千问对话API"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute DashScope chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: DashScope API key
                - model: Model name (qwen-turbo, qwen-max, etc.)
                - messages: List of message dicts
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens
                - stream: Whether to stream

        Returns:
            ActionResult with chat response.
        """
        api_key = params.get('api_key') or os.environ.get('DASHSCOPE_API_KEY')
        model = params.get('model', 'qwen-turbo')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', 2048)
        stream = params.get('stream', False)

        if not api_key:
            return ActionResult(success=False, message="DASHSCOPE_API_KEY is required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            import dashscope
            dashscope.base_api_url = 'https://dashscope.aliyuncs.com/api/v1'
        except ImportError:
            return ActionResult(success=False, message="dashscope not installed. Run: pip install dashscope")

        dashscope.api_key = api_key
        start = time.time()
        try:
            response = dashscope.Generation.call(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                stream=stream,
                result_format='message',
            )
            duration = time.time() - start
            if response.status_code == 200:
                content = response.output.choices[0].message.content or ""
                return ActionResult(
                    success=True, message="DashScope chat completed",
                    data={
                        'content': content,
                        'model': model,
                        'finish_reason': response.output.choices[0].finish_reason,
                    },
                    duration=duration
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"DashScope error {response.code}: {response.message}"
                )
        except Exception as e:
            return ActionResult(success=False, message=f"DashScope API error: {str(e)}")


class DashScopeEmbeddingAction(BaseAction):
    """Generate text embeddings via DashScope API."""
    action_type = "dashscope_embedding"
    display_name = "通义嵌入"
    description = "通义文本向量嵌入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate embeddings.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: DashScope API key
                - texts: List of strings to embed
                - model: Embedding model (text-embedding-v1)

        Returns:
            ActionResult with embedding vectors.
        """
        api_key = params.get('api_key') or os.environ.get('DASHSCOPE_API_KEY')
        texts = params.get('texts', [])
        model = params.get('model', 'text-embedding-v1')

        if not api_key:
            return ActionResult(success=False, message="DASHSCOPE_API_KEY is required")
        if not texts:
            return ActionResult(success=False, message="texts list is required")

        try:
            import dashscope
            from dashscope import TextEmbedding
        except ImportError:
            return ActionResult(success=False, message="dashscope not installed")

        dashscope.api_key = api_key
        start = time.time()
        try:
            embeddings = []
            for text in texts:
                resp = TextEmbedding.call(model=model, input=text)
                if resp.status_code == 200:
                    embeddings.append(resp.output.embeddings[0]['embedding'])
                else:
                    return ActionResult(success=False, message=f"Embedding error: {resp.message}")
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Generated {len(embeddings)} embeddings",
                data={'embeddings': embeddings, 'model': model}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Embedding error: {str(e)}")


class DashScopeImageAction(BaseAction):
    """Generate images via DashScope Stable Diffusion API."""
    action_type = "dashscope_image"
    display_name = "通义图像生成"
    description = "通义万相图像生成"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate image from text prompt.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: DashScope API key
                - prompt: Image description
                - model: Model (wanx-v1)
                - n: Number of images

        Returns:
            ActionResult with image URLs.
        """
        api_key = params.get('api_key') or os.environ.get('DASHSCOPE_API_KEY')
        prompt = params.get('prompt', '')
        model = params.get('model', 'wanx-v1')
        n = params.get('n', 1)

        if not api_key:
            return ActionResult(success=False, message="DASHSCOPE_API_KEY is required")
        if not prompt:
            return ActionResult(success=False, message="prompt is required")

        try:
            import dashscope
            from dashscope import ImageSynthesis
        except ImportError:
            return ActionResult(success=False, message="dashscope not installed")

        dashscope.api_key = api_key
        start = time.time()
        try:
            resp = ImageSynthesis.call(model=model, prompt=prompt, n=n)
            if resp.status_code == 200:
                urls = [item.url for item in resp.output.images]
                duration = time.time() - start
                return ActionResult(
                    success=True, message=f"Generated {len(urls)} image(s)",
                    data={'urls': urls, 'model': model}, duration=duration
                )
            else:
                return ActionResult(success=False, message=f"Image generation error: {resp.message}")
        except Exception as e:
            return ActionResult(success=False, message=f"Image generation error: {str(e)}")
