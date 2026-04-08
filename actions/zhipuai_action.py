"""ZhipuAI (智谱AI) action module for RabAI AutoClick.

Provides ZhipuAI API operations including GLM chat completions and embeddings.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ZhipuChatAction(BaseAction):
    """Execute ZhipuAI GLM chat completions.

    Supports GLM-4, GLM-4V, and other ZhipuAI models via API.
    """
    action_type = "zhipuai_chat"
    display_name = "智谱AI聊天"
    description = "调用智谱GLM API生成回复"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ZhipuAI chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: ZhipuAI API key
                - model: Model name (default: glm-4)
                - messages: List of message dicts
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens

        Returns:
            ActionResult with chat response.
        """
        api_key = params.get('api_key') or os.environ.get('ZHIPU_API_KEY')
        model = params.get('model', 'glm-4')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', 2048)

        if not api_key:
            return ActionResult(success=False, message="ZHIPU_API_KEY is required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            from zhipuai import ZhipuAI
        except ImportError:
            return ActionResult(success=False, message="zhipuai package not installed. Run: pip install zhipuai")

        client = ZhipuAI(api_key=api_key)
        start = time.time()
        try:
            response = client.chat.completions(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            content = response.choices[0].message.content or ""
            duration = time.time() - start
            return ActionResult(
                success=True, message="ZhipuAI chat completed",
                data={'content': content, 'model': model,
                      'finish_reason': response.choices[0].finish_reason},
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ZhipuAI API error: {str(e)}")


class ZhipuEmbeddingAction(BaseAction):
    """Generate text embeddings via ZhipuAI API."""
    action_type = "zhipuai_embedding"
    display_name = "智谱AI嵌入"
    description = "智谱文本向量嵌入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate embeddings for input text.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: ZhipuAI API key
                - input: String to embed
                - model: Embedding model

        Returns:
            ActionResult with embedding vector.
        """
        api_key = params.get('api_key') or os.environ.get('ZHIPU_API_KEY')
        input_text = params.get('input', '')
        model = params.get('model', 'embedding-2')

        if not api_key:
            return ActionResult(success=False, message="ZHIPU_API_KEY is required")
        if not input_text:
            return ActionResult(success=False, message="input text is required")

        try:
            from zhipuai import ZhipuAI
        except ImportError:
            return ActionResult(success=False, message="zhipuai package not installed")

        client = ZhipuAI(api_key=api_key)
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


class ZhipuImageUnderstandingAction(BaseAction):
    """Understand images via ZhipuAI GLM-4V API."""
    action_type = "zhipuai_image_understanding"
    display_name = "智谱AI图像理解"
    description = "GLM-4V图像理解"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Analyze image and return description.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: ZhipuAI API key
                - model: Model (default: glm-4v)
                - image_url: URL or local path to image
                - prompt: Question about the image

        Returns:
            ActionResult with image understanding response.
        """
        api_key = params.get('api_key') or os.environ.get('ZHIPU_API_KEY')
        model = params.get('model', 'glm-4v')
        image_url = params.get('image_url', '')
        prompt = params.get('prompt', '描述这张图片')

        if not api_key:
            return ActionResult(success=False, message="ZHIPU_API_KEY is required")
        if not image_url:
            return ActionResult(success=False, message="image_url is required")

        try:
            from zhipuai import ZhipuAI
        except ImportError:
            return ActionResult(success=False, message="zhipuai package not installed")

        client = ZhipuAI(api_key=api_key)
        messages = [
            {"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": image_url}},
                {"type": "text", "text": prompt}
            ]}
        ]
        start = time.time()
        try:
            response = client.chat.completions(model=model, messages=messages)
            content = response.choices[0].message.content or ""
            duration = time.time() - start
            return ActionResult(
                success=True, message="Image understood",
                data={'content': content, 'model': model}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Image understanding error: {str(e)}")
