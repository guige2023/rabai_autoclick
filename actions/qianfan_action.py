"""Baidu Qianfan (百度千帆) API action module for RabAI AutoClick.

Provides Baidu Qianfan platform operations for LLMs including ERNIE Bot.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class QianfanChatAction(BaseAction):
    """Execute Baidu Qianfan ERNIE Bot chat completions.

    Supports ERNIE Bot, ERNIE Bot Turbo, and other Qianfan models.
    """
    action_type = "qianfan_chat"
    display_name = "百度千帆聊天"
    description = "百度千帆ERNIE Bot对话"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Qianfan chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Baidu API Key
                - secret_key: Baidu Secret Key
                - model: Model name (ernie-bot, ernie-bot-turbo, etc.)
                - messages: List of message dicts
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens

        Returns:
            ActionResult with chat response.
        """
        api_key = params.get('api_key') or os.environ.get('QIANFAN_API_KEY')
        secret_key = params.get('secret_key') or os.environ.get('QIANFAN_SECRET_KEY')
        model = params.get('model', 'ernie-bot')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', 2048)

        if not api_key or not secret_key:
            return ActionResult(success=False, message="QIANFAN_API_KEY and QIANFAN_SECRET_KEY are required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            import qianfan
        except ImportError:
            return ActionResult(success=False, message="qianfan not installed. Run: pip install qianfan")

        start = time.time()
        try:
            chat_comp = qianfan.ChatCompletion()
            response = chat_comp.do(
                model=model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            content = response.body.get('result', '')
            duration = time.time() - start
            return ActionResult(
                success=True, message="Qianfan chat completed",
                data={
                    'content': content,
                    'model': model,
                    'usage': response.body.get('usage', {}),
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Qianfan API error: {str(e)}")


class QianfanEmbeddingAction(BaseAction):
    """Generate text embeddings via Baidu Qianfan API."""
    action_type = "qianfan_embedding"
    display_name = "百度千帆嵌入"
    description = "千帆文本向量嵌入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate embeddings.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Baidu API Key
                - secret_key: Baidu Secret Key
                - texts: List of strings to embed
                - model: Embedding model

        Returns:
            ActionResult with embedding vectors.
        """
        api_key = params.get('api_key') or os.environ.get('QIANFAN_API_KEY')
        secret_key = params.get('secret_key') or os.environ.get('QIANFAN_SECRET_KEY')
        texts = params.get('texts', [])
        model = params.get('model', 'embedding-v1')

        if not api_key or not secret_key:
            return ActionResult(success=False, message="QIANFAN_API_KEY and QIANFAN_SECRET_KEY are required")
        if not texts:
            return ActionResult(success=False, message="texts list is required")

        try:
            import qianfan
        except ImportError:
            return ActionResult(success=False, message="qianfan not installed")

        start = time.time()
        try:
            embedding_comp = qianfan.Embedding()
            embeddings = []
            for text in texts:
                resp = embedding_comp.do(model=model, input=text)
                embeddings.append(resp.body['data'][0]['embedding'])
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Generated {len(embeddings)} embeddings",
                data={'embeddings': embeddings, 'model': model}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Embedding error: {str(e)}")
