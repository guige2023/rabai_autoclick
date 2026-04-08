"""Cohere API action module for RabAI AutoClick.

Provides Cohere API operations including chat, embeddings, and rerank.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CohereChatAction(BaseAction):
    """Execute Cohere Chat API for conversational AI.

    Supports Command R, Command R+, and other Cohere models.
    """
    action_type = "cohere_chat"
    display_name = "Cohere聊天"
    description = "Cohere对话API"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Cohere chat.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Cohere API key
                - model: Model name (default: command-r-plus)
                - message: User message
                - chat_history: Optional list of prior messages
                - temperature: Sampling temperature

        Returns:
            ActionResult with chat response.
        """
        api_key = params.get('api_key') or os.environ.get('COHERE_API_KEY')
        model = params.get('model', 'command-r-plus')
        message = params.get('message', '')
        chat_history = params.get('chat_history', [])
        temperature = params.get('temperature', 0.7)

        if not api_key:
            return ActionResult(success=False, message="COHERE_API_KEY is required")
        if not message:
            return ActionResult(success=False, message="message is required")

        try:
            import cohere
        except ImportError:
            return ActionResult(success=False, message="cohere package not installed. Run: pip install cohere")

        client = cohere.Client(api_key)
        start = time.time()
        try:
            response = client.chat(
                model=model,
                message=message,
                chat_history=chat_history,
                temperature=temperature,
            )
            duration = time.time() - start
            return ActionResult(
                success=True, message="Cohere chat completed",
                data={
                    'content': response.text,
                    'model': model,
                    'finish_reason': response.finish_reason,
                    'citations': response.citations,
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cohere API error: {str(e)}")


class CohereEmbeddingAction(BaseAction):
    """Generate text embeddings via Cohere API."""
    action_type = "cohere_embedding"
    display_name = "Cohere嵌入"
    description = "Cohere文本向量嵌入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate embeddings.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Cohere API key
                - texts: List of strings to embed
                - model: Embedding model (default: embed-english-v3.0)

        Returns:
            ActionResult with embedding vectors.
        """
        api_key = params.get('api_key') or os.environ.get('COHERE_API_KEY')
        texts = params.get('texts', [])
        model = params.get('model', 'embed-english-v3.0')

        if not api_key:
            return ActionResult(success=False, message="COHERE_API_KEY is required")
        if not texts:
            return ActionResult(success=False, message="texts list is required")

        try:
            import cohere
        except ImportError:
            return ActionResult(success=False, message="cohere package not installed")

        client = cohere.Client(api_key)
        start = time.time()
        try:
            response = client.embed(texts=texts, model=model)
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Generated {len(response.embeddings)} embeddings",
                data={
                    'embeddings': response.embeddings,
                    'model': model,
                    'length': len(response.embeddings[0]) if response.embeddings else 0,
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Embedding error: {str(e)}")


class CohereRerankAction(BaseAction):
    """Rerank documents using Cohere Rerank API."""
    action_type = "cohere_rerank"
    display_name = "Cohere重排序"
    description = "Cohere文档重排序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Rerank documents.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Cohere API key
                - query: Search query
                - documents: List of document strings
                - model: Rerank model (default: rerank-english-v3.0)
                - top_n: Number of results to return

        Returns:
            ActionResult with reranked documents.
        """
        api_key = params.get('api_key') or os.environ.get('COHERE_API_KEY')
        query = params.get('query', '')
        documents = params.get('documents', [])
        model = params.get('model', 'rerank-english-v3.0')
        top_n = params.get('top_n', 5)

        if not api_key:
            return ActionResult(success=False, message="COHERE_API_KEY is required")
        if not query or not documents:
            return ActionResult(success=False, message="query and documents are required")

        try:
            import cohere
        except ImportError:
            return ActionResult(success=False, message="cohere package not installed")

        client = cohere.Client(api_key)
        start = time.time()
        try:
            response = client.rerank(
                query=query, documents=documents, model=model, top_n=top_n
            )
            results = [{'index': r.index, 'document': r.document, 'relevance_score': r.relevance_score} for r in response.results]
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Reranked {len(results)} documents",
                data={'results': results, 'model': model}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Rerank error: {str(e)}")
