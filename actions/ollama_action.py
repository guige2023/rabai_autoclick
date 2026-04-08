"""Ollama local LLM action module for RabAI AutoClick.

Provides operations for local LLM inference via Ollama API.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class OllamaChatAction(BaseAction):
    """Execute chat completions via local Ollama server.

    Supports all Ollama models including Llama 2, Mistral, Code Llama, etc.
    """
    action_type = "ollama_chat"
    display_name = "Ollama聊天"
    description = "Ollama本地LLM推理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Ollama chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - base_url: Ollama server URL (default: http://localhost:11434)
                - model: Model name (e.g. llama2, mistral)
                - messages: List of message dicts
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens
                - stream: Whether to stream

        Returns:
            ActionResult with chat response.
        """
        base_url = params.get('base_url', 'http://localhost:11434')
        model = params.get('model', 'llama2')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', 2048)
        stream = params.get('stream', False)

        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            url = f"{base_url.rstrip('/')}/api/chat"
            payload = {
                'model': model,
                'messages': messages,
                'temperature': temperature,
                'stream': stream,
                'options': {
                    'num_predict': max_tokens,
                }
            }
            response = requests.post(url, json=payload, timeout=300)
            response.raise_for_status()
            data = response.json()
            content = data.get('message', {}).get('content', '')
            duration = time.time() - start
            return ActionResult(
                success=True, message="Ollama chat completed",
                data={
                    'content': content,
                    'model': model,
                    'done': data.get('done', True),
                },
                duration=duration
            )
        except requests.exceptions.ConnectionError:
            return ActionResult(success=False, message=f"Cannot connect to Ollama at {base_url}. Is Ollama running?")
        except Exception as e:
            return ActionResult(success=False, message=f"Ollama API error: {str(e)}")


class OllamaGenerateAction(BaseAction):
    """Execute text generation via Ollama API."""
    action_type = "ollama_generate"
    display_name = "Ollama文本生成"
    description = "Ollama文本补全"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Ollama text generation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - base_url: Ollama server URL
                - model: Model name
                - prompt: Input prompt
                - temperature: Sampling temperature
                - max_tokens: Maximum tokens

        Returns:
            ActionResult with generated text.
        """
        base_url = params.get('base_url', 'http://localhost:11434')
        model = params.get('model', 'llama2')
        prompt = params.get('prompt', '')
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', 2048)

        if not prompt:
            return ActionResult(success=False, message="prompt is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            url = f"{base_url.rstrip('/')}/api/generate"
            payload = {
                'model': model,
                'prompt': prompt,
                'temperature': temperature,
                'options': {'num_predict': max_tokens},
            }
            response = requests.post(url, json=payload, timeout=300)
            response.raise_for_status()
            data = response.json()
            content = data.get('response', '')
            duration = time.time() - start
            return ActionResult(
                success=True, message="Ollama generate completed",
                data={'content': content, 'model': model}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Ollama generate error: {str(e)}")


class OllamaModelAction(BaseAction):
    """Manage Ollama models - list, pull, delete."""
    action_type = "ollama_model"
    display_name = "Ollama模型管理"
    description = "Ollama模型管理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage Ollama models.

        Args:
            context: Execution context.
            params: Dict with keys:
                - base_url: Ollama server URL
                - action: 'list', 'pull', 'delete'
                - model: Model name (for pull/delete)

        Returns:
            ActionResult with operation result.
        """
        base_url = params.get('base_url', 'http://localhost:11434')
        action = params.get('action', 'list')
        model = params.get('model', '')

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            base = base_url.rstrip('/')
            if action == 'list':
                url = f"{base}/api/tags"
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                models = response.json().get('models', [])
                return ActionResult(
                    success=True, message=f"Found {len(models)} models",
                    data={'models': [{'name': m['name']} for m in models]}, duration=time.time()-start
                )
            elif action == 'pull':
                if not model:
                    return ActionResult(success=False, message="model name required for pull")
                url = f"{base}/api/pull"
                resp = requests.post(url, json={'name': model}, stream=True, timeout=600)
                resp.raise_for_status()
                return ActionResult(
                    success=True, message=f"Pulled model {model}",
                    data={'model': model}, duration=time.time()-start
                )
            elif action == 'delete':
                if not model:
                    return ActionResult(success=False, message="model name required for delete")
                url = f"{base}/api/delete"
                resp = requests.delete(url, json={'name': model}, timeout=60)
                resp.raise_for_status()
                return ActionResult(
                    success=True, message=f"Deleted model {model}",
                    data={'model': model}, duration=time.time()-start
                )
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Ollama model error: {str(e)}")
