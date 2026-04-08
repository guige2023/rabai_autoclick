"""OpenAI API action module for RabAI AutoClick.

Provides OpenAI API operations including chat completions, embeddings,
image generation, and audio transcription via the OpenAI API.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class OpenAIChatAction(BaseAction):
    """Execute OpenAI chat completions API calls.

    Supports gpt-4, gpt-4-turbo, gpt-3.5-turbo and compatible models.
    Handles streaming responses, function calling, and multi-turn conversations.
    """
    action_type = "openai_chat"
    display_name = "OpenAI聊天"
    description = "调用OpenAI Chat API生成回复"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute OpenAI chat completion.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: OpenAI API key
                - model: Model name (default: gpt-3.5-turbo)
                - messages: List of message dicts with role/content
                - temperature: Sampling temperature 0-2
                - max_tokens: Maximum tokens to generate
                - stream: Whether to stream responses
                - functions: Optional function definitions

        Returns:
            ActionResult with chat response content.
        """
        api_key = params.get('api_key') or os.environ.get('OPENAI_API_KEY')
        model = params.get('model', 'gpt-3.5-turbo')
        messages = params.get('messages', [])
        temperature = params.get('temperature', 1.0)
        max_tokens = params.get('max_tokens', None)
        stream = params.get('stream', False)
        functions = params.get('functions', None)

        if not api_key:
            return ActionResult(success=False, message="OPENAI_API_KEY is required")
        if not messages:
            return ActionResult(success=False, message="messages list is required")

        try:
            import openai
        except ImportError:
            return ActionResult(success=False, message="openai package not installed")

        client = openai.OpenAI(api_key=api_key)
        kwargs: Dict[str, Any] = {
            'model': model,
            'messages': messages,
            'temperature': temperature,
            'stream': stream,
        }
        if max_tokens:
            kwargs['max_tokens'] = max_tokens
        if functions:
            kwargs['functions'] = functions

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
                    success=True,
                    message="Stream completed",
                    data={'content': full_content, 'model': model},
                    duration=duration
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
                    success=True,
                    message="Chat completion successful",
                    data={'content': content, 'usage': usage, 'model': model, 'finish_reason': response.choices[0].finish_reason},
                    duration=duration
                )
        except Exception as e:
            return ActionResult(success=False, message=f"OpenAI API error: {str(e)}")


class OpenAIEmbeddingAction(BaseAction):
    """Generate text embeddings via OpenAI API.

    Supports text-embedding-ada-002 and newer embedding models.
    """
    action_type = "openai_embedding"
    display_name = "OpenAI嵌入"
    description = "生成文本向量嵌入"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate embeddings for input text.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: OpenAI API key
                - model: Embedding model (default: text-embedding-ada-002)
                - input: String or list of strings to embed

        Returns:
            ActionResult with embedding vectors.
        """
        api_key = params.get('api_key') or os.environ.get('OPENAI_API_KEY')
        model = params.get('model', 'text-embedding-ada-002')
        input_text = params.get('input', '')

        if not api_key:
            return ActionResult(success=False, message="OPENAI_API_KEY is required")
        if not input_text:
            return ActionResult(success=False, message="input text is required")

        try:
            import openai
        except ImportError:
            return ActionResult(success=False, message="openai package not installed")

        client = openai.OpenAI(api_key=api_key)
        start = time.time()
        try:
            response = client.embeddings.create(model=model, input=input_text)
            embedding = response.data[0].embedding
            duration = time.time() - start
            return ActionResult(
                success=True,
                message="Embedding generated",
                data={'embedding': embedding, 'model': model, 'tokens': response.usage.total_tokens},
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Embedding error: {str(e)}")


class OpenAIImageAction(BaseAction):
    """Generate images via OpenAI DALL-E API.

    Supports DALL-E 3 and DALL-E 2 models with size and quality options.
    """
    action_type = "openai_image"
    display_name = "OpenAI图像生成"
    description = "通过DALL-E生成图像"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate image from text prompt.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: OpenAI API key
                - prompt: Image description
                - model: Model (dall-e-3 or dall-e-2)
                - size: Image size (1024x1024, 1792x1024, etc.)
                - quality: Image quality (standard or hd)
                - n: Number of images (1-10)

        Returns:
            ActionResult with image URLs.
        """
        api_key = params.get('api_key') or os.environ.get('OPENAI_API_KEY')
        prompt = params.get('prompt', '')
        model = params.get('model', 'dall-e-3')
        size = params.get('size', '1024x1024')
        quality = params.get('quality', 'standard')
        n = params.get('n', 1)

        if not api_key:
            return ActionResult(success=False, message="OPENAI_API_KEY is required")
        if not prompt:
            return ActionResult(success=False, message="prompt is required")

        try:
            import openai
        except ImportError:
            return ActionResult(success=False, message="openai package not installed")

        client = openai.OpenAI(api_key=api_key)
        start = time.time()
        try:
            response = client.images.generate(
                model=model, prompt=prompt, size=size, quality=quality, n=n
            )
            urls = [item.url for item in response.data if item.url]
            duration = time.time() - start
            return ActionResult(
                success=True,
                message=f"Generated {len(urls)} image(s)",
                data={'urls': urls, 'model': model},
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Image generation error: {str(e)}")


class OpenAITranscriptionAction(BaseAction):
    """Transcribe audio via OpenAI Whisper API.

    Supports multiple audio formats and language specification.
    """
    action_type = "openai_transcription"
    display_name = "OpenAI语音转写"
    description = "Whisper音频转文字"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Transcribe audio file to text.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: OpenAI API key
                - file_path: Path to audio file
                - model: Whisper model (whisper-1)
                - language: Optional source language
                - prompt: Optional prompt for context

        Returns:
            ActionResult with transcription text.
        """
        api_key = params.get('api_key') or os.environ.get('OPENAI_API_KEY')
        file_path = params.get('file_path', '')
        model = params.get('model', 'whisper-1')
        language = params.get('language', None)
        prompt = params.get('prompt', None)

        if not api_key:
            return ActionResult(success=False, message="OPENAI_API_KEY is required")
        if not file_path:
            return ActionResult(success=False, message="file_path is required")
        if not os.path.exists(file_path):
            return ActionResult(success=False, message=f"File not found: {file_path}")

        try:
            import openai
        except ImportError:
            return ActionResult(success=False, message="openai package not installed")

        client = openai.OpenAI(api_key=api_key)
        start = time.time()
        try:
            with open(file_path, 'rb') as f:
                kwargs: Dict[str, Any] = {'model': model, 'file': f}
                if language:
                    kwargs['language'] = language
                if prompt:
                    kwargs['prompt'] = prompt
                response = client.audio.transcriptions.create(**kwargs)
            duration = time.time() - start
            return ActionResult(
                success=True,
                message="Transcription completed",
                data={'text': response.text, 'language': getattr(response, 'language', None)},
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transcription error: {str(e)}")
