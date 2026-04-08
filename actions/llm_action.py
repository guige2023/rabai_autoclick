"""LLM (Large Language Model) action module for RabAI AutoClick.

Provides interface for AI/LLM API calls with support for multiple providers,
response streaming, token tracking, and error handling.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class LLMConfig:
    """Configuration for LLM provider."""
    provider: str = "openai"
    model: str = "gpt-4"
    api_key: str = ""
    endpoint: str = ""
    max_tokens: int = 2048
    temperature: float = 0.7
    timeout: float = 60.0
    retry_count: int = 3
    retry_delay: float = 1.0


@dataclass
class TokenTracker:
    """Track token usage for cost estimation."""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    
    def add(self, prompt: int, completion: int) -> None:
        """Add token counts."""
        self.prompt_tokens += prompt
        self.completion_tokens += completion
        self.total_tokens = self.prompt_tokens + self.completion_tokens
    
    def estimate_cost(self, provider: str = "openai") -> float:
        """Estimate cost in USD based on provider pricing."""
        if provider == "openai":
            prompt_cost = self.prompt_tokens * 0.03 / 1000
            completion_cost = self.completion_tokens * 0.06 / 1000
            return prompt_cost + completion_cost
        elif provider == "anthropic":
            prompt_cost = self.prompt_tokens * 0.003 / 1000
            completion_cost = self.completion_tokens * 0.015 / 1000
            return prompt_cost + completion_cost
        return 0.0


class LLMAction(BaseAction):
    """Action for interacting with LLM APIs.
    
    Supports multiple providers: openai, anthropic, local.
    Features:
        - Synchronous and streaming responses
        - Token tracking and cost estimation
        - Retry logic with exponential backoff
        - System prompt templates
        - Message history management
    """
    
    def __init__(self, config: Optional[LLMConfig] = None):
        """Initialize LLM action.
        
        Args:
            config: LLM configuration. Uses defaults if not provided.
        """
        super().__init__()
        self.config = config or LLMConfig()
        self.token_tracker = TokenTracker()
        self._message_history: List[Dict[str, str]] = []
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute LLM query.
        
        Args:
            params: Dictionary containing:
                - prompt: The user prompt (required)
                - system_prompt: Optional system message
                - messages: Optional list of prior messages
                - temperature: Override default temperature
                - max_tokens: Override default max tokens
                - stream: Enable streaming response
        
        Returns:
            ActionResult with generated text and metadata
        """
        try:
            prompt = params.get("prompt", "")
            if not prompt:
                return ActionResult(success=False, message="Prompt is required")
            
            system_prompt = params.get("system_prompt", "")
            messages = params.get("messages", [])
            temperature = params.get("temperature", self.config.temperature)
            max_tokens = params.get("max_tokens", self.config.max_tokens)
            stream = params.get("stream", False)
            
            if not self.config.api_key and self.config.provider not in ("local", "ollama"):
                return ActionResult(
                    success=False,
                    message=f"API key required for {self.config.provider}"
                )
            
            all_messages = []
            if system_prompt:
                all_messages.append({"role": "system", "content": system_prompt})
            if messages:
                all_messages.extend(messages)
            all_messages.append({"role": "user", "content": prompt})
            
            for attempt in range(self.config.retry_count):
                try:
                    response = self._call_api(
                        messages=all_messages,
                        temperature=temperature,
                        max_tokens=max_tokens,
                        stream=stream
                    )
                    
                    if response.get("error"):
                        raise Exception(response["error"])
                    
                    content = response.get("content", "")
                    usage = response.get("usage", {})
                    
                    self.token_tracker.add(
                        prompt=usage.get("prompt_tokens", 0),
                        completion=usage.get("completion_tokens", 0)
                    )
                    
                    if params.get("save_to_history", True):
                        self._message_history.append({"role": "user", "content": prompt})
                        self._message_history.append({"role": "assistant", "content": content})
                    
                    return ActionResult(
                        success=True,
                        message="LLM query completed",
                        data={
                            "content": content,
                            "provider": self.config.provider,
                            "model": self.config.model,
                            "usage": usage,
                            "estimated_cost": self.token_tracker.estimate_cost(self.config.provider),
                            "total_tokens": self.token_tracker.total_tokens,
                            "history_length": len(self._message_history)
                        }
                    )
                    
                except Exception as e:
                    if attempt < self.config.retry_count - 1:
                        time.sleep(self.config.retry_delay * (2 ** attempt))
                        continue
                    return ActionResult(success=False, message=f"LLM error: {str(e)}")
                    
        except Exception as e:
            return ActionResult(success=False, message=f"LLM action failed: {str(e)}")
    
    def _call_api(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
        stream: bool
    ) -> Dict[str, Any]:
        """Call the LLM API based on provider."""
        if self.config.provider == "openai":
            return self._call_openai(messages, temperature, max_tokens, stream)
        elif self.config.provider == "anthropic":
            return self._call_anthropic(messages, temperature, max_tokens)
        elif self.config.provider in ("local", "ollama"):
            return self._call_local(messages, temperature, max_tokens)
        else:
            raise ValueError(f"Unsupported provider: {self.config.provider}")
    
    def _call_openai(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int,
        stream: bool
    ) -> Dict[str, Any]:
        """Call OpenAI API."""
        import urllib.request
        import urllib.error
        
        url = self.config.endpoint or "https://api.openai.com/v1/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.config.api_key}"
        }
        
        body = {
            "model": self.config.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": stream
        }
        
        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers=headers,
            method="POST"
        )
        
        try:
            with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                
                if stream:
                    content = ""
                    for line in result:
                        if "choices" in line:
                            delta = line["choices"][0].get("delta", {})
                            content += delta.get("content", "")
                    return {"content": content, "usage": result.get("usage", {})}
                else:
                    return {
                        "content": result["choices"][0]["message"]["content"],
                        "usage": result.get("usage", {})
                    }
        except urllib.error.HTTPError as e:
            error_body = json.loads(e.read().decode("utf-8"))
            return {"error": error_body.get("error", {}).get("message", str(e))}
    
    def _call_anthropic(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int
    ) -> Dict[str, Any]:
        """Call Anthropic Claude API."""
        import urllib.request
        import urllib.error
        
        url = self.config.endpoint or "https://api.anthropic.com/v1/messages"
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.config.api_key,
            "anthropic-version": "2023-06-01"
        }
        
        system_msg = ""
        user_messages = []
        for msg in messages:
            if msg["role"] == "system":
                system_msg = msg["content"]
            else:
                user_messages.append(msg)
        
        body = {
            "model": self.config.model,
            "messages": user_messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        if system_msg:
            body["system"] = system_msg
        
        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers=headers,
            method="POST"
        )
        
        try:
            with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                return {
                    "content": result["content"][0]["text"],
                    "usage": {
                        "prompt_tokens": result.get("usage", {}).get("input_tokens", 0),
                        "completion_tokens": result.get("usage", {}).get("output_tokens", 0)
                    }
                }
        except urllib.error.HTTPError as e:
            error_body = json.loads(e.read().decode("utf-8"))
            return {"error": error_body.get("error", {}).get("message", str(e))}
    
    def _call_local(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        max_tokens: int
    ) -> Dict[str, Any]:
        """Call local LLM server (Ollama, LM Studio, etc.)."""
        import urllib.request
        import urllib.error
        
        url = self.config.endpoint or "http://localhost:11434/api/chat"
        headers = {"Content-Type": "application/json"}
        
        formatted_messages = []
        for msg in messages:
            role = "system" if msg["role"] == "system" else msg["role"]
            formatted_messages.append({"role": role, "content": msg["content"]})
        
        body = {
            "model": self.config.model,
            "messages": formatted_messages,
            "temperature": temperature,
            "options": {"num_predict": max_tokens},
            "stream": False
        }
        
        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers=headers,
            method="POST"
        )
        
        try:
            with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                return {
                    "content": result.get("message", {}).get("content", ""),
                    "usage": {}
                }
        except urllib.error.HTTPError as e:
            return {"error": str(e)}
    
    def get_history(self) -> List[Dict[str, str]]:
        """Get message history."""
        return self._message_history.copy()
    
    def clear_history(self) -> None:
        """Clear message history."""
        self._message_history.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get token usage statistics."""
        return {
            "prompt_tokens": self.token_tracker.prompt_tokens,
            "completion_tokens": self.token_tracker.completion_tokens,
            "total_tokens": self.token_tracker.total_tokens,
            "estimated_cost": self.token_tracker.estimate_cost(self.config.provider),
            "provider": self.config.provider,
            "model": self.config.model,
            "history_length": len(self._message_history)
        }
