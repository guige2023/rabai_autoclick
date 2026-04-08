"""Retry Backoff Action Module.

Provides configurable retry with
exponential backoff.
"""

import time
from typing import Any, Callable, Dict, Optional
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class RetryConfig:
    """Retry configuration."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True


class RetryBackoffManager:
    """Manages retry with backoff."""

    def __init__(self):
        self._configs: Dict[str, RetryConfig] = {}

    def configure(
        self,
        name: str,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter: bool = True
    ) -> str:
        """Configure retry policy."""
        config = RetryConfig(
            max_attempts=max_attempts,
            initial_delay=initial_delay,
            max_delay=max_delay,
            multiplier=multiplier,
            jitter=jitter
        )
        self._configs[name] = config
        return name

    def execute_with_retry(
        self,
        config_name: str,
        func: Callable,
        *args,
        **kwargs
    ) -> tuple[Any, bool, int]:
        """Execute function with retry."""
        config = self._configs.get(config_name)
        if not config:
            config = RetryConfig()

        last_error = None
        attempt = 0

        while attempt < config.max_attempts:
            try:
                result = func(*args, **kwargs)
                return result, True, attempt + 1

            except Exception as e:
                last_error = e
                attempt += 1

                if attempt >= config.max_attempts:
                    break

                delay = min(
                    config.initial_delay * (config.multiplier ** (attempt - 1)),
                    config.max_delay
                )

                if config.jitter:
                    import random
                    delay = delay * (0.5 + random.random())

                time.sleep(delay)

        return None, False, attempt

    def get_config(self, name: str) -> Optional[Dict]:
        """Get retry config."""
        config = self._configs.get(name)
        if not config:
            return None

        return {
            "max_attempts": config.max_attempts,
            "initial_delay": config.initial_delay,
            "max_delay": config.max_delay,
            "multiplier": config.multiplier,
            "jitter": config.jitter
        }


class RetryBackoffAction(BaseAction):
    """Action for retry operations."""

    def __init__(self):
        super().__init__("retry_backoff")
        self._manager = RetryBackoffManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute retry action."""
        try:
            operation = params.get("operation", "configure")

            if operation == "configure":
                return self._configure(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "get_config":
                return self._get_config(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _configure(self, params: Dict) -> ActionResult:
        """Configure retry."""
        config_name = self._manager.configure(
            name=params.get("name", ""),
            max_attempts=params.get("max_attempts", 3),
            initial_delay=params.get("initial_delay", 1.0),
            max_delay=params.get("max_delay", 60.0),
            multiplier=params.get("multiplier", 2.0),
            jitter=params.get("jitter", True)
        )
        return ActionResult(success=True, data={"config_name": config_name})

    def _execute(self, params: Dict) -> ActionResult:
        """Execute with retry."""
        def default_func():
            return {}

        result, success, attempts = self._manager.execute_with_retry(
            config_name=params.get("config_name", "default"),
            func=params.get("func") or default_func
        )

        return ActionResult(success=success, data={
            "result": result,
            "success": success,
            "attempts": attempts
        })

    def _get_config(self, params: Dict) -> ActionResult:
        """Get config."""
        config = self._manager.get_config(params.get("name", ""))
        if not config:
            return ActionResult(success=False, message="Config not found")
        return ActionResult(success=True, data=config)
