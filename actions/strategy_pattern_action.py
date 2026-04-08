"""Strategy Pattern Action Module.

Provides strategy pattern implementation for
selectable algorithm behaviors.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Strategy:
    """Strategy definition."""
    strategy_id: str
    name: str
    handler: Callable
    description: Optional[str] = None


class StrategyPatternManager:
    """Manages strategy pattern."""

    def __init__(self):
        self._strategies: Dict[str, Strategy] = {}
        self._default_strategy: Optional[str] = None

    def register_strategy(
        self,
        name: str,
        handler: Callable,
        description: Optional[str] = None
    ) -> str:
        """Register a strategy."""
        strategy_id = f"strat_{name.lower().replace(' ', '_')}"

        strategy = Strategy(
            strategy_id=strategy_id,
            name=name,
            handler=handler,
            description=description
        )

        self._strategies[strategy_id] = strategy

        if self._default_strategy is None:
            self._default_strategy = strategy_id

        return strategy_id

    def set_default(self, strategy_id: str) -> bool:
        """Set default strategy."""
        if strategy_id in self._strategies:
            self._default_strategy = strategy_id
            return True
        return False

    def get_strategy(self, strategy_id: str) -> Optional[Strategy]:
        """Get strategy by ID."""
        return self._strategies.get(strategy_id)

    def execute(
        self,
        strategy_id: Optional[str] = None,
        context: Optional[Dict] = None,
        *args,
        **kwargs
    ) -> Any:
        """Execute strategy."""
        strategy = None

        if strategy_id:
            strategy = self._strategies.get(strategy_id)
        elif self._default_strategy:
            strategy = self._strategies.get(self._default_strategy)

        if not strategy:
            raise ValueError("No strategy selected")

        return strategy.handler(context or {}, *args, **kwargs)

    def list_strategies(self) -> List[Dict]:
        """List all strategies."""
        return [
            {
                "strategy_id": s.strategy_id,
                "name": s.name,
                "description": s.description,
                "is_default": s.strategy_id == self._default_strategy
            }
            for s in self._strategies.values()
        ]


class StrategyPatternAction(BaseAction):
    """Action for strategy pattern operations."""

    def __init__(self):
        super().__init__("strategy_pattern")
        self._manager = StrategyPatternManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute strategy action."""
        try:
            operation = params.get("operation", "register")

            if operation == "register":
                return self._register(params)
            elif operation == "set_default":
                return self._set_default(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "list":
                return self._list(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register(self, params: Dict) -> ActionResult:
        """Register strategy."""
        def default_handler(context, *args, **kwargs):
            return {"result": "executed"}

        strategy_id = self._manager.register_strategy(
            name=params.get("name", ""),
            handler=params.get("handler") or default_handler,
            description=params.get("description")
        )
        return ActionResult(success=True, data={"strategy_id": strategy_id})

    def _set_default(self, params: Dict) -> ActionResult:
        """Set default strategy."""
        success = self._manager.set_default(params.get("strategy_id", ""))
        return ActionResult(success=success)

    def _execute(self, params: Dict) -> ActionResult:
        """Execute strategy."""
        try:
            result = self._manager.execute(
                strategy_id=params.get("strategy_id"),
                context=params.get("context")
            )
            return ActionResult(success=True, data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _list(self, params: Dict) -> ActionResult:
        """List strategies."""
        strategies = self._manager.list_strategies()
        return ActionResult(success=True, data={"strategies": strategies})
