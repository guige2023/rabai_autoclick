"""
Action Chaining Module.

Provides utilities for chaining multiple actions together into sequences
with support for branching, parallel execution, and error handling.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, TypeVar, Generic


logger = logging.getLogger(__name__)

T = TypeVar('T')
R = TypeVar('R')


class ChainStrategy(Enum):
    """Strategy for handling action chain execution."""
    SEQUENTIAL = auto()
    PARALLEL = auto()
    RACE = auto()
    FALLBACK = auto()


@dataclass
class ActionResult(Generic[T]):
    """Result of an action execution."""
    success: bool
    value: T | None = None
    error: Exception | None = None
    duration_ms: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


class Action(Generic[T]):
    """
    Represents a single action in a chain.

    Actions are callable objects that return ActionResult when executed.
    """

    def __init__(
        self,
        name: str,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> None:
        """
        Initialize an action.

        Args:
            name: Human-readable name for the action.
            func: The function to execute.
            *args: Positional arguments for the function.
            **kwargs: Keyword arguments for the function.
        """
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs

    async def execute(self) -> ActionResult[T]:
        """
        Execute the action.

        Returns:
            ActionResult containing the outcome.
        """
        import time
        start = time.perf_counter()

        try:
            if asyncio.iscoroutinefunction(self.func):
                value = await self.func(*self.args, **self.kwargs)
            else:
                value = self.func(*self.args, **self.kwargs)

            duration = (time.perf_counter() - start) * 1000

            return ActionResult(
                success=True,
                value=value,
                duration_ms=duration
            )
        except Exception as e:
            duration = (time.perf_counter() - start) * 1000
            logger.error(f"Action '{self.name}' failed: {e}")

            return ActionResult(
                success=False,
                error=e,
                duration_ms=duration,
                metadata={"action_name": self.name}
            )


class ActionChain(Generic[T]):
    """
    A chain of actions that can be executed together.

    Supports sequential, parallel, and race execution strategies.
    """

    def __init__(
        self,
        name: str,
        strategy: ChainStrategy = ChainStrategy.SEQUENTIAL
    ) -> None:
        """
        Initialize an action chain.

        Args:
            name: Name of the chain.
            strategy: Execution strategy for the chain.
        """
        self.name = name
        self.strategy = strategy
        self._actions: list[Action[Any]] = []
        self._error_handlers: dict[str, Callable[[Exception], Any]] = {}

    def add_action(
        self,
        action: Action[T]
    ) -> ActionChain[T]:
        """
        Add an action to the chain.

        Args:
            action: The action to add.

        Returns:
            Self for chaining.
        """
        self._actions.append(action)
        return self

    def on_error(
        self,
        action_name: str,
        handler: Callable[[Exception], Any]
    ) -> ActionChain[T]:
        """
        Register an error handler for a specific action.

        Args:
            action_name: Name of the action to handle errors for.
            handler: Error handler function.

        Returns:
            Self for chaining.
        """
        self._error_handlers[action_name] = handler
        return self

    async def execute(self) -> list[ActionResult[Any]]:
        """
        Execute all actions in the chain based on the strategy.

        Returns:
            List of results from each action execution.
        """
        logger.info(f"Executing chain '{self.name}' with strategy {self.strategy}")

        if self.strategy == ChainStrategy.SEQUENTIAL:
            return await self._execute_sequential()
        elif self.strategy == ChainStrategy.PARALLEL:
            return await self._execute_parallel()
        elif self.strategy == ChainStrategy.RACE:
            return await self._execute_race()
        elif self.strategy == ChainStrategy.FALLBACK:
            return await self._execute_fallback()

        return []

    async def _execute_sequential(self) -> list[ActionResult[Any]]:
        """Execute actions sequentially, stopping on error."""
        results: list[ActionResult[Any]] = []

        for action in self._actions:
            result = await action.execute()
            results.append(result)

            if not result.success:
                handler = self._error_handlers.get(action.name)
                if handler:
                    try:
                        await handler(result.error)
                    except Exception as e:
                        logger.error(f"Error handler failed: {e}")
                else:
                    logger.warning(
                        f"Action '{action.name}' failed, continuing chain"
                    )

        return results

    async def _execute_parallel(self) -> list[ActionResult[Any]]:
        """Execute all actions in parallel."""
        tasks = [action.execute() for action in self._actions]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed: list[ActionResult[Any]] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed.append(ActionResult(
                    success=False,
                    error=result,
                    metadata={"action_name": self._actions[i].name}
                ))
            else:
                processed.append(result)

        return processed

    async def _execute_race(self) -> list[ActionResult[Any]]:
        """Execute actions and return when first one completes."""
        if not self._actions:
            return []

        first_complete = await asyncio.gather(
            *[action.execute() for action in self._actions],
            return_exceptions=True
        )

        results: list[ActionResult[Any]] = []
        for i, result in enumerate(first_complete):
            if isinstance(result, Exception):
                results.append(ActionResult(
                    success=False,
                    error=result,
                    metadata={"action_name": self._actions[i].name}
                ))
            else:
                results.append(result)

        return results

    async def _execute_fallback(self) -> list[ActionResult[Any]]:
        """Execute actions sequentially, using fallback on failure."""
        results: list[ActionResult[Any]] = []

        for action in self._actions:
            result = await action.execute()
            results.append(result)

            if result.success:
                break

        return results


class ChainBuilder(Generic[T]):
    """
    Builder for constructing action chains fluently.
    """

    def __init__(self, name: str) -> None:
        """Initialize the chain builder."""
        self._name = name
        self._strategy = ChainStrategy.SEQUENTIAL
        self._actions: list[Action[Any]] = []
        self._error_handlers: dict[str, Callable[[Exception], Any]] = {}

    def with_strategy(self, strategy: ChainStrategy) -> ChainBuilder[T]:
        """Set the execution strategy."""
        self._strategy = strategy
        return self

    def then(
        self,
        name: str,
        func: Callable[..., R],
        *args: Any,
        **kwargs: Any
    ) -> ChainBuilder[R]:
        """
        Add a sequential action to the chain.

        Args:
            name: Action name.
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Self for chaining.
        """
        action = Action(name, func, *args, **kwargs)
        self._actions.append(action)  # type: ignore
        return self  # type: ignore

    def on_error(
        self,
        action_name: str,
        handler: Callable[[Exception], Any]
    ) -> ChainBuilder[T]:
        """Register an error handler for an action."""
        self._error_handlers[action_name] = handler
        return self

    def build(self) -> ActionChain[T]:
        """
        Build the action chain.

        Returns:
            Configured ActionChain instance.
        """
        chain = ActionChain[T](self._name, self._strategy)

        for action in self._actions:
            chain.add_action(action)

        for name, handler in self._error_handlers.items():
            chain.on_error(name, handler)

        return chain


@dataclass
class BranchCondition(Generic[T]):
    """A condition for branching in an action chain."""
    name: str
    predicate: Callable[[Any], bool]
    chain: ActionChain[T]


class BranchingChain(Generic[T]):
    """
    An action chain that supports branching based on conditions.
    """

    def __init__(self, name: str) -> None:
        """
        Initialize a branching chain.

        Args:
            name: Name of the branching chain.
        """
        self.name = name
        self._default_chain: ActionChain[T] | None = None
        self._branches: list[BranchCondition[T]] = []

    def add_branch(
        self,
        name: str,
        predicate: Callable[[Any], bool],
        chain: ActionChain[T]
    ) -> BranchingChain[T]:
        """
        Add a branch condition.

        Args:
            name: Branch name.
            predicate: Condition function.
            chain: Action chain for this branch.

        Returns:
            Self for chaining.
        """
        self._branches.append(BranchCondition(name, predicate, chain))
        return self

    def set_default(self, chain: ActionChain[T]) -> BranchingChain[T]:
        """
        Set the default branch.

        Args:
            chain: Default action chain.

        Returns:
            Self for chaining.
        """
        self._default_chain = chain
        return self

    async def execute(self, context: Any) -> list[ActionResult[Any]]:
        """
        Execute the branching chain based on context.

        Args:
            context: Context value to evaluate branches against.

        Returns:
            Results from the executed branch.
        """
        for branch in self._branches:
            if branch.predicate(context):
                logger.info(f"Executing branch '{branch.name}'")
                return await branch.chain.execute()

        if self._default_chain:
            logger.info("Executing default branch")
            return await self._default_chain.execute()

        logger.warning(f"No branch matched for context in '{self.name}'")
        return []
