"""ABC utilities v4 - base abstract classes.

Base abstract class utilities.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

__all__ = [
    "BaseAction",
    "Validator",
    "Processor",
    "Transformer",
]


class BaseAction(ABC):
    """Base action class."""

    @abstractmethod
    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the action."""
        pass


class Validator(ABC):
    """Base validator class."""

    @abstractmethod
    def validate(self, value: Any) -> bool:
        """Validate a value."""
        pass


class Processor(ABC):
    """Base processor class."""

    @abstractmethod
    def process(self, data: Any) -> Any:
        """Process data."""
        pass


class Transformer(ABC):
    """Base transformer class."""

    @abstractmethod
    def transform(self, input: Any) -> Any:
        """Transform input to output."""
        pass
