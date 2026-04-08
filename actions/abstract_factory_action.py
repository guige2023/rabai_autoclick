"""Abstract Factory Pattern Action Module.

Provides abstract factory for related
product families.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class AbstractProduct:
    """Abstract product."""
    product_id: str
    family: str


class AbstractFactory:
    """Abstract factory implementation."""
    def __init__(self, factory_id: str, name: str):
        self.factory_id = factory_id
        self.name = name
        self._product_families: Dict[str, Callable] = {}

    def register_family(
        self,
        family: str,
        creator: Callable
    ) -> None:
        """Register a product family."""
        self._product_families[family] = creator

    def create_product(self, family: str) -> AbstractProduct:
        """Create product from family."""
        creator = self._product_families.get(family)
        if not creator:
            raise ValueError(f"Unknown family: {family}")

        return creator()


class AbstractFactoryManager:
    """Manages abstract factories."""

    def __init__(self):
        self._factories: Dict[str, AbstractFactory] = {}

    def create_factory(self, name: str) -> str:
        """Create abstract factory."""
        factory_id = f"abs_fact_{name.lower().replace(' ', '_')}"
        self._factories[factory_id] = AbstractFactory(factory_id, name)
        return factory_id

    def get_factory(self, factory_id: str) -> Optional[AbstractFactory]:
        """Get factory."""
        return self._factories.get(factory_id)


class AbstractFactoryPatternAction(BaseAction):
    """Action for abstract factory pattern operations."""

    def __init__(self):
        super().__init__("abstract_factory")
        self._manager = AbstractFactoryManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute abstract factory action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "register_family":
                return self._register_family(params)
            elif operation == "create_product":
                return self._create_product(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create factory."""
        factory_id = self._manager.create_factory(params.get("name", ""))
        return ActionResult(success=True, data={"factory_id": factory_id})

    def _register_family(self, params: Dict) -> ActionResult:
        """Register family."""
        factory = self._manager.get_factory(params.get("factory_id", ""))
        if not factory:
            return ActionResult(success=False, message="Factory not found")

        factory.register_family(
            params.get("family", ""),
            params.get("creator") or (lambda: AbstractProduct("",""))
        )
        return ActionResult(success=True)

    def _create_product(self, params: Dict) -> ActionResult:
        """Create product."""
        factory = self._manager.get_factory(params.get("factory_id", ""))
        if not factory:
            return ActionResult(success=False, message="Factory not found")

        product = factory.create_product(params.get("family", ""))
        return ActionResult(success=True, data={
            "product_id": product.product_id,
            "family": product.family
        })
