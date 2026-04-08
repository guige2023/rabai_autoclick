"""Factory Pattern Action Module.

Provides factory pattern for object
creation.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Product:
    """Product definition."""
    product_id: str
    name: str
    properties: Dict = field(default_factory=dict)


class Factory:
    """Factory implementation."""
    def __init__(self, factory_id: str, name: str):
        self.factory_id = factory_id
        self.name = name
        self._creators: Dict[str, Callable] = {}

    def register_creator(
        self,
        product_type: str,
        creator: Callable
    ) -> None:
        """Register a product creator."""
        self._creators[product_type] = creator

    def create(self, product_type: str, *args, **kwargs) -> Product:
        """Create a product."""
        creator = self._creators.get(product_type)
        if not creator:
            raise ValueError(f"Unknown product type: {product_type}")

        return creator(*args, **kwargs)


class FactoryManager:
    """Manages factories."""

    def __init__(self):
        self._factories: Dict[str, Factory] = {}

    def create_factory(self, name: str) -> str:
        """Create a factory."""
        factory_id = f"fact_{name.lower().replace(' ', '_')}"
        self._factories[factory_id] = Factory(factory_id, name)
        return factory_id

    def get_factory(self, factory_id: str) -> Optional[Factory]:
        """Get factory."""
        return self._factories.get(factory_id)


class FactoryPatternAction(BaseAction):
    """Action for factory pattern operations."""

    def __init__(self):
        super().__init__("factory")
        self._manager = FactoryManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute factory action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "register":
                return self._register(params)
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

    def _register(self, params: Dict) -> ActionResult:
        """Register creator."""
        factory = self._manager.get_factory(params.get("factory_id", ""))
        if not factory:
            return ActionResult(success=False, message="Factory not found")

        factory.register_creator(
            params.get("product_type", ""),
            params.get("creator") or (lambda: {})
        )
        return ActionResult(success=True)

    def _create_product(self, params: Dict) -> ActionResult:
        """Create product."""
        factory = self._manager.get_factory(params.get("factory_id", ""))
        if not factory:
            return ActionResult(success=False, message="Factory not found")

        product = factory.create(params.get("product_type", ""))
        return ActionResult(success=True, data={
            "product_id": product.product_id,
            "name": product.name
        })
