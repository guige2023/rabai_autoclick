"""Data factory action module for RabAI AutoClick.

Provides data factory pattern operations:
- DataFactoryAction: Factory for creating data objects
- FactoryBuilderAction: Build factory configurations
- FactoryRegistryAction: Register and manage factories
- ProductCreatorAction: Create data products
- PrototypeFactoryAction: Prototype-based factory
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataFactoryAction(BaseAction):
    """Factory for creating data objects."""
    action_type = "data_factory"
    display_name = "数据工厂"
    description = "创建数据对象的工厂"

    def __init__(self):
        super().__init__()
        self._factories = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            factory_type = params.get("factory_type", "default")
            product_config = params.get("product_config", {})
            count = params.get("count", 1)

            if operation == "register":
                factory_func = product_config.get("factory_func")
                self._factories[factory_type] = {
                    "config": product_config,
                    "created_at": datetime.now().isoformat()
                }
                return ActionResult(
                    success=True,
                    data={
                        "factory_type": factory_type,
                        "registered": True,
                        "registered_at": datetime.now().isoformat()
                    },
                    message=f"Factory '{factory_type}' registered"
                )

            elif operation == "create":
                if factory_type not in self._factories:
                    product = self._create_default_product(product_config)
                else:
                    product = self._create_from_factory(factory_type, product_config)

                if count > 1:
                    products = [self._create_default_product(product_config) for _ in range(count)]
                else:
                    products = [product]

                return ActionResult(
                    success=True,
                    data={
                        "factory_type": factory_type,
                        "created_count": len(products),
                        "products": products
                    },
                    message=f"Created {len(products)} product(s) using '{factory_type}' factory"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "factories": list(self._factories.keys()),
                        "count": len(self._factories)
                    },
                    message=f"Factories: {list(self._factories.keys())}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data factory error: {str(e)}")

    def _create_default_product(self, config: Dict) -> Dict:
        return {
            "id": config.get("id", "product_1"),
            "name": config.get("name", "Default Product"),
            "type": config.get("type", "standard"),
            "created_at": datetime.now().isoformat()
        }

    def _create_from_factory(self, factory_type: str, config: Dict) -> Dict:
        return self._create_default_product(config)


class FactoryBuilderAction(BaseAction):
    """Build factory configurations."""
    action_type = "factory_builder"
    display_name = "工厂构建器"
    description = "构建工厂配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            factory_name = params.get("factory_name", "")
            product_schema = params.get("product_schema", {})
            generator_func = params.get("generator_func", None)
            validator_func = params.get("validator_func", None)
            post_processors = params.get("post_processors", [])

            if not factory_name:
                return ActionResult(success=False, message="factory_name is required")

            factory_config = {
                "name": factory_name,
                "product_schema": product_schema,
                "has_generator": generator_func is not None,
                "has_validator": validator_func is not None,
                "post_processor_count": len(post_processors),
                "created_at": datetime.now().isoformat()
            }

            return ActionResult(
                success=True,
                data=factory_config,
                message=f"Factory '{factory_name}' built with schema: {list(product_schema.keys())}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Factory builder error: {str(e)}")


class FactoryRegistryAction(BaseAction):
    """Register and manage factories."""
    action_type = "factory_registry"
    display_name = "工厂注册表"
    description = "注册和管理工厂"

    def __init__(self):
        super().__init__()
        self._registry = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")
            factory_name = params.get("factory_name", "")
            factory_config = params.get("factory_config", {})

            if operation == "register":
                if not factory_name:
                    return ActionResult(success=False, message="factory_name is required")

                self._registry[factory_name] = {
                    "config": factory_config,
                    "registered_at": datetime.now().isoformat(),
                    "use_count": 0
                }

                return ActionResult(
                    success=True,
                    data={
                        "factory_name": factory_name,
                        "registered": True,
                        "total_factories": len(self._registry)
                    },
                    message=f"Factory '{factory_name}' registered"
                )

            elif operation == "get":
                if factory_name not in self._registry:
                    return ActionResult(success=False, message=f"Factory '{factory_name}' not found")

                factory = self._registry[factory_name]
                factory["use_count"] += 1

                return ActionResult(
                    success=True,
                    data={
                        "factory_name": factory_name,
                        "config": factory["config"],
                        "use_count": factory["use_count"]
                    },
                    message=f"Retrieved factory '{factory_name}'"
                )

            elif operation == "unregister":
                if factory_name in self._registry:
                    del self._registry[factory_name]
                return ActionResult(
                    success=True,
                    data={"factory_name": factory_name, "unregistered": True},
                    message=f"Factory '{factory_name}' unregistered"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "factories": list(self._registry.keys()),
                        "count": len(self._registry)
                    },
                    message=f"Registered factories: {len(self._registry)}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Factory registry error: {str(e)}")


class ProductCreatorAction(BaseAction):
    """Create data products."""
    action_type = "product_creator"
    display_name = "产品创建器"
    description = "创建数据产品"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            product_type = params.get("product_type", "")
            attributes = params.get("attributes", {})
            quantity = params.get("quantity", 1)
            template = params.get("template", None)

            if not product_type:
                return ActionResult(success=False, message="product_type is required")

            products = []
            for i in range(quantity):
                product = {
                    "type": product_type,
                    "id": f"{product_type}_{i + 1}",
                    "attributes": attributes,
                    "created_at": datetime.now().isoformat()
                }
                if template:
                    product["template"] = template
                products.append(product)

            return ActionResult(
                success=True,
                data={
                    "product_type": product_type,
                    "created_count": len(products),
                    "products": products
                },
                message=f"Created {len(products)} {product_type} product(s)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Product creator error: {str(e)}")


class PrototypeFactoryAction(BaseAction):
    """Prototype-based factory."""
    action_type = "prototype_factory"
    display_name = "原型工厂"
    description = "基于原型的工厂"

    def __init__(self):
        super().__init__()
        self._prototypes = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            prototype_name = params.get("prototype_name", "")
            prototype = params.get("prototype", {})
            modifications = params.get("modifications", {})

            if operation == "register":
                if not prototype_name:
                    return ActionResult(success=False, message="prototype_name is required")

                self._prototypes[prototype_name] = {
                    "prototype": prototype,
                    "registered_at": datetime.now().isoformat(),
                    "clone_count": 0
                }

                return ActionResult(
                    success=True,
                    data={
                        "prototype_name": prototype_name,
                        "registered": True
                    },
                    message=f"Prototype '{prototype_name}' registered"
                )

            elif operation == "create":
                if prototype_name not in self._prototypes:
                    return ActionResult(success=False, message=f"Prototype '{prototype_name}' not found")

                base = self._prototypes[prototype_name]["prototype"].copy()
                base.update(modifications)
                self._prototypes[prototype_name]["clone_count"] += 1

                return ActionResult(
                    success=True,
                    data={
                        "prototype_name": prototype_name,
                        "clone": base,
                        "clone_count": self._prototypes[prototype_name]["clone_count"]
                    },
                    message=f"Created clone from '{prototype_name}'"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "prototypes": list(self._prototypes.keys()),
                        "count": len(self._prototypes)
                    },
                    message=f"Prototypes: {list(self._prototypes.keys())}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Prototype factory error: {str(e)}")
