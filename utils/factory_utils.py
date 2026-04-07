"""
Factory Pattern Implementation

Provides various factory patterns: simple, factory method, abstract factory,
and dynamic factory registration.
"""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")
TProduct = TypeVar("TProduct")


class FactoryError(Exception):
    """Base exception for factory errors."""
    pass


class ProductNotFoundError(FactoryError):
    """Raised when a requested product is not registered."""
    pass


class ProductRegistrationError(FactoryError):
    """Raised when product registration fails."""
    pass


@dataclass
class ProductSpec(Generic[T]):
    """
    Specification for a product type.

    Type Parameters:
        T: The product type.
    """
    product_id: str
    name: str
    description: str = ""
    factory_func: Callable[..., T] | None = None
    product_class: type[T] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def create(self, *args: Any, **kwargs: Any) -> T:
        """Create a product instance."""
        if self.factory_func is not None:
            return self.factory_func(*args, **kwargs)
        if self.product_class is not None:
            return self.product_class(*args, **kwargs)
        raise ProductRegistrationError(f"No factory method available for '{self.product_id}'")


class ProductFactory(ABC, Generic[TProduct]):
    """
    Abstract base class for product factories.

    Type Parameters:
        TProduct: The type of product this factory creates.
    """

    @abstractmethod
    def create(self, product_id: str, *args: Any, **kwargs: Any) -> TProduct:
        """Create a product by ID."""
        pass

    @abstractmethod
    def register(self, product_id: str, spec: ProductSpec[TProduct]) -> None:
        """Register a product specification."""
        pass

    @abstractmethod
    def unregister(self, product_id: str) -> bool:
        """Unregister a product specification."""
        pass

    @abstractmethod
    def list_products(self) -> list[str]:
        """List all registered product IDs."""
        pass

    @abstractmethod
    def has_product(self, product_id: str) -> bool:
        """Check if a product is registered."""
        pass


class SimpleFactory(ProductFactory[T]):
    """
    Simple factory with direct registration.
    """

    def __init__(self):
        self._products: dict[str, ProductSpec[T]] = {}

    def register(
        self,
        product_id: str,
        spec: ProductSpec[T] | None = None,
        *,
        factory_func: Callable[..., T] | None = None,
        product_class: type[T] | None = None,
        name: str = "",
        description: str = "",
    ) -> None:
        """Register a product."""
        if product_id in self._products:
            raise ProductRegistrationError(f"Product '{product_id}' already registered")

        if spec is None:
            spec = ProductSpec(
                product_id=product_id,
                name=name or product_id,
                description=description,
                factory_func=factory_func,
                product_class=product_class,
            )

        self._products[product_id] = spec

    def register_class(self, product_id: str, cls: type[T], **kwargs: Any) -> None:
        """Register a product class."""
        self.register(
            product_id,
            ProductSpec(
                product_id=product_id,
                name=kwargs.pop("name", cls.__name__),
                description=kwargs.pop("description", ""),
                product_class=cls,
                metadata=kwargs,
            ),
        )

    def unregister(self, product_id: str) -> bool:
        """Unregister a product."""
        if product_id in self._products:
            del self._products[product_id]
            return True
        return False

    def create(self, product_id: str, *args: Any, **kwargs: Any) -> T:
        """Create a product instance."""
        if product_id not in self._products:
            raise ProductNotFoundError(
                f"Product '{product_id}' not found. Available: {self.list_products()}"
            )

        spec = self._products[product_id]
        return spec.create(*args, **kwargs)

    def get_spec(self, product_id: str) -> ProductSpec[T] | None:
        """Get a product specification."""
        return self._products.get(product_id)

    def list_products(self) -> list[str]:
        """List all registered product IDs."""
        return list(self._products.keys())

    def has_product(self, product_id: str) -> bool:
        """Check if a product is registered."""
        return product_id in self._products

    def create_all(self) -> dict[str, T]:
        """Create instances of all registered products."""
        return {pid: self.create(pid) for pid in self._products}


class DynamicFactory(ProductFactory[T]):
    """
    Dynamic factory with decorator-based registration.
    """

    def __init__(self):
        self._products: dict[str, ProductSpec[T]] = {}
        self._aliases: dict[str, str] = {}

    def register(
        self,
        product_id: str,
        spec: ProductSpec[T] | None = None,
        *,
        factory_func: Callable[..., T] | None = None,
        product_class: type[T] | None = None,
        name: str = "",
        description: str = "",
    ) -> None:
        """Register a product."""
        if spec is None:
            spec = ProductSpec(
                product_id=product_id,
                name=name or product_id,
                description=description,
                factory_func=factory_func,
                product_class=product_class,
            )
        self._products[product_id] = spec

    def register_alias(self, alias: str, product_id: str) -> None:
        """Register an alias for a product."""
        if product_id not in self._products:
            raise ProductNotFoundError(f"Cannot alias to unregistered product '{product_id}'")
        self._aliases[alias] = product_id

    def create(self, product_id: str, *args: Any, **kwargs: Any) -> T:
        """Create a product instance."""
        resolved_id = self._aliases.get(product_id, product_id)

        if resolved_id not in self._products:
            raise ProductNotFoundError(
                f"Product '{product_id}' not found. Available: {self.list_products()}"
            )

        spec = self._products[resolved_id]
        return spec.create(*args, **kwargs)

    def unregister(self, product_id: str) -> bool:
        """Unregister a product and its aliases."""
        if product_id in self._products:
            del self._products[product_id]
            # Remove aliases pointing to this product
            self._aliases = {k: v for k, v in self._aliases.items() if v != product_id}
            return True
        return False

    def list_products(self) -> list[str]:
        """List all registered product IDs including aliases."""
        return list(set(self._products.keys()) | set(self._aliases.keys()))

    def has_product(self, product_id: str) -> bool:
        """Check if a product or alias is registered."""
        return product_id in self._products or product_id in self._aliases

    def get_product_id(self, identifier: str) -> str | None:
        """Resolve an identifier to its product ID."""
        if identifier in self._products:
            return identifier
        return self._aliases.get(identifier)


def register(product_id: str, factory: DynamicFactory[T]) -> Callable[[type[T]], type[T]]:
    """
    Decorator to register a product class with a dynamic factory.

    Usage:
        factory = DynamicFactory()

        @register("my_product", factory)
        class MyProduct:
            pass
    """
    def decorator(cls: type[T]) -> type[T]:
        factory.register(product_id, ProductSpec(
            product_id=product_id,
            name=cls.__name__,
            description=cls.__doc__ or "",
            product_class=cls,
        ))
        return cls
    return decorator


class AbstractFactory(ABC, Generic[T]):
    """
    Abstract factory interface for creating families of related products.
    """

    @abstractmethod
    def create_product_a(self) -> T:
        """Create product family A."""
        pass

    @abstractmethod
    def create_product_b(self) -> T:
        """Create product family B."""
        pass


@dataclass
class FactoryMetadata:
    """Metadata for a factory."""
    name: str
    description: str = ""
    version: str = "1.0.0"
    author: str = ""
    tags: list[str] = field(default_factory=list)


class FactoryRegistry:
    """
    Global registry for factories with metadata.
    """

    _instance: FactoryRegistry | None = None

    def __new__(cls) -> FactoryRegistry:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._factories = {}
            cls._instance._metadata = {}
        return cls._instance

    def register(
        self,
        namespace: str,
        factory: ProductFactory,
        metadata: FactoryMetadata | None = None,
    ) -> None:
        """Register a factory with a namespace."""
        self._factories[namespace] = factory
        if metadata:
            self._metadata[namespace] = metadata

    def get(self, namespace: str) -> ProductFactory | None:
        """Get a factory by namespace."""
        return self._factories.get(namespace)

    def list_namespaces(self) -> list[str]:
        """List all registered namespaces."""
        return list(self._factories.keys())

    def get_metadata(self, namespace: str) -> FactoryMetadata | None:
        """Get metadata for a namespace."""
        return self._metadata.get(namespace)
