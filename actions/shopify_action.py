"""Shopify action module for RabAI AutoClick.

Provides e-commerce automation via Shopify Admin API for
orders, products, customers, and inventory management.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ShopifyAction(BaseAction):
    """Shopify Admin API integration for e-commerce operations.

    Supports product management, order processing, customer data,
    and inventory updates.

    Args:
        config: Shopify configuration containing shop_name and access_token
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.shop_name = self.config.get("shop_name", "")
        self.access_token = self.config.get("access_token", "")
        self.api_base = f"https://{self.shop_name}/admin/api/2024-01"
        self.headers = {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Shopify API."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        req = Request(url, data=body, headers=self.headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result if isinstance(result, list) else result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def list_products(
        self,
        limit: int = 50,
        since_id: Optional[str] = None,
    ) -> ActionResult:
        """List products in the store.

        Args:
            limit: Maximum number of products (max 250)
            since_id: Return products after this ID

        Returns:
            ActionResult with products list
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        params = f"limit={min(limit, 250)}"
        if since_id:
            params += f"&since_id={since_id}"

        result = self._make_request("GET", f"products.json?{params}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"products": result.get("products", [])})

    def get_product(self, product_id: str) -> ActionResult:
        """Get a product by ID.

        Args:
            product_id: Product ID

        Returns:
            ActionResult with product data
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        result = self._make_request("GET", f"products/{product_id}.json")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def create_product(
        self,
        title: str,
        body_html: Optional[str] = None,
        vendor: Optional[str] = None,
        product_type: Optional[str] = None,
        variants: Optional[List[Dict]] = None,
        tags: Optional[List[str]] = None,
    ) -> ActionResult:
        """Create a new product.

        Args:
            title: Product title
            body_html: Product description (HTML)
            vendor: Vendor name
            product_type: Product type
            variants: List of variant dicts with price, sku, inventory
            tags: List of product tags

        Returns:
            ActionResult with created product
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        product = {"title": title}
        if body_html:
            product["body_html"] = body_html
        if vendor:
            product["vendor"] = vendor
        if product_type:
            product["product_type"] = product_type
        if variants:
            product["variants"] = variants
        if tags:
            product["tags"] = ",".join(tags)

        result = self._make_request("POST", "products.json", data={"product": product})
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def update_inventory(
        self,
        inventory_item_id: str,
        location_id: str,
        available: int,
    ) -> ActionResult:
        """Update inventory level.

        Args:
            inventory_item_id: Inventory item ID
            location_id: Location ID
            available: New available quantity

        Returns:
            ActionResult with inventory adjustment
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        data = {
            "location_id": location_id,
            "inventory_item_id": inventory_item_id,
            "available": available,
        }
        result = self._make_request(
            "POST", "inventory_levels/set.json", data=data
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def list_orders(
        self,
        status: str = "any",
        limit: int = 50,
        created_at_min: Optional[str] = None,
    ) -> ActionResult:
        """List orders.

        Args:
            status: Filter by status (any, open, closed, cancelled)
            limit: Maximum number of orders
            created_at_min: ISO 8601 datetime to filter

        Returns:
            ActionResult with orders list
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        params = f"status={status}&limit={min(limit, 250)}"
        if created_at_min:
            params += f"&created_at_min={created_at_min}"

        result = self._make_request("GET", f"orders.json?{params}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"orders": result.get("orders", [])})

    def get_order(self, order_id: str) -> ActionResult:
        """Get an order by ID.

        Args:
            order_id: Order ID

        Returns:
            ActionResult with order data
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        result = self._make_request("GET", f"orders/{order_id}.json")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def close_order(self, order_id: str) -> ActionResult:
        """Close an order.

        Args:
            order_id: Order ID

        Returns:
            ActionResult with closed order
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        result = self._make_request("POST", f"orders/{order_id}/close.json")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def list_customers(
        self,
        limit: int = 50,
    ) -> ActionResult:
        """List customers.

        Args:
            limit: Maximum number of customers

        Returns:
            ActionResult with customers list
        """
        if not self.access_token:
            return ActionResult(success=False, error="Missing access_token")

        result = self._make_request("GET", f"customers.json?limit={min(limit, 250)}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"customers": result.get("customers", [])})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Shopify operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "list_products": self.list_products,
            "get_product": self.get_product,
            "create_product": self.create_product,
            "update_inventory": self.update_inventory,
            "list_orders": self.list_orders,
            "get_order": self.get_order,
            "close_order": self.close_order,
            "list_customers": self.list_customers,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
