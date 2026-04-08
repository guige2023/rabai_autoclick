"""Stripe action module for RabAI AutoClick.

Provides payment processing and subscription management
via Stripe API.
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


class StripeAction(BaseAction):
    """Stripe API integration for payments and subscriptions.

    Supports charges, customers, subscriptions, invoices,
    and payment intent operations.

    Args:
        config: Stripe configuration containing api_key
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.api_key = self.config.get("api_key", "")
        self.api_base = "https://api.stripe.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Stripe API."""
        url = f"{self.api_base}/{endpoint}"

        if method == "GET" and data:
            from urllib.parse import urlencode
            url = f"{url}?{urlencode(data)}"
            body = None
        elif data:
            from urllib.parse import urlencode
            body = urlencode(data).encode("utf-8")
        else:
            body = None

        headers = dict(self.headers)
        if body:
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        req = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def create_customer(
        self,
        email: Optional[str] = None,
        name: Optional[str] = None,
        phone: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> ActionResult:
        """Create a new customer.

        Args:
            email: Customer email
            name: Customer name
            phone: Customer phone
            metadata: Optional metadata dict

        Returns:
            ActionResult with created customer
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        data = {}
        if email:
            data["email"] = email
        if name:
            data["name"] = name
        if phone:
            data["phone"] = phone
        if metadata:
            for k, v in metadata.items():
                data[f"metadata[{k}]"] = v

        result = self._make_request("POST", "customers", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def get_customer(self, customer_id: str) -> ActionResult:
        """Retrieve a customer.

        Args:
            customer_id: Stripe customer ID

        Returns:
            ActionResult with customer data
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        result = self._make_request("GET", f"customers/{customer_id}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def create_payment_intent(
        self,
        amount: int,
        currency: str = "usd",
        customer: Optional[str] = None,
        metadata: Optional[Dict] = None,
        description: Optional[str] = None,
    ) -> ActionResult:
        """Create a payment intent.

        Args:
            amount: Amount in smallest currency unit (cents)
            currency: Currency code (usd, eur, etc.)
            customer: Optional customer ID
            metadata: Optional metadata dict
            description: Payment description

        Returns:
            ActionResult with payment intent
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        data = {
            "amount": amount,
            "currency": currency,
        }
        if customer:
            data["customer"] = customer
        if description:
            data["description"] = description
        if metadata:
            for k, v in metadata.items():
                data[f"metadata[{k}]"] = v

        result = self._make_request("POST", "payment_intents", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def create_charge(
        self,
        amount: int,
        currency: str = "usd",
        source: Optional[str] = None,
        customer: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> ActionResult:
        """Create a charge.

        Args:
            amount: Amount in smallest currency unit
            currency: Currency code
            source: Payment source token
            customer: Customer ID
            description: Charge description
            metadata: Optional metadata dict

        Returns:
            ActionResult with charge data
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        data = {
            "amount": amount,
            "currency": currency,
        }
        if source:
            data["source"] = source
        if customer:
            data["customer"] = customer
        if description:
            data["description"] = description
        if metadata:
            for k, v in metadata.items():
                data[f"metadata[{k}]"] = v

        result = self._make_request("POST", "charges", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def create_subscription(
        self,
        customer: str,
        price_id: str,
        quantity: int = 1,
        metadata: Optional[Dict] = None,
    ) -> ActionResult:
        """Create a subscription.

        Args:
            customer: Customer ID
            price_id: Price ID from Stripe
            quantity: Number of subscriptions
            metadata: Optional metadata dict

        Returns:
            ActionResult with subscription data
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        data = {
            "customer": customer,
            "items[0][price]": price_id,
            "items[0][quantity]": quantity,
        }
        if metadata:
            for k, v in metadata.items():
                data[f"metadata[{k}]"] = v

        result = self._make_request("POST", "subscriptions", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def cancel_subscription(self, subscription_id: str) -> ActionResult:
        """Cancel a subscription.

        Args:
            subscription_id: Subscription ID

        Returns:
            ActionResult with cancellation status
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        result = self._make_request(
            "DELETE", f"subscriptions/{subscription_id}"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def list_invoices(
        self,
        customer: Optional[str] = None,
        limit: int = 10,
    ) -> ActionResult:
        """List invoices.

        Args:
            customer: Optional customer ID to filter by
            limit: Maximum number of invoices

        Returns:
            ActionResult with invoices list
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        data = {"limit": limit}
        if customer:
            data["customer"] = customer

        result = self._make_request("GET", "invoices", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"invoices": result.get("data", [])})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Stripe operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "create_customer": self.create_customer,
            "get_customer": self.get_customer,
            "create_payment_intent": self.create_payment_intent,
            "create_charge": self.create_charge,
            "create_subscription": self.create_subscription,
            "cancel_subscription": self.cancel_subscription,
            "list_invoices": self.list_invoices,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
