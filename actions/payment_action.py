"""
Payment and billing utilities - currency conversion, invoice generation, tax calculation.
"""
from typing import Any, Dict, List, Optional, Tuple
import logging
import hashlib
import time
import uuid

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _format_currency(amount: float, currency: str = "USD") -> str:
    symbols = {"USD": "$", "EUR": "€", "GBP": "£", "JPY": "¥", "CNY": "¥", "KRW": "₩"}
    symbol = symbols.get(currency, currency + " ")
    if currency == "JPY":
        return f"{symbol}{int(amount):,}"
    return f"{symbol}{amount:,.2f}"


def _parse_currency(value: str) -> Tuple[float, str]:
    import re
    pattern = r"([A-Z]{3})?\s*([\d,]+\.?\d*)"
    match = re.match(pattern, value.upper().strip())
    if match:
        currency = match.group(1) or "USD"
        amount = float(match.group(2).replace(",", ""))
        return amount, currency
    return 0.0, "USD"


def _calculate_tax(amount: float, rate: float) -> float:
    return round(amount * rate, 2)


def _calculate_discount(amount: float, discount: float, discount_type: str = "percent") -> float:
    if discount_type == "percent":
        return round(amount * (discount / 100.0), 2)
    return round(min(discount, amount), 2)


class PaymentAction(BaseAction):
    """Payment and billing operations.

    Provides currency formatting, tax/discount calculation, invoice generation, payment processing.
    Note: Requires Stripe/PayPal credentials for actual payment processing.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "format_currency")
        amount = float(params.get("amount", 0))
        currency = params.get("currency", "USD")

        try:
            if operation == "format_currency":
                formatted = _format_currency(amount, currency)
                return {"success": True, "formatted": formatted, "amount": amount, "currency": currency}

            elif operation == "parse_currency":
                value = params.get("value", "")
                parsed_amount, parsed_currency = _parse_currency(value)
                return {"success": True, "amount": parsed_amount, "currency": parsed_currency}

            elif operation == "calculate_tax":
                rate = float(params.get("rate", 0))
                inclusive = params.get("inclusive", False)
                if inclusive:
                    net = amount / (1 + rate)
                    tax = amount - net
                else:
                    tax = _calculate_tax(amount, rate)
                    net = amount
                return {
                    "success": True,
                    "gross": round(net + tax, 2),
                    "net": round(net, 2),
                    "tax": round(tax, 2),
                    "rate": rate,
                }

            elif operation == "calculate_discount":
                discount = float(params.get("discount", 0))
                discount_type = params.get("discount_type", "percent")
                discounted = _calculate_discount(amount, discount, discount_type)
                return {
                    "success": True,
                    "original": amount,
                    "discounted": round(amount - discounted, 2),
                    "discount_amount": discounted,
                    "discount_type": discount_type,
                }

            elif operation == "split_payment":
                total = float(params.get("total", amount))
                num_people = int(params.get("num_people", 2))
                if num_people <= 0:
                    return {"success": False, "error": "num_people must be positive"}
                share = round(total / num_people, 2)
                remainder = round(total - share * num_people, 2)
                return {
                    "success": True,
                    "per_person": share,
                    "num_people": num_people,
                    "remainder": remainder,
                    "total": total,
                }

            elif operation == "tip_calculation":
                bill = float(params.get("bill", amount))
                tip_percent = float(params.get("tip_percent", 15))
                num_people = int(params.get("num_people", 1))
                tip = round(bill * tip_percent / 100, 2)
                total = round(bill + tip, 2)
                per_person = round(total / num_people, 2)
                return {
                    "success": True,
                    "bill": bill,
                    "tip": tip,
                    "total": total,
                    "per_person": per_person,
                    "tip_percent": tip_percent,
                }

            elif operation == "currency_conversion":
                from_currency = params.get("from_currency", currency)
                to_currency = params.get("to_currency", "USD")
                rates = {
                    ("USD", "EUR"): 0.85, ("USD", "GBP"): 0.73, ("USD", "JPY"): 110.0,
                    ("USD", "CNY"): 6.45, ("EUR", "USD"): 1.18, ("GBP", "USD"): 1.37,
                }
                rate = rates.get((from_currency, to_currency), 1.0)
                converted = round(amount * rate, 2)
                return {
                    "success": True,
                    "from": {"amount": amount, "currency": from_currency},
                    "to": {"amount": converted, "currency": to_currency},
                    "rate": rate,
                }

            elif operation == "generate_invoice":
                invoice_items = params.get("items", [])
                if not invoice_items:
                    return {"success": False, "error": "items required"}
                subtotal = sum(float(item.get("amount", 0)) * int(item.get("quantity", 1)) for item in invoice_items)
                tax_rate = float(params.get("tax_rate", 0))
                discount_amount = float(params.get("discount", 0))
                after_discount = subtotal - discount_amount
                tax = round(after_discount * tax_rate, 2)
                total = round(after_discount + tax, 2)
                invoice_id = f"INV-{int(time.time())}-{uuid.uuid4().hex[:4].upper()}"
                invoice = {
                    "id": invoice_id,
                    "items": invoice_items,
                    "subtotal": subtotal,
                    "discount": discount_amount,
                    "tax": tax,
                    "tax_rate": tax_rate,
                    "total": total,
                    "currency": currency,
                    "formatted_total": _format_currency(total, currency),
                    "created_at": time.strftime("%Y-%m-%d"),
                }
                return {"success": True, "invoice": invoice}

            elif operation == "generate_receipt":
                items = params.get("items", [])
                payment_method = params.get("payment_method", "card")
                total = float(params.get("total", amount))
                receipt_id = f"RCP-{int(time.time())}-{uuid.uuid4().hex[:4].upper()}"
                receipt = {
                    "id": receipt_id,
                    "items": items,
                    "total": total,
                    "payment_method": payment_method,
                    "currency": currency,
                    "formatted_total": _format_currency(total, currency),
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                }
                return {"success": True, "receipt": receipt}

            elif operation == "calculate_installment":
                principal = float(params.get("principal", amount))
                annual_rate = float(params.get("annual_rate", 0))
                months = int(params.get("months", 12))
                monthly_rate = annual_rate / 12 / 100
                if monthly_rate > 0:
                    payment = principal * (monthly_rate * (1 + monthly_rate) ** months) / ((1 + monthly_rate) ** months - 1)
                else:
                    payment = principal / months
                payment = round(payment, 2)
                total_interest = round(payment * months - principal, 2)
                return {
                    "success": True,
                    "monthly_payment": payment,
                    "total_interest": total_interest,
                    "total_cost": round(payment * months, 2),
                    "principal": principal,
                    "annual_rate": annual_rate,
                    "months": months,
                }

            elif operation == "price_tiers":
                quantity = int(params.get("quantity", 1))
                tiers = params.get("tiers", [{"min": 1, "max": 10, "unit_price": 10}, {"min": 11, "max": 100, "unit_price": 8}])
                applicable_tier = None
                for tier in tiers:
                    if tier["min"] <= quantity <= tier.get("max", float("inf")):
                        applicable_tier = tier
                        break
                if not applicable_tier:
                    applicable_tier = tiers[-1]
                unit_price = applicable_tier["unit_price"]
                subtotal = round(unit_price * quantity, 2)
                return {
                    "success": True,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "subtotal": subtotal,
                    "tier": applicable_tier,
                }

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"PaymentAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for payment operations."""
    return PaymentAction().execute(context, params)
