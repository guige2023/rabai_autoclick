"""Validation action module for RabAI AutoClick.

Provides data validation operations:
- ValidateEmailAction: Validate email
- ValidateURLAction: Validate URL
- ValidateJSONAction: Validate JSON
- ValidatePhoneAction: Validate phone number
- ValidateCreditCardAction: Validate credit card
- ValidatePasswordAction: Validate password strength
- ValidateIPAddressAction: Validate IP address
- ValidateDateAction: Validate date format
"""

import json
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidationRules:
    """Common validation rules."""
    
    EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    PHONE_REGEX = r"^\+?[1-9]\d{1,14}$"
    IPV4_REGEX = r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    URL_REGEX = r"^https?://[^\s/$.?#].[^\s]*$"
    
    @classmethod
    def validate_email(cls, email: str) -> bool:
        """Validate email format."""
        return bool(re.match(cls.EMAIL_REGEX, email))
    
    @classmethod
    def validate_url(cls, url: str) -> bool:
        """Validate URL format."""
        return bool(re.match(cls.URL_REGEX, url))
    
    @classmethod
    def validate_ip(cls, ip: str) -> bool:
        """Validate IPv4 address."""
        return bool(re.match(cls.IPV4_REGEX, ip))
    
    @classmethod
    def validate_phone(cls, phone: str) -> bool:
        """Validate phone number (E.164 format)."""
        cleaned = re.sub(r"[\s\-\(\)]", "", phone)
        return bool(re.match(cls.PHONE_REGEX, cleaned))


class ValidateEmailAction(BaseAction):
    """Validate email address."""
    action_type = "validate_email"
    display_name = "验证邮箱"
    description = "验证邮箱格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            email = params.get("email", "")
            
            if not email:
                return ActionResult(success=False, message="email required")
            
            is_valid = ValidationRules.validate_email(email)
            
            return ActionResult(
                success=is_valid,
                message=f"Email {'valid' if is_valid else 'invalid'}: {email}",
                data={"email": email, "valid": is_valid}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Email validation failed: {str(e)}")


class ValidateURLAction(BaseAction):
    """Validate URL."""
    action_type = "validate_url"
    display_name = "验证URL"
    description = "验证URL格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            
            if not url:
                return ActionResult(success=False, message="url required")
            
            is_valid = ValidationRules.validate_url(url)
            
            return ActionResult(
                success=is_valid,
                message=f"URL {'valid' if is_valid else 'invalid'}: {url}",
                data={"url": url, "valid": is_valid}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"URL validation failed: {str(e)}")


class ValidateJSONAction(BaseAction):
    """Validate JSON."""
    action_type = "validate_json"
    display_name = "验证JSON"
    description = "验证JSON格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            json_str = params.get("json", "")
            file_path = params.get("file_path", "")
            
            if file_path and os.path.exists(file_path):
                with open(file_path, "r") as f:
                    json_str = f.read()
            
            if not json_str:
                return ActionResult(success=False, message="json or file_path required")
            
            try:
                parsed = json.loads(json_str)
                return ActionResult(
                    success=True,
                    message="JSON is valid",
                    data={"valid": True, "parsed": parsed}
                )
            except json.JSONDecodeError as e:
                return ActionResult(
                    success=False,
                    message=f"JSON is invalid: {str(e)}",
                    data={"valid": False, "error": str(e)}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON validation failed: {str(e)}")


class ValidatePhoneAction(BaseAction):
    """Validate phone number."""
    action_type = "validate_phone"
    display_name = "验证电话"
    description = "验证电话号码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            phone = params.get("phone", "")
            
            if not phone:
                return ActionResult(success=False, message="phone required")
            
            is_valid = ValidationRules.validate_phone(phone)
            
            return ActionResult(
                success=is_valid,
                message=f"Phone {'valid' if is_valid else 'invalid'}: {phone}",
                data={"phone": phone, "valid": is_valid}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Phone validation failed: {str(e)}")


class ValidateCreditCardAction(BaseAction):
    """Validate credit card number."""
    action_type = "validate_credit_card"
    display_name = "验证信用卡"
    description = "验证信用卡号"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            card_number = params.get("card_number", "")
            
            if not card_number:
                return ActionResult(success=False, message="card_number required")
            
            cleaned = re.sub(r"[\s\-]", "", card_number)
            
            if not cleaned.isdigit() or len(cleaned) < 13 or len(cleaned) > 19:
                return ActionResult(
                    success=False,
                    message=f"Invalid card number: {card_number}",
                    data={"valid": False, "reason": "Invalid format"}
                )
            
            def luhn_check(number: str) -> bool:
                digits = [int(d) for d in number]
                checksum = 0
                for i, d in enumerate(reversed(digits)):
                    if i % 2 == 1:
                        d *= 2
                        if d > 9:
                            d -= 9
                    checksum += d
                return checksum % 10 == 0
            
            is_valid = luhn_check(cleaned)
            
            card_types = {
                "4": "Visa",
                "5": "Mastercard",
                "3": "Amex",
                "6": "Discover"
            }
            card_type = card_types.get(cleaned[0], "Unknown")
            
            return ActionResult(
                success=is_valid,
                message=f"Credit card {'valid' if is_valid else 'invalid'}: {card_type}",
                data={"valid": is_valid, "card_type": card_type}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Credit card validation failed: {str(e)}")


class ValidatePasswordAction(BaseAction):
    """Validate password strength."""
    action_type = "validate_password"
    display_name = "验证密码"
    description = "验证密码强度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            password = params.get("password", "")
            min_length = params.get("min_length", 8)
            require_uppercase = params.get("require_uppercase", True)
            require_lowercase = params.get("require_lowercase", True)
            require_digit = params.get("require_digit", True)
            require_special = params.get("require_special", True)
            
            if not password:
                return ActionResult(success=False, message="password required")
            
            errors = []
            score = 0
            
            if len(password) >= min_length:
                score += 1
            else:
                errors.append(f"Password must be at least {min_length} characters")
            
            if require_uppercase and re.search(r"[A-Z]", password):
                score += 1
            elif require_uppercase:
                errors.append("Password must contain uppercase letter")
            
            if require_lowercase and re.search(r"[a-z]", password):
                score += 1
            elif require_lowercase:
                errors.append("Password must contain lowercase letter")
            
            if require_digit and re.search(r"\d", password):
                score += 1
            elif require_digit:
                errors.append("Password must contain digit")
            
            if require_special and re.search(r"[!@#$%^&*(),.?\":{}|<>]", password):
                score += 1
            elif require_special:
                errors.append("Password must contain special character")
            
            strength = {1: "weak", 2: "fair", 3: "good", 4: "strong", 5: "very strong"}
            strength_label = strength.get(score, "weak")
            is_valid = score >= 3
            
            return ActionResult(
                success=is_valid,
                message=f"Password strength: {strength_label}",
                data={
                    "valid": is_valid,
                    "strength": strength_label,
                    "score": score,
                    "errors": errors
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Password validation failed: {str(e)}")


class ValidateIPAddressAction(BaseAction):
    """Validate IP address."""
    action_type = "validate_ip"
    display_name = "验证IP"
    description = "验证IP地址"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            ip = params.get("ip", "")
            
            if not ip:
                return ActionResult(success=False, message="ip required")
            
            is_valid = ValidationRules.validate_ip(ip)
            
            return ActionResult(
                success=is_valid,
                message=f"IP {'valid' if is_valid else 'invalid'}: {ip}",
                data={"ip": ip, "valid": is_valid}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"IP validation failed: {str(e)}")


class ValidateDateAction(BaseAction):
    """Validate date format."""
    action_type = "validate_date"
    display_name = "验证日期"
    description = "验证日期格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            date_str = params.get("date", "")
            format_str = params.get("format", "%Y-%m-%d")
            
            if not date_str:
                return ActionResult(success=False, message="date required")
            
            try:
                parsed = datetime.strptime(date_str, format_str)
                return ActionResult(
                    success=True,
                    message=f"Date valid: {date_str}",
                    data={"valid": True, "parsed": parsed.isoformat(), "format": format_str}
                )
            except ValueError:
                return ActionResult(
                    success=False,
                    message=f"Date invalid: {date_str}",
                    data={"valid": False, "expected_format": format_str}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Date validation failed: {str(e)}")
