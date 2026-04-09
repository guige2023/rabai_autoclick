"""
Test Data Utilities for UI Automation.

This module provides utilities for generating, managing, and manipulating
test data for automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import random
import string
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Optional


@dataclass
class TestDataProfile:
    """
    A test data profile for generating data.
    
    Attributes:
        name: Profile name
        first_name: First name
        last_name: Last name
        email: Email address
        phone: Phone number
        address: Street address
        city: City name
        state: State/Province
        zip_code: Postal code
        country: Country
    """
    name: str = ""
    first_name: str = ""
    last_name: str = ""
    email: str = ""
    phone: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    country: str = ""


class DataGenerator:
    """
    Generates various types of test data.
    
    Example:
        gen = DataGenerator()
        email = gen.email("testuser")
        phone = gen.phone()
    """
    
    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
    
    def email(self, username: Optional[str] = None) -> str:
        """
        Generate an email address.
        
        Args:
            username: Optional username part
            
        Returns:
            Email address string
        """
        if username is None:
            username = self.alphanumeric(8)
        
        domains = ["example.com", "test.com", "demo.org"]
        return f"{username}@{random.choice(domains)}"
    
    def phone(self, format: str = "us") -> str:
        """
        Generate a phone number.
        
        Args:
            format: Phone format ("us", "uk", "international")
            
        Returns:
            Phone number string
        """
        if format == "us":
            area = random.randint(200, 999)
            exchange = random.randint(200, 999)
            subscriber = random.randint(1000, 9999)
            return f"({area}) {exchange}-{subscriber}"
        elif format == "uk":
            return f"+44 {random.randint(1000, 9999)} {random.randint(1000, 9999)}"
        else:
            return f"+1 {random.randint(200, 999)} {random.randint(1000, 9999)}"
    
    def alphanumeric(self, length: int = 8, include_spaces: bool = False) -> str:
        """
        Generate an alphanumeric string.
        
        Args:
            length: Length of string
            include_spaces: Whether to include spaces
            
        Returns:
            Random alphanumeric string
        """
        chars = string.ascii_letters + string.digits
        if include_spaces:
            chars += " "
        return "".join(random.choices(chars, k=length))
    
    def numeric(self, length: int = 8) -> str:
        """Generate a numeric string."""
        return "".join(random.choices(string.digits, k=length))
    
    def alphabetic(self, length: int = 8, mixed_case: bool = True) -> str:
        """Generate an alphabetic string."""
        if mixed_case:
            chars = string.ascii_letters
        else:
            chars = string.ascii_lowercase
        return "".join(random.choices(chars, k=length))
    
    def password(
        self,
        length: int = 12,
        include_special: bool = True,
        include_digits: bool = True
    ) -> str:
        """
        Generate a password string.
        
        Args:
            length: Password length
            include_special: Include special characters
            include_digits: Include digits
            
        Returns:
            Password string
        """
        chars = string.ascii_letters
        if include_digits:
            chars += string.digits
        if include_special:
            chars += "!@#$%^&*()_+-="
        
        return "".join(random.choices(chars, k=length))
    
    def uuid(self) -> str:
        """Generate a UUID string."""
        return str(uuid.uuid4())
    
    def date(
        self,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> datetime:
        """
        Generate a random date.
        
        Args:
            start_year: Start year (default: current year - 10)
            end_year: End year (default: current year)
            
        Returns:
            Random datetime
        """
        now = datetime.now()
        start = datetime(start_year or now.year - 10, 1, 1)
        end = datetime(end_year or now.year, 12, 31)
        
        delta = end - start
        random_days = random.randint(0, delta.days)
        
        return start + timedelta(days=random_days)
    
    def date_string(
        self,
        format: str = "%Y-%m-%d",
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> str:
        """
        Generate a date as a formatted string.
        
        Args:
            format: Date format string
            start_year: Start year
            end_year: End year
            
        Returns:
            Formatted date string
        """
        dt = self.date(start_year, end_year)
        return dt.strftime(format)
    
    def url(self) -> str:
        """Generate a random URL."""
        domains = ["example.com", "test.org", "demo.net"]
        paths = ["", "/page", "/products", "/about", "/contact"]
        protocols = ["http", "https"]
        
        protocol = random.choice(protocols)
        domain = random.choice(domains)
        path = random.choice(paths)
        
        return f"{protocol}://www.{domain}{path}"
    
    def ip_address(self) -> str:
        """Generate a random IP address."""
        return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"
    
    def credit_card(self, card_type: Optional[str] = None) -> str:
        """
        Generate a credit card number (test data only).
        
        Args:
            card_type: Card type ("visa", "mastercard", "amex")
            
        Returns:
            Test credit card number
        """
        if card_type == "visa":
            prefix = "4"
        elif card_type == "mastercard":
            prefix = "5"
        elif card_type == "amex":
            prefix = "3"
        else:
            prefix = random.choice(["4", "5", "3"])
        
        number = prefix + self.numeric(14)
        return number + self._calculate_luhn_check_digit(number)
    
    def _calculate_luhn_check_digit(self, number: str) -> str:
        """Calculate Luhn check digit."""
        digits = [int(d) for d in number]
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        
        total = sum(odd_digits)
        for d in even_digits:
            total += sum(divmod(d * 2, 10))
        
        return str((10 - (total % 10)) % 10)


class TestDataManager:
    """
    Manages test data pools and profiles.
    
    Example:
        manager = TestDataManager()
        profile = manager.create_profile("user1")
        data = profile.email
    """
    
    def __init__(self):
        self._profiles: dict[str, TestDataProfile] = {}
        self._generator = DataGenerator()
    
    def create_profile(
        self,
        name: str,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        email: Optional[str] = None
    ) -> TestDataProfile:
        """
        Create a test data profile.
        
        Args:
            name: Profile name
            first_name: Optional first name
            last_name: Optional last name
            email: Optional email
            
        Returns:
            Created TestDataProfile
        """
        profile = TestDataProfile(
            name=name,
            first_name=first_name or self._generator.alphabetic(8, mixed_case=False).capitalize(),
            last_name=last_name or self._generator.alphabetic(8, mixed_case=False).capitalize(),
            email=email or self._generator.email(name.lower().replace(" ", ""))
        )
        
        self._profiles[name] = profile
        return profile
    
    def get_profile(self, name: str) -> Optional[TestDataProfile]:
        """Get a profile by name."""
        return self._profiles.get(name)
    
    def list_profiles(self) -> list[str]:
        """List all profile names."""
        return list(self._profiles.keys())
    
    def delete_profile(self, name: str) -> bool:
        """Delete a profile."""
        if name in self._profiles:
            del self._profiles[name]
            return True
        return False
