"""Data generation action module for RabAI AutoClick.

Provides synthetic data generation for testing including
random numbers, strings, dates, and structured data.
"""

import random
import string
import time
import uuid
import hashlib
import sys
import os
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataGenerator:
    """Generate synthetic data for testing."""
    
    FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rose", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xavier", "Yara", "Zack"]
    LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez"]
    DOMAINS = ["example.com", "test.org", "demo.net", "sample.io", "mail.com", "work.com"]
    CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    COUNTRIES = ["USA", "Canada", "UK", "Germany", "France", "Japan", "Australia", "Brazil", "India", "China"]
    
    @classmethod
    def random_int(cls, min_val: int = 0, max_val: int = 100) -> int:
        """Generate random integer.
        
        Args:
            min_val: Minimum value.
            max_val: Maximum value.
        
        Returns:
            Random integer.
        """
        return random.randint(min_val, max_val)
    
    @classmethod
    def random_float(cls, min_val: float = 0.0, max_val: float = 100.0, decimals: int = 2) -> float:
        """Generate random float.
        
        Args:
            min_val: Minimum value.
            max_val: Maximum value.
            decimals: Number of decimal places.
        
        Returns:
            Random float.
        """
        value = random.uniform(min_val, max_val)
        return round(value, decimals)
    
    @classmethod
    def random_string(cls, length: int = 10, charset: str = None) -> str:
        """Generate random string.
        
        Args:
            length: String length.
            charset: Character set to use.
        
        Returns:
            Random string.
        """
        if charset is None:
            charset = string.ascii_letters + string.digits
        
        return ''.join(random.choices(charset, k=length))
    
    @classmethod
    def random_email(cls, first_name: str = None, last_name: str = None) -> str:
        """Generate random email.
        
        Args:
            first_name: Optional first name.
            last_name: Optional last name.
        
        Returns:
            Random email address.
        """
        if first_name is None:
            first_name = random.choice(cls.FIRST_NAMES).lower()
        if last_name is None:
            last_name = random.choice(cls.LAST_NAMES).lower()
        
        domain = random.choice(cls.DOMAINS)
        
        patterns = [
            f"{first_name}.{last_name}@{domain}",
            f"{first_name[0]}{last_name}@{domain}",
            f"{first_name}{last_name[0]}@{domain}",
            f"{first_name}_{last_name}@{domain}",
        ]
        
        return random.choice(patterns)
    
    @classmethod
    def random_name(cls) -> str:
        """Generate random full name."""
        return f"{random.choice(cls.FIRST_NAMES)} {random.choice(cls.LAST_NAMES)}"
    
    @classmethod
    def random_date(cls, start_date: str = None, end_date: str = None, fmt: str = "%Y-%m-%d") -> str:
        """Generate random date.
        
        Args:
            start_date: Start date string.
            end_date: End date string.
            fmt: Output format.
        
        Returns:
            Random date string.
        """
        if start_date:
            start = datetime.strptime(start_date, fmt)
        else:
            start = datetime.now() - timedelta(days=365)
        
        if end_date:
            end = datetime.strptime(end_date, fmt)
        else:
            end = datetime.now()
        
        delta = end - start
        random_days = random.randint(0, delta.days)
        random_date = start + timedelta(days=random_days)
        
        return random_date.strftime(fmt)
    
    @classmethod
    def random_uuid(cls) -> str:
        """Generate random UUID."""
        return str(uuid.uuid4())
    
    @classmethod
    def random_bool(cls) -> bool:
        """Generate random boolean."""
        return random.choice([True, False])
    
    @classmethod
    def random_choice(cls, choices: List[Any]) -> Any:
        """Random choice from list.
        
        Args:
            choices: List of options.
        
        Returns:
            Random item.
        """
        return random.choice(choices)
    
    @classmethod
    def random_phone(cls) -> str:
        """Generate random phone number."""
        return f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
    
    @classmethod
    def random_address(cls) -> Dict[str, str]:
        """Generate random address."""
        return {
            "street": f"{random.randint(1, 9999)} {random.choice(['Main', 'Oak', 'Maple', 'Cedar', 'Pine'])} {random.choice(['St', 'Ave', 'Rd', 'Blvd', 'Ln'])}",
            "city": random.choice(cls.CITIES),
            "state": random.choice(['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']),
            "zip": f"{random.randint(10000, 99999)}",
            "country": random.choice(cls.COUNTRIES)
        }
    
    @classmethod
    def random_ip(cls) -> str:
        """Generate random IP address."""
        return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"
    
    @classmethod
    def random_url(cls) -> str:
        """Generate random URL."""
        schemes = ["http", "https"]
        return f"{random.choice(schemes)}://{random.choice(cls.DOMAINS)}/{cls.random_string(8)}"
    
    @classmethod
    def random_color(cls) -> str:
        """Generate random hex color."""
        return "#{:06x}".format(random.randint(0, 0xFFFFFF))
    
    @classmethod
    def random_credit_card(cls) -> str:
        """Generate random credit card number (fake)."""
        return f"{random.randint(4000, 4999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
    
    @classmethod
    def generate_record(cls, schema: Dict[str, str]) -> Dict[str, Any]:
        """Generate a record based on schema.
        
        Args:
            schema: Dict mapping field names to type names.
        
        Returns:
            Generated record.
        """
        record = {}
        
        for field_name, field_type in schema.items():
            if field_type == 'int':
                record[field_name] = cls.random_int()
            elif field_type == 'float':
                record[field_name] = cls.random_float()
            elif field_type == 'string':
                record[field_name] = cls.random_string()
            elif field_type == 'email':
                record[field_name] = cls.random_email()
            elif field_type == 'name':
                record[field_name] = cls.random_name()
            elif field_type == 'date':
                record[field_name] = cls.random_date()
            elif field_type == 'uuid':
                record[field_name] = cls.random_uuid()
            elif field_type == 'bool':
                record[field_name] = cls.random_bool()
            elif field_type == 'phone':
                record[field_name] = cls.random_phone()
            elif field_type == 'address':
                record[field_name] = cls.random_address()
            elif field_type == 'ip':
                record[field_name] = cls.random_ip()
            elif field_type == 'url':
                record[field_name] = cls.random_url()
            elif field_type == 'color':
                record[field_name] = cls.random_color()
            elif field_type == 'choice':
                record[field_name] = cls.random_choice([True, False, None, "yes", "no"])
            else:
                record[field_name] = cls.random_string()
        
        return record


class GenerateNumberAction(BaseAction):
    """Generate random number."""
    action_type = "generate_number"
    display_name = "生成数字"
    description = "生成随机数字"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate random number.
        
        Args:
            context: Execution context.
            params: Dict with keys: min, max, type (int/float), decimals.
        
        Returns:
            ActionResult with generated number.
        """
        num_type = params.get('type', 'int')
        min_val = params.get('min', 0)
        max_val = params.get('max', 100)
        decimals = params.get('decimals', 2)
        
        try:
            if num_type == 'float':
                value = DataGenerator.random_float(min_val, max_val, decimals)
            else:
                value = DataGenerator.random_int(int(min_val), int(max_val))
            
            return ActionResult(success=True, message=f"Generated {num_type}", data={"value": value, "type": num_type})
        except Exception as e:
            return ActionResult(success=False, message=f"Generation error: {str(e)}")


class GenerateStringAction(BaseAction):
    """Generate random string."""
    action_type = "generate_string"
    display_name = "生成字符串"
    description = "生成随机字符串"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate random string.
        
        Args:
            context: Execution context.
            params: Dict with keys: length, charset.
        
        Returns:
            ActionResult with generated string.
        """
        length = params.get('length', 10)
        charset = params.get('charset', None)
        
        try:
            value = DataGenerator.random_string(length, charset)
            
            return ActionResult(success=True, message=f"Generated string of length {length}", data={"value": value, "length": length})
        except Exception as e:
            return ActionResult(success=False, message=f"Generation error: {str(e)}")


class GenerateEmailAction(BaseAction):
    """Generate random email."""
    action_type = "generate_email"
    display_name = "生成邮箱"
    description = "生成随机邮箱"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate random email.
        
        Args:
            context: Execution context.
            params: Dict with keys: first_name, last_name.
        
        Returns:
            ActionResult with generated email.
        """
        first_name = params.get('first_name', None)
        last_name = params.get('last_name', None)
        
        try:
            value = DataGenerator.random_email(first_name, last_name)
            
            return ActionResult(success=True, message="Generated email", data={"value": value})
        except Exception as e:
            return ActionResult(success=False, message=f"Generation error: {str(e)}")


class GenerateDateAction(BaseAction):
    """Generate random date."""
    action_type = "generate_date"
    display_name = "生成日期"
    description = "生成随机日期"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate random date.
        
        Args:
            context: Execution context.
            params: Dict with keys: start_date, end_date, format.
        
        Returns:
            ActionResult with generated date.
        """
        start_date = params.get('start_date', None)
        end_date = params.get('end_date', None)
        fmt = params.get('format', '%Y-%m-%d')
        
        try:
            value = DataGenerator.random_date(start_date, end_date, fmt)
            
            return ActionResult(success=True, message="Generated date", data={"value": value, "format": fmt})
        except Exception as e:
            return ActionResult(success=False, message=f"Generation error: {str(e)}")


class GenerateUUIDAction(BaseAction):
    """Generate random UUID."""
    action_type = "generate_uuid"
    display_name = "生成UUID"
    description = "生成随机UUID"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate random UUID.
        
        Args:
            context: Execution context.
            params: Dict (unused).
        
        Returns:
            ActionResult with generated UUID.
        """
        value = DataGenerator.random_uuid()
        
        return ActionResult(success=True, message="Generated UUID", data={"value": value})


class GenerateBatchAction(BaseAction):
    """Generate batch of random records."""
    action_type = "generate_batch"
    display_name = "批量生成"
    description = "批量生成测试数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate batch of records.
        
        Args:
            context: Execution context.
            params: Dict with keys: count, schema.
        
        Returns:
            ActionResult with generated records.
        """
        count = params.get('count', 10)
        schema = params.get('schema', {})
        
        if not schema:
            schema = {
                "id": "uuid",
                "name": "name",
                "email": "email",
                "age": "int",
                "active": "bool"
            }
        
        try:
            records = [DataGenerator.generate_record(schema) for _ in range(count)]
            
            return ActionResult(
                success=True,
                message=f"Generated {count} records",
                data={"records": records, "count": count, "schema": schema}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Generation error: {str(e)}")


class GenerateRecordAction(BaseAction):
    """Generate a single record based on schema."""
    action_type = "generate_record"
    display_name = "生成记录"
    description = "根据Schema生成单条记录"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate record.
        
        Args:
            context: Execution context.
            params: Dict with keys: schema.
        
        Returns:
            ActionResult with generated record.
        """
        schema = params.get('schema', {})
        
        if not schema:
            schema = {
                "name": "name",
                "email": "email",
                "phone": "phone",
                "address": "address"
            }
        
        try:
            record = DataGenerator.generate_record(schema)
            
            return ActionResult(success=True, message="Generated record", data={"record": record})
        except Exception as e:
            return ActionResult(success=False, message=f"Generation error: {str(e)}")
