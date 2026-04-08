"""Data generator action module for RabAI AutoClick.

Provides data generation operations:
- DataGeneratorAction: Generate data based on templates
- SequenceGeneratorAction: Generate sequences
- RandomDataGeneratorAction: Generate random data
- TemplateDataGeneratorAction: Generate data from templates
- FakerDataGeneratorAction: Generate realistic fake data
"""

import random
import string
import uuid
import hashlib
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataGeneratorAction(BaseAction):
    """Generate data based on configuration."""
    action_type = "data_generator"
    display_name = "数据生成"
    description = "根据配置生成数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            generator_type = params.get("generator_type", "random")
            count = params.get("count", 1)
            schema = params.get("schema", {})
            seed = params.get("seed", None)

            if seed is not None:
                random.seed(seed)

            if generator_type == "random":
                data = self._generate_random(count, schema)
            elif generator_type == "sequence":
                data = self._generate_sequence(count, schema)
            elif generator_type == "template":
                data = self._generate_from_template(count, schema)
            else:
                return ActionResult(success=False, message=f"Unknown generator type: {generator_type}")

            return ActionResult(
                success=True,
                data={
                    "generator_type": generator_type,
                    "count": count,
                    "generated": data,
                    "seed": seed
                },
                message=f"Generated {count} items using '{generator_type}' generator"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data generator error: {str(e)}")

    def _generate_random(self, count: int, schema: Dict) -> List:
        results = []
        for _ in range(count):
            item = {}
            for field, field_config in schema.items():
                field_type = field_config.get("type", "string")
                item[field] = self._generate_random_value(field_type, field_config)
            results.append(item)
        return results

    def _generate_random_value(self, field_type: str, config: Dict) -> Any:
        if field_type == "string":
            length = config.get("length", 10)
            return ''.join(random.choices(string.ascii_letters, k=length))
        elif field_type == "int":
            min_val = config.get("min", 0)
            max_val = config.get("max", 100)
            return random.randint(min_val, max_val)
        elif field_type == "float":
            min_val = config.get("min", 0.0)
            max_val = config.get("max", 1.0)
            return random.uniform(min_val, max_val)
        elif field_type == "bool":
            return random.choice([True, False])
        elif field_type == "uuid":
            return str(uuid.uuid4())
        else:
            return None

    def _generate_sequence(self, count: int, schema: Dict) -> List:
        start = schema.get("start", 1)
        step = schema.get("step", 1)
        prefix = schema.get("prefix", "")
        suffix = schema.get("suffix", "")
        return [f"{prefix}{start + i * step}{suffix}" for i in range(count)]

    def _generate_from_template(self, count: int, template: str) -> List:
        results = []
        for _ in range(count):
            result = template.replace("{uuid}", str(uuid.uuid4()))
            result = result.replace("{timestamp}", str(int(datetime.now().timestamp())))
            result = result.replace("{random}", ''.join(random.choices(string.ascii_letters, k=8)))
            results.append(result)
        return results


class SequenceGeneratorAction(BaseAction):
    """Generate sequences of data."""
    action_type = "sequence_generator"
    display_name = "序列生成"
    description = "生成数据序列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            sequence_type = params.get("sequence_type", "numeric")
            start = params.get("start", 1)
            count = params.get("count", 10)
            step = params.get("step", 1)
            format_spec = params.get("format", None)

            if sequence_type == "numeric":
                sequence = [start + i * step for i in range(count)]
            elif sequence_type == "alphanumeric":
                chars = string.ascii_uppercase + string.digits
                sequence = [''.join(random.choices(chars, k=6)) for _ in range(count)]
            elif sequence_type == "alphabetic":
                sequence = [''.join(random.choices(string.ascii_uppercase, k=6)) for _ in range(count)]
            elif sequence_type == "date":
                base_date = datetime.now()
                sequence = [(base_date + timedelta(days=i * step)).isoformat() for i in range(count)]
            elif sequence_type == "uuid":
                sequence = [str(uuid.uuid4()) for _ in range(count)]
            elif sequence_type == "hash":
                sequence = [hashlib.md5(str(i).encode()).hexdigest() for i in range(start, start + count * step, step)]
            else:
                return ActionResult(success=False, message=f"Unknown sequence type: {sequence_type}")

            if format_spec:
                try:
                    sequence = [format_spec.format(x) for x in sequence]
                except:
                    pass

            return ActionResult(
                success=True,
                data={
                    "sequence_type": sequence_type,
                    "start": start,
                    "count": count,
                    "step": step,
                    "sequence": sequence
                },
                message=f"Generated {sequence_type} sequence: {count} items"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Sequence generator error: {str(e)}")


class RandomDataGeneratorAction(BaseAction):
    """Generate random data."""
    action_type = "random_data_generator"
    display_name = "随机数据生成"
    description = "生成随机数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data_type = params.get("data_type", "string")
            count = params.get("count", 10)
            min_val = params.get("min", 0)
            max_val = params.get("max", 100)
            length = params.get("length", 10)
            charset = params.get("charset", "alphanumeric")
            seed = params.get("seed", None)

            if seed is not None:
                random.seed(seed)

            charsets = {
                "alphanumeric": string.ascii_letters + string.digits,
                "alpha": string.ascii_letters,
                "numeric": string.digits,
                "hex": string.hexdigits.lower(),
                "ascii": string.printable
            }

            valid_chars = charsets.get(charset, charsets["alphanumeric"])

            if data_type == "string":
                data = [''.join(random.choices(valid_chars, k=length)) for _ in range(count)]
            elif data_type == "int":
                data = [random.randint(min_val, max_val) for _ in range(count)]
            elif data_type == "float":
                data = [random.uniform(min_val, max_val) for _ in range(count)]
            elif data_type == "bool":
                data = [random.choice([True, False]) for _ in range(count)]
            elif data_type == "choice":
                choices = params.get("choices", ["a", "b", "c"])
                data = [random.choice(choices) for _ in range(count)]
            elif data_type == "uuid":
                data = [str(uuid.uuid4()) for _ in range(count)]
            else:
                return ActionResult(success=False, message=f"Unknown data type: {data_type}")

            return ActionResult(
                success=True,
                data={
                    "data_type": data_type,
                    "count": count,
                    "data": data,
                    "seed": seed
                },
                message=f"Generated {count} random {data_type} values"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Random data generator error: {str(e)}")


class TemplateDataGeneratorAction(BaseAction):
    """Generate data from templates."""
    action_type = "template_data_generator"
    display_name = "模板数据生成"
    description = "从模板生成数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            template = params.get("template", "{name}_{id}")
            count = params.get("count", 10)
            variables = params.get("variables", {})

            if not template:
                return ActionResult(success=False, message="template is required")

            generated = []
            for i in range(count):
                result = template
                result = result.replace("{i}", str(i))
                result = result.replace("{i1}", str(i + 1))
                result = result.replace("{uuid}", str(uuid.uuid4()))
                result = result.replace("{timestamp}", str(int(datetime.now().timestamp())))
                result = result.replace("{date}", datetime.now().date().isoformat())
                result = result.replace("{datetime}", datetime.now().isoformat())
                result = result.replace("{random}", ''.join(random.choices(string.ascii_lowercase, k=8)))

                for var_name, var_values in variables.items():
                    placeholder = f"{{{var_name}}}"
                    if placeholder in result:
                        value = var_values[i % len(var_values)] if isinstance(var_values, list) else var_values
                        result = result.replace(placeholder, str(value))

                generated.append(result)

            return ActionResult(
                success=True,
                data={
                    "template": template,
                    "count": count,
                    "generated": generated,
                    "variables": list(variables.keys())
                },
                message=f"Generated {count} items from template"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Template data generator error: {str(e)}")


class FakerDataGeneratorAction(BaseAction):
    """Generate realistic fake data."""
    action_type = "faker_data_generator"
    display_name = "伪造数据生成"
    description = "生成真实的伪造数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data_category = params.get("category", "person")
            count = params.get("count", 10)
            locale = params.get("locale", "en_US")

            if data_category == "person":
                generated = self._generate_person_data(count)
            elif data_category == "address":
                generated = self._generate_address_data(count)
            elif data_category == "company":
                generated = self._generate_company_data(count)
            elif data_category == "internet":
                generated = self._generate_internet_data(count)
            elif data_category == "datetime":
                generated = self._generate_datetime_data(count)
            else:
                return ActionResult(success=False, message=f"Unknown category: {data_category}")

            return ActionResult(
                success=True,
                data={
                    "category": data_category,
                    "locale": locale,
                    "count": count,
                    "generated": generated
                },
                message=f"Generated {count} {data_category} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Faker data generator error: {str(e)}")

    def _generate_person_data(self, count: int) -> List[Dict]:
        first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia"]
        return [
            {
                "first_name": random.choice(first_names),
                "last_name": random.choice(last_names),
                "age": random.randint(18, 80),
                "email": f"user{i}@example.com"
            }
            for i in range(count)
        ]

    def _generate_address_data(self, count: int) -> List[Dict]:
        streets = ["Main St", "Oak Ave", "Park Blvd", "Lake Dr", "Hill Rd"]
        cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
        return [
            {
                "street": f"{random.randint(100, 9999)} {random.choice(streets)}",
                "city": random.choice(cities),
                "zip": f"{random.randint(10000, 99999)}",
                "country": "USA"
            }
            for _ in range(count)
        ]

    def _generate_company_data(self, count: int) -> List[Dict]:
        prefixes = ["Tech", "Global", "Premier", "United", "Dynamic"]
        suffixes = ["Corp", "Inc", "LLC", "Solutions", "Systems"]
        return [
            {
                "company_name": f"{random.choice(prefixes)} {random.choice(suffixes)}",
                "industry": random.choice(["Technology", "Finance", "Healthcare", "Retail", "Manufacturing"]),
                "employee_count": random.randint(10, 10000)
            }
            for _ in range(count)
        ]

    def _generate_internet_data(self, count: int) -> List[Dict]:
        domains = ["example.com", "test.com", "demo.com", "sample.org"]
        return [
            {
                "email": f"user{i}@{random.choice(domains)}",
                "username": f"user{i}_{random.randint(100, 999)}",
                "url": f"https://{random.choice(domains)}/user/{i}"
            }
            for i in range(count)
        ]

    def _generate_datetime_data(self, count: int) -> List[Dict]:
        base = datetime.now()
        return [
            {
                "date": (base - timedelta(days=random.randint(0, 365))).date().isoformat(),
                "time": f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}",
                "timestamp": int((base - timedelta(days=random.randint(0, 365))).timestamp())
            }
            for _ in range(count)
        ]
